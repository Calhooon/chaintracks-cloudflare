//! #31 (the fix round of PR #31, issue #32): the three writers of one chain
//! state (the cron's poll, the tip stream's push, the link heal) and the
//! push-live mode that silenced the poll, on the test store (a real SQLite
//! carrying the real migrations, every write through the Worker's own
//! storage functions).
//!
//! What production logged on 2026-11-02 (#32): the cron's poll synced
//! 973789 to 973791 as a reorg from 973788 while the tip stream's push
//! walked the same headers (`storedInactive (walked 3)`); the interleaving
//! left 973791 active over a height with no active row, and from 18:49:37Z
//! every tick logged "the push is live; the couriers are not asked this
//! tick", so no heal ran for eleven hours.
//!
//! The tip stream's object is wasm only; its rules are not
//! (`tip_stream::StreamState::ingested`, `PollState::note_behind`,
//! `poll_due`), and a minute of the object here is those rules applied to
//! the door's answers (`push::ingest_announced`) as the object applies them.

use std::cell::Cell;
use std::rc::Rc;

use crate::consensus::ChainParams;
use crate::host_harness::{
    block_hash, header, RecordedWebhooks, Recorder, ScriptedChain, SqliteDb,
};
use crate::push::ingest_announced;
use crate::storage::{
    self, find_chain_tip, find_header_for_height, insert_header, update_chain_tip_to_highest,
    SQL_INSERT_HEADER,
};
use crate::sync::{run_cron, run_pushed_tick, store_behind};
use crate::tip_stream::{poll_due, PollState, StreamState, HEARTBEAT_S, POLL_QUIET_S};
use crate::types::BlockHeader;

/// The anchor `x`: the one checkpoint of this file's rules.
const X: u32 = 965_769;

fn params() -> &'static ChainParams {
    static PARAMS: std::sync::OnceLock<ChainParams> = std::sync::OnceLock::new();
    PARAMS.get_or_init(|| {
        let x = root();
        ChainParams::regtest()
            .with_checkpoints(&format!("{}:{}", x.height, x.hash))
            .unwrap()
    })
}

fn root() -> BlockHeader {
    header(X, "x", &block_hash("the block below x"))
}

/// `base[..=from]` and `n` blocks on `base[from]`, labelled `label`.
fn branch(base: &[BlockHeader], from: usize, n: u32, label: &str) -> Vec<BlockHeader> {
    let mut run = base[..=from].to_vec();
    for i in 1..=n {
        let parent = run[run.len() - 1].hash.clone();
        let height = run[run.len() - 1].height + 1;
        run.push(header(height, &format!("{label} {i}"), &parent));
    }
    run
}

/// The store linked over `c`, each row through the live insert.
async fn linked_store(c: &[BlockHeader]) -> SqliteDb {
    let db = SqliteDb::migrated();
    for h in c {
        let r = insert_header(&db, params(), h).await.unwrap();
        assert!(r.added, "{r:?}");
    }
    db
}

/// One row written past the rules (a bulk row: `previous_header_id` null,
/// its own work), active or not.
fn raw_row(db: &SqliteDb, h: &BlockHeader, active: bool) {
    db.conn()
        .execute(
            SQL_INSERT_HEADER,
            rusqlite::params![
                Option::<i64>::None,
                h.previous_hash,
                h.height,
                i64::from(active),
                0,
                h.hash,
                crate::types::calculate_work(h.bits),
                h.version,
                h.merkle_root,
                h.time,
                h.bits,
                h.nonce
            ],
        )
        .unwrap();
}

fn published(c: &[BlockHeader]) -> ScriptedChain {
    let source = ScriptedChain::new();
    for h in c {
        source.publish(h);
    }
    let top = &c[c.len() - 1];
    source.tip(top.height, &top.hash);
    source
}

/// The active rows in `(from, to]` whose `previous_header_id` is not the row
/// they name by hash (a null link included), by height above `x`.
fn unlinked_by_id(db: &SqliteDb, from: u32, to: u32) -> Vec<u32> {
    let mut stmt = db
        .conn()
        .prepare(
            "SELECT c.height FROM headers c LEFT JOIN headers p ON p.header_id = c.previous_header_id \
             WHERE c.is_active = 1 AND c.height > ?1 AND c.height <= ?2 \
             AND (p.hash IS NULL OR p.hash != c.previous_hash) ORDER BY c.height",
        )
        .unwrap();
    stmt.query_map([from, to], |r| r.get::<_, u32>(0))
        .unwrap()
        .map(|h| h.unwrap() - X)
        .collect()
}

/// What a consumer reads of the store against the chain `c`: the tip (above
/// `x`), the link check, and the heights (above `x`) that do not serve
/// `c`'s block. `None` when every height serves it, linked, at `c`'s tip.
async fn off_chain(db: &SqliteDb, c: &[BlockHeader]) -> Option<String> {
    let tip = find_chain_tip(db).await.unwrap().map(|t| t.height - X);
    let report = storage::link_check(db, X, None).await.unwrap();
    let mut not_served = Vec::new();
    for h in c {
        let served = find_header_for_height(db, h.height).await.unwrap();
        if served.map(|s| s.hash) != Some(h.hash.clone()) {
            not_served.push(h.height - X);
        }
    }
    let at_tip = tip == Some(c[c.len() - 1].height - X);
    (!(at_tip && report.linked && not_served.is_empty())).then(|| {
        format!(
            "tip {tip:?}, linked {}, broken {:?}, not served {not_served:?}",
            report.linked,
            report
                .broken
                .iter()
                .map(|b| b.height - X)
                .collect::<Vec<_>>()
        )
    })
}

// ─── The race (#32 ask 1) ───────────────────────────────────────────────────

/// The store of 18:48:28Z: `x ..= b8`, the tip `b8` (973788).
fn stored_branch() -> Vec<BlockHeader> {
    branch(&[root()], 0, 8, "#32 stored")
}

/// The network's chain at 18:48:44Z: a reorg from `b8`, `n8 ..= n11` on
/// `b7` (973788 to 973791 on 973787): the poll syncs +3 and the push of
/// `n11` walks three ancestors (`storedInactive (walked 3)`).
fn network() -> Vec<BlockHeader> {
    branch(&stored_branch(), 7, 4, "#32 network")
}

#[derive(Clone, Copy, Debug)]
enum Outer {
    Cron,
    Push,
}

/// The cron's pass on `db`; answers whether a writer held the store (the
/// pass ran and wrote nothing, so it left the planted pass stamp where it
/// stood).
async fn cron_pass(db: &SqliteDb, source: &ScriptedChain, hooks: &RecordedWebhooks) -> bool {
    db.conn()
        .execute(
            "UPDATE sync_state SET last_pass_at = 'planted' WHERE id = 1",
            [],
        )
        .unwrap();
    run_cron(db, params(), source, None, hooks).await.unwrap();
    let stamp: Option<String> = db
        .conn()
        .query_row(
            "SELECT last_pass_at FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    stamp.as_deref() == Some("planted")
}

/// One push of the network's tip through the door; answers whether a writer
/// held the store (`"busy"`: nothing was written; the object asks again).
async fn push_tip(db: &SqliteDb, source: &ScriptedChain, hooks: &RecordedWebhooks) -> bool {
    let tip = network().last().unwrap().clone();
    matches!(
        ingest_announced(db, params(), source, hooks, tip).await,
        Ok(a) if a.outcome == "busy"
    )
}

/// One run of the race: `outer` writes through a recorder, and the other
/// writer runs whole right after the outer's `n`th read answers (another
/// isolate between that read and what follows it). A writer turned away by
/// the one holding the store asks again once the holder is done, as the
/// object and the next tick do. `None` when the store serves the network's
/// chain afterwards, linked, at its tip.
async fn race(outer: Outer, n: Option<u32>) -> (Option<String>, u32) {
    let db = linked_store(&stored_branch()).await;
    let source = published(&network());
    let hooks = RecordedWebhooks::new("", "");
    let turned_away = Rc::new(Cell::new(false));
    let reads = {
        let rec = Recorder::new(&db);
        if let Some(n) = n {
            let (s, h, t) = (&source, &hooks, turned_away.clone());
            match outer {
                Outer::Cron => rec.after_read(n, move |db| async move {
                    t.set(push_tip(db, s, h).await);
                }),
                Outer::Push => rec.after_read(n, move |db| async move {
                    t.set(cron_pass(db, s, h).await);
                }),
            }
        }
        match outer {
            Outer::Cron => {
                run_cron(&rec, params(), &source, None, &hooks)
                    .await
                    .unwrap();
            }
            Outer::Push => {
                let tip = network().last().unwrap().clone();
                ingest_announced(&rec, params(), &source, &hooks, tip)
                    .await
                    .unwrap();
            }
        }
        rec.reads()
    };
    if turned_away.get() {
        match outer {
            Outer::Cron => assert!(!push_tip(&db, &source, &hooks).await),
            Outer::Push => assert!(!cron_pass(&db, &source, &hooks).await),
        }
    }
    (off_chain(&db, &network()).await, reads)
}

/// #32 ask 1: the reorg sync of 973789 to 973791 and the push of the same
/// headers, in both orders, the second writer landing after every read of
/// the first: the store serves the network's chain, linked, at its tip,
/// after both. Before the writer lease a push that read the tip before the
/// poll's pass landed went on to write on that read: its reorg walk took
/// the poll's rows above the fork off the active chain, and the store served
/// 973789 as its tip with the network at 973791, linked and behind, until a
/// poll that the live push no longer ran. To red: `push::ingest_announced`
/// and `sync::run_cron`, the lease taken by neither.
#[tokio::test]
async fn the_poll_and_the_push_of_one_reorg_leave_the_chain_served_in_either_order() {
    let mut failures = Vec::new();
    for outer in [Outer::Cron, Outer::Push] {
        let (alone, reads) = race(outer, None).await;
        assert_eq!(alone, None, "{outer:?} alone");
        assert!(reads > 10, "{outer:?} reads {reads}");
        for n in 1..=reads {
            if let (Some(state), _) = race(outer, Some(n)).await {
                failures.push(format!(
                    "{outer:?} outer, the other after read {n}: {state}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ─── The push-live mode (#32 ask 2) ─────────────────────────────────────────

const HB: u64 = HEARTBEAT_S * 1000;
const QUIET: u64 = POLL_QUIET_S * 1000;
const T0: u64 = 1_800_000_000_000;

/// A second of the object: the peer re-sends its tip (Arcade opens every
/// connection with its current tip), the door answers, the object applies
/// the answer as it does (`StreamState::ingested`).
async fn a_second_of_the_push(
    db: &SqliteDb,
    source: &ScriptedChain,
    peer_tip: &BlockHeader,
    st: &mut StreamState,
    now: u64,
) {
    let hooks = RecordedWebhooks::new("", "");
    st.last_beat_at = now;
    match ingest_announced(db, params(), source, &hooks, peer_tip.clone()).await {
        Ok(a) => {
            st.last_tip_height = a.tip_height;
            st.ingested(a.outcome, now);
        }
        Err(_) => st.refused += 1,
    }
}

/// One cron tick at `now`, as `sync::poll_for_new_blocks` and the object's
/// `/wake` run it: the behind count, the poll rule, then the poll or the
/// tick the push covers, both handed the ladder. Answers whether it polled.
async fn a_cron_tick(
    db: &SqliteDb,
    source: &ScriptedChain,
    st: &StreamState,
    poll: &mut PollState,
    now: u64,
) -> bool {
    let hooks = RecordedWebhooks::new("", "");
    poll.note_behind(store_behind(db).await);
    let due = poll_due(st, poll, now, HB, QUIET);
    if due.is_some() {
        poll.last_poll_at = now;
        run_cron(db, params(), source, None, &hooks).await.unwrap();
    } else {
        run_pushed_tick(db, params(), source, &hooks).await.unwrap();
    }
    due.is_some()
}

/// A live stream heard a minute ago, a poll two minutes ago.
fn live_stream() -> (StreamState, PollState) {
    (
        StreamState {
            reading: true,
            last_beat_at: T0,
            last_push_at: T0 - 60_000,
            ..Default::default()
        },
        PollState {
            last_poll_at: T0 - 120_000,
            ..Default::default()
        },
    )
}

/// #32 ask 2 and Read B's Q1: the end state of 2026-11-02 18:48Z (`c9`
/// linked, `c10` stored inactive by the push's walk, `c11` the tip, active,
/// its parent link null, nothing active at `c10`), the peer re-sending its
/// tip `c12` every second: one cron tick links the chain, though the push
/// is live and the tick does not poll. Before the fix the tick the push
/// covers recorded the link state and left the heal to a poll the live
/// push never let run (eleven hours on production). To red:
/// `sync::run_pushed_tick`, the link leg handed no source.
#[tokio::test]
async fn a_tick_under_a_live_push_heals_the_end_state_of_32() {
    let c = branch(&[root()], 0, 12, "#32 end");
    let db = linked_store(&c[..=9]).await;
    raw_row(&db, &c[10], false);
    raw_row(&db, &c[11], true);
    update_chain_tip_to_highest(&db).await.unwrap();
    assert!(!storage::link_check(&db, X, None).await.unwrap().linked);
    let source = published(&c);

    let (mut st, mut poll) = live_stream();
    for s in 0..60 {
        a_second_of_the_push(&db, &source, &c[12], &mut st, T0 + s * 1000).await;
    }
    let polled = a_cron_tick(&db, &source, &st, &mut poll, T0 + 60_000).await;
    assert!(
        !polled,
        "the push is live and heard: the tick does not poll"
    );
    assert_eq!(off_chain(&db, &c).await, None);
    assert_eq!(
        unlinked_by_id(&db, X, X + 12),
        Vec::<u32>::new(),
        "every healed row carries its parent's id"
    );
}

/// Read B's pump: a peer that re-sends a tip the store keeps inactive (a
/// stale peer on a branch the network left: `c12`, an equal-work rival of
/// the store's `b12`) gets `storedInactive` from the door on every send.
/// Before the fix the object counted that as a push heard, the quiet
/// interval never elapsed, and the poll never ran while the network moved
/// on (`c13`). Now only a push that advanced the active chain is heard: the
/// first tick polls and the store follows the network. To red:
/// `tip_stream::StreamState::ingested`, `last_push_at` moved on any write.
#[tokio::test]
async fn a_peer_resending_a_tip_the_store_keeps_inactive_never_silences_the_poll() {
    let stored = branch(&[root()], 0, 12, "pump stored");
    let net = branch(&stored, 10, 3, "pump network");
    let db = linked_store(&stored).await;
    let source = published(&net);
    let (mut st, mut poll) = live_stream();
    st.last_push_at = T0 - 30 * 60_000;
    poll.last_poll_at = T0 - 30 * 60_000;

    let mut first_poll = None;
    for m in 0..11u64 {
        for s in 0..60 {
            let now = T0 + m * 60_000 + s * 1000;
            a_second_of_the_push(&db, &source, &net[12], &mut st, now).await;
        }
        if a_cron_tick(&db, &source, &st, &mut poll, T0 + (m + 1) * 60_000).await {
            first_poll.get_or_insert(m + 1);
        }
    }
    assert!(st.stored >= 60, "the door stored the re-sent tip: {st:?}");
    assert_eq!(first_poll, Some(1), "the first tick polls");
    assert_eq!(off_chain(&db, &net).await, None);
}

/// #32 ask 2: the store's tip below the height the push named
/// (`lastSeenHeight > heightLive`) for more than one tick polls, whatever
/// the push did meanwhile. `sync::store_behind` is the read.
#[tokio::test]
async fn the_store_behind_the_seen_tip_reads_behind() {
    let c = branch(&[root()], 0, 3, "behind");
    let db = linked_store(&c).await;
    assert!(!store_behind(&db).await, "nothing seen yet");
    crate::sync::note_seen(&db, X + 3).await;
    assert!(!store_behind(&db).await, "seen at the tip");
    crate::sync::note_seen(&db, X + 5).await;
    assert!(store_behind(&db).await, "seen above the tip");
}
