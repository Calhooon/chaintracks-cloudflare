//! #34: the six follow-ups of the orphan-tip fix (PR #31) under the writer
//! lease, on the test store (a real SQLite carrying the real migrations,
//! every write through the Worker's own storage functions). Every writer
//! here writes through its own `Guard`, which checks at the storage seam
//! that each write of the `headers` table lands under that writer's lease
//! and inside its term.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::consensus::ChainParams;
use crate::host_harness::{
    block_hash, header, Guard, RecordedWebhooks, Recorder, ScriptedChain, SqliteDb,
};
use crate::push::ingest_announced;
use crate::storage::{
    self, find_chain_tip, find_header_for_height, insert_header, update_chain_tip_to_highest,
    SQL_INSERT_HEADER,
};
use crate::sync::{bulk_write, read_through, run_cron, run_pushed_tick, set_lease_term};
use crate::tip_stream::{
    poll_due, through_the_lease, PollState, StreamState, BUSY_TRIES, HEARTBEAT_S, POLL_QUIET_S,
};
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

/// Another writer holds the lease for two minutes.
fn held_elsewhere(db: &SqliteDb) {
    db.conn()
        .execute(
            "UPDATE sync_state SET lease_owner = 'push:elsewhere', \
             lease_until = CAST(strftime('%s', 'now') AS INTEGER) + 120 WHERE id = 1",
            [],
        )
        .unwrap();
}

fn give_back(db: &SqliteDb) {
    db.conn()
        .execute(
            "UPDATE sync_state SET lease_owner = NULL, lease_until = NULL WHERE id = 1",
            [],
        )
        .unwrap();
}

fn seen_height(db: &SqliteDb) -> Option<u32> {
    db.conn()
        .query_row(
            "SELECT last_seen_height FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// What a consumer reads of the store against the chain `c`: `None` when
/// the tip is `c`'s, the active chain is linked and every height serves
/// `c`'s block.
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
            "tip {tip:?}, linked {}, broken {} from {:?}, not served {}",
            report.linked,
            report.broken.len(),
            report.broken.first().map(|b| b.height - X),
            not_served.len()
        )
    })
}

// ─── Item 1: the read-through and /admin/bulk-sync take the lease ──────────

#[derive(Clone, Copy, Debug)]
enum Outer {
    Cron,
    ReadThrough,
}

/// The read-through as `routes::ensure_fresh_header` runs it: the tip read,
/// then `tip + 1 ..= height` through the ladder.
async fn read_through_to(db: &impl crate::d1::HeaderDb, source: &ScriptedChain, height: u32) {
    let hooks = RecordedWebhooks::new("", "");
    let tip = find_chain_tip(db).await.unwrap().unwrap().height;
    if height > tip {
        read_through(db, params(), source, &hooks, tip, height)
            .await
            .unwrap();
    }
}

async fn cron_pass(db: &impl crate::d1::HeaderDb, source: &ScriptedChain) -> bool {
    run_cron(db, params(), source, None, &RecordedWebhooks::new("", ""))
        .await
        .unwrap()
}

/// One run of the race on one height: the store at `c8`, the network at
/// `c10`, a request for `c10` (two above the tip, inside the grace). The
/// outer writer runs through a recorder; the other runs whole right after
/// the outer's `n`th read answers. Afterwards the cron runs once more (the
/// next tick). Answers what a consumer reads off the network's chain and
/// every write that landed outside its writer's lease.
async fn race(outer: Outer, n: Option<u32>) -> (Option<String>, Vec<String>, u32) {
    let net = branch(&[root()], 0, 10, "#34 race");
    let db = linked_store(&net[..=8]).await;
    let source = published(&net);
    let unleased = Rc::new(RefCell::new(Vec::new()));
    let reads = {
        let rec = Recorder::new(&db);
        if let Some(n) = n {
            let (s, u) = (&source, unleased.clone());
            match outer {
                Outer::Cron => rec.after_read(n, move |db| async move {
                    let g = Guard::new(db, db, "read-through");
                    read_through_to(&g, s, X + 10).await;
                    u.borrow_mut().extend(g.unleased());
                }),
                Outer::ReadThrough => rec.after_read(n, move |db| async move {
                    let g = Guard::new(db, db, "cron");
                    cron_pass(&g, s).await;
                    u.borrow_mut().extend(g.unleased());
                }),
            }
        }
        let g = Guard::new(&rec, &db, "outer");
        match outer {
            Outer::Cron => {
                cron_pass(&g, &source).await;
            }
            Outer::ReadThrough => read_through_to(&g, &source, X + 10).await,
        }
        unleased.borrow_mut().extend(g.unleased());
        rec.reads()
    };
    let g = Guard::new(&db, &db, "next tick");
    cron_pass(&g, &source).await;
    unleased.borrow_mut().extend(g.unleased());
    let found = unleased.borrow().clone();
    (off_chain(&db, &net).await, found, reads)
}

/// Item 1: the public read-through (`routes::ensure_fresh_header` through
/// `sync::read_through`) and the cron's pass racing on one height, in both
/// orders, the second writer landing after every read of the first: the
/// store serves the network's chain afterwards and no write of the
/// `headers` table lands outside its writer's lease. Before #34 the
/// read-through wrote active rows with no lease at all. To red:
/// `sync::read_through`, the lease not taken.
#[tokio::test]
async fn the_read_through_and_the_pass_race_on_one_height_under_the_lease() {
    let mut failures = Vec::new();
    for outer in [Outer::Cron, Outer::ReadThrough] {
        let (alone, unleased, reads) = race(outer, None).await;
        if alone.is_some() || !unleased.is_empty() {
            failures.push(format!("{outer:?} alone: {alone:?} {unleased:?}"));
        }
        assert!(reads > 5, "{outer:?} reads {reads}");
        for n in 1..=reads {
            let (state, unleased, _) = race(outer, Some(n)).await;
            if state.is_some() || !unleased.is_empty() {
                failures.push(format!(
                    "{outer:?} outer, the other after read {n}: {state:?} {:?}",
                    unleased.first()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Item 1: `/admin/bulk-sync`'s write (`sync::bulk_write`) holds the lease
/// over every row it writes, and finding it held writes nothing and says
/// so (`None`: the route answers 503, run again). To red:
/// `sync::bulk_write`, the lease not taken.
#[tokio::test]
async fn the_bulk_sync_write_takes_the_lease() {
    let c = branch(&[root()], 0, 20, "#34 bulk");
    let db = linked_store(&c[..=2]).await;
    held_elsewhere(&db);
    let g = Guard::new(&db, &db, "bulk-sync");
    let answer = bulk_write(&g, params(), &c[3..]).await.unwrap();
    assert_eq!(answer, None, "the lease held elsewhere: nothing written");
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().height, X + 2);
    give_back(&db);
    let answer = bulk_write(&g, params(), &c[3..]).await.unwrap();
    assert_eq!(answer, Some(18));
    assert_eq!(g.unleased(), Vec::<String>::new());
    assert_eq!(off_chain(&db, &c).await, None);
}

// ─── Item 2: a pass refused Busy keeps the quiet timer ─────────────────────

const HB: u64 = HEARTBEAT_S * 1000;
const QUIET: u64 = POLL_QUIET_S * 1000;
const T0: u64 = 1_800_000_000_000;

/// One cron tick at `now` as `sync::poll_for_new_blocks` and the object's
/// `/wake` and `/polled` run it. Answers whether it asked to poll.
async fn a_cron_tick(
    db: &impl crate::d1::HeaderDb,
    source: &ScriptedChain,
    st: &StreamState,
    poll: &mut PollState,
    now: u64,
) -> Option<&'static str> {
    let hooks = RecordedWebhooks::new("", "");
    poll.note_behind(crate::sync::store_behind(db).await);
    let due = poll_due(st, poll, now, HB, QUIET);
    poll.woke(due, now);
    if due.is_some() {
        let ran = run_cron(db, params(), source, None, &hooks).await.unwrap();
        poll.polled(source.calls(), ran, now);
    } else {
        run_pushed_tick(db, params(), source, &hooks).await.unwrap();
    }
    due
}

/// Item 2: a tick whose poll the lease turned away (`sync::run_cron`
/// answered `false`: no courier asked) is no poll; the next tick reads the
/// poll due again. A poll that ran resets the quiet timer as before. To
/// red: `PollState::woke`, `last_poll_at` set at `/wake` (`tip_stream.rs`
/// at v0.7.2 line 281).
#[tokio::test]
async fn a_pass_the_lease_turned_away_keeps_the_quiet_timer() {
    let c = branch(&[root()], 0, 3, "#34 quiet");
    let db = linked_store(&c).await;
    let source = published(&c);
    let st = StreamState {
        reading: true,
        last_beat_at: T0 + 120_000,
        last_push_at: T0 - QUIET,
        ..Default::default()
    };
    let mut poll = PollState {
        last_poll_at: T0 - QUIET,
        ..Default::default()
    };
    held_elsewhere(&db);
    let due = a_cron_tick(&db, &source, &st, &mut poll, T0).await;
    assert_eq!(due, Some("the push was quiet for the interval"));
    give_back(&db);
    assert_eq!(
        poll_due(&st, &poll, T0 + 60_000, HB, QUIET),
        Some("the push was quiet for the interval"),
        "a refused pass is no poll: {poll:?}"
    );
    let due = a_cron_tick(&db, &source, &st, &mut poll, T0 + 60_000).await;
    assert!(due.is_some());
    assert_eq!(
        poll_due(&st, &poll, T0 + 120_000, HB, QUIET),
        None,
        "a pass that ran is a poll: {poll:?}"
    );
}

// ─── Item 3: a push still busy after its tries is logged ───────────────────

/// Item 3: a tip event the lease turns away on every one of its tries is
/// dropped with one line naming the header (kept as the object's
/// `last_fault`, `/status`), and `busy` counts the event once, not each
/// try; an event turned away and then stored counts once too. To red:
/// `tip_stream::through_the_lease`, the per-try count and no line.
#[tokio::test]
async fn a_push_still_busy_after_its_tries_is_logged_and_counted_once() {
    let c = branch(&[root()], 0, 4, "#34 busy");
    let db = linked_store(&c[..=2]).await;
    let source = published(&c);
    let hooks = RecordedWebhooks::new("", "");
    held_elsewhere(&db);
    let mut st = StreamState::default();
    let waits = Cell::new(0);
    let answer = through_the_lease(
        &mut st,
        &c[3],
        BUSY_TRIES,
        || ingest_announced(&db, params(), &source, &hooks, c[3].clone()),
        || {
            waits.set(waits.get() + 1);
            async {}
        },
        || T0,
    )
    .await
    .unwrap();
    assert_eq!(answer.outcome, "busy");
    assert_eq!(waits.get(), BUSY_TRIES);
    assert_eq!(st.busy, 1, "one event, not {} tries", BUSY_TRIES + 1);
    assert_eq!(
        st.last_fault.as_deref(),
        Some(
            format!(
                "tip stream: DROPPED the push of {} at {}: the writer lease stayed held through 16 asks 2000 ms apart; the poll covers it",
                c[3].hash, c[3].height
            )
            .as_str()
        )
    );

    // Turned away once, then stored: one busy event, no drop.
    let mut st = StreamState::default();
    let answer = through_the_lease(
        &mut st,
        &c[3],
        BUSY_TRIES,
        || ingest_announced(&db, params(), &source, &hooks, c[3].clone()),
        || {
            give_back(&db);
            async {}
        },
        || T0,
    )
    .await
    .unwrap();
    assert_eq!(answer.outcome, "active");
    assert_eq!((st.busy, st.last_fault.as_deref()), (1, None));
}

// ─── Item 4: refused pushes raise the seen height ──────────────────────────

/// Item 4, #32's log order (2026-11-02 18:48Z): the store at `b8`
/// (973788); the cron's poll syncs the reorg `n8 ..= n11` (+3, 973791);
/// the push of `n13` and `n14` is refused (its ancestry not to be had:
/// 18:48:50.8Z and 18:48:51.8Z); then the ticks behind a live push. The
/// first refused push raises the seen height to its own, so the behind rule
/// counts from the next tick and the second tick polls, and the store
/// follows the network. Before #34 a refusal left the seen height at the
/// poll's 973791 and no tick polled before the quiet interval. To red:
/// `push::ingest_announced`, the seen height noted after a write alone.
#[tokio::test]
async fn a_refused_push_raises_the_seen_height_and_the_behind_rule_fires() {
    let stored = branch(&[root()], 0, 8, "#32 stored");
    let net = branch(&stored, 7, 7, "#32 network");
    let db = linked_store(&stored).await;
    let source = published(&net[..=11]);
    let hooks = RecordedWebhooks::new("", "");
    assert!(cron_pass(&db, &source).await, "18:48:44 the poll");
    assert_eq!(
        find_chain_tip(&db).await.unwrap().unwrap().hash,
        net[11].hash
    );
    assert_eq!(seen_height(&db), Some(X + 11));

    // 18:48:50.8 and 18:48:51.8: the pushes above a parent no courier serves.
    for (i, h) in [&net[13], &net[14]].into_iter().enumerate() {
        let refused = ingest_announced(&db, params(), &source, &hooks, h.clone()).await;
        assert!(
            matches!(refused, Err(storage::IngestError::Refused(_))),
            "{refused:?}"
        );
        if i == 0 {
            assert_eq!(seen_height(&db), Some(X + 13), "the first refused push");
        }
    }
    // The couriers have the network's chain by the next tick.
    for h in &net[12..] {
        source.publish(h);
    }
    source.tip(net[14].height, &net[14].hash);

    let st = StreamState {
        reading: true,
        last_beat_at: T0 + 120_000,
        last_push_at: T0 - 10_000,
        ..Default::default()
    };
    let mut poll = PollState {
        last_poll_at: T0 - 3_000,
        ..Default::default()
    };
    let mut log = Vec::new();
    for tick in 1..=3u64 {
        let due = a_cron_tick(&db, &source, &st, &mut poll, T0 + tick * 50_000).await;
        log.push((tick, due));
    }
    assert_eq!(
        log,
        vec![
            (1, None),
            (
                2,
                Some("the store sat behind the seen tip for more than one tick")
            ),
            (3, None),
        ]
    );
    assert_eq!(off_chain(&db, &net).await, None);
}

// ─── Item 5: a heal stays inside the lease's term ──────────────────────────

/// Item 5: a broken chain over 300 heights under the tip (`c12 ..= c310`
/// bulk rows, the link null, a height missing every 30 from `c11`: ten
/// breaks), a courier read taking 35 ms, a lease term of 2 s: one pass's
/// heal of 100 heights would read for 3.5 s and write past the term. Pass
/// after pass, the heal reads the term before each span and yields with its
/// progress recorded; the chain ends linked and served, and no write of the
/// `headers` table lands outside the lease or past its term. To red:
/// `sync::heal_links`, the term not read.
#[tokio::test]
async fn a_heal_over_300_broken_heights_stays_inside_a_2_s_term() {
    let c = branch(&[root()], 0, 311, "#34 heal");
    let db = linked_store(&c[..=10]).await;
    for h in &c[12..=310] {
        if (h.height - X) % 30 != 11 {
            raw_row(&db, h, true);
        }
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    let report = storage::link_check(&db, X, None).await.unwrap();
    assert_eq!(report.broken.len(), 10, "{report:?}");
    let source = published(&c);
    source.set_delay_ms(35);
    set_lease_term(Some(2));
    let mut unleased = Vec::new();
    let mut passes = 0;
    while off_chain(&db, &c).await.is_some() && passes < 60 {
        passes += 1;
        let g = Guard::new(&db, &db, "cron");
        cron_pass(&g, &source).await;
        unleased.extend(g.unleased());
        if !unleased.is_empty() {
            break;
        }
    }
    set_lease_term(None);
    assert_eq!(unleased, Vec::<String>::new(), "after {passes} passes");
    assert_eq!(off_chain(&db, &c).await, None, "after {passes} passes");
    assert!(passes > 3, "the heal spread over passes: {passes}");
}
