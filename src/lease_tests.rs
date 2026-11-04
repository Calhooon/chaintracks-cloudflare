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
    block_hash, header, FakeClock, Guard, RecordedWebhooks, Recorder, ScriptedChain, SqliteDb,
};
use crate::push::{hand_over, ingest_announced, HAND_OVER_BUDGET_MS, SEND_TIMEOUT_MS};
use crate::storage::{
    self, find_chain_tip, find_header_for_height, insert_header, update_chain_tip_to_highest,
    SQL_INSERT_HEADER,
};
use crate::sync::{
    admin_ingest_write, backfill_write, bulk_write, read_through, run_cron, run_pushed_tick,
    set_lease_term, ChainSource, TipWebhooks, WebhookDelivery, WebhookTarget,
};
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
    raw_row_worked(db, h, active, &crate::types::calculate_work(h.bits));
}

/// `raw_row` carrying `work` as its chain work.
fn raw_row_worked(db: &SqliteDb, h: &BlockHeader, active: bool, work: &str) {
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
                work,
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
/// `tip + 1 ..= height` through the ladder and judged, then (#34,
/// lane/ct-34-b) the run handed to the push's door as the request's
/// `waitUntil` hands it.
async fn read_through_to(db: &impl crate::d1::HeaderDb, source: &ScriptedChain, height: u32) {
    let hooks = RecordedWebhooks::new("", "");
    let tip = find_chain_tip(db).await.unwrap().unwrap().height;
    if height > tip {
        let fetched = read_through(db, params(), source, tip, height)
            .await
            .unwrap();
        if fetched.hand {
            hand_over(
                db,
                params(),
                source,
                &hooks,
                fetched.run,
                2,
                &FakeClock::at(T0),
            )
            .await;
        }
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
/// orders, the other writer landing after every read of the outer one: the
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
/// so (`Err` with the reason: the route answers 503, run again). To red:
/// `sync::bulk_write`, the lease not taken.
#[tokio::test]
async fn the_bulk_sync_write_takes_the_lease() {
    let c = branch(&[root()], 0, 20, "#34 bulk");
    let db = linked_store(&c[..=2]).await;
    held_elsewhere(&db);
    let g = Guard::new(&db, &db, "bulk-sync");
    let answer = bulk_write(&g, params(), &c[3..]).await.unwrap();
    assert!(
        answer
            .as_ref()
            .is_err_and(|why| why.contains("another writer holds the store")),
        "the lease held elsewhere: nothing written: {answer:?}"
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().height, X + 2);
    give_back(&db);
    let answer = bulk_write(&g, params(), &c[3..]).await.unwrap();
    assert_eq!(answer, Ok(18));
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
    poll.note_behind_or_hinted(crate::sync::store_behind(db).await, st);
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
        "tip stream",
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
        "tip stream",
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

// ─── Item 4: refused pushes and the behind rule ────────────────────────────
//
// #34 (lane/ct-34-b): the lane's item 4 witness (a refused push raises the
// seen height) is replaced by the round's
// `a_refused_push_is_a_hint_to_poll_never_the_present_height`: the same
// #32 order and the behind rule firing at the same tick, through a hint
// to poll instead of the seen height.

// ─── Item 5: a heal stays inside the lease's term ──────────────────────────

/// Item 5: a broken chain over 300 heights under the tip (`c12 ..= c310`
/// bulk rows, the link null, twelve heights missing every 30 from `c11`:
/// ten breaks, 120 heights; #34, lane/ct-34-b: the heal reads only the
/// missing heights now, so the holes are deeper than the lane's single
/// heights), a courier read taking 35 ms, a lease term of 2 s: one pass's
/// heal of 100 heights would read for 3.5 s and write past the term. Pass
/// after pass, the heal reads the term before each span and yields with its
/// progress recorded; the chain ends linked and served, and no write of the
/// `headers` table lands outside the lease or past its term. To red:
/// `sync::heal_links`, the term not read.
///
/// The pass count holds whatever the clock (#34 round 3, Read A2's F1 on
/// PR #36): the lease is stored in whole seconds, so a 2 s term leaves
/// 2001 to 3000 ms at its take. The heal goes on at a check only with at
/// least half the term and twice its longest step left (1000 + 2 * 350
/// ms: a step is ten 35 ms reads), and the check after `k` steps finds at
/// most 3000 - 350 * k ms left, so a pass reads at most 40 heights and the
/// 120 missing ones take 3 passes at least.
#[tokio::test]
async fn a_heal_over_300_broken_heights_stays_inside_a_2_s_term() {
    let c = branch(&[root()], 0, 311, "#34 heal");
    let db = linked_store(&c[..=10]).await;
    for h in &c[12..=310] {
        if !(11..=22).contains(&((h.height - X) % 30)) {
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
    assert!(passes >= 3, "the heal spread over passes: {passes}");
}

// ─── The fix round (lane/ct-34-b) on PR #36 ─────────────────────────────────

/// A courier ladder that answers `open` reads and then never again: the
/// request it serves is still in flight when the harness drops it.
struct Gate<'a> {
    inner: &'a ScriptedChain,
    open: Cell<u32>,
}

impl<'a> Gate<'a> {
    fn new(inner: &'a ScriptedChain, open: u32) -> Self {
        Self {
            inner,
            open: Cell::new(open),
        }
    }

    async fn pass(&self) {
        if self.open.get() == 0 {
            std::future::pending::<()>().await;
        }
        self.open.set(self.open.get() - 1);
    }
}

impl ChainSource for Gate<'_> {
    async fn chain_info(&self) -> worker::Result<crate::woc::WocChainInfo> {
        self.inner.chain_info().await
    }

    async fn header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        self.pass().await;
        self.inner.header_by_height(height).await
    }

    async fn header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        self.pass().await;
        self.inner.header_by_hash(hash).await
    }
}

/// The writer lease's holder while its term stands, `None` when it is free.
fn live_lease(db: &SqliteDb) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT lease_owner FROM sync_state WHERE id = 1 \
             AND lease_until >= CAST(strftime('%s', 'now') AS INTEGER)",
            [],
            |r| r.get(0),
        )
        .ok()
        .flatten()
}

/// The read-through's half that runs on the request's own life
/// (`routes::ensure_fresh_header`): the tip read, then the read-through.
async fn the_request(db: &SqliteDb, source: &impl ChainSource, height: u32) {
    let tip = find_chain_tip(db).await.unwrap().unwrap().height;
    let _ = read_through(db, params(), source, tip, height).await;
}

/// R1 (Read B on PR #36, `sync.rs:1315` with `:160` at 7b1c2d5): a public
/// read-through whose caller goes away mid-flight (LOW's 2 s deadline; the
/// harness drops the request at its third courier read) leaves the writer
/// lease free within one second, and the cron's next pass runs and lands
/// the chain. At 7b1c2d5 the read-through held the cron's 120 s lease
/// across its courier reads, and a request that ended there left it held:
/// every pass refused for two minutes.
#[tokio::test]
async fn a_read_through_whose_caller_goes_away_leaves_the_lease_free() {
    let net = branch(&[root()], 0, 6, "#34b gone");
    let db = linked_store(&net[..=2]).await;
    let source = published(&net);
    let gate = Gate::new(&source, 2);
    {
        use futures_util::FutureExt;
        let request = the_request(&db, &gate, X + 6);
        assert!(
            request.now_or_never().is_none(),
            "the request is in flight at its third courier read"
        );
    }
    let gone = std::time::Instant::now();
    while live_lease(&db).is_some() && gone.elapsed() < std::time::Duration::from_secs(1) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(
        live_lease(&db),
        None,
        "the lease is free within one second of the caller going away"
    );
    assert!(cron_pass(&db, &source).await, "the cron's next pass runs");
    assert_eq!(off_chain(&db, &net).await, None);
}

/// The lease's term lapses under its holder after the holder's `n`th read:
/// with `successor`, another writer has taken it since.
fn lapse(db: &SqliteDb, successor: bool) {
    let sql = if successor {
        "UPDATE sync_state SET lease_owner = 'cron:successor', \
         lease_until = CAST(strftime('%s', 'now') AS INTEGER) + 120 WHERE id = 1"
    } else {
        "UPDATE sync_state SET lease_until = CAST(strftime('%s', 'now') AS INTEGER) - 2 WHERE id = 1"
    };
    db.conn().execute(sql, []).unwrap();
}

/// The fence (Read B's (3) on PR #36): a cron pass whose term lapses after
/// any one of its reads (forced: the term passed, and in the second run a
/// successor took the lease) writes nothing to the `headers` table after
/// the lapse; the successor's pass then completes and the store serves the
/// network's chain. At 7b1c2d5 only the heal read the term, and a holder
/// past it kept writing beside its successor.
#[tokio::test]
async fn a_holder_past_its_term_writes_nothing_after_the_lapse() {
    let net = branch(&[root()], 0, 9, "#34b fence");
    let hooks = RecordedWebhooks::new("", "");
    let reads = {
        let db = linked_store(&net[..=2]).await;
        let rec = Recorder::new(&db);
        run_cron(&rec, params(), &published(&net), None, &hooks)
            .await
            .unwrap();
        rec.reads()
    };
    assert!(reads > 10, "reads {reads}");
    let mut failures = Vec::new();
    for successor in [false, true] {
        for n in 1..=reads {
            let db = linked_store(&net[..=2]).await;
            let source = published(&net);
            {
                let rec = Recorder::new(&db);
                rec.after_read(n, move |db| async move { lapse(db, successor) });
                let g = Guard::new(&rec, &db, "holder");
                let _ = run_cron(&g, params(), &source, None, &hooks).await;
                if let Some(w) = g.unleased().first() {
                    failures.push(format!("successor {successor}, lapsed after read {n}: {w}"));
                }
            }
            give_back(&db);
            let g = Guard::new(&db, &db, "successor");
            let ran = run_cron(&g, params(), &source, None, &hooks).await;
            let state = off_chain(&db, &net).await;
            if !matches!(ran, Ok(true)) || !g.unleased().is_empty() || state.is_some() {
                failures.push(format!(
                    "successor {successor}, lapsed after read {n}: the successor {ran:?} {state:?} {:?}",
                    g.unleased()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Read B's (1) on PR #36 (`routes.rs:783` at 7b1c2d5): `POST
/// /admin/ingest` runs the writer the heal runs (`storage::ingest_pushed`),
/// so it takes the lease as the pass does: held elsewhere, it writes
/// nothing and says so (`None`, the route's 503 with its reason); free, it
/// writes under its own lease.
#[tokio::test]
async fn the_admin_ingest_write_takes_the_lease() {
    let c = branch(&[root()], 0, 6, "#34b ingest");
    let db = linked_store(&c[..=2]).await;
    held_elsewhere(&db);
    let g = Guard::new(&db, &db, "admin-ingest");
    let answer = admin_ingest_write(&g, params(), &c[3..]).await;
    assert!(
        matches!(answer, Ok(None)),
        "the lease held elsewhere: refused, nothing written: {answer:?}"
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().height, X + 2);
    give_back(&db);
    let answer = admin_ingest_write(&g, params(), &c[3..])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(answer.outcome, "active");
    assert_eq!(g.unleased(), Vec::<String>::new());
    assert_eq!(off_chain(&db, &c).await, None);
}

/// Read B's (1) on PR #36 (`routes.rs:890` at 7b1c2d5): `/admin/backfill`'s
/// write (`insert_headers_batch`, which activates rows stored inactive)
/// takes the lease after its courier reads, as `/admin/bulk-sync` does:
/// held elsewhere, nothing written and the route's 503; free, under its own
/// lease. The gap `c3 ..= c6` is below the tip `c10` (bulk rows above it).
#[tokio::test]
async fn the_admin_backfill_write_takes_the_lease() {
    let c = branch(&[root()], 0, 10, "#34b backfill");
    let db = linked_store(&c[..=2]).await;
    for h in &c[7..] {
        raw_row(&db, h, true);
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    let served = |db: &SqliteDb| {
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM headers WHERE is_active = 1",
                [],
                |r| r.get::<_, u32>(0),
            )
            .unwrap()
    };
    assert_eq!(served(&db), 7);
    held_elsewhere(&db);
    let g = Guard::new(&db, &db, "admin-backfill");
    let answer = backfill_write(&g, params(), &c[3..=6]).await.unwrap();
    assert_eq!(answer, None, "the lease held elsewhere: nothing written");
    assert_eq!(served(&db), 7);
    give_back(&db);
    let answer = backfill_write(&g, params(), &c[3..=6]).await.unwrap();
    assert_eq!(answer, Some((4, Vec::new())));
    assert_eq!(g.unleased(), Vec::<String>::new());
    assert_eq!(off_chain(&db, &c).await, None);
}

/// The heal's passes over `db` until the store serves `c` (at most
/// `bound`), each through its own `Guard`: the passes, and every write that
/// landed outside its lease.
async fn heal_passes(
    db: &SqliteDb,
    c: &[BlockHeader],
    source: &ScriptedChain,
    bound: u32,
) -> (u32, Vec<String>) {
    let mut unleased = Vec::new();
    let mut passes = 0;
    while off_chain(db, c).await.is_some() && passes < bound {
        passes += 1;
        let g = Guard::new(db, db, "cron");
        let _ = run_cron(&g, params(), source, None, &RecordedWebhooks::new("", "")).await;
        unleased.extend(g.unleased());
    }
    (passes, unleased)
}

/// R2 (Read B on PR #36, `sync.rs:1008` at 7b1c2d5): rival rows active over
/// `c11 ..= c60` (a branch `r` whose lowest row names no stored block, so
/// two links break: at `c10` and at `c60`, which `c61` names), the
/// network's `c61 ..= c70` above them, a courier read taking 35 ms, a 2 s
/// term. The heal yields pass after pass; each yield keeps what it read,
/// so the next pass goes on from it, and the chain ends linked and served
/// within 20 passes with no write outside the lease. At 7b1c2d5 a yielded
/// run over the rival rows ended `storedInactive` and every pass read the
/// same heights again with no progress.
///
/// #34 round 3 (Read A2's F1 on PR #36: 12 of 30 runs failed alone): the
/// yield holds whatever the clock. The lease is stored in whole seconds,
/// so a 2 s term leaves 2001 to 3000 ms at its take, and a pass reads at
/// most 40 heights (`a_heal_over_300_broken_heights_stays_inside_a_2_s_term`
/// has the arithmetic). The band was 30 heights, which a term taken early
/// in its second read whole in one pass; it is 50 now, which no term can.
#[tokio::test]
async fn a_heal_over_rival_active_rows_moves_pass_after_pass() {
    let c = branch(&[root()], 0, 70, "#34b network");
    let db = linked_store(&c[..=10]).await;
    let mut rival = vec![header(
        X + 11,
        "#34b rival 11",
        &block_hash("nobody has it"),
    )];
    for i in 12..=60 {
        let parent = rival[rival.len() - 1].hash.clone();
        rival.push(header(X + i, &format!("#34b rival {i}"), &parent));
    }
    // The rival rows carry the work of a chain that forked at `c10`, as an
    // active branch does (their lowest row's parent is not stored, so no
    // repair could sum it).
    let mut work = storage::find_header_for_hash(&db, &c[10].hash)
        .await
        .unwrap()
        .unwrap()
        .chain_work;
    for h in rival.iter().chain(&c[61..]) {
        work = crate::types::add_work(&work, &crate::types::calculate_work(h.bits));
        raw_row_worked(&db, h, true, &work);
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    let report = storage::link_check(&db, X, None).await.unwrap();
    assert_eq!(
        report
            .broken
            .iter()
            .map(|b| b.height - X)
            .collect::<Vec<_>>(),
        vec![10, 60],
        "{report:?}"
    );
    let source = published(&c);
    source.set_delay_ms(35);
    set_lease_term(Some(2));
    let (passes, unleased) = heal_passes(&db, &c, &source, 20).await;
    set_lease_term(None);
    assert_eq!(unleased, Vec::<String>::new(), "after {passes} passes");
    assert_eq!(off_chain(&db, &c).await, None, "after {passes} passes");
    assert!(passes > 1, "the heal yielded and went on: {passes} passes");
}

/// #37 (inside R2's reach): a hole two heights deep under bulk rows
/// (`c11` and `c12` missing, `c13 ..= c20` active with no link) heals in
/// one pass. At 7b1c2d5 the heal took the missing `c11` as its floor, the
/// run it read stood on nothing (`PrevNotFound`), and every pass read up
/// to 100 heights through the ladder for no progress.
#[tokio::test]
async fn a_hole_two_heights_deep_heals_in_one_pass() {
    let c = branch(&[root()], 0, 20, "#37 hole");
    let db = linked_store(&c[..=10]).await;
    for h in &c[13..] {
        raw_row(&db, h, true);
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    let source = published(&c);
    let (passes, unleased) = heal_passes(&db, &c, &source, 3).await;
    assert_eq!(unleased, Vec::<String>::new());
    assert_eq!(off_chain(&db, &c).await, None, "after {passes} passes");
    assert_eq!(passes, 1);
}

/// A tip event through the door as the object's session hands it
/// (`tip_stream`'s `tip`, through `through_the_lease`), a second before
/// `T0`: turned away (`busy`) it is asked no more here.
async fn the_object_pushes(
    db: &SqliteDb,
    source: &ScriptedChain,
    st: &mut StreamState,
    h: &BlockHeader,
) -> Result<crate::push::Announced, storage::IngestError> {
    let hooks = RecordedWebhooks::new("", "");
    through_the_lease(
        st,
        h,
        0,
        "tip stream",
        || ingest_announced(db, params(), source, &hooks, h.clone()),
        || async {},
        || T0 - 1_000,
    )
    .await
}

/// Item 5 of the round (Read B's risk on PR #36): #32's log order
/// (2026-11-02 18:48Z). The store at `b8`; the cron's poll syncs the reorg
/// `n8 ..= n11` (+3); the push of `n13` and `n14` is refused, its ancestry
/// not to be had (18:48:50.8Z and 18:48:51.8Z). A refused push is a hint
/// to poll and never the store's height: the seen height and
/// `/getPresentHeight` stay at the poll's `n11`, and the behind rule still
/// fires at the second tick, so the store follows the network. At 7b1c2d5
/// the refusal raised the seen height (and `/getPresentHeight`) to its own
/// unproven height, up to 37 above the tip.
#[tokio::test]
async fn a_refused_push_is_a_hint_to_poll_never_the_present_height() {
    let stored = branch(&[root()], 0, 8, "#32 stored");
    let net = branch(&stored, 7, 7, "#32 network");
    let db = linked_store(&stored).await;
    let source = published(&net[..=11]);
    assert!(cron_pass(&db, &source).await, "18:48:44 the poll");
    assert_eq!(seen_height(&db), Some(X + 11));

    let mut st = StreamState {
        reading: true,
        last_beat_at: T0 + 120_000,
        last_push_at: T0 - 10_000,
        last_tip_height: X + 11,
        ..Default::default()
    };
    for h in [&net[13], &net[14]] {
        let refused = the_object_pushes(&db, &source, &mut st, h).await;
        assert!(
            matches!(refused, Err(storage::IngestError::Refused(_))),
            "{refused:?}"
        );
        assert_eq!(
            seen_height(&db),
            Some(X + 11),
            "a refused push, {}",
            h.height - X
        );
        assert_eq!(storage::present_height(&db).await.unwrap(), Some(X + 11));
    }
    for h in &net[12..] {
        source.publish(h);
    }
    source.tip(net[14].height, &net[14].hash);

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

// ─── Round 3 (lane/ct-34-c) on PR #36: #38 and the two lows ────────────────

const CONSUMER: &str = "https://consumer.example/internal/tip-changed";

/// A consumer of the tip's webhooks that notes, at each send, who holds the
/// writer lease (the seam: what the door holds while it sends); with
/// `hangs`, it never answers.
struct Consumer<'a> {
    db: &'a SqliteDb,
    inner: RecordedWebhooks,
    held_at_send: RefCell<Vec<Option<String>>>,
    hangs: bool,
}

impl<'a> Consumer<'a> {
    fn new(db: &'a SqliteDb, hangs: bool) -> Self {
        Self {
            db,
            inner: RecordedWebhooks::new(CONSUMER, "bearer"),
            held_at_send: RefCell::new(Vec::new()),
            hangs,
        }
    }
}

impl TipWebhooks for Consumer<'_> {
    fn targets(&self) -> Vec<WebhookTarget> {
        self.inner.targets()
    }

    fn token(&self) -> String {
        self.inner.token()
    }

    async fn post(&self, target: &WebhookTarget, token: &str, body: &str) -> WebhookDelivery {
        self.held_at_send.borrow_mut().push(live_lease(self.db));
        if self.hangs {
            std::future::pending::<()>().await;
        }
        self.inner.post(target, token, body).await
    }
}

fn last_error(db: &SqliteDb) -> Option<String> {
    db.conn()
        .query_row("SELECT last_error FROM sync_state WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap()
}

/// #38 (Read A2's F2 and Read B2's F1 on PR #36): the door gives the
/// writer lease back before it sends the tip's webhooks, through the
/// read-through's hand-off and through the tip stream's push alike: at
/// every send the lease is free. At eade8cb the sends ran inside the hold
/// (`ingest_held`), with no timeout, inside `waitUntil`'s 30 s.
#[tokio::test]
async fn the_door_gives_the_lease_back_before_the_tip_webhooks_are_sent() {
    let c = branch(&[root()], 0, 6, "#38 seam");
    let db = linked_store(&c[..=2]).await;
    let source = published(&c);
    let consumer = Consumer::new(&db, false);
    let landed = hand_over(
        &db,
        params(),
        &source,
        &consumer,
        c[3..=5].to_vec(),
        9,
        &FakeClock::at(T0),
    )
    .await;
    assert_eq!(landed, 3);
    let answer = ingest_announced(&db, params(), &source, &consumer, c[6].clone())
        .await
        .unwrap();
    assert_eq!(answer.outcome, "active");
    assert_eq!(*consumer.held_at_send.borrow(), vec![None; 4]);
    assert_eq!(consumer.inner.take().len(), 4);
    assert_eq!(off_chain(&db, &c).await, None);
}

/// #38: a consumer that never answers is cut at `SEND_TIMEOUT_MS` a send,
/// and the hand-off's one budget holds the whole run under a fake clock:
/// six headers, each landing and its send cut at 5 s, so the fifth finds
/// the 20 s spent and the run stops there, recording the two heights left
/// (`lastSyncError`); the lease is free at every send and after; the
/// cron's next pass lands the rest. At eade8cb a send had no timeout, and
/// the run's asks and waits had no bound but the tries of each header.
#[tokio::test]
async fn a_hand_off_send_that_hangs_is_cut_and_the_run_keeps_its_budget() {
    let c = branch(&[root()], 0, 8, "#38 hangs");
    let db = linked_store(&c[..=2]).await;
    let source = published(&c);
    let consumer = Consumer::new(&db, true);
    let clock = FakeClock::at(T0);
    let landed = hand_over(
        &db,
        params(),
        &source,
        &consumer,
        c[3..].to_vec(),
        9,
        &clock,
    )
    .await;
    use crate::push::Clock;
    let spent = clock.now_ms() - T0;
    assert_eq!(landed as u64, HAND_OVER_BUDGET_MS / SEND_TIMEOUT_MS);
    assert!(spent <= HAND_OVER_BUDGET_MS, "{spent} ms");
    assert_eq!(*consumer.held_at_send.borrow(), vec![None; 4]);
    assert_eq!(live_lease(&db), None);
    let fault = last_error(&db).unwrap_or_default();
    assert!(
        fault.contains(&format!(
            "read-through: DROPPED the hand-off at {} ({}..={}, 2 header(s))",
            c[7].hash, c[7].height, c[8].height
        )),
        "{fault}"
    );
    assert!(cron_pass(&db, &source).await);
    assert_eq!(off_chain(&db, &c).await, None);
}

/// #38: a hand-off behind a lease held elsewhere stops inside its budget,
/// whatever its tries: no wait begun that would end past
/// `HAND_OVER_BUDGET_MS`, nothing written, the run recorded as left for
/// the poll.
#[tokio::test]
async fn a_hand_off_behind_a_held_lease_stops_inside_its_budget() {
    let c = branch(&[root()], 0, 8, "#38 held");
    let db = linked_store(&c[..=2]).await;
    held_elsewhere(&db);
    let source = published(&c);
    let consumer = Consumer::new(&db, false);
    let clock = FakeClock::at(T0);
    let landed = hand_over(
        &db,
        params(),
        &source,
        &consumer,
        c[3..].to_vec(),
        100,
        &clock,
    )
    .await;
    use crate::push::Clock;
    let spent = clock.now_ms() - T0;
    assert_eq!(landed, 0);
    assert!(spent <= HAND_OVER_BUDGET_MS, "{spent} ms");
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().height, X + 2);
    let fault = last_error(&db).unwrap_or_default();
    assert!(fault.contains("6 header(s)"), "{fault}");
}

/// A ladder that lets the lease lapse under its holder at its first read
/// by hash (the heal's).
struct Lapsing<'a> {
    inner: &'a ScriptedChain,
    db: &'a SqliteDb,
}

impl ChainSource for Lapsing<'_> {
    async fn chain_info(&self) -> worker::Result<crate::woc::WocChainInfo> {
        self.inner.chain_info().await
    }

    async fn header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        self.inner.header_by_height(height).await
    }

    async fn header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        lapse(self.db, false);
        self.inner.header_by_hash(hash).await
    }
}

fn last_pass_at(db: &SqliteDb) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT last_pass_at FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// Read A2's F3 on PR #36: a pass whose lease lapses mid-heal (the heal
/// goes on past its faults, so the pass reached its end) is no completed
/// pass: `lastPassAt` stays where it stood, the lapse is the fault
/// recorded, and the pass answers it. At eade8cb it was stamped as a
/// completed pass.
#[tokio::test]
async fn a_pass_whose_lease_lapses_mid_heal_is_no_completed_pass() {
    let c = branch(&[root()], 0, 20, "#34c lapse");
    let db = linked_store(&c[..=10]).await;
    for h in &c[13..] {
        raw_row(&db, h, true);
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    let source = published(&c);
    let lapsing = Lapsing {
        inner: &source,
        db: &db,
    };
    let ran = run_cron(
        &db,
        params(),
        &lapsing,
        None,
        &RecordedWebhooks::new("", ""),
    )
    .await;
    assert!(ran.is_err(), "{ran:?}");
    assert_eq!(last_pass_at(&db), None);
    let fault = last_error(&db).unwrap_or_default();
    assert!(fault.contains("the writer lease lapsed"), "{fault}");
    // The next pass heals and is stamped.
    give_back(&db);
    assert!(cron_pass(&db, &source).await);
    assert!(last_pass_at(&db).is_some());
    assert_eq!(off_chain(&db, &c).await, None);
}

/// Read A2's F4 on PR #36: `/admin/bulk-sync` whose lease lapses under its
/// write answers with the lapse as its reason (`Err`: the route's 503, as
/// for a held lease), not as a refused batch (422 at eade8cb); nothing is
/// written past the lapse.
#[tokio::test]
async fn a_bulk_sync_whose_lease_lapses_answers_the_lapse() {
    let c = branch(&[root()], 0, 20, "#34c bulk");
    let db = linked_store(&c[..=2]).await;
    let rec = Recorder::new(&db);
    rec.after_read(1, |db| async move { lapse(db, false) });
    let g = Guard::new(&rec, &db, "bulk-sync");
    let answer = bulk_write(&g, params(), &c[3..]).await.unwrap();
    assert!(
        answer
            .as_ref()
            .is_err_and(|why| why.contains("the writer lease lapsed")),
        "{answer:?}"
    );
    assert_eq!(g.unleased(), Vec::<String>::new());
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().height, X + 2);
}
