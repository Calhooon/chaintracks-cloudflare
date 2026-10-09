//! a private program loop 10 D5 (2026-09-08): the same-height competition at 965877
//! (loop 9) left chaintracks' header store 23 min behind the network while
//! nothing recorded why (no observability, and a poll fault aborted the cron
//! on `?` before its tail). The cron on the host, RED-first: a courier fault
//! is RECORDED and never aborts the tail; the courier ladder takes a header
//! from the next rung; the lag and the last fault are served on `/getInfo`.
use crate::host_harness::{
    block_hash, flags, header, regtest, RecordedWebhooks, ScriptedChain, SqliteDb,
};
use crate::storage::{get_info, insert_header};
use crate::sync::{notify_if_tip_advanced, run_cron};
use crate::types::{BlockHeader, Chain};

const BETA_HOOK: &str =
    "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed";
const BEARER: &str = "tip-webhook-bearer";
/// The loop-9 height: `…2518` and `…21df` competed at 965877; 965878 built on `…21df`.
const H: u32 = 965_877;

struct Tie {
    x: BlockHeader,
    y: BlockHeader,
    /// The block we stored first at H.
    a: BlockHeader,
    /// Its same-height competitor, the one the next block built on.
    a2: BlockHeader,
    /// The block at H+1 on the competitor.
    b2: BlockHeader,
}

fn tie() -> Tie {
    let x = header(H - 2, "x", &block_hash("the block below x"));
    let y = header(H - 1, "y", &x.hash);
    let a = header(H, "a: seen first at 965877", &y.hash);
    let a2 = header(H, "a prime: the competitor at 965877", &y.hash);
    let b2 = header(H + 1, "b prime: 965878 on the competitor", &a2.hash);
    Tie { x, y, a, a2, b2 }
}

/// X, Y, A stored and announced: the store's steady state before the tie.
async fn seed(db: &SqliteDb, hooks: &RecordedWebhooks, t: &Tie) {
    for h in [&t.x, &t.y, &t.a] {
        let r = insert_header(db, regtest(), h).await.unwrap();
        assert!(r.added && r.is_active_tip, "{r:?}");
    }
    notify_if_tip_advanced(db, hooks).await.unwrap();
    hooks.take();
}

fn plain(height: u32, hash: &str) -> String {
    format!(r#"{{"height":{height},"hash":"{hash}"}}"#)
}

fn bodies(hooks: &RecordedWebhooks) -> Vec<String> {
    hooks.take().into_iter().map(|p| p.body).collect()
}

/// `(last_seen_height, last_error)`, read raw.
fn courier_health(db: &SqliteDb) -> (Option<i64>, Option<String>) {
    db.conn()
        .query_row(
            "SELECT last_seen_height, last_error FROM sync_state WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

/// Migration 0007 adds the four health columns and nothing else touches the
/// tip sync: a deploy before the migration still syncs (the writes are loud,
/// best-effort), and `/getInfo` names the missing column.
#[test]
fn migration_0007_adds_the_courier_health_columns() {
    let sql = include_str!("../migrations/0007_sync_state_courier_health.sql");
    for col in [
        "ADD COLUMN last_seen_height INTEGER",
        "ADD COLUMN last_seen_at TEXT",
        "ADD COLUMN last_error TEXT",
        "ADD COLUMN last_error_at TEXT",
    ] {
        assert!(sql.contains(col), "{col}");
    }
}

/// The loop-9 shape on one courier: the network moved to B' (H+1) on the
/// competitor A', the courier serves B' by height but refuses A' by hash. Today
/// the backfill's `?` aborts the cron before its tail and nothing is recorded;
/// fixed, the cron finishes, records the courier's height and the fault (naming
/// the parent it could not get), and the held tip stands.
#[tokio::test]
async fn a_refused_backfill_parent_is_recorded_and_the_cron_still_finishes() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let t = tie();
    seed(&db, &hooks, &t).await;
    chain.publish(&t.b2);
    chain.tip(H + 1, &t.b2.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .expect("a courier fault never aborts the cron");
    let (seen, err) = courier_health(&db);
    assert_eq!(
        seen,
        Some(i64::from(H + 1)),
        "the courier's height is recorded even when the ingest failed"
    );
    let err = err.expect("the fault is recorded");
    assert!(
        err.contains(&t.a2.hash),
        "the fault names the missing parent: {err}"
    );
    assert_eq!(flags(&db, &t.a.hash), (true, true), "the held tip stands");
    assert!(bodies(&hooks).is_empty(), "nothing new to announce");
}

/// `/getInfo` serves the lag (the courier's height minus the stored tip) and
/// the last fault with its time; once the parent is served the branch lands,
/// the lag reads 0 and the fault stays readable as history.
#[tokio::test]
async fn the_live_lag_and_the_last_fault_are_served_on_get_info() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let t = tie();
    seed(&db, &hooks, &t).await;
    chain.publish(&t.b2);
    chain.tip(H + 1, &t.b2.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(
        info.live_lag_blocks,
        Some(1),
        "one block behind the courier"
    );
    assert_eq!(info.last_seen_height, Some(H + 1));
    assert!(info.last_seen_at.is_some());
    assert!(
        info.last_sync_error
            .as_deref()
            .unwrap_or("")
            .contains(&t.a2.hash),
        "{:?}",
        info.last_sync_error
    );
    assert!(info.last_sync_error_at.is_some());
    assert_eq!(info.sync_schema_fault, None);
    // the courier now serves the parent: the branch lands, the tip flips, lag 0
    chain.publish(&t.a2);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.live_lag_blocks, Some(0));
    assert_eq!(info.height_live, H + 1);
    assert_eq!(flags(&db, &t.b2.hash), (true, true));
    assert_eq!(flags(&db, &t.a.hash), (false, false));
    assert!(
        info.last_sync_error.is_some(),
        "the last fault stays readable, with its time"
    );
}

/// Every courier down: the fault is recorded (naming the refusal), no height is
/// seen, and the held tip is still announced (the round-4 rule kept).
#[tokio::test]
async fn every_courier_down_is_recorded_and_the_held_tip_is_still_announced() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let t = tie();
    for h in [&t.x, &t.y, &t.a] {
        insert_header(&db, regtest(), h).await.unwrap();
    }
    chain.set_unavailable(true);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    let (seen, err) = courier_health(&db);
    assert_eq!(seen, None, "nothing seen");
    assert!(
        err.as_deref().unwrap_or("").contains("unavailable"),
        "the refusal is recorded: {err:?}"
    );
    assert_eq!(
        bodies(&hooks),
        vec![plain(H, &t.a.hash)],
        "the held tip is announced"
    );
}

// ─── The ladder ─────────────────────────────────────────────────────────────

use crate::couriers::CourierLadder;

/// The loop-9 shape on TWO couriers: the first serves B' by height but refuses
/// the competitor parent A' by hash; the second holds A'. The ladder takes the
/// parent from the next rung, the branch lands, the reorg is announced, and no
/// poll fault is recorded (a rung's fault is the ladder's business, logged and
/// tallied). To red: `CourierLadder::header_by_hash`, return the first rung's
/// error instead of trying the next.
#[tokio::test]
async fn the_ladder_takes_the_parent_from_the_next_courier() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let t = tie();
    seed(&db, &hooks, &t).await;
    let woc = ScriptedChain::new();
    let arcade = ScriptedChain::new();
    for h in [&t.x, &t.y, &t.a, &t.b2] {
        woc.publish(h);
        arcade.publish(h);
    }
    arcade.publish(&t.a2);
    woc.tip(H + 1, &t.b2.hash);
    arcade.tip(H + 1, &t.b2.hash);
    let ladder = CourierLadder::new(vec![("woc", woc), ("arcade", arcade)], 0, regtest().clone());
    run_cron(&db, regtest(), &ladder, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        flags(&db, &t.b2.hash),
        (true, true),
        "the branch landed through the second rung"
    );
    assert_eq!(flags(&db, &t.a.hash), (false, false));
    assert_eq!(
        bodies(&hooks),
        vec![format!(
            r#"{{"height":{},"hash":"{}","reorgFrom":{H}}}"#,
            H + 1,
            t.b2.hash
        )]
    );
    let (seen, err) = courier_health(&db);
    assert_eq!(seen, Some(i64::from(H + 1)));
    assert_eq!(err, None, "the ladder succeeded: no poll fault");
    let faults = ladder.faults();
    assert_eq!(faults.len(), 1, "{faults:?}");
    assert!(faults[0].starts_with("woc: header by hash"), "{faults:?}");
    assert!(
        ladder.summary().contains("arcade ok"),
        "{}",
        ladder.summary()
    );
}

/// A lagging courier must never read as the chain: the first rung still
/// answers H while the second answers H+1; the ladder follows the highest
/// tip and asks that rung first. To red: `CourierLadder::chain_info`, keep
/// the first answer instead of the highest.
#[tokio::test]
async fn the_ladder_follows_the_highest_courier_tip() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let t = tie();
    seed(&db, &hooks, &t).await;
    let b = header(H + 1, "b: the next block on a", &t.a.hash);
    let woc = ScriptedChain::new();
    let arcade = ScriptedChain::new();
    for h in [&t.x, &t.y, &t.a] {
        woc.publish(h);
        arcade.publish(h);
    }
    arcade.publish(&b);
    woc.tip(H, &t.a.hash);
    arcade.tip(H + 1, &b.hash);
    let ladder = CourierLadder::new(vec![("woc", woc), ("arcade", arcade)], 0, regtest().clone());
    run_cron(&db, regtest(), &ladder, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        flags(&db, &b.hash),
        (true, true),
        "the highest courier's tip landed"
    );
    assert_eq!(bodies(&hooks), vec![plain(H + 1, &b.hash)]);
    assert!(
        ladder.summary().contains("arcade ok 2 faults 0 (tip)"),
        "{}",
        ladder.summary()
    );
    assert!(
        ladder.summary().contains("woc ok 1 faults 0"),
        "{}",
        ladder.summary()
    );
}

/// The preferred rung (it answered the highest tip) refuses every header:
/// after three faults in the tick it is skipped, the other rung serves the
/// rest, and the tally says so. To red: `RUNG_FAULT_CAP` raised past the
/// tick's asks (the first rung is asked for every header).
#[tokio::test]
async fn a_rung_faulting_three_times_in_a_tick_is_skipped_and_counted() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let t = tie();
    seed(&db, &hooks, &t).await;
    let mut chain: Vec<BlockHeader> = Vec::new();
    let mut parent = t.a.hash.clone();
    for k in 1..=5u32 {
        let h = header(H + k, &format!("block {k} above a"), &parent);
        parent = h.hash.clone();
        chain.push(h);
    }
    let woc = ScriptedChain::new(); // answers the tip, serves NO header
    let arcade = ScriptedChain::new();
    for h in [&t.x, &t.y, &t.a] {
        arcade.publish(h);
    }
    for h in &chain {
        arcade.publish(h);
    }
    woc.tip(H + 5, &chain[4].hash);
    arcade.tip(H + 5, &chain[4].hash);
    let ladder = CourierLadder::new(vec![("woc", woc), ("arcade", arcade)], 0, regtest().clone());
    run_cron(&db, regtest(), &ladder, None, &hooks)
        .await
        .unwrap();
    assert_eq!(flags(&db, &chain[4].hash), (true, true), "all five landed");
    let s = ladder.summary();
    assert!(s.contains("woc ok 1 faults 3 (skipped) (tip)"), "{s}");
    assert!(s.contains("arcade ok 6 faults 0"), "{s}");
    let (_, err) = courier_health(&db);
    assert_eq!(err, None, "the ladder served everything: no poll fault");
}

/// The proof-of-work bind, on real mainnet headers under the mainnet rules:
/// genesis and 965900 pass; 965900's hash against a harder `bits` (a smaller
/// target than its hash) fails, as does a zero target. To red:
/// `check_proof_of_work`, flip the compare.
#[test]
fn a_header_must_meet_its_own_target() {
    use crate::types::{compute_block_hash, BlockHeader};
    // 965900 (WoC + Bitails, probed 2026-09-08): hash 000000000000000008e2c789…, bits 0x18276a19
    let raw = hex::decode("0000072938e95d6a6237317b6a920cb75bb617ac25f43a9487fd2b140000000000000000bc2c282b23ad996ccf001d93f614aaea6738ed8cb38bb80741e7a9e013b314614d62a06a196a27188eac2e26").unwrap();
    let h = BlockHeader::from_bytes(&raw, 965_900).unwrap();
    assert_eq!(
        h.hash,
        "000000000000000008e2c789ff129d58d71db0f545fa6d713289e3518b780d66"
    );
    assert_eq!(h.bits, 0x18276a19);
    assert_eq!(compute_block_hash(&h.to_bytes()), h.hash);
    let main = crate::consensus::ChainParams::main();
    assert_eq!(h.check_pow(&main), Ok(()), "a real header meets its target");
    // target 0x076a19… below the hash 0x08e2c7…
    let hash = crate::consensus::from_hex(&h.hash).unwrap();
    assert_eq!(
        crate::consensus::check_proof_of_work(&hash, 0x18076a19, &main),
        Err(crate::consensus::HeaderFault::HighHash { bits: 0x18076a19 }),
        "a hash above its target is no proof of work"
    );
    let mut zero = h.clone();
    zero.bits = 0;
    assert_eq!(
        zero.check_pow(&main),
        Err(crate::consensus::HeaderFault::BitsZero { bits: 0 }),
        "a zero target never passes"
    );
    let genesis = BlockHeader {
        header_id: None,
        previous_header_id: None,
        version: 1,
        previous_hash: "0".repeat(64),
        merkle_root: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
        time: 1_231_006_505,
        bits: 0x1d00ffff,
        nonce: 2_083_236_893,
        height: 0,
        hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".to_string(),
        chain_work: String::new(),
        is_active: true,
        is_chain_tip: false,
    };
    assert_eq!(compute_block_hash(&genesis.to_bytes()), genesis.hash);
    assert_eq!(genesis.check_pow(&main), Ok(()));
}

/// A courier answering a header that fails its own proof of work is a faulting
/// rung, never a header: the ladder moves on. To red: `CourierLadder::bind`,
/// drop the `check_pow` check.
#[tokio::test]
async fn a_courier_serving_a_header_without_proof_of_work_is_a_faulting_rung() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let t = tie();
    seed(&db, &hooks, &t).await;
    let good = header(H + 1, "b: the next block on a", &t.a.hash);
    let mut fake = good.clone();
    fake.bits = 0x0100_0001; // a target of 1: nothing meets it
    fake.chain_work = crate::types::calculate_work(fake.bits);
    fake.hash = crate::types::compute_block_hash(&fake.to_bytes());
    let liar = ScriptedChain::new();
    let honest = ScriptedChain::new();
    for h in [&t.x, &t.y, &t.a] {
        liar.publish(h);
        honest.publish(h);
    }
    liar.publish(&fake);
    honest.publish(&good);
    liar.tip(H + 1, &fake.hash);
    honest.tip(H + 1, &good.hash);
    let ladder = CourierLadder::new(
        vec![("liar", liar), ("honest", honest)],
        0,
        regtest().clone(),
    );
    run_cron(&db, regtest(), &ladder, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        flags(&db, &good.hash),
        (true, true),
        "the honest header landed"
    );
    assert!(
        crate::storage::find_header_for_hash(&db, &fake.hash)
            .await
            .unwrap()
            .is_none(),
        "the fake never entered the store"
    );
    let faults = ladder.faults();
    assert!(
        faults
            .iter()
            .any(|f| f.contains("fails its own proof of work")),
        "{faults:?}"
    );
}

// ─── The idle cron keeps the record current ─────────────────────────────────

use crate::d1::Query;
use crate::host_harness::{Handed, Recorder};
use crate::statement_pins::{
    D5_COURIER_HEALTH, D5_RECORD_SEEN, MAIN_CHAIN_TIP, P04_VALIDATION_STATE, ROUND2_ANNOUNCED_TIP,
    ROUND3_READ_DELIVERIES,
};

/// The deploy of 2026-09-08 21:41Z landed on a chain idle at 965908 for 40
/// minutes: the health columns stayed NULL (only a cron with WORK recorded
/// the seen height), so the launch gate refused "lag unknown" on a healthy
/// store. An idle cron now reads the record once and writes the seen height
/// ONCE when the record is behind the couriers' answer (after the migration,
/// or after an ingest that bypassed the cron); when the record is current it
/// writes nothing (round 5 LOW-4 kept). To red: `run_cron`, the idle path's
/// `if seen != Some(woc_height)` guard dropped (a write every tick) or the
/// record dropped (the gate never passes on a quiet chain).
#[tokio::test]
async fn an_idle_cron_records_the_seen_height_once_when_the_record_is_behind() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let t = tie();
    seed(&db, &hooks, &t).await;
    chain.tip(H, &t.a.hash);
    // cron 1, idle, the record NULL: one seen record
    let before = rec.handed().len();
    run_cron(&rec, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        rec.handed()[before..].to_vec(),
        vec![
            Handed::Execute(Query::new(
                "UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1"
            )),
            Handed::Execute(Query::new(crate::events::SQL_TIP_AGE)),
            Handed::First(Query::new(MAIN_CHAIN_TIP)),
            Handed::First(Query::new(D5_COURIER_HEALTH)),
            Handed::Execute(Query::new(D5_RECORD_SEEN).bind(H)),
            // P0-4: the re-validation state, one read a tick (complete here)
            Handed::First(Query::new(P04_VALIDATION_STATE)),
            // #32: the legacy announce reads the verification ceiling.
            Handed::First(Query::new(P04_VALIDATION_STATE)),
            Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
            Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
        ],
        "the idle cron seeds the courier record and emits age"
    );
    let (seen, _) = courier_health(&db);
    assert_eq!(seen, Some(i64::from(H)));
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(
        info.live_lag_blocks,
        Some(0),
        "a quiet chain reads lag 0, never unknown"
    );
    // cron 2, idle, the record current: no write
    let before = rec.handed().len();
    run_cron(&rec, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        rec.handed()[before..].to_vec(),
        vec![
            Handed::Execute(Query::new(
                "UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1"
            )),
            Handed::Execute(Query::new(crate::events::SQL_TIP_AGE)),
            Handed::First(Query::new(MAIN_CHAIN_TIP)),
            Handed::First(Query::new(D5_COURIER_HEALTH)),
            Handed::First(Query::new(P04_VALIDATION_STATE)),
            // #32: the legacy announce reads the verification ceiling.
            Handed::First(Query::new(P04_VALIDATION_STATE)),
            Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
            Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
        ],
        "a current courier record: only age writes"
    );
    assert!(hooks.take().is_empty());
}
