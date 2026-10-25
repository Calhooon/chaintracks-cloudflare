//! 0.7.1 (#27, bsv-low #877): `lastPassAt` on `/getInfo`, stamped at the end
//! of every completed sync pass, idle included. At 0.7.0 an idle pass on a
//! current record wrote nothing a consumer could read (`lastSyncedAt` moves
//! only on a new tip's claim, audit M6; `lastSeenAt` only when the courier
//! record is behind, D5), so a quiet chain and a stalled tracker read the
//! same. The cron on the host: an idle pass moves `last_pass_at` and nothing
//! else in `sync_state`; a pass with work stamps it too; a pass no courier
//! answered, or one that errs, records its fault and leaves it.
use crate::host_harness::{
    block_hash, header, regtest, RecordedWebhooks, Recorder, ScriptedChain, SqliteDb,
};
use crate::statement_pins::MAIN_CHAIN_TIP;
use crate::storage::{get_info, insert_header};
use crate::sync::{notify_if_tip_advanced, run_cron, run_pushed_tick};
use crate::types::{BlockHeader, Chain};

const HOOK: &str =
    "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed";
const BEARER: &str = "tip-webhook-bearer";
const H: u32 = 965_877;
/// A stamp no pass on the host can write (a second's resolution would hide a
/// stamp written within the same second as the plant).
const PLANTED: &str = "2000-01-01 00:00:00";

/// X, Y, A stored and announced: a current store on a quiet chain at H.
async fn quiet_store(db: &SqliteDb, hooks: &RecordedWebhooks) -> BlockHeader {
    let x = header(H - 2, "x", &block_hash("the block below x"));
    let y = header(H - 1, "y", &x.hash);
    let a = header(H, "a", &y.hash);
    for h in [&x, &y, &a] {
        let r = insert_header(db, regtest(), h).await.unwrap();
        assert!(r.added && r.is_active_tip, "{r:?}");
    }
    notify_if_tip_advanced(db, hooks).await.unwrap();
    hooks.take();
    a
}

fn plant(db: &SqliteDb) {
    db.conn()
        .execute(
            "UPDATE sync_state SET last_pass_at = ?1 WHERE id = 1",
            [PLANTED],
        )
        .unwrap();
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

/// Every column of the `sync_state` row but `last_pass_at`, by name, read raw.
fn sync_state_but_the_stamp(db: &SqliteDb) -> Vec<(String, rusqlite::types::Value)> {
    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT * FROM sync_state WHERE id = 1")
        .unwrap();
    let names: Vec<String> = stmt.column_names().iter().map(|n| n.to_string()).collect();
    assert!(names.iter().any(|n| n == "last_pass_at"), "{names:?}");
    stmt.query_row([], |r| {
        Ok(names
            .iter()
            .enumerate()
            .filter(|(_, n)| *n != "last_pass_at")
            .map(|(i, n)| (n.clone(), r.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .unwrap()
}

/// Migration 0013 adds the one column and nothing else (the shape of 0012).
#[test]
fn migration_0013_adds_the_one_column() {
    let sql = include_str!("../migrations/0013_last_pass_at.sql");
    let statements: Vec<&str> = sql
        .lines()
        .filter(|l| !l.trim_start().starts_with("--") && !l.trim().is_empty())
        .collect();
    assert_eq!(
        statements,
        vec!["ALTER TABLE sync_state ADD COLUMN last_pass_at TEXT;"]
    );
}

/// The quiet chain: an idle pass on a current record moves `last_pass_at`
/// and nothing else in `sync_state` (`updated_at`, M6; the courier record,
/// D5; the announce row and counters; the validation cursor). To red:
/// `run_cron`, the stamp dropped (it stays planted) or a second write on the
/// idle path (a column moves).
#[tokio::test]
async fn an_idle_pass_on_a_current_record_moves_the_stamp_and_nothing_else() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    let chain = ScriptedChain::new();
    let a = quiet_store(&db, &hooks).await;
    chain.tip(H, &a.hash);
    // the first idle pass seeds the courier record (D5); the pin is on the
    // steady state after it
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    plant(&db);
    let before = sync_state_but_the_stamp(&db);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    let stamp = last_pass_at(&db).expect("the idle pass stamps");
    assert_ne!(stamp, PLANTED, "the idle pass moved the stamp");
    assert_eq!(
        sync_state_but_the_stamp(&db),
        before,
        "nothing else in sync_state moved"
    );
    assert!(hooks.take().is_empty());
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.last_pass_at.as_deref(), Some(stamp.as_str()));
}

/// A pass with work stamps too, after the tip it landed.
#[tokio::test]
async fn a_pass_with_work_stamps_the_pass() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    let chain = ScriptedChain::new();
    let a = quiet_store(&db, &hooks).await;
    let b = header(H + 1, "b", &a.hash);
    chain.publish(&b);
    chain.tip(H + 1, &b.hash);
    plant(&db);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.height_live, H + 1, "the pass landed b");
    let stamp = info.last_pass_at.expect("the pass with work stamps");
    assert_ne!(stamp, PLANTED);
}

/// A tick the push covers (E5) is a completed pass: it stamps.
#[tokio::test]
async fn a_tick_the_push_covers_stamps_the_pass() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    quiet_store(&db, &hooks).await;
    plant(&db);
    run_pushed_tick(&db, regtest(), &hooks).await.unwrap();
    assert_ne!(last_pass_at(&db).as_deref(), Some(PLANTED));
}

/// A pass no courier answered records its fault (`lastSyncError`) and leaves
/// the stamp where it stood, so a stalled tracker's `lastPassAt` ages. To
/// red: `run_cron_inner`, the no-answer branch returning `Pass::Completed`.
#[tokio::test]
async fn a_pass_no_courier_answered_records_the_fault_and_leaves_the_stamp() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    let chain = ScriptedChain::new();
    let a = quiet_store(&db, &hooks).await;
    chain.tip(H, &a.hash);
    chain.set_unavailable(true);
    plant(&db);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .expect("a courier fault never aborts the cron");
    assert_eq!(last_pass_at(&db).as_deref(), Some(PLANTED), "no stamp");
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.last_pass_at.as_deref(), Some(PLANTED));
    let err = info.last_sync_error.expect("the fault is recorded");
    assert!(err.contains("chain info"), "{err}");
    assert!(info.last_sync_error_at.is_some());
}

/// A pass that errs (a D1 fault on the tip read) does not stamp.
#[tokio::test]
async fn a_pass_that_errs_leaves_the_stamp() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    let chain = ScriptedChain::new();
    let a = quiet_store(&db, &hooks).await;
    chain.tip(H, &a.hash);
    plant(&db);
    let rec = Recorder::new(&db);
    rec.fault_first_of(MAIN_CHAIN_TIP);
    run_cron(&rec, regtest(), &chain, None, &hooks)
        .await
        .expect_err("the tip read faulted");
    assert_eq!(last_pass_at(&db).as_deref(), Some(PLANTED), "no stamp");
}

/// The `/getInfo` body carries `lastPassAt` beside `lastSyncedAt` once a pass
/// ran, and omits it before the first (as `lastSeenAt`).
#[tokio::test]
async fn get_info_carries_last_pass_at() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    let chain = ScriptedChain::new();
    let a = quiet_store(&db, &hooks).await;
    let info = serde_json::to_value(get_info(&db, &Chain::Main).await.unwrap()).unwrap();
    assert!(info.get("lastPassAt").is_none(), "{info}");
    chain.tip(H, &a.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    let info = serde_json::to_value(get_info(&db, &Chain::Main).await.unwrap()).unwrap();
    assert!(info["lastPassAt"].is_string(), "{info}");
    assert!(info["lastSyncedAt"].is_string(), "{info}");
    assert!(info.get("syncSchemaFault").is_none(), "{info}");
}

/// A deploy before migration 0013: the cron still completes (the stamp is
/// loud and best-effort), every other field is served, and `/getInfo` names
/// the missing column in `syncSchemaFault`.
#[tokio::test]
async fn before_migration_0013_the_cron_syncs_and_get_info_names_the_column() {
    let db = SqliteDb::migrated_through("0012");
    let hooks = RecordedWebhooks::new(HOOK, BEARER);
    let chain = ScriptedChain::new();
    let a = quiet_store(&db, &hooks).await;
    let b = header(H + 1, "b", &a.hash);
    chain.publish(&b);
    chain.tip(H + 1, &b.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .expect("the missing column never stops the sync");
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.height_live, H + 1);
    assert_eq!(info.last_pass_at, None);
    assert!(info.last_seen_at.is_some(), "the other fields are served");
    let fault = info.sync_schema_fault.unwrap_or_default();
    assert!(fault.contains("last_pass_at"), "{fault}");
}
