//! #30, the stall of 2026-11-02 18:48Z to 2026-11-03 06:03Z, on the test store
//! (a real SQLite carrying the real migrations, every write through the
//! Worker's own storage functions).
//!
//! What happened on production (the issue's reads): the tip row at 973791
//! stood over a height with no active row (973790, `active: null` on the link
//! check, a 404 on `/v2/header/height/973790`); every later tip was refused
//! at the context check (`ancestry-missing`), and once the gap passed 36 the
//! parent walk could never close it. The operator's `/admin/backfill` stored
//! what it fetched and left the rows the minute walks had stored inactive;
//! one `POST /admin/ingest` of the run under the tip activated them.
//!
//! The shapes here: a parent walk that aborts mid-way (the orphan row it
//! leaves, and the rows later stored on top of it), a bulk write over that
//! orphan, a hole under the tip deeper than the walk, and the link state a
//! consumer reads on `/getInfo`.

use crate::consensus::ChainParams;
use crate::host_harness::{
    block_hash, flags, header, header_id, RecordedWebhooks, ScriptedChain, SqliteDb,
};
use crate::storage::{
    self, find_chain_tip, find_header_for_height, insert_header, insert_headers_batch,
    update_chain_tip_to_highest, SQL_INSERT_HEADER,
};
use crate::sync::{insert_with_parent_backfill, run_cron};
use crate::types::{BlockHeader, Chain};

/// The anchor `x`: the one checkpoint of this file's rules.
const X: u32 = 965_769;

/// Regtest's rules with one anchor, `x` (the harness's `regtest()` carries
/// two more fixture roots inside the heights this file walks).
fn params() -> &'static ChainParams {
    static PARAMS: std::sync::OnceLock<ChainParams> = std::sync::OnceLock::new();
    PARAMS.get_or_init(|| {
        let x = chain(0)[0].clone();
        ChainParams::regtest()
            .with_checkpoints(&format!("{}:{}", x.height, x.hash))
            .unwrap()
    })
}

/// `x` and the `n` blocks above it: `chain(n)[i]` is at `X + i`.
fn chain(n: u32) -> Vec<BlockHeader> {
    let mut run = vec![header(X, "x", &block_hash("the block below x"))];
    for i in 1..=n {
        let parent = run[run.len() - 1].hash.clone();
        run.push(header(X + i, &format!("#30 c{i}"), &parent));
    }
    run
}

/// The store linked through `c[through]`, each row through the live insert.
async fn linked_store(c: &[BlockHeader], through: usize) -> SqliteDb {
    let db = SqliteDb::migrated();
    for h in &c[..=through] {
        let r = insert_header(&db, params(), h).await.unwrap();
        assert!(r.added && r.is_active_tip, "{r:?}");
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

fn publish(source: &ScriptedChain, c: &[BlockHeader]) {
    for h in c {
        source.publish(h);
    }
}

/// Every height `from ..= to` serves the chain's block (the `/v2/header/height`
/// read) and the active chain is linked over it.
async fn served_and_linked(db: &SqliteDb, c: &[BlockHeader], to: usize) {
    for h in &c[..=to] {
        let served = find_header_for_height(db, h.height).await.unwrap();
        assert_eq!(
            served.as_ref().map(|s| s.hash.as_str()),
            Some(h.hash.as_str()),
            "height {} serves the chain's block",
            h.height
        );
    }
    let report = storage::link_check(db, X, None).await.unwrap();
    assert!(report.linked, "{report:?}");
}

async fn cron(db: &SqliteDb, source: &ScriptedChain) {
    run_cron(db, params(), source, None, &RecordedWebhooks::new("", ""))
        .await
        .unwrap();
}

/// Fix 2: a parent walk that aborts leaves its orphan inactive (`c8`, its
/// parent `c7` not to be had), and a rung's next tip `c9` is stored on top of
/// it, inactive and linked to the orphan. The poll's next pass reads `c8` and
/// `c9` again as dupes; before #30 only a dupe with no parent link resumed
/// the walk, so `c8` was relinked and `c9`, linked to an orphan's work, stayed
/// inactive with every block above it: the tip never passed `c8`. To red:
/// `sync::insert_with_parent_backfill`, the resume keyed on
/// `previous_header_id` alone again.
#[tokio::test]
async fn an_aborted_parent_walk_never_stalls_the_poll() {
    let c = chain(12);
    let db = linked_store(&c, 5).await;
    let source = ScriptedChain::new();
    publish(&source, &c);

    source.set_unavailable(true);
    insert_with_parent_backfill(&db, params(), &source, c[8].clone())
        .await
        .expect_err("the walk to c7 aborted");
    source.set_unavailable(false);
    assert_eq!(flags(&db, &c[8].hash), (false, false), "the orphan c8");
    insert_with_parent_backfill(&db, params(), &source, c[9].clone())
        .await
        .unwrap();
    assert_eq!(flags(&db, &c[9].hash), (false, false), "c9 on the orphan");

    source.tip(X + 12, &c[12].hash);
    cron(&db, &source).await;
    let tip = find_chain_tip(&db).await.unwrap().unwrap();
    assert_eq!(tip.hash, c[12].hash, "the poll passed the orphan");
    served_and_linked(&db, &c, 12).await;
}

/// Fix 2, the production shape: the orphan an aborted walk left (`c7`) is
/// under a bulk write of the chain (the upstream catch-up, `/admin/backfill`,
/// a push: `insert_headers_batch`). Before #30 its `INSERT OR IGNORE` skipped
/// the stored orphan and wrote every row above it active, so the tip stood
/// over a height with no active row (973790 under 973791). To red:
/// `storage::insert_headers_batch`, the activation of the stored rows dropped.
#[tokio::test]
async fn a_bulk_write_over_an_aborted_walk_leaves_no_hole_under_the_tip() {
    let c = chain(20);
    let db = linked_store(&c, 5).await;
    let source = ScriptedChain::new();
    publish(&source, &c);
    source.set_unavailable(true);
    insert_with_parent_backfill(&db, params(), &source, c[7].clone())
        .await
        .expect_err("the walk to c6 aborted");
    assert_eq!(flags(&db, &c[7].hash), (false, false), "the orphan c7");

    insert_headers_batch(&db, params(), &c[6..=20])
        .await
        .unwrap();
    storage::dedupe_active_heights(&db).await.unwrap();
    update_chain_tip_to_highest(&db).await.unwrap();

    assert_eq!(flags(&db, &c[7].hash), (true, false), "c7 is served");
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, c[20].hash);
    served_and_linked(&db, &c, 20).await;
}

/// Fix 3: the rows the minute walks stored inactive (`c11 ..= c14`, linked
/// to each other, no row active at their heights) and the operator's backfill
/// of the span: before #30 the backfill's `INSERT OR IGNORE` left them
/// inactive under the active rows it wrote above them (the 32 broken heights
/// of 973793 to 973859). To red: `storage::insert_headers_batch`, the
/// activation of the stored rows dropped.
#[tokio::test]
async fn the_backfill_activates_the_rows_it_finds_inactive() {
    let c = chain(20);
    let db = linked_store(&c, 10).await;
    for h in &c[11..=14] {
        raw_row(&db, h, false);
    }
    insert_headers_batch(&db, params(), &c[11..=20])
        .await
        .unwrap();
    for h in &c[11..=14] {
        assert!(flags(&db, &h.hash).0, "{} active", h.height);
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    served_and_linked(&db, &c, 20).await;
}

/// The store of the stall: linked through `c10`, no row at `c11`, the rows
/// `c20 ..= c25` stored inactive, every other row of `c12 ..= c60` active,
/// the tip `c60`: the hole under the tip is 49 deep, past the 36 of the
/// parent walk.
async fn store_with_a_hole_under_the_tip(c: &[BlockHeader]) -> SqliteDb {
    let db = linked_store(c, 10).await;
    for h in &c[12..=60] {
        raw_row(&db, h, !(20..=25).contains(&(h.height - X)));
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, c[60].hash);
    assert!(find_header_for_height(&db, X + 11).await.unwrap().is_none());
    db
}

/// Fix 1: the poll recovers from a hole under the tip deeper than the parent
/// walk with no operator: it walks forward by height from the highest linked
/// active row through the ladder and activates what it fetched under the tip
/// (the activating path, `storage::ingest_pushed`). Before #30 the poll only
/// walked back from a new tip, 36 at most, and the hole stayed. To red:
/// `sync::run_cron_inner`, the link leg dropped.
#[tokio::test]
async fn the_poll_heals_a_hole_under_the_tip_deeper_than_the_walk() {
    let c = chain(61);
    let db = store_with_a_hole_under_the_tip(&c).await;
    let source = ScriptedChain::new();
    publish(&source, &c);
    source.tip(X + 61, &c[61].hash);
    cron(&db, &source).await;
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, c[61].hash);
    served_and_linked(&db, &c, 61).await;
}

/// #31 (Read B's residue): the run a heal hands the activating path lands
/// with every row's parent link set, the child above it included. The fast
/// path of `canonicalize_pushed` (every pushed row the sole active row at
/// its height) set no link, so the healed rows (bulk rows: the link null)
/// and the child kept `previous_header_id` null: linked by hash for the
/// link check, unlinked for every walk by id. To red:
/// `storage::canonicalize_pushed`, the fast path's `SQL_LINK_SPAN` dropped.
#[tokio::test]
async fn the_heal_links_every_row_it_heals_by_id() {
    let c = chain(61);
    let db = store_with_a_hole_under_the_tip(&c).await;
    let source = ScriptedChain::new();
    publish(&source, &c);
    source.tip(X + 61, &c[61].hash);
    cron(&db, &source).await;
    served_and_linked(&db, &c, 61).await;
    for h in &c[1..=61] {
        let row = storage::find_header_for_hash(&db, &h.hash)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.previous_header_id,
            Some(header_id(&db, &h.previous_hash)),
            "the row at {} names its parent's id",
            h.height
        );
    }
}

/// The active rows at `height`, read raw.
fn active_at(db: &SqliteDb, height: u32) -> i64 {
    db.conn()
        .query_row(
            "SELECT COUNT(*) FROM headers WHERE height = ?1 AND is_active = 1",
            [height],
            |r| r.get(0),
        )
        .unwrap()
}

/// #31 (Read A's plant 1): a row the bulk writer finds stored inactive at a
/// height where a rival is already active stays inactive: one height, one
/// active row. The shape is the one production logged at 18:48:50Z (an
/// active row at 973792 that does not name the tip 973791 as its parent)
/// under a push of the chain's own 973792, stored inactive. To red:
/// `storage::SQL_ACTIVATE_STORED`, the `NOT EXISTS` guard dropped (two
/// active rows at the height, and the push answers `storedInactive` over
/// them).
#[tokio::test]
async fn a_row_stored_inactive_under_an_active_rival_stays_the_one_inactive_row() {
    let c = chain(12);
    let db = linked_store(&c, 11).await;
    let rival = header(
        X + 12,
        "#31 rival",
        &block_hash("a parent the store never held"),
    );
    raw_row(&db, &rival, true);
    raw_row(&db, &c[12], false);
    let source = ScriptedChain::new();
    publish(&source, &c);
    let pushed = crate::push::ingest_announced(
        &db,
        params(),
        &source,
        &RecordedWebhooks::new("", ""),
        c[12].clone(),
    )
    .await
    .unwrap();
    assert_eq!(pushed.outcome, "storedInactive", "{pushed:?}");
    assert_eq!(active_at(&db, X + 12), 1, "one active row at the height");
    assert_eq!(flags(&db, &rival.hash), (true, false));
    assert_eq!(flags(&db, &c[12].hash), (false, false));
}

/// #31 (Read A's plant 2): the bulk writer activates a row it finds stored
/// inactive at the LAST height of its run (the read of the stored rows
/// covers the run's top height). To red: `storage::SQL_INACTIVE_HASHES_BETWEEN`,
/// `height <= ?2` read as `height < ?2`.
#[tokio::test]
async fn the_bulk_writer_activates_a_row_stored_inactive_at_the_top_of_its_run() {
    let c = chain(14);
    let db = linked_store(&c, 10).await;
    for h in &c[11..=14] {
        raw_row(&db, h, false);
    }
    insert_headers_batch(&db, params(), &c[11..=14])
        .await
        .unwrap();
    for h in &c[11..=14] {
        assert!(flags(&db, &h.hash).0, "{} active", h.height);
    }
    update_chain_tip_to_highest(&db).await.unwrap();
    served_and_linked(&db, &c, 14).await;
}

/// `/getInfo`'s `linkState` as a consumer reads it.
async fn link_state(db: &SqliteDb) -> serde_json::Value {
    let info = storage::get_info(db, &Chain::Main).await.unwrap();
    serde_json::to_value(info).unwrap()["linkState"].clone()
}

/// Fix 4: every pass (idle too) checks the links of the top of the active
/// chain and `/getInfo` carries the answer, so a broken served chain is a
/// read and not a stall a consumer finds. A hole no courier can fill reads
/// `linked: false` with its height; once a courier serves it the idle pass
/// heals it and the read is `linked: true`. To red: `run_cron_inner`, the
/// link leg dropped (no `linkState`).
#[tokio::test]
async fn get_info_carries_the_link_state_of_every_pass() {
    let c = chain(61);
    let db = store_with_a_hole_under_the_tip(&c).await;
    let source = ScriptedChain::new();
    publish(&source, &c[..=10]);
    publish(&source, &c[12..=60]);
    source.tip(X + 60, &c[60].hash);

    cron(&db, &source).await;
    let state = link_state(&db).await;
    assert_eq!(state["linked"], false, "{state}");
    assert_eq!(state["to"], X + 60, "{state}");
    assert_eq!(state["broken"][0], X + 11, "{state}");
    let info = storage::get_info(&db, &Chain::Main).await.unwrap();
    let error = info.last_sync_error.expect("the heal's fault is recorded");
    assert!(error.contains(&format!("{}", X + 11)), "{error}");

    publish(&source, &c[11..=11]);
    cron(&db, &source).await;
    let state = link_state(&db).await;
    assert_eq!(state["linked"], true, "{state}");
    assert_eq!(state["broken"], serde_json::json!([]), "{state}");
    served_and_linked(&db, &c, 60).await;
}
