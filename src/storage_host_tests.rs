//! The unit tier of the host harness (a private program M19B-G2, 2026-09-08): one test
//! per `HeaderDb` method on the rusqlite implementation, then one per storage
//! and announce behavior the reorg producer stands on, each driven through the
//! REAL function (`insert_header`, `handle_reorg`, `find_chain_tip`,
//! `read_pending_reorg` / `clear_pending_reorg`, `notify_if_tip_advanced`) on
//! a real SQLite carrying the real migrations. Every test names, in its doc,
//! the one line that reds it.

use crate::d1::{HeaderDb, Query};
use crate::host_harness::{
    announce_counters, announced, block_hash, flags, header, header_at, header_id, heavy_root,
    min_difficulty_rule, pending, regtest, Handed, RecordedWebhooks, Recorder, SqliteDb,
    EQUAL_WORK_BITS, HEAVY_BITS,
};
use crate::storage::{
    find_chain_tip, find_header_for_height, get_info, handle_reorg, insert_header,
    sql_active_headers_between, sql_chain_tip, sql_header_for_hash, HeaderRow,
    RECORD_PENDING_REORG_SQL, SELECT_HEADER, SQL_ACTIVATE_HASH, SQL_CLEAR_CHAIN_TIP,
    SQL_COUNT_ACTIVE_ABOVE, SQL_DEACTIVATE_ABOVE, SQL_SET_CHAIN_TIP_ACTIVE,
};
use crate::sync::{
    clear_pending_reorg, finish_sync, notify_if_tip_advanced, read_pending_reorg,
    tip_is_unannounced, ANNOUNCED_TIP_SQL, CLEAR_PENDING_REORG_SQL, READ_PENDING_REORG_SQL,
    TIP_ANNOUNCE_SQL,
};
use crate::types::{add_work, calculate_work, BlockHeader, Chain};

const HOOK: &str =
    "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed";

/// The incident height.
const H: u32 = 965_771;

/// X at H-2 (the bootstrap row, its parent unknown to the store), Y at H-1.
fn x_and_y() -> (BlockHeader, BlockHeader) {
    let x = header(H - 2, "x", &block_hash("the block below x"));
    let y = header(H - 1, "y", &x.hash);
    (x, y)
}

/// X, Y and A at H, inserted through the real path: A is the active tip.
async fn chain_to_a(db: &impl HeaderDb) -> (BlockHeader, BlockHeader, BlockHeader) {
    let (x, y) = x_and_y();
    let a = header(H, "a", &y.hash);
    for h in [&x, &y, &a] {
        let r = insert_header(db, regtest(), h).await.unwrap();
        assert!(r.added && r.is_active_tip && r.reorg_depth == 0, "{r:?}");
    }
    (x, y, a)
}

/// `chain_to_a`, then A' (equal work, inactive) and B' (flips the tip): the
/// pending fork is H.
async fn chain_reorged(db: &impl HeaderDb) -> (BlockHeader, BlockHeader) {
    let (_x, y, _a) = chain_to_a(db).await;
    let a2 = header(H, "a prime", &y.hash);
    insert_header(db, regtest(), &a2).await.unwrap();
    let b2 = header(H + 1, "b prime", &a2.hash);
    let r = insert_header(db, regtest(), &b2).await.unwrap();
    assert_eq!(r.reorg_depth, 1, "{r:?}");
    (a2, b2)
}

#[derive(serde::Deserialize)]
struct CountRow {
    cnt: Option<f64>,
}

// ─── One test per HeaderDb method ───────────────────────────────────────────

/// `first` answers the FIRST row of the read in statement order, `None` when
/// the read answers nothing. To red: `SqliteDb::first`, `.next()` to `.last()`.
#[tokio::test]
async fn first_answers_the_first_row_in_statement_order_or_none() {
    let db = SqliteDb::migrated();
    let none: Option<HeaderRow> = Query::new(sql_header_for_hash())
        .bind("nothing")
        .first(&db)
        .await
        .unwrap();
    assert!(none.is_none());
    chain_to_a(&db).await;
    let lowest: Option<HeaderRow> = Query::new(format!("{SELECT_HEADER} ORDER BY height ASC"))
        .first(&db)
        .await
        .unwrap();
    assert_eq!(lowest.unwrap().height, Some(f64::from(H - 2)));
    let highest: Option<HeaderRow> = Query::new(format!("{SELECT_HEADER} ORDER BY height DESC"))
        .first(&db)
        .await
        .unwrap();
    assert_eq!(highest.unwrap().height, Some(f64::from(H)));
    let count: Option<CountRow> = Query::new(SQL_COUNT_ACTIVE_ABOVE)
        .bind(H - 2)
        .first(&db)
        .await
        .unwrap();
    assert_eq!(count.unwrap().cnt, Some(2.0));
}

/// `all` answers every row in statement order. To red: `SqliteDb::all`,
/// `.collect()` after a `.take(1)`.
#[tokio::test]
async fn all_answers_every_row_in_statement_order() {
    let db = SqliteDb::migrated();
    let empty: Vec<HeaderRow> = Query::new(format!("{SELECT_HEADER} ORDER BY height ASC"))
        .all(&db)
        .await
        .unwrap();
    assert!(empty.is_empty());
    let (x, y, a) = chain_to_a(&db).await;
    let rows: Vec<HeaderRow> = Query::new(format!("{SELECT_HEADER} ORDER BY height DESC"))
        .all(&db)
        .await
        .unwrap();
    let hashes: Vec<String> = rows.into_iter().map(|r| r.hash.unwrap()).collect();
    assert_eq!(hashes, vec![a.hash, y.hash, x.hash]);
}

/// `execute` answers the rows a write changed, the number the announce
/// record is built on. To red: `SqliteDb::execute`, `changed as u32` to `0`.
#[tokio::test]
async fn execute_answers_the_rows_it_changed() {
    let db = SqliteDb::migrated();
    let (x, _y, _a) = chain_to_a(&db).await;
    let none = Query::new(SQL_ACTIVATE_HASH)
        .bind("no such hash")
        .run_changes(&db)
        .await
        .unwrap();
    assert_eq!(none, 0);
    let two = Query::new(SQL_DEACTIVATE_ABOVE)
        .bind(H - 2)
        .run_changes(&db)
        .await
        .unwrap();
    assert_eq!(two, 2, "Y and A sit above X");
    let one = Query::new(SQL_ACTIVATE_HASH)
        .bind(x.hash.as_str())
        .run_changes(&db)
        .await
        .unwrap();
    assert_eq!(
        one, 1,
        "an UPDATE that changes no value still counts its matched row"
    );
}

/// `batch` applies every statement, in order, as one transaction: the tip
/// move's clear-then-set lands as a whole. To red: `SqliteDb::batch`, drop
/// the `tx.commit()` (the transaction rolls back on drop).
#[tokio::test]
async fn batch_applies_every_statement_in_order_as_one_transaction() {
    let db = SqliteDb::migrated();
    let (x, _y, a) = chain_to_a(&db).await;
    db.batch(vec![
        Query::new(SQL_CLEAR_CHAIN_TIP),
        Query::new(SQL_SET_CHAIN_TIP_ACTIVE).bind(x.hash.as_str()),
    ])
    .await
    .unwrap();
    assert_eq!(
        flags(&db, &a.hash),
        (true, false),
        "the old tip flag cleared"
    );
    assert_eq!(
        flags(&db, &x.hash),
        (true, true),
        "the new tip set, in the same transaction"
    );
}

// ─── One test per storage behavior the producer stands on ───────────────────

/// A header extending the tip lands active, linked to its parent by row id,
/// with CUMULATIVE work, and takes the tip. To red: `insert_header`, delete
/// the `update_chain_tip(db, &header.hash)` call.
#[tokio::test]
async fn insert_header_lands_a_tip_extension_active_linked_and_cumulative() {
    let db = SqliteDb::migrated();
    let (x, y) = x_and_y();
    let r = insert_header(&db, regtest(), &x).await.unwrap();
    assert!(r.added && r.no_tip && r.no_prev && r.is_active_tip, "{r:?}");
    let r = insert_header(&db, regtest(), &y).await.unwrap();
    assert!(
        r.added && !r.no_tip && !r.no_prev && r.is_active_tip,
        "{r:?}"
    );
    assert_eq!(r.reorg_depth, 0);
    assert_eq!(flags(&db, &x.hash), (true, false));
    assert_eq!(flags(&db, &y.hash), (true, true));
    let stored_y = find_header_for_height(&db, H - 1).await.unwrap().unwrap();
    assert_eq!(stored_y.hash, y.hash);
    assert_eq!(stored_y.previous_header_id, Some(header_id(&db, &x.hash)));
    let g = calculate_work(EQUAL_WORK_BITS);
    assert_eq!(
        stored_y.chain_work,
        add_work(&g, &g),
        "parent work plus this block's"
    );
    let dupe = insert_header(&db, regtest(), &y).await.unwrap();
    assert!(dupe.dupe && !dupe.added, "{dupe:?}");
}

/// An equal-work sibling at the tip's height lands INACTIVE: the tip stays,
/// the served header at that height stays, no fork is recorded. This is the
/// 2026-09-07 shape and exactly why the child has to carry the fork. To red:
/// `insert_header`, `is_more_work(&chain_work, &tip.chain_work)` to
/// `!is_more_work(&tip.chain_work, &chain_work)` (equal work would win).
#[tokio::test]
async fn insert_header_lands_an_equal_work_sibling_inactive() {
    let db = SqliteDb::migrated();
    let (_x, y, a) = chain_to_a(&db).await;
    let a2 = header(H, "a prime", &y.hash);
    let r = insert_header(&db, regtest(), &a2).await.unwrap();
    assert!(r.added && !r.is_active_tip && !r.no_prev, "{r:?}");
    assert_eq!(r.reorg_depth, 0);
    assert_eq!(flags(&db, &a2.hash), (false, false));
    assert_eq!(flags(&db, &a.hash), (true, true));
    assert_eq!(
        find_header_for_height(&db, H).await.unwrap().unwrap().hash,
        a.hash
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, a.hash);
    assert_eq!(pending(&db), None);
}

/// More cumulative work activates a branch: the child of the inactive
/// sibling flips the tip, the old branch above the fork goes inactive, the
/// new one active, the served header at the fork height changes, and the
/// fork is recorded. To red: `handle_reorg`, delete the
/// `batch.add(SQL_ACTIVATE_HASH, ...)` line (A' stays inactive).
#[tokio::test]
async fn insert_header_activates_a_branch_on_more_work() {
    let db = SqliteDb::migrated();
    let (x, y, a) = chain_to_a(&db).await;
    let a2 = header(H, "a prime", &y.hash);
    insert_header(&db, regtest(), &a2).await.unwrap();
    let b2 = header(H + 1, "b prime", &a2.hash);
    let r = insert_header(&db, regtest(), &b2).await.unwrap();
    assert!(r.added && r.is_active_tip && !r.no_prev, "{r:?}");
    assert_eq!(r.reorg_depth, 1, "A alone was orphaned");
    assert_eq!(flags(&db, &x.hash), (true, false));
    assert_eq!(flags(&db, &y.hash), (true, false));
    assert_eq!(flags(&db, &a.hash), (false, false));
    assert_eq!(flags(&db, &a2.hash), (true, false));
    assert_eq!(flags(&db, &b2.hash), (true, true));
    assert_eq!(
        find_header_for_height(&db, H).await.unwrap().unwrap().hash,
        a2.hash
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, b2.hash);
    assert_eq!(pending(&db), Some(i64::from(H)));
}

/// A heavier competitor at the SAME height as the tip takes it at once (more
/// work, not more height), with a one-deep reorg recorded at that height.
/// Under the node's minimum-difficulty rule (`min_difficulty_rule`): Y, 21
/// minutes after X, carries the limit's bits; its sibling, a minute after X,
/// carries X's heavier bits. To red: the same `is_more_work` line as the
/// sibling test.
#[tokio::test]
async fn insert_header_activates_a_heavier_same_height_competitor() {
    let db = SqliteDb::migrated();
    let rule = min_difficulty_rule();
    let x = heavy_root();
    let y = header_at(H - 1, "y, late", &x.hash, EQUAL_WORK_BITS, x.time + 21 * 60);
    insert_header(&db, rule, &x).await.unwrap();
    insert_header(&db, rule, &y).await.unwrap();
    let heavy = header_at(H - 1, "y heavy, prompt", &x.hash, HEAVY_BITS, x.time + 60);
    let r = insert_header(&db, rule, &heavy).await.unwrap();
    assert!(r.added && r.is_active_tip, "{r:?}");
    assert_eq!(r.reorg_depth, 1, "Y orphaned");
    assert_eq!(flags(&db, &y.hash), (false, false));
    assert_eq!(flags(&db, &heavy.hash), (true, true));
    assert_eq!(pending(&db), Some(i64::from(H - 1)));
}

/// The rule the test above runs under is the node's, not a free pass: a
/// prompt Y at the limit's bits, or a late Y at X's, is refused.
#[tokio::test]
async fn under_the_minimum_difficulty_rule_the_bits_still_bind() {
    let db = SqliteDb::migrated();
    let rule = min_difficulty_rule();
    let x = heavy_root();
    insert_header(&db, rule, &x).await.unwrap();
    let prompt_light = header_at(
        H - 1,
        "prompt at the limit",
        &x.hash,
        EQUAL_WORK_BITS,
        x.time + 60,
    );
    let e = insert_header(&db, rule, &prompt_light).await.unwrap_err();
    assert!(format!("{e}").contains("bad-diffbits"), "{e}");
    let late_heavy = header_at(
        H - 1,
        "late at x's bits",
        &x.hash,
        HEAVY_BITS,
        x.time + 21 * 60,
    );
    let e = insert_header(&db, rule, &late_heavy).await.unwrap_err();
    assert!(format!("{e}").contains("bad-diffbits"), "{e}");
}

/// `find_chain_tip` reads the tip row, `None` on an empty store, and on a
/// torn state (two tip rows, audit C4) the higher one. To red:
/// `sql_chain_tip`, `height DESC` to `height ASC`.
#[tokio::test]
async fn find_chain_tip_reads_the_tip_none_when_empty_and_the_higher_of_a_torn_pair() {
    let db = SqliteDb::migrated();
    assert!(find_chain_tip(&db).await.unwrap().is_none());
    let (x, y, a) = chain_to_a(&db).await;
    let tip = find_chain_tip(&db).await.unwrap().unwrap();
    assert_eq!(
        (
            tip.height,
            tip.hash.as_str(),
            tip.is_chain_tip,
            tip.is_active
        ),
        (H, a.hash.as_str(), true, true)
    );
    assert_eq!(tip.previous_header_id, Some(header_id(&db, &y.hash)));
    db.conn()
        .execute(
            "UPDATE headers SET is_chain_tip = 1 WHERE hash = ?1",
            [&x.hash],
        )
        .unwrap();
    assert_eq!(
        find_chain_tip(&db).await.unwrap().unwrap().hash,
        a.hash,
        "the higher tip row wins"
    );
    assert_eq!(Query::new(sql_chain_tip()).sql(), sql_chain_tip());
}

/// `handle_reorg` deactivates the old branch above the common ancestor,
/// activates the new one, answers the count it deactivated, and records
/// `ancestor + 1` as the pending fork; it moves no tip flag (the caller
/// does). To red: `handle_reorg`, `(ancestor_height + 1)` to `ancestor_height`.
#[tokio::test]
async fn handle_reorg_deactivates_above_the_ancestor_activates_the_branch_and_records_ancestor_plus_one(
) {
    let db = SqliteDb::migrated();
    let (_x, y, a) = chain_to_a(&db).await;
    let a2 = header(H, "a prime", &y.hash);
    insert_header(&db, regtest(), &a2).await.unwrap();
    let b2 = header(H + 1, "b prime", &a2.hash);
    let tip = find_chain_tip(&db).await.unwrap().unwrap();
    assert_eq!(tip.hash, a.hash);
    let deactivated = handle_reorg(&db, &b2, &tip).await.unwrap();
    assert_eq!(deactivated, 1);
    assert_eq!(
        flags(&db, &a.hash),
        (false, false),
        "the orphan lost both flags"
    );
    assert_eq!(
        flags(&db, &a2.hash),
        (true, false),
        "activated, no tip flag from handle_reorg"
    );
    assert!(
        find_chain_tip(&db).await.unwrap().is_none(),
        "the tip move is the caller's"
    );
    assert_eq!(pending(&db), Some(i64::from(y.height + 1)));
    // no common ancestor within the walk: refused, nothing changed
    let stranger = header(H + 1, "a stranger", &block_hash("an unknown parent"));
    let orphan_tip = BlockHeader {
        hash: block_hash("another unknown"),
        height: H,
        ..Default::default()
    };
    let err = handle_reorg(&db, &stranger, &orphan_tip).await.unwrap_err();
    assert!(format!("{err}").contains("no common ancestor"), "{err}");
}

/// `read_pending_reorg` answers the fork WITHOUT consuming it (the POST now
/// sits between the read and the clear), and `clear_pending_reorg` is a CAS
/// on the height carried: a stale height is a no-op, the matching one clears.
/// To red: `clear_pending_reorg`, `.bind(carried as i64)` to `.bind(0i64)`.
#[tokio::test]
async fn read_pending_reorg_does_not_consume_and_clear_pending_reorg_is_a_cas() {
    let db = SqliteDb::migrated();
    assert_eq!(read_pending_reorg(&db).await, None);
    chain_reorged(&db).await;
    assert_eq!(read_pending_reorg(&db).await, Some(u64::from(H)));
    assert_eq!(
        read_pending_reorg(&db).await,
        Some(u64::from(H)),
        "a read consumes nothing"
    );
    clear_pending_reorg(&db, u64::from(H + 1)).await;
    assert_eq!(pending(&db), Some(i64::from(H)), "a stale clear is a no-op");
    clear_pending_reorg(&db, u64::from(H)).await;
    assert_eq!(pending(&db), None);
    assert_eq!(read_pending_reorg(&db).await, None);
}

/// Two reorgs between announces MIN-accumulate: the deeper fork is kept, a
/// shallower later one does not raise it. To red: `RECORD_PENDING_REORG_SQL`,
/// `pending_reorg_from > ?1` to `pending_reorg_from < ?1`.
#[tokio::test]
async fn the_pending_fork_min_accumulates_across_reorgs() {
    let db = SqliteDb::migrated();
    let (x, y, _a) = chain_to_a(&db).await;
    let a2 = header(H, "a prime", &y.hash);
    insert_header(&db, regtest(), &a2).await.unwrap();
    let b2 = header(H + 1, "b prime", &a2.hash);
    insert_header(&db, regtest(), &b2).await.unwrap();
    assert_eq!(pending(&db), Some(i64::from(H)));
    // deeper: a branch off X that outworks B' only at H+2
    let c = header(H - 1, "c", &x.hash);
    let d = header(H, "d", &c.hash);
    let e = header(H + 1, "e", &d.hash);
    let f = header(H + 2, "f", &e.hash);
    for h in [&c, &d, &e] {
        let r = insert_header(&db, regtest(), h).await.unwrap();
        assert!(!r.is_active_tip, "{r:?}");
    }
    let r = insert_header(&db, regtest(), &f).await.unwrap();
    assert_eq!(r.reorg_depth, 3, "Y, A prime, B prime");
    assert_eq!(pending(&db), Some(i64::from(H - 1)), "the deeper fork");
    // shallower: a sibling of F and its child
    let g = header(H + 2, "g", &e.hash);
    let g2 = header(H + 3, "g two", &g.hash);
    insert_header(&db, regtest(), &g).await.unwrap();
    let r = insert_header(&db, regtest(), &g2).await.unwrap();
    assert_eq!(r.reorg_depth, 1, "F");
    assert_eq!(
        pending(&db),
        Some(i64::from(H - 1)),
        "a shallower fork does not raise it"
    );
    // the statement alone, the way another isolate's batch runs it
    Query::new(RECORD_PENDING_REORG_SQL)
        .bind(i64::from(H - 3))
        .run(&db)
        .await
        .unwrap();
    assert_eq!(pending(&db), Some(i64::from(H - 3)));
    Query::new(RECORD_PENDING_REORG_SQL)
        .bind(i64::from(H))
        .run(&db)
        .await
        .unwrap();
    assert_eq!(pending(&db), Some(i64::from(H - 3)));
}

/// The clear is a CAS on the height read: a deeper fork recorded between the
/// read and the clear survives to the next announce, a matching height is
/// cleared, a stale height is a no-op. To red: `CLEAR_PENDING_REORG_SQL`,
/// delete ` AND pending_reorg_from = ?1`.
#[tokio::test]
async fn the_cas_clear_clears_a_matching_height_and_never_a_deeper_one_written_after_the_read() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    chain_reorged(&rec).await;
    assert_eq!(pending(&db), Some(i64::from(H)));
    // another isolate's handle_reorg lands a deeper fork right after the read
    rec.after_first_of(READ_PENDING_REORG_SQL, |db| async move {
        Query::new(RECORD_PENDING_REORG_SQL)
            .bind(i64::from(H - 1))
            .run(db)
            .await
            .unwrap();
    });
    assert_eq!(read_pending_reorg(&rec).await, Some(u64::from(H)));
    clear_pending_reorg(&rec, u64::from(H)).await;
    assert_eq!(
        pending(&db),
        Some(i64::from(H - 1)),
        "the deeper fork survived the clear"
    );
    // a stale clear is a no-op
    let changed = Query::new(CLEAR_PENDING_REORG_SQL)
        .bind(i64::from(H))
        .run_changes(&db)
        .await
        .unwrap();
    assert_eq!(changed, 0);
    assert_eq!(pending(&db), Some(i64::from(H - 1)));
    // the matching height clears
    clear_pending_reorg(&rec, u64::from(H - 1)).await;
    assert_eq!(pending(&db), None);
}

/// The pre-check that runs before the POST (`tip_is_unannounced`) and the
/// record written after it (`TIP_ANNOUNCE_SQL`'s WHERE) decide the same on
/// every case: a fresh row, the same tip, a new height, a same-height other
/// hash, a lower height, a NULL hash. To red: `tip_is_unannounced`,
/// `last_height < height` to `last_height <= height`.
#[tokio::test]
async fn the_pre_check_and_the_announce_record_agree() {
    let db = SqliteDb::migrated();
    let cases: [(u32, Option<&str>, u32, &str, bool); 6] = [
        (0, None, H, "a", true),
        (H, Some("a"), H, "a", false),
        (H, Some("a"), H + 1, "b", true),
        (H, Some("a"), H, "b", true),
        (H + 5, Some("x"), H, "a", false),
        (H, None, H, "a", true),
    ];
    for (last_height, last_hash, height, hash, expected) in cases {
        db.conn()
            .execute(
                "UPDATE sync_state SET last_synced_height = ?1, last_announced_hash = ?2 WHERE id = 1",
                rusqlite::params![i64::from(last_height), last_hash],
            )
            .unwrap();
        let by_code = tip_is_unannounced(last_height, last_hash, height, hash);
        let by_statement = Query::new(TIP_ANNOUNCE_SQL)
            .bind(height)
            .bind(hash)
            .run_changes(&db)
            .await
            .unwrap()
            == 1;
        assert_eq!(
            by_code, expected,
            "the pre-check: {last_height} {last_hash:?} to {height} {hash}"
        );
        assert_eq!(
            by_statement, expected,
            "the record: {last_height} {last_hash:?} to {height} {hash}"
        );
    }
}

/// The announce records `(last_synced_height, last_announced_hash)` after a
/// delivery and fires on ANY tip change, height or hash: a new height, a
/// same-height replacement (a heavier competitor), never the same tip again,
/// never a height going backwards; the failure run stays 0 throughout. To
/// red: `TIP_ANNOUNCE_SQL`, `last_announced_hash <> ?2` to
/// `last_announced_hash = ?2`.
#[tokio::test]
async fn the_announce_records_the_hash_and_fires_on_any_tip_change_never_on_the_same_tip() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, "bearer");
    // the node's minimum-difficulty rule: Y late (the limit's bits), its
    // heavier sibling prompt (X's bits); see `min_difficulty_rule`
    let rule = min_difficulty_rule();
    let x = heavy_root();
    let y = header_at(H - 1, "y, late", &x.hash, EQUAL_WORK_BITS, x.time + 21 * 60);
    assert_eq!(announced(&db), (0, None), "a fresh row: never announced");
    insert_header(&db, rule, &x).await.unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(hooks.take().len(), 1);
    assert_eq!(announced(&db), (i64::from(H - 2), Some(x.hash.clone())));
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "the same tip again");
    insert_header(&db, rule, &y).await.unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(hooks.take().len(), 1, "a new height");
    assert_eq!(announced(&db), (i64::from(H - 1), Some(y.hash.clone())));
    // a heavier competitor at the SAME height: the hash changed, the height did not
    let heavy = header_at(H - 1, "y heavy, prompt", &x.hash, HEAVY_BITS, x.time + 60);
    insert_header(&db, rule, &heavy).await.unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(posts.len(), 1, "a same-height replacement fires");
    assert_eq!(
        posts[0].body,
        format!(
            r#"{{"height":{},"hash":"{}","reorgFrom":{}}}"#,
            H - 1,
            heavy.hash,
            H - 1
        )
    );
    assert_eq!(announced(&db), (i64::from(H - 1), Some(heavy.hash.clone())));
    // the row ahead of the tip (a stale isolate, an operator repair): silent, not moved backwards
    db.conn()
        .execute(
            "UPDATE sync_state SET last_synced_height = ?1, last_announced_hash = 'ahead' WHERE id = 1",
            [i64::from(H + 5)],
        )
        .unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "a lower height never fires");
    assert_eq!(
        announced(&db),
        (i64::from(H + 5), Some("ahead".to_string()))
    );
    assert_eq!(
        announce_counters(&db),
        (0, 0),
        "every delivery accepted: no failure run"
    );
}

/// The cron's tail: a D1 fault on the announce path is logged and the
/// cumulative-work repair still runs (round 3, LOW-2). The fault is injected
/// on the announce row read, before any POST; a corrupted legacy work value
/// on Y is repaired all the same. To red: `finish_sync`, the
/// `if let Err(e) = notify_if_tip_advanced(..)` back to a `?` / `unwrap`.
#[tokio::test]
async fn finish_sync_repairs_work_even_when_the_announce_faults() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let hooks = RecordedWebhooks::new(HOOK, "bearer");
    let (_x, y, _a) = chain_to_a(&rec).await;
    db.conn()
        .execute(
            "UPDATE headers SET chain_work = ?1 WHERE hash = ?2",
            rusqlite::params!["ff".repeat(32), y.hash],
        )
        .unwrap();
    rec.fault_first_of(ANNOUNCED_TIP_SQL);
    finish_sync(&rec, &hooks, None).await;
    assert!(
        hooks.take().is_empty(),
        "the announce faulted before any POST"
    );
    let g = calculate_work(EQUAL_WORK_BITS);
    let repaired: String = db
        .conn()
        .query_row(
            "SELECT chain_work FROM headers WHERE hash = ?1",
            [&y.hash],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        repaired,
        add_work(&g, &g),
        "Y's work repaired from its parent"
    );
    let handed = rec.handed();
    let faulted_at = handed
        .iter()
        .position(|h| matches!(h, Handed::First(q) if q.sql() == ANNOUNCED_TIP_SQL))
        .unwrap();
    assert!(
        handed[faulted_at + 1..]
            .iter()
            .any(|h| matches!(h, Handed::All(q) if q.sql() == sql_active_headers_between())),
        "the repair's window read ran after the faulted announce read"
    );
}

/// Round 5 (LOW-2): a migration applied in an earlier shape, or not at all,
/// is LOUD and visible: `/getInfo` names the missing column or table in
/// `tipAnnounceSchemaFault`, and the announce faults on that read or on the
/// claim with NOTHING consumed (no post, the marker, the row and the run
/// untouched). Three shapes: 0006's claim clock missing, 0006's deliveries
/// table missing, 0005's counters missing. To red: `get_info`, the
/// `schema_fault = Some(..)` assignments dropped.
#[tokio::test]
async fn a_missing_announce_column_or_table_is_named_on_get_info_and_the_announce_consumes_nothing()
{
    for (drop, names) in [
        (
            "ALTER TABLE sync_state DROP COLUMN claimed_at",
            "claimed_at",
        ),
        ("DROP TABLE announce_deliveries", "announce_deliveries"),
        (
            "ALTER TABLE sync_state DROP COLUMN announce_failures",
            "announce_failures",
        ),
    ] {
        let db = SqliteDb::migrated();
        let hooks = RecordedWebhooks::new(HOOK, "bearer");
        let (a2, b2) = chain_reorged(&db).await;
        let _ = a2;
        db.conn().execute(drop, []).unwrap();
        let row_before = announced(&db);
        let info = get_info(&db, &Chain::Main).await.unwrap();
        let fault = info.tip_announce_schema_fault.unwrap_or_default();
        assert!(fault.contains(names), "{drop}: /getInfo names it: {fault}");
        let err = notify_if_tip_advanced(&db, &hooks).await.unwrap_err();
        assert!(
            format!("{err}").contains(names),
            "{drop}: the announce fault names it: {err}"
        );
        assert!(hooks.take().is_empty(), "{drop}: nothing posted");
        assert_eq!(
            pending(&db),
            Some(i64::from(H)),
            "{drop}: the marker untouched"
        );
        assert_eq!(
            announced(&db),
            row_before,
            "{drop}: the claim did not land, the row untouched"
        );
        assert_ne!(row_before.1.as_deref(), Some(b2.hash.as_str()));
        finish_sync(&db, &hooks, None).await;
        assert!(
            hooks.take().is_empty(),
            "{drop}: the cron's tail logs the fault and consumes nothing"
        );
    }
    let healthy = get_info(&SqliteDb::migrated(), &Chain::Main).await.unwrap();
    assert_eq!(
        healthy.tip_announce_schema_fault, None,
        "absent when healthy"
    );
}
