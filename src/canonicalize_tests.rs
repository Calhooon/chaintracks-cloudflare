//! #33, the 956433 class, on the test store (a real SQLite carrying the real
//! migrations, every write through the Worker's own storage functions).
//!
//! What happened on production (bsv-stack-lean `docs/p0/p0-4.md`, the repair
//! of 2026-10-09): two blocks competed at 956433 on 2026-07-02; the store
//! kept the orphan active; the row at 956434 named the chain's block as its
//! parent and no row held it. The operator's `POST /admin/ingest` of the
//! chain's header inserted it inactive and answered 500; the dual-active
//! sweep had chosen the orphan because it was inserted last; nothing walked
//! the active chain and said where it was not linked.

use crate::consensus::ChainParams;
use crate::host_harness::{block_hash, flags, header, header_id, pending, SqliteDb};
use crate::storage::{
    self, check_root_for_height, find_chain_tip, find_header_for_height, insert_headers_batch,
    update_chain_tip_to_highest, SQL_INSERT_HEADER,
};
use crate::types::BlockHeader;

/// The fork height of the fixtures: `x` (the anchor of `regtest()`) is two
/// below it, as 956432 sat below the fork on production.
const H: u32 = 965_771;

/// How far the tip stands above the fork: past the 400-step ancestor walk
/// (`find_common_ancestor`), as production's tip stood 13,908 above 956433.
const ABOVE: u32 = 450;

/// Regtest's rules with one anchor, `x` (the harness's `regtest()` carries
/// two more fixture roots inside the 450 heights this file walks).
fn regtest() -> &'static ChainParams {
    static PARAMS: std::sync::OnceLock<ChainParams> = std::sync::OnceLock::new();
    PARAMS.get_or_init(|| {
        let x = header(H - 2, "x", &block_hash("the block below x"));
        ChainParams::regtest()
            .with_checkpoints(&format!("{}:{}", x.height, x.hash))
            .unwrap()
    })
}

/// The fork of 2026-07-02 as fixtures: `x`, `y` (the common parent), the
/// chain's block and the orphan at `H` (both children of `y`), and the run
/// that stands on the chain's block, `H+1 ..= H+above`.
struct Fork {
    x: BlockHeader,
    y: BlockHeader,
    chain: BlockHeader,
    orphan: BlockHeader,
    above: Vec<BlockHeader>,
}

fn fork(above: u32) -> Fork {
    let x = header(H - 2, "x", &block_hash("the block below x"));
    let y = header(H - 1, "#33 y", &x.hash);
    let chain = header(H, "#33 the chain's block", &y.hash);
    let orphan = header(H, "#33 the orphan", &y.hash);
    let mut run = Vec::new();
    let mut parent = chain.hash.clone();
    for i in 1..=above {
        let h = header(H + i, &format!("#33 above {i}"), &parent);
        parent = h.hash.clone();
        run.push(h);
    }
    Fork {
        x,
        y,
        chain,
        orphan,
        above: run,
    }
}

/// One row written past the rules, as the code before P0-4 wrote it: a bulk
/// row, active, `previous_header_id` null.
fn raw_active_row(db: &SqliteDb, h: &BlockHeader) {
    db.conn()
        .execute(
            SQL_INSERT_HEADER,
            rusqlite::params![
                Option::<i64>::None,
                h.previous_hash,
                h.height,
                1,
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

fn delete_row(db: &SqliteDb, hash: &str) {
    let n = db
        .conn()
        .execute("DELETE FROM headers WHERE hash = ?1", [hash])
        .unwrap();
    assert_eq!(n, 1);
}

/// `previous_header_id` of the row with this hash, read raw.
fn previous_id(db: &SqliteDb, hash: &str) -> Option<i64> {
    db.conn()
        .query_row(
            "SELECT previous_header_id FROM headers WHERE hash = ?1",
            [hash],
            |r| r.get(0),
        )
        .unwrap()
}

/// The production store as the long pass found it: the orphan the active row
/// at `H`, the chain's row absent, the row at `H+1` naming the chain's block
/// with no parent link, the tip `above` blocks higher.
async fn store_with_the_orphan_active(above: u32) -> (SqliteDb, Fork) {
    let db = SqliteDb::migrated();
    let f = fork(above);
    let mut run = vec![f.x.clone(), f.y.clone(), f.chain.clone()];
    run.extend(f.above.iter().cloned());
    for chunk in run.chunks(100) {
        insert_headers_batch(&db, regtest(), chunk).await.unwrap();
    }
    delete_row(&db, &f.chain.hash);
    raw_active_row(&db, &f.orphan);
    update_chain_tip_to_highest(&db).await.unwrap();
    assert_eq!(
        find_header_for_height(&db, H).await.unwrap().unwrap().hash,
        f.orphan.hash,
        "the orphan is the served row at the fork height"
    );
    (db, f)
}

/// The operator's push, as `POST /admin/ingest` runs it.
async fn ingest(db: &SqliteDb, headers: &[BlockHeader]) -> worker::Result<storage::IngestOutcome> {
    storage::ingest_pushed(db, regtest(), headers)
        .await
        .map_err(|e| match e {
            storage::IngestError::Refused(e) | storage::IngestError::Store(e) => e,
        })
}

/// THE WITNESS OF THE 500. The orphan is active at `H`, the stored row at
/// `H+1` commits to the chain's block, the tip is 450 above. The operator
/// pushes the chain's 80 bytes. Before #33 the route inserted the row
/// inactive and then failed in the reorg walk ("no common ancestor within
/// limit": the walk gives up 400 below the tip), a 500 with the orphan still
/// served. Now the pushed row is the active row at its height, the orphan
/// inactive, the child linked, the tip where it was, and the answer says so.
#[tokio::test]
async fn the_ingest_activates_the_row_the_stored_child_commits_to() {
    let (db, f) = store_with_the_orphan_active(ABOVE).await;
    let tip = find_chain_tip(&db).await.unwrap().unwrap();
    assert_eq!(tip.height, H + ABOVE);
    let child = &f.above[0];
    assert_eq!(previous_id(&db, &child.hash), None);

    let out = ingest(&db, std::slice::from_ref(&f.chain))
        .await
        .expect("the ingest answers, never a 500 after the insert");

    assert_eq!(out.outcome, "activated", "{out:?}");
    assert_eq!((out.inserted, out.canonicalized), (1, 1), "{out:?}");
    assert_eq!(out.deactivated, vec![f.orphan.hash.clone()]);
    assert_eq!(out.child_relinked.as_deref(), Some(child.hash.as_str()));
    assert_eq!(flags(&db, &f.chain.hash), (true, false));
    assert_eq!(flags(&db, &f.orphan.hash), (false, false));
    assert_eq!(
        previous_id(&db, &child.hash),
        Some(header_id(&db, &f.chain.hash)),
        "the child's parent link is the pushed row"
    );
    // The answers production gave wrong, now right.
    assert_eq!(
        check_root_for_height(&db, &f.chain.merkle_root, H)
            .await
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        check_root_for_height(&db, &f.orphan.merkle_root, H)
            .await
            .unwrap(),
        Some(false)
    );
    assert!(
        storage::find_active_header_for_hash(&db, &f.chain.hash)
            .await
            .unwrap()
            .is_some(),
        "the chain's header is served by hash"
    );
    // Nothing above the fork moved: the tip, and every row on the way to it.
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, tip.hash);
    for h in &f.above {
        assert!(flags(&db, &h.hash).0, "{} stays active", h.height);
    }
    // A consumer holding a proof at the fork height is told to re-verify from it.
    assert_eq!(pending(&db), Some(i64::from(H)));
    // The link check that named the break names none now.
    let report = storage::link_check(&db, H - 2, None).await.unwrap();
    assert!(report.linked && report.broken.is_empty(), "{report:?}");
}

/// The state production was left in after the 500: the chain's row already
/// stored, inactive, with the orphan still served. The same push activates it
/// (the insert is a no-op; the rules still ran on the pushed bytes).
#[tokio::test]
async fn the_ingest_activates_a_row_an_earlier_failed_push_left_inactive() {
    let (db, f) = store_with_the_orphan_active(3).await;
    let r = storage::insert_header(&db, regtest(), &f.chain)
        .await
        .unwrap();
    assert!(r.added && !r.is_active_tip, "{r:?}");
    assert_eq!(flags(&db, &f.chain.hash), (false, false));

    let tip = find_chain_tip(&db).await.unwrap().unwrap();
    let out = ingest(&db, std::slice::from_ref(&f.chain)).await.unwrap();
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, tip.hash);
    for h in &f.above {
        assert!(flags(&db, &h.hash).0, "{} stays active", h.height);
    }
    assert_eq!(flags(&db, &f.chain.hash), (true, false));
    assert_eq!(flags(&db, &f.orphan.hash), (false, false));
    assert_eq!(out.outcome, "activated", "{out:?}");
    assert_eq!((out.inserted, out.canonicalized), (0, 1), "{out:?}");
}

/// The other half of the rule: a pushed header the stored child does NOT
/// commit to is stored inactive and the answer says so. Before #33 the push
/// was "authoritative": it ran the reorg walk from the pushed header, so one
/// operator push of an orphan deactivated every block above the fork and
/// served the orphan as the tip.
#[tokio::test]
async fn a_pushed_header_the_child_does_not_commit_to_is_stored_inactive() {
    let db = SqliteDb::migrated();
    let f = fork(3);
    let mut run = vec![f.x.clone(), f.y.clone(), f.chain.clone()];
    run.extend(f.above.iter().cloned());
    insert_headers_batch(&db, regtest(), &run).await.unwrap();
    update_chain_tip_to_highest(&db).await.unwrap();
    let tip = find_chain_tip(&db).await.unwrap().unwrap();

    let out = ingest(&db, std::slice::from_ref(&f.orphan)).await.unwrap();

    assert_eq!(flags(&db, &f.orphan.hash), (false, false));
    assert_eq!(flags(&db, &f.chain.hash), (true, false));
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, tip.hash);
    for h in &f.above {
        assert!(flags(&db, &h.hash).0, "{} stays active", h.height);
    }
    assert_eq!(out.outcome, "storedInactive", "{out:?}");
    assert_eq!((out.inserted, out.canonicalized), (1, 0), "{out:?}");
    assert!(out.deactivated.is_empty() && out.child_relinked.is_none());
    let reason = out.reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains(&format!("{}", H + 1)) && reason.contains("does not name"),
        "the answer says why: {reason}"
    );
    assert_eq!(pending(&db), None);
}

/// "Its parent the active row below": two blocks of an orphan branch are
/// active at `H` and `H+1`, the chain's row at `H` is stored inactive, and
/// the row at `H+2` names the chain's block at `H+1`. The push of `H+1`
/// alone stands on an inactive parent: activating it would leave `H` broken
/// under it, so it is stored inactive and the answer says why. The push of
/// both, standing on the active row at `H-1`, activates both.
#[tokio::test]
async fn a_push_on_an_inactive_parent_waits_for_the_run_that_reaches_the_active_chain() {
    let (db, f) = store_with_the_orphan_active(3).await;
    delete_row(&db, &f.above[0].hash);
    let orphan2 = header(H + 1, "#33 the orphan's child", &f.orphan.hash);
    raw_active_row(&db, &orphan2);
    storage::insert_header(&db, regtest(), &f.chain)
        .await
        .unwrap();
    assert_eq!(flags(&db, &f.chain.hash), (false, false));

    let out = ingest(&db, std::slice::from_ref(&f.above[0]))
        .await
        .unwrap();
    assert_eq!(flags(&db, &f.above[0].hash), (false, false));
    assert_eq!(flags(&db, &orphan2.hash), (true, false));
    assert_eq!(out.outcome, "storedInactive", "{out:?}");
    assert!(
        out.reason
            .as_deref()
            .is_some_and(|r| r.contains("is not the active row below")),
        "{out:?}"
    );

    let out = ingest(&db, &[f.chain.clone(), f.above[0].clone()])
        .await
        .unwrap();
    assert_eq!(out.outcome, "activated", "{out:?}");
    assert_eq!(out.canonicalized, 2);
    assert_eq!(
        out.deactivated,
        vec![f.orphan.hash.clone(), orphan2.hash.clone()]
    );
    assert_eq!(
        out.child_relinked.as_deref(),
        Some(f.above[1].hash.as_str())
    );
    assert_eq!(pending(&db), Some(i64::from(H)));
    let report = storage::link_check(&db, H - 2, None).await.unwrap();
    assert!(report.linked, "{report:?}");
}

/// A push that passes no rule changes nothing: the header at the fork height
/// with bits the difficulty rule does not answer is refused (the route's
/// 422), and the orphan stays where it was for the operator to see.
#[tokio::test]
async fn a_pushed_header_that_fails_the_rules_is_refused_before_any_write() {
    let (db, f) = store_with_the_orphan_active(3).await;
    let wrong_bits = crate::host_harness::header_with_bits(
        H,
        "#33 wrong bits",
        &f.y.hash,
        crate::host_harness::HEAVY_BITS,
    );
    let refused = storage::ingest_pushed(&db, regtest(), std::slice::from_ref(&wrong_bits)).await;
    assert!(
        matches!(&refused, Err(storage::IngestError::Refused(e)) if format!("{e}").contains("bad-diffbits")),
        "{:?}",
        refused.map_err(|e| match e {
            storage::IngestError::Refused(e) | storage::IngestError::Store(e) => format!("{e}"),
        })
    );
    let count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM headers WHERE hash = ?1",
            [&wrong_bits.hash],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(flags(&db, &f.orphan.hash), (true, false));
}

/// A push that extends the tip is what it always was: stored active, the tip.
#[tokio::test]
async fn a_push_above_the_tip_is_the_tip() {
    let db = SqliteDb::migrated();
    let f = fork(3);
    insert_headers_batch(&db, regtest(), &[f.x.clone(), f.y.clone()])
        .await
        .unwrap();
    update_chain_tip_to_highest(&db).await.unwrap();
    let mut run = vec![f.chain.clone()];
    run.extend(f.above.iter().cloned());
    let out = ingest(&db, &run).await.unwrap();
    assert_eq!(out.outcome, "active", "{out:?}");
    assert_eq!((out.inserted, out.canonicalized), (4, 4), "{out:?}");
    assert_eq!(
        find_chain_tip(&db).await.unwrap().unwrap().hash,
        f.above[2].hash
    );
}

// ─── The sweep of dual-active heights ───────────────────────────────────────

/// The fork of 2026-07-02 as the old code stored it: both blocks at `H`
/// active, the chain's row inserted before the orphan's (so the orphan holds
/// the higher `header_id`), the row at `H+1` naming the chain's block.
async fn store_with_both_rows_active(above: u32) -> (SqliteDb, Fork) {
    let db = SqliteDb::migrated();
    let f = fork(above);
    let mut run = vec![f.x.clone(), f.y.clone(), f.chain.clone()];
    run.extend(f.above.iter().cloned());
    insert_headers_batch(&db, regtest(), &run).await.unwrap();
    raw_active_row(&db, &f.orphan);
    update_chain_tip_to_highest(&db).await.unwrap();
    assert!(header_id(&db, &f.orphan.hash) > header_id(&db, &f.chain.hash));
    assert!(flags(&db, &f.orphan.hash).0 && flags(&db, &f.chain.hash).0);
    (db, f)
}

/// THE WITNESS OF THE SWEEP. Two active rows at `H`; the active row at `H+1`
/// names the EARLIER one. Before #33 the sweep kept the newest ingest, the
/// orphan, and deactivated the block 14,000 blocks of work stood on (how
/// 956433 came to serve an orphan). Now it keeps the row the next height
/// extends.
#[tokio::test]
async fn the_sweep_keeps_the_row_the_next_height_extends_not_the_newest() {
    let (db, f) = store_with_both_rows_active(3).await;
    let swept = storage::dedupe_active_heights(&db).await.unwrap();
    assert_eq!(swept, 1);
    assert_eq!(
        (flags(&db, &f.chain.hash).0, flags(&db, &f.orphan.hash).0),
        (true, false),
        "(the chain's row, the orphan) active"
    );
    assert_eq!(
        find_header_for_height(&db, H).await.unwrap().unwrap().hash,
        f.chain.hash
    );
    for h in &f.above {
        assert!(flags(&db, &h.hash).0, "{} stays active", h.height);
    }
    // Nothing left to sweep.
    assert_eq!(storage::dedupe_active_heights(&db).await.unwrap(), 0);
}

/// Two competing branches of two blocks, all four rows active, the orphan
/// branch the newer rows; the row at `H+2` names the chain's branch. The
/// sweep works from the top down, so the choice at `H+1` is made before `H`
/// asks which of its rows the next height extends.
#[tokio::test]
async fn the_sweep_follows_one_branch_down_through_adjacent_dual_heights() {
    let (db, f) = store_with_both_rows_active(3).await;
    let orphan2 = header(H + 1, "#33 the orphan's child", &f.orphan.hash);
    raw_active_row(&db, &orphan2);
    assert!(header_id(&db, &orphan2.hash) > header_id(&db, &f.above[0].hash));

    assert_eq!(storage::dedupe_active_heights(&db).await.unwrap(), 2);
    assert!(flags(&db, &f.chain.hash).0 && flags(&db, &f.above[0].hash).0);
    assert!(!flags(&db, &f.orphan.hash).0 && !flags(&db, &orphan2.hash).0);
}

/// "And only then the newest": with no active row at the next height to
/// name either (two active rows at the tip's height), the sweep keeps the
/// newest ingest, as it always did.
#[tokio::test]
async fn the_sweep_keeps_the_newest_where_no_next_height_decides() {
    let (db, f) = store_with_both_rows_active(0).await;
    assert_eq!(storage::dedupe_active_heights(&db).await.unwrap(), 1);
    assert_eq!(
        (flags(&db, &f.chain.hash).0, flags(&db, &f.orphan.hash).0),
        (false, true)
    );
}

// ─── The link check ─────────────────────────────────────────────────────────

/// THE WITNESS OF THE LINK CHECK. The store as the long pass found it, one
/// broken link: the active row at `H` is the orphan and the row at `H+1`
/// names the chain's block. The check walks the active chain from `x` to the
/// tip and reports that height and nothing else, with both hashes, under no
/// checkpoint and no rule but "the next height names this row". Before #33
/// nothing in the service answered this question.
#[tokio::test]
async fn the_link_check_reports_the_one_height_the_next_height_does_not_name() {
    let (db, f) = store_with_the_orphan_active(ABOVE).await;
    let report = storage::link_check(&db, H - 2, None).await.unwrap();
    assert_eq!(
        report.broken,
        vec![storage::BrokenLink {
            height: H,
            active: Some(f.orphan.hash.clone()),
            next_names: f.chain.hash.clone(),
            next: f.above[0].hash.clone(),
        }],
        "{report:?}"
    );
    assert!(!report.linked && !report.truncated);
    assert_eq!((report.from, report.to), (H - 2, H + ABOVE));
    assert_eq!(report.checked_through, H + ABOVE);
    assert_eq!(report.rows, u64::from(ABOVE) + 3);
    // The same answer whatever the span of one read, the break on a span's edge or inside it.
    for span in [1, 2, 3, 7, 1000] {
        let spanned = storage::link_check_spans(&db, H - 2, None, span)
            .await
            .unwrap();
        assert_eq!(spanned, report, "span {span}");
    }
    // Asked from above the break, the chain is linked; asked up to it, too.
    let above = storage::link_check(&db, H + 1, None).await.unwrap();
    assert!(above.linked && above.broken.is_empty(), "{above:?}");
    let below = storage::link_check(&db, H - 2, Some(H)).await.unwrap();
    assert!(below.linked && below.rows == 3, "{below:?}");
}

/// From the genesis, with no `from`: a store that starts at an anchor is not
/// linked to the genesis, and the check says where the chain starts (one
/// entry, no active row below the anchor). A linked chain above it adds none.
#[tokio::test]
async fn the_link_check_from_the_genesis_names_where_the_store_starts() {
    let db = SqliteDb::migrated();
    let f = fork(5);
    let mut run = vec![f.x.clone(), f.y.clone(), f.chain.clone()];
    run.extend(f.above.iter().cloned());
    insert_headers_batch(&db, regtest(), &run).await.unwrap();
    update_chain_tip_to_highest(&db).await.unwrap();

    let report = storage::link_check(&db, 0, None).await.unwrap();
    assert_eq!(
        report.broken,
        vec![storage::BrokenLink {
            height: H - 3,
            active: None,
            next_names: f.x.previous_hash.clone(),
            next: f.x.hash.clone(),
        }]
    );
    assert_eq!(report.rows, 8);
    let report = storage::link_check(&db, H - 2, None).await.unwrap();
    assert!(report.linked && report.rows == 8, "{report:?}");
}

/// A hole (no active row at a height under an active row) and a height with
/// two active rows are each reported: the hole with no hash, the second
/// active row as a row the next height does not name.
#[tokio::test]
async fn the_link_check_reports_a_hole_and_a_second_active_row() {
    let (db, f) = store_with_both_rows_active(5).await;
    db.conn()
        .execute(
            "UPDATE headers SET is_active = 0 WHERE hash = ?1",
            [&f.above[2].hash],
        )
        .unwrap();
    let report = storage::link_check(&db, H - 2, None).await.unwrap();
    assert_eq!(
        report
            .broken
            .iter()
            .map(|b| (b.height, b.active.clone()))
            .collect::<Vec<_>>(),
        vec![(H, Some(f.orphan.hash.clone())), (H + 3, None)],
        "{report:?}"
    );
    // The sweep settles the dual height; the hole is the operator's to fill.
    storage::dedupe_active_heights(&db).await.unwrap();
    let report = storage::link_check(&db, H - 2, None).await.unwrap();
    assert_eq!(report.broken.len(), 1, "{report:?}");
    assert_eq!(report.broken[0].height, H + 3);
}
