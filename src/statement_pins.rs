//! Every statement the worker hands D1, pinned against the literal main
//! `d2317f2` ran (bsv-low M19B-G2, 2026-09-08), so the `HeaderDb` seam can
//! never drift the worker path and the host harness apart on the text they
//! execute. The goldens below were extracted from main's source BYTES with a
//! script (Rust string literals folded the way rustc folds a `\`
//! continuation), never typed by hand, and they are literals here on purpose:
//! a pin that read the constants it checks would satisfy itself.
//!
//! To red a pin: change one byte of the named constant in `storage.rs` or
//! `sync.rs`; the matching `assert_eq!` names it.

use crate::storage::{
    sql_active_header_for_hash, sql_active_header_for_height, sql_active_headers_between,
    sql_active_headers_from_to, sql_chain_tip, sql_header_for_hash, sql_header_for_id,
    sql_highest_active_header, RECORD_PENDING_REORG_SQL, SELECT_HEADER, SQL_ACTIVATE_HASH,
    SQL_CANONICALIZE_HEIGHT, SQL_CLEAR_CHAIN_TIP, SQL_COUNT_ACTIVE_ABOVE, SQL_COUNT_HEADERS,
    SQL_DEACTIVATE_ABOVE, SQL_INSERT_HEADER, SQL_RELINK_HEADER, SQL_SET_CHAIN_TIP,
    SQL_SET_CHAIN_TIP_ACTIVE, SQL_SET_CHAIN_WORK, SQL_SYNC_FRESHNESS,
};
use crate::storage::{SQL_ANNOUNCE_COUNTERS, SQL_ANNOUNCE_SCHEMA_PROBE, SQL_COUNT_DELIVERIES};
use crate::sync::{
    ANNOUNCED_TIP_SQL, CLEAR_PENDING_REORG_SQL, COUNT_ANNOUNCE_STUCK_SQL, READ_DELIVERIES_SQL,
    READ_PENDING_REORG_SQL, RECLAIM_ANNOUNCE_SQL, RECORD_DELIVERY_SQL, SET_ANNOUNCE_FAILURES_SQL,
    TIP_ANNOUNCE_SQL,
};

// ─── The goldens: main d2317f2, extracted from the source bytes ─────────────

/// main d2317f2, storage.rs.
pub(crate) const MAIN_SELECT_HEADER: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_CHAIN_TIP: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE is_chain_tip = 1 ORDER BY height DESC, header_id DESC LIMIT 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_ACTIVE_HEADER_FOR_HEIGHT: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE height = ? AND is_active = 1 ORDER BY header_id DESC LIMIT 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_HEADER_FOR_HASH: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE hash = ? LIMIT 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_ACTIVE_HEADER_FOR_HASH: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE hash = ? AND is_active = 1 LIMIT 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_HIGHEST_ACTIVE_HEADER: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE is_active = 1 ORDER BY height DESC, header_id DESC LIMIT 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_HEADER_FOR_ID: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE header_id = ? LIMIT 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_ACTIVE_HEADERS_FROM_TO: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE height >= ? AND height < ? AND is_active = 1 ORDER BY height ASC";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_ACTIVE_HEADERS_BETWEEN: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce FROM headers WHERE is_active = 1 AND height >= ? AND height <= ? ORDER BY height ASC";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_COUNT_ACTIVE_ABOVE: &str =
    "SELECT COUNT(*) as cnt FROM headers WHERE is_active = 1 AND height > ?";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_COUNT_HEADERS: &str = "SELECT COUNT(*) as cnt FROM headers";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_SYNC_FRESHNESS: &str =
    "SELECT last_synced_height, updated_at FROM sync_state WHERE id = 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_INSERT_HEADER: &str =
    "INSERT OR IGNORE INTO headers (previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_CLEAR_CHAIN_TIP: &str =
    "UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_SET_CHAIN_TIP_ACTIVE: &str =
    "UPDATE headers SET is_chain_tip = 1, is_active = 1 WHERE hash = ?";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_SET_CHAIN_TIP: &str = "UPDATE headers SET is_chain_tip = 1 WHERE hash = ?";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_DEACTIVATE_ABOVE: &str =
    "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ? AND is_active = 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_ACTIVATE_HASH: &str = "UPDATE headers SET is_active = 1 WHERE hash = ?";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_RECORD_PENDING_REORG: &str =
    "UPDATE sync_state SET pending_reorg_from =         CASE WHEN pending_reorg_from IS NULL OR pending_reorg_from > ?1 THEN ?1 ELSE pending_reorg_from END      WHERE id = 1";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_RELINK_HEADER: &str =
    "UPDATE headers SET previous_header_id = ?, chain_work = ? WHERE header_id = ?";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_SET_CHAIN_WORK: &str =
    "UPDATE headers SET chain_work = ? WHERE header_id = ?";

/// main d2317f2, storage.rs.
pub(crate) const MAIN_CANONICALIZE_HEIGHT: &str =
    "UPDATE headers SET is_active = CASE WHEN hash = ? THEN 1 ELSE 0 END WHERE height = ?";

/// main d2317f2, sync.rs. Retired by #33 (`CT33_DEDUPE_HEIGHT`): it kept the
/// newest ingest whatever the next height extended. Kept here as the record
/// of what main ran.
pub(crate) const MAIN_DEDUPE_ACTIVE_HEIGHTS: &str =
    "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN              (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)";

/// main d2317f2, routes.rs. Retired by #33 with its twin above.
pub(crate) const MAIN_DEDUPE_ACTIVE_HEIGHTS_INGEST: &str =
    "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)";

/// main d2317f2, sync.rs.
pub(crate) const MAIN_TIP_ANNOUNCE: &str =
    "UPDATE sync_state SET last_synced_height = ?1, last_announced_hash = ?2, live_sync_active = 1, updated_at = datetime('now') WHERE id = 1 AND (last_synced_height < ?1 OR (last_synced_height = ?1 AND (last_announced_hash IS NULL OR last_announced_hash <> ?2)))";

/// main d2317f2, sync.rs.
pub(crate) const MAIN_READ_PENDING_REORG: &str =
    "SELECT pending_reorg_from FROM sync_state WHERE id = 1";

/// main d2317f2, sync.rs.
pub(crate) const MAIN_CLEAR_PENDING_REORG: &str =
    "UPDATE sync_state SET pending_reorg_from = NULL WHERE id = 1 AND pending_reorg_from = ?1";

// ─── Round 2 (the announce delivers before it records): NEW on this branch ──
// These four have no literal on main. Their goldens are this branch's text,
// typed once here, so a later edit of a constant still reds a pin; the
// producer trace and vocabulary pins list them apart from main's.

/// Round 2, sync.rs: the announced tip and the failure run, read before the POST.
pub(crate) const ROUND2_ANNOUNCED_TIP: &str =
    "SELECT last_synced_height, last_announced_hash, announce_failures FROM sync_state WHERE id = 1";

/// Round 2, sync.rs: the failure run, written on an undelivered announce, 0 on delivery.
pub(crate) const ROUND2_SET_ANNOUNCE_FAILURES: &str =
    "UPDATE sync_state SET announce_failures = ?1 WHERE id = 1";

/// Round 2, sync.rs: one stuck event.
pub(crate) const ROUND2_COUNT_ANNOUNCE_STUCK: &str =
    "UPDATE sync_state SET tip_announce_stuck_total = tip_announce_stuck_total + 1 WHERE id = 1";

/// Round 2, storage.rs: the counters on /getInfo.
pub(crate) const ROUND2_ANNOUNCE_COUNTERS: &str =
    "SELECT announce_failures, tip_announce_stuck_total FROM sync_state WHERE id = 1";

// ─── Round 3 (the claim before the POST, per-target deliveries, the retry claim) ──

/// Round 3, sync.rs: the retry claim, a CAS on the claim's own (height, hash)
/// gated on the claim's age.
pub(crate) const ROUND3_RECLAIM_ANNOUNCE: &str =
    "UPDATE sync_state SET claimed_at = datetime('now') WHERE id = 1 AND last_synced_height = ?1 AND last_announced_hash = ?2 AND (claimed_at IS NULL OR (julianday('now') - julianday(claimed_at)) * 86400 >= ?3)";

/// Round 3, sync.rs: which target has which tip.
pub(crate) const ROUND3_READ_DELIVERIES: &str =
    "SELECT target, height, hash, reorg_from FROM announce_deliveries";

/// Round 3, sync.rs: one target accepted this tip.
pub(crate) const ROUND3_RECORD_DELIVERY: &str =
    "INSERT INTO announce_deliveries (target, height, hash, reorg_from, delivered_at) SELECT ?1, ?2, ?3, ?4, datetime('now') FROM sync_state WHERE id = 1 AND last_synced_height = ?2 AND last_announced_hash = ?3 ON CONFLICT(target) DO UPDATE SET height = excluded.height, hash = excluded.hash, reorg_from = COALESCE(excluded.reorg_from, announce_deliveries.reorg_from), delivered_at = excluded.delivered_at";

// ─── Round 4: the ONE deliberate change to a statement main ran ─────────────

/// Round 4, sync.rs: the claim also stamps `claimed_at`, the retry window's
/// clock, so the retry claim never has to touch `updated_at` (the /getInfo
/// freshness signal). Exactly main's literal with one column inserted, and
/// the pin below holds that delta, not a free-standing copy.
pub(crate) const ROUND4_TIP_ANNOUNCE: &str =
    "UPDATE sync_state SET last_synced_height = ?1, last_announced_hash = ?2, live_sync_active = 1, claimed_at = datetime('now'), updated_at = datetime('now') WHERE id = 1 AND (last_synced_height < ?1 OR (last_synced_height = ?1 AND (last_announced_hash IS NULL OR last_announced_hash <> ?2)))";

// ─── Round 5: the /getInfo announce schema probes ───────────────────────────

/// Round 5, storage.rs: the claim clock probe on /getInfo.
pub(crate) const ROUND5_ANNOUNCE_SCHEMA_PROBE: &str =
    "SELECT claimed_at FROM sync_state WHERE id = 1";

/// Round 5, storage.rs: the deliveries table probe on /getInfo.
pub(crate) const ROUND5_COUNT_DELIVERIES: &str = "SELECT COUNT(*) as cnt FROM announce_deliveries";

// ─── The pins ───────────────────────────────────────────────────────────────

/// The round-3 statements are the three above and nothing else; none of them
/// is in main's or round 2's vocabulary.
#[test]
fn the_round_3_statements_are_the_three_new_literals() {
    assert_eq!(RECLAIM_ANNOUNCE_SQL, ROUND3_RECLAIM_ANNOUNCE);
    assert_eq!(READ_DELIVERIES_SQL, ROUND3_READ_DELIVERIES);
    assert_eq!(RECORD_DELIVERY_SQL, ROUND3_RECORD_DELIVERY);
    for s in ROUND3_VOCABULARY {
        assert!(
            !MAIN_VOCABULARY.contains(&s) && !ROUND2_VOCABULARY.contains(&s),
            "{s}"
        );
    }
}

/// Migration 0006 is additive: one CREATE TABLE IF NOT EXISTS (with the
/// fork column, round 4) and one nullable ADD COLUMN on sync_state (the claim
/// clock, round 4); nothing dropped, nothing renamed.
#[test]
fn the_0006_migration_is_additive() {
    let sql = include_str!("../migrations/0006_announce_deliveries.sql");
    assert!(sql.contains("CREATE TABLE IF NOT EXISTS announce_deliveries"));
    assert!(sql.contains("reorg_from INTEGER,"));
    assert!(sql.contains("ALTER TABLE sync_state ADD COLUMN claimed_at TEXT;"));
    let upper = sql.to_ascii_uppercase();
    assert!(!upper.contains("DROP ") && !upper.contains("RENAME "));
    assert_eq!(
        upper.matches("ALTER ").count(),
        1,
        "one ADD COLUMN, no other ALTER"
    );
}

/// The round-2 statements are the four above and nothing else, and every one
/// touches only the two columns migration 0005 adds (plus the announce row's
/// own three), so the pre-0005 statements are untouched.
#[test]
fn the_round_2_statements_are_the_four_new_literals() {
    assert_eq!(ANNOUNCED_TIP_SQL, ROUND2_ANNOUNCED_TIP);
    assert_eq!(SET_ANNOUNCE_FAILURES_SQL, ROUND2_SET_ANNOUNCE_FAILURES);
    assert_eq!(COUNT_ANNOUNCE_STUCK_SQL, ROUND2_COUNT_ANNOUNCE_STUCK);
    assert_eq!(SQL_ANNOUNCE_COUNTERS, ROUND2_ANNOUNCE_COUNTERS);
    for s in ROUND2_VOCABULARY {
        assert!(!MAIN_VOCABULARY.contains(&s), "{s}");
    }
}

/// Migration 0005 is additive: two ADD COLUMNs with defaults, no DROP, no
/// rewrite of an existing column; applying it twice is what wrangler's ledger
/// prevents, and a fresh database takes every migration in order.
#[test]
fn the_0005_migration_is_additive() {
    let sql = include_str!("../migrations/0005_sync_state_announce_counters.sql");
    assert!(sql.contains(
        "ALTER TABLE sync_state ADD COLUMN announce_failures INTEGER NOT NULL DEFAULT 0"
    ));
    assert!(sql.contains(
        "ALTER TABLE sync_state ADD COLUMN tip_announce_stuck_total INTEGER NOT NULL DEFAULT 0"
    ));
    assert!(!sql.to_ascii_uppercase().contains("DROP "));
    assert!(!sql.to_ascii_uppercase().contains("RENAME "));
}

/// The header reads: the SELECT list and every shape built on it.
#[test]
fn the_header_reads_are_the_literals_main_ran() {
    assert_eq!(SELECT_HEADER, MAIN_SELECT_HEADER);
    assert_eq!(sql_chain_tip(), MAIN_CHAIN_TIP);
    assert_eq!(
        sql_active_header_for_height(),
        MAIN_ACTIVE_HEADER_FOR_HEIGHT
    );
    assert_eq!(sql_header_for_hash(), MAIN_HEADER_FOR_HASH);
    assert_eq!(sql_active_header_for_hash(), MAIN_ACTIVE_HEADER_FOR_HASH);
    assert_eq!(sql_highest_active_header(), MAIN_HIGHEST_ACTIVE_HEADER);
    assert_eq!(sql_header_for_id(), MAIN_HEADER_FOR_ID);
    assert_eq!(sql_active_headers_from_to(), MAIN_ACTIVE_HEADERS_FROM_TO);
    assert_eq!(sql_active_headers_between(), MAIN_ACTIVE_HEADERS_BETWEEN);
    assert_eq!(SQL_COUNT_ACTIVE_ABOVE, MAIN_COUNT_ACTIVE_ABOVE);
    assert_eq!(SQL_COUNT_HEADERS, MAIN_COUNT_HEADERS);
    assert_eq!(SQL_SYNC_FRESHNESS, MAIN_SYNC_FRESHNESS);
}

/// The header writes: the insert, the tip moves, the reorg's deactivate and
/// activate, the orphan relink, the work repair, the operator canonicalize.
/// (The two dual-active self-heals main ran were retired by #33; see
/// `the_ct33_statements_are_the_new_literals`.)
#[test]
fn the_header_writes_are_the_literals_main_ran() {
    assert_eq!(SQL_INSERT_HEADER, MAIN_INSERT_HEADER);
    assert_eq!(SQL_CLEAR_CHAIN_TIP, MAIN_CLEAR_CHAIN_TIP);
    assert_eq!(SQL_SET_CHAIN_TIP_ACTIVE, MAIN_SET_CHAIN_TIP_ACTIVE);
    assert_eq!(SQL_SET_CHAIN_TIP, MAIN_SET_CHAIN_TIP);
    assert_eq!(SQL_DEACTIVATE_ABOVE, MAIN_DEACTIVATE_ABOVE);
    assert_eq!(SQL_ACTIVATE_HASH, MAIN_ACTIVATE_HASH);
    assert_eq!(SQL_RELINK_HEADER, MAIN_RELINK_HEADER);
    assert_eq!(SQL_SET_CHAIN_WORK, MAIN_SET_CHAIN_WORK);
    assert_eq!(SQL_CANONICALIZE_HEIGHT, MAIN_CANONICALIZE_HEIGHT);
}

/// The sync_state statements: the announce decision, the pending-reorg
/// record / read / CAS-clear. (`update_sync_state`'s bulk progress write is
/// dead code on main and never executed, so it stays inline and unpinned.)
#[test]
fn the_sync_state_statements_are_the_literals_main_ran() {
    // round 4: the claim gained `claimed_at`; the delta from main's literal is
    // exactly that one column, nothing else.
    assert_eq!(TIP_ANNOUNCE_SQL, ROUND4_TIP_ANNOUNCE);
    assert_eq!(
        TIP_ANNOUNCE_SQL,
        MAIN_TIP_ANNOUNCE.replacen(
            "live_sync_active = 1, ",
            "live_sync_active = 1, claimed_at = datetime('now'), ",
            1
        )
    );
    assert_ne!(TIP_ANNOUNCE_SQL, MAIN_TIP_ANNOUNCE);
    assert_eq!(RECORD_PENDING_REORG_SQL, MAIN_RECORD_PENDING_REORG);
    assert_eq!(READ_PENDING_REORG_SQL, MAIN_READ_PENDING_REORG);
    assert_eq!(CLEAR_PENDING_REORG_SQL, MAIN_CLEAR_PENDING_REORG);
}

/// The whole vocabulary, so a statement added to the worker without a golden
/// here is caught by the producer trace pin rather than slipping through.
pub(crate) const MAIN_VOCABULARY: [&str; 27] = [
    MAIN_SELECT_HEADER,
    MAIN_CHAIN_TIP,
    MAIN_ACTIVE_HEADER_FOR_HEIGHT,
    MAIN_HEADER_FOR_HASH,
    MAIN_ACTIVE_HEADER_FOR_HASH,
    MAIN_HIGHEST_ACTIVE_HEADER,
    MAIN_HEADER_FOR_ID,
    MAIN_ACTIVE_HEADERS_FROM_TO,
    MAIN_ACTIVE_HEADERS_BETWEEN,
    MAIN_COUNT_ACTIVE_ABOVE,
    MAIN_COUNT_HEADERS,
    MAIN_SYNC_FRESHNESS,
    MAIN_INSERT_HEADER,
    MAIN_CLEAR_CHAIN_TIP,
    MAIN_SET_CHAIN_TIP_ACTIVE,
    MAIN_SET_CHAIN_TIP,
    MAIN_DEACTIVATE_ABOVE,
    MAIN_ACTIVATE_HASH,
    MAIN_RECORD_PENDING_REORG,
    MAIN_RELINK_HEADER,
    MAIN_SET_CHAIN_WORK,
    MAIN_CANONICALIZE_HEIGHT,
    MAIN_DEDUPE_ACTIVE_HEIGHTS,
    MAIN_DEDUPE_ACTIVE_HEIGHTS_INGEST,
    MAIN_TIP_ANNOUNCE,
    MAIN_READ_PENDING_REORG,
    MAIN_CLEAR_PENDING_REORG,
];

/// The statements new in round 2.
pub(crate) const ROUND2_VOCABULARY: [&str; 4] = [
    ROUND2_ANNOUNCED_TIP,
    ROUND2_SET_ANNOUNCE_FAILURES,
    ROUND2_COUNT_ANNOUNCE_STUCK,
    ROUND2_ANNOUNCE_COUNTERS,
];

/// The statements new in round 3.
pub(crate) const ROUND3_VOCABULARY: [&str; 3] = [
    ROUND3_RECLAIM_ANNOUNCE,
    ROUND3_READ_DELIVERIES,
    ROUND3_RECORD_DELIVERY,
];

/// The statement changed in round 4 (the claim with `claimed_at`).
pub(crate) const ROUND4_VOCABULARY: [&str; 1] = [ROUND4_TIP_ANNOUNCE];

/// The statements new in round 5 (the /getInfo probes; never on the producer path).
pub(crate) const ROUND5_VOCABULARY: [&str; 2] =
    [ROUND5_ANNOUNCE_SCHEMA_PROBE, ROUND5_COUNT_DELIVERIES];

/// bsv-low loop 10 D5 (2026-09-08), storage.rs: the courier health, read by /getInfo and by the idle cron (one read a tick).
pub(crate) const D5_COURIER_HEALTH: &str =
    "SELECT last_seen_height, last_seen_at, last_error, last_error_at FROM sync_state WHERE id = 1";

/// bsv-low loop 10 D5, sync.rs: the highest courier tip of the tick, recorded on every cron.
pub(crate) const D5_RECORD_SEEN: &str =
    "UPDATE sync_state SET last_seen_height = ?1, last_seen_at = datetime('now') WHERE id = 1";
/// bsv-low loop 10 D5, sync.rs: the last poll fault, recorded and never cleared.
pub(crate) const D5_RECORD_FAULT: &str =
    "UPDATE sync_state SET last_error = ?1, last_error_at = datetime('now') WHERE id = 1";
/// The statements new in D5 (the cron's two records, the /getInfo read).
pub(crate) const D5_VOCABULARY: [&str; 3] = [D5_RECORD_SEEN, D5_RECORD_FAULT, D5_COURIER_HEALTH];

/// The D5 statements are the three literals above and nothing else in any earlier vocabulary.
#[test]
fn the_d5_statements_are_the_three_new_literals() {
    assert_eq!(crate::storage::SQL_COURIER_HEALTH, D5_COURIER_HEALTH);
    assert_eq!(crate::sync::RECORD_SEEN_SQL, D5_RECORD_SEEN);
    assert_eq!(crate::sync::RECORD_FAULT_SQL, D5_RECORD_FAULT);
    for s in D5_VOCABULARY {
        assert!(
            !MAIN_VOCABULARY.contains(&s) && !ROUND5_VOCABULARY.contains(&s),
            "{s}"
        );
    }
}

/// P0-4 (bsv-stack-lean #35, 2026-10-08), storage.rs: the re-validation
/// state (migration 0008), read once a cron tick, by /getInfo, and by every
/// route that serves a header or a root (the ceiling).
pub(crate) const P04_VALIDATION_STATE: &str =
    "SELECT validated_height, validated_hash, validation_fault, validation_complete FROM sync_state WHERE id = 1";
/// P0-4, storage.rs: the re-validation cursor advanced.
pub(crate) const P04_SET_VALIDATED: &str =
    "UPDATE sync_state SET validated_height = ?1, validated_hash = ?2, validation_complete = ?3 WHERE id = 1";
/// P0-4, storage.rs: the re-validation stopped on a refusal.
pub(crate) const P04_SET_VALIDATION_FAULT: &str =
    "UPDATE sync_state SET validation_fault = ?1, validated_height = ?2, validated_hash = ?3, validation_complete = 0 WHERE id = 1";
/// P0-4, storage.rs: the operator's restart.
pub(crate) const P04_RESTART_VALIDATION: &str =
    "UPDATE sync_state SET validated_height = NULL, validated_hash = NULL, validation_fault = NULL, validation_complete = 0 WHERE id = 1";
/// The statements new in P0-4.
pub(crate) const P04_VOCABULARY: [&str; 4] = [
    P04_VALIDATION_STATE,
    P04_SET_VALIDATED,
    P04_SET_VALIDATION_FAULT,
    P04_RESTART_VALIDATION,
];

/// The P0-4 statements are the four literals above and nothing else in any
/// earlier vocabulary.
#[test]
fn the_p04_statements_are_the_four_new_literals() {
    assert_eq!(crate::storage::SQL_VALIDATION_STATE, P04_VALIDATION_STATE);
    assert_eq!(crate::storage::SQL_SET_VALIDATED, P04_SET_VALIDATED);
    assert_eq!(
        crate::storage::SQL_SET_VALIDATION_FAULT,
        P04_SET_VALIDATION_FAULT
    );
    assert_eq!(
        crate::storage::SQL_RESTART_VALIDATION,
        P04_RESTART_VALIDATION
    );
    for s in P04_VOCABULARY {
        assert!(
            !MAIN_VOCABULARY.contains(&s)
                && !ROUND5_VOCABULARY.contains(&s)
                && !D5_VOCABULARY.contains(&s),
            "{s}"
        );
    }
}

/// #33 (2026-10-09), storage.rs: the operator ingest sets the parent link of
/// the stored child that commits to the pushed header.
pub(crate) const CT33_LINK_CHILD: &str =
    "UPDATE headers SET previous_header_id = (SELECT header_id FROM headers WHERE hash = ?1) WHERE hash = ?2";
/// #33, storage.rs: the sweep's one read, the dual-active heights from the top.
pub(crate) const CT33_DUAL_ACTIVE_HEIGHTS: &str =
    "SELECT height FROM headers WHERE is_active = 1 GROUP BY height HAVING COUNT(*) > 1 ORDER BY height DESC LIMIT ?1";
/// #33, storage.rs: one height's sweep, the extended row and only then the newest.
pub(crate) const CT33_DEDUPE_HEIGHT: &str =
    "UPDATE headers SET is_active = 0 WHERE height = ?1 AND is_active = 1 AND header_id != COALESCE((SELECT MAX(e.header_id) FROM headers e WHERE e.height = ?1 AND e.is_active = 1 AND EXISTS (SELECT 1 FROM headers c WHERE c.height = ?1 + 1 AND c.is_active = 1 AND c.previous_hash = e.hash)), (SELECT MAX(n.header_id) FROM headers n WHERE n.height = ?1 AND n.is_active = 1))";
/// #33, storage.rs: the link check's read, the children whose parent by height is not the row they name.
pub(crate) const CT33_BROKEN_LINKS: &str =
    "SELECT c.height AS height, c.hash AS hash, c.previous_hash AS previous_hash, p.hash AS below FROM headers c LEFT JOIN headers p ON p.is_active = 1 AND p.height = c.height - 1 WHERE c.is_active = 1 AND c.height > ?1 AND c.height <= ?2 AND (p.hash IS NULL OR p.hash != c.previous_hash) ORDER BY c.height ASC LIMIT ?3";
/// #33, storage.rs: the link check's count of active rows in its range.
pub(crate) const CT33_COUNT_ACTIVE_BETWEEN: &str =
    "SELECT COUNT(*) as cnt FROM headers WHERE is_active = 1 AND height >= ?1 AND height <= ?2";
/// The statements new in #33.
pub(crate) const CT33_VOCABULARY: [&str; 5] = [
    CT33_LINK_CHILD,
    CT33_DUAL_ACTIVE_HEIGHTS,
    CT33_DEDUPE_HEIGHT,
    CT33_BROKEN_LINKS,
    CT33_COUNT_ACTIVE_BETWEEN,
];

/// The #33 statements are the literals above and nothing in any earlier vocabulary.
#[test]
fn the_ct33_statements_are_the_new_literals() {
    assert_eq!(crate::storage::SQL_LINK_CHILD, CT33_LINK_CHILD);
    assert_eq!(
        crate::storage::SQL_DUAL_ACTIVE_HEIGHTS,
        CT33_DUAL_ACTIVE_HEIGHTS
    );
    assert_eq!(crate::storage::SQL_DEDUPE_HEIGHT, CT33_DEDUPE_HEIGHT);
    assert_eq!(crate::storage::SQL_BROKEN_LINKS, CT33_BROKEN_LINKS);
    assert_eq!(
        crate::storage::SQL_COUNT_ACTIVE_BETWEEN,
        CT33_COUNT_ACTIVE_BETWEEN
    );
    for s in CT33_VOCABULARY {
        assert!(
            !MAIN_VOCABULARY.contains(&s)
                && !ROUND5_VOCABULARY.contains(&s)
                && !D5_VOCABULARY.contains(&s)
                && !P04_VOCABULARY.contains(&s),
            "{s}"
        );
    }
}

/// The round-5 probes are the two literals above and nothing else.
#[test]
fn the_round_5_statements_are_the_two_new_literals() {
    assert_eq!(SQL_ANNOUNCE_SCHEMA_PROBE, ROUND5_ANNOUNCE_SCHEMA_PROBE);
    assert_eq!(SQL_COUNT_DELIVERIES, ROUND5_COUNT_DELIVERIES);
    for s in ROUND5_VOCABULARY {
        assert!(
            !MAIN_VOCABULARY.contains(&s)
                && !ROUND2_VOCABULARY.contains(&s)
                && !ROUND3_VOCABULARY.contains(&s),
            "{s}"
        );
    }
}
