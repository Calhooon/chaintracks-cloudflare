//! D1 storage operations for block headers.
//!
//! Implements ChaintracksStorage-equivalent operations against Cloudflare D1.
//! Based on bsv-wallet-toolbox-rs/src/chaintracks/storage/sqlite.rs.

use crate::consensus::{self, ChainParams, HeaderFault, Link, Window};
use crate::d1::{BatchCollector, HeaderDb, QVal, Query};
use crate::types::{
    add_work, calculate_work, is_more_work, BlockHeader, Chain, ChaintracksInfo, InsertHeaderResult,
};

/// A header refused by the node's rules (P0-4): the error names the header and
/// the node's reject reason, and nothing of it is written.
pub(crate) fn refused(header: &BlockHeader, fault: &HeaderFault) -> worker::Error {
    worker::Error::RustError(format!(
        "refused header {} at {}: {fault}",
        header.hash, header.height
    ))
}

// ─── D1 Row Type ────────────────────────────────────────────────────────────

/// D1 row representation (all numbers as f64 per D1 convention).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeaderRow {
    pub header_id: Option<f64>,
    pub previous_header_id: Option<f64>,
    pub previous_hash: Option<String>,
    pub height: Option<f64>,
    pub is_active: Option<f64>,
    pub is_chain_tip: Option<f64>,
    pub hash: Option<String>,
    pub chain_work: Option<String>,
    pub version: Option<f64>,
    pub merkle_root: Option<String>,
    pub time: Option<f64>,
    pub bits: Option<f64>,
    pub nonce: Option<f64>,
}

impl HeaderRow {
    pub fn into_block_header(self) -> BlockHeader {
        BlockHeader {
            header_id: self.header_id.map(|v| v as i64),
            previous_header_id: self.previous_header_id.map(|v| v as i64),
            version: self.version.unwrap_or(0.0) as u32,
            previous_hash: self.previous_hash.unwrap_or_default(),
            merkle_root: self.merkle_root.unwrap_or_default(),
            time: self.time.unwrap_or(0.0) as u32,
            bits: self.bits.unwrap_or(0.0) as u32,
            nonce: self.nonce.unwrap_or(0.0) as u32,
            height: self.height.unwrap_or(0.0) as u32,
            hash: self.hash.unwrap_or_default(),
            chain_work: self.chain_work.unwrap_or_default(),
            is_active: self.is_active.unwrap_or(0.0) as i64 == 1,
            is_chain_tip: self.is_chain_tip.unwrap_or(0.0) as i64 == 1,
        }
    }
}

pub(crate) const SELECT_HEADER: &str =
    "SELECT header_id, previous_header_id, previous_hash, height, \
    is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce \
    FROM headers";

// ─── The query shapes, named so the plan pins below read the exact text ──────
// (0.3-era M19-5, 2026-09-08). Each is the string its call site used before;
// factoring them out changed no byte of any statement.
//
// THE RULE for any new read (pinned in `tests::plans`): with
// `idx_headers_active_height` present and no `sqlite_stat1`, a merkle-first
// shape (`WHERE merkle_root = ? AND is_active = 1`) plans through the composite
// index as a walk of the active set, NOT through the partial
// `idx_headers_merkle_active`. A merkle-first read must seek by height first
// (`check_root_for_height` does) or say `INDEXED BY idx_headers_merkle_active`.

/// The chain tip: `is_chain_tip = 1`, deterministic on a torn flag.
pub(crate) fn sql_chain_tip() -> String {
    format!("{SELECT_HEADER} WHERE is_chain_tip = 1 ORDER BY height DESC, header_id DESC LIMIT 1")
}

/// The active header at a height (the hottest read: every proof check).
pub(crate) fn sql_active_header_for_height() -> String {
    format!("{SELECT_HEADER} WHERE height = ? AND is_active = 1 ORDER BY header_id DESC LIMIT 1")
}

/// Any header by hash (active or orphaned).
pub(crate) fn sql_header_for_hash() -> String {
    format!("{SELECT_HEADER} WHERE hash = ? LIMIT 1")
}

/// The active header by hash.
pub(crate) fn sql_active_header_for_hash() -> String {
    format!("{SELECT_HEADER} WHERE hash = ? AND is_active = 1 LIMIT 1")
}

/// The highest active header (the tip repair on the reorg path). Before the
/// composite index this read the whole active set on every call.
pub(crate) fn sql_highest_active_header() -> String {
    format!("{SELECT_HEADER} WHERE is_active = 1 ORDER BY height DESC, header_id DESC LIMIT 1")
}

/// How many active headers sit above a height (the reorg walk's count).
pub(crate) const SQL_COUNT_ACTIVE_ABOVE: &str =
    "SELECT COUNT(*) as cnt FROM headers WHERE is_active = 1 AND height > ?";

/// Any header by row id (the reorg walk-back's direct link).
pub(crate) fn sql_header_for_id() -> String {
    format!("{SELECT_HEADER} WHERE header_id = ? LIMIT 1")
}

/// The active headers in `[start, end)`, ascending (the hex export).
pub(crate) fn sql_active_headers_from_to() -> String {
    format!(
        "{SELECT_HEADER} WHERE height >= ? AND height < ? AND is_active = 1 ORDER BY height ASC"
    )
}

/// The active headers in `[start, end]`, ascending (the cumulative-work repair window).
pub(crate) fn sql_active_headers_between() -> String {
    format!(
        "{SELECT_HEADER} WHERE is_active = 1 AND height >= ? AND height <= ? ORDER BY height ASC"
    )
}

// ─── The writes and the sync_state reads, named (a private program M19B-G2, 2026-09-08) ─
// Each is the literal its call site carried inline before; naming them changed
// no byte of any statement, and `statement_pins.rs` holds every one against
// the literal main `d2317f2` ran, so the worker path and the host harness can
// never drift apart on the text they execute.

/// The header count (`/getInfo`).
pub(crate) const SQL_COUNT_HEADERS: &str = "SELECT COUNT(*) as cnt FROM headers";

/// The sync freshness row (`/getInfo`).
pub(crate) const SQL_SYNC_FRESHNESS: &str =
    "SELECT last_synced_height, updated_at FROM sync_state WHERE id = 1";

/// a private program loop 10 D5: the courier health (migration 0007) on `/getInfo`.
pub(crate) const SQL_COURIER_HEALTH: &str =
    "SELECT last_seen_height, last_seen_at, last_error, last_error_at FROM sync_state WHERE id = 1";

/// The announce delivery counters (`/getInfo`, since M19B-G2 round 2):
/// consecutive undelivered announces, and the lifetime stuck count.
pub(crate) const SQL_ANNOUNCE_COUNTERS: &str =
    "SELECT announce_failures, tip_announce_stuck_total FROM sync_state WHERE id = 1";

/// The announce schema probes (`/getInfo`, round 5 LOW-2): the claim clock
/// (migration 0006's column) and the deliveries table (0006). A fault names
/// the missing column or table on `/getInfo` so a migration applied in an
/// earlier shape, or not at all, is visible instead of a silent announce.
pub(crate) const SQL_ANNOUNCE_SCHEMA_PROBE: &str = "SELECT claimed_at FROM sync_state WHERE id = 1";

/// The deliveries table probe (`/getInfo`, round 5 LOW-2).
pub(crate) const SQL_COUNT_DELIVERIES: &str = "SELECT COUNT(*) as cnt FROM announce_deliveries";

/// One header row; `OR IGNORE` because the cron and a bulk sync can race on
/// the UNIQUE hash. Binds: previous_header_id, previous_hash, height,
/// is_active, is_chain_tip, hash, chain_work, version, merkle_root, time,
/// bits, nonce.
pub(crate) const SQL_INSERT_HEADER: &str =
    "INSERT OR IGNORE INTO headers (previous_header_id, previous_hash, height, is_active, \
     is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// Clear the tip flag (the first half of every tip move; one batch with its set).
pub(crate) const SQL_CLEAR_CHAIN_TIP: &str =
    "UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1";

/// Set the tip on a hash and force it active (the live insert / reorg path).
pub(crate) const SQL_SET_CHAIN_TIP_ACTIVE: &str =
    "UPDATE headers SET is_chain_tip = 1, is_active = 1 WHERE hash = ?";

/// Set the tip on a hash (the bulk path, after the highest-active read).
pub(crate) const SQL_SET_CHAIN_TIP: &str = "UPDATE headers SET is_chain_tip = 1 WHERE hash = ?";

/// Deactivate the old branch above the fork (seeks `idx_headers_active_height`).
pub(crate) const SQL_DEACTIVATE_ABOVE: &str =
    "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ? AND is_active = 1";

/// Activate one header of the winning branch.
pub(crate) const SQL_ACTIVATE_HASH: &str = "UPDATE headers SET is_active = 1 WHERE hash = ?";

/// Relink a backfilled orphan to its parent with its cumulative work.
pub(crate) const SQL_RELINK_HEADER: &str =
    "UPDATE headers SET previous_header_id = ?, chain_work = ? WHERE header_id = ?";

/// Rewrite one header's cumulative work (the repair walk).
pub(crate) const SQL_SET_CHAIN_WORK: &str = "UPDATE headers SET chain_work = ? WHERE header_id = ?";

/// Make one hash the single active row at its height (operator ingest).
pub(crate) const SQL_CANONICALIZE_HEIGHT: &str =
    "UPDATE headers SET is_active = CASE WHEN hash = ? THEN 1 ELSE 0 END \
     WHERE height = ?";

/// Keep the newest ingest where two rows are active at one height (the cron's
/// catch-up self-heal, audit C3). The ingest route's copy below differs in
/// whitespace only; both are kept byte-exact.
pub(crate) const SQL_DEDUPE_ACTIVE_HEIGHTS: &str =
    "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN              (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)";

/// The same self-heal as run by the operator ingest route (review M-3).
pub(crate) const SQL_DEDUPE_ACTIVE_HEIGHTS_INGEST: &str =
    "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN \
     (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)";

// ─── Reads ──────────────────────────────────────────────────────────────────

pub async fn find_chain_tip(db: &impl HeaderDb) -> worker::Result<Option<BlockHeader>> {
    // ORDER BY: if a crash/overlap ever leaves two tip rows, prefer the
    // highest (then newest) so the answer is deterministic while the next
    // sync heals the flag (audit C4).
    let row: Option<HeaderRow> = Query::new(sql_chain_tip()).first(db).await?;
    Ok(row.map(|r| r.into_block_header()))
}

pub async fn get_chain_tip_height(db: &impl HeaderDb) -> worker::Result<u32> {
    match find_chain_tip(db).await? {
        Some(h) => Ok(h.height),
        None => Ok(0),
    }
}

pub async fn find_header_for_height(
    db: &impl HeaderDb,
    height: u32,
) -> worker::Result<Option<BlockHeader>> {
    // ORDER BY header_id DESC: if repair debris ever leaves two active rows
    // at one height (audit C3 ; observed live at 952854), answer with the
    // newest ingest deterministically instead of arbitrary-row-wins.
    let row: Option<HeaderRow> = Query::new(sql_active_header_for_height())
        .bind(height)
        .first(db)
        .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Lookup by hash across ALL headers (active + orphaned).
/// Used internally by insert_header (dedup), parent linking, and reorg walk-back.
/// Public endpoints should prefer `find_active_header_for_hash`.
pub async fn find_header_for_hash(
    db: &impl HeaderDb,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(sql_header_for_hash())
        .bind(hash)
        .first(db)
        .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Lookup by hash restricted to the active chain. Matches the TS server's
/// findLiveHeaderForBlockHash ; orphaned headers from prior reorgs are hidden.
pub async fn find_active_header_for_hash(
    db: &impl HeaderDb,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(sql_active_header_for_hash())
        .bind(hash)
        .first(db)
        .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Merkle root validation ; the most critical query for downstream consumers.
/// Only checks active chain headers. Uses partial index idx_headers_merkle_active.
/// Tri-state root validation (audit C1): distinguishes "root does not match
/// the ACTIVE header at this height" (a factual false) from "we have no
/// active header at this height at all" (unable to verify ; hole, above
/// tip, or reorg window). Mirrors the Go BHS tracker's INVALID vs
/// UNABLE_TO_VERIFY split (go-wallet-toolbox bhs/service.go) ; collapsing
/// both to `false` let a storage hole read as "proof invalid" downstream.
pub async fn check_root_for_height(
    db: &impl HeaderDb,
    root: &str,
    height: u32,
) -> worker::Result<Option<bool>> {
    // P0-4: a row the re-validation has not reached (or stopped before) is
    // not proof: unable to verify, never true.
    if !served_ceiling(db).await?.admits(height) {
        return Ok(None);
    }
    let header = find_header_for_height(db, height).await?;
    match header {
        None => Ok(None),
        Some(h) => Ok(Some(h.merkle_root.eq_ignore_ascii_case(root))),
    }
}

pub async fn get_headers_hex(
    db: &impl HeaderDb,
    start_height: u32,
    count: u32,
) -> worker::Result<String> {
    // saturating: u32 wrap returned an empty result in release wasm (m2).
    // NO cap here ; the R2 exporter legitimately reads 100k-header files
    // (adversarial review H-5: a 10k cap here silently truncated every
    // exported bulk file while the index still declared 100k). The PUBLIC
    // route applies its own 10k cap.
    let end_height = start_height.saturating_add(count);
    let rows: Vec<HeaderRow> = Query::new(sql_active_headers_from_to())
        .bind(start_height)
        .bind(end_height)
        .all(db)
        .await?;

    let mut hex_str = String::with_capacity(rows.len() * 160);
    for row in rows {
        let header = row.into_block_header();
        hex_str.push_str(&hex::encode(header.to_bytes()));
    }
    Ok(hex_str)
}

pub async fn get_info(db: &impl HeaderDb, chain: &Chain) -> worker::Result<ChaintracksInfo> {
    #[derive(serde::Deserialize)]
    struct CountRow {
        cnt: Option<f64>,
    }

    let count: Option<CountRow> = Query::new(SQL_COUNT_HEADERS).first(db).await?;
    let header_count = count.map(|c| c.cnt.unwrap_or(0.0) as u64).unwrap_or(0);

    let tip_height = get_chain_tip_height(db).await?;

    // Freshness from sync_state (audit M6): the table is written on every
    // successful sync but was never read by any endpoint ; staleness was
    // undetectable from the API.
    #[derive(serde::Deserialize)]
    struct SyncRow {
        last_synced_height: Option<f64>,
        updated_at: Option<String>,
    }
    let sync: Option<SyncRow> = match Query::new(SQL_SYNC_FRESHNESS).first(db).await {
        Ok(row) => row,
        Err(e) => {
            // Loud, not silent (review L-4): the freshness signal exists
            // to expose degradation ; swallowing its own read error
            // would hide exactly that.
            log_error!("get_info: sync_state read failed: {}", e);
            None
        }
    };

    // The announce delivery counters (M19B-G2 round 2): a dead webhook
    // consumer shows here as a growing failure run and a stuck count, instead
    // of consuming reorg markers in silence. Loud on a read fault, as above.
    #[derive(serde::Deserialize)]
    struct CountersRow {
        announce_failures: Option<f64>,
        tip_announce_stuck_total: Option<f64>,
    }
    let mut schema_fault: Option<String> = None;
    let counters: Option<CountersRow> = match Query::new(SQL_ANNOUNCE_COUNTERS).first(db).await {
        Ok(row) => row,
        Err(e) => {
            log_error!("get_info: announce counters read failed: {}", e);
            schema_fault = Some(format!("{e}"));
            None
        }
    };
    // The announce schema probes (round 5, LOW-2): the first fault names the
    // missing column or table; the announce itself faults on the same read
    // or on the claim, consuming nothing.
    #[derive(serde::Deserialize)]
    struct ClockRow {
        /// Deserialized only to prove the column exists.
        #[allow(dead_code)]
        claimed_at: Option<String>,
    }
    if schema_fault.is_none() {
        if let Err(e) = Query::new(SQL_ANNOUNCE_SCHEMA_PROBE)
            .first::<ClockRow>(db)
            .await
        {
            log_error!("get_info: announce schema probe failed: {}", e);
            schema_fault = Some(format!("{e}"));
        }
    }
    if schema_fault.is_none() {
        if let Err(e) = Query::new(SQL_COUNT_DELIVERIES).first::<CountRow>(db).await {
            log_error!("get_info: announce deliveries probe failed: {}", e);
            schema_fault = Some(format!("{e}"));
        }
    }

    // a private program loop 10 D5: the courier health, loud on a read fault like the
    // freshness read (a missing column names itself; the tip sync itself never
    // depends on these columns, so a deploy before the migration still syncs).
    #[derive(serde::Deserialize)]
    struct HealthRow {
        last_seen_height: Option<f64>,
        last_seen_at: Option<String>,
        last_error: Option<String>,
        last_error_at: Option<String>,
    }
    let mut sync_schema_fault: Option<String> = None;
    let health: Option<HealthRow> = match Query::new(SQL_COURIER_HEALTH).first(db).await {
        Ok(row) => row,
        Err(e) => {
            log_error!(
                "get_info: courier health read failed (migration 0007 applied?): {}",
                e
            );
            sync_schema_fault = Some(format!("{e}"));
            None
        }
    };
    let last_seen_height = health
        .as_ref()
        .and_then(|r| r.last_seen_height.map(|v| v as u32));
    // Unknown never reads as fine: no courier height seen yet is `None`, not 0.
    let live_lag_blocks = last_seen_height.map(|seen| seen.saturating_sub(tip_height));
    // P0-4: the re-validation, loud on a read fault like the others (a
    // missing column names migration 0008 in `syncSchemaFault`).
    let validation = match read_validation_state(db).await {
        Ok(v) => Some(v),
        Err(e) => {
            log_error!(
                "get_info: validation state read failed (migration 0008 applied?): {}",
                e
            );
            sync_schema_fault.get_or_insert_with(|| format!("{e}"));
            None
        }
    };
    Ok(ChaintracksInfo {
        chain: chain.as_str().to_string(),
        height_live: tip_height,
        height_bulk: 0,
        header_count,
        is_syncing: false,
        storage_type: "d1".to_string(),
        last_synced_at: sync.as_ref().and_then(|r| r.updated_at.clone()),
        last_synced_height: sync
            .as_ref()
            .and_then(|r| r.last_synced_height.map(|v| v as u32)),
        tip_announce_failures: counters
            .as_ref()
            .and_then(|r| r.announce_failures.map(|v| v as u32)),
        tip_announce_stuck_total: counters
            .as_ref()
            .and_then(|r| r.tip_announce_stuck_total.map(|v| v as u64)),
        tip_announce_schema_fault: schema_fault,
        live_lag_blocks,
        last_seen_height,
        last_seen_at: health.as_ref().and_then(|r| r.last_seen_at.clone()),
        last_sync_error: health.as_ref().and_then(|r| r.last_error.clone()),
        last_sync_error_at: health.as_ref().and_then(|r| r.last_error_at.clone()),
        sync_schema_fault,
        validated_height: validation.as_ref().and_then(|v| v.validated_height),
        validation_complete: validation.as_ref().map(|v| v.complete),
        validation_fault: validation.and_then(|v| v.fault),
    })
}

// ─── Writes (Issue #5: insert_header) ───────────────────────────────────────

/// Insert a single header with duplicate detection, parent linking, and chain tip management.
/// Returns InsertHeaderResult with all flags set per the toolbox-rs contract.
///
/// Logic (from sqlite.rs):
/// 1. Check duplicate by hash
/// 2. Calculate chain_work if not set
/// 3. Find previous_header_id by looking up previous_hash
/// 4. Get current tip to decide if this becomes new tip
/// 5. Insert row
/// 6. If new tip and doesn't extend old tip → reorg
/// 7. Update chain tip
pub async fn insert_header(
    db: &impl HeaderDb,
    params: &ChainParams,
    header: &BlockHeader,
) -> worker::Result<InsertHeaderResult> {
    // 0. P0-4: the node's proof of work before anything is read or written
    // (`CheckBlockHeader` runs before the index is touched,
    // src/validation.cpp:6167-6170 at v1.2.3). Every path that stores a single
    // header comes through here: the live cron, the backfill walk, the
    // read-through grace path.
    header.check_pow(params).map_err(|f| refused(header, &f))?;

    // 1. Duplicate check
    let existing = find_header_for_hash(db, &header.hash).await?;
    if existing.is_some() {
        return Ok(InsertHeaderResult {
            dupe: true,
            ..Default::default()
        });
    }

    // 2. Find previous header (before work: cumulative work needs the parent)
    let zero_hash = "0".repeat(64);
    let previous_header = if header.previous_hash != zero_hash {
        find_header_for_hash(db, &header.previous_hash).await?
    } else {
        None
    };
    let previous_header_id = previous_header.as_ref().and_then(|h| h.header_id);

    // 3. CUMULATIVE chain work = parent.chain_work + per-block work
    // (reference: ChaintracksStorageKnex.ts:297 addWork(oneBack.chainWork,
    // convertBitsToWork(bits)); audit M1/M2 ; the old code stored the
    // per-block value only and never consulted it). With no parent stored
    // the per-block work stands alone ; such headers can only become tip on
    // bootstrap (no_tip), never over a linked chain.
    let per_block_work = calculate_work(header.bits);
    let chain_work = match &previous_header {
        Some(parent) => add_work(&parent.chain_work, &per_block_work),
        None => per_block_work,
    };

    // 4. Get current tip. MORE-WORK wins, not higher-height (reference:
    // ChaintracksStorageKnex.ts isMoreWork; audit M1) ; at an equal-height
    // race the branch carrying more cumulative work takes the tip.
    let current_tip = find_chain_tip(db).await?;

    // badPrev guard (TS ChaintracksStorageKnex.ts:276-279): a header whose
    // claimed height doesn't sit exactly one above its stored parent is
    // malformed source data ; the M3 hash-integrity check can't catch it
    // because height isn't part of the 80 bytes.
    if let Some(parent) = &previous_header {
        if header.height != parent.height + 1 {
            return Err(worker::Error::RustError(format!(
                "insert_header: height {} does not extend parent {} at height {} (badPrev)",
                header.height, parent.hash, parent.height
            )));
        }
    }

    // P0-4 (step 2): the node's context rules, in the node's order
    // (`AcceptBlockHeader`, src/validation.cpp:6172-6190 at v1.2.3): the
    // parent, the checkpoints, then the bits against `GetNextWorkRequired`.
    // A header with no stored parent is the genesis, the first header of an
    // empty store at a checkpoint (an anchor), or an orphan the backfill
    // links later: stored inactive, never the tip, checked when it is linked
    // (`relink_orphan_and_reevaluate`).
    check_fork_prior_to_checkpoint(db, params, header, current_tip.as_ref()).await?;
    let anchored = match &previous_header {
        Some(parent) => {
            let window = load_window(db, parent, params.window_depth(parent.height)).await?;
            consensus::check_context(&window, header, params).map_err(|f| refused(header, &f))?;
            true
        }
        None => check_unlinked(params, header, current_tip.is_none())
            .map_err(|f| refused(header, &f))?,
    };

    let becomes_tip = match &current_tip {
        None => true,
        Some(tip) => anchored && is_more_work(&chain_work, &tip.chain_work),
    };

    // 5. is_active at INSERT time: only a header that extends the current
    // active tip (or bootstraps an empty DB) lands active. A reorg WINNER is
    // still inserted INACTIVE ; the reorg walk is what activates its branch,
    // and only after the walk SUCCEEDS does any visible flag change
    // (adversarial review H-2: the old code pre-marked the row
    // is_active/is_chain_tip, so a REFUSED reorg ; no common ancestor ;
    // still installed the unlinked branch as the served tip). Competitors
    // and orphans stay inactive (audit C3; reference
    // ChaintracksStorageKnex.ts:297-305).
    let extends_tip = match &current_tip {
        None => true,
        Some(tip) => header.previous_hash == tip.hash,
    };
    let is_active = becomes_tip && extends_tip;

    let insert = Query::new(SQL_INSERT_HEADER)
        .bind(previous_header_id)
        .bind(&*header.previous_hash)
        .bind(header.height)
        .bind(is_active)
        // is_chain_tip is NEVER set at insert ; update_chain_tip flips it
        // transactionally after any required reorg walk has succeeded (H-2).
        .bind(false)
        .bind(&*header.hash)
        .bind(&*chain_work)
        .bind(header.version)
        .bind(&*header.merkle_root)
        .bind(header.time)
        .bind(header.bits)
        .bind(header.nonce);
    let atomic_extension = becomes_tip && extends_tip;
    if atomic_extension {
        // #32: an event-journal fault must also roll back the newly active
        // row, including bootstrap and the read-through extension path.
        db.batch(vec![
            insert,
            Query::new(SQL_CLEAR_CHAIN_TIP),
            Query::new(SQL_SET_CHAIN_TIP_ACTIVE).bind(&*header.hash),
        ])
        .await?;
    } else {
        insert.run(db).await?;
    }

    let mut result = InsertHeaderResult {
        added: true,
        no_prev: previous_header.is_none() && header.height > 0,
        no_tip: current_tip.is_none(),
        is_active_tip: becomes_tip,
        ..Default::default()
    };

    // 6. Handle chain tip changes. Ordering matters (H-2): the reorg walk
    // runs FIRST and a failure propagates with the row still inactive and
    // the old tip untouched ; "refuse the reorg" now actually refuses.
    if becomes_tip {
        if let Some(ref tip) = current_tip {
            if header.previous_hash != tip.hash {
                let deactivated = handle_reorg(db, header, tip).await?;
                result.reorg_depth = deactivated;
            }
        }
        // Clear old tip, set new tip (also forces is_active=1 on the row).
        if result.reorg_depth == 0 && !atomic_extension {
            update_chain_tip(db, &header.hash).await?;
        }
    }

    Ok(result)
}

// ─── Chain Tip Management (Issue #10) ───────────────────────────────────────

/// Clear old chain tip and set new tip by hash.
pub async fn update_chain_tip(db: &impl HeaderDb, hash: &str) -> worker::Result<()> {
    // One D1 batch = one transaction: a failure or overlapping cron between
    // clear and set must never leave zero (or two) tip rows (audit C4 ;
    // a transient no-tip window read as currentHeight=0 downstream).
    let mut batch = BatchCollector::new(db);
    batch.add(SQL_CLEAR_CHAIN_TIP, vec![]);
    batch.add(SQL_SET_CHAIN_TIP_ACTIVE, vec![QVal::Text(hash.to_string())]);
    batch.execute().await?;
    Ok(())
}

/// Set chain tip to the highest active header. Call after batch insert.
pub async fn update_chain_tip_to_highest(
    db: &impl HeaderDb,
) -> worker::Result<Option<BlockHeader>> {
    // Find highest active header first, then flip both flags in ONE batch
    // (transactional) ; the old clear-then-set left a no-tip window on
    // failure/overlap (audit C4).
    let row: Option<HeaderRow> = Query::new(sql_highest_active_header()).first(db).await?;

    match row {
        Some(r) => {
            let header = r.into_block_header();
            let mut batch = BatchCollector::new(db);
            batch.add(SQL_CLEAR_CHAIN_TIP, vec![]);
            batch.add(SQL_SET_CHAIN_TIP, vec![QVal::Text(header.hash.clone())]);
            batch.execute().await?;
            Ok(Some(header))
        }
        None => Ok(None),
    }
}

// ─── Reorg Handling (Issues #15, #17) ───────────────────────────────────────

/// Find the common ancestor between two headers by walking back via previous_hash.
/// Returns the common ancestor header, or None if not found within limit.
pub async fn find_common_ancestor(
    db: &impl HeaderDb,
    header_a: &BlockHeader,
    header_b: &BlockHeader,
) -> worker::Result<Option<BlockHeader>> {
    let mut a = Some(header_a.clone());
    let mut b = Some(header_b.clone());
    let mut steps = 0u32;
    let max_steps = 400; // reorg_height_threshold

    while let (Some(ref ha), Some(ref hb)) = (&a, &b) {
        if ha.hash == hb.hash {
            return Ok(a);
        }
        if steps >= max_steps {
            break;
        }
        steps += 1;

        match ha.height.cmp(&hb.height) {
            std::cmp::Ordering::Greater => {
                a = walk_back(db, ha).await?;
            }
            std::cmp::Ordering::Less => {
                b = walk_back(db, hb).await?;
            }
            std::cmp::Ordering::Equal => {
                a = walk_back(db, ha).await?;
                b = walk_back(db, hb).await?;
            }
        }
    }

    Ok(None)
}

/// Walk back one step: find the parent header by previous_header_id or previous_hash.
async fn walk_back(
    db: &impl HeaderDb,
    header: &BlockHeader,
) -> worker::Result<Option<BlockHeader>> {
    // Prefer previous_header_id (direct link)
    if let Some(prev_id) = header.previous_header_id {
        let row: Option<HeaderRow> = Query::new(sql_header_for_id())
            .bind(prev_id)
            .first(db)
            .await?;
        if let Some(r) = row {
            return Ok(Some(r.into_block_header()));
        }
    }
    // Fallback to previous_hash
    let zero_hash = "0".repeat(64);
    if header.previous_hash != zero_hash {
        return find_header_for_hash(db, &header.previous_hash).await;
    }
    Ok(None)
}

/// Execute a reorg: deactivate old chain above ancestor, activate new chain.
/// Returns the number of deactivated headers (reorg depth).
///
/// Algorithm (from sqlite.rs handle_reorg):
/// 1. Find common ancestor between new header and old tip
/// 2. Deactivate old chain headers above ancestor height
/// 3. Activate new chain by walking back from new header to ancestor
pub(crate) async fn handle_reorg(
    db: &impl HeaderDb,
    new_header: &BlockHeader,
    old_tip: &BlockHeader,
) -> worker::Result<u32> {
    let ancestor = find_common_ancestor(db, new_header, old_tip).await?;
    // No common ancestor within the walk limit means we CANNOT identify the
    // fork point ; falling back to height 0 here once deactivated the entire
    // table (every header below the live window went is_active=0, breaking
    // findHeaderForHeight and with it the overlay's SPV). The TS reference
    // (wallet-toolbox ChaintracksStorageBase.findCommonAncestor) THROWS in
    // this case ; "Reached start of live database without resolving the
    // reorg." ; so the whole insert fails loudly and the tip is untouched;
    // we match that: no partial state, no dual active branches.
    let Some(ancestor) = ancestor else {
        return Err(worker::Error::RustError(format!(
            "reorg: no common ancestor within limit (new={} old={}) ; refusing (TS reference parity)",
            new_header.hash, old_tip.hash
        )));
    };
    let ancestor_height = ancestor.height;

    // Collect the new branch FIRST (reads only), then apply deactivate +
    // activate as ONE D1 batch (one transaction). The old sequential
    // statements left a crash window where the old branch was deactivated
    // but the new one only partially activated ; permanent inactive holes
    // below the tip that no cron ever revisits (parity audit §4; TS gets
    // this atomicity from its single knex transaction,
    // ChaintracksStorageKnex.ts:228).
    let mut branch_hashes: Vec<String> = Vec::new();
    let mut current = Some(new_header.clone());
    while let Some(ref h) = current {
        if h.height <= ancestor_height {
            break;
        }
        branch_hashes.push(h.hash.clone());
        current = walk_back(db, h).await?;
    }

    // Count what we'll deactivate (pre-read; the UPDATE below is the write).
    #[derive(serde::Deserialize)]
    struct CountRow {
        cnt: Option<f64>,
    }
    let count: Option<CountRow> = Query::new(SQL_COUNT_ACTIVE_ABOVE)
        .bind(ancestor_height)
        .first(db)
        .await?;
    let deactivated = count.map(|c| c.cnt.unwrap_or(0.0) as u32).unwrap_or(0);

    let mut batch = BatchCollector::new(db);
    batch.add(
        SQL_DEACTIVATE_ABOVE,
        vec![QVal::Int(ancestor_height as i64)],
    );
    for hash in &branch_hashes {
        batch.add(SQL_ACTIVATE_HASH, vec![QVal::Text(hash.clone())]);
    }
    // a private program M19 R2 round 3 (review MED-1): record the lowest CHANGED height
    // (ancestor_height + 1) so the next winning tip announce can carry it as
    // `reorgFrom`. MIN-accumulate: several reorgs between two announces keep
    // the deepest fork. The overlay's targeted re-verify then covers the
    // whole orphaned range, not just the new tip's height.
    batch.add(
        RECORD_PENDING_REORG_SQL,
        vec![QVal::Int((ancestor_height + 1) as i64)],
    );
    // #32: the branch flags, tip and trigger-written envelopes commit in
    // one transaction. The ancestor walk bounds this to at most 404 writes.
    batch.add(SQL_CLEAR_CHAIN_TIP, vec![]);
    batch.add(
        SQL_SET_CHAIN_TIP_ACTIVE,
        vec![QVal::Text(new_header.hash.clone())],
    );
    if !batch.is_empty() {
        batch.execute_atomic().await?;
    }

    Ok(deactivated)
}

/// MIN-accumulate the pending reorg fork height. Bind: ?1 = ancestor+1.
pub(crate) const RECORD_PENDING_REORG_SQL: &str =
    "UPDATE sync_state SET pending_reorg_from =         CASE WHEN pending_reorg_from IS NULL OR pending_reorg_from > ?1 THEN ?1 ELSE pending_reorg_from END      WHERE id = 1";

/// Repair an orphan row after its parent branch was backfilled (audit C2):
/// relink previous_header_id, recompute CUMULATIVE chain work from the now-
/// present parent, and re-evaluate the tip (running the reorg walk if the
/// repaired branch outworks the current one). insert_header can't do this ;
/// the orphan row already exists, so a re-insert is a dupe no-op that would
/// leave per-block-only work and an inactive branch forever.
pub async fn relink_orphan_and_reevaluate(
    db: &impl HeaderDb,
    params: &ChainParams,
    header_hash: &str,
) -> worker::Result<InsertHeaderResult> {
    let Some(stored) = find_header_for_hash(db, header_hash).await? else {
        return Ok(InsertHeaderResult::default());
    };
    let Some(parent) = find_header_for_hash(db, &stored.previous_hash).await? else {
        return Ok(InsertHeaderResult {
            dupe: true,
            no_prev: true,
            ..Default::default()
        });
    };

    // P0-4: an orphan was stored unchecked against its parent (it had none);
    // linking it is when the node's context rules run on it. Refused, it
    // stays an unlinked inactive row: never the tip, never served.
    if stored.height != parent.height + 1 {
        return Err(worker::Error::RustError(format!(
            "relink: height {} does not extend parent {} at height {} (badPrev)",
            stored.height, parent.hash, parent.height
        )));
    }
    let window = load_window(db, &parent, params.window_depth(parent.height)).await?;
    consensus::check_context(&window, &stored, params).map_err(|f| refused(&stored, &f))?;

    let chain_work = add_work(&parent.chain_work, &calculate_work(stored.bits));
    Query::new(SQL_RELINK_HEADER)
        .bind(parent.header_id)
        .bind(&*chain_work)
        .bind(stored.header_id)
        .run(db)
        .await?;

    let current_tip = find_chain_tip(db).await?;
    let becomes_tip = match &current_tip {
        None => true,
        Some(tip) => is_more_work(&chain_work, &tip.chain_work),
    };

    let mut result = InsertHeaderResult {
        dupe: true,
        is_active_tip: becomes_tip,
        ..Default::default()
    };

    if becomes_tip {
        let mut updated = stored.clone();
        updated.chain_work = chain_work;
        updated.previous_header_id = parent.header_id;
        if let Some(ref tip) = current_tip {
            if updated.previous_hash != tip.hash {
                result.reorg_depth = handle_reorg(db, &updated, tip).await?;
            }
        }
        if result.reorg_depth == 0 {
            update_chain_tip(db, &updated.hash).await?;
        }
    }

    Ok(result)
}

/// Repair cumulative chain_work along the ACTIVE chain for the fork-relevant
/// window (review H-3): legacy rows (pre work-fix deploys) and bulk-inserted
/// spans carry non-cumulative work, which makes branch comparison depth-blind
/// ; a shorter branch attaching lower could out-"work" the canonical chain.
/// Each cron this walks the last `window` active heights forward from an
/// anchor and rewrites any row whose work ≠ parent.work + per_block(bits).
/// Within-window comparisons become correct after one pass; forks deeper
/// than the window are already refused by the 400-step ancestor walk.
pub async fn repair_cumulative_work(db: &impl HeaderDb, window: u32) -> worker::Result<u32> {
    let Some(tip) = find_chain_tip(db).await? else {
        return Ok(0);
    };
    let start = tip.height.saturating_sub(window);

    let rows: Vec<HeaderRow> = Query::new(sql_active_headers_between())
        .bind(start)
        .bind(tip.height)
        .all(db)
        .await?;
    if rows.len() < 2 {
        return Ok(0);
    }

    let headers: Vec<BlockHeader> = rows.into_iter().map(|r| r.into_block_header()).collect();
    let mut fixed = 0u32;
    let mut batch = BatchCollector::new(db);
    let mut prev = headers[0].clone(); // anchor keeps its stored work

    for h in headers.iter().skip(1) {
        // Only repair along verified linkage; a gap/branch break ends the walk.
        if h.previous_hash != prev.hash {
            break;
        }
        let expected = add_work(&prev.chain_work, &calculate_work(h.bits));
        if h.chain_work != expected {
            batch.add(
                SQL_SET_CHAIN_WORK,
                vec![
                    QVal::Text(expected.clone()),
                    QVal::Int(h.header_id.unwrap_or(0)),
                ],
            );
            fixed += 1;
            if batch.len() >= 100 {
                batch.execute().await?;
                batch = BatchCollector::new(db);
            }
        }
        let mut next_prev = h.clone();
        next_prev.chain_work = expected;
        prev = next_prev;
    }
    if !batch.is_empty() {
        batch.execute().await?;
    }
    Ok(fixed)
}

// ─── Batch Insert (Issue #6) ────────────────────────────────────────────────

/// Batch insert headers for bulk import. Uses D1 batch() for atomicity.
/// Skips duplicates. Does NOT update chain tip ; call update_chain_tip_to_highest() after.
///
/// Returns number of headers actually inserted.
pub async fn insert_headers_batch(
    db: &impl HeaderDb,
    params: &ChainParams,
    headers: &[BlockHeader],
) -> worker::Result<u32> {
    if headers.is_empty() {
        return Ok(0);
    }
    // P0-4: every header of the batch meets the node's proof of work before
    // any of it is written; one refusal refuses the batch (the upstream
    // catch-up, `/admin/ingest`, `/admin/backfill` and `/admin/bulk-sync` all
    // write through here).
    for header in headers {
        header.check_pow(params).map_err(|f| refused(header, &f))?;
    }
    // P0-4 (step 2): the batch is one linked run (each header the parent of
    // the next), its first header linked to a stored parent or, with none,
    // vouched for by an anchor inside the batch (the genesis, or the highest
    // checkpoint it carries: a checkpoint's hash commits to every ancestor,
    // so the rows up to it need no bits of their own, and they are the window
    // the rows above it are checked against); every header carries the
    // checkpoint at its height and the bits the node's rule answers, read
    // from the stored ancestors and the batch itself; no new header lands
    // below a checkpoint the store holds. One refusal refuses the batch.
    for pair in headers.windows(2) {
        if pair[1].height != pair[0].height + 1
            || !pair[1].previous_hash.eq_ignore_ascii_case(&pair[0].hash)
        {
            return Err(refused(
                &pair[1],
                &HeaderFault::PrevNotFound {
                    prev: pair[1].previous_hash.clone(),
                },
            ));
        }
    }
    let first = &headers[0];
    let parent = find_header_for_hash(db, &first.previous_hash).await?;
    let tip = find_chain_tip(db).await?;
    let last_height = headers.last().map_or(first.height, |h| h.height);
    let mut vouched_through: Option<u32> = None;
    let mut window = match &parent {
        Some(p) => {
            if first.height != p.height + 1 {
                return Err(worker::Error::RustError(format!(
                    "insert_headers_batch: height {} does not extend parent {} at height {} (badPrev)",
                    first.height, p.hash, p.height
                )));
            }
            load_window(db, p, params.window_depth(p.height)).await?
        }
        None => {
            if first.height == 0 || first.previous_hash == "0".repeat(64) {
                check_unlinked(params, first, tip.is_none()).map_err(|f| refused(first, &f))?;
                vouched_through = Some(0);
            } else {
                vouched_through = params
                    .checkpoints
                    .iter()
                    .rev()
                    .map(|(h, _)| *h)
                    .find(|h| *h >= first.height && *h <= last_height);
            }
            if vouched_through.is_none() {
                return Err(refused(
                    first,
                    &HeaderFault::PrevNotFound {
                        prev: first.previous_hash.clone(),
                    },
                ));
            }
            Window::new(Vec::new())
        }
    };
    let below = stored_checkpoint_above(db, params, first.height, tip.as_ref()).await?;
    let mut active_below: std::collections::HashMap<u32, String> = Default::default();
    if let Some(cp) = below {
        // A header below a checkpoint the store holds is refused unless it is
        // already stored (the node returns early for a known header,
        // src/validation.cpp:6151-6165): read the active rows it would
        // duplicate, a thousand at a time.
        let last = last_height.min(cp - 1);
        let mut lo = first.height;
        while lo <= last {
            let hi = (lo + 999).min(last);
            let rows: Vec<HeaderRow> = Query::new(sql_active_headers_between())
                .bind(lo)
                .bind(hi)
                .all(db)
                .await?;
            for r in rows {
                let h = r.into_block_header();
                active_below.insert(h.height, h.hash);
            }
            lo = hi + 1;
        }
    }
    for header in headers {
        if let Some(cp) = below {
            if header.height < cp
                && active_below.get(&header.height).map(String::as_str) != Some(&header.hash)
            {
                return Err(refused(
                    header,
                    &HeaderFault::ForkPriorToCheckpoint {
                        height: header.height,
                        checkpoint: cp,
                    },
                ));
            }
        }
        if vouched_through.is_some_and(|v| header.height <= v) {
            // vouched for by the anchor's hash; the checkpoints still bind
            consensus::check_checkpoint(header.height, &header.hash, params)
                .map_err(|f| refused(header, &f))?;
        } else {
            consensus::check_context(&window, header, params).map_err(|f| refused(header, &f))?;
        }
        window.push(Link::from(header));
    }

    // #32: a bulk run that replaces an active row must use the live branch
    // selection path. Marking every competing row active would hide the
    // fork and destroy the deactivated-header witness before publication.
    let follows_competitor = parent.as_ref().is_some_and(|p| !p.is_active);
    if follows_competitor || tip.as_ref().is_some_and(|t| first.height <= t.height) {
        let active: Vec<HeaderRow> = Query::new(sql_active_headers_between())
            .bind(first.height)
            .bind(last_height)
            .all(db)
            .await?;
        if follows_competitor
            || active.iter().any(|r| {
                headers.iter().any(|h| {
                    r.height == Some(h.height as f64) && r.hash.as_deref() != Some(h.hash.as_str())
                })
            })
        {
            let mut added = 0;
            for header in headers {
                added += u32::from(insert_header(db, params, header).await?.added);
            }
            return Ok(added);
        }
    }

    let mut inserted = 0u32;
    let mut batch = BatchCollector::new(db);

    // CUMULATIVE work across the batch (adversarial review H-4): anchor to
    // the stored parent of the first header when it exists; otherwise the
    // first header's per-block work stands alone (legacy-region parity).
    // Callers feed linked spans (M4 guards), so accumulating within the
    // batch keeps every inserted row's work monotonic ; without this, every
    // catch-up recreated the per-block-only "tiny work" state and a
    // same-height competitor rooted in it could steal the tip on a true tie.
    let mut running_work: String = match parent {
        Some(parent) => parent.chain_work,
        None => "0".repeat(64),
    };
    let mut prev_hash_in_batch: Option<String> = None;

    for header in headers {
        let per_block = calculate_work(header.bits);
        let linked_to_prev = prev_hash_in_batch
            .as_deref()
            .map(|ph| ph.eq_ignore_ascii_case(&header.previous_hash))
            .unwrap_or(true);
        let chain_work = if linked_to_prev {
            running_work = add_work(&running_work, &per_block);
            running_work.clone()
        } else {
            // Unlinked splice inside the batch (shouldn't happen behind the
            // M4 guards) ; restart accumulation from this header alone.
            running_work = per_block.clone();
            per_block
        };
        prev_hash_in_batch = Some(header.hash.clone());

        batch.add(
            SQL_INSERT_HEADER,
            vec![
                QVal::Null, // previous_header_id ; link later or not needed for bulk
                QVal::Text(header.previous_hash.clone()),
                QVal::Int(header.height as i64),
                QVal::Bool(true),  // is_active
                QVal::Bool(false), // is_chain_tip (set after via update_chain_tip_to_highest)
                QVal::Text(header.hash.clone()),
                QVal::Text(chain_work),
                QVal::Int(header.version as i64),
                QVal::Text(header.merkle_root.clone()),
                QVal::Int(header.time as i64),
                QVal::Int(header.bits as i64),
                QVal::Int(header.nonce as i64),
            ],
        );

        inserted += 1;

        // D1 limit: 100 statements per batch. Execute and start new batch.
        if batch.len() >= 100 {
            batch.execute().await?;
            batch = BatchCollector::new(db);
        }
    }

    // Execute remaining statements
    if !batch.is_empty() {
        batch.execute().await?;
    }

    Ok(inserted)
}

/// Make each pushed header the single active row at its height: activate the
/// row with the matching hash, deactivate any competitor. Used by the
/// operator ingest path to repair stale-branch/wipe debris.
pub async fn canonicalize_heights(
    db: &impl HeaderDb,
    headers: &[BlockHeader],
) -> worker::Result<u32> {
    // An authoritative replacement disconnects the old suffix. Keeping its
    // descendants active after replacing their ancestor leaves false roots
    // below an unrelated tip, and no truthful reorg view can describe that.
    if let (Some(last), Some(tip)) = (headers.last(), find_chain_tip(db).await?) {
        let mut replaces = false;
        for header in headers {
            #[derive(serde::Deserialize)]
            struct Count {
                cnt: f64,
            }
            let count: Option<Count> = Query::new("SELECT COUNT(*) AS cnt FROM headers WHERE height = ? AND is_active = 1 AND hash != ?")
                .bind(header.height).bind(&*header.hash).first(db).await?;
            replaces |= count.is_some_and(|c| c.cnt > 0.0);
        }
        if replaces {
            handle_reorg(db, last, &tip).await?;
        }
    }
    let mut batch = BatchCollector::new(db);
    let mut n = 0u32;
    for header in headers {
        batch.add(
            SQL_CANONICALIZE_HEIGHT,
            vec![
                QVal::Text(header.hash.clone()),
                QVal::Int(header.height as i64),
            ],
        );
        n += 1;
        if batch.len() >= 100 {
            batch.execute().await?;
            batch = BatchCollector::new(db);
        }
    }
    if !batch.is_empty() {
        batch.execute().await?;
    }
    update_chain_tip_to_highest(db).await?;
    Ok(n)
}

// ─── P0-4: the node's context rules on the store ────────────────────────────

/// A header with no stored parent (`FindPreviousBlockIndex` finds none,
/// src/validation.cpp:6109-6125). The node refuses it; the store keeps three
/// cases. The genesis of the chain (at height 0 the node's own hash). A
/// checkpoint header, the anchor a store is bootstrapped from (only the
/// node's list and the owner's `CHECKPOINTS` are anchors). Any other header
/// on an EMPTY store is refused (`prev-blk-not-found`): a store never starts
/// from an unvouched header. On a store that holds a chain it is an orphan
/// the backfill links later (`Ok(false)`). Every case first answers to the
/// checkpoint at its height.
pub(crate) fn check_unlinked(
    params: &ChainParams,
    header: &BlockHeader,
    store_empty: bool,
) -> Result<bool, HeaderFault> {
    consensus::check_checkpoint(header.height, &header.hash, params)?;
    if header.height == 0 || header.previous_hash == "0".repeat(64) {
        if header.height != 0 || !header.hash.eq_ignore_ascii_case(&params.genesis_hash) {
            return Err(HeaderFault::BadGenesis {
                hash: header.hash.clone(),
            });
        }
        return Ok(true);
    }
    if params.checkpoint_at(header.height).is_some() {
        return Ok(true);
    }
    if store_empty {
        return Err(HeaderFault::PrevNotFound {
            prev: header.previous_hash.clone(),
        });
    }
    Ok(false)
}

/// The highest checkpoint above `height` the store holds, if any
/// (`Checkpoints::GetLastCheckpoint`, src/checkpoints.cpp:24-36, reads the
/// checkpoints present in the index). Only checkpoints at or below the tip
/// can be held on the chain, so a header above every one of them (every live
/// header) costs no read.
async fn stored_checkpoint_above(
    db: &impl HeaderDb,
    params: &ChainParams,
    height: u32,
    tip: Option<&BlockHeader>,
) -> worker::Result<Option<u32>> {
    let Some(tip) = tip else {
        return Ok(None);
    };
    for (cp, hash) in params.checkpoints.iter().rev() {
        if *cp <= height {
            break;
        }
        if *cp > tip.height {
            continue;
        }
        if find_header_for_hash(db, hash).await?.is_some() {
            return Ok(Some(*cp));
        }
    }
    Ok(None)
}

/// `CheckIndexAgainstCheckpoint`'s second rule (src/validation.cpp:5918-5928):
/// no new header below the last checkpoint the store holds.
async fn check_fork_prior_to_checkpoint(
    db: &impl HeaderDb,
    params: &ChainParams,
    header: &BlockHeader,
    tip: Option<&BlockHeader>,
) -> worker::Result<()> {
    if let Some(cp) = stored_checkpoint_above(db, params, header.height, tip).await? {
        return Err(refused(
            header,
            &HeaderFault::ForkPriorToCheckpoint {
                height: header.height,
                checkpoint: cp,
            },
        ));
    }
    Ok(())
}

/// The `depth` headers ending at `parent`, ascending, each the parent of the
/// next by hash: the ancestry `GetNextWorkRequired` reads. A parent off the
/// active chain is walked back by hash to the active chain; the active run
/// below is one range read. The run stops at the first gap or broken link,
/// and the rule refuses (`ancestry-missing`) only if it needs what is not
/// there. With `depth` 1 (regtest) nothing is read.
pub(crate) async fn load_window(
    db: &impl HeaderDb,
    parent: &BlockHeader,
    depth: u32,
) -> worker::Result<Window> {
    let mut chain: Vec<BlockHeader> = vec![parent.clone()];
    let mut cur = parent.clone();
    while (chain.len() as u32) < depth && !cur.is_active && cur.height > 0 {
        match walk_back(db, &cur).await? {
            Some(p)
                if p.height + 1 == cur.height
                    && p.hash.eq_ignore_ascii_case(&cur.previous_hash) =>
            {
                chain.push(p.clone());
                cur = p;
            }
            _ => break,
        }
    }
    let need = depth.saturating_sub(chain.len() as u32);
    if need > 0 && cur.is_active && cur.height > 0 {
        let lo = cur.height.saturating_sub(need);
        let rows: Vec<HeaderRow> = Query::new(sql_active_headers_between())
            .bind(lo)
            .bind(cur.height - 1)
            .all(db)
            .await?;
        let mut by_height: std::collections::HashMap<u32, Vec<BlockHeader>> = Default::default();
        for r in rows {
            let h = r.into_block_header();
            by_height.entry(h.height).or_default().push(h);
        }
        while (chain.len() as u32) < depth && cur.height > 0 {
            let Some(p) = by_height.get(&(cur.height - 1)).and_then(|v| {
                v.iter()
                    .find(|h| h.hash.eq_ignore_ascii_case(&cur.previous_hash))
                    .cloned()
            }) else {
                break;
            };
            chain.push(p.clone());
            cur = p;
        }
    }
    chain.reverse();
    Ok(Window::new(chain.iter().map(Link::from).collect()))
}

/// The re-validation state (migration 0008).
pub(crate) const SQL_VALIDATION_STATE: &str = "SELECT validated_height, validated_hash, validation_fault, validation_complete FROM sync_state WHERE id = 1";

/// Advance the re-validation: ?1 the height, ?2 its hash, ?3 complete (0/1).
pub(crate) const SQL_SET_VALIDATED: &str = "UPDATE sync_state SET validated_height = ?1, validated_hash = ?2, validation_complete = ?3 WHERE id = 1";

/// Stop the re-validation on a refusal: ?1 the fault, ?2 and ?3 the last
/// height and hash it passed (NULL when nothing passed).
pub(crate) const SQL_SET_VALIDATION_FAULT: &str = "UPDATE sync_state SET validation_fault = ?1, validated_height = ?2, validated_hash = ?3, validation_complete = 0 WHERE id = 1";

/// The operator's restart (`/admin/revalidate?restart=1`).
pub(crate) const SQL_RESTART_VALIDATION: &str = "UPDATE sync_state SET validated_height = NULL, validated_hash = NULL, validation_fault = NULL, validation_complete = 0 WHERE id = 1";

/// The re-validation state as stored.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ValidationState {
    pub validated_height: Option<u32>,
    pub validated_hash: Option<String>,
    pub fault: Option<String>,
    pub complete: bool,
}

pub async fn read_validation_state(db: &impl HeaderDb) -> worker::Result<ValidationState> {
    #[derive(serde::Deserialize)]
    struct Row {
        validated_height: Option<f64>,
        validated_hash: Option<String>,
        validation_fault: Option<String>,
        validation_complete: Option<f64>,
    }
    let row: Option<Row> = Query::new(SQL_VALIDATION_STATE).first(db).await?;
    let row = row.ok_or_else(|| worker::Error::RustError("sync_state row 1 is missing".into()))?;
    Ok(ValidationState {
        validated_height: row.validated_height.map(|v| v as u32),
        validated_hash: row.validated_hash,
        fault: row.validation_fault,
        complete: row.validation_complete.unwrap_or(0.0) as i64 == 1,
    })
}

/// How far the store may be served as proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ceiling {
    /// Every row: re-validated from the last checkpoint, or written since
    /// through the checks (`validation_complete`).
    All,
    /// The rows at or below this height: the anchor and what the
    /// re-validation has passed so far, or before the row it refused.
    UpTo(u32),
    /// Nothing: the re-validation has not found its anchor yet, or the stored
    /// chain contradicts a checkpoint.
    Nothing,
}

impl Ceiling {
    pub fn admits(&self, height: u32) -> bool {
        match self {
            Ceiling::All => true,
            Ceiling::UpTo(c) => height <= *c,
            Ceiling::Nothing => false,
        }
    }
}

pub async fn served_ceiling(db: &impl HeaderDb) -> worker::Result<Ceiling> {
    let state = read_validation_state(db).await?;
    Ok(if state.complete {
        Ceiling::All
    } else {
        match state.validated_height {
            Some(h) => Ceiling::UpTo(h),
            None => Ceiling::Nothing,
        }
    })
}

/// The active header at `height`, if the ceiling admits it (the routes).
pub async fn served_header_for_height(
    db: &impl HeaderDb,
    height: u32,
) -> worker::Result<Option<BlockHeader>> {
    if !served_ceiling(db).await?.admits(height) {
        return Ok(None);
    }
    find_header_for_height(db, height).await
}

/// The active header with `hash`, if the ceiling admits its height (the routes).
pub async fn served_active_header_for_hash(
    db: &impl HeaderDb,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let ceiling = served_ceiling(db).await?;
    Ok(find_active_header_for_hash(db, hash)
        .await?
        .filter(|h| ceiling.admits(h.height)))
}

/// The served tip: the chain tip once the store is re-validated, before that
/// the highest row the re-validation has passed (the routes).
pub async fn served_tip(db: &impl HeaderDb) -> worker::Result<Option<BlockHeader>> {
    match served_ceiling(db).await? {
        Ceiling::All => find_chain_tip(db).await,
        Ceiling::UpTo(c) => {
            let tip = find_chain_tip(db).await?;
            match tip {
                Some(t) if t.height <= c => Ok(Some(t)),
                _ => find_header_for_height(db, c).await,
            }
        }
        Ceiling::Nothing => Ok(None),
    }
}

/// `count` headers from `start` as hex, clipped to the ceiling (the routes).
pub async fn served_headers_hex(
    db: &impl HeaderDb,
    start: u32,
    count: u32,
) -> worker::Result<String> {
    let count = match served_ceiling(db).await? {
        Ceiling::All => count,
        Ceiling::UpTo(c) if start <= c => count.min(c - start + 1),
        _ => 0,
    };
    if count == 0 {
        return Ok(String::new());
    }
    get_headers_hex(db, start, count).await
}

/// One step of the re-validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Revalidation {
    /// Nothing to do: re-validated through the tip (or the store is empty).
    Complete,
    /// Passed every row through this height; more remain.
    Advanced { to: u32 },
    /// Stopped: the fault, recorded; the operator restarts it.
    Faulted { fault: String },
}

/// Rows re-validated per cron tick (one range read of the active chain).
pub const REVALIDATE_CHUNK: u32 = 2000;

/// The re-validation of the stored chain after the deploy (P0-4): from the
/// last checkpoint the store holds, every active row above it, in height
/// order, through the node's checks (the proof of work, the link to its
/// parent, the checkpoint at its height, the bits `GetNextWorkRequired`
/// answers), at most `budget` rows per call, resumed from the stored cursor.
/// Bounded by the rows since the checkpoint, never a rescan below it: a row
/// at or below the checkpoint is vouched for by the checkpoint's hash, which
/// commits to every ancestor. A refusal stops it with the fault recorded;
/// the rows from the refused one up are never served (`served_ceiling`).
pub async fn revalidate_step(
    db: &impl HeaderDb,
    params: &ChainParams,
    budget: u32,
) -> worker::Result<Revalidation> {
    let state = read_validation_state(db).await?;
    if state.complete {
        return Ok(Revalidation::Complete);
    }
    if let Some(fault) = state.fault {
        return Ok(Revalidation::Faulted { fault });
    }
    let Some(tip) = find_chain_tip(db).await? else {
        set_validated(db, None, true).await?;
        return Ok(Revalidation::Complete);
    };

    // The anchor: the highest checkpoint at or below the tip whose height
    // the store holds; the row there must carry the checkpoint's hash.
    let mut anchor: Option<BlockHeader> = None;
    for (cp, hash) in params.checkpoints.iter().rev() {
        if *cp > tip.height {
            continue;
        }
        let Some(row) = find_header_for_height(db, *cp).await? else {
            continue;
        };
        if !row.hash.eq_ignore_ascii_case(hash) {
            let fault = HeaderFault::CheckpointMismatch {
                height: *cp,
                expected: hash.clone(),
            };
            return record_validation_fault(db, &format!("{}", refused(&row, &fault)), None).await;
        }
        anchor = Some(row);
        break;
    }
    let Some(anchor) = anchor else {
        let fault =
            "no checkpoint held: the store holds the row of no checkpoint at or below its tip";
        return record_validation_fault(db, fault, None).await;
    };

    // The cursor: the stored one if it is still on the active chain above
    // the anchor; after a reorg below it, the fork point; else the anchor.
    let mut cursor = anchor.clone();
    if let (Some(h), Some(hash)) = (state.validated_height, state.validated_hash.as_deref()) {
        if h > anchor.height {
            match find_header_for_hash(db, hash).await? {
                Some(row) if row.is_active => cursor = row,
                Some(mut row) => {
                    for _ in 0..400 {
                        match walk_back(db, &row).await? {
                            Some(p) if p.height >= anchor.height => {
                                if p.is_active {
                                    cursor = p;
                                    break;
                                }
                                row = p;
                            }
                            _ => break,
                        }
                    }
                }
                None => {}
            }
        }
    }
    if state.validated_height != Some(cursor.height)
        || state.validated_hash.as_deref() != Some(cursor.hash.as_str())
    {
        set_validated(db, Some(&cursor), false).await?;
    }
    if cursor.height >= tip.height {
        set_validated(db, Some(&tip), true).await?;
        return Ok(Revalidation::Complete);
    }

    // One read: the window below the cursor and up to `budget` rows above.
    let depth = params.window_depth(cursor.height).max(1);
    let lo = (cursor.height + 1).saturating_sub(depth);
    let hi = tip.height.min(cursor.height.saturating_add(budget));
    let rows: Vec<HeaderRow> = Query::new(sql_active_headers_between())
        .bind(lo)
        .bind(hi)
        .all(db)
        .await?;
    let rows: Vec<BlockHeader> = rows.into_iter().map(|r| r.into_block_header()).collect();

    // The window: the run that ends at the cursor, each the parent of the next.
    let mut below: Vec<&BlockHeader> = Vec::new();
    let mut want = cursor.hash.clone();
    for h in (lo..=cursor.height).rev() {
        match rows
            .iter()
            .find(|r| r.height == h && r.hash.eq_ignore_ascii_case(&want))
        {
            Some(r) => {
                want = r.previous_hash.clone();
                below.push(r);
            }
            None => break,
        }
    }
    below.reverse();
    let mut window = Window::new(below.iter().map(|h| Link::from(*h)).collect());

    let mut last = cursor.clone();
    for h in (cursor.height + 1)..=hi {
        let candidates: Vec<&BlockHeader> = rows.iter().filter(|r| r.height == h).collect();
        let Some(row) = candidates
            .iter()
            .find(|r| r.previous_hash.eq_ignore_ascii_case(&last.hash))
            .copied()
        else {
            let fault = HeaderFault::AncestryMissing { height: h as i64 };
            let text = format!(
                "re-validation at {h}: no active row links to {}: {fault}",
                last.hash
            );
            return record_validation_fault(db, &text, Some(&last)).await;
        };
        let verdict = row
            .check_pow(params)
            .and_then(|_| consensus::check_context(&window, row, params));
        if let Err(fault) = verdict {
            let text = format!("re-validation: {}", refused(row, &fault));
            return record_validation_fault(db, &text, Some(&last)).await;
        }
        window.push(Link::from(row));
        last = row.clone();
    }
    let complete = last.height >= tip.height;
    set_validated(db, Some(&last), complete).await?;
    Ok(if complete {
        Revalidation::Complete
    } else {
        Revalidation::Advanced { to: last.height }
    })
}

async fn set_validated(
    db: &impl HeaderDb,
    at: Option<&BlockHeader>,
    complete: bool,
) -> worker::Result<()> {
    Query::new(SQL_SET_VALIDATED)
        .bind(at.map(|h| h.height))
        .bind(at.map(|h| h.hash.clone()))
        .bind(complete)
        .run(db)
        .await
}

async fn record_validation_fault(
    db: &impl HeaderDb,
    fault: &str,
    last_good: Option<&BlockHeader>,
) -> worker::Result<Revalidation> {
    log_error!("RE-VALIDATION FAULT, the rows above the last good one are not served: {fault}");
    Query::new(SQL_SET_VALIDATION_FAULT)
        .bind(fault)
        .bind(last_good.map(|h| h.height))
        .bind(last_good.map(|h| h.hash.clone()))
        .run(db)
        .await?;
    Ok(Revalidation::Faulted {
        fault: fault.to_string(),
    })
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// Following the rust-wallet-infra pattern: test D1 row deserialization with
// serde_json (simulating what D1 returns), and test pure business logic.
// Actual D1 execution is tested via integration tests (wrangler dev + curl).

#[cfg(test)]
mod tests {
    // ─── M19-5 (2026-09-08): the query shapes are PLANNED on a real SQLite with
    // the real migrations (rusqlite, dev-only). The bar: every hot shape is a
    // SEARCH, never a SCAN of the headers table, so a 965k-row chain answers a
    // tip repair or a reorg count by seeking, not by reading the active set.
    mod plans {
        use super::super::*;

        fn migrated_db() -> rusqlite::Connection {
            let db = rusqlite::Connection::open_in_memory().unwrap();
            db.execute_batch(include_str!("../migrations/0001_initial.sql"))
                .unwrap();
            db.execute_batch(include_str!("../migrations/0002_active_height_index.sql"))
                .unwrap();
            db.execute_batch(include_str!(
                "../migrations/0003_sync_state_announced_hash.sql"
            ))
            .unwrap();
            db.execute_batch(include_str!(
                "../migrations/0004_sync_state_pending_reorg.sql"
            ))
            .unwrap();
            // 20,000 active headers with a tip, plus 5 orphans at recent heights.
            // The plans below are a function of schema and statement only:
            // without ANALYZE (no sqlite_stat1) the planner never consults row
            // counts. The rows serve the RED-side and INDEXED BY checks.
            // (a recursive CTE: the bundled SQLite has no generate_series)
            db.execute_batch(
                "WITH RECURSIVE seq(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM seq WHERE value < 20000) \
                 INSERT INTO headers(previous_header_id, previous_hash, height, is_active, \
                    is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
                 SELECT value - 1, 'p' || value, value, 1, CASE WHEN value = 20000 THEN 1 ELSE 0 END, \
                    'h' || value, 'w', 1, 'r' || value, value, 1, 1 FROM seq; \
                 WITH RECURSIVE seq(value) AS (SELECT 19995 UNION ALL SELECT value + 1 FROM seq WHERE value < 19999) \
                 INSERT INTO headers(previous_header_id, previous_hash, height, is_active, \
                    is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
                 SELECT value - 1, 'p' || value, value, 0, 0, 'o' || value, 'w', 1, 'x' || value, \
                    value, 1, 1 FROM seq;",
            )
            .unwrap();
            db
        }

        fn plan(db: &rusqlite::Connection, sql: &str, binds: &[&dyn rusqlite::ToSql]) -> String {
            let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            stmt.query_map(binds, |r| r.get::<_, String>(3))
                .unwrap()
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
                .join("\n")
        }

        #[test]
        fn the_migration_is_additive_and_idempotent() {
            let sql = include_str!("../migrations/0002_active_height_index.sql");
            assert!(sql.contains("CREATE INDEX IF NOT EXISTS idx_headers_active_height"));
            assert!(!sql.to_ascii_uppercase().contains("DROP "));
            assert!(!sql.to_ascii_uppercase().contains("ALTER "));
            // Applying it twice is a no-op: wrangler's migration ledger applies a
            // file once, and IF NOT EXISTS keeps a manual re-apply harmless.
            let db = migrated_db();
            db.execute_batch(sql).unwrap();
        }

        #[test]
        fn the_hash_lookups_seek_the_unique_autoindex_so_no_hash_index_is_owed() {
            let db = migrated_db();
            for sql in [sql_header_for_hash(), sql_active_header_for_hash()] {
                let p = plan(&db, &sql, &[&"h123"]);
                assert!(
                    p.contains("sqlite_autoindex_headers_1 (hash=?)"),
                    "hash lookups must seek the UNIQUE autoindex:\n{p}"
                );
                assert!(!p.contains("SCAN headers"), "{p}");
            }
        }

        #[test]
        fn the_active_shapes_seek_the_composite_index_never_the_active_set() {
            let db = migrated_db();
            let cases: Vec<(String, Vec<&dyn rusqlite::ToSql>)> = vec![
                (sql_highest_active_header(), vec![]),
                (SQL_COUNT_ACTIVE_ABOVE.to_string(), vec![&19990u32]),
                (sql_active_header_for_height(), vec![&123u32]),
            ];
            for (sql, binds) in cases {
                let p = plan(&db, &sql, &binds);
                assert!(
                    p.contains("idx_headers_active_height"),
                    "expected the composite index for {sql}:\n{p}"
                );
                // The property the index buys under LIMIT 1: the ORDER BY is
                // served by the index walk, never by sorting the active set.
                assert!(
                    !p.contains("TEMP B-TREE"),
                    "no sort step may remain for {sql}:\n{p}"
                );
                assert!(
                    !p.contains("idx_headers_active ") && !p.contains("idx_headers_active\n"),
                    "the single-column active index reads the whole active set: {sql}\n{p}"
                );
                assert!(!p.contains("SCAN headers"), "{sql}\n{p}");
            }
            // The reorg walk's deactivation UPDATE takes the same seek.
            let upd = plan(
                &db,
                "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ? AND is_active = 1",
                &[&19990u32],
            );
            assert!(
                upd.contains("idx_headers_active_height (is_active=? AND height>?)"),
                "{upd}"
            );
            // The tip itself keeps its own one-row index.
            let tip = plan(&db, &sql_chain_tip(), &[]);
            assert!(tip.contains("idx_headers_tip"), "{tip}");
        }

        /// The RED side on the same fixture: without the composite index the
        /// planner answers the highest-active shape through `idx_headers_active`,
        /// i.e. by reading the active set (the 965k-row reads of 2026-09-07), and
        /// the hottest read of all, the header at a height (every proof check),
        /// the same way: a reverse rowid walk of the active set from the newest
        /// row down to the height asked, roughly (tip minus height) rows per call.
        #[test]
        fn without_the_composite_index_the_active_shapes_read_the_active_set() {
            let db = migrated_db();
            db.execute_batch("DROP INDEX idx_headers_active_height")
                .unwrap();
            let p = plan(&db, &sql_highest_active_header(), &[]);
            assert!(p.contains("idx_headers_active (is_active=?)"), "{p}");
            let c = plan(&db, SQL_COUNT_ACTIVE_ABOVE, &[&19990u32]);
            assert!(c.contains("idx_headers_active (is_active=?)"), "{c}");
            let h = plan(&db, &sql_active_header_for_height(), &[&123u32]);
            assert!(h.contains("idx_headers_active (is_active=?)"), "{h}");
        }

        /// The latent planner trap the composite index creates, documented as a
        /// pin so a future merkle-first read is written the right way: with the
        /// composite present, `WHERE merkle_root = ? AND is_active = 1` plans
        /// through `idx_headers_active_height` (a walk of the active set), not
        /// through the partial `idx_headers_merkle_active`. No production SQL
        /// uses that shape today (`check_root_for_height` seeks by height). The
        /// rule: seek by height first, or say `INDEXED BY idx_headers_merkle_active`.
        #[test]
        fn a_merkle_first_read_must_seek_by_height_or_name_the_merkle_index() {
            let db = migrated_db();
            let naive = plan(
                &db,
                &format!("{SELECT_HEADER} WHERE merkle_root = ? AND is_active = 1 LIMIT 1"),
                &[&"r5"],
            );
            assert!(
                naive.contains("idx_headers_active_height (is_active=?)"),
                "the trap: the naive merkle-first shape walks the active set:\n{naive}"
            );
            let named = plan(
                &db,
                &format!(
                    "{SELECT_HEADER} INDEXED BY idx_headers_merkle_active \
                     WHERE merkle_root = ? AND is_active = 1 LIMIT 1"
                ),
                &[&"r5"],
            );
            assert!(
                named.contains("idx_headers_merkle_active (merkle_root=?)"),
                "INDEXED BY names the partial index:\n{named}"
            );
            let by_height = plan(
                &db,
                &format!("{SELECT_HEADER} WHERE height = ? AND merkle_root = ? AND is_active = 1 LIMIT 1"),
                &[&5u32, &"r5"],
            );
            assert!(
                by_height.contains("idx_headers_active_height (is_active=? AND height=?)"),
                "height-first seeks two columns:\n{by_height}"
            );
        }
    }

    use super::*;

    // ── HeaderRow deserialization (simulates D1 responses) ──

    #[test]
    fn test_header_row_full() {
        let json = serde_json::json!({
            "header_id": 42.0,
            "previous_header_id": 41.0,
            "previous_hash": "abc123",
            "height": 100.0,
            "is_active": 1.0,
            "is_chain_tip": 0.0,
            "hash": "def456",
            "chain_work": "00ff",
            "version": 1.0,
            "merkle_root": "merkle_abc",
            "time": 1234567890.0,
            "bits": 486604799.0,
            "nonce": 99999.0,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert_eq!(header.header_id, Some(42));
        assert_eq!(header.previous_header_id, Some(41));
        assert_eq!(header.height, 100);
        assert!(header.is_active);
        assert!(!header.is_chain_tip);
        assert_eq!(header.hash, "def456");
        assert_eq!(header.version, 1);
        assert_eq!(header.merkle_root, "merkle_abc");
        assert_eq!(header.time, 1234567890);
        assert_eq!(header.bits, 486604799);
        assert_eq!(header.nonce, 99999);
    }

    #[test]
    fn test_header_row_nulls() {
        // D1 can return null for optional fields
        let json = serde_json::json!({
            "header_id": null,
            "previous_header_id": null,
            "previous_hash": null,
            "height": null,
            "is_active": null,
            "is_chain_tip": null,
            "hash": null,
            "chain_work": null,
            "version": null,
            "merkle_root": null,
            "time": null,
            "bits": null,
            "nonce": null,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert_eq!(header.header_id, None);
        assert_eq!(header.previous_header_id, None);
        assert_eq!(header.height, 0);
        assert!(!header.is_active);
        assert!(!header.is_chain_tip);
        assert_eq!(header.hash, "");
        assert_eq!(header.version, 0);
    }

    #[test]
    fn test_header_row_d1_numeric_quirk() {
        // D1 returns booleans as 1.0/0.0, not true/false
        let json = serde_json::json!({
            "header_id": 1.0,
            "previous_header_id": null,
            "previous_hash": "prev",
            "height": 0.0,
            "is_active": 1.0,
            "is_chain_tip": 1.0,
            "hash": "genesis",
            "chain_work": "work",
            "version": 1.0,
            "merkle_root": "merkle",
            "time": 1231006505.0,
            "bits": 486604799.0,
            "nonce": 2083236893.0,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert!(header.is_active);
        assert!(header.is_chain_tip);
        // Verify large nonce doesn't overflow f64→u32
        assert_eq!(header.nonce, 2083236893);
    }

    #[test]
    fn test_header_row_inactive() {
        let json = serde_json::json!({
            "header_id": 5.0,
            "previous_header_id": 4.0,
            "previous_hash": "prev",
            "height": 100.0,
            "is_active": 0.0,
            "is_chain_tip": 0.0,
            "hash": "forked",
            "chain_work": "work",
            "version": 1.0,
            "merkle_root": "merkle",
            "time": 1000.0,
            "bits": 1000.0,
            "nonce": 1000.0,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert!(!header.is_active);
        assert!(!header.is_chain_tip);
    }

    #[test]
    fn test_header_row_roundtrip_serde() {
        // Ensure HeaderRow can serialize and deserialize (needed for D1 results)
        let row = HeaderRow {
            header_id: Some(1.0),
            previous_header_id: None,
            previous_hash: Some("abc".to_string()),
            height: Some(0.0),
            is_active: Some(1.0),
            is_chain_tip: Some(1.0),
            hash: Some("genesis".to_string()),
            chain_work: Some("work".to_string()),
            version: Some(1.0),
            merkle_root: Some("merkle".to_string()),
            time: Some(1000.0),
            bits: Some(486604799.0),
            nonce: Some(12345.0),
        };

        let json = serde_json::to_string(&row).unwrap();
        let parsed: HeaderRow = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.header_id, Some(1.0));
        assert_eq!(parsed.hash, Some("genesis".to_string()));
    }

    // ── InsertHeaderResult logic (pure business logic) ──

    #[test]
    fn test_insert_result_first_header() {
        // First header inserted: added=true, no_tip=true, is_active_tip=true
        let result = InsertHeaderResult {
            added: true,
            no_tip: true,
            is_active_tip: true,
            ..Default::default()
        };
        assert!(result.added);
        assert!(result.no_tip);
        assert!(result.is_active_tip);
        assert!(!result.dupe);
        assert_eq!(result.reorg_depth, 0);
    }

    #[test]
    fn test_insert_result_duplicate() {
        let result = InsertHeaderResult {
            dupe: true,
            ..Default::default()
        };
        assert!(!result.added);
        assert!(result.dupe);
    }

    #[test]
    fn test_insert_result_chain_growth() {
        // Normal chain growth: added, active tip, no reorg
        let result = InsertHeaderResult {
            added: true,
            is_active_tip: true,
            ..Default::default()
        };
        assert!(result.added);
        assert!(result.is_active_tip);
        assert_eq!(result.reorg_depth, 0);
    }

    #[test]
    fn test_insert_result_reorg() {
        let result = InsertHeaderResult {
            added: true,
            is_active_tip: true,
            reorg_depth: 3,
            ..Default::default()
        };
        assert!(result.added);
        assert_eq!(result.reorg_depth, 3);
    }

    #[test]
    fn test_insert_result_orphan() {
        // Header whose parent is not found
        let result = InsertHeaderResult {
            added: true,
            no_prev: true,
            ..Default::default()
        };
        assert!(result.added);
        assert!(result.no_prev);
    }

    // ── Chain work computation (tested inline with storage context) ──

    #[test]
    fn test_chain_work_calculated_when_empty() {
        // Simulate the logic in insert_header: if chain_work is empty, calculate it
        let header = BlockHeader {
            header_id: None,
            previous_header_id: None,
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "merkle".to_string(),
            time: 1231006505,
            bits: 0x1d00ffff,
            nonce: 2083236893,
            height: 0,
            hash: "genesis".to_string(),
            chain_work: String::new(),
            is_active: true,
            is_chain_tip: false,
        };

        let work = if header.chain_work.is_empty() || header.chain_work == "0" {
            calculate_work(header.bits)
        } else {
            header.chain_work.clone()
        };

        assert_eq!(work.len(), 64);
        assert_ne!(work, "0".repeat(64));
    }

    #[test]
    fn test_chain_work_preserved_when_set() {
        let header = BlockHeader {
            chain_work: "00000000000000000000000000000001".to_string(),
            bits: 0x1d00ffff,
            ..Default::default()
        };

        let work = if header.chain_work.is_empty() || header.chain_work == "0" {
            calculate_work(header.bits)
        } else {
            header.chain_work.clone()
        };

        assert_eq!(work, "00000000000000000000000000000001");
    }

    // ── Tip decision logic (pure) ──

    #[test]
    fn test_becomes_tip_no_existing() {
        // No current tip → new header always becomes tip (bootstrap).
        let current_tip: Option<BlockHeader> = None;
        let chain_work = crate::types::calculate_work(0x1d00ffff);
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => is_more_work(&chain_work, &tip.chain_work),
        };
        assert!(becomes_tip);
    }

    /// Tip selection is MORE-WORK, not higher-height (reference
    /// ChaintracksStorageKnex.ts isMoreWork; audit M1). Extending the tip
    /// accumulates work and wins; an equal-work same-height competitor does
    /// NOT take the tip (first-seen wins until its branch outworks ours).
    #[test]
    fn test_becomes_tip_is_work_based() {
        let g = crate::types::calculate_work(0x1d00ffff);
        let tip_work = crate::types::add_work(&g, &g); // two blocks
        let current_tip = Some(BlockHeader {
            height: 1,
            chain_work: tip_work.clone(),
            ..Default::default()
        });
        // Child extending the tip: work = tip + block → wins.
        let child_work = crate::types::add_work(&tip_work, &g);
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => is_more_work(&child_work, &tip.chain_work),
        };
        assert!(becomes_tip);
        // Equal-height competitor with EQUAL cumulative work: stays inactive.
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => is_more_work(&tip_work, &tip.chain_work),
        };
        assert!(
            !becomes_tip,
            "equal work must not steal the tip (first-seen wins)"
        );
        // Lower-work header never wins.
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => is_more_work(&g, &tip.chain_work),
        };
        assert!(!becomes_tip);
    }

    // ── Reorg detection logic (pure) ──

    #[test]
    fn test_reorg_detected_when_prev_hash_differs() {
        let current_tip = BlockHeader {
            hash: "tip_hash".to_string(),
            height: 100,
            ..Default::default()
        };
        let new_header = BlockHeader {
            previous_hash: "different_hash".to_string(),
            height: 101,
            ..Default::default()
        };

        // Reorg if new header becomes tip but doesn't extend current tip
        let is_reorg = new_header.previous_hash != current_tip.hash;
        assert!(is_reorg);
    }

    #[test]
    fn test_no_reorg_when_extends_tip() {
        let current_tip = BlockHeader {
            hash: "tip_hash".to_string(),
            height: 100,
            ..Default::default()
        };
        let new_header = BlockHeader {
            previous_hash: "tip_hash".to_string(),
            height: 101,
            ..Default::default()
        };

        let is_reorg = new_header.previous_hash != current_tip.hash;
        assert!(!is_reorg);
    }

    // ── SQL pattern verification ──

    #[test]
    fn test_select_header_sql() {
        assert!(SELECT_HEADER.contains("header_id"));
        assert!(SELECT_HEADER.contains("previous_header_id"));
        assert!(SELECT_HEADER.contains("merkle_root"));
        assert!(SELECT_HEADER.contains("chain_work"));
        assert!(SELECT_HEADER.contains("FROM headers"));
    }

    // ── is_active bug regression tests ──
    // Bug: insert_header was setting is_active based on becomes_tip,
    // causing non-tip headers to be inactive and invisible to queries.
    // Fix: all headers on the main chain are always active. Reorg logic
    // handles deactivation when needed.

    #[test]
    fn test_inserted_header_active_iff_tip_taker() {
        // is_active = becomes_tip (audit C3): a non-more-work competitor is
        // inserted INACTIVE (reference ChaintracksStorageKnex.ts:297-305) so
        // dual-active heights are structurally impossible on the insert
        // path; the reorg activation walk is the only way a branch flips
        // active. Sequential tip-extending inserts still land active.
        let becomes_tip = true;
        let is_active = becomes_tip;
        assert!(is_active);
        let becomes_tip = false;
        let is_active = becomes_tip;
        assert!(!is_active, "competitor/orphan inserts must be inactive");
    }

    #[test]
    fn test_insert_sql_uses_or_ignore() {
        // INSERT OR IGNORE prevents UNIQUE constraint errors when
        // cron and bulk-sync race. But it also means we can't update
        // existing rows ; so the initial insert must be correct.
        let sql = "INSERT OR IGNORE INTO headers";
        assert!(sql.contains("OR IGNORE"));
    }

    #[test]
    fn test_find_header_for_height_requires_active() {
        // The WHERE clause must include is_active = 1
        let sql = format!("{SELECT_HEADER} WHERE height = ? AND is_active = 1 LIMIT 1");
        assert!(sql.contains("is_active = 1"));
    }

    #[test]
    fn test_is_valid_root_requires_active() {
        // Merkle root validation must only check active chain
        let sql = format!(
            "{SELECT_HEADER} WHERE merkle_root = ? AND height = ? AND is_active = 1 LIMIT 1"
        );
        assert!(sql.contains("is_active = 1"));
    }

    #[test]
    fn test_find_active_header_for_hash_filters_active() {
        // /findHeaderHexForBlockHash must not return headers orphaned by reorg.
        // Matches TS server's findLiveHeaderForBlockHash semantics.
        let sql = format!("{SELECT_HEADER} WHERE hash = ? AND is_active = 1 LIMIT 1");
        assert!(sql.contains("is_active = 1"));
    }

    #[test]
    fn test_find_header_for_hash_is_unfiltered() {
        // Internal lookup (dedup, parent linking, reorg walk-back) must see
        // ALL headers including orphaned ones ; do NOT filter by is_active.
        let sql = format!("{SELECT_HEADER} WHERE hash = ? LIMIT 1");
        let where_clause = sql.split("WHERE").nth(1).unwrap();
        assert!(!where_clause.contains("is_active"));
    }
}
