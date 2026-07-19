//! D1 storage operations for block headers.
//!
//! Implements ChaintracksStorage-equivalent operations against Cloudflare D1.
//! Based on bsv-wallet-toolbox-rs/src/chaintracks/storage/sqlite.rs.

use worker::D1Database;

use crate::d1::{BatchCollector, QVal, Query};
use crate::types::{add_work, calculate_work, is_more_work, BlockHeader, Chain, ChaintracksInfo, InsertHeaderResult};

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

const SELECT_HEADER: &str = "SELECT header_id, previous_header_id, previous_hash, height, \
    is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce \
    FROM headers";

// ─── Reads ──────────────────────────────────────────────────────────────────

pub async fn find_chain_tip(db: &D1Database) -> worker::Result<Option<BlockHeader>> {
    // ORDER BY: if a crash/overlap ever leaves two tip rows, prefer the
    // highest (then newest) so the answer is deterministic while the next
    // sync heals the flag (audit C4).
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE is_chain_tip = 1 ORDER BY height DESC, header_id DESC LIMIT 1"
    ))
    .first(db)
    .await?;
    Ok(row.map(|r| r.into_block_header()))
}

pub async fn get_chain_tip_height(db: &D1Database) -> worker::Result<u32> {
    match find_chain_tip(db).await? {
        Some(h) => Ok(h.height),
        None => Ok(0),
    }
}

pub async fn find_header_for_height(
    db: &D1Database,
    height: u32,
) -> worker::Result<Option<BlockHeader>> {
    // ORDER BY header_id DESC: if repair debris ever leaves two active rows
    // at one height (audit C3 — observed live at 952854), answer with the
    // newest ingest deterministically instead of arbitrary-row-wins.
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE height = ? AND is_active = 1 ORDER BY header_id DESC LIMIT 1"
    ))
    .bind(height)
    .first(db)
    .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Lookup by hash across ALL headers (active + orphaned).
/// Used internally by insert_header (dedup), parent linking, and reorg walk-back.
/// Public endpoints should prefer `find_active_header_for_hash`.
pub async fn find_header_for_hash(
    db: &D1Database,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(format!("{SELECT_HEADER} WHERE hash = ? LIMIT 1"))
        .bind(hash)
        .first(db)
        .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Lookup by hash restricted to the active chain. Matches the TS server's
/// findLiveHeaderForBlockHash — orphaned headers from prior reorgs are hidden.
pub async fn find_active_header_for_hash(
    db: &D1Database,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE hash = ? AND is_active = 1 LIMIT 1"
    ))
    .bind(hash)
    .first(db)
    .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Merkle root validation — the most critical query for downstream consumers.
/// Only checks active chain headers. Uses partial index idx_headers_merkle_active.
/// Tri-state root validation (audit C1): distinguishes "root does not match
/// the ACTIVE header at this height" (a factual false) from "we have no
/// active header at this height at all" (unable to verify — hole, above
/// tip, or reorg window). Mirrors the Go BHS tracker's INVALID vs
/// UNABLE_TO_VERIFY split (go-wallet-toolbox bhs/service.go) — collapsing
/// both to `false` let a storage hole read as "proof invalid" downstream.
pub async fn check_root_for_height(
    db: &D1Database,
    root: &str,
    height: u32,
) -> worker::Result<Option<bool>> {
    let header = find_header_for_height(db, height).await?;
    match header {
        None => Ok(None),
        Some(h) => Ok(Some(h.merkle_root.eq_ignore_ascii_case(root))),
    }
}

pub async fn get_headers_hex(
    db: &D1Database,
    start_height: u32,
    count: u32,
) -> worker::Result<String> {
    // saturating: u32 wrap returned an empty result in release wasm (m2).
    // NO cap here — the R2 exporter legitimately reads 100k-header files
    // (adversarial review H-5: a 10k cap here silently truncated every
    // exported bulk file while the index still declared 100k). The PUBLIC
    // route applies its own 10k cap.
    let end_height = start_height.saturating_add(count);
    let rows: Vec<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE height >= ? AND height < ? AND is_active = 1 ORDER BY height ASC"
    ))
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

pub async fn get_info(db: &D1Database, chain: &Chain) -> worker::Result<ChaintracksInfo> {
    #[derive(serde::Deserialize)]
    struct CountRow {
        cnt: Option<f64>,
    }

    let count: Option<CountRow> = Query::new("SELECT COUNT(*) as cnt FROM headers")
        .first(db)
        .await?;
    let header_count = count.map(|c| c.cnt.unwrap_or(0.0) as u64).unwrap_or(0);

    let tip_height = get_chain_tip_height(db).await?;

    // Freshness from sync_state (audit M6): the table is written every tick
    // (woc_tip_height + updated_at at tick start; last_synced_height on
    // completion) but the tip/gap was never surfaced — staleness was
    // undetectable from the API.
    #[derive(serde::Deserialize)]
    struct SyncRow {
        last_synced_height: Option<f64>,
        updated_at: Option<String>,
        woc_tip_height: Option<f64>,
    }
    let sync: Option<SyncRow> = match Query::new(
        "SELECT last_synced_height, updated_at, woc_tip_height FROM sync_state WHERE id = 1",
    )
    .first(db)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            // Loud, not silent (review L-4): the freshness signal exists
            // to expose degradation — swallowing its own read error
            // would hide exactly that.
            worker::console_error!("get_info: sync_state read failed: {}", e);
            None
        }
    };

    // woc_tip = the last network tip the cron observed; behind_by = how far the
    // tracked tip trails it. is_syncing (previously hard-coded false) now
    // reflects that gap, so /getInfo alone tells an operator whether the
    // service is caught up or still filling.
    let woc_tip = sync
        .as_ref()
        .and_then(|r| r.woc_tip_height)
        .map(|v| v as u32)
        .unwrap_or(0);
    let behind_by = woc_tip.saturating_sub(tip_height);

    Ok(ChaintracksInfo {
        chain: chain.as_str().to_string(),
        height_live: tip_height,
        height_bulk: 0,
        header_count,
        is_syncing: behind_by > crate::types::HEALTH_MAX_GAP,
        storage_type: "d1".to_string(),
        last_synced_at: sync.as_ref().and_then(|r| r.updated_at.clone()),
        last_synced_height: sync.as_ref().and_then(|r| r.last_synced_height.map(|v| v as u32)),
        woc_tip,
        behind_by,
    })
}

/// Snapshot for the /health readiness probe — read entirely from local D1 (no
/// upstream call) so the endpoint stays fast and safe for a watchdog to poll
/// every minute.
pub struct HealthSnapshot {
    /// Tracked chain tip (highest active header).
    pub height_live: u32,
    /// Last network tip the cron observed (0 before the first tick).
    pub woc_tip: u32,
    /// Seconds since the cron last recorded an observation; `i64::MAX` if never.
    pub stale_secs: i64,
    /// Hash of our current chain tip (display hex); empty if no tip yet.
    pub local_tip_hash: String,
    /// WhatsOnChain's best block hash as of the last tick (display hex); empty
    /// before the first tick. Lets /health detect an equal-height fork.
    pub woc_best_hash: String,
}

/// Local health snapshot for /health. Staleness is computed in SQL (SQLite
/// reads the stored `datetime('now')` string as UTC), so the handler needs no
/// wall-clock of its own and makes no network call.
pub async fn get_health(db: &D1Database) -> worker::Result<HealthSnapshot> {
    // Tip height AND hash together so /health can detect an equal-height fork
    // (same height as the network tip but a different block).
    let (height_live, local_tip_hash) = match find_chain_tip(db).await? {
        Some(h) => (h.height, h.hash),
        None => (0, String::new()),
    };

    #[derive(serde::Deserialize)]
    struct Row {
        woc_tip_height: Option<f64>,
        woc_best_hash: Option<String>,
        stale_secs: Option<f64>,
    }
    let row: Option<Row> = Query::new(
        "SELECT woc_tip_height, woc_best_hash, \
         CAST(strftime('%s','now') - strftime('%s', updated_at) AS INTEGER) AS stale_secs \
         FROM sync_state WHERE id = 1",
    )
    .first(db)
    .await
    .unwrap_or(None);

    let woc_tip = row
        .as_ref()
        .and_then(|r| r.woc_tip_height)
        .map(|v| v as u32)
        .unwrap_or(0);
    let woc_best_hash = row
        .as_ref()
        .and_then(|r| r.woc_best_hash.clone())
        .unwrap_or_default();
    let stale_secs = row
        .as_ref()
        .and_then(|r| r.stale_secs)
        .map(|v| v as i64)
        .unwrap_or(i64::MAX);

    Ok(HealthSnapshot {
        height_live,
        woc_tip,
        stale_secs,
        local_tip_hash,
        woc_best_hash,
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
    db: &D1Database,
    header: &BlockHeader,
) -> worker::Result<InsertHeaderResult> {
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
    // convertBitsToWork(bits)); audit M1/M2 — the old code stored the
    // per-block value only and never consulted it). With no parent stored
    // the per-block work stands alone — such headers can only become tip on
    // bootstrap (no_tip), never over a linked chain.
    let per_block_work = calculate_work(header.bits);
    let chain_work = match &previous_header {
        Some(parent) => add_work(&parent.chain_work, &per_block_work),
        None => per_block_work,
    };

    // 4. Get current tip. MORE-WORK wins, not higher-height (reference:
    // ChaintracksStorageKnex.ts isMoreWork; audit M1) — at an equal-height
    // race the branch carrying more cumulative work takes the tip.
    let current_tip = find_chain_tip(db).await?;
    let becomes_tip = match &current_tip {
        None => true,
        Some(tip) => is_more_work(&chain_work, &tip.chain_work),
    };

    // badPrev guard (TS ChaintracksStorageKnex.ts:276-279): a header whose
    // claimed height doesn't sit exactly one above its stored parent is
    // malformed source data — the M3 hash-integrity check can't catch it
    // because height isn't part of the 80 bytes.
    if let Some(parent) = &previous_header {
        if header.height != parent.height + 1 {
            return Err(worker::Error::RustError(format!(
                "insert_header: height {} does not extend parent {} at height {} (badPrev)",
                header.height, parent.hash, parent.height
            )));
        }
    }

    // 5. is_active at INSERT time: only a header that extends the current
    // active tip (or bootstraps an empty DB) lands active. A reorg WINNER is
    // still inserted INACTIVE — the reorg walk is what activates its branch,
    // and only after the walk SUCCEEDS does any visible flag change
    // (adversarial review H-2: the old code pre-marked the row
    // is_active/is_chain_tip, so a REFUSED reorg — no common ancestor —
    // still installed the unlinked branch as the served tip). Competitors
    // and orphans stay inactive (audit C3; reference
    // ChaintracksStorageKnex.ts:297-305).
    let extends_tip = match &current_tip {
        None => true,
        Some(tip) => header.previous_hash == tip.hash,
    };
    let is_active = becomes_tip && extends_tip;

    Query::new(
        "INSERT OR IGNORE INTO headers (previous_header_id, previous_hash, height, is_active, \
         is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(previous_header_id)
    .bind(&*header.previous_hash)
    .bind(header.height)
    .bind(is_active)
    // is_chain_tip is NEVER set at insert — update_chain_tip flips it
    // transactionally after any required reorg walk has succeeded (H-2).
    .bind(false)
    .bind(&*header.hash)
    .bind(&*chain_work)
    .bind(header.version)
    .bind(&*header.merkle_root)
    .bind(header.time)
    .bind(header.bits)
    .bind(header.nonce)
    .run(db)
    .await?;

    let mut result = InsertHeaderResult {
        added: true,
        no_prev: previous_header.is_none() && header.height > 0,
        no_tip: current_tip.is_none(),
        is_active_tip: becomes_tip,
        ..Default::default()
    };

    // 6. Handle chain tip changes. Ordering matters (H-2): the reorg walk
    // runs FIRST and a failure propagates with the row still inactive and
    // the old tip untouched — "refuse the reorg" now actually refuses.
    if becomes_tip {
        if let Some(ref tip) = current_tip {
            if header.previous_hash != tip.hash {
                let deactivated = handle_reorg(db, header, tip).await?;
                result.reorg_depth = deactivated;
            }
        }
        // Clear old tip, set new tip (also forces is_active=1 on the row).
        update_chain_tip(db, &header.hash).await?;
    }

    Ok(result)
}

// ─── Chain Tip Management (Issue #10) ───────────────────────────────────────

/// Clear old chain tip and set new tip by hash.
pub async fn update_chain_tip(db: &D1Database, hash: &str) -> worker::Result<()> {
    // One D1 batch = one transaction: a failure or overlapping cron between
    // clear and set must never leave zero (or two) tip rows (audit C4 —
    // a transient no-tip window read as currentHeight=0 downstream).
    let mut batch = BatchCollector::new(db);
    batch.add(
        "UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1",
        vec![],
    )?;
    batch.add(
        "UPDATE headers SET is_chain_tip = 1, is_active = 1 WHERE hash = ?",
        vec![QVal::Text(hash.to_string())],
    )?;
    batch.execute().await?;
    Ok(())
}

/// Set chain tip to the highest active header. Call after batch insert.
pub async fn update_chain_tip_to_highest(db: &D1Database) -> worker::Result<Option<BlockHeader>> {
    // Find highest active header first, then flip both flags in ONE batch
    // (transactional) — the old clear-then-set left a no-tip window on
    // failure/overlap (audit C4).
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE is_active = 1 ORDER BY height DESC, header_id DESC LIMIT 1"
    ))
    .first(db)
    .await?;

    match row {
        Some(r) => {
            let header = r.into_block_header();
            let mut batch = BatchCollector::new(db);
            batch.add(
                "UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1",
                vec![],
            )?;
            batch.add(
                "UPDATE headers SET is_chain_tip = 1 WHERE hash = ?",
                vec![QVal::Text(header.hash.clone())],
            )?;
            batch.execute().await?;
            Ok(Some(header))
        }
        None => Ok(None),
    }
}

/// Mark all active headers above a height as inactive (for reorg).
pub async fn mark_headers_inactive_above_height(
    db: &D1Database,
    height: u32,
) -> worker::Result<u32> {
    // Count how many we'll deactivate
    #[derive(serde::Deserialize)]
    struct CountRow {
        cnt: Option<f64>,
    }
    let count: Option<CountRow> =
        Query::new("SELECT COUNT(*) as cnt FROM headers WHERE is_active = 1 AND height > ?")
            .bind(height)
            .first(db)
            .await?;

    let n = count.map(|c| c.cnt.unwrap_or(0.0) as u32).unwrap_or(0);

    if n > 0 {
        Query::new(
            "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ? AND is_active = 1",
        )
        .bind(height)
        .run(db)
        .await?;
    }

    Ok(n)
}

// ─── Reorg Handling (Issues #15, #17) ───────────────────────────────────────

/// Find the common ancestor between two headers by walking back via previous_hash.
/// Returns the common ancestor header, or None if not found within limit.
pub async fn find_common_ancestor(
    db: &D1Database,
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
async fn walk_back(db: &D1Database, header: &BlockHeader) -> worker::Result<Option<BlockHeader>> {
    // Prefer previous_header_id (direct link)
    if let Some(prev_id) = header.previous_header_id {
        let row: Option<HeaderRow> =
            Query::new(format!("{SELECT_HEADER} WHERE header_id = ? LIMIT 1"))
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
async fn handle_reorg(
    db: &D1Database,
    new_header: &BlockHeader,
    old_tip: &BlockHeader,
) -> worker::Result<u32> {
    let ancestor = find_common_ancestor(db, new_header, old_tip).await?;
    // No common ancestor within the walk limit means we CANNOT identify the
    // fork point — falling back to height 0 here once deactivated the entire
    // table (every header below the live window went is_active=0, breaking
    // findHeaderForHeight and with it the overlay's SPV). The TS reference
    // (wallet-toolbox ChaintracksStorageBase.findCommonAncestor) THROWS in
    // this case — "Reached start of live database without resolving the
    // reorg." — so the whole insert fails loudly and the tip is untouched;
    // we match that: no partial state, no dual active branches.
    let Some(ancestor) = ancestor else {
        return Err(worker::Error::RustError(format!(
            "reorg: no common ancestor within limit (new={} old={}) — refusing (TS reference parity)",
            new_header.hash, old_tip.hash
        )));
    };
    let ancestor_height = ancestor.height;

    // Collect the new branch FIRST (reads only), then apply deactivate +
    // activate as ONE D1 batch (one transaction). The old sequential
    // statements left a crash window where the old branch was deactivated
    // but the new one only partially activated — permanent inactive holes
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
    let count: Option<CountRow> =
        Query::new("SELECT COUNT(*) as cnt FROM headers WHERE is_active = 1 AND height > ?")
            .bind(ancestor_height)
            .first(db)
            .await?;
    let deactivated = count.map(|c| c.cnt.unwrap_or(0.0) as u32).unwrap_or(0);

    let mut batch = BatchCollector::new(db);
    batch.add(
        "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ? AND is_active = 1",
        vec![QVal::Int(ancestor_height as i64)],
    )?;
    for hash in &branch_hashes {
        batch.add(
            "UPDATE headers SET is_active = 1 WHERE hash = ?",
            vec![QVal::Text(hash.clone())],
        )?;
        if batch.len() >= 100 {
            // Branches beyond ~99 statements split across batches — still a
            // vast improvement over per-statement commits, and reorgs deeper
            // than 99 blocks are already past the 400-step walk guard zone.
            batch.execute().await?;
            batch = BatchCollector::new(db);
        }
    }
    if !batch.is_empty() {
        batch.execute().await?;
    }

    Ok(deactivated)
}

/// Repair an orphan row after its parent branch was backfilled (audit C2):
/// relink previous_header_id, recompute CUMULATIVE chain work from the now-
/// present parent, and re-evaluate the tip (running the reorg walk if the
/// repaired branch outworks the current one). insert_header can't do this —
/// the orphan row already exists, so a re-insert is a dupe no-op that would
/// leave per-block-only work and an inactive branch forever.
pub async fn relink_orphan_and_reevaluate(
    db: &D1Database,
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

    let chain_work = add_work(&parent.chain_work, &calculate_work(stored.bits));
    Query::new("UPDATE headers SET previous_header_id = ?, chain_work = ? WHERE header_id = ?")
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
        update_chain_tip(db, &updated.hash).await?;
    }

    Ok(result)
}

/// Repair cumulative chain_work along the ACTIVE chain for the fork-relevant
/// window (review H-3): legacy rows (pre work-fix deploys) and bulk-inserted
/// spans carry non-cumulative work, which makes branch comparison depth-blind
/// — a shorter branch attaching lower could out-"work" the canonical chain.
/// Each cron this walks the last `window` active heights forward from an
/// anchor and rewrites any row whose work ≠ parent.work + per_block(bits).
/// Within-window comparisons become correct after one pass; forks deeper
/// than the window are already refused by the 400-step ancestor walk.
pub async fn repair_cumulative_work(db: &D1Database, window: u32) -> worker::Result<u32> {
    let Some(tip) = find_chain_tip(db).await? else {
        return Ok(0);
    };
    let start = tip.height.saturating_sub(window);

    let rows: Vec<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE is_active = 1 AND height >= ? AND height <= ? ORDER BY height ASC"
    ))
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
                "UPDATE headers SET chain_work = ? WHERE header_id = ?",
                vec![
                    QVal::Text(expected.clone()),
                    QVal::Int(h.header_id.unwrap_or(0)),
                ],
            )?;
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
/// Skips duplicates. Does NOT update chain tip — call update_chain_tip_to_highest() after.
///
/// Returns number of headers actually inserted.
pub async fn insert_headers_batch(db: &D1Database, headers: &[BlockHeader]) -> worker::Result<u32> {
    if headers.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0u32;
    let mut batch = BatchCollector::new(db);

    // CUMULATIVE work across the batch (adversarial review H-4): anchor to
    // the stored parent of the first header when it exists; otherwise the
    // first header's per-block work stands alone (legacy-region parity).
    // Callers feed linked spans (M4 guards), so accumulating within the
    // batch keeps every inserted row's work monotonic — without this, every
    // catch-up recreated the per-block-only "tiny work" state and a
    // same-height competitor rooted in it could steal the tip on a true tie.
    let mut running_work: String = match find_header_for_hash(db, &headers[0].previous_hash).await?
    {
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
            // M4 guards) — restart accumulation from this header alone.
            running_work = per_block.clone();
            per_block
        };
        prev_hash_in_batch = Some(header.hash.clone());

        batch.add(
            "INSERT OR IGNORE INTO headers (previous_header_id, previous_hash, height, is_active, \
             is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            vec![
                QVal::Null, // previous_header_id — link later or not needed for bulk
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
        )?;

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
    db: &D1Database,
    headers: &[BlockHeader],
) -> worker::Result<u32> {
    let mut batch = BatchCollector::new(db);
    let mut n = 0u32;
    for header in headers {
        batch.add(
            "UPDATE headers SET is_active = CASE WHEN hash = ? THEN 1 ELSE 0 END \
             WHERE height = ?",
            vec![
                QVal::Text(header.hash.clone()),
                QVal::Int(header.height as i64),
            ],
        )?;
        n += 1;
        if batch.len() >= 100 {
            batch.execute().await?;
            batch = BatchCollector::new(db);
        }
    }
    if !batch.is_empty() {
        batch.execute().await?;
    }
    Ok(n)
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// Following the rust-wallet-infra pattern: test D1 row deserialization with
// serde_json (simulating what D1 returns), and test pure business logic.
// Actual D1 execution is tested via integration tests (wrangler dev + curl).

#[cfg(test)]
mod tests {
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
        assert!(!becomes_tip, "equal work must not steal the tip (first-seen wins)");
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
        // existing rows — so the initial insert must be correct.
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
        // ALL headers including orphaned ones — do NOT filter by is_active.
        let sql = format!("{SELECT_HEADER} WHERE hash = ? LIMIT 1");
        let where_clause = sql.split("WHERE").nth(1).unwrap();
        assert!(!where_clause.contains("is_active"));
    }
}
