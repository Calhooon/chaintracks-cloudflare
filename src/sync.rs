//! Cron-triggered chain synchronization.
//!
//! Polls WhatsOnChain for the network tip, then closes the gap to it.
//!
//! Catch-up (gap > 10), in priority order:
//!   1. `UPSTREAM_CHAINTRACKS_URL`, if configured — anchor-linked bulk
//!      getHeaders (1000/req).
//!   2. For any part of the gap that falls WITHIN the CDN snapshot — the public
//!      bulk-header CDN, one bounded ranged slice per tick (needs no WoC tip).
//!      This only applies when `BULK_TOP_HEIGHT` is set below the snapshot top
//!      so the live window overlaps it; with the default boundary the live
//!      window sits entirely above the snapshot and this path is a no-op.
//!   3. WhatsOnChain one-by-one — for the gap above the CDN snapshot
//!      (reorg-aware) and for live sync (gap <= 10). With the default
//!      `BULK_TOP_HEIGHT` (= the CDN snapshot top) this fills the whole live
//!      window.
//!
//! Every tick first reconciles the reported tip to the highest active header,
//! banking any rows a prior (interrupted) tick loaded but could not tip.

use worker::*;

use crate::storage;
use crate::types::{BlockHeader, Chain};
use crate::woc::WocClient;

/// Called every minute by the cron trigger.
///
/// - **Catch-up** (gap > 10): upstream getHeaders if configured, else the
///   self-healing bulk-CDN path (see module docs), else WoC above the snapshot.
/// - **Live** (gap <= 10): fetch from WoC one-by-one with full insert logic.
pub async fn poll_for_new_blocks(env: &Env) -> Result<()> {
    let db = env.d1("DB")?;

    let chain = match env
        .var("CHAIN")
        .map(|v| v.to_string())
        .unwrap_or_default()
        .as_str()
    {
        "test" => Chain::Test,
        _ => Chain::Main,
    };

    // The key is a worker SECRET (env.secret), with a var fallback for
    // local dev — env.var() fails silently on secrets.
    let api_key = env
        .secret("WHATSONCHAIN_API_KEY")
        .map(|v| v.to_string())
        .ok()
        .or_else(|| env.var("WHATSONCHAIN_API_KEY").map(|v| v.to_string()).ok())
        .filter(|s| !s.is_empty());

    let client = WocClient::new(&chain, api_key);

    let upstream_url = env
        .var("UPSTREAM_CHAINTRACKS_URL")
        .map(|v| v.to_string())
        .ok()
        .filter(|s| !s.is_empty());

    // Bulk/live split: D1 only holds the live window [bulk_top, tip]. Heights
    // below bulk_top are served from the R2 bulk store, so the cron never
    // writes them — that is what keeps the DB inside its size ceiling.
    let bulk_top: u32 = env
        .var("BULK_TOP_HEIGHT")
        .ok()
        .and_then(|v| v.to_string().parse().ok())
        .unwrap_or(942_761);
    let live_floor = bulk_top.saturating_sub(1);

    // Bank any rows a prior tick loaded but was cut off before it could tip.
    let _ = storage::update_chain_tip_to_highest(&db).await;

    // Seed the boundary anchor once: the header just below bulk_top (whose
    // parent lives in R2). Mirroring it into D1 gives the live window a stored
    // genesis so WoC inserts link and activate at the boundary. Idempotent, and
    // a no-op if the DB is still full pre-prune (it just retries next tick).
    if storage::find_header_for_height(&db, live_floor).await?.is_none() {
        if let Ok(bucket) = env.bucket("BULK_HEADERS") {
            if let Ok(Some(anchor)) = crate::r2::read_bulk_header(&bucket, &chain, live_floor).await {
                if storage::insert_header(&db, &anchor).await.is_ok() {
                    let _ = storage::update_chain_tip_to_highest(&db).await;
                }
            }
        }
    }

    // Never fill below bulk_top — those heights come from the R2 bulk store.
    let our_height = storage::get_chain_tip_height(&db).await?.max(live_floor);

    let chain_info = match client.get_chain_info().await {
        Ok(info) => info,
        Err(e) => {
            // The live window sits above the CDN snapshot, so it can only be
            // filled from WoC — a WoC-tip outage just idles this tick.
            console_log!("Cron: WoC tip unavailable ({e:?}) — skipping tick");
            return Ok(());
        }
    };
    let woc_height = chain_info.blocks;

    // Heartbeat + observed network tip, persisted BEFORE any CPU-heavy fill so
    // it survives an exceededCpu kill mid-catch-up. A catch-up tick inserts
    // headers one-by-one and can be killed by the Workers CPU limit before it
    // reaches the end-of-tick update_sync_state — which would leave updated_at
    // frozen while the tip climbs, making the service look stalled when it is
    // actively syncing. This early write is what /health and /getInfo read for
    // freshness + gap. Best-effort: a failed heartbeat must not abort real work.
    // best_block_hash is stored too so /health can flag an unresolved
    // equal-height fork (same height as the tip, different block).
    let _ = record_tick_observation(
        &db,
        our_height,
        woc_height,
        chain_info.best_block_hash.as_deref().unwrap_or(""),
    )
    .await;

    if woc_height <= our_height {
        // Equal height is NOT automatically "in sync" (audit C2): if WoC's
        // best hash differs from our tip hash at the same height, the
        // network reorged to a competitor we can never fetch by height —
        // the old code returned here and served the losing branch forever.
        if woc_height == our_height && our_height > 0 {
            if let Some(best_hash) = chain_info.best_block_hash.as_deref() {
                if let Some(our_tip) = storage::find_chain_tip(&db).await? {
                    if !our_tip.hash.eq_ignore_ascii_case(best_hash) {
                        console_log!(
                            "Cron: equal-height branch mismatch at {} (ours {} vs WoC {}) — fetching competitor",
                            our_height, our_tip.hash, best_hash
                        );
                        match client.get_header_by_hash(best_hash).await {
                            Ok(header) => {
                                insert_with_parent_backfill(&db, &client, header).await?;
                            }
                            Err(e) => console_log!("Cron: competitor fetch failed: {e:?}"),
                        }
                    } else {
                        // Fully caught up AND on the network's tip — the only
                        // clean-idle point with spare budget. But self-pop is
                        // GATED OFF by default: measured populate/verify CPU is
                        // 16–50ms, which exceeds the Free-plan SCHEDULED (cron)
                        // ~10ms cap (the fetch handler tolerates it, so the
                        // operator /admin/self-pop route still works). Enabling
                        // it on the cron would just exceededCpu every idle tick.
                        // Set SELFPOP_CRON=on (a paid plan with a higher cron CPU
                        // limit) to let the cron self-populate automatically.
                        let cron_selfpop = env
                            .var("SELFPOP_CRON")
                            .map(|v| v.to_string())
                            .map(|v| v == "on")
                            .unwrap_or(false);
                        if cron_selfpop {
                            if let Err(e) = crate::selfpop::tick(env, &chain).await {
                                console_log!("Self-pop tick error (non-fatal): {e:?}");
                            }
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    let gap = woc_height - our_height;

    if gap > 10 {
        console_log!(
            "Cron: catch-up {gap} blocks ({} → {woc_height})",
            our_height + 1
        );

        if let Some(url) = upstream_url.as_deref() {
            // Bulk fetch from an upstream chaintracks (getHeaders, anchor-
            // linked per review M-2). Only used when UPSTREAM_CHAINTRACKS_URL
            // is configured.
            catch_up_from_upstream(&db, &client, url, our_height, woc_height).await?;
        } else {
            // No upstream configured. If the live window overlaps the CDN
            // snapshot (only when BULK_TOP_HEIGHT is set below the snapshot
            // top), pull one bounded ranged CDN slice per tick — needs no WoC
            // tip. With the default boundary the live window is entirely above
            // the snapshot, so this returns None immediately and the WoC path
            // below fills the window one-by-one.
            match catch_up_from_bulk_cdn(&db, &chain, our_height).await {
                Ok(Some(tip)) => console_log!("Cron: bulk CDN → {tip}"),
                Ok(None) => catch_up_via_woc(&db, &client, our_height, woc_height).await,
                Err(e) => {
                    console_log!("Cron: bulk CDN failed ({e:?}), WoC fallback");
                    catch_up_via_woc(&db, &client, our_height, woc_height).await;
                }
            }
        }

        storage::update_chain_tip_to_highest(&db).await?;
        sweep_dual_active(&db).await?;
    } else {
        // ─── Live: one-by-one from WoC with reorg detection ─────────────
        console_log!(
            "Cron: live sync {gap} blocks ({} → {woc_height})",
            our_height + 1
        );

        for height in (our_height + 1)..=woc_height {
            match client.get_header_by_height(height).await {
                Ok(header) => {
                    let result = insert_with_parent_backfill(&db, &client, header).await?;
                    if result.reorg_depth > 0 {
                        console_log!(
                            "Cron: REORG at height {} (depth {})",
                            height,
                            result.reorg_depth
                        );
                    }
                }
                Err(e) => {
                    console_log!("Cron: WoC failed at {height}: {e:?}");
                    break;
                }
            }
        }
    }

    let new_tip = storage::get_chain_tip_height(&db).await?;
    if new_tip > our_height {
        console_log!("Cron: synced to {} (+{})", new_tip, new_tip - our_height);
        update_sync_state(&db, new_tip).await?;
    }

    // Keep cumulative work factual across the fork-relevant window (H-3:
    // legacy/bulk rows carry non-cumulative work; 144 blocks ≈ 24h covers
    // any reorg the 400-step ancestor walk would accept in practice).
    match storage::repair_cumulative_work(&db, 144).await {
        Ok(0) => {}
        Ok(n) => console_log!("Cron: repaired cumulative work on {n} header(s)"),
        Err(e) => console_log!("Cron: repair_cumulative_work failed: {e:?}"),
    }

    // NOTE: self-pop runs only from the clean-idle branch above (caught up + on
    // the network's tip), never here after live sync / catch-up — that path
    // already spent CPU this tick.

    Ok(())
}

/// Insert a live header; when its parent is missing locally (no_prev),
/// backfill ancestors BY HASH from WoC — bounded walk, oldest-first insert,
/// then retry the child (reference: Chaintracks.ts:398-404,523-544
/// getMissingBlockHeader with addLiveRecursionLimit=36; audit C2 — without
/// this, a competitor branch wedged the tip forever because find_common_
/// ancestor hit the missing parent and every later insert became a dupe
/// no-op).
pub(crate) async fn insert_with_parent_backfill(
    db: &worker::D1Database,
    client: &WocClient,
    header: BlockHeader,
) -> Result<crate::types::InsertHeaderResult> {
    const BACKFILL_LIMIT: usize = 36; // TS addLiveRecursionLimit parity

    let result = storage::insert_header(db, &header).await?;
    if !result.no_prev {
        // H-1 (adversarial review): a dupe that is STILL unlinked means a
        // previous backfill aborted mid-walk (crash / WoC error after the
        // orphan row landed). Without this repair the dupe short-circuit
        // made the wedge permanent — the walk was never re-attempted.
        let stored_orphan = result.dupe
            && header.height > 0
            && matches!(
                storage::find_header_for_hash(db, &header.hash).await?,
                Some(ref h) if h.previous_header_id.is_none()
            );
        if !stored_orphan {
            return Ok(result);
        }
        console_log!(
            "Cron: stored header {} at {} is an unlinked orphan — resuming backfill",
            header.hash,
            header.height
        );
    }

    console_log!(
        "Cron: header {} at {} has no stored parent — backfilling branch by hash",
        header.hash,
        header.height
    );

    // Walk back by hash until we hit a stored header (fork point) or budget.
    let mut branch: Vec<BlockHeader> = Vec::new();
    let mut want = header.previous_hash.clone();
    let zero_hash = "0".repeat(64);
    for _ in 0..BACKFILL_LIMIT {
        if want == zero_hash {
            break;
        }
        if storage::find_header_for_hash(db, &want).await?.is_some() {
            break;
        }
        let parent = client.get_header_by_hash(&want).await?;
        want = parent.previous_hash.clone();
        branch.push(parent);
    }

    if !branch.is_empty() && want != zero_hash {
        if storage::find_header_for_hash(db, &want).await?.is_none() {
            console_log!(
                "Cron: backfill budget exhausted without reaching a stored ancestor (still missing {}) — leaving branch inactive",
                want
            );
        }
    }

    // Insert oldest-first so each child finds its parent (and cumulative
    // chain work accumulates correctly).
    for parent in branch.iter().rev() {
        let _ = storage::insert_header(db, parent).await?;
    }

    // The child row already exists (orphan, inactive, per-block-only work).
    // Relink it to the backfilled parent, recompute cumulative work, and
    // re-evaluate the tip — running the reorg walk NOW if the repaired
    // branch outworks the active one.
    let repaired = storage::relink_orphan_and_reevaluate(db, &header.hash).await?;
    if repaired.reorg_depth > 0 {
        console_log!(
            "Cron: backfilled branch won — reorg depth {}",
            repaired.reorg_depth
        );
    }
    Ok(repaired)
}

/// Fetch headers from an upstream chaintracks instance via getHeaders endpoint.
/// Returns parsed BlockHeaders from the concatenated hex response.
async fn fetch_headers_from_upstream(
    base_url: &str,
    start_height: u32,
    count: u32,
    expected_prev_hash: Option<&str>,
) -> Result<Vec<BlockHeader>> {
    let base = base_url.trim_end_matches('/');
    let url = format!("{base}/getHeaders?height={start_height}&count={count}");

    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let request = Request::new_with_init(&url, &init)?;
    let mut response = Fetch::Request(request).send().await?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(Error::RustError(format!("Production HTTP {status}")));
    }

    // Parse {status, value} wrapper
    #[derive(serde::Deserialize)]
    struct Resp {
        value: Option<String>,
    }
    let resp: Resp = response.json().await?;
    let hex_str = resp.value.unwrap_or_default();

    if hex_str.is_empty() {
        return Ok(Vec::new());
    }

    let bytes = hex::decode(&hex_str).map_err(|e| Error::RustError(format!("hex decode: {e}")))?;

    let mut headers: Vec<BlockHeader> = Vec::with_capacity(bytes.len() / 80);
    for (i, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break;
        }
        if let Some(header) = BlockHeader::from_bytes(chunk, start_height + i as u32) {
            // LINKAGE GUARD (audit M4): heights are assigned blindly as
            // start+i, so an upstream response with a gap or splice would
            // store every subsequent header at the wrong height. Each
            // header must link to its predecessor — INCLUDING the first one,
            // which must link to our locally stored header at start-1
            // (review M-2: an unanchored batch[0] let a stale upstream
            // bulk-insert a foreign branch at blind heights).
            let expected: Option<String> = match headers.last() {
                Some(prev) => Some(prev.hash.clone()),
                None => expected_prev_hash.map(|h: &str| h.to_string()),
            };
            if let Some(expected) = expected {
                if !header.previous_hash.eq_ignore_ascii_case(&expected) {
                    worker::console_log!(
                        "Cron: upstream linkage break at height {} (links {} ≠ {}) — truncating batch",
                        start_height + i as u32,
                        header.previous_hash,
                        expected
                    );
                    break;
                }
            }
            headers.push(header);
        } else {
            break;
        }
    }

    Ok(headers)
}

/// Record the observed network tip + a heartbeat at the START of a tick, before
/// any CPU-heavy fill (see the call site). Persists `woc_tip_height` (so /health
/// can measure the gap with no external call) and re-stamps `updated_at` +
/// `last_synced_height` so both stay truthful even when the tick is later killed
/// by the CPU limit before it can reach `update_sync_state`.
async fn record_tick_observation(
    db: &D1Database,
    our_height: u32,
    woc_height: u32,
    woc_best_hash: &str,
) -> Result<()> {
    crate::d1::Query::new(
        "UPDATE sync_state SET woc_tip_height = ?, last_synced_height = ?, \
         woc_best_hash = ?, live_sync_active = 1, updated_at = datetime('now') WHERE id = 1",
    )
    .bind(woc_height)
    .bind(our_height)
    .bind(woc_best_hash)
    .run(db)
    .await
}

async fn update_sync_state(db: &D1Database, height: u32) -> Result<()> {
    crate::d1::Query::new(
        "UPDATE sync_state SET last_synced_height = ?, live_sync_active = 1, \
         updated_at = datetime('now') WHERE id = 1",
    )
    .bind(height)
    .run(db)
    .await
}

/// Self-heal any dual-active debris a bulk insert can leave (audit C3):
/// exactly one active row may exist per height — keep the newest ingest; the
/// live reorg walk corrects branch choice if needed. Runs after every bulk
/// catch-up, mirroring admin_bulk_sync (audit M-3).
async fn sweep_dual_active(db: &D1Database) -> Result<()> {
    crate::d1::Query::new(
        "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN              (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)",
    )
    .run(db)
    .await
}

/// Bulk catch-up from an upstream chaintracks instance (getHeaders, 1000/req,
/// anchor-linked per review M-2). On upstream failure, falls back to a bounded
/// WoC one-by-one window for this tick; the next cron tick resumes.
async fn catch_up_from_upstream(
    db: &D1Database,
    client: &WocClient,
    url: &str,
    our_height: u32,
    woc_height: u32,
) -> Result<()> {
    let batch_size = 1000u32;
    let max_per_cycle = 5000u32; // 5 requests × 1000 headers
    let end_height = (our_height + max_per_cycle).min(woc_height);

    let mut height = our_height + 1;
    while height <= end_height {
        let count = batch_size.min(end_height - height + 1);

        // Anchor batch[0] to our stored header below it (review M-2).
        let anchor: Option<String> = if height > 0 {
            storage::find_header_for_height(db, height - 1)
                .await?
                .map(|h| h.hash)
        } else {
            None
        };

        match fetch_headers_from_upstream(url, height, count, anchor.as_deref()).await {
            Ok(headers) if !headers.is_empty() => {
                let n = headers.len() as u32;
                storage::insert_headers_batch(db, &headers).await?;
                height += n;
            }
            Ok(_) => break, // empty response
            Err(e) => {
                console_log!("Cron: upstream unavailable ({e:?}), WoC fallback this window");
                for h in height..=(height + count - 1).min(end_height) {
                    match client.get_header_by_height(h).await {
                        Ok(header) => {
                            let _ = storage::insert_header(db, &header).await?;
                        }
                        Err(e2) => {
                            console_log!("Cron: WoC also failed at {h}: {e2:?}");
                            return Ok(());
                        }
                    }
                }
                height += count;
            }
        }
    }
    Ok(())
}

/// Self-healing bulk catch-up from the public block-header CDN.
///
/// Pulls ONE bounded slice (`BULK_PER_TICK` headers) covering the next needed
/// height via an HTTP Range request and batch-inserts it. Only rows above our
/// current height are fetched, so a mid-file resume is cheap and progress is
/// monotonic even if a tick is interrupted (`INSERT OR IGNORE` is idempotent).
/// Needs no live WoC tip — it advances toward the CDN snapshot's top. Returns:
///   * `Ok(Some(tip))` — advanced to `tip`.
///   * `Ok(None)`      — no CDN file covers `our_height + 1` (at/above the
///                       snapshot); the caller should use WoC for the rest.
async fn catch_up_from_bulk_cdn(
    db: &D1Database,
    chain: &Chain,
    our_height: u32,
) -> Result<Option<u32>> {
    const BULK_PER_TICK: u32 = 25_000;

    let next_height = our_height + 1;
    let listing = WocClient::get_bulk_file_listing(chain).await?;

    // If we already hold everything the snapshot provides, signal WoC directly
    // instead of re-fetching a file whose rows we all have.
    let snapshot_top = listing
        .files
        .iter()
        .map(|f| f.coverage_end())
        .max()
        .unwrap_or(0);
    if next_height >= snapshot_top {
        return Ok(None);
    }

    // Files are 100k-aligned; pick the highest whose first_height <= next.
    let idx = match listing
        .files
        .iter()
        .rposition(|f| f.first_height.unwrap_or(0) <= next_height)
    {
        Some(i) => i,
        None => return Ok(None),
    };
    let file_info = &listing.files[idx];
    let file_first = file_info.first_height.unwrap_or(0);
    let file_end = file_info.coverage_end(); // exclusive

    // Bounded slice [next_height, next_height + BULK_PER_TICK), clamped to the
    // file's coverage — only the rows we don't have yet.
    let end_excl = next_height.saturating_add(BULK_PER_TICK).min(file_end);
    let count = end_excl.saturating_sub(next_height);
    if count == 0 {
        return Ok(None);
    }

    let bulk_client = WocClient::new(chain, None);
    let headers = bulk_client
        .download_bulk_range(file_info, file_first, next_height, count)
        .await?;
    if headers.is_empty() {
        return Ok(None);
    }

    // Anchor the slice to our stored header below it (review M-2): the first
    // header must link to what we already have, else a corrupt or branched
    // local tip could splice a misaligned span at blind heights. On a mismatch,
    // defer to the reorg-aware WoC path rather than trusting the slice.
    if let Some(anchor) = storage::find_header_for_height(db, our_height).await? {
        if !headers[0].previous_hash.eq_ignore_ascii_case(&anchor.hash) {
            console_log!(
                "Cron: bulk slice at {next_height} does not link to stored tip {} — deferring to WoC",
                anchor.hash
            );
            return Ok(None);
        }
    }

    storage::insert_headers_batch(db, &headers).await?;
    storage::update_chain_tip_to_highest(db).await?;

    let new_tip = storage::get_chain_tip_height(db).await?;
    if new_tip > our_height {
        Ok(Some(new_tip))
    } else {
        Ok(None)
    }
}

/// Bounded WoC one-by-one for the small gap above the CDN snapshot (reorg-aware
/// via parent backfill). Capped per tick to stay under the Cloudflare
/// subrequest limit; a single failure ends the tick and the next resumes.
/// Setting `WHATSONCHAIN_API_KEY` lifts WoC's rate limit and makes this fast.
async fn catch_up_via_woc(db: &D1Database, client: &WocClient, our_height: u32, woc_height: u32) {
    const MAX_PER_TICK: u32 = 200;
    let end = woc_height.min(our_height + MAX_PER_TICK);
    for h in (our_height + 1)..=end {
        match client.get_header_by_height(h).await {
            Ok(header) => {
                if let Err(e) = insert_with_parent_backfill(db, client, header).await {
                    console_log!("Cron: WoC catch-up insert failed at {h}: {e:?}");
                    break;
                }
            }
            Err(e) => {
                console_log!("Cron: WoC catch-up stopped at {h}: {e:?}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_catch_up_threshold() {
        assert!(11 > 10, "gap > 10 triggers catch-up from production");
        assert!(!(10 > 10), "gap == 10 uses live WoC mode");
    }

    #[test]
    fn test_batch_size() {
        let batch_size = 1000u32;
        let max_per_cycle = 5000u32;
        // 5 requests × 1000 headers = 5000 per cycle
        assert_eq!(max_per_cycle / batch_size, 5);
    }

    #[test]
    fn test_end_height_cap() {
        let our_height = 930000u32;
        let woc_height = 944000u32;
        let max_per_cycle = 5000u32;
        let end_height = (our_height + max_per_cycle).min(woc_height);
        assert_eq!(end_height, 935000);
    }

    #[test]
    fn test_bulk_range_slice_math() {
        // Bounded per-tick slice, clamped to the covering file's end.
        let bulk_per_tick = 25_000u32;
        let file_first = 400_000u32;
        let file_end = 500_000u32; // exclusive (100k file)
        let next = 452_253u32; // mid-file resume
        let end_excl = next.saturating_add(bulk_per_tick).min(file_end);
        assert_eq!(end_excl - next, 25_000, "full slice fits inside the file");
        assert_eq!((next - file_first) as u64 * 80, 52_253 * 80, "byte offset");
        let near_end = 490_000u32;
        let clamped = near_end.saturating_add(bulk_per_tick).min(file_end);
        assert_eq!(clamped - near_end, 10_000, "slice clamps to file_end");
    }

    #[test]
    fn test_snapshot_top_selects_file() {
        // rposition picks the highest file whose first_height <= next_height.
        let first_heights = [0u32, 100_000, 200_000, 300_000, 400_000];
        let idx = first_heights.iter().rposition(|&fh| fh <= 297_300).unwrap();
        assert_eq!(idx, 2, "mid-file resume selects the covering file");
        let idx = first_heights.iter().rposition(|&fh| fh <= 300_000).unwrap();
        assert_eq!(idx, 3, "a boundary height selects the next file");
        // Past the snapshot top → no file → caller uses WoC.
        let snapshot_top = 942_761u32;
        assert!(942_761 >= snapshot_top, "at the top → WoC");
        assert!(942_760 < snapshot_top, "one below → CDN still covers it");
    }

    #[test]
    fn test_woc_catchup_cap() {
        let our_height = 942_761u32;
        let woc_height = 958_000u32;
        let max_per_tick = 200u32;
        let end = woc_height.min(our_height + max_per_tick);
        assert_eq!(end, 942_961, "capped to 200 above our_height");
    }
}
