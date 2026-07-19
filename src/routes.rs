//! HTTP routing for Chaintracks API.
//!
//! Mirrors the chaintracks-server ChaintracksService endpoints.
//! All responses wrapped in `{status: "success", value: T}` to match
//! the format expected by rust-wallet-infra and rust-overlay consumers.

use worker::*;

use crate::storage;
use crate::types::{BlockHeader, Chain};

/// Public block header (8 fields, matching production /findHeaderHexForHeight).
/// Omits internal tracking fields (headerId, chainWork, isActive, etc.)
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicBlockHeader {
    version: u32,
    previous_hash: String,
    merkle_root: String,
    time: u32,
    bits: u32,
    nonce: u32,
    height: u32,
    hash: String,
}

impl From<BlockHeader> for PublicBlockHeader {
    fn from(h: BlockHeader) -> Self {
        Self {
            version: h.version,
            previous_hash: h.previous_hash,
            merkle_root: h.merkle_root,
            time: h.time,
            bits: h.bits,
            nonce: h.nonce,
            height: h.height,
            hash: h.hash,
        }
    }
}

/// Standard response wrapper matching ChaintracksService format.
fn wrap_success(value: impl serde::Serialize) -> Result<Response> {
    Response::from_json(&serde_json::json!({
        "status": "success",
        "value": value
    }))
}

fn wrap_error(message: &str, status_code: u16) -> Result<Response> {
    // Code tracks the HTTP status (review L-1: everything used to say
    // ERR_NOT_FOUND, including 503 degraded-service responses).
    let code = match status_code {
        404 => "ERR_NOT_FOUND",
        400 => "ERR_BAD_REQUEST",
        401 => "ERR_UNAUTHORIZED",
        503 => "ERR_UNAVAILABLE",
        _ => "ERR_INTERNAL",
    };
    let body = serde_json::json!({
        "status": "error",
        "code": code,
        "description": message
    });
    let response = Response::from_json(&body)?;
    Ok(response.with_status(status_code))
}

pub async fn handle_request(mut req: Request, env: &Env) -> Result<Response> {
    let path = req.path();
    let method = req.method();

    // CORS preflight
    if method == Method::Options {
        return cors_preflight();
    }

    let chain = match env
        .var("CHAIN")
        .map(|v| v.to_string())
        .unwrap_or_default()
        .as_str()
    {
        "test" => Chain::Test,
        _ => Chain::Main,
    };

    let db = env.d1("DB")?;
    // ── /admin/* auth gate ──────────────────────────────────────────────
    // The admin surface can rewrite arbitrary headers and canonicalize
    // heights — with the worker URL baked into public configs it MUST be
    // token-gated. Token lives in the ADMIN_TOKEN worker secret (env.secret;
    // env.var fallback for local dev — same pattern as WHATSONCHAIN_API_KEY
    // in sync.rs). FAIL CLOSED: no secret configured ⇒ all admin calls are
    // refused (503), never open.
    if path.starts_with("/admin/") {
        let expected = env
            .secret("ADMIN_TOKEN")
            .map(|v| v.to_string())
            .ok()
            .or_else(|| env.var("ADMIN_TOKEN").map(|v| v.to_string()).ok())
            .filter(|s| !s.is_empty());
        let Some(expected) = expected else {
            return wrap_error("Admin surface disabled: no ADMIN_TOKEN configured", 503);
        };
        let presented = req
            .headers()
            .get("Authorization")
            .ok()
            .flatten()
            .and_then(|h| h.strip_prefix("Bearer ").map(|t| t.to_string()));
        // Constant-time-ish compare: length check + byte fold (no early exit).
        let authorized = presented
            .map(|p| {
                p.len() == expected.len()
                    && p.bytes()
                        .zip(expected.bytes())
                        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                        == 0
            })
            .unwrap_or(false);
        if !authorized {
            return wrap_error("Unauthorized", 401);
        }
    }

    // ── /v2 wire shim (go-chaintracks / Arcade contract) ────────────────
    // Speaks the v2 surface overlay-express-era clients use (ts-stack
    // GoChaintracksServiceClient; spec source: ts-stack conformance
    // sync/chaintracks-v2-http.json, reference chaintracks-server@1.0.2).
    // Mounted at both /v2/* and /chaintracks/v2/* . SSE tip/reorg streams
    // are NOT implemented (Workers-cron architecture has no push source;
    // v2 clients poll fine without them).
    let v2_path = path
        .strip_prefix("/chaintracks/v2")
        .or_else(|| path.strip_prefix("/v2"))
        .map(|p| p.to_string());
    if let Some(v2p) = v2_path {
        if method == Method::Get {
            return handle_v2(&db, env, &chain, &v2p, &req.url()?).await;
        }
        return v2_error("ERR_NOT_FOUND", "Not found", 404);
    }

    let response = match (method, path.as_str()) {
        // Health (plain text, no wrapper — matches production root endpoint)
        (Method::Get, "/") => health(&chain),
        // Machine-readable readiness probe for uptime watchdogs (200 / 503 + JSON).
        (Method::Get, "/health") => health_check(&db).await,

        // Info & chain
        (Method::Get, "/getChain") => wrap_success(chain.as_str()),
        (Method::Get, "/getInfo") => get_info(&db, &chain).await,
        (Method::Get, "/currentHeight") => current_height(&db).await,
        // TS ChaintracksService wire parity: the toolbox client and
        // rust-overlay probe /getPresentHeight (external chain height).
        // WoC is the truthful source; our own tip is the fallback when WoC
        // is unreachable (slightly stale is better than 404-breaking every
        // stock client).
        (Method::Get, "/getPresentHeight") => get_present_height(&db, &chain).await,

        // Chain tip
        (Method::Get, "/findChainTipHashHex") => find_chain_tip_hash(&db).await,
        (Method::Get, "/findChainTipHeaderHex") => find_chain_tip_header_hex(&db).await,

        // Header queries
        (Method::Get, "/findHeaderHexForHeight") => {
            // (read-through grace: see ensure_fresh_header)
            let url = req.url()?;
            find_header_hex_for_height(&db, env, &chain, &url).await
        }
        (Method::Get, "/findHeaderHexForBlockHash") => {
            let url = req.url()?;
            find_header_hex_for_block_hash(&db, &url).await
        }
        (Method::Get, "/getHeaders") => {
            let url = req.url()?;
            get_headers(&db, env, &url).await
        }

        // Validation
        (Method::Get, "/isValidRootForHeight") => {
            let url = req.url()?;
            is_valid_root_for_height(&db, env, &chain, &url).await
        }

        // Admin: ingest raw headers pushed by an operator (concatenated
        // 80-byte header hex in the body, heights from ?start=). For gaps
        // where in-worker WoC fetching is rate-limited — the operator
        // fetches at their own pace and pushes.
        (Method::Post, "/admin/ingest") => {
            let url = req.url()?;
            let body = req.text().await?;
            admin_ingest(&db, &url, &body).await
        }

        // Admin: backfill a below-tip header gap from WoC. The cron only
        // walks forward from the tip, so a hole under it (e.g. between the
        // stale CDN bulk files and the live window) is never revisited.
        (Method::Get, "/admin/backfill") => {
            let url = req.url()?;
            admin_backfill(&db, &chain, env, &url).await
        }

        // Admin: trigger bulk CDN sync for a single file
        (Method::Get, "/admin/bulk-sync") => {
            let url = req.url()?;
            admin_bulk_sync(&db, &chain, &url).await
        }

        // Admin: export headers from D1 to R2
        (Method::Get, "/admin/export-r2") => {
            let url = req.url()?;
            admin_export_r2(&db, env, &chain, &url).await
        }

        // Admin: populate the R2 bulk store from the public CDN (bulk/live split)
        (Method::Get, "/admin/import-cdn") => {
            let url = req.url()?;
            admin_import_cdn(env, &chain, &url).await
        }

        // Admin: run ONE self-pop unit of work now (bypasses the cron's idle
        // gate) — for testing the streaming populate / lazy verify before the
        // live window is caught up. The cron runs the same tick automatically.
        (Method::Get, "/admin/self-pop") => match crate::selfpop::tick(env, &chain).await {
            Ok(()) => wrap_success("self-pop tick ran"),
            Err(e) => wrap_error(&format!("self-pop error: {e}"), 500),
        },

        // Serve bulk header files from R2
        (Method::Get, path) if path.starts_with("/headers/") => serve_r2_file(env, path).await,

        _ => Response::error("Not Found", 404),
    };

    response.map(add_cors)
}

fn health(chain: &Chain) -> Result<Response> {
    // Root health endpoint returns plain text (matches production exactly)
    Response::ok(format!("Chaintracks {chain}Net Block Header Service"))
}

/// Readiness verdict for /health. A pure function of the three inputs so the
/// 200/503 boundary is unit-tested without a live D1 or wall-clock.
struct HealthVerdict {
    healthy: bool,
    reason: &'static str,
}

fn health_verdict(
    height_live: u32,
    woc_tip: u32,
    stale_secs: i64,
    local_tip_hash: &str,
    woc_best_hash: &str,
) -> HealthVerdict {
    // Cron heartbeat: woc_tip_height + updated_at are written at the START of
    // every tick, so a stale heartbeat means the cron itself stopped running.
    // 60s cadence → tolerate a few missed ticks before declaring it stalled.
    const MAX_STALE_SECS: i64 = 300;

    // Nothing observed yet (fresh deploy, cron has not run) — not ready.
    if woc_tip == 0 || height_live == 0 {
        return HealthVerdict {
            healthy: false,
            reason: "no-data",
        };
    }
    // Heartbeat stale ⇒ the tip has not been confirmed recently. This covers a
    // dead cron OR a sustained WhatsOnChain outage (the heartbeat is written
    // only after a successful tip fetch): both mean the tip is effectively
    // frozen, which is what a watchdog must alarm on. `secondsSinceSync` in the
    // body lets the operator tell the two apart.
    if stale_secs > MAX_STALE_SECS {
        return HealthVerdict {
            healthy: false,
            reason: "stale",
        };
    }
    let gap = woc_tip.saturating_sub(height_live);
    // Unresolved equal-height fork: we sit AT the network tip height but on a
    // DIFFERENT block. A gap-only check reads this as caught up (gap 0) while
    // SPV reads serve a losing branch — the merkleRoot at the tip is wrong.
    // sync.rs tries to switch to the network's block each tick; until it does,
    // report degraded. (Empty woc_best_hash = not yet observed → skip; that
    // only happens before the first tick, already covered by the no-data guard.
    // The check is gated on gap == 0 so normal below-tip lag, where the hashes
    // differ simply because they are at different heights, is not misflagged.)
    if gap == 0 && !woc_best_hash.is_empty() && !local_tip_hash.eq_ignore_ascii_case(woc_best_hash) {
        return HealthVerdict {
            healthy: false,
            reason: "forked",
        };
    }
    // Heartbeat fresh but the tracked tip trails the network tip by too much.
    if gap > crate::types::HEALTH_MAX_GAP {
        return HealthVerdict {
            healthy: false,
            reason: "behind",
        };
    }
    HealthVerdict {
        healthy: true,
        reason: "ok",
    }
}

/// Machine-readable liveness/readiness probe for uptime watchdogs. LOCAL-ONLY
/// (no upstream call) so it stays fast and can be polled every minute without
/// hammering WhatsOnChain. 200 when the tracked tip is close to the last
/// observed network tip AND the cron heartbeat is fresh; 503 otherwise (behind,
/// stalled cron, or no data yet). Body is JSON in both cases.
async fn health_check(db: &worker::D1Database) -> Result<Response> {
    let h = storage::get_health(db).await?;
    let v = health_verdict(
        h.height_live,
        h.woc_tip,
        h.stale_secs,
        &h.local_tip_hash,
        &h.woc_best_hash,
    );
    let body = serde_json::json!({
        "status": if v.healthy { "ok" } else { "degraded" },
        "healthy": v.healthy,
        "reason": v.reason,
        "heightLive": h.height_live,
        "wocTip": h.woc_tip,
        "behindBy": h.woc_tip.saturating_sub(h.height_live),
        "secondsSinceSync": h.stale_secs,
        "tipHash": h.local_tip_hash,
        "wocBestHash": h.woc_best_hash,
    });
    let resp = Response::from_json(&body)?;
    Ok(resp.with_status(if v.healthy { 200 } else { 503 }))
}

#[cfg(test)]
mod health_verdict_tests {
    use super::health_verdict;

    // Distinct stand-in tip hashes.
    const H_A: &str = "aaaa";
    const H_B: &str = "bbbb";

    #[test]
    fn healthy_when_slightly_behind_and_fresh() {
        // gap 3 (< MAX_GAP), fresh; hashes differ only because we're a few back
        assert!(health_verdict(958_400, 958_403, 30, H_A, H_B).healthy);
    }

    #[test]
    fn healthy_when_exact_tip_match() {
        // gap 0 and our tip IS the network best block
        assert!(health_verdict(958_403, 958_403, 30, H_A, H_A).healthy);
    }

    #[test]
    fn degraded_when_far_behind_even_if_fresh() {
        // mid-catch-up: cron alive (30s) but ~14k behind
        let v = health_verdict(943_800, 958_400, 30, H_A, H_B);
        assert!(!v.healthy);
        assert_eq!(v.reason, "behind");
    }

    #[test]
    fn degraded_when_heartbeat_stale_even_if_caught_up() {
        // exact tip match but no heartbeat for 10 min ⇒ frozen, not trustworthy
        let v = health_verdict(958_403, 958_403, 600, H_A, H_A);
        assert!(!v.healthy);
        assert_eq!(v.reason, "stale");
    }

    #[test]
    fn degraded_on_unresolved_equal_height_fork() {
        // same height as the network tip but a DIFFERENT block — a competitor
        // branch sync.rs has not switched to yet. gap-only would call it healthy.
        let v = health_verdict(958_403, 958_403, 30, H_A, H_B);
        assert!(!v.healthy);
        assert_eq!(v.reason, "forked");
    }

    #[test]
    fn equal_height_fork_resolved_is_healthy() {
        // competitor fetch succeeded: our tip == network best block
        assert!(health_verdict(958_403, 958_403, 30, H_B, H_B).healthy);
    }

    #[test]
    fn no_fork_flag_when_one_block_behind() {
        // gap 1: hash naturally differs (one block back) — normal lag, NOT a
        // fork; must stay healthy (the fork check is gated on gap == 0)
        assert!(health_verdict(958_402, 958_403, 30, H_A, H_B).healthy);
    }

    #[test]
    fn empty_best_hash_does_not_flag_fork() {
        // defensive: gap 0 but no stored best hash yet → not forked
        assert!(health_verdict(958_403, 958_403, 30, H_A, "").healthy);
    }

    #[test]
    fn degraded_before_first_tick() {
        let v = health_verdict(0, 0, 0, "", "");
        assert!(!v.healthy);
        assert_eq!(v.reason, "no-data");
    }

    #[test]
    fn gap_boundary_is_inclusive() {
        // gap == HEALTH_MAX_GAP (6) healthy; 7 degraded (gap>0, so no fork check)
        assert!(health_verdict(958_400, 958_406, 30, H_A, H_B).healthy);
        assert!(!health_verdict(958_400, 958_407, 30, H_A, H_B).healthy);
    }

    #[test]
    fn stale_boundary() {
        // 300s ok, 301s stalled
        assert!(health_verdict(958_400, 958_401, 300, H_A, H_B).healthy);
        assert!(!health_verdict(958_400, 958_401, 301, H_A, H_B).healthy);
    }
}

async fn get_info(db: &worker::D1Database, chain: &Chain) -> Result<Response> {
    let info = storage::get_info(db, chain).await?;
    wrap_success(&info)
}

async fn current_height(db: &worker::D1Database) -> Result<Response> {
    // A missing tip row is a DEGRADED-SERVICE state, never chain state: the
    // old code returned {status:"success", value:0}, which a consumer reads
    // as a 953k-block reorg or a frozen clock (audit C4). Error instead so
    // callers fall back to another source.
    match storage::find_chain_tip(db).await? {
        Some(tip) => wrap_success(tip.height),
        None => wrap_error("No chain tip (service syncing or degraded)", 503),
    }
}

async fn get_present_height(db: &worker::D1Database, chain: &Chain) -> Result<Response> {
    let client = crate::woc::WocClient::new(chain, None);
    match client.get_chain_info().await {
        Ok(info) if info.blocks > 0 => wrap_success(info.blocks),
        _ => match storage::find_chain_tip(db).await? {
            Some(tip) => wrap_success(tip.height),
            None => wrap_error("No chain tip (service syncing or degraded)", 503),
        },
    }
}

async fn find_chain_tip_hash(db: &worker::D1Database) -> Result<Response> {
    match storage::find_chain_tip(db).await? {
        Some(h) => wrap_success(&h.hash),
        None => wrap_error("No chain tip", 404),
    }
}

async fn find_chain_tip_header_hex(db: &worker::D1Database) -> Result<Response> {
    match storage::find_chain_tip(db).await? {
        // Production returns full header JSON, not just hex
        Some(h) => wrap_success(&h),
        None => wrap_error("No chain tip", 404),
    }
}

/// Bulk/live boundary: heights below this are served from the R2 bulk store
/// (immutable historical headers), heights at/above from the D1 live window.
/// Configurable via BULK_TOP_HEIGHT (default = the projectbabbage CDN snapshot
/// top). Below the boundary the reorg/tip machinery never applies.
fn bulk_top_height(env: &Env) -> u32 {
    env.var("BULK_TOP_HEIGHT")
        .ok()
        .and_then(|v| v.to_string().parse().ok())
        .unwrap_or(942_761)
}

async fn find_header_hex_for_height(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let height: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "height")
        .and_then(|(_, v)| v.parse().ok())
        .ok_or_else(|| Error::RustError("Missing ?height= parameter".into()))?;

    // Bulk/live split: heights below the bulk boundary come from the R2 bulk
    // store; recent heights from D1.
    if height < bulk_top_height(env) {
        let bucket = env.bucket("BULK_HEADERS")?;
        return match crate::r2::read_bulk_header(&bucket, chain, height).await? {
            Some(h) => wrap_success(PublicBlockHeader::from(h)),
            None => wrap_error("Header not found", 404),
        };
    }

    if let Some(h) = storage::find_header_for_height(db, height).await? {
        return wrap_success(PublicBlockHeader::from(h));
    }
    // Fresh-block grace: verified read-through from WoC (tip+1..=tip+6).
    if ensure_fresh_header(db, env, chain, height).await?.is_some() {
        if let Some(h) = storage::find_header_for_height(db, height).await? {
            return wrap_success(PublicBlockHeader::from(h));
        }
    }
    wrap_error("Header not found", 404)
}

/// Read-through grace window for FRESH blocks (owner decision 2026-07-08):
/// the cron ingests once a minute, so a just-mined block is locally unknown
/// for up to ~60s — and a fail-closed consumer (overlay SPV) would bounce a
/// legitimate proof during that window. If the requested height is at most
/// GRACE_BLOCKS above our tip, fetch it live from WoC NOW, ingest it through
/// the full validation path (hash integrity, badPrev, parent backfill, work
/// accounting), and serve the verified answer. This is "grace WITH
/// verification" — the TS references are equally fail-closed but query WoC
/// live, so this exactly reproduces their effective behavior. A height
/// beyond the grace window (or unknown to WoC too) still answers 404:
/// unverifiable is never accepted.
const GRACE_BLOCKS: u32 = 6;

async fn ensure_fresh_header(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    height: u32,
) -> Result<Option<()>> {
    let tip = match storage::find_chain_tip(db).await? {
        Some(t) => t.height,
        None => return Ok(None),
    };
    if height <= tip || height > tip.saturating_add(GRACE_BLOCKS) {
        return Ok(None);
    }
    let api_key = env
        .secret("WHATSONCHAIN_API_KEY")
        .map(|v| v.to_string())
        .ok()
        .or_else(|| env.var("WHATSONCHAIN_API_KEY").map(|v| v.to_string()).ok())
        .filter(|s| !s.is_empty());
    let client = crate::woc::WocClient::new(chain, api_key);
    // Fill the whole gap tip+1..=height so linkage/backfill stays simple.
    for h in (tip + 1)..=height {
        match client.get_header_by_height(h).await {
            Ok(header) => {
                let _ = crate::sync::insert_with_parent_backfill(db, &client, header).await?;
            }
            Err(e) => {
                worker::console_log!("read-through: WoC has no header at {} yet: {:?}", h, e);
                return Ok(None);
            }
        }
    }
    Ok(Some(()))
}

async fn find_header_hex_for_block_hash(
    db: &worker::D1Database,
    url: &url::Url,
) -> Result<Response> {
    let hash = url
        .query_pairs()
        .find(|(k, _)| k == "hash")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| Error::RustError("Missing ?hash= parameter".into()))?;

    match storage::find_active_header_for_hash(db, &hash).await? {
        Some(h) => wrap_success(PublicBlockHeader::from(h)),
        None => wrap_error("Header not found", 404),
    }
}

async fn get_headers(db: &worker::D1Database, env: &Env, url: &url::Url) -> Result<Response> {
    let height: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "height")
        .and_then(|(_, v)| v.parse().ok())
        .ok_or_else(|| Error::RustError("Missing ?height= parameter".into()))?;
    let count: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "count")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(1);

    // getHeaders replicates a contiguous span from D1; heights below the bulk
    // boundary live in the R2 bulk files, not D1 (bulk/live split). Serving
    // them from D1 would mislabel the live window, so refuse and point callers
    // at the bulk files instead of returning a wrong answer.
    if height < bulk_top_height(env) {
        return wrap_error(
            "Heights below the bulk boundary are served from the R2 bulk files (/headers/); getHeaders serves the live window only",
            409,
        );
    }

    // Public cap only — internal callers (R2 export) read full 100k files.
    let hex_str = storage::get_headers_hex(db, height, count.min(10_000)).await?;
    wrap_success(&hex_str)
}

async fn is_valid_root_for_height(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let root = url
        .query_pairs()
        .find(|(k, _)| k == "root")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| Error::RustError("Missing ?root= parameter".into()))?;
    let height: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "height")
        .and_then(|(_, v)| v.parse().ok())
        .ok_or_else(|| Error::RustError("Missing ?height= parameter".into()))?;

    // Bulk/live split: below the bulk boundary, validate the root against the
    // R2 bulk store (immutable). A bulk miss = unable-to-verify (404), keeping
    // the same INVALID-vs-UNABLE distinction the D1 tri-state path below makes.
    if height < bulk_top_height(env) {
        let bucket = env.bucket("BULK_HEADERS")?;
        return match crate::r2::read_bulk_header(&bucket, chain, height).await? {
            Some(h) => wrap_success(h.merkle_root.eq_ignore_ascii_case(&root)),
            None => wrap_error(
                &format!("No bulk header at height {height} — unable to verify root"),
                404,
            ),
        };
    }

    // Tri-state (audit C1, Go BHS INVALID vs UNABLE_TO_VERIFY split):
    //  * active header at height + root matches   → success true
    //  * active header at height + root differs   → success false (factual)
    //  * NO active header at height (hole / above tip / reorg window)
    //    → 404 error — "unable to verify" must be distinguishable from
    //    "invalid", or a storage hole reads as proof-rejection downstream
    //    (wallet-infra already treats an error here as "fall back to WoC").
    if let Some(valid) = storage::check_root_for_height(db, &root, height).await? {
        return wrap_success(valid);
    }
    // Fresh-block grace: verified read-through from WoC (tip+1..=tip+6).
    if ensure_fresh_header(db, env, chain, height).await?.is_some() {
        if let Some(valid) = storage::check_root_for_height(db, &root, height).await? {
            return wrap_success(valid);
        }
    }
    wrap_error(
        &format!("No active header at height {height} — unable to verify root"),
        404,
    )
}

// ═════════════════════════════════════════════════════════════════════════
// /v2 wire shim — go-chaintracks contract
// (vectors: ts-stack conformance/vectors/sync/chaintracks-v2-http.json)
// ═════════════════════════════════════════════════════════════════════════

fn v2_error(code: &str, description: &str, status: u16) -> Result<Response> {
    let body = serde_json::json!({
        "status": "error",
        "code": code,
        "description": description,
    });
    Ok(Response::from_json(&body)?.with_status(status))
}

fn v2_json(value: impl serde::Serialize, cache: &str) -> Result<Response> {
    let body = serde_json::json!({ "status": "success", "value": value });
    let resp = Response::from_json(&body)?;
    let headers = resp.headers().clone();
    let _ = headers.set("Cache-Control", cache);
    Ok(resp.with_headers(headers))
}

fn v2_binary(bytes: Vec<u8>, cache: &str, extra: &[(&str, String)]) -> Result<Response> {
    let resp = Response::from_bytes(bytes)?;
    let headers = resp.headers().clone();
    let _ = headers.set("Content-Type", "application/octet-stream");
    let _ = headers.set("Cache-Control", cache);
    for (k, v) in extra {
        let _ = headers.set(k, v);
    }
    Ok(resp.with_headers(headers))
}

fn v2_valid_hash(h: &str) -> bool {
    h.len() == 64 && h.chars().all(|c| c.is_ascii_hexdigit())
}

async fn handle_v2(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    v2_path: &str,
    url: &url::Url,
) -> Result<Response> {
    match v2_path {
        "/network" => v2_json(chain.as_str(), "no-cache"),

        "/tip" | "/tip.bin" => {
            let Some(tip) = storage::find_chain_tip(db).await? else {
                return v2_error("ERR_NO_TIP", "Chain tip not found", 404);
            };
            if v2_path.ends_with(".bin") {
                let height = tip.height.to_string();
                v2_binary(
                    tip.to_bytes().to_vec(),
                    "no-cache",
                    &[("X-Block-Height", height)],
                )
            } else {
                v2_json(PublicBlockHeader::from(tip), "no-cache")
            }
        }

        p if p.starts_with("/header/height/") => {
            let raw = p.trim_start_matches("/header/height/");
            let (raw, want_bin) = match raw.strip_suffix(".bin") {
                Some(r) => (r, true),
                None => (raw, false),
            };
            let Ok(height) = raw.parse::<u32>() else {
                return v2_error("ERR_INVALID_PARAMS", "Invalid height parameter", 400);
            };
            // Bulk/live split: heights below the bulk boundary come from the R2
            // bulk store, mirroring the v1 /findHeaderHexForHeight routing.
            if height < bulk_top_height(env) {
                let h = match env.bucket("BULK_HEADERS") {
                    Ok(bucket) => crate::r2::read_bulk_header(&bucket, chain, height).await?,
                    Err(_) => None,
                };
                return match h {
                    Some(h) if want_bin => {
                        let hh = h.height.to_string();
                        v2_binary(
                            h.to_bytes().to_vec(),
                            "public, max-age=3600",
                            &[("X-Block-Height", hh)],
                        )
                    }
                    Some(h) => v2_json(PublicBlockHeader::from(h), "public, max-age=3600"),
                    None => v2_error(
                        "ERR_NOT_FOUND",
                        &format!("Header not found at height {height}"),
                        404,
                    ),
                };
            }
            let mut header = storage::find_header_for_height(db, height).await?;
            if header.is_none() {
                // Same fresh-block read-through grace as the v1 surface.
                if ensure_fresh_header(db, env, chain, height).await?.is_some() {
                    header = storage::find_header_for_height(db, height).await?;
                }
            }
            match header {
                Some(h) if want_bin => {
                    let hh = h.height.to_string();
                    v2_binary(
                        h.to_bytes().to_vec(),
                        "public, max-age=3600",
                        &[("X-Block-Height", hh)],
                    )
                }
                Some(h) => v2_json(PublicBlockHeader::from(h), "public, max-age=3600"),
                None => v2_error(
                    "ERR_NOT_FOUND",
                    &format!("Header not found at height {height}"),
                    404,
                ),
            }
        }

        p if p.starts_with("/header/hash/") => {
            let raw = p.trim_start_matches("/header/hash/");
            let (raw, want_bin) = match raw.strip_suffix(".bin") {
                Some(r) => (r, true),
                None => (raw, false),
            };
            if !v2_valid_hash(raw) {
                return v2_error("ERR_INVALID_PARAMS", "Invalid hash parameter", 400);
            }
            match storage::find_active_header_for_hash(db, &raw.to_lowercase()).await? {
                Some(h) if want_bin => {
                    let hh = h.height.to_string();
                    v2_binary(
                        h.to_bytes().to_vec(),
                        "public, max-age=3600",
                        &[("X-Block-Height", hh)],
                    )
                }
                Some(h) => v2_json(PublicBlockHeader::from(h), "public, max-age=3600"),
                None => v2_error(
                    "ERR_NOT_FOUND",
                    &format!("Header not found for hash {raw}"),
                    404,
                ),
            }
        }

        "/headers" | "/headers.bin" => {
            let q = |k: &str| {
                url.query_pairs()
                    .find(|(key, _)| key == k)
                    .map(|(_, v)| v.to_string())
            };
            let Some(height) = q("height").and_then(|v| v.parse::<u32>().ok()) else {
                return v2_error(
                    "ERR_INVALID_PARAMS",
                    "Invalid or missing height parameter",
                    400,
                );
            };
            let count = match q("count").and_then(|v| v.parse::<u32>().ok()) {
                Some(c) if c >= 1 => c,
                _ => {
                    return v2_error(
                        "ERR_INVALID_PARAMS",
                        "Invalid or missing count parameter",
                        400,
                    )
                }
            };
            // Bulk/live split: this replicates a contiguous D1 span; heights
            // below the bulk boundary live in the R2 bulk files, not D1. Refuse
            // rather than serve the mislabeled live window.
            if height < bulk_top_height(env) {
                return v2_error(
                    "ERR_NOT_FOUND",
                    "Heights below the bulk boundary are served from the R2 bulk files (/headers/)",
                    409,
                );
            }
            // Public cap mirrors the v1 route; huge counts truncate to what
            // exists (the vector expects X-Header-Count <= requested).
            let hex_str = storage::get_headers_hex(db, height, count.min(10_000)).await?;
            let bytes = hex::decode(&hex_str)
                .map_err(|e| Error::RustError(format!("hex decode: {e}")))?;
            let n = (bytes.len() / 80) as u32;
            v2_binary(
                bytes,
                "public, max-age=3600",
                &[
                    ("X-Start-Height", height.to_string()),
                    ("X-Header-Count", n.to_string()),
                ],
            )
        }

        _ => v2_error("ERR_NOT_FOUND", "Not found", 404),
    }
}

/// Admin endpoint: ingest operator-pushed headers.
/// Usage: POST /admin/ingest?start=942761 with the body a hex string of
/// concatenated 80-byte headers (heights assigned sequentially from start).
/// Linkage/PoW sanity lives with the operator; heights below the tip never
/// move the chain tip.
async fn admin_ingest(db: &worker::D1Database, url: &url::Url, body: &str) -> Result<Response> {
    let Some(start) = url
        .query_pairs()
        .find(|(k, _)| k == "start")
        .and_then(|(_, v)| v.parse::<u32>().ok())
    else {
        return wrap_error("Missing start query parameter", 400);
    };
    let hex_str = body.trim();
    let bytes = match hex::decode(hex_str) {
        Ok(b) => b,
        Err(e) => return wrap_error(&format!("hex decode: {e}"), 400),
    };
    if bytes.is_empty() || bytes.len() % 80 != 0 {
        return wrap_error("body must be a non-empty multiple of 80 bytes", 400);
    }
    let mut headers = Vec::with_capacity(bytes.len() / 80);
    for (i, chunk) in bytes.chunks(80).enumerate() {
        if let Some(header) = BlockHeader::from_bytes(chunk, start + i as u32) {
            headers.push(header);
        }
    }
    let inserted = storage::insert_headers_batch(db, &headers).await?;
    // Ingest is an authoritative canonical statement for each height: the
    // pushed header becomes the active row and any competing row at that
    // height (stale reorg branch, wipe debris) is deactivated — observed
    // live: a stale 952854 stayed active and failed isValidRootForHeight
    // for the TRUE root, so wallet-infra rejected valid BEEFs.
    let canonicalized = storage::canonicalize_heights(db, &headers).await?;
    wrap_success(serde_json::json!({
        "start": start,
        "parsed": headers.len(),
        "inserted": inserted,
        "canonicalized": canonicalized,
    }))
}

/// Admin endpoint: backfill a below-tip gap one header at a time from WoC.
/// Usage: /admin/backfill?from=942761&to=943500 — the span is clamped to 800
/// heights per invocation (Workers subrequest budget); drive larger gaps with
/// repeated calls. Inserts via the batch path and never touches the chain
/// tip (the gap is below it by definition).
async fn admin_backfill(
    db: &worker::D1Database,
    chain: &Chain,
    env: &worker::Env,
    url: &url::Url,
) -> Result<Response> {
    let get = |k: &str| -> Option<u32> {
        url.query_pairs()
            .find(|(key, _)| key == k)
            .and_then(|(_, v)| v.parse().ok())
    };
    let (Some(from), Some(to)) = (get("from"), get("to")) else {
        return wrap_error("Missing from/to query parameters", 400);
    };
    if to < from {
        return wrap_error("to must be >= from", 400);
    }
    // ~800 WoC subrequests per invocation keeps us inside the 1000 cap.
    let to = to.min(from + 799);

    // The key is a worker SECRET (env.secret), with a var fallback for
    // local dev — env.var() fails silently on secrets.
    let api_key = env
        .secret("WHATSONCHAIN_API_KEY")
        .map(|v| v.to_string())
        .ok()
        .or_else(|| env.var("WHATSONCHAIN_API_KEY").map(|v| v.to_string()).ok())
        .filter(|s| !s.is_empty());
    let client = crate::woc::WocClient::new(chain, api_key);

    let mut headers = Vec::with_capacity((to - from + 1) as usize);
    for height in from..=to {
        match client.get_header_by_height(height).await {
            Ok(header) => headers.push(header),
            Err(e) => {
                // Insert what we have — the caller re-runs from the gap.
                console_log!("backfill: WoC failed at {height}: {e:?}");
                break;
            }
        }
    }
    let fetched = headers.len() as u32;
    let inserted = if headers.is_empty() {
        0
    } else {
        storage::insert_headers_batch(db, &headers).await?
    };

    wrap_success(serde_json::json!({
        "from": from,
        "to": to,
        "fetched": fetched,
        "inserted": inserted,
        "nextFrom": from + fetched,
    }))
}

/// Admin endpoint: download one bulk CDN file and insert into D1.
/// Usage: /admin/bulk-sync?file=0 (file index 0-8)
/// Each file contains ~100k headers. Run one at a time.
async fn admin_bulk_sync(
    db: &worker::D1Database,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let file_idx: usize = url
        .query_pairs()
        .find(|(k, _)| k == "file")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);

    // Get file listing from CDN
    let listing = crate::woc::WocClient::get_bulk_file_listing(chain).await?;

    if file_idx >= listing.files.len() {
        return wrap_error(
            &format!(
                "File index {} out of range (0-{})",
                file_idx,
                listing.files.len() - 1
            ),
            400,
        );
    }

    let file_info = &listing.files[file_idx];
    let start_height = file_info.first_height.unwrap_or(file_idx as u32 * 100_000);

    // Download and parse
    let client = crate::woc::WocClient::new(chain, None);
    let headers = client.download_bulk_file(file_info, start_height).await?;
    let count = headers.len();

    // Batch insert
    let inserted = storage::insert_headers_batch(db, &headers).await?;

    // Update chain tip
    storage::update_chain_tip_to_highest(db).await?;

    // Self-heal dual-active debris this bulk path can create (review M-3 —
    // the sweep otherwise only runs on cron catch-up, which may be never).
    crate::d1::Query::new(
        "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN \
         (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)",
    )
    .run(db)
    .await?;

    wrap_success(serde_json::json!({
        "file": file_info.file_name,
        "startHeight": start_height,
        "headersInFile": count,
        "inserted": inserted,
    }))
}

/// Admin endpoint: export headers from D1 to R2 as bulk binary files.
/// Usage: /admin/export-r2 (exports all) or /admin/export-r2?file=0 (single file)
async fn admin_export_r2(
    db: &worker::D1Database,
    env: &worker::Env,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let bucket = env.bucket("BULK_HEADERS")?;

    // Use the worker's own URL as CDN base (served via /headers/ route)
    let cdn_base_url = format!(
        "https://{}/headers",
        url.host_str()
            .unwrap_or("localhost")
    );

    let file_param: Option<u32> = url
        .query_pairs()
        .find(|(k, _)| k == "file")
        .and_then(|(_, v)| v.parse().ok());

    match file_param {
        Some(idx) => {
            let count = crate::r2::export_bulk_file(db, &bucket, chain, idx, &cdn_base_url).await?;
            wrap_success(serde_json::json!({
                "file": format!("{chain}Net_{idx}.headers"),
                "exported": count,
            }))
        }
        None => {
            let result = crate::r2::export_all(db, &bucket, chain, &cdn_base_url).await?;
            wrap_success(serde_json::json!({
                "totalExported": result.total_exported,
                "fileCount": result.file_count,
            }))
        }
    }
}

/// Admin: download a whole bulk CDN header file and store it in the R2 bulk
/// bucket under the same key. Run once per file (0..9) to populate the bulk
/// store for the bulk/live split, so old-height reads hit R2 instead of the CDN.
/// Usage: /admin/import-cdn?file=0
async fn admin_import_cdn(env: &Env, chain: &Chain, url: &url::Url) -> Result<Response> {
    let file_idx: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "file")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let bucket = env.bucket("BULK_HEADERS")?;
    let bytes = crate::r2::import_file_from_cdn(&bucket, chain, file_idx).await?;
    wrap_success(serde_json::json!({
        "file": format!("{}Net_{}.headers", chain.as_str(), file_idx),
        "bytesWritten": bytes,
        "headers": bytes / 80,
    }))
}

/// Serve bulk header files from R2 bucket.
/// /headers/mainNetBlockHeaders.json — index
/// /headers/mainNet_0.headers — binary file
async fn serve_r2_file(env: &worker::Env, path: &str) -> Result<Response> {
    let bucket = env.bucket("BULK_HEADERS")?;

    // Strip /headers/ prefix to get the R2 key
    let key = path.trim_start_matches("/headers/");
    if key.is_empty() {
        return Response::error("Not Found", 404);
    }

    match crate::r2::serve_file(&bucket, key).await? {
        Some(bytes) => {
            let headers = Headers::new();
            headers.set("Cache-Control", "public, max-age=3600")?;

            if key.ends_with(".json") {
                headers.set("Content-Type", "application/json")?;
            } else if key.ends_with(".headers") {
                headers.set("Content-Type", "application/octet-stream")?;
            }

            Ok(Response::from_bytes(bytes)?.with_headers(headers))
        }
        None => Response::error("Not Found", 404),
    }
}

fn cors_preflight() -> Result<Response> {
    let headers = Headers::new();
    headers.set("Access-Control-Allow-Origin", "*")?;
    headers.set("Access-Control-Allow-Methods", "GET, OPTIONS")?;
    headers.set("Access-Control-Allow-Headers", "*")?;
    headers.set("Access-Control-Max-Age", "86400")?;
    Ok(Response::empty()?.with_status(204).with_headers(headers))
}

fn add_cors(mut response: Response) -> Response {
    let _ = response
        .headers_mut()
        .set("Access-Control-Allow-Origin", "*");
    response
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_wrap_success_format() {
        // Verify the wrapper format matches ChaintracksService
        let json = serde_json::json!({
            "status": "success",
            "value": "main"
        });
        assert_eq!(json["status"], "success");
        assert_eq!(json["value"], "main");
    }

    #[test]
    fn test_wrap_success_number() {
        let json = serde_json::json!({
            "status": "success",
            "value": 870000
        });
        assert_eq!(json["value"], 870000);
    }

    #[test]
    fn test_wrap_success_boolean() {
        let json = serde_json::json!({
            "status": "success",
            "value": true
        });
        assert_eq!(json["value"], true);
    }

    #[test]
    fn test_wrap_error_format() {
        let json = serde_json::json!({
            "status": "error",
            "code": "ERR_NOT_FOUND",
            "description": "Header not found"
        });
        assert_eq!(json["status"], "error");
        assert_eq!(json["code"], "ERR_NOT_FOUND");
    }

    #[test]
    fn test_health_text_format() {
        // Production root returns plain text, not JSON wrapper
        let expected = "Chaintracks mainNet Block Header Service";
        assert!(expected.contains("Chaintracks"));
        assert!(expected.contains("mainNet"));
    }
}
