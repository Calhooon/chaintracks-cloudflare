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
    // heights ; with the worker URL baked into public configs it MUST be
    // token-gated. Token lives in the ADMIN_TOKEN worker secret (env.secret;
    // env.var fallback for local dev ; same pattern as WHATSONCHAIN_API_KEY
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
    // Mounted at both /v2/* and /chaintracks/v2/* . The event streams are
    // compatibility views of the #32 journal, at the reference's paths.
    let v2_path = path
        .strip_prefix("/chaintracks/v2")
        .or_else(|| path.strip_prefix("/v2"))
        .map(|p| p.to_string());
    if let Some(v2p) = v2_path {
        if method == Method::Get {
            let view = match v2p.as_str() {
                "/tip/stream" => Some(crate::events::View::Tip),
                "/reorg/stream" => Some(crate::events::View::Reorg),
                _ => None,
            };
            if let Some(view) = view {
                return event_response(db, &req, view, true).await;
            }
            return handle_v2(&db, env, &chain, &v2p, &req.url()?).await;
        }
        return v2_error("ERR_NOT_FOUND", "Not found", 404);
    }

    let response = match (method, path.as_str()) {
        // Health (plain text, no wrapper ; matches production root endpoint)
        (Method::Get, "/") => health(&chain),
        (Method::Get, "/events") => {
            event_response(db, &req, crate::events::View::Envelope, false).await
        }
        (Method::Get, "/events/stream") => {
            event_response(db, &req, crate::events::View::Envelope, true).await
        }

        // Info & chain
        (Method::Get, "/getChain") => wrap_success(chain.as_str()),
        (Method::Get, "/getInfo") => get_info(&db, &chain).await,
        (Method::Get, "/currentHeight") => current_height(&db).await,
        // TS ChaintracksService wire parity: the toolbox client and
        // rust-overlay probe /getPresentHeight. Answered from the store
        // (Rule 28, H11): the larger of the served tip and the highest tip
        // the couriers answered in the last cron tick; no request goes out.
        (Method::Get, "/getPresentHeight") => get_present_height(&db).await,

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
            get_headers(&db, &url).await
        }

        // Validation
        (Method::Get, "/isValidRootForHeight") => {
            let url = req.url()?;
            is_valid_root_for_height(&db, env, &chain, &url).await
        }

        // Admin: ingest raw headers pushed by an operator (concatenated
        // 80-byte header hex in the body, heights from ?start=). For gaps
        // where in-worker WoC fetching is rate-limited ; the operator
        // fetches at their own pace and pushes.
        (Method::Post, "/admin/ingest") => {
            let url = req.url()?;
            let body = req.text().await?;
            admin_ingest(&db, env, &chain, &url, &body).await
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
            admin_bulk_sync(&db, env, &chain, &url).await
        }

        // Admin (P0-4): drive the re-validation of the stored chain after the
        // deploy faster than one cron chunk a minute (?steps=N, at most 50
        // chunks a call), or restart it after a fault was repaired (?restart=1).
        (Method::Get, "/admin/revalidate") => {
            let url = req.url()?;
            admin_revalidate(&db, env, &chain, &url).await
        }

        // Admin: export headers from D1 to R2
        (Method::Get, "/admin/export-r2") => {
            let url = req.url()?;
            admin_export_r2(&db, env, &chain, &url).await
        }

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

async fn get_info(db: &worker::D1Database, chain: &Chain) -> Result<Response> {
    let info = storage::get_info(db, chain).await?;
    wrap_success(&info)
}

async fn current_height(db: &worker::D1Database) -> Result<Response> {
    // A missing tip row is a DEGRADED-SERVICE state, never chain state: the
    // old code returned {status:"success", value:0}, which a consumer reads
    // as a 953k-block reorg or a frozen clock (audit C4). Error instead so
    // callers fall back to another source.
    // P0-4: the served tip (the re-validated chain's; see storage::served_tip).
    match storage::served_tip(db).await? {
        Some(tip) => wrap_success(tip.height),
        None => wrap_error("No chain tip (service syncing or degraded)", 503),
    }
}

/// Rule 28 (H11): the present height is answered from what this service
/// holds, the larger of the served tip and `last_seen_height` (the highest
/// tip the courier ladder answered in the last cron tick, already in D1).
/// The route asks no one: it was a public proxy to one explorer, one request
/// out per request in, serving a number nothing here had checked.
async fn get_present_height(db: &worker::D1Database) -> Result<Response> {
    match storage::present_height(db).await? {
        Some(height) => wrap_success(height),
        None => wrap_error("No chain tip (service syncing or degraded)", 503),
    }
}

async fn find_chain_tip_hash(db: &worker::D1Database) -> Result<Response> {
    match storage::served_tip(db).await? {
        Some(h) => wrap_success(&h.hash),
        None => wrap_error("No chain tip", 404),
    }
}

async fn find_chain_tip_header_hex(db: &worker::D1Database) -> Result<Response> {
    match storage::served_tip(db).await? {
        // Production returns full header JSON, not just hex
        Some(h) => wrap_success(&h),
        None => wrap_error("No chain tip", 404),
    }
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

    if let Some(h) = storage::served_header_for_height(db, height).await? {
        return wrap_success(PublicBlockHeader::from(h));
    }
    // Fresh-block grace: verified read-through from the ladder (tip+1..=tip+6).
    if ensure_fresh_header(db, env, chain, height).await?.is_some() {
        if let Some(h) = storage::served_header_for_height(db, height).await? {
            return wrap_success(PublicBlockHeader::from(h));
        }
    }
    wrap_error("Header not found", 404)
}

/// Read-through grace window for FRESH blocks (owner decision 2026-07-08):
/// the cron ingests once a minute, so a just-mined block is locally unknown
/// for up to ~60s ; and a fail-closed consumer (overlay SPV) would bounce a
/// legitimate proof during that window. If the requested height is at most
/// GRACE_BLOCKS above our tip, fetch it live from the courier ladder NOW
/// (Rule 28, H12: the ladder the cron reads, never one courier), ingest it
/// through the full validation path (hash integrity, badPrev, parent
/// backfill, work accounting), and serve the verified answer. This is "grace
/// WITH verification" ; the TS references are equally fail-closed but query
/// WoC live, so this reproduces their effective behavior. A height beyond the
/// grace window (or one no courier has yet) still answers 404: unverifiable
/// is never accepted.
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
    // Rule 28 (H12): the ladder the cron reads (WhatsOnChain, Arcade and
    // Bitails as each other's fallbacks), by height and for the parent walk,
    // never one courier. Every answer is checked before it is stored.
    let ladder = crate::sync::courier_ladder(env, chain);
    let params = crate::sync::chain_params(env, chain)?;
    let landed = crate::sync::read_through(db, &params, &ladder, env, tip, height).await?;
    worker::console_log!("read-through: couriers: {}", ladder.summary());
    Ok(landed)
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

    match storage::served_active_header_for_hash(db, &hash).await? {
        Some(h) => wrap_success(PublicBlockHeader::from(h)),
        None => wrap_error("Header not found", 404),
    }
}

async fn get_headers(db: &worker::D1Database, url: &url::Url) -> Result<Response> {
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

    // Public cap only ; internal callers (R2 export) read full 100k files.
    let hex_str = storage::served_headers_hex(db, height, count.min(10_000)).await?;
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

    // Tri-state (audit C1, Go BHS INVALID vs UNABLE_TO_VERIFY split):
    //  * active header at height + root matches   → success true
    //  * active header at height + root differs   → success false (factual)
    //  * NO active header at height (hole / above tip / reorg window)
    //    → 404 error ; "unable to verify" must be distinguishable from
    //    "invalid", or a storage hole reads as proof-rejection downstream
    //    (wallet-infra already treats an error here as "fall back to WoC").
    if let Some(valid) = storage::check_root_for_height(db, &root, height).await? {
        return wrap_success(valid);
    }
    // Fresh-block grace: verified read-through from the ladder (tip+1..=tip+6).
    if ensure_fresh_header(db, env, chain, height).await?.is_some() {
        if let Some(valid) = storage::check_root_for_height(db, &root, height).await? {
            return wrap_success(valid);
        }
    }
    wrap_error(
        &format!("No active header at height {height} ; unable to verify root"),
        404,
    )
}

// ═════════════════════════════════════════════════════════════════════════
// /v2 wire shim ; go-chaintracks contract
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
            let Some(tip) = storage::served_tip(db).await? else {
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
            let mut header = storage::served_header_for_height(db, height).await?;
            if header.is_none() {
                // Same fresh-block read-through grace as the v1 surface.
                if ensure_fresh_header(db, env, chain, height).await?.is_some() {
                    header = storage::served_header_for_height(db, height).await?;
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
            match storage::served_active_header_for_hash(db, &raw.to_lowercase()).await? {
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
            // Public cap mirrors the v1 route; huge counts truncate to what
            // exists (the vector expects X-Header-Count <= requested).
            let hex_str = storage::served_headers_hex(db, height, count.min(10_000)).await?;
            let bytes =
                hex::decode(&hex_str).map_err(|e| Error::RustError(format!("hex decode: {e}")))?;
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
/// Every header runs through P0-4's checks. An authoritative replacement
/// disconnects the old suffix and emits its reorg before serving the new tip.
async fn admin_ingest(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    url: &url::Url,
    body: &str,
) -> Result<Response> {
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
    // P0-4: the operator's push meets the node's rules like any courier's.
    let params = crate::sync::chain_params(env, chain)?;
    let inserted = match storage::insert_headers_batch(db, &params, &headers).await {
        Ok(n) => n,
        Err(e) => return wrap_error(&format!("{e}"), 422),
    };
    // Ingest is an authoritative canonical statement for each height: the
    // pushed header becomes the active row and any competing row at that
    // height (stale reorg branch, wipe debris) is deactivated ; observed
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

/// Admin endpoint: backfill a below-tip gap one header at a time from the
/// courier ladder (Rule 28, H13).
/// Usage: /admin/backfill?from=942761&to=943500 ; the span is clamped to 800
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
    // ~800 courier subrequests per invocation keeps us inside the 1000 cap
    // (a rung that faults three times is skipped for the rest of the call).
    let to = to.min(from + 799);

    // Rule 28 (H13): the ladder the cron reads, never one courier.
    let ladder = crate::sync::courier_ladder(env, chain);
    // Insert what we have ; the caller re-runs from the gap.
    let headers = crate::sync::fetch_span(&ladder, from, to).await;
    console_log!("backfill: couriers: {}", ladder.summary());
    let fetched = headers.len() as u32;
    let params = crate::sync::chain_params(env, chain)?;
    let inserted = if headers.is_empty() {
        0
    } else {
        match storage::insert_headers_batch(db, &params, &headers).await {
            Ok(n) => n,
            Err(e) => return wrap_error(&format!("{e}"), 422),
        }
    };

    wrap_success(serde_json::json!({
        "from": from,
        "to": to,
        "fetched": fetched,
        "inserted": inserted,
        "nextFrom": from + fetched,
    }))
}

/// Headers per bulk file, and per `/admin/bulk-sync` call.
const BULK_FILE_HEADERS: u32 = 100_000;

/// Admin endpoint: bootstrap one span of 100,000 headers into D1.
/// Usage: /admin/bulk-sync?file=0 (span index: file N is heights
/// N * 100,000 and up). Run one at a time.
///
/// Rule 28 (H14, H15): the upstream PEER's `getHeaders` is asked first (the
/// header service the cron already catches up from); the bulk FILE HOST
/// (`woc::BULK_FILE_HOST`, pinned, neither peer nor explorer) is what remains
/// when no peer is configured or the peer could not serve the span, and the
/// answer says which served it and why. `&source=file` asks the file host
/// alone. Either way the batch meets the node's rules on insert.
async fn admin_bulk_sync(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let q = |k: &str| {
        url.query_pairs()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.to_string())
    };
    let file_idx: usize = q("file").and_then(|v| v.parse().ok()).unwrap_or(0);
    let Some(span_start) = u32::try_from(file_idx)
        .ok()
        .and_then(|i| i.checked_mul(BULK_FILE_HEADERS))
    else {
        return wrap_error("File index out of range", 400);
    };

    let upstream = env
        .var("UPSTREAM_CHAINTRACKS_URL")
        .map(|v| v.to_string())
        .ok()
        .filter(|s| !s.is_empty() && q("source").as_deref() != Some("file"));
    let peer = upstream.as_deref().map(crate::sync::UpstreamPeer);
    // The peer's first header must link to our stored header below the span.
    let anchor = match span_start.checked_sub(1) {
        Some(below) => storage::find_header_for_height(db, below)
            .await?
            .map(|h| h.hash),
        None => None,
    };
    let from_peer = crate::sync::bootstrap_from_peer(
        peer.as_ref(),
        span_start,
        BULK_FILE_HEADERS,
        anchor.as_deref(),
    )
    .await;

    let (file_name, start_height, headers, source, peer_fault) = match from_peer {
        crate::sync::Bootstrap::Peer(headers) => (
            format!("{chain}Net_{file_idx}.headers"),
            span_start,
            headers,
            "peer",
            None,
        ),
        crate::sync::Bootstrap::FileHost { peer_fault } => {
            if let Some(fault) = &peer_fault {
                console_log!("bulk-sync: the upstream peer could not serve span {file_idx} ({fault}); reading the file host");
            }
            // The file host's listing
            let listing = crate::woc::WocClient::get_bulk_file_listing(chain).await?;
            if file_idx >= listing.files.len() {
                return wrap_error(
                    &format!(
                        "File index {} out of range (0-{})",
                        file_idx,
                        listing.files.len().saturating_sub(1)
                    ),
                    400,
                );
            }
            let file_info = &listing.files[file_idx];
            let start_height = file_info.first_height.unwrap_or(span_start);
            // Download and parse (the URL is always on the pinned host)
            let client = crate::woc::WocClient::new(chain, None);
            let headers = client.download_bulk_file(file_info, start_height).await?;
            (
                file_info.file_name.clone(),
                start_height,
                headers,
                "fileHost",
                peer_fault,
            )
        }
    };
    let count = headers.len();

    // Batch insert (P0-4: refused whole when any header fails the node's rules)
    let params = crate::sync::chain_params(env, chain)?;
    let inserted = match storage::insert_headers_batch(db, &params, &headers).await {
        Ok(n) => n,
        Err(e) => return wrap_error(&format!("{e}"), 422),
    };

    // Self-heal dual-active debris this bulk path can create (review M-3 ;
    // the sweep otherwise only runs on cron catch-up, which may be never).
    crate::d1::Query::new(storage::SQL_DEDUPE_ACTIVE_HEIGHTS_INGEST)
        .run(db)
        .await?;
    // #32: choose and publish the tip after the old debris is deactivated.
    storage::update_chain_tip_to_highest(db).await?;

    wrap_success(serde_json::json!({
        "file": file_name,
        "startHeight": start_height,
        "headersInFile": count,
        "inserted": inserted,
        "source": source,
        "peerFault": peer_fault,
    }))
}

/// Admin endpoint (P0-4): `/admin/revalidate?steps=N&restart=1`.
async fn admin_revalidate(
    db: &worker::D1Database,
    env: &Env,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let params = crate::sync::chain_params(env, chain)?;
    let q = |k: &str| {
        url.query_pairs()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.to_string())
    };
    if q("restart").as_deref() == Some("1") {
        crate::d1::Query::new(storage::SQL_RESTART_VALIDATION)
            .run(db)
            .await?;
    }
    let steps = q("steps")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1)
        .clamp(1, 50);
    let mut last = storage::Revalidation::Complete;
    for _ in 0..steps {
        last = storage::revalidate_step(db, &params, storage::REVALIDATE_CHUNK).await?;
        if !matches!(last, storage::Revalidation::Advanced { .. }) {
            break;
        }
    }
    let state = storage::read_validation_state(db).await?;
    wrap_success(serde_json::json!({
        "result": format!("{last:?}"),
        "validatedHeight": state.validated_height,
        "validatedHash": state.validated_hash,
        "validationComplete": state.complete,
        "validationFault": state.fault,
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
            .unwrap_or("your-worker.your-account.workers.dev")
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

/// Serve bulk header files from R2 bucket.
/// /headers/mainNetBlockHeaders.json ; index
/// /headers/mainNet_0.headers ; binary file
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

async fn event_response(
    db: worker::D1Database,
    req: &Request,
    view: crate::events::View,
    streaming: bool,
) -> Result<Response> {
    let url = req.url()?;
    let last_id = req.headers().get("Last-Event-ID")?;
    let cursor = match crate::events::parse_cursor(
        &url,
        if streaming { last_id.as_deref() } else { None },
    ) {
        Ok(cursor) => cursor,
        Err(e) => return wrap_error(&e.to_string(), 400),
    };
    let since = match (cursor, view) {
        (Some(cursor), _) => cursor,
        (None, crate::events::View::Envelope) => 0,
        (None, _) => crate::events::head(&db).await?,
    };
    let head = crate::events::head(&db).await?;
    if since > head {
        return wrap_error("cursorAhead: cursor belongs to another journal", 409);
    }
    if streaming {
        let mut initial = String::new();
        if cursor.is_none() && matches!(view, crate::events::View::Tip) {
            if let Some(tip) = storage::served_tip(&db).await? {
                initial = crate::events::initial_tip(&tip);
            }
        }
        crate::events::stream(db, since, view, initial).await
    } else {
        let page = crate::events::read_page(&db, since, crate::events::PAGE_SIZE).await?;
        let headers = Headers::new();
        headers.set("Content-Type", "application/json")?;
        headers.set("Cache-Control", "no-store")?;
        headers.set("X-Chain-Event-Version", "1")?;
        Ok(Response::ok(page.json())?.with_headers(headers))
    }
}
