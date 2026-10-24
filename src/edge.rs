//! chaintracks-cloudflare #16, the edge half of the 0.7.0 feed (bsv-stack-lean
//! `docs/p0/chaintracks-reorg-feed.md` section 8, CF5). Every deactivated
//! header of a reorg the service can journal stands within 400 of the prior
//! tip: the common-ancestor walk stops at 400 steps (`storage.rs`, the
//! reorg's ancestor walk) and the decoder refuses a deeper reorg
//! (`events::decode`). A header more than the settled line below the served
//! tip is therefore outside every reorg; under `HEADER_EDGE_DEPTH` it is
//! answered from `caches.default` with a day's lifetime and a tag, and every
//! nearer height answers `no-cache` in both modes (0.6.0's `public,
//! max-age=3600` let a cache serve a deactivated header for an hour).

use worker::{Env, Response};

/// The deepest reorg the service journals: the settled line can never be
/// set nearer the tip than this.
pub const REORG_BOUND: u32 = 400;

/// A settled answer's lifetime in the cache and downstream.
pub const SETTLED: &str = "public, max-age=86400";

/// Every height within the settled line of the served tip.
pub const NEAR: &str = "no-cache";

/// `HEADER_EDGE_DEPTH`: unset (or not a number), no cache and the 0.6.0
/// lifetimes below the line of 400; a number, the settled line (at least
/// [`REORG_BOUND`]; 400 by the owner's word) and the cache below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edge {
    Off,
    Depth(u32),
}

impl Edge {
    pub fn parse(raw: Option<&str>) -> Self {
        raw.and_then(|r| r.trim().parse::<u32>().ok())
            .map_or(Edge::Off, |d| Edge::Depth(d.max(REORG_BOUND)))
    }

    pub fn of(env: &Env) -> Self {
        Self::parse(
            env.var("HEADER_EDGE_DEPTH")
                .ok()
                .map(|v| v.to_string())
                .as_deref(),
        )
    }

    /// The settled line: more than this many below the served tip.
    pub fn line(&self) -> u32 {
        match self {
            Edge::Off => REORG_BOUND,
            Edge::Depth(d) => *d,
        }
    }
}

/// More than `line` below the served tip.
pub fn settled(tip: Option<u32>, height: u32, line: u32) -> bool {
    tip.is_some_and(|t| t > height && t - height > line)
}

/// The `Cache-Control` of an answer: `no-cache` within the line in both
/// modes; below it the 0.6.0 lifetime (`v060`) with the var unset, a day's
/// with it set.
pub fn control(edge: Edge, settled: bool, v060: Option<&'static str>) -> Option<&'static str> {
    match (settled, edge) {
        (false, _) => Some(NEAR),
        (true, Edge::Off) => v060,
        (true, Edge::Depth(_)) => Some(SETTLED),
    }
}

/// The tags of one height's answer: its own and its thousand's.
pub fn height_tags(height: u32) -> String {
    format!("h-{height},hb-{}", height / 1000)
}

/// The tags of a range's answer: every thousand it touches.
pub fn range_tags(start: u32, last: u32) -> String {
    (start / 1000..=last / 1000)
        .map(|b| format!("hb-{b}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The tags a purge of `heights` names: each height's, then each thousand's,
/// once each, in order.
pub fn purge_tags(heights: &[u32]) -> Vec<String> {
    let mut sorted = heights.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut buckets: Vec<u32> = sorted.iter().map(|h| h / 1000).collect();
    buckets.dedup();
    sorted
        .iter()
        .map(|h| format!("h-{h}"))
        .chain(buckets.iter().map(|b| format!("hb-{b}")))
        .collect()
}

/// The zone purge's request bodies: at most 100 tags a request (the purge
/// page's limit, design section 3 [LIT]); a purge of one replacement is one.
pub fn purge_bodies(tags: &[String]) -> Vec<String> {
    tags.chunks(100)
        .map(|c| serde_json::json!({ "tags": c }).to_string())
        .collect()
}

pub fn purge_url(zone: &str) -> String {
    format!("https://api.cloudflare.com/client/v4/zones/{zone}/purge_cache")
}

/// The answers the edge keeps, one canonical key each (a query's order or an
/// extra parameter never makes a second entry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    HeightV1(u32),
    HeightV2 { height: u32, bin: bool },
    HashV1(String),
    HashV2 { hash: String, bin: bool },
    Root { height: u32, root: String },
    RangeV1 { start: u32, count: u32 },
    RangeV2 { start: u32, count: u32, bin: bool },
}

impl Key {
    pub fn url(&self, origin: &str) -> String {
        let bin = |b: &bool| if *b { ".bin" } else { "" };
        let path = match self {
            Key::HeightV1(h) => format!("v1/height/{h}"),
            Key::HeightV2 { height, bin: b } => format!("v2/height/{height}{}", bin(b)),
            Key::HashV1(hash) => format!("v1/hash/{hash}"),
            Key::HashV2 { hash, bin: b } => format!("v2/hash/{hash}{}", bin(b)),
            Key::Root { height, root } => format!("root/{height}/{root}"),
            Key::RangeV1 { start, count } => format!("v1/range/{start}/{count}"),
            Key::RangeV2 {
                start,
                count,
                bin: b,
            } => format!("v2/range/{start}/{count}{}", bin(b)),
        };
        format!("{origin}/__edge/{path}")
    }
}

/// The keys a replacement at `height` (its old header `hashes`) deletes in
/// the colo that writes it; the zone purge reaches the roots and ranges.
pub fn local_keys(origin: &str, heights: &[u32], hashes: &[String]) -> Vec<String> {
    let mut keys = Vec::new();
    for h in heights {
        keys.push(Key::HeightV1(*h).url(origin));
        for bin in [false, true] {
            keys.push(Key::HeightV2 { height: *h, bin }.url(origin));
        }
    }
    for hash in hashes {
        keys.push(Key::HashV1(hash.clone()).url(origin));
        for bin in [false, true] {
            keys.push(
                Key::HashV2 {
                    hash: hash.to_lowercase(),
                    bin,
                }
                .url(origin),
            );
        }
    }
    keys
}

/// The kept answer for `key`, when the edge is on and holds one. A cache
/// fault is a miss, never a failed request.
pub async fn lookup(edge: Edge, key: &str) -> Option<Response> {
    if edge == Edge::Off {
        return None;
    }
    match worker::Cache::default().get(key, false).await {
        Ok(hit) => hit,
        Err(e) => {
            log_error!("edge: cache read failed: {e:?}");
            None
        }
    }
}

/// Set the answer's `Cache-Control`, and keep a settled answer at `key` with
/// its tags when the edge is on (written on the D1 read that serves it).
pub async fn answer(
    edge: Edge,
    key: &str,
    mut response: Response,
    settled: bool,
    tags: String,
    v060: Option<&'static str>,
) -> worker::Result<Response> {
    if let Some(control) = control(edge, settled, v060) {
        response.headers_mut().set("Cache-Control", control)?;
    }
    if settled && edge != Edge::Off {
        response.headers_mut().set("Cache-Tag", &tags)?;
        match response.cloned() {
            Ok(kept) => {
                if let Err(e) = worker::Cache::default().put(key, kept).await {
                    log_error!("edge: cache write failed: {e:?}");
                }
            }
            Err(e) => log_error!("edge: answer not cloned: {e:?}"),
        }
    }
    Ok(response)
}

/// What one purge did, for the operator's answer.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Purged {
    pub tags: usize,
    pub local_deleted: usize,
    /// The zone purge's status per request; empty when no zone or token is set.
    pub zone_statuses: Vec<u16>,
    pub zone_skipped: Option<String>,
}

/// Purge `heights` (and the old headers' `hashes`): `cache.delete` in this
/// colo, then the zone's purge by tag (the zone id `HEADER_EDGE_ZONE_ID`, the
/// token the secret `HEADER_EDGE_PURGE_TOKEN`, read by `env.secret` alone and
/// never logged). A purge's 200 says Cloudflare received it, not that an
/// entry went (the purge page [LIT]); whether a tag purge reaches Cache API
/// entries in every colo is beta's read (design section 8).
pub async fn purge(env: &Env, origin: &str, heights: &[u32], hashes: &[String]) -> Purged {
    let cache = worker::Cache::default();
    let mut local_deleted = 0;
    for key in local_keys(origin, heights, hashes) {
        if let Ok(worker::CacheDeletionOutcome::Success) = cache.delete(key.as_str(), false).await {
            local_deleted += 1;
        }
    }
    let tags = purge_tags(heights);
    let zone = env
        .var("HEADER_EDGE_ZONE_ID")
        .ok()
        .map(|v| v.to_string())
        .filter(|z| !z.is_empty());
    let token = env
        .secret("HEADER_EDGE_PURGE_TOKEN")
        .ok()
        .map(|v| v.to_string())
        .filter(|t| !t.is_empty());
    let mut purged = Purged {
        tags: tags.len(),
        local_deleted,
        zone_statuses: Vec::new(),
        zone_skipped: None,
    };
    let (Some(zone), Some(token)) = (zone, token) else {
        purged.zone_skipped = Some("HEADER_EDGE_ZONE_ID or HEADER_EDGE_PURGE_TOKEN unset".into());
        log_error!("edge: zone purge skipped: no zone id or no token");
        return purged;
    };
    for body in purge_bodies(&tags) {
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);
        let headers = worker::Headers::new();
        let _ = headers.set("Authorization", &format!("Bearer {token}"));
        let _ = headers.set("Content-Type", "application/json");
        init.with_headers(headers);
        init.with_body(Some(body.into()));
        let status = match worker::Request::new_with_init(&purge_url(&zone), &init) {
            Ok(req) => match worker::Fetch::Request(req).send().await {
                Ok(r) => r.status_code(),
                Err(e) => {
                    log_error!("edge: zone purge failed: {e:?}");
                    0
                }
            },
            Err(_) => 0,
        };
        log!("edge: zone purge of {} tags answered {status}", tags.len());
        purged.zone_statuses.push(status);
    }
    purged
}
