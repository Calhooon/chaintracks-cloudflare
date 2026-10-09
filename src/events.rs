//! #32: one journal, one envelope, three transports. See docs/CHAIN-EVENTS.md.

use crate::d1::{HeaderDb, Query};
use crate::sync::{TipWebhooks, WebhookDelivery};
use crate::types::BlockHeader;
use serde::{Deserialize, Serialize};
use worker::{Error, Result};

pub const PAGE_SIZE: u32 = 100;
const MAX_CURSOR: u64 = 9_007_199_254_740_991;

/// These bytes and the work belong to the header, independent of D1 flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventHeader {
    pub version: u32,
    pub previous_hash: String,
    pub merkle_root: String,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
    pub height: u32,
    pub hash: String,
    pub chain_work: String,
}

impl From<&BlockHeader> for EventHeader {
    fn from(h: &BlockHeader) -> Self {
        Self {
            version: h.version,
            previous_hash: h.previous_hash.clone(),
            merkle_root: h.merkle_root.clone(),
            time: h.time,
            bits: h.bits,
            nonce: h.nonce,
            height: h.height,
            hash: h.hash.clone(),
            chain_work: h.chain_work.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outpoint {
    pub txid: String,
    pub vout: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ChainEvent {
    #[serde(rename = "tip")]
    Tip {
        height: u32,
        hash: String,
        time: u32,
        header: EventHeader,
    },
    #[serde(rename = "fork")]
    Fork {
        height: u32,
        #[serde(rename = "competingTips")]
        competing_tips: Vec<EventHeader>,
        depth: u32,
    },
    #[serde(rename = "reorg")]
    Reorg {
        #[serde(rename = "forkHeight")]
        fork_height: u32,
        depth: u32,
        #[serde(rename = "deactivatedHeaders")]
        deactivated_headers: Vec<EventHeader>,
        #[serde(rename = "newTip")]
        new_tip: EventHeader,
    },
    #[serde(rename = "invalidated")]
    Invalidated {
        #[serde(rename = "blockHash")]
        block_hash: String,
    },
    #[serde(rename = "frozen")]
    Frozen { outpoint: Outpoint },
    #[serde(rename = "tipAge")]
    TipAge { seconds: u64, tip: EventHeader },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Envelope {
    pub v: u8,
    #[serde(flatten)]
    pub event: ChainEvent,
}

fn fault(message: impl Into<String>) -> Error {
    Error::RustError(message.into())
}

/// An observable error, including for future versions or kinds. Never skip
/// and acknowledge an envelope that the reader cannot interpret.
pub fn decode(payload: &str) -> Result<Envelope> {
    let mut value: serde_json::Value =
        serde_json::from_str(payload).map_err(|e| fault(format!("unknownShape: {e}")))?;
    let record = value
        .as_object_mut()
        .ok_or_else(|| fault("unknownShape: expected object"))?;
    let version = record
        .remove("v")
        .ok_or_else(|| fault("unknownVersion: missing v"))?;
    if version.as_u64() != Some(1) {
        return Err(fault(format!("unknownVersion: {version}")));
    }
    let kind = record
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !matches!(
        kind,
        "tip" | "fork" | "reorg" | "invalidated" | "frozen" | "tipAge"
    ) {
        return Err(fault(format!("unknownKind: {kind}")));
    }
    let event: ChainEvent =
        serde_json::from_value(value).map_err(|e| fault(format!("unknownShape: {e}")))?;
    let valid_hash = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
    let valid_header = |h: &EventHeader| {
        h.height <= i32::MAX as u32
            && valid_hash(&h.hash)
            && valid_hash(&h.previous_hash)
            && valid_hash(&h.merkle_root)
            && valid_hash(&h.chain_work)
    };
    let valid = match &event {
        ChainEvent::Tip {
            height,
            hash,
            time,
            header,
        } => {
            valid_header(header)
                && *height == header.height
                && *hash == header.hash
                && *time == header.time
        }
        ChainEvent::Fork {
            height,
            competing_tips,
            depth,
        } => {
            *depth > 0
                && competing_tips.len() == 2
                && competing_tips.iter().all(valid_header)
                && competing_tips[0].hash != competing_tips[1].hash
                && competing_tips[0]
                    .height
                    .checked_sub(*depth)
                    .and_then(|h| h.checked_add(1))
                    == Some(*height)
        }
        ChainEvent::Reorg {
            fork_height,
            depth,
            deactivated_headers,
            new_tip,
        } => {
            *depth > 0
                && *depth <= 400
                && deactivated_headers.len() == *depth as usize
                && valid_header(new_tip)
                && deactivated_headers.iter().all(valid_header)
                && deactivated_headers
                    .last()
                    .is_some_and(|h| h.height == *fork_height)
                && deactivated_headers
                    .windows(2)
                    .all(|p| p[0].height == p[1].height + 1 && p[0].previous_hash == p[1].hash)
        }
        ChainEvent::Invalidated { block_hash } => valid_hash(block_hash),
        ChainEvent::Frozen { outpoint } => valid_hash(&outpoint.txid),
        ChainEvent::TipAge { tip, .. } => valid_header(tip),
    };
    if !valid {
        return Err(fault("unknownShape: inconsistent chain event"));
    }
    Ok(Envelope { v: 1, event })
}

#[derive(Debug, Clone)]
pub struct JournalEvent {
    pub cursor: u64,
    /// The original bytes, never reserialized by a transport.
    pub payload: String,
}

pub struct Page {
    pub cursor: u64,
    pub events: Vec<JournalEvent>,
    pub has_more: bool,
}

pub async fn head(db: &impl HeaderDb) -> Result<u64> {
    #[derive(Deserialize)]
    struct Row {
        cursor: f64,
    }
    let row: Row = Query::new("SELECT COALESCE(MAX(cursor), 0) AS cursor FROM chain_events")
        .first(db)
        .await?
        .ok_or_else(|| fault("event journal unavailable"))?;
    if row.cursor < 0.0 || row.cursor > MAX_CURSOR as f64 {
        return Err(fault("event cursor exhausted"));
    }
    Ok(row.cursor as u64)
}

pub async fn read_page(db: &impl HeaderDb, since: u64, limit: u32) -> Result<Page> {
    if since > MAX_CURSOR || !(1..=PAGE_SIZE).contains(&limit) {
        return Err(fault("invalid event cursor or page size"));
    }
    if since > head(db).await? {
        return Err(fault("cursorAhead: cursor belongs to another journal"));
    }
    #[derive(Deserialize)]
    struct Row {
        cursor: f64,
        payload: String,
    }
    let mut rows: Vec<Row> = Query::new(
        "SELECT cursor, payload FROM chain_events WHERE cursor > ? ORDER BY cursor LIMIT ?",
    )
    .bind(since)
    .bind(limit + 1)
    .all(db)
    .await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        decode(&row.payload)?;
        events.push(JournalEvent {
            cursor: row.cursor as u64,
            payload: row.payload,
        });
    }
    Ok(Page {
        cursor: events.last().map_or(since, |e| e.cursor),
        events,
        has_more,
    })
}

impl Page {
    pub fn json(&self) -> String {
        let events = self
            .events
            .iter()
            .map(|e| format!("{{\"cursor\":\"{}\",\"event\":{}}}", e.cursor, e.payload))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"v\":1,\"cursor\":\"{}\",\"events\":[{events}],\"hasMore\":{}}}",
            self.cursor, self.has_more
        )
    }
}

/// Last-Event-ID takes precedence when an EventSource reconnects to its
/// original URL. Decimal cursors stay within D1's exact JS integer range.
pub fn parse_cursor(url: &url::Url, last_event_id: Option<&str>) -> Result<Option<u64>> {
    let values: Vec<_> = url
        .query_pairs()
        .filter(|(k, _)| k == "since")
        .map(|(_, v)| v.into_owned())
        .collect();
    if values.len() > 1 {
        return Err(fault("duplicate since cursor"));
    }
    let raw = last_event_id.or_else(|| values.first().map(String::as_str));
    raw.map(|raw| {
        if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
            return Err(fault("cursor must be unsigned decimal"));
        }
        raw.parse::<u64>()
            .ok()
            .filter(|n| *n <= MAX_CURSOR)
            .ok_or_else(|| fault("cursor out of range"))
    })
    .transpose()
}

#[derive(Clone, Copy)]
pub enum View {
    Envelope,
    Tip,
    Reorg,
}

/// [SRC] ts-stack@fb1b2da GoChaintracksServiceClient.ts:642-680 requires
/// either eight base keys, or all five storage keys. Partial storage keys
/// fail, so the view deliberately contains the eight base keys only.
fn ts_header(h: &EventHeader) -> serde_json::Value {
    let mut value = serde_json::to_value(h).expect("header serialization");
    value.as_object_mut().unwrap().remove("chainWork");
    value
}

pub fn initial_tip(header: &BlockHeader) -> String {
    format!("data: {}\n\n", ts_header(&EventHeader::from(header)))
}

pub fn compatibility_view(envelope: &Envelope, view: View) -> Option<serde_json::Value> {
    match (&envelope.event, view) {
        (ChainEvent::Tip { header, .. }, View::Tip) => Some(ts_header(header)),
        (
            ChainEvent::Reorg {
                depth,
                deactivated_headers,
                new_tip,
                ..
            },
            View::Reorg,
        ) => Some(serde_json::json!({
            "depth": depth,
            "oldTip": ts_header(&deactivated_headers[0]),
            "newTip": ts_header(new_tip),
            "deactivatedHeaders": deactivated_headers.iter().map(ts_header).collect::<Vec<_>>()
        })),
        _ => None,
    }
}

pub fn sse_frame(event: &JournalEvent, view: View) -> Result<String> {
    let data = match view {
        View::Envelope => Some(event.payload.clone()),
        _ => compatibility_view(&decode(&event.payload)?, view).map(|v| v.to_string()),
    };
    Ok(match data {
        Some(data) => format!("id: {}\ndata: {data}\n\n", event.cursor),
        None => format!("id: {}\n\n", event.cursor),
    })
}

/// The scheduled handler exists at rust-chaintracks@461dc39 src/lib.rs:68-74
/// and wrangler.toml:53-54. Keep age observable even when couriers are down.
pub const SQL_TIP_AGE: &str = "INSERT OR IGNORE INTO chain_events(payload, event_key) SELECT json_object('v', 1, 'kind', 'tipAge', 'seconds', MAX(0, unixepoch() - time), 'tip', json(payload)), 'tipAge:' || hash || ':' || (unixepoch() / 60) FROM chain_event_served_tip";

pub async fn timer(db: &impl HeaderDb) -> Result<()> {
    Query::new("UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1")
        .run(db)
        .await?;
    Query::new(SQL_TIP_AGE).run(db).await
}

/// At-least-once webhook outbox. A refusal stops this target at its cursor;
/// other targets still run. A crash after acceptance can repeat the event.
pub async fn deliver(db: &impl HeaderDb, hooks: &impl TipWebhooks) -> Result<()> {
    let targets = hooks.targets();
    if targets.is_empty() {
        return Ok(());
    }
    if targets.len() > 32 {
        return Err(fault("at most 32 chain-event webhook targets"));
    }
    let token = hooks.token();
    if token.is_empty() {
        return Err(fault("chain-event webhook token is unset"));
    }
    let budget = (32 / targets.len()) as u32;
    for target in targets {
        let key = format!(
            "{}={}",
            target.binding.as_deref().unwrap_or_default(),
            target.url
        );
        #[derive(Deserialize)]
        struct Row {
            cursor: f64,
        }
        let row: Option<Row> =
            Query::new("SELECT cursor FROM chain_event_deliveries WHERE target = ?")
                .bind(&*key)
                .first(db)
                .await?;
        let since = row.map_or(0, |r| r.cursor as u64);
        let page = read_page(db, since, budget).await?;
        for event in page.events {
            match hooks
                .post_event(&target, &token, &event.payload, event.cursor)
                .await
            {
                WebhookDelivery::Accepted(_) => {
                    Query::new("INSERT INTO chain_event_deliveries(target, cursor) VALUES (?, ?) ON CONFLICT(target) DO UPDATE SET cursor = MAX(chain_event_deliveries.cursor, excluded.cursor)")
                        .bind(&*key).bind(event.cursor).run(db).await?;
                }
                failure => {
                    log_error!(
                        "Chain-event delivery failed at cursor {}: {failure:?}",
                        event.cursor
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}

/// The stream owns its D1 binding. Dropping the response cancels its Delay;
/// no isolate-global subscriber registry or spawned task is needed.
pub async fn stream(
    db: worker::D1Database,
    since: u64,
    view: View,
    initial: String,
) -> Result<worker::Response> {
    let page = read_page(&db, since, PAGE_SIZE).await?;
    let mut ready = initial;
    for event in &page.events {
        ready.push_str(&sse_frame(event, view)?);
    }
    if ready.is_empty() {
        ready.push_str(": heartbeat\n\n");
    }
    let body = futures_util::stream::unfold(
        Some((db, page.cursor, ready, page.has_more)),
        move |state| async move {
            let (db, cursor, ready, more) = state?;
            if !ready.is_empty() {
                return Some((
                    Ok::<_, Error>(ready.into_bytes()),
                    Some((db, cursor, String::new(), more)),
                ));
            }
            if !more {
                worker::Delay::from(std::time::Duration::from_secs(15)).await;
            }
            match read_page(&db, cursor, PAGE_SIZE).await {
                Ok(page) => {
                    let mut chunk = String::new();
                    for event in &page.events {
                        match sse_frame(event, view) {
                            Ok(frame) => chunk.push_str(&frame),
                            Err(e) => return Some((Err(e), None)),
                        }
                    }
                    if chunk.is_empty() {
                        chunk.push_str(": heartbeat\n\n");
                    }
                    Some((
                        Ok(chunk.into_bytes()),
                        Some((db, page.cursor, String::new(), page.has_more)),
                    ))
                }
                Err(e) => {
                    // This is a reported feed fault with no id, so a reader cannot
                    // acknowledge the bad record. Envelope clients must report it.
                    let payload = serde_json::json!({"v":1,"error":"feedUnavailable","description":e.to_string()});
                    Some((
                        Ok(format!("event: error\ndata: {payload}\n\n").into_bytes()),
                        None,
                    ))
                }
            }
        },
    );
    let headers = worker::Headers::new();
    headers.set("Content-Type", "text/event-stream")?;
    headers.set("Cache-Control", "no-store")?;
    headers.set("X-Chain-Event-Version", "1")?;
    headers.set("Access-Control-Expose-Headers", "X-Chain-Event-Version")?;
    headers.set("Access-Control-Allow-Origin", "*")?;
    worker::Response::from_stream(body).map(|r| r.with_headers(headers))
}
