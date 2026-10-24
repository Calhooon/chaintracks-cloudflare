//! #32: one journal, one envelope, three transports. See docs/CHAIN-EVENTS.md.

use crate::d1::{HeaderDb, Query};
use crate::sync::{TipWebhooks, WebhookDelivery};
use crate::types::BlockHeader;
use serde::{Deserialize, Serialize};
use worker::{Error, Result};

pub const PAGE_SIZE: u32 = 100;
const MAX_CURSOR: u64 = 9_007_199_254_740_991;

/// `CHAIN_EVENT_FEED`, read once per invocation: `"2"` is the 0.7.0 feed
/// (bsv-stack-lean `docs/p0/chaintracks-reorg-feed.md` sections 4 to 6: no
/// minute's `tipAge` row, one query a stream wake and a clean end before the
/// platform's bound, the retention with its floor, the outbox's one head
/// read); unset, `"1"` or any other value is the 0.6.0 feed exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Feed {
    #[default]
    V1,
    V2,
}

impl Feed {
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some("2") => Feed::V2,
            _ => Feed::V1,
        }
    }

    pub fn of(env: &worker::Env) -> Self {
        Self::parse(
            env.var("CHAIN_EVENT_FEED")
                .ok()
                .map(|v| v.to_string())
                .as_deref(),
        )
    }
}

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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SplitTip {
    pub hash: String,
    pub height: u32,
    pub chain_work: String,
    pub rungs: Vec<String>,
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
    #[serde(rename = "split")]
    Split {
        key: String,
        #[serde(rename = "forkHeight")]
        fork_height: u32,
        tips: Vec<SplitTip>,
    },
    #[serde(rename = "splitResolved")]
    SplitResolved {
        key: String,
        #[serde(rename = "forkHeight")]
        fork_height: u32,
    },
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
    if !matches!(version.as_u64(), Some(1 | 2)) {
        return Err(fault(format!("unknownVersion: {version}")));
    }
    let kind = record
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let split_kind = matches!(kind, "split" | "splitResolved");
    if !(matches!(
        kind,
        "tip" | "fork" | "reorg" | "invalidated" | "frozen" | "tipAge"
    ) || (version.as_u64() == Some(2) && split_kind))
    {
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
    let valid_split_key = |key: &str, height: u32| {
        height > 0
            && height <= i32::MAX as u32
            && key.strip_prefix("split:").is_some_and(valid_hash)
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
        ChainEvent::Split {
            key,
            fork_height,
            tips,
        } => {
            valid_split_key(key, *fork_height)
                && tips.len() == 2
                && tips.iter().all(|tip| {
                    valid_hash(&tip.hash)
                        && valid_hash(&tip.chain_work)
                        && tip.height >= *fork_height
                        && tip.height <= i32::MAX as u32
                        && !tip.rungs.is_empty()
                        && tip.rungs.iter().all(|name| !name.is_empty())
                        && tip.rungs.windows(2).all(|names| names[0] < names[1])
                })
                && tips[0].hash != tips[1].hash
                && tips[0]
                    .rungs
                    .iter()
                    .all(|name| !tips[1].rungs.contains(name))
        }
        ChainEvent::SplitResolved { key, fork_height } => valid_split_key(key, *fork_height),
    };
    if !valid {
        return Err(fault("unknownShape: inconsistent chain event"));
    }
    Ok(Envelope {
        v: version.as_u64().unwrap() as u8,
        event,
    })
}

#[derive(Debug, Clone)]
pub struct JournalEvent {
    pub cursor: u64,
    /// The original bytes, never reserialized by a transport; for a stored
    /// row this service cannot decode, its `unreadable` envelope.
    pub payload: String,
    /// The decode fault of a stored row delivered as `unreadable`.
    pub fault: Option<String>,
}

/// chaintracks-cloudflare #8: the service delivers every stored row at its
/// cursor. A row it cannot decode itself goes out as this version-1 envelope
/// in the row's place, never dropped and never a whole-page error; a reader
/// stops at its cursor (an unknown kind to a version-1 decoder).
pub fn unreadable(cursor: u64, fault: &str) -> String {
    format!(
        "{{\"v\":1,\"kind\":\"unreadable\",\"cursor\":{cursor},\"fault\":{}}}",
        serde_json::Value::from(fault)
    )
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
    let events = rows
        .into_iter()
        .map(|row| journal_event(row.cursor as u64, row.payload))
        .collect::<Vec<_>>();
    Ok(Page {
        cursor: events.last().map_or(since, |e| e.cursor),
        events,
        has_more,
    })
}

fn journal_event(cursor: u64, payload: String) -> JournalEvent {
    match decode(&payload) {
        Ok(_) => JournalEvent {
            cursor,
            payload,
            fault: None,
        },
        Err(e) => {
            let fault = e.to_string();
            log_error!("Chain event at cursor {cursor} is unreadable: {fault}");
            JournalEvent {
                cursor,
                payload: unreadable(cursor, &fault),
                fault: Some(fault),
            }
        }
    }
}

/// Change 2 (CF4): "Queries per Worker invocation (read subrequest limits) |
/// 1000 (Workers Paid)" [LIT] https://developers.cloudflare.com/d1/platform/limits/
/// (Last updated Apr 21, 2026). A feed-2 stream spends at most this many,
/// then writes `retry: 1000` and ends: an ordinary reconnect with
/// `Last-Event-ID`, never `feedUnavailable` for the bound.
pub const STREAM_QUERY_BOUND: u32 = 900;

/// The D1 queries one stream invocation has spent against its bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBudget {
    spent: u32,
    bound: u32,
}

impl QueryBudget {
    /// `bound` above [`STREAM_QUERY_BOUND`] is lowered to it: the test hook
    /// `CHAIN_EVENT_STREAM_BOUND` can shorten a stream, never lengthen one.
    pub fn new(spent: u32, bound: u32) -> Self {
        Self {
            spent,
            bound: bound.min(STREAM_QUERY_BOUND),
        }
    }

    /// One more query, when the bound leaves room for it.
    pub fn take(&mut self) -> bool {
        if self.spent >= self.bound {
            return false;
        }
        self.spent += 1;
        true
    }

    #[cfg(test)]
    pub fn spent(&self) -> u32 {
        self.spent
    }
}

/// The served tip as a feed-2 read carries it: the 410's `tip` and the
/// heartbeat's age.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServedTip {
    pub height: u32,
    pub hash: String,
    pub time: u32,
}

/// One feed-2 read: the page, the journal's head and the served tip, in one
/// D1 query (a page request, a stream's open and each of its wakes).
pub struct Wake {
    pub page: Page,
    pub head: u64,
    /// Change 3 (CF2): the retention's floor (`chain_event_state.floor`).
    pub floor: u64,
    pub tip: Option<ServedTip>,
}

impl Wake {
    /// A cursor below the floor: the rows above it are no longer whole.
    pub fn expired(&self, since: u64) -> bool {
        since < self.floor
    }

    /// The 410 body: the floor and the served tip, never a page with a hole.
    pub fn expired_json(&self) -> String {
        let tip = self.tip.as_ref().map_or(
            serde_json::Value::Null,
            |t| serde_json::json!({"height": t.height, "hash": t.hash}),
        );
        serde_json::json!({"v": 1, "error": "cursorExpired", "floor": self.floor.to_string(), "tip": tip})
            .to_string()
    }
}

/// The one query of a feed-2 read. The anchor row is always present, so an
/// empty page still carries the head and the tip.
pub const SQL_WAKE: &str = "SELECT a.head AS head, a.floor AS floor, a.tip AS tip, e.cursor AS cursor, e.payload AS payload FROM (SELECT (SELECT COALESCE(MAX(cursor), 0) FROM chain_events) AS head, (SELECT floor FROM chain_event_state WHERE id = 1) AS floor, (SELECT json_object('height', height, 'hash', hash, 'time', time) FROM chain_event_served_tip) AS tip) a LEFT JOIN (SELECT cursor, payload FROM chain_events WHERE cursor > ? ORDER BY cursor LIMIT ?) e ON 1 ORDER BY e.cursor";

pub async fn read_wake(db: &impl HeaderDb, since: u64, limit: u32) -> Result<Wake> {
    if since > MAX_CURSOR || !(1..=PAGE_SIZE).contains(&limit) {
        return Err(fault("invalid event cursor or page size"));
    }
    #[derive(Deserialize)]
    struct Row {
        head: f64,
        floor: Option<f64>,
        tip: Option<String>,
        cursor: Option<f64>,
        payload: Option<String>,
    }
    let rows: Vec<Row> = Query::new(SQL_WAKE)
        .bind(since)
        .bind(limit + 1)
        .all(db)
        .await?;
    let anchor = rows
        .first()
        .ok_or_else(|| fault("event journal unavailable"))?;
    if anchor.head < 0.0 || anchor.head > MAX_CURSOR as f64 {
        return Err(fault("event cursor exhausted"));
    }
    let head = anchor.head as u64;
    let floor = anchor
        .floor
        .ok_or_else(|| fault("chain_event_state row 1 is missing (migration 0012?)"))?
        as u64;
    let tip = match &anchor.tip {
        Some(raw) => Some(
            serde_json::from_str::<ServedTip>(raw)
                .map_err(|e| fault(format!("served tip unreadable: {e}")))?,
        ),
        None => None,
    };
    let mut events = rows
        .into_iter()
        .filter_map(|r| Some(journal_event(r.cursor? as u64, r.payload?)))
        .collect::<Vec<_>>();
    let has_more = events.len() > limit as usize;
    events.truncate(limit as usize);
    Ok(Wake {
        page: Page {
            cursor: events.last().map_or(since, |e| e.cursor),
            events,
            has_more,
        },
        head,
        floor,
        tip,
    })
}

/// Change 3 (CF2): a chain row is kept 90 days, a `tipAge` row 24 hours, by
/// `created_at` (the owner's word on DECISIONS section 14.1, design
/// section 6).
pub const RETAIN_CHAIN_SECS: u64 = 90 * 86_400;
pub const RETAIN_TIP_AGE_SECS: u64 = 86_400;

/// The rows one tick deletes, and the floor it raises, in one transaction.
/// ?1 the floor read before the batch, ?2 the chain rows' cutoff, ?3 the
/// `tipAge` rows' cutoff (both bound from the Worker's clock, so the two
/// statements pick the same rows). At most 1,000 rows a tick, never the head
/// row. The chain rows are the prefix above the floor older than ?2: the scan
/// by cursor stops at the oldest row younger than ?2. The `tipAge` rows come by their
/// `event_key` index alone. A row of any other kind (`unreadable`'s stored
/// rows) is kept.
const RETAIN_PICKED: &str = "WITH head AS (SELECT COALESCE(MAX(cursor), 0) AS c FROM chain_events), young AS (SELECT COALESCE((SELECT cursor FROM chain_events WHERE cursor > ?1 AND created_at >= ?2 ORDER BY cursor LIMIT 1), (SELECT c FROM head)) AS c), picked AS (SELECT cursor, 1 AS chain FROM chain_events WHERE cursor > ?1 AND cursor < (SELECT c FROM young) AND json_extract(payload, '$.kind') IN ('tip', 'fork', 'reorg', 'invalidated', 'frozen', 'split', 'splitResolved') UNION ALL SELECT cursor, 0 AS chain FROM chain_events WHERE event_key >= 'tipAge:' AND event_key < 'tipAge;' AND created_at < ?3 AND cursor < (SELECT c FROM head) ORDER BY cursor LIMIT 1000)";

pub fn sql_retain_floor() -> String {
    format!("{RETAIN_PICKED} UPDATE chain_event_state SET floor = MAX(floor, COALESCE((SELECT MAX(cursor) FROM picked WHERE chain = 1), 0)) WHERE id = 1")
}

pub fn sql_retain_delete() -> String {
    format!("{RETAIN_PICKED} DELETE FROM chain_events WHERE cursor IN (SELECT cursor FROM picked)")
}

/// Change 3 (CF2), the scheduled tick's retention under feed 2: the floor
/// rises to the greatest deleted chain row's cursor in the transaction that
/// deletes it, so a cursor at or above the floor never reads a hole.
pub async fn retain(db: &impl HeaderDb, now: u64) -> Result<()> {
    #[derive(Deserialize)]
    struct Row {
        floor: f64,
    }
    let floor = Query::new("SELECT floor FROM chain_event_state WHERE id = 1")
        .first::<Row>(db)
        .await?
        .ok_or_else(|| fault("chain_event_state row 1 is missing"))?
        .floor as u64;
    let chain_cutoff = now.saturating_sub(RETAIN_CHAIN_SECS);
    let tip_age_cutoff = now.saturating_sub(RETAIN_TIP_AGE_SECS);
    let bind = |q: Query| q.bind(floor).bind(chain_cutoff).bind(tip_age_cutoff);
    db.batch(vec![
        bind(Query::new(sql_retain_floor())),
        bind(Query::new(sql_retain_delete())),
    ])
    .await
}

/// Seconds since the epoch, from the Worker's clock (the host's in tests).
pub fn now_secs() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        worker::Date::now().as_millis() / 1000
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }
}

/// A feed-2 heartbeat: a comment, so no client parses it and no contract
/// moves; the served tip's age is there for an operator (CF3).
pub fn heartbeat(tip: Option<&ServedTip>, now: u64) -> String {
    match tip {
        Some(tip) => format!(
            ": heartbeat tipAge={}\n\n",
            now.saturating_sub(u64::from(tip.time))
        ),
        None => ": heartbeat\n\n".to_string(),
    }
}

/// The frames a feed-2 stream writes when its bound is spent.
pub const STREAM_END: &str = "retry: 1000\n\n";

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

/// A compatibility view has no `unreadable` shape: the row keeps its id and
/// carries no data, like any kind the view does not match.
pub fn sse_frame(event: &JournalEvent, view: View) -> Result<String> {
    let data = match view {
        View::Envelope => Some(event.payload.clone()),
        _ if event.fault.is_some() => None,
        _ => compatibility_view(&decode(&event.payload)?, view).map(|v| v.to_string()),
    };
    Ok(match data {
        Some(data) => format!("id: {}\ndata: {data}\n\n", event.cursor),
        None => format!("id: {}\n\n", event.cursor),
    })
}

/// The scheduled handler exists at rust-chaintracks@461dc39 src/lib.rs:68-74
/// and wrangler.toml:53-54. Keep age observable even when couriers are down.
/// Feed 1 only: under feed 2 a same tip journals nothing (CF3); the publish
/// trigger's `tipAge` row at each tip change stays (`0009` `chain_event_publish`)
/// and the stream's heartbeat carries the age for an operator.
pub const SQL_TIP_AGE: &str = "INSERT OR IGNORE INTO chain_events(payload, event_key) SELECT json_object('v', 1, 'kind', 'tipAge', 'seconds', MAX(0, unixepoch() - time), 'tip', json(payload)), 'tipAge:' || hash || ':' || (unixepoch() / 60) FROM chain_event_served_tip";

pub async fn timer(db: &impl HeaderDb, feed: Feed) -> Result<()> {
    Query::new("UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1")
        .run(db)
        .await?;
    match feed {
        Feed::V1 => Query::new(SQL_TIP_AGE).run(db).await,
        Feed::V2 => Ok(()),
    }
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
    if hooks.feed() == Feed::V2 {
        return deliver_v2(db, hooks, targets, &token, budget).await;
    }
    for target in targets {
        let key = target_key(&target);
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
        post_events(db, hooks, &target, &key, &token, page.events).await?;
    }
    Ok(())
}

fn target_key(target: &crate::sync::WebhookTarget) -> String {
    format!(
        "{}={}",
        target.binding.as_deref().unwrap_or_default(),
        target.url
    )
}

/// POST each row in order; a 2xx advances the target's cursor, a refusal
/// stops this target at its cursor (the next tick retries it).
async fn post_events(
    db: &impl HeaderDb,
    hooks: &impl TipWebhooks,
    target: &crate::sync::WebhookTarget,
    key: &str,
    token: &str,
    events: Vec<JournalEvent>,
) -> Result<()> {
    for event in events {
        match hooks
            .post_event(target, token, &event.payload, event.cursor)
            .await
        {
            WebhookDelivery::Accepted(_) => {
                Query::new("INSERT INTO chain_event_deliveries(target, cursor) VALUES (?, ?) ON CONFLICT(target) DO UPDATE SET cursor = MAX(chain_event_deliveries.cursor, excluded.cursor)")
                    .bind(key).bind(event.cursor).run(db).await?;
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
    Ok(())
}

/// Change 4, the feed-2 outbox's one read a tick for every target: the
/// journal's head, the floor and every stored delivery cursor.
pub const SQL_OUTBOX: &str = "SELECT a.head AS head, a.floor AS floor, d.target AS target, d.cursor AS cursor FROM (SELECT (SELECT COALESCE(MAX(cursor), 0) FROM chain_events) AS head, (SELECT floor FROM chain_event_state WHERE id = 1) AS floor) a LEFT JOIN chain_event_deliveries d ON 1";

/// A behind target's rows: its cursor is known to stand inside the journal.
pub const SQL_ROWS: &str =
    "SELECT cursor, payload FROM chain_events WHERE cursor > ? ORDER BY cursor LIMIT ?";

/// Change 4: a target at the head costs nothing past the one read; a behind
/// target its page and its POSTs; the at-least-once contract is 0.6.0's. A
/// target with no stored cursor starts at the floor (0 until the retention
/// deletes a chain row): it never held a cursor, so nothing is skipped for
/// it. A stored cursor ahead of the journal or below the floor is logged
/// and that target waits for the operator; the others still run.
async fn deliver_v2(
    db: &impl HeaderDb,
    hooks: &impl TipWebhooks,
    targets: Vec<crate::sync::WebhookTarget>,
    token: &str,
    budget: u32,
) -> Result<()> {
    #[derive(Deserialize)]
    struct Row {
        head: f64,
        floor: Option<f64>,
        target: Option<String>,
        cursor: Option<f64>,
    }
    let rows: Vec<Row> = Query::new(SQL_OUTBOX).all(db).await?;
    let anchor = rows
        .first()
        .ok_or_else(|| fault("event journal unavailable"))?;
    let head = anchor.head as u64;
    let floor = anchor
        .floor
        .ok_or_else(|| fault("chain_event_state row 1 is missing (migration 0012?)"))?
        as u64;
    let stored: std::collections::HashMap<String, u64> = rows
        .iter()
        .filter_map(|r| Some((r.target.clone()?, r.cursor? as u64)))
        .collect();
    for target in targets {
        let key = target_key(&target);
        let since = stored.get(&key).copied().unwrap_or(floor);
        if since == head {
            continue;
        }
        if since > head {
            log_error!("Chain-event target {key} is at {since}, ahead of the journal's {head} (cursorAhead)");
            continue;
        }
        if since < floor {
            log_error!(
                "Chain-event target {key} is at {since}, below the floor {floor} (cursorExpired)"
            );
            continue;
        }
        let rows: Vec<RowsRow> = Query::new(SQL_ROWS)
            .bind(since)
            .bind(budget)
            .all(db)
            .await?;
        let events = rows
            .into_iter()
            .map(|r| journal_event(r.cursor as u64, r.payload))
            .collect();
        post_events(db, hooks, &target, &key, token, events).await?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct RowsRow {
    cursor: f64,
    payload: String,
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
    sse_response(body)
}

fn sse_response<S>(body: S) -> Result<worker::Response>
where
    S: futures_util::Stream<Item = Result<Vec<u8>>> + 'static,
{
    let headers = worker::Headers::new();
    headers.set("Content-Type", "text/event-stream")?;
    headers.set("Cache-Control", "no-store")?;
    headers.set("X-Chain-Event-Version", "2")?;
    headers.set("Access-Control-Expose-Headers", "X-Chain-Event-Version")?;
    headers.set("Access-Control-Allow-Origin", "*")?;
    worker::Response::from_stream(body).map(|r| r.with_headers(headers))
}

fn feed_unavailable(description: &str) -> Vec<u8> {
    // This is a reported feed fault with no id, so a reader cannot
    // acknowledge the bad record. Envelope clients must report it.
    let payload = serde_json::json!({"v":1,"error":"feedUnavailable","description":description});
    format!("event: error\ndata: {payload}\n\n").into_bytes()
}

/// The frames of one feed-2 read: the rows, else a heartbeat with the age.
fn wake_frames(wake: &Wake, view: View) -> Result<String> {
    let mut chunk = String::new();
    for event in &wake.page.events {
        chunk.push_str(&sse_frame(event, view)?);
    }
    if chunk.is_empty() {
        chunk = heartbeat(wake.tip.as_ref(), now_secs());
    }
    Ok(chunk)
}

/// Change 2 (CF4), the feed-2 stream: one D1 query a wake (the page, the
/// head and the tip together, `SQL_WAKE`), and a clean end with `retry:`
/// when `budget` is spent. `opening` is the route's own read at the open,
/// already counted in `budget`.
pub fn stream_v2(
    db: worker::D1Database,
    opening: Wake,
    view: View,
    initial: String,
    budget: QueryBudget,
) -> Result<worker::Response> {
    let mut ready = initial;
    ready.push_str(&wake_frames(&opening, view)?);
    let body = futures_util::stream::unfold(
        Some((
            db,
            opening.page.cursor,
            ready,
            opening.page.has_more,
            budget,
        )),
        move |state| async move {
            let (db, cursor, ready, more, mut budget) = state?;
            if !ready.is_empty() {
                return Some((
                    Ok::<_, Error>(ready.into_bytes()),
                    Some((db, cursor, String::new(), more, budget)),
                ));
            }
            if !budget.take() {
                return Some((Ok(STREAM_END.as_bytes().to_vec()), None));
            }
            if !more {
                worker::Delay::from(std::time::Duration::from_secs(15)).await;
            }
            match read_wake(&db, cursor, PAGE_SIZE).await {
                Ok(wake) if cursor > wake.head => Some((
                    Ok(feed_unavailable(
                        "cursorAhead: cursor belongs to another journal",
                    )),
                    None,
                )),
                // The floor passed this stream's cursor: the reconnect hears
                // the 410 with the floor and the tip.
                Ok(wake) if wake.expired(cursor) => {
                    Some((Ok(STREAM_END.as_bytes().to_vec()), None))
                }
                Ok(wake) => match wake_frames(&wake, view) {
                    Ok(chunk) => Some((
                        Ok(chunk.into_bytes()),
                        Some((
                            db,
                            wake.page.cursor,
                            String::new(),
                            wake.page.has_more,
                            budget,
                        )),
                    )),
                    Err(e) => Some((Err(e), None)),
                },
                Err(e) => Some((Ok(feed_unavailable(&e.to_string())), None)),
            }
        },
    );
    sse_response(body)
}
