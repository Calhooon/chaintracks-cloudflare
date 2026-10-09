//! E5 (Rule 28): the push source. One Durable Object holds the outbound
//! Server-Sent Events stream of a peer header service's tip
//! (`ARCADE_URL` + `/chaintracks/v2/tip/stream`, `sse.rs` for the format at
//! the pin) and hands every tip it carries to the one door,
//! `push::ingest_announced`, in-process. The peer is believed for nothing:
//! each header is re-derived by the service's rules before it counts.
//!
//! The object does not count on staying resident between events
//! (bsv-stack-lean `docs/p0/rule-28-chaintracks.md`, the design note). It
//! reads inside its alarm handler, one session at a time: the handler first
//! sets a watchdog alarm past the session's end, connects, reads until the
//! session's budget is spent (then it reconnects at once), the peer closes,
//! a read fails, or no byte arrives for the heartbeat interval (then it
//! reconnects after a backoff). An alarm handler may run 15 minutes of wall
//! time (Cloudflare, Durable Objects limits), so a session is 10. If the
//! object is evicted mid-session the watchdog reconnects it; if the object
//! is gone altogether (no alarm, not reading) the minute cron wakes it
//! (`/wake`). The last event id is kept in the object's storage and sent as
//! `Last-Event-ID` on every reconnect (Arcade sends none and opens every
//! connection with its current tip, so a gap is filled by the door's parent
//! walk; a peer that sends ids replays from it).

// The object and the cron's wake are wasm only; on the host the suite runs
// the pure half (the state, the backoff, the wake rule).
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use serde::{Deserialize, Serialize};

/// The route of the peer's tip stream under `ARCADE_URL`
/// ([SRC] arcade@1ae1208 services/chaintracks_server/service.go:150 and
/// routes.go:209).
pub(crate) const STREAM_PATH: &str = "/chaintracks/v2/tip/stream";

/// The binding and the one object's name.
pub(crate) const BINDING: &str = "TIP_STREAM";
pub(crate) const OBJECT_NAME: &str = "tip";

/// No byte from the peer for this long and the stream is dead: three of
/// Arcade's 15-second keepalives ([SRC] arcade@1ae1208
/// services/chaintracks_server/routes.go:519-536). `TIP_STREAM_HEARTBEAT_S`
/// overrides it (the local check runs it at 3).
pub(crate) const HEARTBEAT_S: u64 = 45;

/// One session's wall time, inside the alarm handler's 15 minutes.
/// `TIP_STREAM_SESSION_S` overrides it.
pub(crate) const SESSION_S: u64 = 600;

/// The first reconnect after a drop, doubled per drop in a row to the cap.
pub(crate) const BACKOFF_FIRST_MS: u64 = 1_000;
pub(crate) const BACKOFF_CAP_MS: u64 = 60_000;

/// The poll's interval behind a live push: the minute cron still asks the
/// couriers when the push has stored nothing for this long and no poll has
/// run in it (an average block interval: a quiet chain costs one poll in ten
/// minutes, not ten), and every minute while the stream is down.
/// `TIP_STREAM_QUIET_S` overrides it.
pub(crate) const POLL_QUIET_S: u64 = 600;

/// The wait before the next connect after `drops_in_row` drops in a row.
pub(crate) fn backoff_ms(drops_in_row: u32) -> u64 {
    let shift = drops_in_row.saturating_sub(1).min(16);
    (BACKOFF_FIRST_MS << shift).min(BACKOFF_CAP_MS)
}

/// What the object knows, in its storage under one key and in memory while
/// it reads. Times are milliseconds since the epoch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct StreamState {
    /// The `id` of the last event the peer sent, sent back on reconnect.
    pub last_event_id: Option<String>,
    /// The served tip after the last event.
    pub last_tip_height: u32,
    pub last_tip_hash: String,
    /// A session is reading the stream.
    pub reading: bool,
    pub connected_at: u64,
    /// The last byte from the peer (an event or a keepalive).
    pub last_beat_at: u64,
    /// The last push the door stored (a new header, not a repeat).
    pub last_push_at: u64,
    pub connects: u32,
    pub drops: u32,
    pub drops_in_row: u32,
    pub last_drop: Option<String>,
    /// Tip events read; of them stored, known (already served), refused.
    pub events: u32,
    pub stored: u32,
    pub known: u32,
    pub refused: u32,
    pub last_fault: Option<String>,
    /// Courier requests the door's parent walk made.
    pub walk_requests: u32,
}

impl StreamState {
    /// The stream is up: a session is reading and the peer spoke within the
    /// heartbeat interval.
    pub fn live(&self, now: u64, heartbeat_ms: u64) -> bool {
        self.reading && now.saturating_sub(self.last_beat_at) <= heartbeat_ms
    }

    /// The cron should bring the alarm forward: the stream is not live and
    /// no reconnect is due within a minute (none scheduled, or only the
    /// watchdog of a session that died with its object).
    pub fn wake_due(&self, now: u64, heartbeat_ms: u64, alarm: Option<u64>) -> bool {
        !self.live(now, heartbeat_ms) && alarm.is_none_or(|at| at > now + 60_000)
    }
}

/// The poll's record, under its own key: the cron writes it through `/wake`
/// and `/polled` while a session may be reading (the session writes only
/// `StreamState`, so neither overwrites the other).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct PollState {
    pub last_poll_at: u64,
    /// Cron ticks that asked the couriers, and that did not.
    pub polls_run: u32,
    pub polls_skipped: u32,
    /// The courier requests the polls made (`CourierLadder::requests`).
    pub poll_requests: u32,
    pub last_reason: Option<String>,
}

/// Whether this cron tick asks the couriers, and why: always while the
/// stream is down; behind a live stream only when neither a push nor a poll
/// has landed within the quiet interval. `None`: the push covers the tick.
pub(crate) fn poll_due(
    stream: &StreamState,
    poll: &PollState,
    now: u64,
    heartbeat_ms: u64,
    quiet_ms: u64,
) -> Option<&'static str> {
    if !stream.live(now, heartbeat_ms) {
        return Some("the stream is down");
    }
    let heard = stream.last_push_at.max(poll.last_poll_at);
    (now.saturating_sub(heard) >= quiet_ms).then_some("the push was quiet for the interval")
}

#[cfg(target_arch = "wasm32")]
mod object {
    use super::*;
    use crate::storage::IngestError;
    use crate::{events, push, sse, sync};
    use futures_util::future::{select, Either};
    use futures_util::StreamExt;
    use std::cell::RefCell;
    use std::time::Duration;
    use worker::*;

    const KEY: &str = "state";
    const POLL_KEY: &str = "poll";

    fn now_ms() -> u64 {
        Date::now().as_millis()
    }

    struct Config {
        url: String,
        heartbeat_ms: u64,
        session_ms: u64,
        quiet_ms: u64,
    }

    impl Config {
        fn from_env(env: &Env) -> Self {
            let var = |name: &str| {
                env.var(name)
                    .map(|v| v.to_string())
                    .ok()
                    .filter(|s| !s.is_empty())
            };
            let secs = |name: &str, default: u64| {
                var(name).and_then(|v| v.parse().ok()).unwrap_or(default) * 1000
            };
            let base = var("ARCADE_URL")
                .unwrap_or_else(|| crate::couriers::ARCADE_DEFAULT_URL.to_string());
            Self {
                url: format!("{}{STREAM_PATH}", base.trim_end_matches('/')),
                heartbeat_ms: secs("TIP_STREAM_HEARTBEAT_S", HEARTBEAT_S),
                session_ms: secs("TIP_STREAM_SESSION_S", SESSION_S),
                quiet_ms: secs("TIP_STREAM_QUIET_S", POLL_QUIET_S),
            }
        }
    }

    #[durable_object]
    pub struct TipStream {
        state: State,
        env: Env,
        /// The state while a session reads (keepalives are kept here, not
        /// written); `None` until loaded from storage.
        live: RefCell<Option<StreamState>>,
    }

    impl DurableObject for TipStream {
        fn new(state: State, env: Env) -> Self {
            Self {
                state,
                env,
                live: RefCell::new(None),
            }
        }

        /// `POST /wake` (the cron): bring the alarm forward when the stream
        /// is gone; answers `{woke, live, state}`. `GET /status`: the state.
        async fn fetch(&self, req: Request) -> Result<Response> {
            let cfg = Config::from_env(&self.env);
            let st = self.load().await;
            let now = now_ms();
            match (req.method(), req.path().as_str()) {
                (Method::Post, "/wake") => {
                    let storage = self.state.storage();
                    let alarm = storage.get_alarm().await?.map(|v| v as u64);
                    let woke = st.wake_due(now, cfg.heartbeat_ms, alarm);
                    if woke {
                        storage.set_alarm(0i64).await?;
                        log!("tip stream: woken by the cron (alarm {alarm:?})");
                    }
                    let mut poll = self.poll().await;
                    let due = poll_due(&st, &poll, now, cfg.heartbeat_ms, cfg.quiet_ms);
                    match due {
                        Some(why) => {
                            poll.polls_run += 1;
                            poll.last_poll_at = now;
                            poll.last_reason = Some(why.to_string());
                        }
                        None => poll.polls_skipped += 1,
                    }
                    storage.put(POLL_KEY, &poll).await?;
                    Response::from_json(&serde_json::json!({
                        "woke": woke,
                        "live": st.live(now, cfg.heartbeat_ms),
                        "poll": due.is_some(),
                        "reason": due,
                        "state": st,
                        "polls": poll,
                    }))
                }
                (Method::Post, "/polled") => {
                    let requests: u32 = req
                        .url()?
                        .query_pairs()
                        .find(|(k, _)| k == "requests")
                        .and_then(|(_, v)| v.parse().ok())
                        .unwrap_or(0);
                    let mut poll = self.poll().await;
                    poll.poll_requests += requests;
                    self.state.storage().put(POLL_KEY, &poll).await?;
                    Response::from_json(&poll)
                }
                (Method::Get, "/status") => Response::from_json(&serde_json::json!({
                    "live": st.live(now, cfg.heartbeat_ms),
                    "state": st,
                    "polls": self.poll().await,
                })),
                _ => Response::error("not found", 404),
            }
        }

        async fn alarm(&self) -> Result<Response> {
            self.session().await;
            Response::ok("")
        }
    }

    impl TipStream {
        async fn load(&self) -> StreamState {
            if let Some(st) = self.live.borrow().clone() {
                return st;
            }
            let stored = self
                .state
                .storage()
                .get::<StreamState>(KEY)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            *self.live.borrow_mut() = Some(stored.clone());
            stored
        }

        async fn poll(&self) -> PollState {
            self.state
                .storage()
                .get::<PollState>(POLL_KEY)
                .await
                .ok()
                .flatten()
                .unwrap_or_default()
        }

        fn publish(&self, st: &StreamState) {
            *self.live.borrow_mut() = Some(st.clone());
        }

        async fn save(&self, st: &StreamState) {
            self.publish(st);
            if let Err(e) = self.state.storage().put(KEY, st).await {
                log_error!("tip stream: state not saved: {e:?}");
            }
        }

        async fn session(&self) {
            let cfg = Config::from_env(&self.env);
            let storage = self.state.storage();
            // The watchdog: if this object dies mid-session, the alarm reconnects.
            let watchdog = (cfg.session_ms + cfg.heartbeat_ms + 60_000) as i64;
            if let Err(e) = storage.set_alarm(watchdog).await {
                log_error!("tip stream: watchdog not set: {e:?}");
            }
            let mut st = self.load().await;
            let now = now_ms();
            st.connects += 1;
            st.connected_at = now;
            st.last_beat_at = now;
            st.reading = true;
            self.save(&st).await;
            log!(
                "tip stream: connect {} to {} (last event id {:?})",
                st.connects,
                cfg.url,
                st.last_event_id
            );

            let dropped = self.read(&cfg, &mut st).await;
            st.reading = false;
            let next = match dropped {
                None => 0,
                Some(why) => {
                    st.drops += 1;
                    st.drops_in_row += 1;
                    let wait = backoff_ms(st.drops_in_row);
                    log!("tip stream: dropped ({why}); reconnect in {wait} ms");
                    st.last_drop = Some(why);
                    wait
                }
            };
            self.save(&st).await;
            if let Err(e) = storage.set_alarm(next as i64).await {
                log_error!("tip stream: reconnect alarm not set (the watchdog stands): {e:?}");
            }
        }

        /// One session: `None` when its budget is spent, the drop's reason
        /// otherwise.
        async fn read(&self, cfg: &Config, st: &mut StreamState) -> Option<String> {
            let headers = Headers::new();
            let _ = headers.set("Accept", "text/event-stream");
            let _ = headers.set("Cache-Control", "no-cache");
            if let Some(id) = &st.last_event_id {
                let _ = headers.set("Last-Event-ID", id);
            }
            let mut init = RequestInit::new();
            init.with_method(Method::Get).with_headers(headers);
            let req = match Request::new_with_init(&cfg.url, &init) {
                Ok(r) => r,
                Err(e) => return Some(format!("request: {e}")),
            };
            let mut resp = match Fetch::Request(req).send().await {
                Ok(r) => r,
                Err(e) => return Some(format!("connect: {e}")),
            };
            if resp.status_code() != 200 {
                return Some(format!("connect: status {}", resp.status_code()));
            }
            let mut body = match resp.stream() {
                Ok(b) => Box::pin(b),
                Err(e) => return Some(format!("body: {e}")),
            };
            let mut parser = sse::Parser::default();
            let deadline = st.connected_at + cfg.session_ms;
            loop {
                let now = now_ms();
                if now >= deadline {
                    return None;
                }
                let wait = cfg.heartbeat_ms.min(deadline - now);
                let timer = Delay::from(Duration::from_millis(wait));
                match select(body.next(), timer).await {
                    Either::Right(_) => {
                        if now_ms() >= deadline {
                            return None;
                        }
                        return Some(format!(
                            "heartbeat: no byte from the peer in {} s",
                            cfg.heartbeat_ms / 1000
                        ));
                    }
                    Either::Left((None, _)) => return Some("the peer closed the stream".into()),
                    Either::Left((Some(Err(e)), _)) => return Some(format!("read: {e}")),
                    Either::Left((Some(Ok(bytes)), _)) => {
                        st.last_beat_at = now_ms();
                        let items = match parser.feed(&bytes) {
                            Ok(items) => items,
                            Err(e) => return Some(e),
                        };
                        for item in items {
                            if let sse::Item::Event { id, event, data } = item {
                                if id.is_some() {
                                    st.last_event_id = id;
                                }
                                if event == "message" || event == "tip" {
                                    self.tip(st, &data).await;
                                }
                            }
                        }
                        self.publish(st);
                    }
                }
            }
        }

        /// One tip event through the door.
        async fn tip(&self, st: &mut StreamState, data: &str) {
            st.events += 1;
            let db = match self.env.d1("DB") {
                Ok(db) => db,
                Err(e) => {
                    st.last_fault = Some(format!("store: {e}"));
                    return;
                }
            };
            let header = match sse::tip_header(data) {
                Ok(Some(h)) => h,
                Ok(None) => return,
                Err(e) => {
                    log_error!("push: refused: {e}");
                    sync::record_fault(&db, &format!("push: {e}")).await;
                    st.refused += 1;
                    st.last_fault = Some(e);
                    self.save(st).await;
                    return;
                }
            };
            let chain = sync::chain_of(&self.env);
            let params = match sync::chain_params(&self.env, &chain) {
                Ok(p) => p,
                Err(e) => {
                    st.last_fault = Some(format!("rules: {e}"));
                    return;
                }
            };
            let ladder = sync::courier_ladder(&self.env, &chain);
            match push::ingest_announced(&db, &params, &ladder, &self.env, header).await {
                Ok(a) => {
                    st.drops_in_row = 0;
                    st.last_tip_height = a.tip_height;
                    st.last_tip_hash = a.tip_hash.clone();
                    if a.stored() {
                        st.stored += 1;
                        st.last_push_at = now_ms();
                        log!(
                            "tip stream: {} (walked {}){}, the tip {} {}",
                            a.outcome,
                            a.walked,
                            a.reason.map(|r| format!(": {r}")).unwrap_or_default(),
                            a.tip_height,
                            a.tip_hash
                        );
                        if let Err(e) =
                            events::deliver(&db, &sync::EventWebhookConfig(&self.env)).await
                        {
                            log_error!("Chain-event outbox error (cursor retained): {e:?}");
                        }
                    } else {
                        st.known += 1;
                    }
                }
                Err(IngestError::Refused(e)) => {
                    st.refused += 1;
                    st.last_fault = Some(e.to_string());
                }
                Err(IngestError::Store(e)) => {
                    log_error!("tip stream: store fault: {e:?}");
                    st.last_fault = Some(format!("store: {e}"));
                }
            }
            st.walk_requests += ladder.requests();
            self.save(st).await;
        }
    }
}

/// The cron's half (wasm only): wake the object if it is gone, and learn
/// whether this tick polls. Answers the object's reply, or `None` when the
/// binding is absent or the call fails (the poll then runs as it always has).
#[cfg(target_arch = "wasm32")]
pub(crate) async fn wake(env: &worker::Env) -> Option<serde_json::Value> {
    call(env, "https://tip-stream/wake").await
}

/// The cron's record of a poll's courier requests.
#[cfg(target_arch = "wasm32")]
pub(crate) async fn polled(env: &worker::Env, requests: u32) {
    let _ = call(
        env,
        &format!("https://tip-stream/polled?requests={requests}"),
    )
    .await;
}

#[cfg(target_arch = "wasm32")]
async fn call(env: &worker::Env, url: &str) -> Option<serde_json::Value> {
    let stub = env
        .durable_object(BINDING)
        .ok()?
        .id_from_name(OBJECT_NAME)
        .ok()?
        .get_stub()
        .ok()?;
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Post);
    let req = worker::Request::new_with_init(url, &init).ok()?;
    match stub.fetch_with_request(req).await {
        Ok(mut resp) => resp.json().await.ok(),
        Err(e) => {
            log_error!("Cron: the tip stream's object did not answer {url}: {e:?}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_backoff_doubles_from_a_second_to_a_minute() {
        let waits: Vec<u64> = (1..=9).map(backoff_ms).collect();
        assert_eq!(
            waits,
            vec![1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000, 60_000]
        );
        assert_eq!(backoff_ms(u32::MAX), BACKOFF_CAP_MS);
    }

    #[test]
    fn the_stream_is_live_while_reading_and_the_peer_spoke_within_the_heartbeat() {
        let hb = HEARTBEAT_S * 1000;
        let st = StreamState {
            reading: true,
            last_beat_at: 1_000_000,
            ..Default::default()
        };
        assert!(st.live(1_000_000 + hb, hb));
        assert!(!st.live(1_000_001 + hb, hb), "silent past the heartbeat");
        let closed = StreamState {
            reading: false,
            ..st.clone()
        };
        assert!(!closed.live(1_000_000, hb), "a drop is not live");
    }

    #[test]
    fn the_cron_wakes_the_object_only_when_it_is_gone() {
        let hb = HEARTBEAT_S * 1000;
        let now = 10_000_000;
        let reading = StreamState {
            reading: true,
            last_beat_at: now - 1_000,
            ..Default::default()
        };
        assert!(
            !reading.wake_due(now, hb, Some(now + 700_000)),
            "live: left alone"
        );
        let gone = StreamState::default();
        assert!(gone.wake_due(now, hb, None), "no alarm, not reading: woken");
        assert!(
            gone.wake_due(now, hb, Some(now + 700_000)),
            "only a dead session's watchdog: woken"
        );
        assert!(
            !gone.wake_due(now, hb, Some(now + 4_000)),
            "a reconnect due within the minute: left alone"
        );
        let died = StreamState {
            reading: true,
            last_beat_at: now - hb - 1,
            ..Default::default()
        };
        assert!(
            died.wake_due(now, hb, Some(now + 600_000)),
            "evicted mid-session: woken"
        );
    }

    #[test]
    fn the_state_reads_back_from_an_older_record_with_defaults() {
        let st: StreamState = serde_json::from_str(r#"{"lastEventId":"7","connects":3}"#).unwrap();
        assert_eq!(st.last_event_id.as_deref(), Some("7"));
        assert_eq!((st.connects, st.drops), (3, 0));
    }

    #[test]
    fn the_poll_runs_while_the_stream_is_down_and_once_a_quiet_interval_behind_it() {
        let hb = HEARTBEAT_S * 1000;
        let quiet = POLL_QUIET_S * 1000;
        let now = 100_000_000;
        let down = StreamState::default();
        let none = PollState::default();
        assert_eq!(
            poll_due(&down, &none, now, hb, quiet),
            Some("the stream is down")
        );
        let live = StreamState {
            reading: true,
            last_beat_at: now - 2_000,
            last_push_at: now - 60_000,
            ..Default::default()
        };
        assert_eq!(
            poll_due(&live, &none, now, hb, quiet),
            None,
            "a push a minute ago covers the tick"
        );
        let quiet_chain = StreamState {
            last_push_at: now - quiet,
            ..live.clone()
        };
        assert_eq!(
            poll_due(&quiet_chain, &none, now, hb, quiet),
            Some("the push was quiet for the interval")
        );
        let polled = PollState {
            last_poll_at: now - 60_000,
            ..Default::default()
        };
        assert_eq!(
            poll_due(&quiet_chain, &polled, now, hb, quiet),
            None,
            "one poll per quiet interval, not one a minute"
        );
        // A quiet hour behind a live stream, a cron tick a minute.
        let mut record = PollState::default();
        let mut runs = 0;
        for m in 0..60u64 {
            let t = now + m * 60_000;
            let beating = StreamState {
                last_beat_at: t,
                ..quiet_chain.clone()
            };
            if poll_due(&beating, &record, t, hb, quiet).is_some() {
                runs += 1;
                record.last_poll_at = t;
            }
        }
        assert_eq!(runs, 6, "a quiet live hour polls six times, not sixty");
    }
}
