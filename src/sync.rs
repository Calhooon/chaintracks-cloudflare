//! Cron-triggered chain synchronization.
//!
//! Polls the courier ladder (`couriers.rs`: WhatsOnChain, Arcade, Bitails as
//! each other's fallbacks) for new blocks, ingests headers into D1. During
//! catch-up, optionally fetches bulk headers from an upstream chaintracks
//! instance, a peer (UPSTREAM_CHAINTRACKS_URL env var) ; much faster than
//! one-by-one. Falls back to the ladder if upstream is unset or fails.
//!
//! Rule 28: every read here leaves the service because headers come from
//! outside it (the irreducible case; the reason and the fallback shape are
//! named at `couriers.rs`), and every answer is re-derived locally before it
//! counts. The minute poll is the routine read of the three; the push source
//! that goes ahead of it is designed in bsv-stack-lean
//! `docs/p0/rule-28-chaintracks.md`.

use worker::*;

use std::collections::HashMap;

use crate::consensus::ChainParams;
use crate::d1::{BatchCollector, HeaderDb, QVal};
use crate::storage;
use crate::types::{BlockHeader, Chain};
use crate::woc::{WocChainInfo, WocClient};

/// Called every minute by the cron trigger.
///
/// Two modes:
/// - **Catch-up** (gap > 10): fetch bulk hex from production chaintracks,
///   parse 80-byte headers, batch insert. ~1000 headers per request.
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

    let upstream_url = env
        .var("UPSTREAM_CHAINTRACKS_URL")
        .map(|v| v.to_string())
        .ok()
        .filter(|s| !s.is_empty());

    let params = chain_params(env, &chain)?;

    // E5: the push source goes ahead of the poll. The object holding the
    // peer's tip stream is woken if it is gone, and answers whether this
    // tick asks the couriers (`tip_stream::poll_due`: every minute while the
    // stream is down, once a quiet interval behind it). No answer (no
    // binding, a fault) is a poll, as before E5.
    #[cfg(target_arch = "wasm32")]
    let wake = crate::tip_stream::wake(env).await;
    #[cfg(not(target_arch = "wasm32"))]
    let wake: Option<serde_json::Value> = None;
    let poll = wake
        .as_ref()
        .and_then(|w| w.get("poll"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let result = if poll {
        let ladder = courier_ladder(env, &chain);
        let result = run_cron(&db, &params, &ladder, upstream_url.as_deref(), env).await;
        let requests = ladder.requests();
        log!(
            "Cron: the poll asked the couriers {requests} times ({})",
            wake.as_ref()
                .and_then(|w| w.get("reason"))
                .and_then(|v| v.as_str())
                .unwrap_or("no push source answered")
        );
        #[cfg(target_arch = "wasm32")]
        crate::tip_stream::polled(env, requests).await;
        result
    } else {
        log!("Cron: the push is live; the couriers are not asked this tick");
        run_pushed_tick(&db, &params, env).await
    };
    if let Err(e) = crate::events::deliver(&db, &EventWebhookConfig(env)).await {
        log_error!("Chain-event outbox error (cursor retained): {e:?}");
    }
    result
}

/// E5: a cron tick the push covers. Everything of the cron that reads no
/// courier still runs: the tip's age (#32), one step of the re-validation
/// (P0-4), the announce (a refused delivery is retried every minute) and the
/// cumulative-work repair.
pub(crate) async fn run_pushed_tick(
    db: &impl HeaderDb,
    params: &ChainParams,
    hooks: &impl TipWebhooks,
) -> Result<()> {
    crate::events::timer(db).await?;
    revalidate(db, params).await;
    finish_sync(db, hooks, None).await;
    Ok(())
}

/// The courier ladder of this deploy, for one cron tick or one request:
/// WhatsOnChain (its key a worker SECRET, with a var fallback for local dev;
/// `env.var()` fails silently on secrets), Arcade and Bitails as each other's
/// fallbacks, the start rotating per minute (bsv-low loop 10 D5: loop 9's
/// 965877 left the store 23 min behind on one courier's refusal). Rule 28:
/// every path that reads a header from outside builds THIS ladder, the cron,
/// the read-through (H12) and the operator's backfill (H13), so no path
/// depends on one courier and the three cannot drift apart.
pub(crate) fn courier_ladder(
    env: &Env,
    chain: &Chain,
) -> crate::couriers::CourierLadder<crate::couriers::Courier> {
    let api_key = env
        .secret("WHATSONCHAIN_API_KEY")
        .map(|v| v.to_string())
        .ok()
        .or_else(|| env.var("WHATSONCHAIN_API_KEY").map(|v| v.to_string()).ok())
        .filter(|s| !s.is_empty());
    let var = |name: &str| {
        env.var(name)
            .map(|v| v.to_string())
            .ok()
            .filter(|s| !s.is_empty())
    };
    let minute = Date::now().as_millis() / 60_000;
    crate::couriers::CourierLadder::for_chain(
        chain,
        WocClient::new(chain, api_key),
        var("ARCADE_URL"),
        var("BITAILS_URL"),
        minute,
    )
}

/// The deploy's chain from the `CHAIN` var (`"test"`, else main), as the
/// cron and the routes read it.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn chain_of(env: &Env) -> Chain {
    match env
        .var("CHAIN")
        .map(|v| v.to_string())
        .unwrap_or_default()
        .as_str()
    {
        "test" => Chain::Test,
        _ => Chain::Main,
    }
}

/// The chain's rules (P0-4): the node's parameters for `chain`, with the
/// owner's checkpoints from the `CHECKPOINTS` var (`"height:hash,..."`) on
/// top of the node's list. A malformed var is an error, never ignored: the
/// cron skips and every route answers 503 until it is fixed.
pub(crate) fn chain_params(env: &Env, chain: &Chain) -> Result<ChainParams> {
    let params = ChainParams::for_chain(chain);
    match env.var("CHECKPOINTS").map(|v| v.to_string()) {
        Ok(spec) if !spec.trim().is_empty() => params
            .with_checkpoints(&spec)
            .map_err(|e| Error::RustError(format!("CHECKPOINTS: {e}"))),
        _ => Ok(params),
    }
}

/// bsv-low loop 10 D5: the courier health the cron records (migration 0007),
/// best-effort and LOUD: a missing column (the migration not applied) never
/// stops the sync, and `/getInfo` names it (`syncSchemaFault`). Binds: ?1 the
/// highest tip any courier answered this tick.
pub(crate) const RECORD_SEEN_SQL: &str =
    "UPDATE sync_state SET last_seen_height = ?1, last_seen_at = datetime('now') WHERE id = 1";
/// The LAST poll fault, with its time; never cleared (a reader judges its
/// age). Binds: ?1 the fault text (capped).
pub(crate) const RECORD_FAULT_SQL: &str =
    "UPDATE sync_state SET last_error = ?1, last_error_at = datetime('now') WHERE id = 1";
const FAULT_TEXT_CAP: usize = 480;

async fn record_seen(db: &impl HeaderDb, height: u32) {
    if let Err(e) = crate::d1::Query::new(RECORD_SEEN_SQL)
        .bind(height)
        .run(db)
        .await
    {
        log_error!(
            "Cron: could not record the seen height {height} (migration 0007 applied?): {e:?}"
        );
    }
}

/// E5: a pushed header's height recorded as the couriers' answer is
/// (`/getPresentHeight` serves the larger of it and the served tip), only
/// when it is above what stands, so a push of an older header never lowers
/// the record. A read fault is loud and skips the write, as below.
pub(crate) async fn note_seen(db: &impl HeaderDb, height: u32) {
    #[derive(serde::Deserialize)]
    struct SeenRow {
        last_seen_height: Option<f64>,
    }
    match crate::d1::Query::new(storage::SQL_COURIER_HEALTH)
        .first::<SeenRow>(db)
        .await
    {
        Ok(row) => {
            let seen = row.and_then(|r| r.last_seen_height.map(|v| v as u32));
            if seen.is_none_or(|s| s < height) {
                record_seen(db, height).await;
            }
        }
        Err(e) => {
            log_error!("push: could not read the courier record (migration 0007 applied?): {e:?}")
        }
    }
}

/// The idle tick's half of the record (D5): read what stands; write the
/// couriers' answer only when it differs. A read fault is loud and skips the
/// write (the migration not applied: `/getInfo` names it).
async fn keep_seen_current(db: &impl HeaderDb, woc_height: u32) {
    #[derive(serde::Deserialize)]
    struct SeenRow {
        last_seen_height: Option<f64>,
    }
    let seen = match crate::d1::Query::new(storage::SQL_COURIER_HEALTH)
        .first::<SeenRow>(db)
        .await
    {
        Ok(row) => row.and_then(|r| r.last_seen_height.map(|v| v as u32)),
        Err(e) => {
            log_error!("Cron: could not read the courier record (migration 0007 applied?): {e:?}");
            return;
        }
    };
    if seen != Some(woc_height) {
        record_seen(db, woc_height).await;
    }
}

pub(crate) async fn record_fault(db: &impl HeaderDb, text: &str) {
    let capped: String = text.chars().take(FAULT_TEXT_CAP).collect();
    if let Err(e) = crate::d1::Query::new(RECORD_FAULT_SQL)
        .bind(capped)
        .run(db)
        .await
    {
        log_error!("Cron: could not record the poll fault (migration 0007 applied?): {e:?}");
    }
}

/// The chain the cron syncs from: WhatsOnChain on the worker (`WocClient`),
/// a scripted chain on the host (round 4), so the cron's own decision tree
/// (idle, the equal-height competitor, live, the WoC fallback of catch-up)
/// runs in `cargo test`. The upstream bulk fetch is the one path that stays
/// worker-only (`fetch_headers_from_upstream`, a public fetch).
pub(crate) trait ChainSource {
    async fn chain_info(&self) -> Result<WocChainInfo>;
    async fn header_by_height(&self, height: u32) -> Result<BlockHeader>;
    async fn header_by_hash(&self, hash: &str) -> Result<BlockHeader>;
    /// bsv-low loop 10 D5: the tick's per-courier tally for the log (the
    /// ladder answers; a single source has nothing to report).
    fn report(&self) -> Option<String> {
        None
    }
}

impl ChainSource for WocClient {
    async fn chain_info(&self) -> Result<WocChainInfo> {
        self.get_chain_info().await
    }

    async fn header_by_height(&self, height: u32) -> Result<BlockHeader> {
        self.get_header_by_height(height).await
    }

    async fn header_by_hash(&self, hash: &str) -> Result<BlockHeader> {
        self.get_header_by_hash(hash).await
    }
}

/// The cron's body, on any store, chain source and webhook consumer.
pub(crate) async fn run_cron(
    db: &impl HeaderDb,
    params: &ChainParams,
    source: &impl ChainSource,
    upstream_url: Option<&str>,
    hooks: &impl TipWebhooks,
) -> Result<()> {
    // #32: age is emitted even when the source is unreachable. This timer
    // uses the verified served tip and deduplicates overlapping cron ticks.
    crate::events::timer(db).await?;
    // The tip, read ONCE per cron (round 5, LOW-4): the height decides the
    // sync mode, the competitor check compares its hash, and the announce
    // reuses it unless a competitor landed meanwhile.
    let mut tip = storage::find_chain_tip(db).await?;
    let our_height = tip.as_ref().map_or(0, |t| t.height);

    let chain_info = match source.chain_info().await {
        Ok(info) => info,
        Err(e) => {
            log_error!("Cron: no courier answered the tip, skipping the sync: {e:?}");
            // D5: recorded (the last fault, with its time), never silent.
            record_fault(db, &format!("chain info: {e:?}")).await;
            if let Some(r) = source.report() {
                log!("Cron: couriers: {r}");
            }
            // round 4 (MED-1): the announce still runs, so a refused delivery
            // is retried by every cron, not only by one that moved the tip.
            announce_tip(db, hooks, tip).await;
            return Ok(());
        }
    };
    let woc_height = chain_info.blocks;

    if woc_height <= our_height {
        // Equal height is NOT automatically "in sync" (audit C2): if WoC's
        // best hash differs from our tip hash at the same height, the
        // network reorged to a competitor we can never fetch by height,
        // the old code returned here and served the losing branch forever.
        if woc_height == our_height && our_height > 0 {
            if let (Some(best_hash), Some(our_tip)) =
                (chain_info.best_block_hash.as_deref(), tip.as_ref())
            {
                if !our_tip.hash.eq_ignore_ascii_case(best_hash) {
                    log!(
                        "Cron: equal-height branch mismatch at {} (ours {} vs WoC {}), fetching competitor",
                        our_height, our_tip.hash, best_hash
                    );
                    match source.header_by_hash(best_hash).await {
                        Ok(header) => {
                            match insert_with_parent_backfill(db, params, source, header).await {
                                Ok(_) => {
                                    // a competitor landed: the tip may have flipped
                                    tip = storage::find_chain_tip(db).await?;
                                }
                                Err(e) => {
                                    // D5: recorded, never an abort before the tail.
                                    log_error!("Cron: competitor {best_hash} at {our_height} could not be ingested: {e:?}");
                                    record_fault(
                                        db,
                                        &format!("competitor {best_hash} at {our_height}: {e:?}"),
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            log_error!("Cron: competitor fetch failed: {e:?}");
                            record_fault(
                                db,
                                &format!("competitor {best_hash} at {our_height}: {e:?}"),
                            )
                            .await;
                        }
                    }
                }
            }
        }
        // D5 (the 21:41Z deploy of 2026-09-08 landed on a chain idle for 40
        // minutes): the idle cron keeps the courier record CURRENT, one read a
        // tick and one write only when the record is behind the couriers'
        // answer (after migration 0007, or after an ingest that bypassed the
        // cron), so the launch gate reads lag 0 on a quiet chain and never
        // "unknown"; round 5's "writes nothing" holds whenever the record is
        // current. The tally is logged on every tick (the quiet chain shows
        // the ladder answering).
        keep_seen_current(db, woc_height).await;
        revalidate(db, params).await;
        if let Some(r) = source.report() {
            log!("Cron: idle at {our_height}; couriers: {r}");
        }
        // round 4 (MED-1): the idle cron announces too (the announce row and
        // the deliveries read; a retry claim only when a target is owed): a
        // refused delivery is retried every minute, and the equal-height
        // competitor ingested just above (the 2026-09-07 shape) is announced
        // the moment it flips the tip.
        announce_tip(db, hooks, tip).await;
        return Ok(());
    }

    // D5: the highest tip any courier answered, recorded on every cron that
    // has work (the idle cron writes nothing, round 5 LOW-4; an idle tick's
    // lag is 0 by construction). /getInfo serves this minus the stored tip.
    record_seen(db, woc_height).await;
    let gap = woc_height - our_height;

    if gap > 10 {
        // ─── Catch-up: bulk fetch from upstream chaintracks if configured ─
        // getHeaders returns concatenated 80-byte hex , 1000 headers/request.
        // If no upstream configured, fall straight through to WoC one-by-one.
        let batch_size = 1000u32;
        let max_per_cycle = 5000u32; // 5 requests × 1000 headers
        let end_height = (our_height + max_per_cycle).min(woc_height);

        log!(
            "Cron: catch-up {} blocks ({} → {end_height})",
            end_height - our_height,
            our_height + 1
        );

        let mut height = our_height + 1;
        let mut used_fallback = upstream_url.is_none();
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
            let upstream_result = match upstream_url {
                Some(url) => {
                    fetch_headers_from_upstream(url, height, count, anchor.as_deref()).await
                }
                None => Err(Error::RustError("upstream not configured".into())),
            };

            // P0-4: a batch the node's rules refuse is an upstream fault like
            // any other (recorded, then the courier fallback for the span),
            // never a cron abort before the announce and the repair.
            let upstream_result = match upstream_result {
                Ok(headers) if !headers.is_empty() => {
                    match storage::insert_headers_batch(db, params, &headers).await {
                        Ok(_) => Ok(headers),
                        Err(e) => {
                            log_error!("Cron: upstream batch at {height} refused: {e:?}");
                            record_fault(db, &format!("upstream batch at {height}: {e:?}")).await;
                            Err(e)
                        }
                    }
                }
                other => other,
            };
            match upstream_result {
                Ok(headers) if !headers.is_empty() => {
                    height += headers.len() as u32;
                }
                Ok(_) => break, // empty response
                Err(e) => {
                    if !used_fallback {
                        log!("Cron: upstream unavailable ({e:?}), falling back to WoC");
                        used_fallback = true;
                    }
                    // WoC fallback: one-by-one (slower but independent)
                    for h in height..=(height + count - 1).min(end_height) {
                        match source.header_by_height(h).await {
                            Ok(header) => {
                                // P0-4: a refusal is recorded and ends the
                                // span; the tail still runs.
                                if let Err(e) = storage::insert_header(db, params, &header).await {
                                    log_error!(
                                        "Cron: the header at {h} could not be ingested: {e:?}"
                                    );
                                    record_fault(db, &format!("ingest at {h}: {e:?}")).await;
                                    height = end_height + 1; // break outer loop
                                    break;
                                }
                            }
                            Err(e2) => {
                                log!("Cron: WoC also failed at {h}: {e2:?}");
                                height = end_height + 1; // break outer loop
                                break;
                            }
                        }
                    }
                    if height <= end_height {
                        height += count;
                    }
                }
            }
        }
        // Self-heal any dual-active debris the bulk path can leave (audit
        // C3): exactly one active row may exist per height; keep the row
        // the next height extends, and only then the newest ingest (#33).
        storage::dedupe_active_heights(db).await?;
        storage::update_chain_tip_to_highest(db).await?;
    } else {
        // ─── Live: one-by-one from WoC with reorg detection ─────────────
        log!(
            "Cron: live sync {gap} blocks ({} → {woc_height})",
            our_height + 1
        );

        for height in (our_height + 1)..=woc_height {
            match source.header_by_height(height).await {
                Ok(header) => match insert_with_parent_backfill(db, params, source, header).await {
                    Ok(result) => {
                        if result.reorg_depth > 0 {
                            log!(
                                "Cron: REORG at height {} (depth {})",
                                height,
                                result.reorg_depth
                            );
                        }
                    }
                    Err(e) => {
                        // D5 (loop 9, 965877): the backfill's fault used to abort
                        // the cron HERE, before the announce and the repair,
                        // recording nothing for 23 minutes. Recorded; the tail
                        // runs; the next cron asks again (the ladder, by then,
                        // may hold the parent on another rung).
                        log_error!("Cron: the header at {height} could not be ingested: {e:?}");
                        record_fault(db, &format!("ingest at {height}: {e:?}")).await;
                        break;
                    }
                },
                Err(e) => {
                    log_error!("Cron: no courier served the header at {height}: {e:?}");
                    record_fault(db, &format!("header at {height}: {e:?}")).await;
                    break;
                }
            }
        }
    }

    let new_tip = storage::find_chain_tip(db).await?;
    let new_height = new_tip.as_ref().map_or(0, |t| t.height);
    if new_height > our_height {
        log!(
            "Cron: synced to {} (+{})",
            new_height,
            new_height - our_height
        );
    }
    revalidate(db, params).await;
    if let Some(r) = source.report() {
        log!("Cron: couriers: {r}");
    }
    finish_sync(db, hooks, new_tip).await;

    Ok(())
}

/// P0-4: one step of the re-validation of the stored chain (after the deploy,
/// from the last checkpoint; `storage::revalidate_step`), non-fatal and loud.
/// Once complete it is one read a tick. Run by the cron with work; the idle
/// cron runs it too, so a quiet chain still finishes the pass.
pub(crate) async fn revalidate(db: &impl HeaderDb, params: &ChainParams) {
    match storage::revalidate_step(db, params, storage::REVALIDATE_CHUNK).await {
        Ok(storage::Revalidation::Complete) => {}
        Ok(storage::Revalidation::Advanced { to }) => {
            log!("Cron: re-validated the stored chain through {to}");
        }
        Ok(storage::Revalidation::Faulted { fault }) => {
            log_error!("Cron: RE-VALIDATION STOPPED (the rows above it are not served): {fault}");
        }
        Err(e) => log_error!("Cron: re-validation read failed (migration 0008 applied?): {e:?}"),
    }
}

/// The tip announce, non-fatal (round 3, LOW-2): an announce fault is
/// logged LOUD, never propagated, so a D1 fault on the announce path can
/// neither fail the cron nor skip the repair that follows it in
/// `finish_sync`. Nothing is consumed by a fault: every write of the announce
/// comes after the reads and the claim, and a missing column or table (a
/// migration not applied, round 5 LOW-2) faults on a read or on the claim.
/// `/getInfo` names the missing column or table (`tipAnnounceSchemaFault`).
/// Called by every cron, idle or not (round 4, MED-1), with the tip the cron
/// already read (`None` reads it), and by the read-through path.
pub(crate) async fn announce_tip(
    db: &impl HeaderDb,
    hooks: &impl TipWebhooks,
    tip: Option<BlockHeader>,
) {
    // #32: the legacy webhook is a compatibility view of the served chain,
    // including while P0-4's verification pass is in progress.
    let tip = match storage::served_ceiling(db).await {
        Ok(storage::Ceiling::All) => tip,
        Ok(_) => match storage::served_tip(db).await {
            Ok(Some(tip)) => Some(tip),
            Ok(None) => return,
            Err(e) => {
                log_error!("Tip announce ceiling read failed: {e:?}");
                return;
            }
        },
        Err(e) => {
            log_error!("Tip announce ceiling read failed: {e:?}");
            return;
        }
    };
    if let Err(e) = notify_tip(db, hooks, tip).await {
        log_error!(
            "Cron: TIP ANNOUNCE FAULT, nothing consumed (a missing column or table means a migration was not applied; /getInfo tipAnnounceSchemaFault names it): {e:?}"
        );
    }
}

/// The cron's tail after a sync that moved the tip: the announce (bsv-low
/// W2-P4: tell our consumers the tip moved so THEY push it to their clients
/// as an event instead of every client polling `/tip`; keyed on the
/// PERSISTED announce state, not this run's delta, because the read-through
/// grace path (`routes::ensure_fresh_header`) ingests a just-mined block on
/// a consumer's request and used to leave the cron with nothing to announce,
/// block 965076, 2026-09-03), then the cumulative-work repair over the
/// fork-relevant window (H-3: legacy/bulk rows carry non-cumulative work;
/// 144 blocks, about a day, covers any reorg the 400-step ancestor walk
/// would accept).
pub(crate) async fn finish_sync(
    db: &impl HeaderDb,
    hooks: &impl TipWebhooks,
    tip: Option<BlockHeader>,
) {
    announce_tip(db, hooks, tip).await;
    match storage::repair_cumulative_work(db, 144).await {
        Ok(0) => {}
        Ok(n) => log!("Cron: repaired cumulative work on {n} header(s)"),
        Err(e) => log!("Cron: repair_cumulative_work failed: {e:?}"),
    }
}

/// Insert a live header; when its parent is missing locally (no_prev),
/// backfill ancestors BY HASH from WoC ; bounded walk, oldest-first insert,
/// then retry the child (reference: Chaintracks.ts:398-404,523-544
/// getMissingBlockHeader with addLiveRecursionLimit=36; audit C2 ; without
/// this, a competitor branch wedged the tip forever because find_common_
/// ancestor hit the missing parent and every later insert became a dupe
/// no-op).
pub(crate) async fn insert_with_parent_backfill(
    db: &impl HeaderDb,
    params: &ChainParams,
    source: &impl ChainSource,
    header: BlockHeader,
) -> Result<crate::types::InsertHeaderResult> {
    const BACKFILL_LIMIT: usize = 36; // TS addLiveRecursionLimit parity

    let result = storage::insert_header(db, params, &header).await?;
    if !result.no_prev {
        // H-1 (adversarial review): a dupe that is STILL unlinked means a
        // previous backfill aborted mid-walk (crash / WoC error after the
        // orphan row landed). Without this repair the dupe short-circuit
        // made the wedge permanent ; the walk was never re-attempted.
        let stored_orphan = result.dupe
            && header.height > 0
            && matches!(
                storage::find_header_for_hash(db, &header.hash).await?,
                Some(ref h) if h.previous_header_id.is_none()
            );
        if !stored_orphan {
            return Ok(result);
        }
        log!(
            "Cron: stored header {} at {} is an unlinked orphan ; resuming backfill",
            header.hash,
            header.height
        );
    }

    log!(
        "Cron: header {} at {} has no stored parent ; backfilling branch by hash",
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
        let parent = source.header_by_hash(&want).await?;
        want = parent.previous_hash.clone();
        branch.push(parent);
    }

    if !branch.is_empty()
        && want != zero_hash
        && storage::find_header_for_hash(db, &want).await?.is_none()
    {
        log!(
            "Cron: backfill budget exhausted without reaching a stored ancestor (still missing {}) ; leaving branch inactive",
            want
        );
    }

    // Insert oldest-first so each child finds its parent (and cumulative
    // chain work accumulates correctly).
    for parent in branch.iter().rev() {
        let _ = storage::insert_header(db, params, parent).await?;
    }

    // The child row already exists (orphan, inactive, per-block-only work).
    // Relink it to the backfilled parent, recompute cumulative work, and
    // re-evaluate the tip ; running the reorg walk NOW if the repaired
    // branch outworks the active one.
    let repaired = storage::relink_orphan_and_reevaluate(db, params, &header.hash).await?;
    if repaired.reorg_depth > 0 {
        log!(
            "Cron: backfilled branch won ; reorg depth {}",
            repaired.reorg_depth
        );
    }
    Ok(repaired)
}

/// The read-through for a FRESH block (`routes::ensure_fresh_header`): fill
/// `tip + 1 ..= height` from `source` through the full validation path (the
/// node's rules, the parent walk) and announce whatever landed. `Some` when
/// the whole span landed; `None` when a header could not be had or was
/// refused, which the route answers as "unable to verify", never an error
/// page and never "no such block". Rule 28 (H12): `source` is the courier
/// ladder on the worker, so a fresh block never depends on one courier.
pub(crate) async fn read_through(
    db: &impl HeaderDb,
    params: &ChainParams,
    source: &impl ChainSource,
    hooks: &impl TipWebhooks,
    tip: u32,
    height: u32,
) -> Result<Option<()>> {
    // Fill the whole gap tip+1..=height so linkage/backfill stays simple.
    for h in (tip + 1)..=height {
        match source.header_by_height(h).await {
            Ok(header) => {
                // P0-4: a header the node's rules refuse is never stored, and
                // the request answers "unable to verify", never an error page.
                if let Err(e) = insert_with_parent_backfill(db, params, source, header).await {
                    log!("read-through: header at {h} refused: {e:?}");
                    announce_tip(db, hooks, None).await;
                    return Ok(None);
                }
            }
            Err(e) => {
                log!("read-through: no courier has the header at {h} yet: {e:?}");
                // Whatever DID land is a tip move the consumers must hear.
                announce_tip(db, hooks, None).await;
                return Ok(None);
            }
        }
    }
    // The chain tip moved on a consumer's request, not the cron's: announce it
    // here (bsv-low W2-P4 ; the cron alone left block 965076 unannounced).
    announce_tip(db, hooks, None).await;
    Ok(Some(()))
}

/// The headers of `from ..= to` by height from `source`, in order, ending at
/// the first height no courier served (the operator's backfill inserts what
/// it has and re-runs from the gap). Rule 28 (H13): `source` is the courier
/// ladder on the worker; each answer is bound to the height asked and to its
/// own proof of work before it counts, and a rung faulting three times is
/// skipped for the rest of the call.
pub(crate) async fn fetch_span(source: &impl ChainSource, from: u32, to: u32) -> Vec<BlockHeader> {
    let mut headers = Vec::with_capacity((to.saturating_sub(from) + 1) as usize);
    for height in from..=to {
        match source.header_by_height(height).await {
            Ok(header) => headers.push(header),
            Err(e) => {
                log!("backfill: no courier served the header at {height}: {e:?}");
                break;
            }
        }
    }
    headers
}

/// A PEER header service: another service speaking the header protocol, asked
/// for a run of headers by `getHeaders` (the worker's is our own upstream,
/// `UPSTREAM_CHAINTRACKS_URL`; the host suite scripts one). A peer is a
/// courier like any other: nothing it serves is believed for who served it.
pub(crate) trait HeaderPeer {
    /// Up to `count` headers from `start`, each linked to the one below it and
    /// the first to `expected_prev` when given; fewer when the peer holds fewer.
    async fn headers(
        &self,
        start: u32,
        count: u32,
        expected_prev: Option<&str>,
    ) -> Result<Vec<BlockHeader>>;
}

/// The upstream peer of this deploy, by its base URL.
pub(crate) struct UpstreamPeer<'a>(pub &'a str);

impl HeaderPeer for UpstreamPeer<'_> {
    async fn headers(
        &self,
        start: u32,
        count: u32,
        expected_prev: Option<&str>,
    ) -> Result<Vec<BlockHeader>> {
        fetch_headers_from_upstream(self.0, start, count, expected_prev).await
    }
}

/// Headers per `getHeaders` request of the bootstrap (the catch-up's batch).
pub(crate) const BOOTSTRAP_BATCH: u32 = 1000;

/// Where the bootstrap of a span reads from.
#[derive(Debug)]
pub(crate) enum Bootstrap {
    /// The peer served the span, or its prefix up to the peer's own tip.
    Peer(Vec<BlockHeader>),
    /// The peer could not be had, so the file host is what remains:
    /// `peer_fault` is why (`None`: no peer is configured). "Could not look"
    /// stays apart from an answer; it is reported, never dropped.
    FileHost { peer_fault: Option<String> },
}

/// The bootstrap of `count` headers from `start` (Rule 28, H14): the upstream
/// peer's `getHeaders` is asked FIRST, a batch at a time, each batch linked to
/// the one below it (`anchor` is our stored header below `start`, when the
/// store holds one). Any peer fault, a broken link or an empty answer hands
/// the whole span to the file host (`woc::BULK_FILE_HOST`), so one span is
/// never stitched from two sources. Whichever serves it, the batch meets the
/// node's rules on insert and is refused whole on one fault.
pub(crate) async fn bootstrap_from_peer(
    peer: Option<&impl HeaderPeer>,
    start: u32,
    count: u32,
    anchor: Option<&str>,
) -> Bootstrap {
    let Some(peer) = peer else {
        return Bootstrap::FileHost { peer_fault: None };
    };
    let fault = |text: String| Bootstrap::FileHost {
        peer_fault: Some(text),
    };
    let end = start.saturating_add(count);
    let mut headers: Vec<BlockHeader> = Vec::new();
    let mut height = start;
    while height < end {
        let ask = BOOTSTRAP_BATCH.min(end - height);
        let below = headers
            .last()
            .map(|h| h.hash.clone())
            .or_else(|| anchor.map(str::to_string));
        let mut batch = match peer.headers(height, ask, below.as_deref()).await {
            Ok(batch) => batch,
            Err(e) => return fault(format!("getHeaders at {height}: {e}")),
        };
        batch.truncate(ask as usize);
        let mut expected = below;
        for (i, h) in batch.iter().enumerate() {
            let at = height + i as u32;
            let linked = expected
                .as_deref()
                .is_none_or(|p| h.previous_hash.eq_ignore_ascii_case(p));
            if h.height != at || !linked {
                return fault(format!(
                    "getHeaders at {height}: the header at {at} does not link to the one below it"
                ));
            }
            expected = Some(h.hash.clone());
        }
        let got = batch.len() as u32;
        headers.extend(batch);
        if got < ask {
            break; // the peer's own tip
        }
        height += got;
    }
    if headers.is_empty() {
        return fault(format!("the peer holds no header at {start}"));
    }
    Bootstrap::Peer(headers)
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
            // header must link to its predecessor ; INCLUDING the first one,
            // which must link to our locally stored header at start-1
            // (review M-2: an unanchored batch[0] let a stale upstream
            // bulk-insert a foreign branch at blind heights).
            let expected: Option<String> = match headers.last() {
                Some(prev) => Some(prev.hash.clone()),
                None => expected_prev_hash.map(|h: &str| h.to_string()),
            };
            if let Some(expected) = expected {
                if !header.previous_hash.eq_ignore_ascii_case(&expected) {
                    log!(
                        "Cron: upstream linkage break at height {} (links {} ≠ {}) ; truncating batch",
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

/// The atomic CLAIM that decides who announces (bsv-low M19 R2 round 2,
/// review H3; restored before the POST in M19B-G2 round 3, MED-1): the row
/// changes when the tip's `(height, hash)` differs from the last CLAIMED
/// pair, height OR hash. A same-height replacement (the common reorg shape)
/// fires; the same tip again does not; a NULL hash (a row written before
/// migration 0003, or by the bulk-sync progress writer) counts as never
/// announced; a lower height never fires and never moves the row backwards
/// (round 3, L4). Concurrent read-throughs for the same new block all race
/// here and exactly one changes the row (block 965077 was announced five
/// times before this write existed). The claim also stamps `claimed_at`
/// (round 4), the start of the in-flight window `RECLAIM_ANNOUNCE_SQL` waits
/// out, and `updated_at`, the /getInfo freshness signal (audit M6), which
/// only a NEW tip's claim moves: the one deliberate change to a statement
/// main ran (`statement_pins.rs` holds the exact delta). Binds: ?1 height,
/// ?2 hash.
pub(crate) const TIP_ANNOUNCE_SQL: &str =
    "UPDATE sync_state SET last_synced_height = ?1, last_announced_hash = ?2, live_sync_active = 1, \
     claimed_at = datetime('now'), updated_at = datetime('now') \
     WHERE id = 1 AND (last_synced_height < ?1 OR (last_synced_height = ?1 AND (last_announced_hash IS NULL OR last_announced_hash <> ?2)))";

/// Read the pending reorg fork height (review MED-1). Bind: none.
pub(crate) const READ_PENDING_REORG_SQL: &str =
    "SELECT pending_reorg_from FROM sync_state WHERE id = 1";
/// Clear ONLY the value we are about to announce (review LOW-2): a CAS on
/// the read height, so a DEEPER fork another isolate MIN-accumulated between
/// the read and the clear is never wiped un-announced (it survives to the
/// next announce; SQLite `RETURNING` gives the post-update value, so it
/// cannot return the old height in one statement, hence the compare-and-clear).
/// Bind: ?1 = the height read.
pub(crate) const CLEAR_PENDING_REORG_SQL: &str =
    "UPDATE sync_state SET pending_reorg_from = NULL WHERE id = 1 AND pending_reorg_from = ?1";

/// The announced tip and the delivery counter, read BEFORE the POST:
/// `(last_synced_height, last_announced_hash, announce_failures)`.
pub(crate) const ANNOUNCED_TIP_SQL: &str =
    "SELECT last_synced_height, last_announced_hash, announce_failures FROM sync_state WHERE id = 1";

/// The consecutive undelivered announces, written on failure (the value read
/// plus one; two isolates failing together lose one count, which a signal
/// this loud can bear) and reset to 0 on delivery. Bind: ?1 = the count.
pub(crate) const SET_ANNOUNCE_FAILURES_SQL: &str =
    "UPDATE sync_state SET announce_failures = ?1 WHERE id = 1";

/// One stuck event: every `ANNOUNCE_STUCK_AFTER` consecutive undelivered
/// announces, with a loud log line; read back on `/getInfo`.
pub(crate) const COUNT_ANNOUNCE_STUCK_SQL: &str =
    "UPDATE sync_state SET tip_announce_stuck_total = tip_announce_stuck_total + 1 WHERE id = 1";

/// After this many consecutive undelivered announces the announce is STUCK:
/// the line is loud and `tip_announce_stuck_total` bumps (again at every
/// multiple, so a long outage keeps paging); the marker and the announce row
/// still stay untouched, owed to the next cron.
pub(crate) const ANNOUNCE_STUCK_AFTER: u32 = 5;

/// The retry claim (round 3): the tip is already claimed but a target is
/// still owed it (a refused delivery, an isolate evicted mid-POST, a target
/// added since). A CAS on the claim's own `(height, hash)`, so a newer claim
/// is never touched, gated on the claim's age (`claimed_at`, round 4; a NULL,
/// the claim of the pre-0006 build, counts as old) so an announce still in
/// flight in another isolate is not duplicated; atomic, so one isolate
/// retries per window. It stamps `claimed_at` only, never `updated_at`, so a
/// dead target retried every minute cannot keep the /getInfo freshness
/// signal fresh through a stalled sync. Binds: ?1 height, ?2 hash, ?3 the
/// minimum age in seconds.
pub(crate) const RECLAIM_ANNOUNCE_SQL: &str =
    "UPDATE sync_state SET claimed_at = datetime('now') WHERE id = 1 AND last_synced_height = ?1 AND last_announced_hash = ?2 AND (claimed_at IS NULL OR (julianday('now') - julianday(claimed_at)) * 86400 >= ?3)";

/// How long a claim is presumed in flight before a retry may take it over:
/// shorter than the one-minute cron, so the cron after a refused delivery
/// retries; longer than any POST the announce waits on.
pub(crate) const CLAIM_IN_FLIGHT_S: u32 = 45;

/// Which target has which tip, and which pending fork it has been told
/// about (round 3, MED-2; round 4, LOW-3): `target` is the URL.
pub(crate) const READ_DELIVERIES_SQL: &str =
    "SELECT target, height, hash, reorg_from FROM announce_deliveries";

/// One target accepted this tip, carrying `reorg_from` (NULL when the body
/// carried none). Guarded on the claim row (round 4, LOW-1): a delivery of a
/// tip that is no longer the claimed one (an isolate that delivered slowly
/// while another claimed and delivered the next tip) records nothing, so it
/// can never rewrite a newer record. A body that carried no fork keeps the
/// fork the target had already heard (COALESCE). Binds: ?1 target URL, ?2
/// height, ?3 hash, ?4 reorg_from.
pub(crate) const RECORD_DELIVERY_SQL: &str =
    "INSERT INTO announce_deliveries (target, height, hash, reorg_from, delivered_at) \
     SELECT ?1, ?2, ?3, ?4, datetime('now') FROM sync_state WHERE id = 1 AND last_synced_height = ?2 AND last_announced_hash = ?3 \
     ON CONFLICT(target) DO UPDATE SET height = excluded.height, hash = excluded.hash, \
     reorg_from = COALESCE(excluded.reorg_from, announce_deliveries.reorg_from), delivered_at = excluded.delivered_at";

/// `TIP_ANNOUNCE_SQL`'s WHERE clause, in code, for the pre-check that runs
/// before the claim (pinned to agree with the statement on every case:
/// `the_pre_check_and_the_announce_record_agree`).
pub(crate) fn tip_is_unannounced(
    last_height: u32,
    last_hash: Option<&str>,
    height: u32,
    hash: &str,
) -> bool {
    last_height < height || (last_height == height && last_hash.is_none_or(|h| h != hash))
}

/// The tip webhook body: `{"height": n, "hash": "<64 hex, lower-case>"}`
/// (the hash since bsv-low M19 round 2; consumers that read only `height`
/// are unaffected).
pub fn tip_webhook_body(height: u64, hash: &str, reorg_from: Option<u64>) -> String {
    let hash = hash.trim().to_ascii_lowercase();
    match reorg_from {
        Some(from) => format!("{{\"height\":{height},\"hash\":\"{hash}\",\"reorgFrom\":{from}}}"),
        None => format!("{{\"height\":{height},\"hash\":\"{hash}\"}}"),
    }
}

/// The announced tip as the row stands.
pub(crate) struct AnnouncedTip {
    pub height: u32,
    pub hash: Option<String>,
    /// Consecutive undelivered announces so far.
    pub failures: u32,
}

/// Read the announce row (`ANNOUNCED_TIP_SQL`). A fault is an error: without
/// the row there is no telling whether the tip is new, and an announce must
/// never fire or stay silent on a guess.
pub(crate) async fn read_announced_tip(db: &impl HeaderDb) -> Result<AnnouncedTip> {
    #[derive(serde::Deserialize)]
    struct Row {
        last_synced_height: Option<f64>,
        last_announced_hash: Option<String>,
        announce_failures: Option<f64>,
    }
    let row: Option<Row> = crate::d1::Query::new(ANNOUNCED_TIP_SQL).first(db).await?;
    let row = row.ok_or_else(|| Error::RustError("sync_state row 1 is missing".to_string()))?;
    Ok(AnnouncedTip {
        height: row.last_synced_height.unwrap_or(0.0) as u32,
        hash: row.last_announced_hash,
        failures: row.announce_failures.unwrap_or(0.0) as u32,
    })
}

/// One target's recorded delivery: the `(height, hash)` it accepted last and
/// the pending fork it has been told about.
pub(crate) struct Delivery {
    pub height: u32,
    pub hash: String,
    pub reorg_from: Option<u64>,
}

/// Every target's recorded delivery, keyed by the target URL.
pub(crate) async fn read_deliveries(db: &impl HeaderDb) -> Result<HashMap<String, Delivery>> {
    #[derive(serde::Deserialize)]
    struct Row {
        target: Option<String>,
        height: Option<f64>,
        hash: Option<String>,
        reorg_from: Option<f64>,
    }
    let rows: Vec<Row> = crate::d1::Query::new(READ_DELIVERIES_SQL).all(db).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            Some((
                r.target?,
                Delivery {
                    height: r.height? as u32,
                    hash: r.hash?,
                    reorg_from: r.reorg_from.map(|v| v as u64),
                },
            ))
        })
        .collect())
}

/// Announce the tip to the consumers, from WHICHEVER path moved the chain
/// tip (the cron's live/bulk sync or a request's read-through ingest), on
/// ANY tip change, height or hash (bsv-low M19 round 2: a same-height
/// replacement is a reorg the consumers must hear), and to each target
/// exactly until it accepts.
///
/// The order (M19B-G2 rounds 2 and 3): read the tip, the claim row and the
/// per-target deliveries; a NEW tip is CLAIMED atomically (`TIP_ANNOUNCE_SQL`,
/// one isolate wins, the rest return); a tip already claimed but still owed
/// to some target is RE-CLAIMED once its in-flight window has passed
/// (`RECLAIM_ANNOUNCE_SQL`, again one isolate); the winner reads the pending
/// fork, POSTs to the owed targets only, records each accepted target's
/// delivery, and when no target is left owed CAS-clears the fork carried and
/// resets the failure run. A refused, failed or partial delivery (or no
/// bearer) consumes nothing: the claim stays, the owed targets stay owed,
/// the failure run counts it, and the next announce after the window re-sends
/// the same body to the owed targets alone (a repeat is idempotent on the
/// overlay side, and its reorg arm is refute based). After
/// `ANNOUNCE_STUCK_AFTER` consecutive undelivered announces the line is loud
/// and `tip_announce_stuck_total` counts it.
#[cfg(test)]
pub(crate) async fn notify_if_tip_advanced(
    db: &impl HeaderDb,
    hooks: &impl TipWebhooks,
) -> Result<()> {
    notify_tip(db, hooks, None).await
}

/// `notify_if_tip_advanced` on a tip already read this cron (round 5, LOW-4:
/// the idle cron reads the tip once); `None` reads it.
pub(crate) async fn notify_tip(
    db: &impl HeaderDb,
    hooks: &impl TipWebhooks,
    known: Option<BlockHeader>,
) -> Result<()> {
    let tip = match known {
        Some(t) => t,
        None => match storage::find_chain_tip(db).await? {
            Some(t) => t,
            None => return Ok(()),
        },
    };
    let hash = tip.hash.trim().to_ascii_lowercase();
    let announced = read_announced_tip(db).await?;
    let delivered = read_deliveries(db).await?;
    let targets = hooks.targets();
    let owed: Vec<WebhookTarget> = targets
        .iter()
        .filter(|t| {
            delivered
                .get(&t.url)
                .is_none_or(|d| d.height != tip.height || d.hash != hash)
        })
        .cloned()
        .collect();
    let claim_is_tip =
        announced.height == tip.height && announced.hash.as_deref() == Some(hash.as_str());
    let claimed = if tip_is_unannounced(
        announced.height,
        announced.hash.as_deref(),
        tip.height,
        &hash,
    ) {
        crate::d1::Query::new(TIP_ANNOUNCE_SQL)
            .bind(tip.height)
            .bind(hash.as_str())
            .run_changes(db)
            .await?
            == 1
    } else if claim_is_tip && !owed.is_empty() {
        crate::d1::Query::new(RECLAIM_ANNOUNCE_SQL)
            .bind(tip.height)
            .bind(hash.as_str())
            .bind(CLAIM_IN_FLIGHT_S)
            .run_changes(db)
            .await?
            == 1
    } else {
        false
    };
    if !claimed {
        return Ok(());
    }
    // review MED-1: carry the fork height `handle_reorg` recorded, so the
    // overlay runs its targeted re-verify over [reorgFrom, tip]. Best-effort:
    // a read fault omits the marker from THIS announce (it stays recorded and
    // rides the next one), never blocks the announce. Round 4 (LOW-3): each
    // owed target carries the fork until IT has accepted a body carrying it,
    // so a target that already heard it is not made to re-verify on every
    // block while another target stays dead.
    let reorg_from = read_pending_reorg(db).await;
    let heard_before = |url: &str| delivered.get(url).and_then(|d| d.reorg_from);
    let owed: Vec<(WebhookTarget, Option<u64>)> = owed
        .into_iter()
        .map(|t| {
            let carry = reorg_from.filter(|v| heard_before(&t.url) != Some(*v));
            (t, carry)
        })
        .collect();
    let height = u64::from(tip.height);
    let round = notify_tip_webhooks(hooks, &owed, height, &hash).await;
    if !round.accepted.is_empty() {
        let mut batch = BatchCollector::new(db);
        for (url, carried) in &round.accepted {
            batch.add(
                RECORD_DELIVERY_SQL,
                vec![
                    QVal::Text(url.clone()),
                    QVal::Int(i64::from(tip.height)),
                    QVal::Text(hash.clone()),
                    QVal::from(carried.map(|v| v as i64)),
                ],
            );
        }
        batch.execute().await?;
    }
    // The fork is cleared once EVERY configured target has accepted a body
    // carrying it: the CAS-clear of exactly the fork carried (review LOW-2: a
    // deeper fork MIN-accumulated meanwhile survives to the next announce).
    if let Some(carried) = reorg_from {
        let heard_by_all = targets.iter().all(|t| {
            let now = round
                .accepted
                .iter()
                .find(|(url, _)| url == &t.url)
                .and_then(|(_, c)| *c);
            now == Some(carried) || heard_before(&t.url) == Some(carried)
        });
        if heard_by_all {
            clear_pending_reorg(db, carried).await;
        }
    }
    if round.failed == 0 {
        // Every owed target accepted: the failure run to 0, always (round 3,
        // LOW-3: never on the pre-POST read's value).
        crate::d1::Query::new(SET_ANNOUNCE_FAILURES_SQL)
            .bind(0u32)
            .run(db)
            .await?;
    } else {
        let failures = announced.failures.saturating_add(1);
        crate::d1::Query::new(SET_ANNOUNCE_FAILURES_SQL)
            .bind(failures)
            .run(db)
            .await?;
        if failures % ANNOUNCE_STUCK_AFTER == 0 {
            log_error!(
                "Cron: TIP ANNOUNCE STUCK: {height} {hash} reorgFrom {reorg_from:?} undelivered {failures} announces in a row ({}/{} owed targets refused); the claim and any reorg marker are kept and the owed targets are re-sent from the next announce on",
                round.failed,
                owed.len()
            );
            crate::d1::Query::new(COUNT_ANNOUNCE_STUCK_SQL)
                .run(db)
                .await?;
        }
    }
    Ok(())
}

/// The pending reorg fork height (review MED-1), read only. Best-effort: a
/// fault answers `None` (the marker stays recorded and rides the next
/// announce, over a wider range, which the overlay's re-verify tolerates).
pub(crate) async fn read_pending_reorg(db: &impl HeaderDb) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct Row {
        pending_reorg_from: Option<f64>,
    }
    match crate::d1::Query::new(READ_PENDING_REORG_SQL)
        .first::<Row>(db)
        .await
    {
        Ok(Some(r)) => r.pending_reorg_from.map(|v| v as u64),
        Ok(None) | Err(_) => None,
    }
}

/// CAS-clear the fork height that was carried (review LOW-2): if a deeper
/// fork raced in (a smaller height, MIN-accumulated), this no-ops and the
/// deeper marker survives to the next announce. Best-effort: a fault leaves
/// the marker, which rides the next announce again (a repeat the overlay
/// dedups).
pub(crate) async fn clear_pending_reorg(db: &impl HeaderDb, carried: u64) {
    if let Err(e) = crate::d1::Query::new(CLEAR_PENDING_REORG_SQL)
        .bind(carried as i64)
        .run(db)
        .await
    {
        log!("notify: clearing pending_reorg_from failed: {e:?}");
    }
}

/// `TIP_WEBHOOK_URLS` ; comma-separated POST targets, each receiving
/// `{"height": <u64>, "hash": "<64 hex>"}` with `Authorization: Bearer <TIP_WEBHOOK_TOKEN>`.
/// One tip-webhook target: `[BINDING=]URL`. With a binding name the POST
/// rides that SERVICE BINDING ; the consumer is a Worker on this account, and
/// Cloudflare refuses a plain fetch between two Workers on one zone (error
/// 1042 behind a 404; every `*.workers.dev` host of an account is ONE zone ;
/// proven on beta 2026-09-03). Without one it is a public fetch (a consumer
/// on another zone).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookTarget {
    pub binding: Option<String>,
    pub url: String,
}

pub fn parse_webhook_targets(raw: &str) -> Vec<WebhookTarget> {
    raw.split(',')
        .map(str::trim)
        .filter_map(|entry| {
            let (binding, url) = match entry.split_once('=') {
                Some((b, u)) if !b.trim().is_empty() && !b.contains("://") && !b.contains('/') => {
                    (Some(b.trim().to_string()), u.trim())
                }
                _ => (None, entry),
            };
            (url.starts_with("https://") || url.starts_with("http://")).then(|| WebhookTarget {
                binding,
                url: url.to_string(),
            })
        })
        .collect()
}

/// The consumer side of the announce: WHERE the tip webhook goes and HOW the
/// POST travels. The worker (`Env`, below) reads `TIP_WEBHOOK_URLS` and
/// `TIP_WEBHOOK_TOKEN` and sends over the named service binding, or a public
/// fetch when the target names none (see `WebhookTarget`); the host harness
/// records the posts instead, so `notify_if_tip_advanced` is driven end to end
/// in `cargo test` and the body it sends is asserted (bsv-low M19B-G2).
pub(crate) trait TipWebhooks {
    /// The targets, parsed from `TIP_WEBHOOK_URLS`; empty means no webhook.
    fn targets(&self) -> Vec<WebhookTarget>;
    /// The bearer (`TIP_WEBHOOK_TOKEN`); empty means unset.
    fn token(&self) -> String;
    /// POST `body` (JSON) to `target` with `Authorization: Bearer <token>`.
    async fn post(&self, target: &WebhookTarget, token: &str, body: &str) -> WebhookDelivery;
    async fn post_event(
        &self,
        target: &WebhookTarget,
        token: &str,
        body: &str,
        _cursor: u64,
    ) -> WebhookDelivery {
        self.post(target, token, body).await
    }
}

/// What one tip-webhook POST came back with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WebhookDelivery {
    /// A 2xx.
    Accepted(u16),
    /// The consumer, or an edge in front of it, refused: the status, the
    /// `server` header and the first 200 chars of the body on one line, so
    /// the log says WHO refused (an edge 404 page reads differently from the
    /// consumer's own refusal).
    Refused {
        status: u16,
        server: String,
        excerpt: String,
    },
    /// No answer at all: a bad url, an unavailable binding, a fetch fault.
    Failed(String),
}

impl TipWebhooks for Env {
    fn targets(&self) -> Vec<WebhookTarget> {
        self.var("TIP_WEBHOOK_URLS")
            .ok()
            .map(|v| parse_webhook_targets(&v.to_string()))
            .unwrap_or_default()
    }

    fn token(&self) -> String {
        self.secret("TIP_WEBHOOK_TOKEN")
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_default()
    }

    async fn post(&self, target: &WebhookTarget, token: &str, body: &str) -> WebhookDelivery {
        send_webhook(self, target, token, body, None).await
    }
}

/// What one announce round came to: the targets that accepted (with the fork
/// each one's body carried), and how many refused, failed, or could not be
/// sent to (no bearer).
pub(crate) struct Round {
    pub accepted: Vec<(String, Option<u64>)>,
    pub failed: usize,
}

/// POST the announce to every owed target (every one, even after a refusal,
/// so each consumer that can hear it hears it now), each with the fork it is
/// still owed (round 4, LOW-3).
async fn notify_tip_webhooks(
    hooks: &impl TipWebhooks,
    owed: &[(WebhookTarget, Option<u64>)],
    height: u64,
    hash: &str,
) -> Round {
    let mut round = Round {
        accepted: Vec::new(),
        failed: 0,
    };
    if owed.is_empty() {
        return round;
    }
    let token = hooks.token();
    let token = token.trim();
    if token.is_empty() {
        log!("Cron: TIP_WEBHOOK_URLS set but TIP_WEBHOOK_TOKEN missing, no webhook sent");
        round.failed = owed.len();
        return round;
    }
    for (target, carry) in owed {
        let url = &target.url;
        let body = tip_webhook_body(height, hash, *carry);
        match hooks.post(target, token, &body).await {
            WebhookDelivery::Accepted(_) => {
                log!(
                    "Cron: tip webhook {url} ok (height {height} hash {hash} reorgFrom {carry:?})"
                );
                round.accepted.push((url.clone(), *carry));
            }
            WebhookDelivery::Refused {
                status,
                server,
                excerpt,
            } => {
                round.failed += 1;
                log!("Cron: tip webhook {url} HTTP {status} (server={server}) {excerpt}")
            }
            WebhookDelivery::Failed(why) => {
                round.failed += 1;
                log!("Cron: tip webhook {url} failed: {why}")
            }
        }
    }
    round
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
}

#[cfg(test)]
mod tip_webhook_tests {
    use super::{
        parse_webhook_targets, tip_webhook_body, WebhookTarget, CLEAR_PENDING_REORG_SQL,
        READ_PENDING_REORG_SQL, TIP_ANNOUNCE_SQL,
    };
    use crate::storage::RECORD_PENDING_REORG_SQL;

    #[test]
    fn the_webhook_body_carries_the_height_the_hash_and_an_optional_reorg_from() {
        let h = "00000000000000001DE5AA96BAA3566CE66E4941F8295CC44CC85FC75949DB4D";
        let body = tip_webhook_body(965771, h, None);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["height"], 965771);
        assert_eq!(v["hash"], h.to_ascii_lowercase());
        assert!(
            v.get("reorgFrom").is_none(),
            "no marker on a plain extension"
        );
        // review MED-1: a reorg announce carries the fork height
        let with_reorg = tip_webhook_body(965773, h, Some(965771));
        let v: serde_json::Value = serde_json::from_str(&with_reorg).unwrap();
        assert_eq!(v["reorgFrom"], 965771);
        assert_eq!(v["height"], 965773);
    }

    /// bsv-low M19 round 2 (review H3): the announce decision, executed as the
    /// SHIPPED statement on the shipped migrations under real SQLite. A
    /// same-height hash change FIRES (the 2026-09-07 reorg shape); the same
    /// tip again does NOT (the cron and the read-through never double-fire);
    /// a new height fires; a pre-0003 row (NULL hash) fires once; a lower
    /// height (any tip change) fires.
    #[test]
    fn the_announce_fires_on_any_tip_change_and_never_on_the_same_tip_real_sqlite() {
        // M19B-G2 round 4: the claim also stamps `claimed_at` (migration
        // 0006), so the fixture is every migration in order (it was 0001 to
        // 0003 by hand); the assertions below are unchanged.
        let db = rusqlite::Connection::open_in_memory().unwrap();
        for (_, sql) in crate::host_harness::migrations() {
            db.execute_batch(&sql).unwrap();
        }
        let announce = |height: i64, hash: &str| -> usize {
            db.execute(TIP_ANNOUNCE_SQL, rusqlite::params![height, hash])
                .unwrap()
        };
        let orphan = "153e10f4";
        let canonical = "1de5aa96";
        // a pre-0003 row: last_synced_height is 0 and the hash NULL
        assert_eq!(announce(965770, "aaaa"), 1, "the first announce");
        assert_eq!(announce(965770, "aaaa"), 0, "the same tip again: silent");
        assert_eq!(announce(965771, orphan), 1, "a new height");
        assert_eq!(
            announce(965771, orphan),
            0,
            "the same block again (a second read-through): silent"
        );
        assert_eq!(
            announce(965771, canonical),
            1,
            "the SAME height, another hash: the reorg the consumers must hear"
        );
        assert_eq!(announce(965771, canonical), 0);
        // review L4: a LOWER height must NOT fire and must NOT write
        // last_synced_height backwards (it would re-announce every cron).
        // chaintracks only advances the tip on more work, so a lower announce
        // is stale; the reorg producer is the same-height hash change above
        // and the `reorgFrom` marker, never a height going backwards.
        assert_eq!(announce(965770, "bbbb"), 0, "a lower height never fires");
        let held: i64 = db
            .query_row(
                "SELECT last_synced_height FROM sync_state WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(held, 965771, "last_synced_height was not moved backwards");
        // the bulk-sync progress writer moves the height without a hash: the next announce fires once
        db.execute("UPDATE sync_state SET last_synced_height = 965772, last_announced_hash = NULL WHERE id = 1", []).unwrap();
        assert_eq!(
            announce(965772, "cccc"),
            1,
            "a NULL hash counts as never announced"
        );
        assert_eq!(announce(965772, "cccc"), 0);
        let (h, hash): (i64, Option<String>) = db
            .query_row(
                "SELECT last_synced_height, last_announced_hash FROM sync_state WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((h, hash.as_deref()), (965772, Some("cccc")));
    }

    #[test]
    fn the_0003_migration_is_additive() {
        let sql = include_str!("../migrations/0003_sync_state_announced_hash.sql");
        assert!(sql.contains("ALTER TABLE sync_state ADD COLUMN last_announced_hash TEXT"));
        assert!(!sql.to_ascii_uppercase().contains("DROP "));
    }

    /// bsv-low M19 R2 round 3 (review MED-1): `handle_reorg`'s pending-fork
    /// write MIN-accumulates, and the announce reads + clears it. Executed as
    /// the SHIPPED statements on the shipped migrations under real SQLite
    /// (the same tier the codebase pins its header logic at; `insert_header`
    /// itself takes the D1 binding and has no host harness, a pre-existing
    /// gap noted in the build log).
    #[test]
    fn the_pending_reorg_fork_min_accumulates_and_the_announce_reads_and_clears_it_real_sqlite() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            include_str!("../migrations/0001_initial.sql"),
            include_str!("../migrations/0002_active_height_index.sql"),
            include_str!("../migrations/0003_sync_state_announced_hash.sql"),
            include_str!("../migrations/0004_sync_state_pending_reorg.sql"),
        ] {
            db.execute_batch(sql).unwrap();
        }
        let record = |from: i64| db.execute(RECORD_PENDING_REORG_SQL, [from]).unwrap();
        let pending = || -> Option<i64> {
            db.query_row(READ_PENDING_REORG_SQL, [], |r| r.get::<_, Option<i64>>(0))
                .unwrap()
        };
        assert_eq!(pending(), None, "no reorg pending on a fresh row");
        record(965_772); // a reorg forking at 965771 (ancestor 965770 → +1)... here fork+1 = 965772
        assert_eq!(pending(), Some(965_772));
        record(965_770); // a DEEPER reorg before the next announce: keep the deepest
        assert_eq!(
            pending(),
            Some(965_770),
            "MIN-accumulate keeps the deepest fork"
        );
        record(965_775); // a shallower one does not raise it
        assert_eq!(pending(), Some(965_770));
        // review LOW-2: the announce reads the value, then CAS-clears ONLY
        // that value. Interleave: another isolate MIN-accumulates a DEEPER
        // fork between the read and the clear ; the clear must no-op so the
        // deeper marker survives.
        let read = pending().unwrap(); // 965_770 (the deepest so far)
        record(965_768); // a DEEPER fork races in before the clear
        db.execute(CLEAR_PENDING_REORG_SQL, [read]).unwrap(); // CAS on the read value
        assert_eq!(
            pending(),
            Some(965_768),
            "the deeper marker is NOT wiped; it survives to the next announce"
        );
        // the next announce reads + clears the deeper one
        let read2 = pending().unwrap();
        db.execute(CLEAR_PENDING_REORG_SQL, [read2]).unwrap();
        assert_eq!(pending(), None, "cleared once carried");
        // a stale clear (the value already moved) is a harmless no-op
        record(965_760);
        db.execute(CLEAR_PENDING_REORG_SQL, [999_999i64]).unwrap();
        assert_eq!(
            pending(),
            Some(965_760),
            "a clear for a value not present changes nothing"
        );
    }

    #[test]
    fn the_0004_migration_is_additive() {
        let sql = include_str!("../migrations/0004_sync_state_pending_reorg.sql");
        assert!(sql.contains("ALTER TABLE sync_state ADD COLUMN pending_reorg_from INTEGER"));
        assert!(!sql.to_ascii_uppercase().contains("DROP "));
    }

    fn t(binding: Option<&str>, url: &str) -> WebhookTarget {
        WebhookTarget {
            binding: binding.map(str::to_string),
            url: url.to_string(),
        }
    }

    #[test]
    fn parse_webhook_targets_splits_trims_and_keeps_only_http_targets() {
        assert_eq!(
            parse_webhook_targets(
                " https://a.example/internal/tip-changed , http://b.local/x,,junk, "
            ),
            vec![
                t(None, "https://a.example/internal/tip-changed"),
                t(None, "http://b.local/x")
            ]
        );
        assert!(parse_webhook_targets("").is_empty());
    }

    #[test]
    fn parse_webhook_targets_reads_a_service_binding_prefix() {
        assert_eq!(
            parse_webhook_targets(
                "APP_LAYER_BETA=https://low-app-layer-beta.example/internal/tip-changed"
            ),
            vec![t(
                Some("APP_LAYER_BETA"),
                "https://low-app-layer-beta.example/internal/tip-changed"
            )]
        );
        // A query-string '=' is not a binding separator; a binding never
        // carries a scheme or a path.
        assert_eq!(
            parse_webhook_targets("https://a.example/x?k=v, B=https://b.example/y?k=v"),
            vec![
                t(None, "https://a.example/x?k=v"),
                t(Some("B"), "https://b.example/y?k=v")
            ]
        );
        // A malformed entry (empty binding name, or a binding with no URL) is
        // dropped, never sent somewhere half-parsed.
        assert!(parse_webhook_targets("=https://a.example/x").is_empty());
        assert!(parse_webhook_targets("APP_LAYER=nope").is_empty());
    }
}

// #32: uses the existing service-binding transport, with separate opt-in
// targets so the legacy height webhook remains compatible.
pub(crate) struct EventWebhookConfig<'a>(pub(crate) &'a Env);

impl TipWebhooks for EventWebhookConfig<'_> {
    fn targets(&self) -> Vec<WebhookTarget> {
        self.0
            .var("CHAIN_EVENT_WEBHOOK_URLS")
            .ok()
            .map(|v| parse_webhook_targets(&v.to_string()))
            .unwrap_or_default()
    }
    fn token(&self) -> String {
        self.0
            .secret("CHAIN_EVENT_WEBHOOK_TOKEN")
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_default()
    }
    async fn post(&self, target: &WebhookTarget, token: &str, body: &str) -> WebhookDelivery {
        send_webhook(self.0, target, token, body, None).await
    }
    async fn post_event(
        &self,
        target: &WebhookTarget,
        token: &str,
        body: &str,
        cursor: u64,
    ) -> WebhookDelivery {
        send_webhook(self.0, target, token, body, Some(cursor)).await
    }
}

async fn send_webhook(
    env: &Env,
    target: &WebhookTarget,
    token: &str,
    body: &str,
    cursor: Option<u64>,
) -> WebhookDelivery {
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {token}"));
    let _ = headers.set("content-type", "application/json");
    if let Some(cursor) = cursor {
        let _ = headers.set("X-Chain-Event-Cursor", &cursor.to_string());
    }
    init.with_headers(headers);
    init.with_body(Some(body.to_string().into()));
    let Ok(req) = Request::new_with_init(&target.url, &init) else {
        return WebhookDelivery::Failed("bad url".to_string());
    };
    let sent = match &target.binding {
        Some(b) => match env.service(b) {
            Ok(svc) => svc.fetch_request(req).await,
            Err(e) => {
                return WebhookDelivery::Failed(format!("service binding {b} unavailable: {e}"))
            }
        },
        None => Fetch::Request(req).send().await,
    };
    match sent {
        Ok(r) if (200..300).contains(&r.status_code()) => {
            WebhookDelivery::Accepted(r.status_code())
        }
        Ok(mut r) => {
            let status = r.status_code();
            let server = r.headers().get("server").ok().flatten().unwrap_or_default();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body
                .chars()
                .take(200)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            WebhookDelivery::Refused {
                status,
                server,
                excerpt,
            }
        }
        Err(e) => WebhookDelivery::Failed(format!("{e:?}")),
    }
}
