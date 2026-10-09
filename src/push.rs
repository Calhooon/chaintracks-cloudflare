//! E5 (Rule 28): the one door for a pushed header.
//!
//! The minute poll asks the couriers for a tip the network has already
//! announced; a push source hands the header in as it is announced. Whatever
//! pushes (the tip stream's Durable Object, `tip_stream.rs`; any later source
//! that speaks a header), the header comes in here and nowhere else, and it
//! counts for nothing until the service's own rules have passed it: proof of
//! work, the difficulty rule, the checkpoints, the parent, through
//! `storage::ingest_pushed`, the function the operator's `POST /admin/ingest`
//! runs (#33), so a push is stored and activated exactly as an operator's
//! push is. A push adds speed and removes the routine read; it adds no
//! authority (bsv-stack-lean `docs/p0/rule-28-chaintracks.md`, the design
//! note, option B).
//!
//! A push that fails a rule is a refused header and a logged fault, never a
//! stored row; a push that is silent is covered by the minute poll, kept
//! behind it (`sync::poll_for_new_blocks`).

use crate::consensus::ChainParams;
use crate::d1::HeaderDb;
use crate::storage::{self, IngestError};
use crate::sync::{self, ChainSource, TipWebhooks};
use crate::types::BlockHeader;

/// The most ancestors one push may fetch through the ladder before it stands
/// on a stored row: the live path's bound (`sync::insert_with_parent_backfill`,
/// the TS reference's `addLiveRecursionLimit`). A pushed tip further than
/// this above the store is refused; the minute poll's catch-up covers it.
pub(crate) const PARENT_WALK_LIMIT: usize = 36;

/// What the door did with one pushed header.
#[derive(Debug)]
pub(crate) struct Announced {
    /// `"known"`: the pushed header is already the active row at its height
    /// (a stream's opening tip after a reconnect); nothing was written.
    /// Otherwise the outcome of `storage::ingest_pushed`: `"active"`,
    /// `"activated"` or `"storedInactive"`.
    pub outcome: &'static str,
    /// The ancestors fetched through the ladder ahead of the header, by hash.
    pub walked: u32,
    /// The served tip after the call.
    pub tip_height: u32,
    pub tip_hash: String,
    /// Why the header is `"storedInactive"`, from the ingest.
    pub reason: Option<String>,
}

impl Announced {
    /// The push changed the store (a header written, or a row activated).
    pub fn stored(&self) -> bool {
        self.outcome != "known"
    }
}

/// THE DOOR. One pushed header: refused at once if its own proof of work
/// fails (no request leaves the service on a header that cannot be a block);
/// `"known"` if it is already served; otherwise its missing ancestors are
/// fetched by hash through `source` (the courier ladder on the Worker) until
/// a stored row is reached, and the run goes through `storage::ingest_pushed`
/// whole. A refusal writes nothing and is recorded as the poll's faults are
/// (`sync_state.last_error`, `/getInfo`). After a write the announced height
/// is recorded as the couriers' is (`/getPresentHeight`) and the tip is
/// announced to the consumers (`sync::announce_tip`).
pub(crate) async fn ingest_announced(
    db: &impl HeaderDb,
    params: &ChainParams,
    source: &impl ChainSource,
    hooks: &impl TipWebhooks,
    header: BlockHeader,
) -> Result<Announced, IngestError> {
    let pushed = format!("{} at {}", header.hash, header.height);
    let refuse = |e: worker::Error| {
        let text = format!("push: {pushed}: {e}");
        async move {
            log_error!("{text}");
            sync::record_fault(db, &text).await;
            IngestError::Refused(e)
        }
    };
    if let Err(fault) = header.check_pow(params) {
        return Err(refuse(storage::refused(&header, &fault)).await);
    }
    let stored = storage::find_header_for_hash(db, &header.hash)
        .await
        .map_err(IngestError::Store)?;
    if stored.as_ref().is_some_and(|s| s.is_active) {
        return answer(db, "known", 0, None).await;
    }

    // The run from the first stored ancestor up to the header, oldest first.
    let mut run = vec![header];
    let zero = "0".repeat(64);
    loop {
        let want = run[0].previous_hash.clone();
        if want == zero
            || storage::find_header_for_hash(db, &want)
                .await
                .map_err(IngestError::Store)?
                .is_some()
        {
            break;
        }
        if run.len() > PARENT_WALK_LIMIT {
            let e = worker::Error::RustError(format!(
                "pushed header {} at {} stands on no stored row within {PARENT_WALK_LIMIT} ancestors (still missing {want}); the poll's catch-up covers it",
                run[run.len() - 1].hash,
                run[run.len() - 1].height
            ));
            return Err(refuse(e).await);
        }
        match source.header_by_hash(&want).await {
            Ok(parent) => run.insert(0, parent),
            Err(e) => {
                let e = worker::Error::RustError(format!(
                    "the parent {want} of a pushed header could not be had: {e}"
                ));
                return Err(refuse(e).await);
            }
        }
    }

    let walked = (run.len() - 1) as u32;
    let height = run[run.len() - 1].height;
    let out = match storage::ingest_pushed(db, params, &run).await {
        Ok(out) => out,
        Err(IngestError::Refused(e)) => return Err(refuse(e).await),
        Err(e) => return Err(e),
    };
    sync::note_seen(db, height).await;
    sync::announce_tip(db, hooks, None).await;
    answer(db, out.outcome, walked, out.reason).await
}

async fn answer(
    db: &impl HeaderDb,
    outcome: &'static str,
    walked: u32,
    reason: Option<String>,
) -> Result<Announced, IngestError> {
    let tip = storage::find_chain_tip(db)
        .await
        .map_err(IngestError::Store)?;
    Ok(Announced {
        outcome,
        walked,
        tip_height: tip.as_ref().map_or(0, |t| t.height),
        tip_hash: tip.map(|t| t.hash).unwrap_or_default(),
        reason,
    })
}
