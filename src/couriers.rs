//! bsv-low loop 10 D5 (2026-09-08): the courier ladder for the LIVE sync path.
//!
//! Loop 9 (2026-09-08 15:53Z): a same-height competition at 965877 left this
//! store 23 min behind the network. ONE courier (WhatsOnChain) fed the live
//! path, a poll fault aborted the cron on `?` before its tail, and nothing
//! recorded why (no observability). The reference (`Chaintracks.addLiveHeader`)
//! asks EVERY live ingestor for a missing parent (`getMissingBlockHeader`);
//! the owner's 2026-09-04 ruling for every chain question that leaves our
//! services: all couriers as each other's fallbacks, a rotating start, a rung
//! that faults three times in a tick skipped for the rest of it, counted,
//! never silent.
//!
//! The ladder widens AVAILABILITY, not authority: every answer is bound to
//! the question (the height or the hash asked), to its own bytes (the hash is
//! recomputed from the fields) and to its own proof of work under the node's
//! rule (`BlockHeader::check_pow`, P0-4), and the store's most-work rule still decides
//! the tip. A courier that answers wrong is a faulting rung, never a header.
//! `chain_info` asks every rung and follows the HIGHEST tip (a lagging courier
//! must never read as the chain); the rung that answered it is asked first
//! for the headers of that tick.
//!
//! Rule 28 (the owner's ruling of 2026-10-09; bsv-stack-lean
//! `docs/p0/rule-28-explorer-calls.md` sections 3 and 5): a third-party chain
//! explorer is break-glass, and every read that leaves our services is judged
//! by one question, what header, proof or own-index answer do we already hold
//! for it. Why THESE reads stay: headers come from outside the service. A
//! header service has no header to hold until it is told one, so no header,
//! proof or index of ours can answer for a block we have not yet been told
//! about. This is the irreducible case of the whole stack, and the ladder is
//! where it lives: every outward header read of the worker (the cron, the
//! read-through, the operator's backfill) goes through it.
//!
//! What makes it safe to take a header from anyone: every answer is
//! re-derived locally (proof of work, the difficulty rule, the checkpoints,
//! ancestry) before it counts. The hash is recomputed from the fields and the
//! work checked here, in `bind`; the bits the node's rule answers, the
//! checkpoint at the height and the link to a stored parent are checked on
//! insert (`storage::insert_header`, `storage::insert_headers_batch`). A
//! header is believed for its work and its ancestry, never for who served it.
//! The one number that is not re-derived is a courier's claimed tip HEIGHT:
//! it only decides whether to ask for headers, and it is recorded as
//! `last_seen_height` (served by `/getPresentHeight` and `/getInfo` as what
//! the couriers said, never as our tip); the headers it leads to are checked
//! like any other.
//!
//! The fallback shape, in the rule's words:
//! - a negative needs a second provider: a rung that does not serve a header
//!   is a fault of that rung, and the next rung is asked; for the tip every
//!   rung is asked and the highest wins, so "no new block" is never one
//!   lagging courier's word while another answers;
//! - "could not look" is never "nothing there": when every rung has faulted
//!   the answer is an error naming each fault, recorded with its time
//!   (`sync_state.last_error`), and the routes answer "unable to verify";
//!   the ladder has no answer that means "no such block";
//! - the start rotates, per minute, so no courier is the one everything
//!   leans on; a courier that answers wrong is a faulting rung with the
//!   evidence in the log, one that rate-limits is skipped after three faults.
//!
//! The rungs are not all explorers: Arcade's chaintracks v2 is a PEER header
//! service; WhatsOnChain and Bitails are explorers. Testnet's ladder is
//! WhatsOnChain alone (`for_chain`): there a negative has no second provider,
//! and it still reads as "could not look".
use std::cell::{Cell, RefCell};

use worker::{Fetch, Headers, Method, Request, RequestInit};

use crate::consensus::ChainParams;
use crate::sync::ChainSource;
use crate::types::{calculate_work, BlockHeader, Chain};
use crate::woc::{WocChainInfo, WocClient};

/// A rung faulting this many times in one tick is skipped for the rest of it.
pub const RUNG_FAULT_CAP: u32 = 3;
/// Arcade's chaintracks v2 (the overlay's proof + reorg source, `ARCADE_URL`).
pub const ARCADE_DEFAULT_URL: &str = "https://arcade-v2-us-1.bsvblockchain.tech";
/// Bitails (`BITAILS_URL`); pruned mode, but its headers answer (probed 2026-09-08).
pub const BITAILS_DEFAULT_URL: &str = "https://api.bitails.io";

fn excerpt(s: &str) -> String {
    s.chars().take(160).collect()
}

fn rust_err(text: String) -> worker::Error {
    worker::Error::RustError(text)
}

async fn fetch_json<T: serde::de::DeserializeOwned>(who: &str, url: &str) -> worker::Result<T> {
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let headers = Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);
    let request = Request::new_with_init(url, &init)?;
    let mut response = Fetch::Request(request).send().await?;
    let status = response.status_code();
    if !(200..300).contains(&status) {
        let body = response.text().await.unwrap_or_default();
        return Err(rust_err(format!("{who} HTTP {status}: {}", excerpt(&body))));
    }
    response
        .json::<T>()
        .await
        .map_err(|e| rust_err(format!("{who} parse error: {e}")))
}

// ─── Arcade chaintracks v2 ──────────────────────────────────────────────────

/// Arcade's chaintracks v2: `/chaintracks/v2/tip`, `/header/height/{h}`,
/// `/header/hash/{hash}` (the reference server's JSON header shape, `bits`
/// a number, hashes in display order; probed live 2026-09-08).
pub struct ArcadeClient {
    base_url: String,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct ArcadeHeader {
    version: u32,
    #[serde(rename = "previousHash")]
    previous_hash: String,
    #[serde(rename = "merkleRoot")]
    merkle_root: String,
    time: u32,
    bits: u32,
    nonce: u32,
    height: u32,
    hash: String,
}

impl ArcadeHeader {
    pub(crate) fn into_block_header(self) -> worker::Result<BlockHeader> {
        let header = BlockHeader {
            header_id: None,
            previous_header_id: None,
            version: self.version,
            previous_hash: self.previous_hash,
            merkle_root: self.merkle_root,
            time: self.time,
            bits: self.bits,
            nonce: self.nonce,
            height: self.height,
            hash: self.hash,
            chain_work: calculate_work(self.bits),
            is_active: true,
            is_chain_tip: false,
        };
        let computed = header.computed_hash();
        if !computed.eq_ignore_ascii_case(&header.hash) {
            return Err(rust_err(format!(
                "Arcade header integrity failure at height {}: claimed hash {} but fields hash to {}",
                header.height, header.hash, computed
            )));
        }
        Ok(header)
    }
}

impl ArcadeClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
    pub async fn get_chain_info(&self) -> worker::Result<WocChainInfo> {
        let url = format!("{}/chaintracks/v2/tip", self.base_url);
        let tip: ArcadeHeader = fetch_json("Arcade", &url).await?;
        Ok(WocChainInfo {
            blocks: tip.height,
            best_block_hash: Some(tip.hash),
        })
    }
    pub async fn get_header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        let url = format!("{}/chaintracks/v2/header/height/{height}", self.base_url);
        let h: ArcadeHeader = fetch_json("Arcade", &url).await?;
        h.into_block_header()
    }
    pub async fn get_header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        let url = format!("{}/chaintracks/v2/header/hash/{hash}", self.base_url);
        let h: ArcadeHeader = fetch_json("Arcade", &url).await?;
        h.into_block_header()
    }
}

// ─── Bitails ────────────────────────────────────────────────────────────────

/// Bitails: `/network/info`, `/block/height/{h}`, `/block/{hash}` (the raw
/// 80-byte header hex in `header`; probed live 2026-09-08).
pub struct BitailsClient {
    base_url: String,
}

#[derive(Debug, serde::Deserialize)]
struct BitailsInfo {
    blocks: u32,
    #[serde(rename = "bestBlockhash")]
    best_block_hash: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct BitailsBlock {
    hash: String,
    height: u32,
    header: String,
}

impl BitailsBlock {
    fn into_block_header(self) -> worker::Result<BlockHeader> {
        let bytes = hex::decode(&self.header)
            .map_err(|e| rust_err(format!("Bitails header hex at height {}: {e}", self.height)))?;
        let header = BlockHeader::from_bytes(&bytes, self.height).ok_or_else(|| {
            rust_err(format!(
                "Bitails header at height {} is not 80 bytes",
                self.height
            ))
        })?;
        if !header.hash.eq_ignore_ascii_case(&self.hash) {
            return Err(rust_err(format!(
                "Bitails header integrity failure at height {}: claimed hash {} but bytes hash to {}",
                self.height, self.hash, header.hash
            )));
        }
        Ok(header)
    }
}

impl BitailsClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
    pub async fn get_chain_info(&self) -> worker::Result<WocChainInfo> {
        let url = format!("{}/network/info", self.base_url);
        let info: BitailsInfo = fetch_json("Bitails", &url).await?;
        Ok(WocChainInfo {
            blocks: info.blocks,
            best_block_hash: info.best_block_hash,
        })
    }
    pub async fn get_header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        let url = format!("{}/block/height/{height}", self.base_url);
        let b: BitailsBlock = fetch_json("Bitails", &url).await?;
        b.into_block_header()
    }
    pub async fn get_header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        let url = format!("{}/block/{hash}", self.base_url);
        let b: BitailsBlock = fetch_json("Bitails", &url).await?;
        b.into_block_header()
    }
}

// ─── One courier on the worker ──────────────────────────────────────────────

pub enum Courier {
    Woc(WocClient),
    Arcade(ArcadeClient),
    Bitails(BitailsClient),
}

impl ChainSource for Courier {
    async fn chain_info(&self) -> worker::Result<WocChainInfo> {
        match self {
            Courier::Woc(c) => c.get_chain_info().await,
            Courier::Arcade(c) => c.get_chain_info().await,
            Courier::Bitails(c) => c.get_chain_info().await,
        }
    }
    async fn header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        match self {
            Courier::Woc(c) => c.get_header_by_height(height).await,
            Courier::Arcade(c) => c.get_header_by_height(height).await,
            Courier::Bitails(c) => c.get_header_by_height(height).await,
        }
    }
    async fn header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        match self {
            Courier::Woc(c) => c.get_header_by_hash(hash).await,
            Courier::Arcade(c) => c.get_header_by_hash(hash).await,
            Courier::Bitails(c) => c.get_header_by_hash(hash).await,
        }
    }
}

// ─── The ladder ─────────────────────────────────────────────────────────────

struct Rung<S> {
    name: &'static str,
    implementation: String,
    source: S,
    observed: RefCell<Option<(WocChainInfo, u64)>>,
    ok: Cell<u32>,
    faults: Cell<u32>,
}

impl<S> Rung<S> {
    fn skipped(&self) -> bool {
        self.faults.get() >= RUNG_FAULT_CAP
    }
}

/// The ladder for ONE cron tick: a rotating start, the highest tip wins the
/// tick's preference, a rung faulting `RUNG_FAULT_CAP` times is skipped for
/// the rest of the tick, every fault kept for the record.
pub struct CourierLadder<S> {
    rungs: Vec<Rung<S>>,
    start: usize,
    preferred: Cell<Option<usize>>,
    faults: RefCell<Vec<String>>,
    /// The chain's rules every answer is bound to (P0-4).
    params: ChainParams,
    split_depth: Option<String>,
}

impl CourierLadder<Courier> {
    /// The worker's ladder: mainnet gets WoC, Arcade and Bitails (the start
    /// rung rotating per minute); testnet keeps WoC alone (the other two serve
    /// no testnet endpoint we have probed).
    pub fn for_chain(
        chain: &Chain,
        woc: WocClient,
        arcade_url: Option<String>,
        bitails_url: Option<String>,
        minute: u64,
    ) -> Self {
        let mut rungs: Vec<(&'static str, Courier)> = vec![("woc", Courier::Woc(woc))];
        if matches!(chain, Chain::Main) {
            rungs.push((
                "arcade",
                Courier::Arcade(ArcadeClient::new(
                    arcade_url.as_deref().unwrap_or(ARCADE_DEFAULT_URL),
                )),
            ));
            rungs.push((
                "bitails",
                Courier::Bitails(BitailsClient::new(
                    bitails_url.as_deref().unwrap_or(BITAILS_DEFAULT_URL),
                )),
            ));
        }
        let n = rungs.len();
        Self::new(
            rungs,
            (minute % n as u64) as usize,
            ChainParams::for_chain(chain),
        )
    }
}

impl<S: ChainSource> CourierLadder<S> {
    pub fn new(rungs: Vec<(&'static str, S)>, start: usize, params: ChainParams) -> Self {
        assert!(!rungs.is_empty(), "a ladder has at least one rung");
        let n = rungs.len();
        Self {
            rungs: rungs
                .into_iter()
                .map(|(name, source)| Rung {
                    name,
                    implementation: crate::rungs::default_implementation(name).into(),
                    source,
                    observed: RefCell::new(None),
                    ok: Cell::new(0),
                    faults: Cell::new(0),
                })
                .collect(),
            start: start % n,
            preferred: Cell::new(None),
            faults: RefCell::new(Vec::new()),
            params,
            split_depth: None,
        }
    }

    pub fn with_configuration(
        mut self,
        definitions: &[crate::rungs::Definition],
        split_depth: Option<String>,
    ) -> Self {
        for rung in &mut self.rungs {
            if let Some(d) = definitions.iter().find(|d| d.name == rung.name) {
                rung.implementation.clone_from(&d.implementation);
            }
        }
        self.split_depth = split_depth;
        self
    }

    /// The rungs to ask, in order: the tick's preferred rung first, then the
    /// rotation from the start rung; a skipped rung is never asked again.
    fn order(&self) -> Vec<usize> {
        let n = self.rungs.len();
        let mut out: Vec<usize> = Vec::with_capacity(n);
        if let Some(p) = self.preferred.get() {
            if !self.rungs[p].skipped() {
                out.push(p);
            }
        }
        for k in 0..n {
            let i = (self.start + k) % n;
            if !out.contains(&i) && !self.rungs[i].skipped() {
                out.push(i);
            }
        }
        out
    }

    fn note_ok(&self, i: usize) {
        self.rungs[i].ok.set(self.rungs[i].ok.get() + 1);
    }

    fn note_fault(&self, i: usize, what: &str, e: &worker::Error) {
        let rung = &self.rungs[i];
        rung.faults.set(rung.faults.get() + 1);
        let text = format!("{}: {what}: {e}", rung.name);
        log!("Cron: courier {text}");
        if rung.faults.get() == RUNG_FAULT_CAP {
            log!(
                "Cron: courier {} faulted {RUNG_FAULT_CAP} times this tick, skipped for the rest of it",
                rung.name
            );
        }
        self.faults.borrow_mut().push(text);
    }

    fn all_faulted(&self, what: &str) -> worker::Error {
        rust_err(format!(
            "{what}: every courier faulted: {}",
            self.faults.borrow().join("; ")
        ))
    }

    /// The tick's per-rung tally, for the cron's log line.
    pub fn summary(&self) -> String {
        self.rungs
            .iter()
            .enumerate()
            .map(|(i, r)| {
                format!(
                    "{} ok {} faults {}{}{}",
                    r.name,
                    r.ok.get(),
                    r.faults.get(),
                    if r.skipped() { " (skipped)" } else { "" },
                    if self.preferred.get() == Some(i) {
                        " (tip)"
                    } else {
                        ""
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(" · ")
    }

    /// E5: the requests the rungs were asked this tick (each answer and each
    /// fault is one), the count the push's reduction of the poll is read in.
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub fn requests(&self) -> u32 {
        self.rungs.iter().map(|r| r.ok.get() + r.faults.get()).sum()
    }

    /// Every fault of the tick, in order (the pins read it).
    #[cfg(test)]
    pub fn faults(&self) -> Vec<String> {
        self.faults.borrow().clone()
    }

    fn bind(&self, name: &str, h: BlockHeader, by: &str) -> worker::Result<BlockHeader> {
        if let Err(fault) = h.check_pow(&self.params) {
            return Err(rust_err(format!(
                "{name} served header {} at {} ({by}) that fails its own proof of work: {fault}",
                h.hash, h.height
            )));
        }
        Ok(h)
    }
}

impl<S: ChainSource> ChainSource for CourierLadder<S> {
    /// Every rung is asked; the HIGHEST tip wins (ties: the first in order),
    /// and its rung is preferred for the tick's headers.
    async fn chain_info(&self) -> worker::Result<WocChainInfo> {
        let mut best: Option<(usize, WocChainInfo)> = None;
        for i in self.order() {
            let observed_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
            match self.rungs[i].source.chain_info().await {
                Ok(info) => {
                    self.note_ok(i);
                    *self.rungs[i].observed.borrow_mut() = Some((
                        WocChainInfo {
                            blocks: info.blocks,
                            best_block_hash: info.best_block_hash.clone(),
                        },
                        observed_at,
                    ));
                    if best.as_ref().is_none_or(|(_, b)| info.blocks > b.blocks) {
                        best = Some((i, info));
                    }
                }
                Err(e) => self.note_fault(i, "chain info", &e),
            }
        }
        match best {
            Some((i, info)) => {
                self.preferred.set(Some(i));
                Ok(info)
            }
            None => Err(self.all_faulted("chain info")),
        }
    }

    async fn header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        let what = format!("header by height {height}");
        for i in self.order() {
            let name = self.rungs[i].name;
            let answer = self.rungs[i]
                .source
                .header_by_height(height)
                .await
                .and_then(|h| {
                    if h.height != height {
                        return Err(rust_err(format!(
                            "{name} answered height {} for height {height}",
                            h.height
                        )));
                    }
                    self.bind(name, h, "by height")
                });
            match answer {
                Ok(h) => {
                    self.note_ok(i);
                    return Ok(h);
                }
                Err(e) => self.note_fault(i, &what, &e),
            }
        }
        Err(self.all_faulted(&what))
    }

    async fn header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        let what = format!("header by hash {hash}");
        for i in self.order() {
            let name = self.rungs[i].name;
            let answer = self.rungs[i]
                .source
                .header_by_hash(hash)
                .await
                .and_then(|h| {
                    if !h.hash.eq_ignore_ascii_case(hash) {
                        return Err(rust_err(format!(
                            "{name} answered hash {} for hash {hash}",
                            h.hash
                        )));
                    }
                    self.bind(name, h, "by hash")
                });
            match answer {
                Ok(h) => {
                    self.note_ok(i);
                    return Ok(h);
                }
                Err(e) => self.note_fault(i, &what, &e),
            }
        }
        Err(self.all_faulted(&what))
    }

    fn report(&self) -> Option<String> {
        Some(self.summary())
    }

    async fn finish_observations(
        &self,
        db: &impl crate::d1::HeaderDb,
        params: &ChainParams,
    ) -> worker::Result<bool> {
        use crate::rungs::{Definition, Observation};
        let depth = crate::rungs::split_depth(self.split_depth.as_deref())?;
        let before = crate::storage::find_chain_tip(db).await?.map(|h| h.hash);
        let mut observations = Vec::with_capacity(self.rungs.len());
        for (i, rung) in self.rungs.iter().enumerate() {
            let observed = rung
                .observed
                .borrow()
                .as_ref()
                .map(|(info, at)| (info.blocks, info.best_block_hash.clone(), *at));
            let mut observation = None;
            if let Some((height, hash, observed_at)) = observed {
                let valid_hash = hash
                    .as_ref()
                    .is_none_or(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()));
                if height > i32::MAX as u32 || !valid_hash {
                    self.note_fault(i, "tip observation", &rust_err("malformed tip".into()));
                } else {
                    let hash = hash.map(|h| h.to_ascii_lowercase());
                    let stored = match &hash {
                        Some(h) => crate::storage::find_header_for_hash(db, h).await?,
                        None => None,
                    };
                    let checked = stored.as_ref().is_some_and(|h| {
                        h.height == height && (h.is_active || h.previous_header_id.is_some())
                    });
                    let mut served_hash = hash.clone();
                    if !checked && !rung.skipped() {
                        let answer = match &hash {
                            Some(h) => rung.source.header_by_hash(h).await,
                            None => rung.source.header_by_height(height).await,
                        }
                        .and_then(|h| {
                            if h.height != height
                                || hash
                                    .as_ref()
                                    .is_some_and(|want| !h.hash.eq_ignore_ascii_case(want))
                            {
                                return Err(rust_err(
                                    "tip header does not match the rung's announcement".into(),
                                ));
                            }
                            self.bind(rung.name, h, "advertised tip")
                        });
                        match answer {
                            Ok(header) => {
                                self.note_ok(i);
                                served_hash = Some(header.hash.to_ascii_lowercase());
                                if let Err(e) = crate::sync::insert_with_parent_backfill(
                                    db, params, self, header,
                                )
                                .await
                                {
                                    crate::sync::record_fault(
                                        db,
                                        &format!("{} tip ingest: {e}", rung.name),
                                    )
                                    .await;
                                }
                            }
                            Err(e) => {
                                self.note_fault(i, "advertised tip", &e);
                                crate::sync::record_fault(db, &format!("{} tip: {e}", rung.name))
                                    .await;
                            }
                        }
                    }
                    if let Some(hash) = served_hash {
                        observation = Some(Observation {
                            hash,
                            height,
                            observed_at,
                        });
                    }
                }
            }
            observations.push((
                Definition {
                    name: rung.name.into(),
                    implementation: rung.implementation.clone(),
                },
                observation,
            ));
        }
        let after = crate::storage::find_chain_tip(db).await?.map(|h| h.hash);
        if before != after {
            crate::sync::revalidate(db, params).await;
        }
        crate::rungs::record(db, &observations, depth).await?;
        Ok(before != after)
    }
}
