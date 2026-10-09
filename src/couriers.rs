//! a private program loop 10 D5 (2026-09-08): the courier ladder for the LIVE sync path.
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
use std::cell::{Cell, RefCell};

use worker::{Fetch, Headers, Method, Request, RequestInit};

use crate::consensus::ChainParams;
use crate::sync::ChainSource;
use crate::types::{calculate_work, compute_block_hash, BlockHeader, Chain};
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
struct ArcadeHeader {
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
    fn into_block_header(self) -> worker::Result<BlockHeader> {
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
        let computed = compute_block_hash(&header.to_bytes());
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
    source: S,
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
                    source,
                    ok: Cell::new(0),
                    faults: Cell::new(0),
                })
                .collect(),
            start: start % n,
            preferred: Cell::new(None),
            faults: RefCell::new(Vec::new()),
            params,
        }
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
            match self.rungs[i].source.chain_info().await {
                Ok(info) => {
                    self.note_ok(i);
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
}
