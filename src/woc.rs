//! WhatsOnChain HTTP client via worker::Fetch.
//!
//! CF Workers can't use reqwest — all HTTP goes through worker::Fetch.
//! Based on ~/bsv/rust-wallet-infra/src/services/woc.rs and
//! ~/bsv/rust-overlay/crates/overlay-cloudflare/src/chain_tracker.rs.

use serde::Deserialize;
use worker::{console_log, Fetch, Headers, Method, Request, RequestInit};

use crate::types::{BlockHeader, Chain};

// ─── WoC Response Types ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct WocChainInfo {
    pub blocks: u32,
    /// Best block hash — used to detect equal-height reorgs (audit C2): a
    /// tip-height match with a DIFFERENT hash means WoC switched branches
    /// at our height and we must fetch the competitor.
    #[serde(rename = "bestblockhash")]
    pub best_block_hash: Option<String>,
}

/// WoC block header response from /block/{hash}/header or /block/headers
#[derive(Debug, Clone, Deserialize)]
pub struct WocBlockHeader {
    pub hash: String,
    pub height: u32,
    pub version: u32,
    pub merkleroot: String,
    pub time: u32,
    pub nonce: u32,
    pub bits: String,
    #[serde(rename = "previousblockhash")]
    pub previous_block_hash: Option<String>,
}

impl WocBlockHeader {
    /// Convert to our BlockHeader type.
    ///
    /// INTEGRITY (audit M3): the hash is RECOMPUTED from the 80 serialized
    /// header bytes and must match what WoC claimed — the reference rejects
    /// any ingested header whose hash doesn't hash from its own fields
    /// (wallet-toolbox blockHeaderUtilities.ts:371-373 validateHeaderFormat).
    /// A mismatch means WoC served inconsistent fields; storing it would
    /// desync hash-keyed lookups from the stored bytes.
    pub fn into_block_header(self) -> worker::Result<BlockHeader> {
        let bits = u32::from_str_radix(&self.bits, 16).unwrap_or(0);
        let chain_work = crate::types::calculate_work(bits);
        // WoC returns "" (empty string, not null) for genesis-style rows —
        // normalize to the canonical 64-zero hash (review L-5).
        let previous_hash = self
            .previous_block_hash
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "0".repeat(64));

        let header = BlockHeader {
            header_id: None,
            previous_header_id: None,
            version: self.version,
            previous_hash,
            merkle_root: self.merkleroot,
            time: self.time,
            bits,
            nonce: self.nonce,
            height: self.height,
            hash: self.hash,
            chain_work,
            is_active: true,
            is_chain_tip: false,
        };

        let computed = crate::types::compute_block_hash(&header.to_bytes());
        if !computed.eq_ignore_ascii_case(&header.hash) {
            return Err(worker::Error::RustError(format!(
                "WoC header integrity failure at height {}: claimed hash {} but fields hash to {}",
                header.height, header.hash, computed
            )));
        }
        Ok(header)
    }
}

// ─── CDN Types ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkHeaderFilesInfo {
    pub files: Vec<BulkHeaderFileInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkHeaderFileInfo {
    pub file_name: String,
    pub first_height: Option<u32>,
    /// Number of headers in this file. Lets the cron compute the snapshot's
    /// top height and pull only the bytes it needs per tick.
    pub count: Option<u32>,
    /// Hash of the last header in this file (from the CDN index). Used to
    /// verify an imported bulk file is byte-aligned and complete before it is
    /// trusted as the bulk store.
    pub last_hash: Option<String>,
    pub source_url: Option<String>,
}

impl BulkHeaderFileInfo {
    /// Height just past the last header this file provides (exclusive upper
    /// bound). Falls back to the standard 100k stride when `count` is absent.
    pub fn coverage_end(&self) -> u32 {
        self.first_height.unwrap_or(0) + self.count.unwrap_or(100_000)
    }
}

// ─── WoC Client ─────────────────────────────────────────────────────────────

pub struct WocClient {
    base_url: String,
    api_key: Option<String>,
}

impl WocClient {
    pub fn new(chain: &Chain, api_key: Option<String>) -> Self {
        Self {
            base_url: chain.woc_base_url().to_string(),
            api_key,
        }
    }

    /// Build a GET request with optional API key.
    fn build_request(&self, url: &str) -> worker::Result<Request> {
        let mut init = RequestInit::new();
        init.with_method(Method::Get);

        let headers = Headers::new();
        let _ = headers.set("Accept", "application/json");
        if let Some(ref key) = self.api_key {
            if !key.is_empty() {
                // WoC only grants the authenticated quota via the Authorization
                // header (verified live: woc-api-key returns no rate headers).
                let _ = headers.set("Authorization", key);
            }
        }
        init.with_headers(headers);

        Request::new_with_init(url, &init)
    }

    /// Fetch JSON from a URL, returning the parsed response.
    async fn fetch_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> worker::Result<T> {
        let request = self.build_request(url)?;
        let mut response = Fetch::Request(request).send().await?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            let body = response.text().await.unwrap_or_default();
            return Err(worker::Error::RustError(format!(
                "WoC HTTP {status}: {body}"
            )));
        }

        response
            .json::<T>()
            .await
            .map_err(|e| worker::Error::RustError(format!("WoC parse error: {e}")))
    }

    /// Fetch raw binary from a URL.
    async fn fetch_bytes(&self, url: &str) -> worker::Result<Vec<u8>> {
        let mut init = RequestInit::new();
        init.with_method(Method::Get);
        let request = Request::new_with_init(url, &init)?;
        let mut response = Fetch::Request(request).send().await?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            return Err(worker::Error::RustError(format!(
                "CDN HTTP {status} for {url}"
            )));
        }

        response.bytes().await
    }

    /// Fetch a byte range `[start, end]` (inclusive) via an HTTP Range request.
    /// The header CDN answers 206 Partial Content, so a caller can pull just
    /// the slice it needs instead of the whole multi-MB file.
    async fn fetch_bytes_range(&self, url: &str, start: u64, end: u64) -> worker::Result<Vec<u8>> {
        let mut init = RequestInit::new();
        init.with_method(Method::Get);
        let headers = Headers::new();
        let _ = headers.set("Range", &format!("bytes={start}-{end}"));
        init.with_headers(headers);

        let request = Request::new_with_init(url, &init)?;
        let mut response = Fetch::Request(request).send().await?;

        let status = response.status_code();
        // 206 Partial Content (and 200 if the origin ignores Range) are OK.
        if !(200..300).contains(&status) {
            return Err(worker::Error::RustError(format!(
                "CDN range HTTP {status} for {url}"
            )));
        }

        response.bytes().await
    }

    // ─── API Methods ────────────────────────────────────────────────────────

    /// Get current chain info (tip height, best block hash).
    pub async fn get_chain_info(&self) -> worker::Result<WocChainInfo> {
        let url = format!("{}/chain/info", self.base_url);
        self.fetch_json(&url).await
    }

    /// Get block header by height. Returns parsed BlockHeader.
    ///
    /// Uses WoC `/block/height/{N}` which returns full block JSON with header fields.
    pub async fn get_header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        let url = format!("{}/block/height/{height}", self.base_url);
        let woc_header: WocBlockHeader = self.fetch_json(&url).await?;
        woc_header.into_block_header()
    }

    /// Get block header by hash (WoC `/block/hash/{hash}/header`). Used for
    /// competitor-branch and missing-parent backfill during reorgs (audit
    /// C2) — those headers are unreachable by height because a different
    /// branch owns the height locally.
    pub async fn get_header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        // NOTE the path: WoC serves header-by-hash at /block/{hash}/header —
        // NOT /block/hash/{hash}/header (that 404s; verified live 2026-07-07.
        // Adversarial review C-1: the wrong path made ALL reorg backfill
        // machinery dead on arrival).
        let url = format!("{}/block/{hash}/header", self.base_url);
        let woc_header: WocBlockHeader = self.fetch_json(&url).await?;
        woc_header.into_block_header()
    }

    // ─── Bulk CDN ───────────────────────────────────────────────────────────

    /// Fetch the CDN file listing for bulk header download.
    pub async fn get_bulk_file_listing(chain: &Chain) -> worker::Result<BulkHeaderFilesInfo> {
        // Primary CDN is down (DNS not resolving), use legacy CDN
        let cdn_base = "https://cdn.projectbabbage.com/blockheaders";
        let index_url = match chain {
            Chain::Main => format!("{cdn_base}/mainNetBlockHeaders.json"),
            Chain::Test => format!("{cdn_base}/testNetBlockHeaders.json"),
        };

        let mut init = RequestInit::new();
        init.with_method(Method::Get);
        let request = Request::new_with_init(&index_url, &init)?;
        let mut response = Fetch::Request(request).send().await?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            return Err(worker::Error::RustError(format!("CDN index HTTP {status}")));
        }

        response
            .json::<BulkHeaderFilesInfo>()
            .await
            .map_err(|e| worker::Error::RustError(format!("CDN parse: {e}")))
    }

    /// Download and parse a bulk header file (80 bytes per header, concatenated binary).
    pub async fn download_bulk_file(
        &self,
        file_info: &BulkHeaderFileInfo,
        start_height: u32,
    ) -> worker::Result<Vec<BlockHeader>> {
        let cdn_base = "https://cdn.projectbabbage.com/blockheaders";
        let url = file_info
            .source_url
            .as_deref()
            .map(|base| format!("{base}/{}", file_info.file_name))
            .unwrap_or_else(|| format!("{cdn_base}/{}", file_info.file_name));

        console_log!(
            "Downloading bulk headers: {} (height {}+)",
            file_info.file_name,
            start_height
        );

        let bytes = self.fetch_bytes(&url).await?;
        let mut headers: Vec<BlockHeader> = Vec::with_capacity(bytes.len() / 80);

        for (i, chunk) in bytes.chunks(80).enumerate() {
            if chunk.len() < 80 {
                break;
            }
            let height = start_height + i as u32;
            if let Some(header) = BlockHeader::from_bytes(chunk, height) {
                // Linkage guard (audit M4 extension — the CDN path had none):
                // heights are assigned blindly as start+i, so a gap/splice in
                // the file would store every later header at the wrong
                // height. Truncate at the first break; ingest the verified
                // prefix only (TS validateBufferOfHeaders parity).
                if let Some(prev) = headers.last() {
                    if !header.previous_hash.eq_ignore_ascii_case(&prev.hash) {
                        console_log!(
                            "Bulk file {} linkage break at height {} — truncating",
                            file_info.file_name,
                            height
                        );
                        break;
                    }
                }
                headers.push(header);
            } else {
                break;
            }
        }

        console_log!(
            "Parsed {} headers from {}",
            headers.len(),
            file_info.file_name
        );
        Ok(headers)
    }

    /// Download and parse a bounded slice of a bulk header file via an HTTP
    /// Range request — `count` headers starting at `start_height`. Keeps a
    /// single cron tick small enough to always complete (tiny download + parse
    /// + one bounded batch insert) instead of pulling the whole file.
    ///
    /// `file_first_height` is the height of the file's first header (the byte
    /// offset origin); `start_height` must be >= it and inside the file. Same
    /// linkage guard as `download_bulk_file` (audit M4): truncate at the first
    /// header that does not link to its predecessor.
    pub async fn download_bulk_range(
        &self,
        file_info: &BulkHeaderFileInfo,
        file_first_height: u32,
        start_height: u32,
        count: u32,
    ) -> worker::Result<Vec<BlockHeader>> {
        if count == 0 || start_height < file_first_height {
            return Ok(Vec::new());
        }
        let cdn_base = "https://cdn.projectbabbage.com/blockheaders";
        let url = file_info
            .source_url
            .as_deref()
            .map(|base| format!("{base}/{}", file_info.file_name))
            .unwrap_or_else(|| format!("{cdn_base}/{}", file_info.file_name));

        let byte_start = (start_height - file_first_height) as u64 * 80;
        let byte_end = byte_start + (count as u64 * 80) - 1; // inclusive

        console_log!(
            "Range {}: heights {}..{} ({} bytes)",
            file_info.file_name,
            start_height,
            start_height + count,
            count * 80
        );

        let bytes = self.fetch_bytes_range(&url, byte_start, byte_end).await?;
        let mut headers: Vec<BlockHeader> = Vec::with_capacity(bytes.len() / 80);
        for (i, chunk) in bytes.chunks(80).enumerate() {
            if chunk.len() < 80 {
                break;
            }
            let height = start_height + i as u32;
            if let Some(header) = BlockHeader::from_bytes(chunk, height) {
                if let Some(prev) = headers.last() {
                    if !header.previous_hash.eq_ignore_ascii_case(&prev.hash) {
                        console_log!(
                            "Bulk range {} linkage break at height {} — truncating",
                            file_info.file_name,
                            height
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

    /// Fetch the raw bytes of a whole bulk-header CDN file (for one-time R2
    /// import). `mainNet_{idx}.headers` etc. — ~8 MB, 100k×80-byte headers.
    pub async fn download_bulk_file_raw(
        &self,
        chain: &Chain,
        file_idx: u32,
    ) -> worker::Result<Vec<u8>> {
        let url = format!(
            "https://cdn.projectbabbage.com/blockheaders/{}Net_{}.headers",
            chain.as_str(),
            file_idx
        );
        self.fetch_bytes(&url).await
    }
}

/// Fetch a single 80-byte header from the projectbabbage bulk CDN by height
/// (HTTP Range). Read-through fallback for the R2 bulk store so header reads
/// never fail while R2 is being populated or if it has a gap.
pub async fn cdn_header_by_height(
    chain: &Chain,
    height: u32,
) -> worker::Result<Option<BlockHeader>> {
    let idx = height / 100_000;
    let offset = (height % 100_000) as u64 * 80;
    let url = format!(
        "https://cdn.projectbabbage.com/blockheaders/{}Net_{}.headers",
        chain.as_str(),
        idx
    );
    let client = WocClient::new(chain, None);
    let bytes = client.fetch_bytes_range(&url, offset, offset + 79).await?;
    // If the origin ignored the Range and returned other than the requested 80
    // bytes, refuse rather than risk labeling a different block as this height
    // (a wrong header here would reject a valid SPV proof as invalid).
    if bytes.len() != 80 {
        return Ok(None);
    }
    Ok(BlockHeader::from_bytes(&bytes, height))
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_woc_chain_info_deser() {
        let json = serde_json::json!({
            "chain": "main",
            "blocks": 870000,
            "headers": 870000,
            "bestblockhash": "000000000000000003a1b8e956c1a5f5e54e4b0b6e8a3c7d8e9f0a1b2c3d4e5f",
            "difficulty": 123456789.0,
            "mediantime": 1700000000,
            "verificationprogress": 0.9999
        });

        let info: WocChainInfo = serde_json::from_value(json).unwrap();
        assert_eq!(info.blocks, 870000);
    }

    #[test]
    fn test_woc_chain_info_minimal() {
        let json = serde_json::json!({
            "blocks": 100
        });
        let info: WocChainInfo = serde_json::from_value(json).unwrap();
        assert_eq!(info.blocks, 100);
    }

    #[test]
    fn test_woc_block_header_deser() {
        let json = serde_json::json!({
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            "height": 0,
            "version": 1,
            "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
            "time": 1231006505,
            "nonce": 2083236893,
            "bits": "1d00ffff",
            "previousblockhash": null
        });

        let woc: WocBlockHeader = serde_json::from_value(json).unwrap();
        assert_eq!(woc.height, 0);
        assert_eq!(woc.bits, "1d00ffff");
        assert!(woc.previous_block_hash.is_none());

        // Real genesis fields hash to the claimed hash — integrity passes.
        let header = woc.into_block_header().expect("genesis integrity");
        assert_eq!(header.bits, 0x1d00ffff);
        assert_eq!(header.previous_hash, "0".repeat(64));
        assert!(!header.chain_work.is_empty());
        assert!(header.is_active);
        assert!(!header.is_chain_tip);
    }

    /// Integrity regression (audit M3): a header whose claimed hash does
    /// not hash from its own fields must be REJECTED at ingest, matching
    /// wallet-toolbox validateHeaderFormat (blockHeaderUtilities.ts:371-373).
    /// The old code stored WoC's hash verbatim — one inconsistent response
    /// desynced hash-keyed lookups from the stored bytes forever.
    #[test]
    fn test_woc_block_header_integrity_rejects_mismatched_hash() {
        let json = serde_json::json!({
            "hash": "hash_1",           // fabricated — cannot hash from fields
            "height": 1,
            "version": 1,
            "merkleroot": "merkle_1",
            "time": 1231006506,
            "nonce": 12345,
            "bits": "1d00ffff",
            "previousblockhash": "hash_0"
        });

        let woc: WocBlockHeader = serde_json::from_value(json).unwrap();
        assert!(
            woc.into_block_header().is_err(),
            "mismatched hash must be rejected at ingest"
        );
    }

    #[test]
    fn test_woc_bits_hex_parse() {
        // Verify bits hex parsing for various values
        let bits_str = "1d00ffff";
        let bits = u32::from_str_radix(bits_str, 16).unwrap();
        assert_eq!(bits, 0x1d00ffff);

        let bits_str = "170d21b9";
        let bits = u32::from_str_radix(bits_str, 16).unwrap();
        assert_eq!(bits, 0x170d21b9);
    }

    #[test]
    fn test_bulk_file_listing_deser() {
        let json = serde_json::json!({
            "files": [
                {
                    "fileName": "mainNet_0.headers",
                    "firstHeight": 0,
                    "count": 100000,
                    "sourceUrl": "https://bsv-headers.babbage.systems"
                }
            ],
            "headersPerFile": 100000
        });

        let info: BulkHeaderFilesInfo = serde_json::from_value(json).unwrap();
        assert_eq!(info.files.len(), 1);
        assert_eq!(info.files[0].file_name, "mainNet_0.headers");
        assert_eq!(info.files[0].first_height, Some(0));
    }

    #[test]
    fn test_bulk_file_listing_minimal() {
        let json = serde_json::json!({
            "files": []
        });
        let info: BulkHeaderFilesInfo = serde_json::from_value(json).unwrap();
        assert!(info.files.is_empty());
    }

    #[test]
    fn test_woc_full_block_response_deser() {
        // Real response from GET /v1/bsv/main/block/height/1
        // (trimmed to relevant fields — serde ignores unknown fields)
        let json = serde_json::json!({
            "hash": "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
            "confirmations": 944060,
            "size": 215,
            "height": 1,
            "version": 1,
            "versionHex": "00000001",
            "merkleroot": "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098",
            "txcount": 1,
            "nTx": 0,
            "num_tx": 1,
            "time": 1231469665,
            "mediantime": 1231469665,
            "nonce": 2573394689u64,
            "bits": "1d00ffff",
            "difficulty": 1,
            "chainwork": "0000000000000000000000000000000000000000000000000000000200020002",
            "previousblockhash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            "nextblockhash": "000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd"
        });

        let woc: WocBlockHeader = serde_json::from_value(json).unwrap();
        // Real block-1 fields hash to the claimed hash — integrity passes.
        let header = woc.into_block_header().expect("block-1 integrity");
        assert_eq!(header.height, 1);
        assert_eq!(
            header.hash,
            "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048"
        );
        assert_eq!(
            header.previous_hash,
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
        assert_eq!(
            header.merkle_root,
            "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098"
        );
        assert_eq!(header.bits, 0x1d00ffff);
        assert_eq!(header.nonce, 2573394689);
    }
}
