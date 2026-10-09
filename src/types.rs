//! Core types for Chaintracks.
//!
//! Based on bsv-wallet-toolbox-rs/src/chaintracks/types.rs.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::consensus::{self, ChainParams, HeaderFault};

/// Network chain identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Chain {
    #[default]
    Main,
    Test,
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Chain {
    pub fn as_str(&self) -> &'static str {
        match self {
            Chain::Main => "main",
            Chain::Test => "test",
        }
    }

    #[allow(dead_code)]
    pub fn woc_base_url(&self) -> &'static str {
        match self {
            Chain::Main => "https://api.whatsonchain.com/v1/bsv/main",
            Chain::Test => "https://api.whatsonchain.com/v1/bsv/test",
        }
    }
}

/// Block header with all fields needed for storage and queries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct BlockHeader {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_header_id: Option<i64>,
    pub version: u32,
    pub previous_hash: String,
    pub merkle_root: String,
    pub time: u32,
    pub bits: u32,
    pub nonce: u32,
    pub height: u32,
    pub hash: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub chain_work: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_active: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_chain_tip: bool,
}

impl BlockHeader {
    /// The header's OWN proof of work, as the node checks it before a header
    /// enters its index (`CheckBlockHeader`, bitcoin-sv v1.2.3
    /// src/validation.cpp:5736-5747, through `CheckProofOfWork`,
    /// src/pow.cpp:150-171): the compact `bits` refused when negative, zero,
    /// overflowing or above the chain's `powLimit`; the claimed hash must be
    /// the hash of the 80 bytes (the store keys rows by it); the hash, read as
    /// a 256-bit number, at most the target. P0-4 (2026-10-08) made this the
    /// check every path runs before a header is bound or stored (a private program loop
    /// 10 D5 had it on the courier ladder alone, with the claimed hash).
    pub fn check_pow(&self, params: &ChainParams) -> Result<(), HeaderFault> {
        consensus::checked_target(self.bits, params)?;
        let computed = compute_block_hash(&self.to_bytes());
        if !computed.eq_ignore_ascii_case(&self.hash) {
            return Err(HeaderFault::HashMismatch {
                claimed: self.hash.clone(),
                computed,
            });
        }
        let hash = consensus::from_hex(&computed).unwrap_or(consensus::ZERO);
        consensus::check_proof_of_work(&hash, self.bits, params)
    }

    /// Serialize header to 80-byte array (Bitcoin wire format).
    ///
    /// Hashes are stored in display format (big-endian, reversed from wire) to
    /// match the production ChaintracksService. This method reverses them back
    /// to internal byte order (little-endian) for the wire format.
    pub fn to_bytes(&self) -> [u8; 80] {
        let mut bytes = [0u8; 80];
        bytes[0..4].copy_from_slice(&self.version.to_le_bytes());

        // previous_hash: stored in display format, must reverse for wire format
        if let Ok(mut prev) = hex::decode(&self.previous_hash) {
            if prev.len() == 32 {
                prev.reverse();
                bytes[4..36].copy_from_slice(&prev);
            }
        }

        // merkle_root: stored in display format, must reverse for wire format
        if let Ok(mut merkle) = hex::decode(&self.merkle_root) {
            if merkle.len() == 32 {
                merkle.reverse();
                bytes[36..68].copy_from_slice(&merkle);
            }
        }

        bytes[68..72].copy_from_slice(&self.time.to_le_bytes());
        bytes[72..76].copy_from_slice(&self.bits.to_le_bytes());
        bytes[76..80].copy_from_slice(&self.nonce.to_le_bytes());
        bytes
    }

    /// Parse a block header from 80 bytes at a given height.
    ///
    /// Stores hashes in display format (big-endian, reversed from wire) to
    /// match the production ChaintracksService and WoC API format.
    #[allow(dead_code)]
    pub fn from_bytes(bytes: &[u8], height: u32) -> Option<Self> {
        if bytes.len() < 80 {
            return None;
        }

        let version = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        // Wire format is internal byte order — reverse for display format
        let mut prev_bytes = bytes[4..36].to_vec();
        prev_bytes.reverse();
        let previous_hash = hex::encode(&prev_bytes);
        let mut merkle_bytes = bytes[36..68].to_vec();
        merkle_bytes.reverse();
        let merkle_root = hex::encode(&merkle_bytes);
        let time = u32::from_le_bytes(bytes[68..72].try_into().ok()?);
        let bits = u32::from_le_bytes(bytes[72..76].try_into().ok()?);
        let nonce = u32::from_le_bytes(bytes[76..80].try_into().ok()?);
        let hash = compute_block_hash(&bytes[0..80]);
        let chain_work = calculate_work(bits);

        Some(Self {
            header_id: None,
            previous_header_id: None,
            version,
            previous_hash,
            merkle_root,
            time,
            bits,
            nonce,
            height,
            hash,
            chain_work,
            is_active: true,
            is_chain_tip: false,
        })
    }
}

/// Double SHA-256 hash of header bytes, returned as hex (reversed for display).
#[allow(dead_code)]
pub fn compute_block_hash(header_bytes: &[u8]) -> String {
    let first = Sha256::digest(header_bytes);
    let second = Sha256::digest(first);
    let mut reversed = second.to_vec();
    reversed.reverse();
    hex::encode(reversed)
}

// ─── Work ────────────────────────────────────────────────────────────────────
//
// The 256-bit arithmetic and the compact decode are the node's, ported in
// `consensus` (P0-4, 2026-10-08). The decode here used to answer the MAXIMUM
// target for an exponent past 256 bits ("treat as max target (work ≈ 1)",
// f86752b:280-285), so a header carrying such bits met its target with any
// hash; `SetCompact` flags it as an overflow and `CheckProofOfWork` refuses it.

/// PER-BLOCK work from difficulty bits: `GetBlockProof` exactly
/// (`consensus::block_proof`), as a 64-char big-endian hex string; zero for a
/// negative, overflowing or zero target, so a malformed header can never win
/// tip selection. Genesis bits 0x1d00ffff → 0x…0100010001 (vector-tested).
pub fn calculate_work(bits: u32) -> String {
    consensus::to_hex(&consensus::block_proof(bits))
}

/// Sum two 64-char hex work values (cumulative chain work accumulation,
/// TS addWork parity). Malformed inputs are treated as zero so legacy rows
/// (which stored a garbage per-block constant) degrade harmlessly.
pub fn add_work(a_hex: &str, b_hex: &str) -> String {
    let a = consensus::from_hex(a_hex).unwrap_or(consensus::ZERO);
    let b = consensus::from_hex(b_hex).unwrap_or(consensus::ZERO);
    consensus::to_hex(&consensus::add(&a, &b))
}

/// True if work `a` is strictly more than `b` (both 64-char hex, fixed
/// width ⇒ lexicographic compare is numeric compare; TS isMoreWork parity).
/// Malformed values compare as zero.
pub fn is_more_work(a_hex: &str, b_hex: &str) -> bool {
    let a = consensus::from_hex(a_hex).unwrap_or(consensus::ZERO);
    let b = consensus::from_hex(b_hex).unwrap_or(consensus::ZERO);
    consensus::cmp(&a, &b) == std::cmp::Ordering::Greater
}

/// Result of inserting a header into storage.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InsertHeaderResult {
    /// True if header was newly added (not a duplicate)
    pub added: bool,
    /// True if this was a duplicate header
    pub dupe: bool,
    /// True if this header is now the active chain tip
    pub is_active_tip: bool,
    /// Depth of reorg if one occurred (0 = no reorg)
    pub reorg_depth: u32,
    /// True if previous header was not found
    pub no_prev: bool,
    /// True if no chain tip exists
    pub no_tip: bool,
}

/// Chaintracks service info (returned by /getInfo).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChaintracksInfo {
    pub chain: String,
    pub height_live: u32,
    pub height_bulk: u32,
    pub header_count: u64,
    pub is_syncing: bool,
    pub storage_type: String,
    /// RFC3339 timestamp of the last successful sync (from sync_state) —
    /// the API-visible freshness signal (audit M6): consumers must be able
    /// to detect a stale tip instead of trusting it blindly.
    pub last_synced_at: Option<String>,
    /// Height recorded at the last successful sync.
    pub last_synced_height: Option<u32>,
    /// Consecutive tip announces no consumer accepted (M19B-G2 round 2; the
    /// announce and any reorg marker are re-sent every cron until one does).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_announce_failures: Option<u32>,
    /// How many times that run reached `ANNOUNCE_STUCK_AFTER` (each with a
    /// loud log line): a dead app-layer, visible rather than silent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_announce_stuck_total: Option<u64>,
    /// The announce schema fault, when a read the announce needs fails: the
    /// error text names the missing column or table (a migration applied in
    /// an earlier shape, or not at all), round 5 LOW-2. Absent when healthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_announce_schema_fault: Option<String>,
    /// a private program loop 10 D5 (2026-09-08): how many blocks the store trails the
    /// highest tip any courier answered on the last cron (`last_seen_height`
    /// minus the stored tip, floored at 0). 0 is healthy; 1 for more than a
    /// few minutes is the loop-9 shape (a parent no courier served); absent
    /// until the first cron after migration 0007.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_lag_blocks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<String>,
    /// The LAST poll fault (a courier refusal, an ingest fault), with its time;
    /// never cleared, a reader judges it by its age.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sync_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sync_error_at: Option<String>,
    /// The courier-health read faulted (migration 0007 not applied): the error
    /// text names the missing column. Absent when healthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_schema_fault: Option<String>,
    /// P0-4: the re-validation of the stored chain under the node's header
    /// rules (migration 0008). `validatedHeight` is the last row it passed;
    /// until `validationComplete` the rows above it are not served as proof;
    /// `validationFault` is the refusal that stopped it (the rows from the
    /// refused one up stay unserved until the operator restarts it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validated_height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_complete: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_fault: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // BSV genesis block header (mainnet) — display format (matching production API)
    const GENESIS_PREV_HASH: &str =
        "0000000000000000000000000000000000000000000000000000000000000000";
    // Merkle root in display format (reversed from wire) — matches production API
    const GENESIS_MERKLE_ROOT: &str =
        "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b";
    const GENESIS_HASH: &str = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";

    fn genesis_header() -> BlockHeader {
        BlockHeader {
            header_id: None,
            previous_header_id: None,
            version: 1,
            previous_hash: GENESIS_PREV_HASH.to_string(),
            merkle_root: GENESIS_MERKLE_ROOT.to_string(),
            time: 1231006505,
            bits: 0x1d00ffff,
            nonce: 2083236893,
            height: 0,
            hash: GENESIS_HASH.to_string(),
            chain_work: String::new(),
            is_active: true,
            is_chain_tip: true,
        }
    }

    #[test]
    fn test_genesis_block_hash() {
        let header = genesis_header();
        let bytes = header.to_bytes();
        let hash = compute_block_hash(&bytes);
        assert_eq!(hash, GENESIS_HASH);
    }

    #[test]
    fn test_to_bytes_from_bytes_roundtrip() {
        let original = genesis_header();
        let bytes = original.to_bytes();
        let parsed = BlockHeader::from_bytes(&bytes, 0).expect("should parse");

        assert_eq!(parsed.version, original.version);
        assert_eq!(parsed.previous_hash, original.previous_hash);
        assert_eq!(parsed.merkle_root, original.merkle_root);
        assert_eq!(parsed.time, original.time);
        assert_eq!(parsed.bits, original.bits);
        assert_eq!(parsed.nonce, original.nonce);
        assert_eq!(parsed.hash, original.hash);
    }

    #[test]
    fn test_from_bytes_too_short() {
        let short = [0u8; 79];
        assert!(BlockHeader::from_bytes(&short, 0).is_none());
    }

    #[test]
    fn test_calculate_work_genesis() {
        // Genesis block bits = 0x1d00ffff → EXACT work 0x0100010001
        // (2^256 / (0x00000000ffff0000…0000 + 1); Bitcoin vector). The old
        // u128 implementation returned the constant …0001 for every real
        // header (audit M2) — this vector would have caught it.
        let work = calculate_work(0x1d00ffff);
        assert_eq!(work.len(), 64);
        assert_eq!(
            work,
            format!("{:0>64}", "100010001"),
            "genesis per-block work must be 0x100010001"
        );
    }

    #[test]
    fn test_calculate_work_modern_bits_exceeds_genesis() {
        // A modern mainnet difficulty (~2^73 work per block) must dwarf
        // genesis work — the old constant made all blocks equal.
        let modern = calculate_work(0x180ed64f);
        let genesis = calculate_work(0x1d00ffff);
        assert!(is_more_work(&modern, &genesis));
        assert_ne!(modern, genesis);
    }

    #[test]
    fn test_add_work_cumulative() {
        // height-1 cumulative chain work on mainnet = 0x200020002
        let g = calculate_work(0x1d00ffff);
        let sum = add_work(&g, &g);
        assert_eq!(sum, format!("{:0>64}", "200020002"));
        // adding malformed (legacy garbage) degrades to identity, not junk
        assert_eq!(add_work(&g, "not-hex"), format!("{:0>64}", "100010001"));
    }

    #[test]
    fn test_is_more_work_ordering() {
        let g = calculate_work(0x1d00ffff);
        let g2 = add_work(&g, &g);
        assert!(is_more_work(&g2, &g));
        assert!(!is_more_work(&g, &g2));
        assert!(!is_more_work(&g, &g));
        // malformed compares as zero — can never win
        assert!(is_more_work(&g, "garbage"));
        assert!(!is_more_work("garbage", &g));
    }

    #[test]
    fn test_calculate_work_zero_mantissa() {
        assert_eq!(calculate_work(0x1d000000), "0".repeat(64));
    }

    #[test]
    fn test_calculate_work_zero_exponent() {
        // Exponent 0 with nonzero mantissa: target decodes to 0 = malformed.
        // Malformed bits now yield ZERO work (old code returned MAX): a
        // garbage header must never be able to WIN tip selection.
        let work = calculate_work(0x00ffffff);
        assert_eq!(work, "0".repeat(64));
    }

    #[test]
    fn test_chain_display() {
        assert_eq!(format!("{}", Chain::Main), "main");
        assert_eq!(format!("{}", Chain::Test), "test");
    }

    #[test]
    fn test_chain_as_str() {
        assert_eq!(Chain::Main.as_str(), "main");
        assert_eq!(Chain::Test.as_str(), "test");
    }

    #[test]
    fn test_insert_header_result_default() {
        let result = InsertHeaderResult::default();
        assert!(!result.added);
        assert!(!result.dupe);
        assert!(!result.is_active_tip);
        assert_eq!(result.reorg_depth, 0);
    }

    #[test]
    fn test_to_bytes_length() {
        let header = genesis_header();
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 80);
    }

    #[test]
    fn test_block_header_serde_roundtrip() {
        let header = genesis_header();
        let json = serde_json::to_string(&header).expect("serialize");
        let parsed: BlockHeader = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.hash, header.hash);
        assert_eq!(parsed.height, header.height);
    }

    // ── Vector Tests: known block data from production ──────────────────
    // These values are captured from a reference chaintracks instance and
    // must match exactly. If any of these fail, our serialization diverges.

    // Block 1 data from production /findHeaderHexForHeight?height=1
    const BLOCK1_HASH: &str = "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048";
    const BLOCK1_PREV_HASH: &str =
        "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
    const BLOCK1_MERKLE_ROOT: &str =
        "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098";

    fn block1_header() -> BlockHeader {
        BlockHeader {
            header_id: None,
            previous_header_id: None,
            version: 1,
            previous_hash: BLOCK1_PREV_HASH.to_string(),
            merkle_root: BLOCK1_MERKLE_ROOT.to_string(),
            time: 1231469665,
            bits: 0x1d00ffff,
            nonce: 2573394689,
            height: 1,
            hash: BLOCK1_HASH.to_string(),
            chain_work: String::new(),
            is_active: true,
            is_chain_tip: false,
        }
    }

    #[test]
    fn test_block1_hash_vector() {
        let header = block1_header();
        let bytes = header.to_bytes();
        let hash = compute_block_hash(&bytes);
        assert_eq!(hash, BLOCK1_HASH, "Block 1 hash must match production");
    }

    #[test]
    fn test_block1_chains_to_genesis() {
        let b1 = block1_header();
        assert_eq!(b1.previous_hash, GENESIS_HASH);
    }

    #[test]
    fn test_genesis_to_bytes_hex_matches_production() {
        // Production getHeaders?height=0&count=1 returns this exact hex
        let expected_hex = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let header = genesis_header();
        let actual_hex = hex::encode(header.to_bytes());
        assert_eq!(
            actual_hex, expected_hex,
            "Genesis to_bytes hex must match production getHeaders response"
        );
    }

    #[test]
    fn test_block1_to_bytes_hex_matches_production() {
        // Production getHeaders?height=1&count=1 returns this hex
        // (extracted from getHeaders?height=0&count=2, second 160 chars)
        let expected_hex = "010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e36299";
        let header = block1_header();
        let actual_hex = hex::encode(header.to_bytes());
        assert_eq!(
            actual_hex, expected_hex,
            "Block 1 to_bytes hex must match production getHeaders response"
        );
    }

    #[test]
    fn test_concatenated_headers_match_production() {
        // Production getHeaders?height=0&count=2 returns genesis+block1 concatenated
        let expected = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e36299";
        let genesis_hex = hex::encode(genesis_header().to_bytes());
        let block1_hex = hex::encode(block1_header().to_bytes());
        let actual = format!("{genesis_hex}{block1_hex}");
        assert_eq!(
            actual, expected,
            "Concatenated headers must match production getHeaders?height=0&count=2"
        );
    }

    #[test]
    fn test_from_bytes_genesis_vector() {
        // Parse the production hex back and verify all fields
        let hex_str = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
        let bytes = hex::decode(hex_str).unwrap();
        let header = BlockHeader::from_bytes(&bytes, 0).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.previous_hash, GENESIS_PREV_HASH);
        assert_eq!(header.merkle_root, GENESIS_MERKLE_ROOT);
        assert_eq!(header.time, 1231006505);
        assert_eq!(header.bits, 0x1d00ffff);
        assert_eq!(header.nonce, 2083236893);
        assert_eq!(header.hash, GENESIS_HASH);
    }

    #[test]
    fn test_from_bytes_block1_vector() {
        let hex_str = "010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e36299";
        let bytes = hex::decode(hex_str).unwrap();
        let header = BlockHeader::from_bytes(&bytes, 1).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.previous_hash, BLOCK1_PREV_HASH);
        assert_eq!(header.merkle_root, BLOCK1_MERKLE_ROOT);
        assert_eq!(header.time, 1231469665);
        assert_eq!(header.bits, 0x1d00ffff);
        assert_eq!(header.nonce, 2573394689);
        assert_eq!(header.hash, BLOCK1_HASH);
    }

    #[test]
    fn test_serde_matches_production_response() {
        // Production /findHeaderHexForHeight?height=0 returns this JSON
        let production_json = serde_json::json!({
            "version": 1,
            "previousHash": "0000000000000000000000000000000000000000000000000000000000000000",
            "merkleRoot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
            "time": 1231006505,
            "bits": 486604799,
            "nonce": 2083236893,
            "height": 0,
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        });

        // Parse as our type
        let header: BlockHeader = serde_json::from_value(production_json).unwrap();
        assert_eq!(header.hash, GENESIS_HASH);
        assert_eq!(
            header.merkle_root,
            "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
        );
        assert_eq!(header.bits, 0x1d00ffff);
    }
}
