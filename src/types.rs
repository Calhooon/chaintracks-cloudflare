//! Core types for Chaintracks.
//!
//! Based on bsv-wallet-toolbox-rs/src/chaintracks/types.rs.

use bsv_rs::block::ChainWork;
use serde::{Deserialize, Serialize};

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
    /// check every path runs before a header is bound or stored (bsv-low loop
    /// 10 D5 had it on the courier ladder alone, with the claimed hash). The
    /// rule is bsv-rs 0.6.0's (`bsv_rs::block::check_proof_of_work`); the
    /// claimed-hash comparison is this store's.
    pub fn check_pow(&self, params: &ChainParams) -> Result<(), HeaderFault> {
        consensus::check_bits(self.bits, params)?;
        let wire = self.wire();
        let computed = wire.id();
        if !computed.eq_ignore_ascii_case(&self.hash) {
            return Err(HeaderFault::HashMismatch {
                claimed: self.hash.clone(),
                computed,
            });
        }
        consensus::check_proof_of_work(&wire.hash(), self.bits, params)
    }

    /// The 80-byte frame as bsv-rs types it (`bsv_rs::block::BlockHeader`,
    /// bitcoin-sv v1.2.3 src/primitives/block.h:23-65). The row stores the
    /// hashes in display order; the frame holds them in internal order. A
    /// stored hash that is not 64 hex digits frames as zeros, as `to_bytes`
    /// always has (its claimed hash then cannot match, and `check_pow` refuses
    /// it). The version's bits are the same as an `i32`'s (the node's type).
    pub fn wire(&self) -> bsv_rs::block::BlockHeader {
        bsv_rs::block::BlockHeader {
            version: self.version as i32,
            prev_hash: internal_order(&self.previous_hash),
            merkle_root: internal_order(&self.merkle_root),
            time: self.time,
            bits: self.bits,
            nonce: self.nonce,
        }
    }

    /// The header's own hash in display order (`bsv_rs::block::BlockHeader::id`).
    pub fn computed_hash(&self) -> String {
        self.wire().id()
    }

    /// Serialize header to 80-byte array (Bitcoin wire format), through
    /// `bsv_rs::block::BlockHeader::to_binary`.
    pub fn to_bytes(&self) -> [u8; 80] {
        self.wire().to_binary()
    }

    /// Parse a block header from exactly 80 bytes at a given height
    /// (`bsv_rs::block::BlockHeader::from_binary`, the sibling's `decode`).
    /// Every caller hands it one 80-byte chunk; a longer slice is refused
    /// (until bsv-rs 0.6.0 the bytes past 80 were ignored).
    ///
    /// Stores hashes in display format (big-endian, reversed from wire) to
    /// match the production ChaintracksService and WoC API format.
    pub fn from_bytes(bytes: &[u8], height: u32) -> Option<Self> {
        let wire = bsv_rs::block::BlockHeader::from_binary(bytes).ok()?;
        Some(Self {
            header_id: None,
            previous_header_id: None,
            version: wire.version as u32,
            previous_hash: wire.prev_id(),
            merkle_root: wire.merkle_root_hex(),
            time: wire.time,
            bits: wire.bits,
            nonce: wire.nonce,
            height,
            hash: wire.id(),
            chain_work: calculate_work(wire.bits),
            is_active: true,
            is_chain_tip: false,
        })
    }
}

/// A display-order hash as the frame carries it (internal order); zeros when
/// it is not 64 hex digits.
fn internal_order(display: &str) -> bsv_rs::primitives::Hash32 {
    let mut out = [0u8; 32];
    if let Ok(bytes) = hex::decode(display) {
        if bytes.len() == 32 {
            out.copy_from_slice(&bytes);
            out.reverse();
        }
    }
    out
}

// ─── Work ────────────────────────────────────────────────────────────────────
//
// The 256-bit arithmetic and the compact decode are bsv-rs 0.6.0's
// (`bsv_rs::block::ChainWork`, `CompactTarget`; until 0.4.0 the node's port in
// `consensus`, P0-4, 2026-10-08). The decode here used to answer the MAXIMUM
// target for an exponent past 256 bits ("treat as max target (work ≈ 1)",
// f86752b:280-285), so a header carrying such bits met its target with any
// hash; `SetCompact` flags it and `CheckProofOfWork` refuses it.
//
// The store keeps chain work as 64 hex digits; these three read and write that
// form. A stored value that is not 64 hex digits reads as zero: the legacy
// rows' tolerance stays here, at the store, and bsv-rs's `ChainWork::from_hex`
// refuses (the ruling of 2026-10-12, Q5).

/// A stored chain work; zero when it is not 64 hex digits.
fn stored_work(hex: &str) -> ChainWork {
    if hex.len() != 64 {
        return ChainWork::ZERO;
    }
    ChainWork::from_hex(hex).unwrap_or(ChainWork::ZERO)
}

/// PER-BLOCK work from difficulty bits: `GetBlockProof` exactly
/// (`ChainWork::from_bits`), as a 64-char big-endian hex string; zero for a
/// negative, overflowing or zero target, so a malformed header can never win
/// tip selection. Genesis bits 0x1d00ffff → 0x…0100010001 (vector-tested).
pub fn calculate_work(bits: u32) -> String {
    ChainWork::from_bits(bits).to_hex()
}

/// Sum two 64-char hex work values (cumulative chain work accumulation,
/// TS addWork parity), modulo 2^256 as the node's `nChainWork`. Malformed
/// inputs are treated as zero so legacy rows (which stored a garbage
/// per-block constant) degrade harmlessly.
pub fn add_work(a_hex: &str, b_hex: &str) -> String {
    stored_work(a_hex).wrapping_add(stored_work(b_hex)).to_hex()
}

/// True if work `a` is strictly more than `b` (numeric, `ChainWork`'s `Ord`;
/// TS isMoreWork parity). Malformed values compare as zero.
pub fn is_more_work(a_hex: &str, b_hex: &str) -> bool {
    stored_work(a_hex) > stored_work(b_hex)
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
    /// bsv-low loop 10 D5 (2026-09-08): how many blocks the store trails the
    /// highest tip any courier answered on the last cron (`last_seen_height`
    /// minus the stored tip, floored at 0). 0 is healthy; 1 for more than a
    /// few minutes is the loop-9 shape (a parent no courier served); absent
    /// until the first cron after migration 0007.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_lag_blocks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_height: Option<u32>,
    #[serde(default)]
    pub rung_tips: Vec<crate::rungs::RungTip>,
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
        assert_eq!(header.computed_hash(), GENESIS_HASH);
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
        // Amended at 0.4.0: the frame is exactly 80 bytes (bsv-rs 0.6.0
        // `BlockHeader::from_binary`, the sibling's `decode`); a longer slice
        // is refused where the bytes past 80 used to be ignored.
        let long = [0u8; 81];
        assert!(BlockHeader::from_bytes(&long, 0).is_none());
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
        assert_eq!(
            header.computed_hash(),
            BLOCK1_HASH,
            "Block 1 hash must match production"
        );
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
