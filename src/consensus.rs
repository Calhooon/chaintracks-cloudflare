//! The node's header rules, ported (P0-4, bsv-stack-lean #35, 2026-10-08).
//!
//! Every rule here is the node's, read at the sibling's pin: bitcoin-sv
//! v1.2.3 = `6504a3aff65ba97c0f6c80962b033e35ecbfed4b` (bsv-script-lean
//! docs/PROVENANCE.md §1; the sibling's issue #63 owns header validity and
//! proof of work). Nothing here is re-derived: each function names the node
//! function it ports and the lines it was read from with
//! `git show v1.2.3:<path>`.
//!
//! - the compact encoding (`SetCompact`, `GetCompact`, src/arith_uint256.cpp:182-225),
//!   `CheckProofOfWork` (src/pow.cpp:150-171) and `GetBlockProof`
//!   (src/block_index.cpp:114-127) are bsv-rs's (`bsv_rs::block`), read at
//!   the same pin; `check_proof_of_work` here names its refusals as `HeaderFault`.
//! - the difficulty rule is bsv-rs 0.8.1's (`src/block` as at 0.7.2;
//!   `bsv_rs::block::next_work_required` and `ChainParams`):`GetNextWorkRequired` with its three regimes,
//!   src/pow.cpp:95-117 (22-93, 119-148, 177-297), `GetMedianTimePast`
//!   (src/block_index.h:720-737) and the `SetChainWork` sums
//!   (src/block_index.h:661-666), with the `powLimit`, spacing, timespan and
//!   `daaHeight` of each chain (src/chainparams.cpp:1047-1068, :1343-1364,
//!   :1471-1490). This module ported them until 0.5.0; bsv-rs carries that
//!   port with its citations. What stays here is the host's: the genesis
//!   hash, the checkpoints, the `Window` the store loads, and the faults.
//! - `check_checkpoint`: `Checkpoints::CheckBlock`, src/checkpoints.cpp:14-22, and
//!   the checkpoint lists of src/chainparams.cpp:1124-1160 (main), :1419-1428
//!   (testnet), :1530-1533 (regtest).

use std::fmt;

use bsv_rs::block::{
    ChainWork, CompactTarget, DifficultyError, HeaderAncestry, HeaderLink, PowError,
};
use bsv_rs::primitives::{Hash32, U256};

// ─── The primitives ─────────────────────────────────────────────────────────
//
// The 256-bit arithmetic, the compact encoding (`SetCompact`, `GetCompact`),
// `CheckProofOfWork`, `GetBlockProof`, the pow limits and, from 0.5.0, the
// difficulty rule are bsv-rs's (`bsv_rs::primitives::U256`, `bsv_rs::block`),
// read at the same node pin and the sibling's #63 and #64. Until 0.4.0 the
// primitives were ported here, until 0.5.0 the rule (the ruling of
// 2026-10-12, Q2: it moves into bsv-rs with the chain parameters); the node's
// tables that tested each port run in bsv-rs's own suite as well as below.

/// Exactly 64 hex digits: a block hash in display order, a stored chain work.
fn is_hash_hex(s: &str) -> bool {
    s.len() == 64 && U256::from_hex(s).is_ok()
}

// ─── The chain parameters ───────────────────────────────────────────────────

/// The consensus parameters a header check reads, per chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainParams {
    /// The node's network name (`CBaseChainParams`): "main", "test", "regtest".
    pub network: &'static str,
    /// What the difficulty rule and `CheckProofOfWork` read: `powLimit`, the
    /// spacing and timespan, the minimum-difficulty and no-retarget flags,
    /// `daaHeight` (bsv-rs's, with the node's lines).
    pub pow: bsv_rs::block::ChainParams,
    /// `consensus.hashGenesisBlock`, display order.
    pub genesis_hash: String,
    /// `checkpointData`, ascending by height, hashes in display order. The
    /// node's own list is the default; `with_checkpoints` adds the owner's.
    pub checkpoints: Vec<(u32, String)>,
}

/// `CMainParams::checkpointData` (src/chainparams.cpp:1124-1160), verbatim.
const MAIN_CHECKPOINTS: &[(u32, &str)] = &[
    (
        11111,
        "0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d",
    ),
    (
        33333,
        "000000002dd5588a74784eaa7ab0507a18ad16a236e7b1ce69f00d7ddfb5d0a6",
    ),
    (
        74000,
        "0000000000573993a3c9e41ce34471c079dcf5f52a0e824a81e7f953b8661a20",
    ),
    (
        105000,
        "00000000000291ce28027faea320c8d2b054b2e0fe44a773f3eefb151d6bdc97",
    ),
    (
        134444,
        "00000000000005b12ffd4cd315cd34ffd4a594f430ac814c91184a0d42d2b0fe",
    ),
    (
        168000,
        "000000000000099e61ea72015e79632f216fe6cb33d7899acb35b75c8303b763",
    ),
    (
        193000,
        "000000000000059f452a5f7340de6682a977387c17010ff6e6c3bd83ca8b1317",
    ),
    (
        210000,
        "000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e",
    ),
    (
        216116,
        "00000000000001b4f4b433e81ee46494af945cf96014816a4e2370f11b23df4e",
    ),
    (
        225430,
        "00000000000001c108384350f74090433e7fcf79a606b8e797f065b130575932",
    ),
    (
        250000,
        "000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214",
    ),
    (
        279000,
        "0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40",
    ),
    (
        295000,
        "00000000000000004d9b4ef50f0f9d686fd69db2e03af35a100370c64632a983",
    ),
    (
        478558,
        "0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43",
    ),
    (
        504031,
        "0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c",
    ),
    (
        530359,
        "0000000000000000011ada8bd08f46074f44a8f155396f43e38acf9501c49103",
    ),
];

/// `CTestNetParams::checkpointData` (src/chainparams.cpp:1419-1428), verbatim.
const TEST_CHECKPOINTS: &[(u32, &str)] = &[
    (
        546,
        "000000002a936ca763904c3c35fce2f3556c559c0214345d31b1bcebf76acb70",
    ),
    (
        1155875,
        "00000000f17c850672894b9a75b63a1e72830bbd5f4c8889b5c1a80e7faef138",
    ),
    (
        1188697,
        "0000000000170ed0918077bde7b4d36cc4c91be69fa09211f748240dabe047fb",
    ),
];

/// `CRegTestParams::checkpointData` (src/chainparams.cpp:1530-1533): its genesis.
const REGTEST_CHECKPOINTS: &[(u32, &str)] = &[(
    0,
    "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
)];

fn owned(list: &[(u32, &str)]) -> Vec<(u32, String)> {
    list.iter().map(|(h, s)| (*h, s.to_string())).collect()
}

impl ChainParams {
    /// `CMainParams` (src/chainparams.cpp:1033-1160): the limits and
    /// `daaHeight` bsv-rs's, the genesis hash asserted at 1097-1099.
    pub fn main() -> Self {
        Self {
            network: "main",
            pow: bsv_rs::block::ChainParams::main(),
            genesis_hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
                .to_string(),
            checkpoints: owned(MAIN_CHECKPOINTS),
        }
    }

    /// `CTestNetParams` (src/chainparams.cpp:1329-1428): the limits and
    /// `daaHeight` bsv-rs's, the genesis hash asserted at 1388-1390.
    pub fn test() -> Self {
        Self {
            network: "test",
            pow: bsv_rs::block::ChainParams::test(),
            genesis_hash: "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
                .to_string(),
            checkpoints: owned(TEST_CHECKPOINTS),
        }
    }

    /// `CRegTestParams` (src/chainparams.cpp:1456-1533): the limits and
    /// `daaHeight` bsv-rs's, the genesis hash asserted at 1513-1515. The service never serves regtest; the host
    /// tests mine their fixtures under it, as the node's own regtest does.
    #[allow(dead_code)]
    pub fn regtest() -> Self {
        Self {
            network: "regtest",
            pow: bsv_rs::block::ChainParams::regtest(),
            genesis_hash: "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"
                .to_string(),
            checkpoints: owned(REGTEST_CHECKPOINTS),
        }
    }

    pub fn for_chain(chain: &crate::types::Chain) -> Self {
        match chain {
            crate::types::Chain::Main => Self::main(),
            crate::types::Chain::Test => Self::test(),
        }
    }

    /// The owner's checkpoints on top of the node's: `"height:hash,..."`
    /// (the `CHECKPOINTS` var). A height already listed takes the new hash.
    /// Anything malformed is an error, never a silent default.
    pub fn with_checkpoints(mut self, spec: &str) -> Result<Self, String> {
        for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let (h, hash) = entry
                .split_once(':')
                .ok_or_else(|| format!("checkpoint {entry:?}: expected height:hash"))?;
            let height: u32 = h
                .trim()
                .parse()
                .map_err(|e| format!("checkpoint {entry:?}: height: {e}"))?;
            let hash = hash.trim().to_ascii_lowercase();
            if !is_hash_hex(&hash) {
                return Err(format!("checkpoint {entry:?}: hash is not 64 hex digits"));
            }
            match self.checkpoints.iter_mut().find(|(ch, _)| *ch == height) {
                Some(slot) => slot.1 = hash,
                None => self.checkpoints.push((height, hash)),
            }
        }
        self.checkpoints.sort_by_key(|(h, _)| *h);
        Ok(self)
    }

    /// The checkpoint at `height`, if the list has one.
    pub fn checkpoint_at(&self, height: u32) -> Option<&str> {
        self.checkpoints
            .iter()
            .find(|(h, _)| *h == height)
            .map(|(_, hash)| hash.as_str())
    }
}

// ─── The faults ─────────────────────────────────────────────────────────────

/// Why a header is refused. Each names the node's reject reason where the
/// node has one (`high-hash` is `CheckBlockHeader`'s, src/validation.cpp:5739-5744).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderFault {
    /// The claimed hash is not the double SHA-256 of the 80 bytes. The node
    /// computes the hash itself and has no claimed one; the store keys every
    /// row by the claimed hash, so the two must be the same.
    HashMismatch { claimed: String, computed: String },
    /// `CheckProofOfWork`: the compact target has the sign bit set.
    BitsNegative { bits: u32 },
    /// `CheckProofOfWork`: the compact target reaches past 256 bits.
    BitsOverflow { bits: u32 },
    /// `CheckProofOfWork`: the compact target decodes to zero.
    BitsZero { bits: u32 },
    /// `CheckProofOfWork`: the target is easier than the chain's `powLimit`.
    BitsAboveLimit { bits: u32 },
    /// `CheckProofOfWork`: the hash is above the target.
    HighHash { bits: u32 },
    /// `ContextualCheckBlockHeader`: the bits are not `GetNextWorkRequired`'s
    /// answer for the header's parent and time (src/validation.cpp:5943-5948).
    BadDiffBits { expected: u32, got: u32 },
    /// `CheckIndexAgainstCheckpoint`: a header at a checkpoint height whose
    /// hash is not the checkpoint's (src/validation.cpp:5910-5916).
    CheckpointMismatch { height: u32, expected: String },
    /// `CheckIndexAgainstCheckpoint`: a new header below the last checkpoint
    /// the store holds (src/validation.cpp:5918-5928).
    ForkPriorToCheckpoint { height: u32, checkpoint: u32 },
    /// `FindPreviousBlockIndex`: the parent is not held (src/validation.cpp:6121-6124),
    /// and the header is neither the genesis nor a checkpoint to anchor on.
    PrevNotFound { prev: String },
    /// A header at height 0 that is not the chain's genesis.
    BadGenesis { hash: String },
    /// The rule needs an ancestor the store does not hold (a hole below the
    /// parent): the bits cannot be checked, so the header is refused.
    AncestryMissing { height: i64 },
    /// The rule has no answer where the node asserts or throws (a parent too
    /// low for its regime, no work between the DAA's suitable blocks,
    /// parameters with no adjustment interval): the header is refused.
    NoDifficultyAnswer { error: DifficultyError },
}

impl HeaderFault {
    /// The node's reject reason for this fault.
    pub fn reason(&self) -> &'static str {
        match self {
            HeaderFault::HashMismatch { .. } => "bad-hash",
            HeaderFault::BitsNegative { .. }
            | HeaderFault::BitsOverflow { .. }
            | HeaderFault::BitsZero { .. }
            | HeaderFault::BitsAboveLimit { .. }
            | HeaderFault::HighHash { .. } => "high-hash",
            HeaderFault::BadDiffBits { .. } => "bad-diffbits",
            HeaderFault::CheckpointMismatch { .. } => "checkpoint mismatch",
            HeaderFault::ForkPriorToCheckpoint { .. } => "bad-fork-prior-to-checkpoint",
            HeaderFault::PrevNotFound { .. } => "prev-blk-not-found",
            HeaderFault::BadGenesis { .. } => "bad-genesis",
            HeaderFault::AncestryMissing { .. } => "ancestry-missing",
            HeaderFault::NoDifficultyAnswer { .. } => "no-difficulty-answer",
        }
    }
}

impl fmt::Display for HeaderFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = self.reason();
        match self {
            HeaderFault::HashMismatch { claimed, computed } => {
                write!(
                    f,
                    "{reason}: claimed hash {claimed}, the fields hash to {computed}"
                )
            }
            HeaderFault::BitsNegative { bits } => {
                write!(f, "{reason}: bits {bits:#010x} negative")
            }
            HeaderFault::BitsOverflow { bits } => {
                write!(f, "{reason}: bits {bits:#010x} overflow")
            }
            HeaderFault::BitsZero { bits } => write!(f, "{reason}: bits {bits:#010x} zero target"),
            HeaderFault::BitsAboveLimit { bits } => {
                write!(
                    f,
                    "{reason}: bits {bits:#010x} above the proof-of-work limit"
                )
            }
            HeaderFault::HighHash { bits } => {
                write!(f, "{reason}: hash above the target of bits {bits:#010x}")
            }
            HeaderFault::BadDiffBits { expected, got } => {
                write!(
                    f,
                    "{reason}: bits {got:#010x}, the rule's answer is {expected:#010x}"
                )
            }
            HeaderFault::CheckpointMismatch { height, expected } => {
                write!(f, "{reason}: at {height} the checkpoint is {expected}")
            }
            HeaderFault::ForkPriorToCheckpoint { height, checkpoint } => {
                write!(
                    f,
                    "{reason}: {height} is below the checkpoint at {checkpoint}"
                )
            }
            HeaderFault::PrevNotFound { prev } => write!(f, "{reason}: parent {prev}"),
            HeaderFault::BadGenesis { hash } => write!(f, "{reason}: {hash}"),
            HeaderFault::AncestryMissing { height } => {
                write!(f, "{reason}: no stored ancestor at {height}")
            }
            HeaderFault::NoDifficultyAnswer { error } => write!(f, "{reason}: {error}"),
        }
    }
}

// ─── Proof of work ──────────────────────────────────────────────────────────

/// A bsv-rs refusal as this service names it. Every one is the node's
/// `high-hash` (the node does not tell them apart). bsv-rs names them in the
/// node's test order (src/pow.cpp:159-160: negative, zero, overflow, above the
/// limit), so an exponent past 256 bits, whose word truncates to zero, comes
/// back as a zero target; the service names the overflow before the zero, as
/// it always has, so the refusal it records and serves reads the same.
fn pow_fault(fault: PowError, bits: u32) -> HeaderFault {
    match fault {
        PowError::BitsNegative { bits } => HeaderFault::BitsNegative { bits },
        PowError::BitsZero { bits } if CompactTarget::from_bits(bits).overflow => {
            HeaderFault::BitsOverflow { bits }
        }
        PowError::BitsZero { bits } => HeaderFault::BitsZero { bits },
        PowError::BitsOverflow { bits } => HeaderFault::BitsOverflow { bits },
        PowError::BitsAboveLimit { bits } => HeaderFault::BitsAboveLimit { bits },
        PowError::HighHash { bits } => HeaderFault::HighHash { bits },
        _ => HeaderFault::HighHash { bits },
    }
}

/// The `bits` alone, refused exactly where `CheckProofOfWork` refuses them
/// (src/pow.cpp:156-163): negative, zero, overflowing, or above `powLimit`.
/// The zero hash meets every target the bits may carry, so a refusal here is
/// the bits', never the hash's.
pub(crate) fn check_bits(bits: u32, params: &ChainParams) -> Result<(), HeaderFault> {
    check_proof_of_work(&[0u8; 32], bits, params)
}

/// `CheckProofOfWork(hash, nBits)` (src/pow.cpp:150-171), through
/// `bsv_rs::block::check_proof_of_work`; `hash` in internal order.
pub(crate) fn check_proof_of_work(
    hash: &Hash32,
    bits: u32,
    params: &ChainParams,
) -> Result<(), HeaderFault> {
    bsv_rs::block::check_proof_of_work(hash, bits, &params.pow.pow_limit)
        .map_err(|fault| pow_fault(fault, bits))
}

// ─── Checkpoints ────────────────────────────────────────────────────────────

/// `Checkpoints::CheckBlock` (src/checkpoints.cpp:14-22): a header at a checkpoint
/// height must carry the checkpoint's hash.
pub(crate) fn check_checkpoint(
    height: u32,
    hash: &str,
    params: &ChainParams,
) -> Result<(), HeaderFault> {
    match params.checkpoint_at(height) {
        Some(expected) if !expected.eq_ignore_ascii_case(hash) => {
            Err(HeaderFault::CheckpointMismatch {
                height,
                expected: expected.to_string(),
            })
        }
        _ => Ok(()),
    }
}

// ─── The difficulty rule ────────────────────────────────────────────────────

/// What the rule reads of a header: bsv-rs's `HeaderLink`.
pub(crate) type Link = HeaderLink;

impl From<&crate::types::BlockHeader> for Link {
    fn from(h: &crate::types::BlockHeader) -> Self {
        Link {
            height: h.height,
            time: h.time,
            bits: h.bits,
        }
    }
}

/// A bsv-rs refusal as this service names it: an ancestor the store does not
/// hold is the hole it always was; the rest are the node's asserts.
fn difficulty_fault(error: DifficultyError) -> HeaderFault {
    match error {
        DifficultyError::MissingAncestor { height } => HeaderFault::AncestryMissing {
            height: height as i64,
        },
        error => HeaderFault::NoDifficultyAnswer { error },
    }
}

/// A run of consecutive ancestors, ascending by height, each the parent of
/// the next (the caller has checked the hash links: `storage::load_window`,
/// `storage::insert_headers_batch`): the `HeaderAncestry` bsv-rs's rule
/// reads. The chain work between two of them is the node's `nChainWork`
/// difference (`SetChainWork`, src/block_index.h:661-666), answered from
/// prefix sums computed once per window rather than bsv-rs's default sum.
pub(crate) struct Window {
    links: Vec<Link>,
    /// `work[i]` = the proofs of `links[1..=i]`.
    work: Vec<ChainWork>,
}

impl Window {
    pub fn new(links: Vec<Link>) -> Self {
        let mut w = Window {
            links: Vec::with_capacity(links.len()),
            work: Vec::with_capacity(links.len()),
        };
        for link in links {
            w.push(link);
        }
        w
    }

    pub fn push(&mut self, link: Link) {
        let sum = match self.work.last() {
            Some(last) => last.wrapping_add(ChainWork::from_bits(link.bits)),
            None => ChainWork::ZERO,
        };
        self.links.push(link);
        self.work.push(sum);
    }

    fn index(&self, height: u32) -> Option<usize> {
        let i = height.checked_sub(self.links.first()?.height)? as usize;
        (i < self.links.len()).then_some(i)
    }
}

impl HeaderAncestry for Window {
    fn link_at(&self, height: u32) -> Option<Link> {
        self.index(height).map(|i| self.links[i])
    }

    fn work_between(&self, first: u32, last: u32) -> Result<ChainWork, DifficultyError> {
        let at = |height| {
            self.index(height)
                .map(|i| self.work[i])
                .ok_or(DifficultyError::MissingAncestor { height })
        };
        let (a, b) = (at(first)?, at(last)?);
        let difference = b.as_u256().wrapping_sub(*a.as_u256());
        Ok(ChainWork::from_be_bytes(difference.to_be_bytes()))
    }
}

/// `GetNextWorkRequired(pindexPrev, pblock)` (src/pow.cpp:95-117) for a
/// header with time `new_time` whose parent is at `prev_height` in `w`:
/// `bsv_rs::block::next_work_required`. The genesis case (no parent) is the
/// caller's: a header with no parent is the genesis or a checkpoint, never a
/// header whose bits are computed.
pub(crate) fn next_work_required(
    w: &Window,
    prev_height: u32,
    new_time: u32,
    params: &ChainParams,
) -> Result<u32, HeaderFault> {
    bsv_rs::block::next_work_required(&params.pow, w, prev_height, new_time)
        .map_err(difficulty_fault)
}

/// The node's context checks for a header whose parent is at `prev_height`
/// in `w`, in the node's order (`AcceptBlockHeader`, src/validation.cpp:6179-6190):
/// the checkpoint, then the bits.
pub(crate) fn check_context(
    w: &Window,
    header: &crate::types::BlockHeader,
    params: &ChainParams,
) -> Result<(), HeaderFault> {
    check_checkpoint(header.height, &header.hash, params)?;
    let expected = next_work_required(w, header.height - 1, header.time, params)?;
    if header.bits != expected {
        return Err(HeaderFault::BadDiffBits {
            expected,
            got: header.bits,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cmp::Ordering;

    fn hex(s: &str) -> U256 {
        U256::from_hex(s).unwrap()
    }

    // The node's names for the bsv-rs calls the difficulty vectors below read
    // (`SetCompact`, `GetCompact`, `base_uint`'s operators), so each vector
    // reads as the node's test does. The node's tables for these primitives
    // (`setcompact_test`, `bignum_SetCompact`, `GetBlockProof`, the arithmetic)
    // tested the port this module carried until 0.4.0; they run in bsv-rs
    // 0.6.0's suite now (`tests/chain_primitives.rs`).
    struct Compact {
        value: U256,
    }
    fn set_compact(bits: u32) -> Compact {
        Compact {
            value: CompactTarget::from_bits(bits).target,
        }
    }
    fn get_compact(a: &U256) -> u32 {
        a.to_compact()
    }
    fn shr(a: &U256, shift: u32) -> U256 {
        a.shr(shift)
    }
    fn add(a: &U256, b: &U256) -> U256 {
        a.wrapping_add(*b)
    }
    fn sub(a: &U256, b: &U256) -> U256 {
        a.wrapping_sub(*b)
    }
    fn cmp(a: &U256, b: &U256) -> Ordering {
        a.cmp(b)
    }

    #[test]
    fn the_pow_limits_are_the_node_values() {
        assert_eq!(
            ChainParams::main().pow.pow_limit.to_hex(),
            "00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
        assert_eq!(
            ChainParams::test().pow.pow_limit,
            ChainParams::main().pow.pow_limit
        );
        assert_eq!(
            ChainParams::regtest().pow.pow_limit.to_hex(),
            "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
        // GetCompact of the limits: the bits a min-difficulty header carries.
        assert_eq!(ChainParams::main().pow.pow_limit_bits(), 0x1d00ffff);
        assert_eq!(ChainParams::regtest().pow.pow_limit_bits(), 0x207fffff);
    }

    #[test]
    fn check_proof_of_work_refuses_each_range_the_node_refuses() {
        let main = ChainParams::main();
        let any = hex("1").to_le_bytes();
        assert_eq!(
            check_proof_of_work(&any, 0x1d80ffff, &main),
            Err(HeaderFault::BitsNegative { bits: 0x1d80ffff })
        );
        assert_eq!(
            check_proof_of_work(&any, 0x407fffff, &main),
            Err(HeaderFault::BitsOverflow { bits: 0x407fffff })
        );
        assert_eq!(
            check_proof_of_work(&any, 0x21123456, &main),
            Err(HeaderFault::BitsOverflow { bits: 0x21123456 })
        );
        assert_eq!(
            check_proof_of_work(&any, 0x1d000000, &main),
            Err(HeaderFault::BitsZero { bits: 0x1d000000 })
        );
        // 0x1d010000 decodes to 0x01 << 224, above mainnet's 0xffffffff << 192 limit.
        assert_eq!(
            check_proof_of_work(&any, 0x1d010000, &main),
            Err(HeaderFault::BitsAboveLimit { bits: 0x1d010000 })
        );
        // regtest's limit admits it.
        assert_eq!(
            check_proof_of_work(&any, 0x1d010000, &ChainParams::regtest()),
            Ok(())
        );
        // the hash exactly at the target passes; one above fails.
        let target = set_compact(0x1d00ffff).value;
        assert_eq!(
            check_proof_of_work(&target.to_le_bytes(), 0x1d00ffff, &main),
            Ok(())
        );
        assert_eq!(
            check_proof_of_work(&add(&target, &U256::ONE).to_le_bytes(), 0x1d00ffff, &main),
            Err(HeaderFault::HighHash { bits: 0x1d00ffff })
        );
        for bits in [0x1d80ffff, 0x407fffff, 0x21123456, 0x1d000000, 0x1d010000] {
            assert_eq!(check_bits(bits, &main).unwrap_err().reason(), "high-hash");
        }
        assert_eq!(check_bits(0x1d00ffff, &main), Ok(()));
    }

    // ─── The node's own vectors for the difficulty rule ─────────────────────
    //
    // bitcoin-sv v1.2.3 src/test/pow_tests.cpp, ported test for test: the
    // node builds block indexes with times and bits and no proof of work
    // (`GetBlockIndex`, :125-138), and so do these (a `Window` of links).
    // The rule is bsv-rs's since 0.5.0; the tables pin it through the
    // service's `Window`, the ancestry the store hands it.

    fn link(height: u32, time: u32, bits: u32) -> Link {
        Link { height, time, bits }
    }

    /// `get_next_work` (pow_tests.cpp:30-43), `get_next_work_pow_limit`
    /// (:46-59), `get_next_work_lower_limit_actual` (:62-75),
    /// `get_next_work_upper_limit_actual` (:78-91).
    #[test]
    fn calculate_next_work_required_matches_the_node_vectors() {
        let main = ChainParams::main();
        let rows: &[(u32, u32, u32, i64, u32)] = &[
            (32255, 1262152739, 0x1d00ffff, 1261130161, 0x1d00d86a),
            (2015, 1233061996, 0x1d00ffff, 1231006505, 0x1d00ffff),
            (68543, 1279297671, 0x1c05a3f4, 1279008237, 0x1c0168fd),
            (46367, 1269211443, 0x1c387f6f, 1263163443, 0x1d00e1fd),
        ];
        for &(height, time, bits, first_time, expected) in rows {
            assert_eq!(
                bsv_rs::block::calculate_next_work_required(
                    &main.pow,
                    link(height, time, bits),
                    first_time
                ),
                expected,
                "block {height}"
            );
        }
    }

    /// A window built the way the node's tests build their chains: a first
    /// block at `time0`, then each block `interval` seconds after the last.
    struct Chain {
        w: Window,
        next: u32,
        time: u32,
    }

    impl Chain {
        fn new(time0: u32, bits: u32) -> Self {
            Chain {
                w: Window::new(vec![link(0, time0, bits)]),
                next: 1,
                time: time0,
            }
        }
        fn add(&mut self, interval: i64, bits: u32) {
            self.time = (self.time as i64 + interval) as u32;
            self.w.push(link(self.next, self.time, bits));
            self.next += 1;
        }
        fn tip(&self) -> u32 {
            self.next - 1
        }
        /// `GetNextWorkRequired(blocks.Tip(), &blkHeaderDummy)`: the dummy
        /// header's time is 0 (only the min-difficulty rule reads it).
        fn next_work(&self, params: &ChainParams) -> u32 {
            next_work_required(&self.w, self.tip(), 0, params).unwrap()
        }
        fn next_cash_work(&self, params: &ChainParams) -> u32 {
            bsv_rs::block::next_cash_work_required(&params.pow, &self.w, self.tip(), 0).unwrap()
        }
    }

    /// `retargeting_test` (pow_tests.cpp:140-249): the emergency adjustment
    /// on mainnet's parameters, below the DAA height.
    #[test]
    fn the_emergency_adjustment_matches_retargeting_test() {
        let main = ChainParams::main();
        let pow_limit = main.pow.pow_limit;
        let mut current = shr(&pow_limit, 1);
        let initial_bits = get_compact(&current);
        let mut c = Chain::new(1269211443, initial_bits);
        for _ in 1..100 {
            c.add(main.pow.pow_target_spacing, initial_bits);
        }
        for _ in 100..110 {
            c.add(2 * 3600, initial_bits);
            assert_eq!(c.next_work(&main), initial_bits);
        }
        let step = |current: &mut U256| {
            *current = set_compact(get_compact(current)).value;
            *current = add(current, &shr(current, 2));
        };
        c.add(2 * 3600, initial_bits);
        step(&mut current);
        assert_eq!(c.next_work(&main), get_compact(&current));
        c.add(2 * 3600, get_compact(&current));
        step(&mut current);
        assert_eq!(c.next_work(&main), get_compact(&current));
        c.add(2 * 3600, get_compact(&current));
        step(&mut current);
        assert_eq!(c.next_work(&main), get_compact(&current));
        c.add(2 * 3600, get_compact(&current));
        step(&mut current);
        assert_ne!(get_compact(&pow_limit), get_compact(&current));
        assert_eq!(c.next_work(&main), get_compact(&pow_limit));
        c.add(2 * 3600, get_compact(&current));
        assert_ne!(get_compact(&pow_limit), get_compact(&current));
        assert_eq!(c.next_work(&main), get_compact(&pow_limit));
    }

    /// `cash_difficulty_test` (pow_tests.cpp:251-496): the DAA, every value
    /// and every bound the node checks.
    #[test]
    fn the_daa_matches_cash_difficulty_test() {
        let main = ChainParams::main();
        let pow_limit = main.pow.pow_limit;
        let pow_limit_bits = get_compact(&pow_limit);
        let initial_bits = get_compact(&shr(&pow_limit, 4));
        let mut c = Chain::new(1269211443, initial_bits);
        for _ in 1..2050 {
            c.add(600, initial_bits);
        }
        let mut bits = c.next_cash_work(&main);
        for _ in 0..10 {
            c.add(600, bits);
            assert_eq!(c.next_cash_work(&main), bits);
        }
        c.add(6000, bits);
        assert_eq!(c.next_cash_work(&main), bits);
        c.add(2 * 600 - 6000, bits);
        assert_eq!(c.next_cash_work(&main), bits);
        for _ in 0..20 {
            c.add(600, bits);
            assert_eq!(c.next_cash_work(&main), bits);
        }
        c.add(550, bits);
        assert_eq!(c.next_cash_work(&main), bits);
        let target = |b: u32| set_compact(b).value;
        for _ in 0..10 {
            c.add(550, bits);
            let next = c.next_cash_work(&main);
            let (cur, nxt) = (target(bits), target(next));
            assert_eq!(cmp(&nxt, &cur), Ordering::Less);
            assert_eq!(cmp(&sub(&cur, &nxt), &shr(&cur, 10)), Ordering::Less);
            bits = next;
        }
        assert_eq!(bits, 0x1c0fe7b1);
        for _ in 0..20 {
            c.add(10, bits);
            let next = c.next_cash_work(&main);
            let (cur, nxt) = (target(bits), target(next));
            assert_eq!(cmp(&nxt, &cur), Ordering::Less);
            assert_eq!(cmp(&sub(&cur, &nxt), &shr(&cur, 4)), Ordering::Less);
            bits = next;
        }
        assert_eq!(bits, 0x1c0db19f);
        c.add(6000, bits);
        bits = c.next_cash_work(&main);
        assert_eq!(bits, 0x1c0d9222);
        for _ in 0..93 {
            c.add(6000, bits);
            let next = c.next_cash_work(&main);
            let (cur, nxt) = (target(bits), target(next));
            assert_ne!(cmp(&nxt, &pow_limit), Ordering::Greater);
            assert_eq!(cmp(&nxt, &cur), Ordering::Greater);
            assert_eq!(cmp(&sub(&nxt, &cur), &shr(&cur, 3)), Ordering::Less);
            bits = next;
        }
        assert_eq!(bits, 0x1c2f13b9);
        c.add(6000, bits);
        bits = c.next_cash_work(&main);
        assert_eq!(bits, 0x1c2ee9bf);
        for _ in 0..192 {
            c.add(6000, bits);
            let next = c.next_cash_work(&main);
            let (cur, nxt) = (target(bits), target(next));
            assert_ne!(cmp(&nxt, &pow_limit), Ordering::Greater);
            assert_eq!(cmp(&nxt, &cur), Ordering::Greater);
            assert_eq!(cmp(&sub(&nxt, &cur), &shr(&cur, 3)), Ordering::Less);
            bits = next;
        }
        assert_eq!(bits, 0x1d00ffff);
        for _ in 0..5 {
            c.add(6000, bits);
            let next = c.next_cash_work(&main);
            assert_eq!(next, pow_limit_bits);
            bits = next;
        }
    }

    /// Regtest never retargets (pow.cpp:106-109); testnet's 20-minute rule
    /// answers the limit (pow.cpp:44-51, 264-271).
    #[test]
    fn regtest_keeps_the_parents_bits_and_testnet_grants_the_limit_after_20_minutes() {
        let regtest = ChainParams::regtest();
        let w = Window::new(vec![link(7, 1000, 0x2000ffff)]);
        assert_eq!(
            next_work_required(&w, 7, 1_000_000, &regtest),
            Ok(0x2000ffff)
        );
        let test = ChainParams::test();
        let w = Window::new(vec![link(1_600_000, 1000, 0x1a33c833)]);
        assert_eq!(
            next_work_required(&w, 1_600_000, 1000 + 1201, &test),
            Ok(0x1d00ffff)
        );
        // 20 minutes exactly is not more than 20 minutes: the DAA answers,
        // and it needs the 147 headers this window does not hold.
        assert_eq!(
            next_work_required(&w, 1_600_000, 1000 + 1200, &test),
            Err(HeaderFault::AncestryMissing { height: 1_599_998 })
        );
    }

    #[test]
    fn the_checkpoint_lists_are_the_node_lists_and_the_owner_adds_to_them() {
        let main = ChainParams::main();
        assert_eq!(main.checkpoints.len(), 16);
        assert_eq!(
            main.checkpoint_at(530359),
            Some("0000000000000000011ada8bd08f46074f44a8f155396f43e38acf9501c49103")
        );
        assert_eq!(ChainParams::test().checkpoints.len(), 3);
        assert_eq!(
            ChainParams::regtest().checkpoint_at(0),
            Some(ChainParams::regtest().genesis_hash.as_str())
        );
        let hash = "00000000000000000a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f6071";
        let p = ChainParams::main()
            .with_checkpoints(&format!(" 965000:{} ,530359:{hash}", hash.to_uppercase()))
            .unwrap();
        assert_eq!(p.checkpoints.len(), 17, "one added, one replaced");
        assert_eq!(p.checkpoint_at(965000), Some(hash));
        assert_eq!(p.checkpoint_at(530359), Some(hash));
        assert!(
            p.checkpoints.windows(2).all(|w| w[0].0 < w[1].0),
            "ascending"
        );
        for bad in [
            "965000",
            "x:00",
            "965000:abc",
            "965000:zz00000000000000000000000000000000000000000000000000000000000000",
        ] {
            assert!(ChainParams::main().with_checkpoints(bad).is_err(), "{bad}");
        }
        assert_eq!(
            check_checkpoint(530359, "00", &ChainParams::main()),
            Err(HeaderFault::CheckpointMismatch {
                height: 530359,
                expected: "0000000000000000011ada8bd08f46074f44a8f155396f43e38acf9501c49103".into()
            })
        );
        assert_eq!(check_checkpoint(530360, "00", &ChainParams::main()), Ok(()));
    }

    #[test]
    fn the_window_depth_covers_what_each_regime_reads() {
        assert_eq!(ChainParams::regtest().pow.window_depth(965_000), 1);
        assert_eq!(ChainParams::main().pow.window_depth(886_000), 147);
        assert_eq!(ChainParams::main().pow.window_depth(478_000), 17);
        assert_eq!(
            ChainParams::main().pow.window_depth(479_807),
            2016,
            "479808 is a boundary"
        );
        assert_eq!(
            ChainParams::test().pow.window_depth(1_000),
            1001,
            "the walk back, never past genesis"
        );
    }
}
