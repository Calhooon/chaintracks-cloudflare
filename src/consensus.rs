//! The node's header rules, ported (P0-4, bsv-stack-lean #35, 2026-10-08).
//!
//! Every rule here is the node's, read at the sibling's pin: bitcoin-sv
//! v1.2.3 = `6504a3aff65ba97c0f6c80962b033e35ecbfed4b` (bsv-script-lean
//! docs/PROVENANCE.md §1; the sibling's issue #63 owns header validity and
//! proof of work). Nothing here is re-derived: each function names the node
//! function it ports and the lines it was read from with
//! `git show v1.2.3:<path>`.
//!
//! - `set_compact`: `arith_uint256::SetCompact`, src/arith_uint256.cpp:182-201.
//! - `get_compact`: `arith_uint256::GetCompact`, src/arith_uint256.cpp:203-225.
//! - `check_proof_of_work`: `CheckProofOfWork`, src/pow.cpp:150-171.
//! - `block_proof`: `GetBlockProof`, src/block_index.cpp:114-127.
//! - the `powLimit` of each chain: src/chainparams.cpp:1047-1048 (main),
//!   :1343-1344 (testnet), :1471-1472 (regtest).
//! - `next_work_required`: `GetNextWorkRequired` with its three regimes,
//!   src/pow.cpp:95-117 (regtest's no-retarget, the legacy retarget with the
//!   emergency adjustment and testnet's minimum-difficulty rule at 22-93 and
//!   119-148, the DAA at 177-297), reading block times, `GetMedianTimePast`
//!   (src/block_index.h:720-737) and the chain work `SetChainWork` sums
//!   (src/block_index.h:661-666).
//! - `check_checkpoint`: `Checkpoints::CheckBlock`, src/checkpoints.cpp:14-22, and
//!   the checkpoint lists of src/chainparams.cpp:1124-1160 (main), :1419-1428
//!   (testnet), :1530-1533 (regtest).

use std::cmp::Ordering;
use std::fmt;

// ─── 256-bit arithmetic, as `base_uint<256>` computes it ────────────────────
//
// Little-endian `[u64; 4]` limbs. Every operation is modulo 2^256, as the
// node's `base_uint` is (its `<<=` drops the bits shifted past the top,
// arith_uint256.cpp:20-33, which is what makes an overflowing compact decode
// to a truncated value rather than a large one).

pub(crate) type U256 = [u64; 4];

pub(crate) const ZERO: U256 = [0; 4];
pub(crate) const ONE: U256 = [1, 0, 0, 0];

pub(crate) fn is_zero(a: &U256) -> bool {
    a.iter().all(|&x| x == 0)
}

pub(crate) fn cmp(a: &U256, b: &U256) -> Ordering {
    for i in (0..4).rev() {
        match a[i].cmp(&b[i]) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }
    Ordering::Equal
}

pub(crate) fn not(a: &U256) -> U256 {
    [!a[0], !a[1], !a[2], !a[3]]
}

pub(crate) fn add(a: &U256, b: &U256) -> U256 {
    let mut out = [0u64; 4];
    let mut carry = 0u64;
    for i in 0..4 {
        let (s1, c1) = a[i].overflowing_add(b[i]);
        let (s2, c2) = s1.overflowing_add(carry);
        out[i] = s2;
        carry = (c1 as u64) + (c2 as u64);
    }
    out
}

pub(crate) fn sub(a: &U256, b: &U256) -> U256 {
    let mut out = [0u64; 4];
    let mut borrow = 0u64;
    for i in 0..4 {
        let (d1, b1) = a[i].overflowing_sub(b[i]);
        let (d2, b2) = d1.overflowing_sub(borrow);
        out[i] = d2;
        borrow = (b1 as u64) + (b2 as u64);
    }
    out
}

/// `-a`, two's complement (`base_uint::operator-()`, arith_uint256.h:78).
pub(crate) fn neg(a: &U256) -> U256 {
    add(&not(a), &ONE)
}

pub(crate) fn shl(a: &U256, shift: u32) -> U256 {
    if shift >= 256 {
        return ZERO;
    }
    let limbs = (shift / 64) as usize;
    let bits = shift % 64;
    let mut out = [0u64; 4];
    for i in (limbs..4).rev() {
        out[i] = a[i - limbs] << bits;
        if bits > 0 && i > limbs {
            out[i] |= a[i - limbs - 1] >> (64 - bits);
        }
    }
    out
}

pub(crate) fn shr(a: &U256, shift: u32) -> U256 {
    if shift >= 256 {
        return ZERO;
    }
    let limbs = (shift / 64) as usize;
    let bits = shift % 64;
    let mut out = [0u64; 4];
    for i in 0..(4 - limbs) {
        out[i] = a[i + limbs] >> bits;
        if bits > 0 && i + limbs + 1 < 4 {
            out[i] |= a[i + limbs + 1] << (64 - bits);
        }
    }
    out
}

/// `a * b` modulo 2^256 (`base_uint::operator*=(uint32_t)`,
/// arith_uint256.cpp:52-60: the carry out of the top word is dropped). The
/// node's retarget multiplies by an `int64_t` timespan, which overload
/// resolution converts to `uint32_t` (a standard conversion beats the
/// `base_uint(uint64_t)` constructor); every timespan it multiplies by is
/// clamped positive and far below 2^32, so the two agree.
pub(crate) fn mul_u32(a: &U256, b: u32) -> U256 {
    let mut out = [0u64; 4];
    let mut carry = 0u128;
    for i in 0..4 {
        let n = (a[i] as u128) * (b as u128) + carry;
        out[i] = n as u64;
        carry = n >> 64;
    }
    out
}

/// floor(n / d) (`base_uint::operator/=`, arith_uint256.cpp:78-105). The
/// node throws on a zero divisor; every caller here divides by a value it
/// has shown non-zero, and a zero divisor answers zero rather than panic.
pub(crate) fn div(n: &U256, d: &U256) -> U256 {
    if is_zero(d) {
        return ZERO;
    }
    let mut quotient = [0u64; 4];
    let mut remainder = [0u64; 4];
    for bit in (0..256).rev() {
        remainder = shl(&remainder, 1);
        remainder[0] |= (n[bit / 64] >> (bit % 64)) & 1;
        if cmp(&remainder, d) != Ordering::Less {
            remainder = sub(&remainder, d);
            quotient[bit / 64] |= 1u64 << (bit % 64);
        }
    }
    quotient
}

/// The position of the highest set bit plus one (`base_uint::bits`,
/// arith_uint256.cpp:152-162).
pub(crate) fn bits(a: &U256) -> u32 {
    for i in (0..4).rev() {
        if a[i] != 0 {
            return 64 * i as u32 + (64 - a[i].leading_zeros());
        }
    }
    0
}

pub(crate) fn from_u64(v: u64) -> U256 {
    [v, 0, 0, 0]
}

pub(crate) fn to_hex(a: &U256) -> String {
    format!("{:016x}{:016x}{:016x}{:016x}", a[3], a[2], a[1], a[0])
}

/// A 64-hex big-endian number (a block hash in display order, a stored
/// chain work). Anything else is `None`.
pub(crate) fn from_hex(s: &str) -> Option<U256> {
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u64; 4];
    for (i, limb) in out.iter_mut().enumerate() {
        let start = 64 - (i + 1) * 16;
        *limb = u64::from_str_radix(&s[start..start + 16], 16).ok()?;
    }
    Some(out)
}

// ─── The compact encoding ───────────────────────────────────────────────────

/// A decoded compact `bits`: the value and the node's two flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Compact {
    pub value: U256,
    pub negative: bool,
    pub overflow: bool,
}

/// `arith_uint256::SetCompact` (src/arith_uint256.cpp:182-201), exactly:
/// the 23-bit word shifted by `8 * (size - 3)` modulo 2^256; negative when
/// the sign bit 0x00800000 is set on a non-zero word; overflow when a
/// non-zero word reaches past 256 bits.
pub(crate) fn set_compact(compact: u32) -> Compact {
    let size = compact >> 24;
    let mut word = compact & 0x007f_ffff;
    let value = if size <= 3 {
        word >>= 8 * (3 - size);
        from_u64(word as u64)
    } else {
        shl(&from_u64(word as u64), 8 * (size - 3))
    };
    let negative = word != 0 && (compact & 0x0080_0000) != 0;
    let overflow =
        word != 0 && (size > 34 || (word > 0xff && size > 33) || (word > 0xffff && size > 32));
    Compact {
        value,
        negative,
        overflow,
    }
}

/// `arith_uint256::GetCompact(fNegative = false)` (src/arith_uint256.cpp:203-225).
pub(crate) fn get_compact(a: &U256) -> u32 {
    let mut size = bits(a).div_ceil(8);
    let mut compact: u32 = if size <= 3 {
        (a[0] << (8 * (3 - size))) as u32
    } else {
        shr(a, 8 * (size - 3))[0] as u32
    };
    if compact & 0x0080_0000 != 0 {
        compact >>= 8;
        size += 1;
    }
    compact | (size << 24)
}

// ─── The chain parameters ───────────────────────────────────────────────────

/// The consensus parameters a header check reads, per chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainParams {
    /// The node's network name (`CBaseChainParams`): "main", "test", "regtest".
    pub network: &'static str,
    /// `consensus.powLimit`: the easiest target a header may carry.
    pub pow_limit: U256,
    /// `consensus.nPowTargetSpacing`, seconds.
    pub pow_target_spacing: i64,
    /// `consensus.nPowTargetTimespan`, seconds.
    pub pow_target_timespan: i64,
    /// `consensus.fPowAllowMinDifficultyBlocks`: testnet's 20-minute rule.
    pub allow_min_difficulty_blocks: bool,
    /// `consensus.fPowNoRetargeting`: regtest's constant difficulty.
    pub no_retargeting: bool,
    /// `consensus.daaHeight`: the DAA governs a header whose PARENT is at or
    /// above this height (src/pow.cpp:111).
    pub daa_height: u32,
    /// `consensus.hashGenesisBlock`, display order.
    pub genesis_hash: String,
    /// `checkpointData`, ascending by height, hashes in display order. The
    /// node's own list is the default; `with_checkpoints` adds the owner's.
    pub checkpoints: Vec<(u32, String)>,
}

/// `uint256S("00000000ffff…ff")`: mainnet's and testnet's `powLimit`.
const POW_LIMIT_MAIN: U256 = [u64::MAX, u64::MAX, u64::MAX, 0x0000_0000_ffff_ffff];
/// `uint256S("7fff…ff")`: regtest's `powLimit`.
const POW_LIMIT_REGTEST: U256 = [u64::MAX, u64::MAX, u64::MAX, 0x7fff_ffff_ffff_ffff];

/// Two weeks, `14 * 24 * 60 * 60` (every chain, src/chainparams.cpp:1050, 1346, 1474).
const TWO_WEEKS: i64 = 14 * 24 * 60 * 60;
/// Ten minutes, `10 * 60` (every chain, src/chainparams.cpp:1051, 1347, 1475).
const TEN_MINUTES: i64 = 10 * 60;

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
    /// `CMainParams` (src/chainparams.cpp:1033-1160): the limits at
    /// 1047-1053, `daaHeight` 504031 at 1068, the genesis hash asserted at
    /// 1097-1099.
    pub fn main() -> Self {
        Self {
            network: "main",
            pow_limit: POW_LIMIT_MAIN,
            pow_target_spacing: TEN_MINUTES,
            pow_target_timespan: TWO_WEEKS,
            allow_min_difficulty_blocks: false,
            no_retargeting: false,
            daa_height: 504_031,
            genesis_hash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
                .to_string(),
            checkpoints: owned(MAIN_CHECKPOINTS),
        }
    }

    /// `CTestNetParams` (src/chainparams.cpp:1329-1428): the limits at
    /// 1343-1349 (the minimum-difficulty rule on), `daaHeight` 1188697 at
    /// 1364, the genesis hash asserted at 1388-1390.
    pub fn test() -> Self {
        Self {
            network: "test",
            pow_limit: POW_LIMIT_MAIN,
            pow_target_spacing: TEN_MINUTES,
            pow_target_timespan: TWO_WEEKS,
            allow_min_difficulty_blocks: true,
            no_retargeting: false,
            daa_height: 1_188_697,
            genesis_hash: "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
                .to_string(),
            checkpoints: owned(TEST_CHECKPOINTS),
        }
    }

    /// `CRegTestParams` (src/chainparams.cpp:1456-1533): the limits at
    /// 1471-1477 (no retargeting), `daaHeight` 0 at 1490, the genesis hash
    /// asserted at 1513-1515. The service never serves regtest; the host
    /// tests mine their fixtures under it, as the node's own regtest does.
    #[allow(dead_code)]
    pub fn regtest() -> Self {
        Self {
            network: "regtest",
            pow_limit: POW_LIMIT_REGTEST,
            pow_target_spacing: TEN_MINUTES,
            pow_target_timespan: TWO_WEEKS,
            allow_min_difficulty_blocks: true,
            no_retargeting: true,
            daa_height: 0,
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
            if from_hex(&hash).is_none() {
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

    /// `DifficultyAdjustmentInterval()` (src/consensus/params.h:47-49): 2016.
    pub fn difficulty_adjustment_interval(&self) -> i64 {
        self.pow_target_timespan / self.pow_target_spacing
    }

    /// `GetCompact(powLimit)`: the bits of a minimum-difficulty header.
    pub fn pow_limit_bits(&self) -> u32 {
        get_compact(&self.pow_limit)
    }

    /// The checkpoint at `height`, if the list has one.
    pub fn checkpoint_at(&self, height: u32) -> Option<&str> {
        self.checkpoints
            .iter()
            .find(|(h, _)| *h == height)
            .map(|(_, hash)| hash.as_str())
    }

    /// How many headers, ending at the parent, `next_work_required` may read
    /// for a header whose parent is at `prev_height`: the parent alone under
    /// regtest's rule; 147 under the DAA (the parent back to the median of
    /// three around `prev - 144`); a full interval for the legacy retarget at
    /// a boundary and for testnet's walk back; 17 for the emergency
    /// adjustment (the median time past at `prev - 6`). Never more than the
    /// chain holds.
    pub fn window_depth(&self, prev_height: u32) -> u32 {
        let depth = if self.no_retargeting {
            1
        } else if prev_height >= self.daa_height {
            147
        } else {
            let interval = self.difficulty_adjustment_interval();
            if self.allow_min_difficulty_blocks || (prev_height as i64 + 1) % interval == 0 {
                interval as u32
            } else {
                17
            }
        };
        depth.min(prev_height + 1)
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
        }
    }
}

// ─── Proof of work ──────────────────────────────────────────────────────────

/// The target `bits` encode, refused exactly where `CheckProofOfWork` refuses
/// it (src/pow.cpp:156-163): negative, zero, overflowing, or above `powLimit`.
/// The node refuses all four alike; the fault names the first that holds, the
/// overflow before the zero (an exponent past 256 bits shifts the word out and
/// decodes to zero BECAUSE it overflowed).
pub(crate) fn checked_target(bits: u32, params: &ChainParams) -> Result<U256, HeaderFault> {
    let c = set_compact(bits);
    if c.negative {
        return Err(HeaderFault::BitsNegative { bits });
    }
    if c.overflow {
        return Err(HeaderFault::BitsOverflow { bits });
    }
    if is_zero(&c.value) {
        return Err(HeaderFault::BitsZero { bits });
    }
    if cmp(&c.value, &params.pow_limit) == Ordering::Greater {
        return Err(HeaderFault::BitsAboveLimit { bits });
    }
    Ok(c.value)
}

/// `CheckProofOfWork(hash, nBits)` (src/pow.cpp:150-171).
pub(crate) fn check_proof_of_work(
    hash: &U256,
    bits: u32,
    params: &ChainParams,
) -> Result<(), HeaderFault> {
    let target = checked_target(bits, params)?;
    if cmp(hash, &target) == Ordering::Greater {
        return Err(HeaderFault::HighHash { bits });
    }
    Ok(())
}

/// `GetBlockProof` (src/block_index.cpp:114-127): `~target / (target + 1) + 1`,
/// and zero for a negative, overflowing or zero target.
pub(crate) fn block_proof(bits: u32) -> U256 {
    let c = set_compact(bits);
    if c.negative || c.overflow || is_zero(&c.value) {
        return ZERO;
    }
    add(&div(&not(&c.value), &add(&c.value, &ONE)), &ONE)
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

/// What the rule reads of a header: its height, time and bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Link {
    pub height: u32,
    pub time: u32,
    pub bits: u32,
}

impl From<&crate::types::BlockHeader> for Link {
    fn from(h: &crate::types::BlockHeader) -> Self {
        Link {
            height: h.height,
            time: h.time,
            bits: h.bits,
        }
    }
}

/// A run of consecutive ancestors, ascending by height, each the parent of
/// the next (the caller has checked the hash links: `storage::load_window`,
/// `storage::insert_headers_batch`). The chain work between two of them is
/// the sum of their `GetBlockProof`, which is what the node's `nChainWork`
/// difference is (`SetChainWork`, src/block_index.h:661-666); the prefix sums
/// are computed once per window.
pub(crate) struct Window {
    links: Vec<Link>,
    /// `work[i]` = the proofs of `links[1..=i]`.
    work: Vec<U256>,
}

impl Window {
    pub fn new(links: Vec<Link>) -> Self {
        let mut work = Vec::with_capacity(links.len());
        let mut sum = ZERO;
        for (i, l) in links.iter().enumerate() {
            if i > 0 {
                sum = add(&sum, &block_proof(l.bits));
            }
            work.push(sum);
        }
        Window { links, work }
    }

    pub fn push(&mut self, link: Link) {
        let sum = match self.work.last() {
            Some(last) => add(last, &block_proof(link.bits)),
            None => ZERO,
        };
        self.links.push(link);
        self.work.push(sum);
    }

    fn index(&self, height: i64) -> Result<usize, HeaderFault> {
        let first = match self.links.first() {
            Some(l) => l.height as i64,
            None => return Err(HeaderFault::AncestryMissing { height }),
        };
        let i = height - first;
        if i < 0 || i as usize >= self.links.len() {
            return Err(HeaderFault::AncestryMissing { height });
        }
        Ok(i as usize)
    }

    pub fn get(&self, height: i64) -> Result<Link, HeaderFault> {
        Ok(self.links[self.index(height)?])
    }

    /// `GetMedianTimePast` (src/block_index.h:722-737): the median of the
    /// times of the block and up to ten before it (fewer only at genesis).
    pub fn median_time_past(&self, height: i64) -> Result<i64, HeaderFault> {
        let mut times = Vec::with_capacity(11);
        for h in (height - 10).max(0)..=height {
            times.push(self.get(h)?.time as i64);
        }
        times.sort_unstable();
        Ok(times[times.len() / 2])
    }

    /// `pindexLast->GetChainWork() - pindexFirst->GetChainWork()`.
    fn work_between(&self, first: i64, last: i64) -> Result<U256, HeaderFault> {
        let a = self.work[self.index(first)?];
        let b = self.work[self.index(last)?];
        Ok(sub(&b, &a))
    }
}

/// `GetNextWorkRequired(pindexPrev, pblock)` (src/pow.cpp:95-117) for a
/// header with time `new_time` whose parent is at `prev_height` in `w`.
/// The genesis case (no parent) is the caller's: a header with no parent is
/// the genesis or a checkpoint, never a header whose bits are computed.
pub(crate) fn next_work_required(
    w: &Window,
    prev_height: u32,
    new_time: u32,
    params: &ChainParams,
) -> Result<u32, HeaderFault> {
    let prev = w.get(prev_height as i64)?;
    if params.no_retargeting {
        return Ok(prev.bits);
    }
    if prev.height >= params.daa_height {
        return next_cash_work_required(w, prev, new_time, params);
    }
    next_eda_work_required(w, prev, new_time, params)
}

/// `GetNextEDAWorkRequired` (src/pow.cpp:22-93): the legacy retarget every
/// interval, testnet's minimum-difficulty rule, and the emergency adjustment.
fn next_eda_work_required(
    w: &Window,
    prev: Link,
    new_time: u32,
    params: &ChainParams,
) -> Result<u32, HeaderFault> {
    let interval = params.difficulty_adjustment_interval();
    let height = prev.height as i64 + 1;
    if height % interval == 0 {
        let first = w.get(height - interval)?;
        return Ok(calculate_next_work_required(
            prev,
            first.time as i64,
            params,
        ));
    }
    let pow_limit_bits = params.pow_limit_bits();
    if params.allow_min_difficulty_blocks {
        if new_time as i64 > prev.time as i64 + 2 * params.pow_target_spacing {
            return Ok(pow_limit_bits);
        }
        let mut index = prev;
        while index.height != 0
            && (index.height as i64) % interval != 0
            && index.bits == pow_limit_bits
        {
            index = w.get(index.height as i64 - 1)?;
        }
        return Ok(index.bits);
    }
    if prev.bits == pow_limit_bits {
        return Ok(pow_limit_bits);
    }
    let six = w.get(height - 7)?;
    let mtp6 = w.median_time_past(prev.height as i64)? - w.median_time_past(six.height as i64)?;
    if mtp6 < 12 * 3600 {
        return Ok(prev.bits);
    }
    let mut pow = set_compact(prev.bits).value;
    pow = add(&pow, &shr(&pow, 2));
    if cmp(&pow, &params.pow_limit) == Ordering::Greater {
        pow = params.pow_limit;
    }
    Ok(get_compact(&pow))
}

/// `CalculateNextWorkRequired` (src/pow.cpp:119-148): the two-week retarget,
/// the timespan clamped to a quarter and four times the target.
pub(crate) fn calculate_next_work_required(
    prev: Link,
    first_block_time: i64,
    params: &ChainParams,
) -> u32 {
    if params.no_retargeting {
        return prev.bits;
    }
    let timespan = params.pow_target_timespan;
    let actual = (prev.time as i64 - first_block_time).clamp(timespan / 4, timespan * 4);
    let mut target = set_compact(prev.bits).value;
    target = mul_u32(&target, actual as u32);
    target = div(&target, &from_u64(timespan as u64));
    if cmp(&target, &params.pow_limit) == Ordering::Greater {
        target = params.pow_limit;
    }
    get_compact(&target)
}

/// `GetSuitableBlock` (src/pow.cpp:214-245): the median by time of the block
/// and its two parents, by the node's own sorting network (its choice among
/// equal times decides which block's chain work is read).
fn suitable_block(w: &Window, height: i64) -> Result<Link, HeaderFault> {
    let mut blocks = [w.get(height - 2)?, w.get(height - 1)?, w.get(height)?];
    if blocks[0].time > blocks[2].time {
        blocks.swap(0, 2);
    }
    if blocks[0].time > blocks[1].time {
        blocks.swap(0, 1);
    }
    if blocks[1].time > blocks[2].time {
        blocks.swap(1, 2);
    }
    Ok(blocks[1])
}

/// `ComputeTarget` (src/pow.cpp:177-208).
fn compute_target(
    w: &Window,
    first: Link,
    last: Link,
    params: &ChainParams,
) -> Result<U256, HeaderFault> {
    let mut work = w.work_between(first.height as i64, last.height as i64)?;
    work = mul_u32(&work, params.pow_target_spacing as u32);
    let spacing = params.pow_target_spacing;
    let actual = (last.time as i64 - first.time as i64).clamp(72 * spacing, 288 * spacing);
    work = div(&work, &from_u64(actual as u64));
    Ok(div(&neg(&work), &work))
}

/// `GetNextCashWorkRequired` (src/pow.cpp:256-297): the DAA over the last
/// 144 blocks.
fn next_cash_work_required(
    w: &Window,
    prev: Link,
    new_time: u32,
    params: &ChainParams,
) -> Result<u32, HeaderFault> {
    if params.allow_min_difficulty_blocks
        && new_time as i64 > prev.time as i64 + 2 * params.pow_target_spacing
    {
        return Ok(params.pow_limit_bits());
    }
    let height = prev.height as i64;
    if height < params.difficulty_adjustment_interval() {
        // The node asserts here (src/pow.cpp:275); a chain this short has no
        // DAA answer, so the header cannot be checked.
        return Err(HeaderFault::AncestryMissing {
            height: height - 146,
        });
    }
    let last = suitable_block(w, height)?;
    let first = suitable_block(w, height - 144)?;
    let target = compute_target(w, first, last, params)?;
    if cmp(&target, &params.pow_limit) == Ordering::Greater {
        return Ok(params.pow_limit_bits());
    }
    Ok(get_compact(&target))
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

    fn hex(s: &str) -> U256 {
        from_hex(&format!("{s:0>64}")).unwrap()
    }

    /// The node's own table, `setcompact_test`
    /// (src/test/arith_uint256_tests.cpp:501-538 at v1.2.3): input, negative,
    /// overflow, value.
    #[test]
    fn set_compact_matches_the_node_table() {
        let rows: &[(u32, bool, bool, &str)] = &[
            (0x00123456, false, false, "0"),
            (0x01123456, false, false, "12"),
            (0x02123456, false, false, "1234"),
            (0x03123456, false, false, "123456"),
            (0x04123456, false, false, "12345600"),
            (
                0x20123456,
                false,
                false,
                "1234560000000000000000000000000000000000000000000000000000000000",
            ),
            (
                0x21123456,
                false,
                true,
                "3456000000000000000000000000000000000000000000000000000000000000",
            ),
            (0x00923456, false, false, "0"),
            (0x01923456, true, false, "12"),
            (0x02923456, true, false, "1234"),
            (0x03923456, true, false, "123456"),
            (0x04923456, true, false, "12345600"),
            (
                0x20923456,
                true,
                false,
                "1234560000000000000000000000000000000000000000000000000000000000",
            ),
            (
                0x21923456,
                true,
                true,
                "3456000000000000000000000000000000000000000000000000000000000000",
            ),
        ];
        for &(input, negative, overflow, value) in rows {
            let c = set_compact(input);
            assert_eq!(
                (c.negative, c.overflow, c.value),
                (negative, overflow, hex(value)),
                "{input:#010x}"
            );
        }
    }

    /// The node's `bignum_SetCompact` (src/test/arith_uint256_tests.cpp:540-703):
    /// value, the round trip through `GetCompact`, and the flags.
    #[test]
    fn set_and_get_compact_match_bignum_set_compact() {
        let rows: &[(u32, &str, u32, bool, bool)] = &[
            (0, "0", 0, false, false),
            (0x00123456, "0", 0, false, false),
            (0x01003456, "0", 0, false, false),
            (0x02000056, "0", 0, false, false),
            (0x03000000, "0", 0, false, false),
            (0x04000000, "0", 0, false, false),
            (0x00923456, "0", 0, false, false),
            (0x01803456, "0", 0, false, false),
            (0x02800056, "0", 0, false, false),
            (0x03800000, "0", 0, false, false),
            (0x04800000, "0", 0, false, false),
            (0x01123456, "12", 0x01120000, false, false),
            (0x02123456, "1234", 0x02123400, false, false),
            (0x03123456, "123456", 0x03123456, false, false),
            (0x04123456, "12345600", 0x04123456, false, false),
            (0x05009234, "92340000", 0x05009234, false, false),
            (
                0x20123456,
                "1234560000000000000000000000000000000000000000000000000000000000",
                0x20123456,
                false,
                false,
            ),
        ];
        for &(input, value, round_trip, negative, overflow) in rows {
            let c = set_compact(input);
            assert_eq!(c.value, hex(value), "{input:#010x} value");
            assert_eq!(
                get_compact(&c.value),
                round_trip,
                "{input:#010x} GetCompact"
            );
            assert_eq!(
                (c.negative, c.overflow),
                (negative, overflow),
                "{input:#010x} flags"
            );
        }
        // The negative rows whose GetCompact(true) keeps the sign: the value
        // and the flags (the sign is the flag; this service never encodes one).
        let c = set_compact(0x01fedcba);
        assert_eq!((c.value, c.negative, c.overflow), (hex("7e"), true, false));
        let c = set_compact(0x04923456);
        assert_eq!(
            (c.value, c.negative, c.overflow),
            (hex("12345600"), true, false)
        );
        // "Make sure that we don't generate compacts with the 0x00800000 bit set".
        assert_eq!(get_compact(&from_u64(0x80)), 0x02008000);
        let c = set_compact(0xff123456);
        assert_eq!((c.negative, c.overflow), (false, true));
    }

    #[test]
    fn the_pow_limits_are_the_node_values() {
        assert_eq!(
            to_hex(&ChainParams::main().pow_limit),
            "00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
        assert_eq!(ChainParams::test().pow_limit, ChainParams::main().pow_limit);
        assert_eq!(
            to_hex(&ChainParams::regtest().pow_limit),
            "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        );
        // GetCompact of the limits: the bits a min-difficulty header carries.
        assert_eq!(get_compact(&ChainParams::main().pow_limit), 0x1d00ffff);
        assert_eq!(get_compact(&ChainParams::regtest().pow_limit), 0x207fffff);
    }

    #[test]
    fn check_proof_of_work_refuses_each_range_the_node_refuses() {
        let main = ChainParams::main();
        let any = hex("1");
        assert_eq!(
            check_proof_of_work(&any, 0x1d80ffff, &main),
            Err(HeaderFault::BitsNegative { bits: 0x1d80ffff })
        );
        assert_eq!(
            check_proof_of_work(&any, 0x407fffff, &main),
            Err(HeaderFault::BitsOverflow { bits: 0x407fffff })
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
        assert_eq!(check_proof_of_work(&target, 0x1d00ffff, &main), Ok(()));
        assert_eq!(
            check_proof_of_work(&add(&target, &ONE), 0x1d00ffff, &main),
            Err(HeaderFault::HighHash { bits: 0x1d00ffff })
        );
    }

    #[test]
    fn block_proof_is_get_block_proof() {
        // genesis bits: 2^256 / (0xffff << 208 + 1) = 0x100010001.
        assert_eq!(block_proof(0x1d00ffff), hex("100010001"));
        for bad in [0x1d80ffff, 0x407fffff, 0x1d000000, 0] {
            assert_eq!(block_proof(bad), ZERO, "{bad:#010x}");
        }
    }

    // ─── The node's own vectors for the difficulty rule ─────────────────────
    //
    // bitcoin-sv v1.2.3 src/test/pow_tests.cpp, ported test for test: the
    // node builds block indexes with times and bits and no proof of work
    // (`GetBlockIndex`, :125-138), and so do these (a `Window` of links).

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
                calculate_next_work_required(link(height, time, bits), first_time, &main),
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
            next_cash_work_required(&self.w, self.w.get(self.tip() as i64).unwrap(), 0, params)
                .unwrap()
        }
    }

    /// `retargeting_test` (pow_tests.cpp:140-249): the emergency adjustment
    /// on mainnet's parameters, below the DAA height.
    #[test]
    fn the_emergency_adjustment_matches_retargeting_test() {
        let main = ChainParams::main();
        let pow_limit = main.pow_limit;
        let mut current = shr(&pow_limit, 1);
        let initial_bits = get_compact(&current);
        let mut c = Chain::new(1269211443, initial_bits);
        for _ in 1..100 {
            c.add(main.pow_target_spacing, initial_bits);
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
        let pow_limit = main.pow_limit;
        let pow_limit_bits = get_compact(&pow_limit);
        let initial_bits = get_compact(&shr(&pow_limit, 4));
        let mut c = Chain::new(1269211443, initial_bits);
        for _ in 1..2050 {
            c.add(600, initial_bits);
        }
        let mut bits = next_cash_work_required(&c.w, c.w.get(2049).unwrap(), 0, &main).unwrap();
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
        assert_eq!(ChainParams::regtest().window_depth(965_000), 1);
        assert_eq!(ChainParams::main().window_depth(886_000), 147);
        assert_eq!(ChainParams::main().window_depth(478_000), 17);
        assert_eq!(
            ChainParams::main().window_depth(479_807),
            2016,
            "479808 is a boundary"
        );
        assert_eq!(
            ChainParams::test().window_depth(1_000),
            1001,
            "the walk back, never past genesis"
        );
    }

    #[test]
    fn the_arithmetic_agrees_with_itself() {
        let a = hex("123456789abcdef0fedcba9876543210");
        assert_eq!(shr(&shl(&a, 77), 77), a);
        assert_eq!(shl(&a, 256), ZERO);
        assert_eq!(div(&mul_u32(&a, 600), &from_u64(600)), a);
        assert_eq!(add(&neg(&a), &a), ZERO);
        assert_eq!(bits(&ONE), 1);
        assert_eq!(bits(&shl(&ONE, 200)), 201);
        assert_eq!(bits(&ZERO), 0);
    }
}
