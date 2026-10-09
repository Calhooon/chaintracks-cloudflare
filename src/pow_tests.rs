//! P0-4 step 1 (bsv-stack-lean #35, 2026-10-08): the compact `bits` decode and
//! the proof of work on every path a header enters the store, RED-first.
//!
//! The node's rule, cited through the sibling's pin (bsv-script-lean
//! docs/PROVENANCE.md §1, bitcoin-sv v1.2.3 = 6504a3af; the sibling's issue
//! #63 owns it): `CheckProofOfWork` (src/pow.cpp:150-171) refuses a negative,
//! zero or overflowing compact target and a target above the chain's
//! `powLimit`, then a hash above the target; `arith_uint256::SetCompact`
//! (src/arith_uint256.cpp:182-201) sets the negative and overflow flags. The
//! base decoded an exponent past 256 bits as the MAXIMUM target ("treat as
//! max target", types.rs:280-285 at f86752b), so a header carrying such bits
//! met its target with any hash.
use crate::consensus::{ChainParams, HeaderFault};
use crate::host_harness::{flags, header, regtest, RecordedWebhooks, ScriptedChain, SqliteDb};
use crate::storage::{find_chain_tip, find_header_for_hash, insert_header, insert_headers_batch};
use crate::sync::{insert_with_parent_backfill, notify_if_tip_advanced, run_cron};
use crate::types::BlockHeader;

const BETA_HOOK: &str =
    "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed";
const BEARER: &str = "tip-webhook-bearer";
const H: u32 = 965_900;

/// Mainnet genesis, the node's own `CreateGenesisBlock` header.
fn genesis() -> BlockHeader {
    let raw = hex::decode("0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c").unwrap();
    BlockHeader::from_bytes(&raw, 0).unwrap()
}

/// Mainnet 965900 (WoC and Bitails, probed 2026-09-08; the courier tests' vector).
fn mainnet_965900() -> BlockHeader {
    let raw = hex::decode("0000072938e95d6a6237317b6a920cb75bb617ac25f43a9487fd2b140000000000000000bc2c282b23ad996ccf001d93f614aaea6738ed8cb38bb80741e7a9e013b314614d62a06a196a27188eac2e26").unwrap();
    BlockHeader::from_bytes(&raw, 965_900).unwrap()
}

/// A header whose `bits` are replaced, its claimed hash kept: the decode is
/// what is under test, so the hash stays one a real header carries.
fn with_bits(h: &BlockHeader, bits: u32) -> BlockHeader {
    let mut out = h.clone();
    out.bits = bits;
    out
}

/// The scenario's header: exponent 0x40 (a shift of 488 bits), any hash.
const EXPONENT_0X40: u32 = 0x407f_ffff;

// ─── The decode ─────────────────────────────────────────────────────────────

#[test]
fn an_exponent_past_256_bits_is_refused_never_the_maximum_target() {
    let main = ChainParams::main();
    let mut h = with_bits(&mainnet_965900(), EXPONENT_0X40);
    h.hash = "f".repeat(64);
    assert_eq!(
        h.check_pow(&main),
        Err(HeaderFault::BitsOverflow {
            bits: EXPONENT_0X40
        }),
        "exponent 0x40 with the hash ff..ff passed"
    );
    // SetCompact's own overflow vector (arith_uint256_tests.cpp:700-702).
    let h = with_bits(&mainnet_965900(), 0xff12_3456);
    assert_eq!(
        h.check_pow(&main),
        Err(HeaderFault::BitsOverflow { bits: 0xff12_3456 }),
        "0xff123456 overflows; it passed"
    );
}

#[test]
fn negative_compact_bits_are_refused() {
    // 0x1d80ffff: the sign bit 0x00800000 set with a non-zero word
    // (arith_uint256.cpp:195); the base masked it away and decoded genesis's
    // own target, so genesis's hash passed.
    let h = with_bits(&genesis(), 0x1d80_ffff);
    assert_eq!(
        h.check_pow(&ChainParams::main()),
        Err(HeaderFault::BitsNegative { bits: 0x1d80_ffff }),
        "negative bits 0x1d80ffff passed"
    );
}

#[test]
fn overflowing_compact_bits_are_refused() {
    // 0x21123456: size 33 with a word above 0xffff (arith_uint256.cpp:198,
    // the setcompact_test row 0x21123456 overflow=true). The base shifted it
    // to 0x3456 << 240, far above any real hash.
    let h = with_bits(&genesis(), 0x2112_3456);
    assert_eq!(
        h.check_pow(&ChainParams::main()),
        Err(HeaderFault::BitsOverflow { bits: 0x2112_3456 }),
        "overflowing bits 0x21123456 passed"
    );
}

#[test]
fn real_mainnet_headers_pass() {
    let main = ChainParams::main();
    assert_eq!(genesis().check_pow(&main), Ok(()), "genesis");
    assert_eq!(mainnet_965900().check_pow(&main), Ok(()), "965900");
}

// ─── Every path a header enters the store ───────────────────────────────────

struct Seeded {
    a: BlockHeader,
    bad: BlockHeader,
}

/// X, Y, A stored and announced; `bad` is the next block on A with exponent
/// 0x40 in its bits.
async fn seeded(db: &SqliteDb, hooks: &RecordedWebhooks) -> Seeded {
    let x = header(H - 2, "x", &"1".repeat(64));
    let y = header(H - 1, "y", &x.hash);
    let a = header(H, "a", &y.hash);
    for h in [&x, &y, &a] {
        insert_header(db, regtest(), h).await.unwrap();
    }
    notify_if_tip_advanced(db, hooks).await.unwrap();
    hooks.take();
    let bad = with_bits(
        &header(H + 1, "b with exponent 0x40", &a.hash),
        EXPONENT_0X40,
    );
    Seeded { a, bad }
}

async fn stored(db: &SqliteDb, hash: &str) -> bool {
    find_header_for_hash(db, hash).await.unwrap().is_some()
}

/// The last poll fault the cron recorded (migration 0007), read raw.
fn last_fault(db: &SqliteDb) -> Option<String> {
    db.conn()
        .query_row("SELECT last_error FROM sync_state WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap()
}

/// The live path: the cron through the courier ladder. The witness of the gap:
/// accepted and the tip advanced.
#[tokio::test]
async fn the_live_path_refuses_exponent_0x40_and_the_tip_stays() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let s = seeded(&db, &hooks).await;
    let woc = ScriptedChain::new();
    woc.publish(&s.bad);
    woc.tip(H + 1, &s.bad.hash);
    let ladder = crate::couriers::CourierLadder::new(vec![("woc", woc)], 0, regtest().clone());
    run_cron(&db, regtest(), &ladder, None, &hooks)
        .await
        .unwrap();
    assert!(
        !stored(&db, &s.bad.hash).await,
        "the header entered the store"
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, s.a.hash);
    assert_eq!(flags(&db, &s.a.hash), (true, true));
    let fault = last_fault(&db).expect("the refusal is recorded");
    assert!(
        fault.contains("high-hash: bits 0x407fffff overflow"),
        "{fault}"
    );
}

/// The catch-up path (a gap above 10, no upstream): the courier fallback
/// inserts one by one; a refusal is recorded, ends the span, and the cron's
/// tail still runs (the announce of what did land).
#[tokio::test]
async fn the_catch_up_path_refuses_exponent_0x40_records_it_and_runs_the_tail() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let s = seeded(&db, &hooks).await;
    let woc = ScriptedChain::new();
    let good = header(H + 1, "b: the honest next block", &s.a.hash);
    woc.publish(&good);
    let bad = with_bits(
        &header(H + 2, "c with exponent 0x40", &good.hash),
        EXPONENT_0X40,
    );
    woc.publish(&bad);
    woc.tip(H + 12, &bad.hash);
    run_cron(&db, regtest(), &woc, None, &hooks).await.unwrap();
    assert!(
        stored(&db, &good.hash).await,
        "the honest block before it landed"
    );
    assert!(
        !stored(&db, &bad.hash).await,
        "the header entered the store"
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, good.hash);
    let fault = last_fault(&db).expect("the refusal is recorded");
    assert!(
        fault.contains("ingest at 965902") && fault.contains("overflow"),
        "{fault}"
    );
    assert_eq!(hooks.take().len(), 1, "the tail announced the honest block");
}

/// A header whose claimed hash is not the hash of its fields is refused: the
/// store keys every row by the claimed hash, so a proof of work checked on
/// any other hash proves nothing about the row.
#[tokio::test]
async fn a_claimed_hash_that_is_not_the_hash_of_the_fields_is_refused() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let s = seeded(&db, &hooks).await;
    let mut liar = header(H + 1, "b", &s.a.hash);
    liar.hash = format!("{}{}", "0".repeat(32), "1".repeat(32));
    let e = insert_header(&db, regtest(), &liar).await.unwrap_err();
    assert!(format!("{e}").contains("bad-hash"), "{e}");
    assert!(!stored(&db, &liar.hash).await);
}

/// The read-through path (`routes::ensure_fresh_header`) hands a courier's
/// header straight to `insert_with_parent_backfill`, no ladder in between.
#[tokio::test]
async fn the_read_through_path_refuses_exponent_0x40() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let s = seeded(&db, &hooks).await;
    let woc = ScriptedChain::new();
    let r = insert_with_parent_backfill(&db, regtest(), &woc, s.bad.clone()).await;
    assert!(r.is_err(), "accepted: {r:?}");
    let e = format!("{}", r.unwrap_err());
    assert!(e.contains("high-hash: bits 0x407fffff overflow"), "{e}");
    assert!(
        !stored(&db, &s.bad.hash).await,
        "the header entered the store"
    );
    assert_eq!(find_chain_tip(&db).await.unwrap().unwrap().hash, s.a.hash);
}

/// The bulk paths (the upstream catch-up, `/admin/ingest`, `/admin/backfill`,
/// `/admin/bulk-sync`) all write through `insert_headers_batch`.
#[tokio::test]
async fn the_bulk_path_refuses_exponent_0x40_and_stores_nothing_of_the_batch() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let s = seeded(&db, &hooks).await;
    let good = header(H + 1, "b: the honest next block", &s.a.hash);
    let r = insert_headers_batch(&db, regtest(), &[good.clone(), s.bad.clone()]).await;
    assert!(r.is_err(), "accepted: {r:?}");
    let e = format!("{}", r.unwrap_err());
    assert!(e.contains("high-hash: bits 0x407fffff overflow"), "{e}");
    assert!(
        !stored(&db, &s.bad.hash).await,
        "the header entered the store"
    );
    assert!(!stored(&db, &good.hash).await, "the batch is refused whole");
}
