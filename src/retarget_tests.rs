//! P0-4 step 2 (bsv-stack-lean #35, 2026-10-08): the difficulty rule and the
//! checkpoints on every path a header enters the store, and the stored chain
//! re-validated after the deploy, RED-first.
//!
//! The node's rules, cited through the sibling's pin (bitcoin-sv v1.2.3 =
//! 6504a3af, bsv-script-lean docs/PROVENANCE.md section 1; the sibling's #63
//! owns header validity, #69 the eras of the difficulty rule, #73 the
//! checkpoints): `ContextualCheckBlockHeader` refuses a header whose bits are
//! not `GetNextWorkRequired`'s answer ("bad-diffbits", src/validation.cpp:5943-5948),
//! `CheckIndexAgainstCheckpoint` a header that contradicts a checkpoint
//! (src/validation.cpp:5903-5931). The base checked neither: any bits that met
//! their own target were stored, at any height.
//!
//! Real chain data: Teranode v0.16.0 (the sibling's pin, 4edb60a4) ships
//! testnet headers 1602530-1602710 (`test/testnet_headers_1602530_1602710.bin`,
//! from WhatsOnChain per its README), copied to `src/testdata/`.
use crate::consensus::ChainParams;
use crate::host_harness::{header, header_with_bits, regtest, SqliteDb, HEAVY_BITS};
use crate::storage::{
    check_root_for_height, find_header_for_hash, insert_header, insert_headers_batch,
};
use crate::types::BlockHeader;

const H: u32 = 965_900;

/// The testnet run 1602530..=1602710 from Teranode v0.16.0.
const TESTNET_1602530: &[u8] = include_bytes!("testdata/test_1602530_1602710.bin");

fn run(bytes: &[u8], first: u32) -> Vec<BlockHeader> {
    bytes
        .chunks(80)
        .enumerate()
        .map(|(i, c)| BlockHeader::from_bytes(c, first + i as u32).unwrap())
        .collect()
}

/// A testnet header on top of the real 1602682, mined for this test
/// (2026-10-08, 9 minutes on the Studio, never broadcast): the minimum
/// difficulty 0x1d00ffff, 600 s after its parent. Its proof of work is real;
/// testnet's rule grants the minimum difficulty only to a header more than
/// 20 minutes after its parent (src/pow.cpp:264-271), so its bits are wrong.
const WITNESS_1602683: &str = "00000020d826db8f95129f488573cf1f8fba9b3716c5a9acb5f4a3934b2e00000000000023eb40b70a23238a5a970b93ed6ba28190ac210d8f90215f719feeabc515cb6f06bff865ffff001dd1abae12";

async fn stored(db: &SqliteDb, hash: &str) -> bool {
    find_header_for_hash(db, hash).await.unwrap().is_some()
}

/// Regtest never retargets: every header carries its parent's bits
/// (src/pow.cpp:106-109). A heavier child with real proof of work at other
/// bits is no regtest header. The witness of the gap: stored, and the tip.
#[tokio::test]
async fn a_regtest_header_whose_bits_differ_from_its_parents_is_refused() {
    let db = SqliteDb::migrated();
    let x = header(H - 2, "x", &"1".repeat(64));
    let y = header(H - 1, "y", &x.hash);
    for h in [&x, &y] {
        insert_header(&db, regtest(), h).await.unwrap();
    }
    let heavy = header_with_bits(H, "a heavy child of y", &y.hash, HEAVY_BITS);
    let r = insert_header(&db, regtest(), &heavy).await;
    assert!(r.is_err(), "accepted: {r:?}");
    let e = format!("{}", r.unwrap_err());
    assert!(e.contains("bad-diffbits"), "{e}");
    assert!(
        !stored(&db, &heavy.hash).await,
        "the header entered the store"
    );
}

/// Regtest's one checkpoint is its genesis (src/chainparams.cpp:1530-1533).
/// A header at height 0 that is not it contradicts the checkpoint. The
/// witness of the gap: stored as the root of the chain.
#[tokio::test]
async fn a_header_at_height_0_that_is_not_the_genesis_contradicts_the_checkpoint() {
    let db = SqliteDb::migrated();
    let impostor = header(0, "not the regtest genesis", &"0".repeat(64));
    let r = insert_header(&db, regtest(), &impostor).await;
    assert!(r.is_err(), "accepted: {r:?}");
    let e = format!("{}", r.unwrap_err());
    assert!(e.contains("checkpoint mismatch"), "{e}");
    assert!(
        !stored(&db, &impostor.hash).await,
        "the header entered the store"
    );
}

/// Real testnet: 1602530..=1602682 stored, then a header with real proof of
/// work at the minimum difficulty only 600 s after 1602682. The witness of
/// the gap: stored. The real 1602683 (1208 s after its parent, the minimum
/// difficulty earned) is stored: the control.
#[tokio::test]
async fn a_testnet_header_at_the_minimum_difficulty_without_the_20_minute_wait_is_refused() {
    let db = SqliteDb::migrated();
    let chain = run(TESTNET_1602530, 1_602_530);
    let upto_682 = &chain[..=(1_602_682 - 1_602_530) as usize];
    // The store starts from a checkpoint (the owner's `CHECKPOINTS`): the
    // real 1602682 vouches for the run below it, the window of 1602683.
    let params = ChainParams::test()
        .with_checkpoints(&format!("1602682:{}", upto_682.last().unwrap().hash))
        .unwrap();
    insert_headers_batch(&db, &params, upto_682).await.unwrap();
    crate::storage::update_chain_tip_to_highest(&db)
        .await
        .unwrap();

    let witness =
        BlockHeader::from_bytes(&hex::decode(WITNESS_1602683).unwrap(), 1_602_683).unwrap();
    assert_eq!(witness.bits, 0x1d00ffff);
    assert_eq!(witness.check_pow(&params), Ok(()), "real proof of work");
    let r = insert_header(&db, &params, &witness).await;
    assert!(r.is_err(), "accepted: {r:?}");
    let e = format!("{}", r.unwrap_err());
    assert!(e.contains("bad-diffbits"), "{e}");
    assert!(
        !stored(&db, &witness.hash).await,
        "the header entered the store"
    );

    let real = chain[(1_602_683 - 1_602_530) as usize].clone();
    assert_eq!(real.bits, 0x1d00ffff);
    insert_header(&db, &params, &real).await.unwrap();
    assert!(stored(&db, &real.hash).await, "the real 1602683 is refused");
}

/// Regtest's genesis, the node's `CreateGenesisBlock(1296688602, 2,
/// 0x207fffff, 1, 50 * COIN)` (src/chainparams.cpp:1511-1515).
pub(crate) fn regtest_genesis() -> BlockHeader {
    let h = BlockHeader {
        version: 1,
        previous_hash: "0".repeat(64),
        merkle_root: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string(),
        time: 1_296_688_602,
        bits: 0x207f_ffff,
        nonce: 2,
        height: 0,
        hash: String::new(),
        ..Default::default()
    };
    BlockHeader {
        hash: crate::types::compute_block_hash(&h.to_bytes()),
        ..h
    }
}

/// A store written before the deploy: the migrations up to 0007, the rows
/// inserted as the base inserted them (active, no check), then the
/// migrations that came after, as a deploy applies them. Returns the rows.
pub(crate) async fn legacy_store(rows: &[BlockHeader]) -> SqliteDb {
    let db = SqliteDb::migrated_through("0007");
    for h in rows {
        db.conn()
            .execute(
                "INSERT INTO headers (previous_header_id, previous_hash, height, is_active, \
                 is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
                 VALUES (NULL, ?1, ?2, 1, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![
                    h.previous_hash,
                    h.height,
                    h.hash,
                    crate::types::calculate_work(h.bits),
                    h.version,
                    h.merkle_root,
                    h.time,
                    h.bits,
                    h.nonce
                ],
            )
            .unwrap();
    }
    db.conn()
        .execute(
            "UPDATE headers SET is_chain_tip = 1 WHERE height = (SELECT MAX(height) FROM headers)",
            [],
        )
        .unwrap();
    db.apply_migrations_after("0007");
    db
}

/// The regtest genesis and five blocks; the fourth carries bits its parent
/// does not (real proof of work, wrong bits), the store's legacy.
pub(crate) fn chain_with_bad_bits_at_3() -> Vec<BlockHeader> {
    let g = regtest_genesis();
    let b1 = header(1, "legacy 1", &g.hash);
    let b2 = header(2, "legacy 2", &b1.hash);
    let b3 = header_with_bits(3, "legacy 3, wrong bits", &b2.hash, HEAVY_BITS);
    let b4 = header_with_bits(4, "legacy 4", &b3.hash, HEAVY_BITS);
    let b5 = header_with_bits(5, "legacy 5", &b4.hash, HEAVY_BITS);
    vec![g, b1, b2, b3, b4, b5]
}

/// After the deploy, a row the base stored unchecked is not served as proof
/// until the re-validation from the last checkpoint has passed it. The
/// witness of the gap: the root of the row with wrong bits answers valid.
#[tokio::test]
async fn a_legacy_row_with_wrong_bits_is_not_served_after_the_deploy() {
    let rows = chain_with_bad_bits_at_3();
    let db = legacy_store(&rows).await;
    let served = check_root_for_height(&db, &rows[3].merkle_root, 3)
        .await
        .unwrap();
    assert_eq!(served, None, "served before re-validation");
}

// ─── Real chains pass: the rule replayed on every checked header ────────────
//
// The runs below are Teranode v0.16.0's (the sibling's pin), cut where noted
// (`src/testdata/README.md`). A run is stored with the node's own checkpoint
// or, where the run carries none, one the owner would configure
// (`CHECKPOINTS`): the checkpoint vouches for the rows up to it (their bits
// are not computed: the window they form is what the rows above it are
// checked against), and every row above it is checked by the rule.

const MAIN_886001: &[u8] = include_bytes!("testdata/main_886001_888000.bin");
const MAIN_477792: &[u8] = include_bytes!("testdata/main_477792_479824.bin");
const MAIN_501984: &[u8] = include_bytes!("testdata/main_501984_504031.bin");
const TESTNET_0: &[u8] = include_bytes!("testdata/test_0_547.bin");

/// How many headers above `from` carry other bits than their parent's: the
/// retargets the replay crossed (counted independently from the raw files,
/// 2026-10-08, before these tests ran).
fn bits_changes(chain: &[BlockHeader], from: u32) -> usize {
    chain
        .windows(2)
        .filter(|p| p[1].height > from && p[1].bits != p[0].bits)
        .count()
}

fn anchored(params: ChainParams, at: &BlockHeader) -> ChainParams {
    params
        .with_checkpoints(&format!("{}:{}", at.height, at.hash))
        .unwrap()
}

async fn count_rows(db: &SqliteDb) -> i64 {
    db.conn()
        .query_row("SELECT COUNT(*) FROM headers", [], |r| r.get(0))
        .unwrap()
}

/// Mainnet 886001..=888000 (the DAA: every block retargets). Anchored at
/// 886147, so 886148..=888000 are checked: 1853 headers, 1853 retargets;
/// the batch path to 887900, the single path (the window read back from the
/// store, 147 rows a header) for the last hundred.
#[tokio::test]
async fn real_mainnet_daa_headers_pass_on_the_batch_and_the_single_paths() {
    let chain = run(MAIN_886001, 886_001);
    let params = anchored(ChainParams::main(), &chain[146]);
    assert_eq!(chain[146].height, 886_147);
    let db = SqliteDb::migrated();
    let split = (887_900 - 886_001 + 1) as usize;
    insert_headers_batch(&db, &params, &chain[..split])
        .await
        .unwrap();
    crate::storage::update_chain_tip_to_highest(&db)
        .await
        .unwrap();
    for h in &chain[split..] {
        let r = insert_header(&db, &params, h).await.unwrap();
        assert!(r.added && r.is_active_tip, "{} {r:?}", h.height);
    }
    assert_eq!(count_rows(&db).await, 2000);
    assert_eq!(bits_changes(&chain, 886_147), 1853);
    // and the same run replayed through the rule alone, header by header
    let mut w = crate::consensus::Window::new(
        chain[..147]
            .iter()
            .map(crate::consensus::Link::from)
            .collect(),
    );
    for h in &chain[147..] {
        crate::consensus::check_context(&w, h, &params).unwrap();
        w.push(crate::consensus::Link::from(h));
    }
}

/// Mainnet 477792..=479824: the node's own checkpoint 478558 (the August
/// 2017 fork block) vouches for the run up to it; 478559..=479824 are checked
/// by the legacy rule with the emergency adjustment: 1266 headers, the 10
/// emergency adjustments from 478577, and the two-week retarget at 479808.
#[tokio::test]
async fn real_mainnet_emergency_adjustments_and_a_retarget_pass_under_the_node_checkpoint() {
    let chain = run(MAIN_477792, 477_792);
    let params = ChainParams::main();
    assert_eq!(
        params.checkpoint_at(478_558),
        Some(chain[(478_558 - 477_792) as usize].hash.as_str())
    );
    let db = SqliteDb::migrated();
    insert_headers_batch(&db, &params, &chain).await.unwrap();
    assert_eq!(count_rows(&db).await, chain.len() as i64);
    assert_eq!(bits_changes(&chain, 478_558), 10);
    assert_eq!(
        479_808 % 2016,
        0,
        "a retarget boundary in the checked range"
    );
}

/// Mainnet 501984..=504031: anchored at 502000, so 502001..=504031 are
/// checked: the two-week retarget at 504000 (its window reaches back to
/// 501984) and the node's checkpoint 504031, the DAA activation block.
#[tokio::test]
async fn real_mainnet_retarget_at_504000_and_the_daa_activation_checkpoint_pass() {
    let chain = run(MAIN_501984, 501_984);
    let params = anchored(ChainParams::main(), &chain[16]);
    assert_eq!(chain[16].height, 502_000);
    assert_eq!(
        params.checkpoint_at(504_031),
        Some(chain.last().unwrap().hash.as_str())
    );
    let db = SqliteDb::migrated();
    insert_headers_batch(&db, &params, &chain).await.unwrap();
    assert_eq!(count_rows(&db).await, chain.len() as i64);
    assert_eq!(bits_changes(&chain, 502_000), 1);
}

/// Testnet from its genesis through 547: the genesis anchors the store, the
/// minimum-difficulty rule's walk back runs on every header, and the node's
/// checkpoint 546 binds.
#[tokio::test]
async fn real_testnet_from_genesis_through_the_checkpoint_at_546_passes() {
    let chain = run(TESTNET_0, 0);
    let params = ChainParams::test();
    assert_eq!(chain[0].hash, params.genesis_hash);
    let db = SqliteDb::migrated();
    insert_headers_batch(&db, &params, &chain).await.unwrap();
    assert_eq!(count_rows(&db).await, 548);
}

/// Testnet 1602530..=1602710 anchored at 1602676: 34 headers checked by
/// the DAA, 8 of them late enough for the minimum difficulty, 1602683 among
/// them.
#[tokio::test]
async fn real_testnet_daa_with_the_20_minute_rule_passes() {
    let chain = run(TESTNET_1602530, 1_602_530);
    let params = anchored(ChainParams::test(), &chain[146]);
    let db = SqliteDb::migrated();
    insert_headers_batch(&db, &params, &chain).await.unwrap();
    assert_eq!(count_rows(&db).await, 181);
    let late = chain
        .windows(2)
        .filter(|p| {
            p[1].height > 1_602_676 && p[1].bits == 0x1d00ffff && p[1].time > p[0].time + 1200
        })
        .count();
    assert_eq!(late, 8);
}

// ─── A chain that contradicts a checkpoint is refused ───────────────────────

/// The real mainnet run against a checkpoint list whose 478558 is another
/// hash: refused at 478558, nothing of the batch stored.
#[tokio::test]
async fn a_real_chain_contradicting_a_checkpoint_is_refused_whole() {
    let chain = run(MAIN_477792, 477_792);
    let other = "00000000000000000000000000000000000000000000000000000000000000aa";
    let params = ChainParams::main()
        .with_checkpoints(&format!("478558:{other}"))
        .unwrap();
    let db = SqliteDb::migrated();
    let e = insert_headers_batch(&db, &params, &chain)
        .await
        .unwrap_err();
    let e = format!("{e}");
    assert!(e.contains("checkpoint mismatch: at 478558"), "{e}");
    assert_eq!(count_rows(&db).await, 0);
    // a checkpoint above the vouching one, in the checked range: refused too
    let params = ChainParams::main()
        .with_checkpoints(&format!("479000:{other}"))
        .unwrap();
    let e = insert_headers_batch(&db, &params, &chain)
        .await
        .unwrap_err();
    assert!(
        format!("{e}").contains("checkpoint mismatch: at 479000"),
        "{e}"
    );
    assert_eq!(count_rows(&db).await, 0);
}

/// A store never starts from an unvouched header: on an empty store a
/// header that is neither the genesis nor a checkpoint is refused
/// (`prev-blk-not-found`), on the single path and the batch path. To red:
/// `check_unlinked`, drop the `store_empty` refusal.
#[tokio::test]
async fn an_empty_store_refuses_a_first_header_no_checkpoint_vouches_for() {
    let db = SqliteDb::migrated();
    let stranger = header(H + 7, "no anchor", &"2".repeat(64));
    let e = insert_header(&db, regtest(), &stranger).await.unwrap_err();
    assert!(format!("{e}").contains("prev-blk-not-found"), "{e}");
    let e = insert_headers_batch(&db, regtest(), std::slice::from_ref(&stranger))
        .await
        .unwrap_err();
    assert!(format!("{e}").contains("prev-blk-not-found"), "{e}");
    assert_eq!(count_rows(&db).await, 0);
}

/// `bad-fork-prior-to-checkpoint`: once the store holds a checkpoint, no
/// new header lands below it, on the single path or the batch path.
#[tokio::test]
async fn a_fork_below_a_held_checkpoint_is_refused() {
    let db = SqliteDb::migrated();
    let x = header(H - 2, "x", &"1".repeat(64));
    let y = header(H - 1, "y", &x.hash);
    let a = header(H, "a", &y.hash);
    let params = regtest()
        .clone()
        .with_checkpoints(&format!("{}:{}", a.height, a.hash))
        .unwrap();
    for h in [&x, &y, &a] {
        insert_header(&db, &params, h).await.unwrap();
    }
    let fork = header(H - 1, "a fork below the checkpoint at a", &x.hash);
    let e = insert_header(&db, &params, &fork).await.unwrap_err();
    assert!(
        format!("{e}").contains("bad-fork-prior-to-checkpoint"),
        "{e}"
    );
    let e = insert_headers_batch(&db, &params, std::slice::from_ref(&fork))
        .await
        .unwrap_err();
    assert!(
        format!("{e}").contains("bad-fork-prior-to-checkpoint"),
        "{e}"
    );
    assert!(!stored(&db, &fork.hash).await);
    // the stored rows themselves are no fork: a batch of them is a no-op
    insert_headers_batch(&db, &params, std::slice::from_ref(&y))
        .await
        .unwrap();
}

/// A store with a hole below a parent: the rule needs the missing row, so
/// the header is refused (`ancestry-missing`), never guessed.
#[tokio::test]
async fn a_hole_in_the_ancestry_refuses_the_header() {
    let chain = run(MAIN_886001, 886_001);
    let params = anchored(ChainParams::main(), &chain[146]);
    let db = SqliteDb::migrated();
    insert_headers_batch(&db, &params, &chain[..1000])
        .await
        .unwrap();
    crate::storage::update_chain_tip_to_highest(&db)
        .await
        .unwrap();
    db.conn()
        .execute("DELETE FROM headers WHERE height = 886950", [])
        .unwrap();
    let next = &chain[1000];
    let e = insert_header(&db, &params, next).await.unwrap_err();
    assert!(format!("{e}").contains("ancestry-missing"), "{e}");
    assert!(!stored(&db, &next.hash).await);
}

/// An orphan stored unlinked (its parent unknown) is checked when the
/// backfill links it: a parent the rule refuses never lands, and the orphan
/// stays an inactive row, never the tip.
#[tokio::test]
async fn an_orphan_on_a_refused_parent_never_becomes_the_tip() {
    let db = SqliteDb::migrated();
    let x = header(H - 2, "x", &"1".repeat(64));
    let y = header(H - 1, "y", &x.hash);
    let a = header(H, "a", &y.hash);
    for h in [&x, &y, &a] {
        insert_header(&db, regtest(), h).await.unwrap();
    }
    let bad_parent = header_with_bits(H + 1, "a parent at the wrong bits", &a.hash, HEAVY_BITS);
    let orphan = header_with_bits(H + 2, "its child", &bad_parent.hash, HEAVY_BITS);
    let woc = crate::host_harness::ScriptedChain::new();
    woc.publish(&bad_parent);
    let e = crate::sync::insert_with_parent_backfill(&db, regtest(), &woc, orphan.clone())
        .await
        .unwrap_err();
    assert!(format!("{e}").contains("bad-diffbits"), "{e}");
    assert!(!stored(&db, &bad_parent.hash).await);
    assert_eq!(
        crate::host_harness::flags(&db, &orphan.hash),
        (false, false)
    );
    assert_eq!(
        crate::storage::find_chain_tip(&db)
            .await
            .unwrap()
            .unwrap()
            .hash,
        a.hash
    );
}

// ─── The stored chain re-validated after the deploy ─────────────────────────

use crate::storage::{
    read_validation_state, revalidate_step, served_ceiling, served_tip, Ceiling, Revalidation,
};

/// A fresh store has nothing to re-validate (migration 0008 marks it
/// complete); a store with rows starts the pass and serves nothing as proof
/// until the pass has found its anchor.
#[tokio::test]
async fn migration_0008_completes_an_empty_store_and_starts_the_pass_on_a_full_one() {
    let db = SqliteDb::migrated();
    assert!(read_validation_state(&db).await.unwrap().complete);
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::All);
    let db = legacy_store(&chain_with_bad_bits_at_3()).await;
    let state = read_validation_state(&db).await.unwrap();
    assert!(!state.complete && state.validated_height.is_none());
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::Nothing);
    assert!(served_tip(&db).await.unwrap().is_none());
}

/// The legacy row with wrong bits: the pass anchors on regtest's genesis
/// checkpoint, passes 1 and 2, refuses 3 (`bad-diffbits`), records the
/// fault and stops; 2 and below are served, 3 and above never. A restart
/// stops at the same row.
#[tokio::test]
async fn the_pass_stops_at_the_first_refused_row_and_serves_only_below_it() {
    let rows = chain_with_bad_bits_at_3();
    let db = legacy_store(&rows).await;
    let params = ChainParams::regtest();
    let r = revalidate_step(&db, &params, 2000).await.unwrap();
    let Revalidation::Faulted { fault } = r else {
        panic!("{r:?}")
    };
    assert!(
        fault.contains("bad-diffbits") && fault.contains(" at 3:"),
        "{fault}"
    );
    let state = read_validation_state(&db).await.unwrap();
    assert_eq!(
        (
            state.validated_height,
            state.validated_hash.as_deref(),
            state.complete
        ),
        (Some(2), Some(rows[2].hash.as_str()), false)
    );
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::UpTo(2));
    for (h, served) in [(1, Some(true)), (2, Some(true)), (3, None), (5, None)] {
        assert_eq!(
            check_root_for_height(&db, &rows[h].merkle_root, h as u32)
                .await
                .unwrap(),
            served,
            "root at {h}"
        );
    }
    assert_eq!(served_tip(&db).await.unwrap().map(|t| t.height), Some(2));
    let info = crate::storage::get_info(&db, &crate::types::Chain::Main)
        .await
        .unwrap();
    assert_eq!(info.validated_height, Some(2));
    assert_eq!(info.validation_complete, Some(false));
    assert!(info.validation_fault.unwrap().contains("bad-diffbits"));
    // a second step does nothing; a restart stops at the same row
    assert!(matches!(
        revalidate_step(&db, &params, 2000).await.unwrap(),
        Revalidation::Faulted { .. }
    ));
    crate::d1::Query::new(crate::storage::SQL_RESTART_VALIDATION)
        .run(&db)
        .await
        .unwrap();
    assert!(matches!(
        revalidate_step(&db, &params, 2000).await.unwrap(),
        Revalidation::Faulted { .. }
    ));
}

/// A valid legacy store is passed in chunks, served up to the cursor while
/// the pass runs, and wholly once it reaches the tip.
#[tokio::test]
async fn the_pass_advances_in_chunks_and_completes_at_the_tip() {
    let mut rows = vec![regtest_genesis()];
    for h in 1..=20u32 {
        let parent = rows.last().unwrap().hash.clone();
        rows.push(header(h, &format!("legacy {h}"), &parent));
    }
    let db = legacy_store(&rows).await;
    let params = ChainParams::regtest();
    assert_eq!(
        revalidate_step(&db, &params, 5).await.unwrap(),
        Revalidation::Advanced { to: 5 }
    );
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::UpTo(5));
    assert_eq!(
        check_root_for_height(&db, &rows[5].merkle_root, 5)
            .await
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        check_root_for_height(&db, &rows[6].merkle_root, 6)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        revalidate_step(&db, &params, 5).await.unwrap(),
        Revalidation::Advanced { to: 10 }
    );
    assert_eq!(
        revalidate_step(&db, &params, 100).await.unwrap(),
        Revalidation::Complete
    );
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::All);
    assert_eq!(served_tip(&db).await.unwrap().map(|t| t.height), Some(20));
}

/// A reorg below the cursor while the pass runs: the cursor steps back to
/// the fork and the new branch is passed.
#[tokio::test]
async fn a_reorg_below_the_cursor_moves_it_back_to_the_fork() {
    let mut rows = vec![regtest_genesis()];
    for h in 1..=8u32 {
        let parent = rows.last().unwrap().hash.clone();
        rows.push(header(h, &format!("legacy {h}"), &parent));
    }
    let db = legacy_store(&rows).await;
    let params = ChainParams::regtest();
    assert_eq!(
        revalidate_step(&db, &params, 6).await.unwrap(),
        Revalidation::Advanced { to: 6 }
    );
    // the branch from 3: 4'..=10' replaces 4..=8 (written as the base wrote)
    db.conn()
        .execute(
            "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height >= 4",
            [],
        )
        .unwrap();
    let mut branch = vec![rows[3].clone()];
    for h in 4..=10u32 {
        let parent = branch.last().unwrap().hash.clone();
        branch.push(header(h, &format!("branch {h}"), &parent));
    }
    for h in &branch[1..] {
        db.conn()
            .execute(
                "INSERT INTO headers (previous_header_id, previous_hash, height, is_active, \
                 is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
                 VALUES (NULL, ?1, ?2, 1, ?3, ?4, '00', ?5, ?6, ?7, ?8, ?9)",
                rusqlite::params![
                    h.previous_hash,
                    h.height,
                    h.height == 10,
                    h.hash,
                    h.version,
                    h.merkle_root,
                    h.time,
                    h.bits,
                    h.nonce
                ],
            )
            .unwrap();
    }
    assert_eq!(
        revalidate_step(&db, &params, 100).await.unwrap(),
        Revalidation::Complete
    );
    let state = read_validation_state(&db).await.unwrap();
    assert_eq!(
        state.validated_hash.as_deref(),
        Some(branch[7].hash.as_str())
    );
}

/// The stored chain contradicts a checkpoint: nothing is served.
#[tokio::test]
async fn a_stored_chain_contradicting_its_checkpoint_serves_nothing() {
    let rows = chain_with_bad_bits_at_3();
    let db = legacy_store(&rows[..3]).await;
    let params = ChainParams::regtest()
        .with_checkpoints(&format!(
            "2:{}",
            "00000000000000000000000000000000000000000000000000000000000000bb"
        ))
        .unwrap();
    let r = revalidate_step(&db, &params, 100).await.unwrap();
    let Revalidation::Faulted { fault } = r else {
        panic!("{r:?}")
    };
    assert!(fault.contains("checkpoint mismatch: at 2"), "{fault}");
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::Nothing);
    assert_eq!(
        check_root_for_height(&db, &rows[1].merkle_root, 1)
            .await
            .unwrap(),
        None
    );
}

/// Real mainnet as a legacy store: 886001..=888000 written unchecked, then
/// the deploy; the pass from the owner's checkpoint 886147 completes in one
/// chunk (1853 rows) and serves everything.
#[tokio::test]
async fn a_real_mainnet_legacy_store_passes_from_its_checkpoint() {
    let chain = run(MAIN_886001, 886_001);
    let params = anchored(ChainParams::main(), &chain[146]);
    let db = legacy_store(&chain).await;
    assert_eq!(
        revalidate_step(&db, &params, crate::storage::REVALIDATE_CHUNK)
            .await
            .unwrap(),
        Revalidation::Complete
    );
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::All);
    let last = chain.last().unwrap();
    assert_eq!(
        check_root_for_height(&db, &last.merkle_root, last.height)
            .await
            .unwrap(),
        Some(true)
    );
}

/// The cron runs the pass: an idle cron on a legacy store advances it.
#[tokio::test]
async fn the_cron_runs_the_pass() {
    let mut rows = vec![regtest_genesis()];
    for h in 1..=4u32 {
        let parent = rows.last().unwrap().hash.clone();
        rows.push(header(h, &format!("legacy {h}"), &parent));
    }
    let db = legacy_store(&rows).await;
    let hooks = crate::host_harness::RecordedWebhooks::new(
        "https://consumer.example/tip-changed",
        "bearer",
    );
    let chain = crate::host_harness::ScriptedChain::new();
    chain.tip(4, &rows[4].hash);
    let params = ChainParams::regtest();
    crate::sync::run_cron(&db, &params, &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(served_ceiling(&db).await.unwrap(), Ceiling::All);
}

/// The whole of Teranode v0.16.0's
/// `services/blockchain/testdata/mainnet_headers_477792_504031.bin` (26,240
/// headers, 2.1 MB; not copied here), replayed through the store from the
/// node's checkpoint 478558 to its checkpoint 504031. Run by hand:
/// `P04_EDA_FILE=<path> cargo test --lib -- --ignored whole_eda_era`.
#[tokio::test]
#[ignore]
async fn whole_eda_era_from_checkpoint_478558_to_checkpoint_504031() {
    let path = std::env::var("P04_EDA_FILE").expect("P04_EDA_FILE");
    let bytes = std::fs::read(path).unwrap();
    let chain = run(&bytes, 477_792);
    assert_eq!(chain.last().unwrap().height, 504_031);
    let db = SqliteDb::migrated();
    insert_headers_batch(&db, &ChainParams::main(), &chain)
        .await
        .unwrap();
    assert_eq!(count_rows(&db).await, 26_240);
    eprintln!(
        "checked {} headers above 478558, {} bits changes",
        504_031 - 478_558,
        bits_changes(&chain, 478_558)
    );
}

/// A header whose parent is off the active chain is checked against ITS
/// branch: the window walks back by hash from the inactive parent to the
/// active chain. Under the node's minimum-difficulty rule
/// (`min_difficulty_rule`): A' (late, the limit's bits) lands inactive beside
/// A; its prompt child B' must carry the bits of the last non-limit header on
/// its own branch (Y's), and outworks A. To red: `load_window`, stop at an
/// inactive parent.
#[tokio::test]
async fn a_header_on_a_fork_is_checked_against_its_own_branch() {
    use crate::host_harness::{flags, header_at, heavy_root, min_difficulty_rule, EQUAL_WORK_BITS};
    let db = SqliteDb::migrated();
    let rule = min_difficulty_rule();
    let x = heavy_root();
    let y = header_at(x.height + 1, "y", &x.hash, HEAVY_BITS, x.time + 60);
    let a = header_at(y.height + 1, "a", &y.hash, HEAVY_BITS, y.time + 60);
    for h in [&x, &y, &a] {
        insert_header(&db, rule, h).await.unwrap();
    }
    let a2 = header_at(
        y.height + 1,
        "a prime, late",
        &y.hash,
        EQUAL_WORK_BITS,
        y.time + 21 * 60,
    );
    insert_header(&db, rule, &a2).await.unwrap();
    assert_eq!(flags(&db, &a2.hash), (false, false), "less work: inactive");
    let b2 = header_at(
        a2.height + 1,
        "b prime, prompt",
        &a2.hash,
        HEAVY_BITS,
        a2.time + 60,
    );
    let r = insert_header(&db, rule, &b2).await.unwrap();
    assert!(r.is_active_tip && r.reorg_depth == 1, "{r:?}");
    assert_eq!(flags(&db, &a.hash), (false, false));
    // the same child at the limit's bits is refused: its branch says Y's
    let wrong = header_at(
        a2.height + 1,
        "b prime at the limit",
        &a2.hash,
        EQUAL_WORK_BITS,
        a2.time + 60,
    );
    let e = insert_header(&db, rule, &wrong).await.unwrap_err();
    assert!(format!("{e}").contains("bad-diffbits"), "{e}");
}
