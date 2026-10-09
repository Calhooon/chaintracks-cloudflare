//! E5, the one door (`push::ingest_announced`), on the test store (a real
//! SQLite carrying the real migrations, every write through the Worker's own
//! storage functions). The chain source is the scripted ladder: every request
//! the door makes to it is counted, so a test can say a push asked no one.

use crate::host_harness::{
    block_hash, flags, header, header_with_bits, regtest, RecordedWebhooks, ScriptedChain,
    SqliteDb, HEAVY_BITS,
};
use crate::push::{ingest_announced, Announced, PARENT_WALK_LIMIT};
use crate::storage::{
    find_chain_tip, insert_headers_batch, update_chain_tip_to_highest, IngestError,
};
use crate::types::BlockHeader;

/// The anchor of `regtest()`: `x` at this height.
const X: u32 = 965_769;
/// The stored chain: `x` and five blocks on it, the tip at `X + 5`.
const STORED: u32 = 5;

/// The store: `x ..= x+5`, linked, the tip at `X + 5`.
async fn store() -> (SqliteDb, Vec<BlockHeader>) {
    let db = SqliteDb::migrated();
    let mut chain = vec![header(X, "x", &block_hash("the block below x"))];
    for i in 1..=STORED {
        let parent = chain[chain.len() - 1].hash.clone();
        chain.push(header(X + i, &format!("E5 stored {i}"), &parent));
    }
    insert_headers_batch(&db, regtest(), &chain).await.unwrap();
    update_chain_tip_to_highest(&db).await.unwrap();
    assert_eq!(
        find_chain_tip(&db).await.unwrap().unwrap().height,
        X + STORED
    );
    (db, chain)
}

/// Every row of the store, ordered, read raw: what "unchanged" means.
fn rows(db: &SqliteDb) -> Vec<(i64, String, bool, bool)> {
    let mut stmt = db
        .conn()
        .prepare("SELECT height, hash, is_active, is_chain_tip FROM headers ORDER BY height, hash")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn last_error(db: &SqliteDb) -> Option<String> {
    db.conn()
        .query_row("SELECT last_error FROM sync_state WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap()
}

fn last_seen(db: &SqliteDb) -> Option<i64> {
    db.conn()
        .query_row(
            "SELECT last_seen_height FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

fn hooks() -> RecordedWebhooks {
    RecordedWebhooks::new("https://consumer.example/tip", "synthetic-bearer")
}

async fn push(
    db: &SqliteDb,
    chain: &ScriptedChain,
    hooks: &RecordedWebhooks,
    h: &BlockHeader,
) -> Result<Announced, IngestError> {
    ingest_announced(db, regtest(), chain, hooks, h.clone()).await
}

/// A run of `n` headers on `parent`, labelled.
fn run_on(parent: &BlockHeader, n: u32, label: &str) -> Vec<BlockHeader> {
    let mut out: Vec<BlockHeader> = Vec::new();
    for i in 1..=n {
        let p = out.last().map_or(parent.hash.clone(), |h| h.hash.clone());
        out.push(header(parent.height + i, &format!("{label} {i}"), &p));
    }
    out
}

/// THE REFUSED PUSH. Three wrong headers pushed at the tip: one at bits the
/// difficulty rule does not answer for its parent (mined, so its own proof of
/// work holds), one whose claimed hash is not the hash of its bytes, one that
/// names the tip as its parent from two heights above it. Each is refused,
/// the store is the same row for row, the tip has not moved, the fault is
/// recorded, no consumer is told anything, and the ladder is never asked.
#[tokio::test]
async fn a_pushed_header_that_fails_a_rule_is_refused_and_the_store_unchanged() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let before = rows(&db);
    let ladder = ScriptedChain::new();
    let hooks = hooks();

    let wrong_bits = header_with_bits(tip.height + 1, "E5 wrong bits", &tip.hash, HEAVY_BITS);
    let mut wrong_hash = header(tip.height + 1, "E5 wrong hash", &tip.hash);
    wrong_hash.nonce = wrong_hash.nonce.wrapping_add(1);
    let wrong_height = header(tip.height + 2, "E5 wrong height", &tip.hash);

    for (what, bad) in [
        ("the difficulty rule", &wrong_bits),
        ("the claimed hash", &wrong_hash),
        ("the parent's height", &wrong_height),
    ] {
        let got = push(&db, &ladder, &hooks, bad).await;
        assert!(
            matches!(got, Err(IngestError::Refused(_))),
            "{what}: refused, not {got:?}"
        );
        assert_eq!(rows(&db), before, "{what}: the store is unchanged");
        assert_eq!(
            find_chain_tip(&db).await.unwrap().unwrap().hash,
            tip.hash,
            "{what}: the tip has not moved"
        );
        let fault = last_error(&db).unwrap_or_default();
        assert!(
            fault.starts_with("push: ") && fault.contains(&bad.hash),
            "{what}: the fault is recorded with the header: {fault}"
        );
    }
    assert_eq!(ladder.calls(), 0, "a refused push asks no courier");
    assert!(hooks.take().is_empty(), "no consumer hears a refused push");
    assert_eq!(last_seen(&db), None, "a refused push is not a seen height");
}

/// THE ACCEPTED PUSH. The next block, pushed: stored, the active row and the
/// tip, the seen height recorded, the consumers told, and no request to any
/// courier (the push is the read the poll would have made).
#[tokio::test]
async fn a_pushed_tip_is_stored_active_the_tip_moves_and_the_consumers_hear_it() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let ladder = ScriptedChain::new();
    let hooks = hooks();
    let next = header(tip.height + 1, "E5 the next block", &tip.hash);

    let got = push(&db, &ladder, &hooks, &next).await.expect("accepted");

    assert_eq!(got.outcome, "active", "{got:?}");
    assert_eq!((got.walked, got.tip_height), (0, next.height));
    assert_eq!(got.tip_hash, next.hash);
    assert_eq!(flags(&db, &next.hash), (true, true), "active and the tip");
    assert_eq!(flags(&db, &tip.hash), (true, false));
    assert_eq!(last_seen(&db), Some(next.height as i64));
    assert_eq!(
        ladder.calls(),
        0,
        "a push on a stored parent asks no courier"
    );
    let posts = hooks.take();
    assert_eq!(posts.len(), 1, "{posts:?}");
    assert!(
        posts[0].body.contains(&next.hash),
        "the announce names the pushed tip: {}",
        posts[0].body
    );
}

/// A push that arrives above a gap (the stream dropped for two blocks): its
/// missing parents are fetched by hash through the ladder, two requests, and
/// the run is ingested whole; the tip is the pushed header.
#[tokio::test]
async fn a_pushed_header_above_a_gap_walks_its_parents_through_the_ladder() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let ladder = ScriptedChain::new();
    let hooks = hooks();
    let run = run_on(&tip, 3, "E5 above the gap");
    ladder.publish(&run[0]);
    ladder.publish(&run[1]);

    let got = push(&db, &ladder, &hooks, &run[2]).await.expect("accepted");

    assert_eq!((got.outcome, got.walked), ("active", 2), "{got:?}");
    assert_eq!(ladder.calls(), 2, "one request per missing parent");
    for h in &run {
        assert!(flags(&db, &h.hash).0, "{} stored active", h.height);
    }
    assert_eq!(got.tip_hash, run[2].hash);
}

/// A reorg arrives as the new tip's header: the competing branch forks below
/// the tip, its first block is fetched by hash, and the branch, outworking
/// the stored one, takes the tip; the old tip is inactive.
#[tokio::test]
async fn a_reorg_arrives_as_the_new_tip_and_the_parent_walk_does_the_rest() {
    let (db, chain_rows) = store().await;
    let old_tip = chain_rows.last().unwrap().clone();
    let fork_parent = &chain_rows[chain_rows.len() - 2];
    let ladder = ScriptedChain::new();
    let hooks = hooks();
    let branch = run_on(fork_parent, 2, "E5 the winning branch");
    ladder.publish(&branch[0]);

    let got = push(&db, &ladder, &hooks, &branch[1])
        .await
        .expect("accepted");

    assert_eq!(got.walked, 1, "{got:?}");
    assert_eq!(got.tip_hash, branch[1].hash, "{got:?}");
    assert_eq!(flags(&db, &branch[0].hash), (true, false));
    assert_eq!(flags(&db, &branch[1].hash), (true, true));
    assert_eq!(
        flags(&db, &old_tip.hash),
        (false, false),
        "the old tip is off the chain"
    );
}

/// A sibling of the tip at equal work: stored, never served; the tip stays.
#[tokio::test]
async fn a_competitor_the_stored_chain_does_not_extend_is_stored_inactive() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let parent = &chain_rows[chain_rows.len() - 2];
    let ladder = ScriptedChain::new();
    let hooks = hooks();
    let sibling = header(tip.height, "E5 the sibling", &parent.hash);

    let got = push(&db, &ladder, &hooks, &sibling)
        .await
        .expect("answered");

    assert_eq!(got.outcome, "storedInactive", "{got:?}");
    assert!(
        got.reason
            .as_deref()
            .is_some_and(|r| r.contains("does not name")),
        "{got:?}"
    );
    assert_eq!(got.tip_hash, tip.hash);
    assert_eq!(flags(&db, &sibling.hash), (false, false));
    assert_eq!(flags(&db, &tip.hash), (true, true));
}

/// The stream's opening tip after a reconnect is the tip we hold: `known`,
/// nothing written, nobody asked, nobody told.
#[tokio::test]
async fn a_repeat_of_the_served_tip_is_known_and_writes_nothing() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let before = rows(&db);
    let ladder = ScriptedChain::new();
    let hooks = hooks();

    let got = push(&db, &ladder, &hooks, &tip).await.expect("answered");

    assert_eq!(got.outcome, "known", "{got:?}");
    assert!(!got.stored());
    assert_eq!(rows(&db), before);
    assert_eq!(ladder.calls(), 0);
    assert!(hooks.take().is_empty());
}

/// A push further above the store than the walk's bound is refused after the
/// bound's requests, and nothing of the run is stored: the catch-up's job.
#[tokio::test]
async fn a_push_beyond_the_walk_limit_is_refused_and_nothing_is_stored() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let before = rows(&db);
    let ladder = ScriptedChain::new();
    let hooks = hooks();
    let run = run_on(&tip, PARENT_WALK_LIMIT as u32 + 3, "E5 far above");
    for h in &run[..run.len() - 1] {
        ladder.publish(h);
    }

    let got = push(&db, &ladder, &hooks, run.last().unwrap()).await;

    assert!(matches!(got, Err(IngestError::Refused(_))), "{got:?}");
    assert_eq!(ladder.calls(), PARENT_WALK_LIMIT as u32);
    assert_eq!(rows(&db), before);
    assert!(last_error(&db)
        .unwrap_or_default()
        .contains("no stored row within"));
}

/// E5 item 3: a cron tick the push covers asks no courier (it is handed
/// none) and still does the rest of the cron's work: a tip that moved
/// without an announce (a backfill, a push whose announce was refused) is
/// announced on the tick.
#[tokio::test]
async fn a_tick_the_push_covers_asks_no_courier_and_still_announces() {
    let (db, chain_rows) = store().await;
    let tip = chain_rows.last().unwrap().clone();
    let next = header(tip.height + 1, "E5 unannounced", &tip.hash);
    insert_headers_batch(&db, regtest(), std::slice::from_ref(&next))
        .await
        .unwrap();
    update_chain_tip_to_highest(&db).await.unwrap();
    let hooks = hooks();

    crate::sync::run_pushed_tick(&db, regtest(), &hooks)
        .await
        .unwrap();

    let posts = hooks.take();
    assert_eq!(posts.len(), 1, "{posts:?}");
    assert!(posts[0].body.contains(&next.hash), "{}", posts[0].body);
    crate::sync::run_pushed_tick(&db, regtest(), &hooks)
        .await
        .unwrap();
    assert!(
        hooks.take().is_empty(),
        "the same tip is not announced twice"
    );
}
