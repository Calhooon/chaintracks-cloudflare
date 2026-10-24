//! The 0.7.0 feed (`CHAIN_EVENT_FEED = "2"`) on the Worker's real storage
//! paths, beside the 0.6.0 feed it leaves unchanged under `"1"`. The steps are
//! the witness's: bsv-stack-lean
//! `corpus/scenarios/2026-10-23-chaintracks-reorg-event-with-a-cursor.md`.

use crate::events::{self, ChainEvent, Feed};
use crate::host_harness::{block_hash, header, regtest, RecordedWebhooks, ScriptedChain, SqliteDb};
use crate::storage;
use crate::sync;

fn kinds(db: &SqliteDb, after: u64) -> Vec<String> {
    let mut statement = db
        .conn()
        .prepare("SELECT json_extract(payload, '$.kind') FROM chain_events WHERE cursor > ? ORDER BY cursor")
        .unwrap();
    statement
        .query_map([after as i64], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

async fn served_chain() -> (SqliteDb, crate::consensus::ChainParams) {
    let db = SqliteDb::migrated();
    let root = header(100, "0.7.0 checkpoint", &block_hash("0.7.0 parent"));
    let a1 = header(101, "0.7.0 a101", &root.hash);
    let params = regtest()
        .clone()
        .with_checkpoints(&format!("{}:{}", root.height, root.hash))
        .unwrap();
    for h in [&root, &a1] {
        storage::insert_header(&db, &params, h).await.unwrap();
    }
    (db, params)
}

/// Witness steps 1 and 6 (CF3): a minute's tick on an unchanged served tip
/// journals no row under feed 2 and the 0.6.0 `tipAge` row under feed 1.
#[tokio::test]
async fn a_same_tip_tick_journals_nothing_under_feed_2_and_tip_age_under_feed_1() {
    let (db, params) = served_chain().await;
    let source = ScriptedChain::new();
    source.set_unavailable(true);
    let mut hooks = RecordedWebhooks::new("", "");
    hooks.feed = Feed::V2;
    let before = events::head(&db).await.unwrap();
    sync::run_cron(&db, &params, &source, None, &hooks)
        .await
        .unwrap();
    sync::run_pushed_tick(&db, &params, &hooks).await.unwrap();
    assert!(
        kinds(&db, before).is_empty(),
        "feed 2: a same tip writes no row"
    );
    // Another minute under feed 1: the dedup key moves with the minute.
    db.conn()
        .execute(
            "UPDATE chain_events SET event_key = 'previous-minute:' || cursor WHERE event_key LIKE 'tipAge:%'",
            [],
        )
        .unwrap();
    hooks.feed = Feed::V1;
    sync::run_cron(&db, &params, &source, None, &hooks)
        .await
        .unwrap();
    assert_eq!(kinds(&db, before), vec!["tipAge"], "feed 1: the 0.6.0 row");
}

/// The publish trigger's `tipAge` row at a tip change stays under feed 2.
#[tokio::test]
async fn a_tip_change_still_journals_tip_and_tip_age_under_feed_2() {
    let (db, params) = served_chain().await;
    let before = events::head(&db).await.unwrap();
    let tip = storage::served_tip(&db).await.unwrap().unwrap();
    let a2 = header(102, "0.7.0 a102", &tip.hash);
    storage::insert_header(&db, &params, &a2).await.unwrap();
    events::timer(&db, Feed::V2).await.unwrap();
    assert_eq!(kinds(&db, before), vec!["tip", "tipAge"]);
    let page = events::read_page(&db, before, 100).await.unwrap();
    assert!(matches!(
        events::decode(&page.events[0].payload).unwrap().event,
        ChainEvent::Tip { height: 102, .. }
    ));
}

#[test]
fn the_feed_var_reads_2_as_the_design_and_anything_else_as_0_6_0() {
    assert_eq!(Feed::parse(Some("2")), Feed::V2);
    assert_eq!(Feed::parse(Some(" 2 ")), Feed::V2);
    for other in [None, Some("1"), Some(""), Some("3"), Some("two")] {
        assert_eq!(Feed::parse(other), Feed::V1);
    }
}

/// Change 2 (CF4), the bound's arithmetic on the counter: an envelope stream
/// opened with a cursor spends one query at the open and one a wake, so it
/// wakes 899 times and ends with `retry:` at 900 queries, inside the
/// platform's 1,000; at 0.6.0 (three before the body, two a wake) the
/// queries pass 1,000 on wake 499, after 498 wakes of 15 s (7,470 s).
#[test]
fn the_stream_ends_at_900_queries_inside_the_platform_bound() {
    let mut budget = events::QueryBudget::new(1, events::STREAM_QUERY_BOUND);
    let mut wakes = 0;
    while budget.take() {
        wakes += 1;
    }
    assert_eq!(wakes, 899);
    assert_eq!(budget.spent(), 900);
    assert!(budget.spent() < 1000);
    assert!(!budget.take(), "a spent budget stays spent");
    // The tip view without a cursor: the head and the served tip first.
    let mut budget = events::QueryBudget::new(5, events::STREAM_QUERY_BOUND);
    let mut wakes = 0;
    while budget.take() {
        wakes += 1;
    }
    assert_eq!(wakes, 895);
    // The test hook lowers the bound and never raises it.
    let mut hooked = events::QueryBudget::new(1, 1);
    assert!(!hooked.take());
    let mut raised = events::QueryBudget::new(899, 5_000);
    assert!(raised.take());
    assert!(!raised.take());
    // 0.6.0: 3 + 2 * wakes queries pass 1,000 on wake 499.
    assert_eq!((1000 - 3) / 2 + 1, 499);
    assert_eq!(498 * 15, 7_470);
    assert_eq!(events::STREAM_END, "retry: 1000\n\n");
}

/// The feed-2 read is the page the 0.6.0 read gives, in one query, with the
/// head and the served tip beside it; the heartbeat carries the tip's age.
#[tokio::test]
async fn one_query_reads_the_page_the_head_and_the_tip() {
    let (db, _) = served_chain().await;
    let whole = events::read_page(&db, 0, 100).await.unwrap();
    let mut cursor = 0;
    let mut replay = Vec::new();
    loop {
        let wake = events::read_wake(&db, cursor, 1).await.unwrap();
        let page = events::read_page(&db, cursor, 1).await.unwrap();
        assert_eq!(wake.page.json(), page.json());
        assert_eq!(wake.head, whole.cursor);
        let tip = wake.tip.clone().unwrap();
        let served = storage::served_tip(&db).await.unwrap().unwrap();
        assert_eq!(
            (tip.height, tip.hash.as_str(), tip.time),
            (served.height, served.hash.as_str(), served.time)
        );
        replay.extend(wake.page.events.iter().map(|e| e.payload.clone()));
        cursor = wake.page.cursor;
        if !wake.page.has_more {
            break;
        }
    }
    assert_eq!(
        replay,
        whole
            .events
            .iter()
            .map(|e| e.payload.clone())
            .collect::<Vec<_>>()
    );
    let empty = events::read_wake(&db, cursor, 100).await.unwrap();
    assert!(empty.page.events.is_empty());
    assert_eq!(empty.page.cursor, cursor);
    let tip = empty.tip.unwrap();
    assert_eq!(
        events::heartbeat(Some(&tip), u64::from(tip.time) + 42),
        ": heartbeat tipAge=42\n\n"
    );
    assert_eq!(events::heartbeat(Some(&tip), 0), ": heartbeat tipAge=0\n\n");
    assert_eq!(events::heartbeat(None, 0), ": heartbeat\n\n");
    assert!(events::read_wake(&db, cursor, 0).await.is_err());
}

/// An empty store still answers the anchor row: head 0, no tip.
#[tokio::test]
async fn the_read_of_an_empty_journal_is_head_0_and_no_tip() {
    let db = SqliteDb::migrated();
    let wake = events::read_wake(&db, 0, 100).await.unwrap();
    assert_eq!(wake.head, 0);
    assert!(wake.tip.is_none());
    assert!(wake.page.events.is_empty());
}
