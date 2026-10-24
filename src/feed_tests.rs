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
