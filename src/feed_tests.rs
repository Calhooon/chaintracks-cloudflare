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

fn insert_row(db: &SqliteDb, payload: &str, key: Option<&str>, created_at: u64) -> u64 {
    db.conn()
        .execute(
            "INSERT INTO chain_events(payload, event_key, created_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![payload, key, created_at as i64],
        )
        .unwrap();
    db.conn().last_insert_rowid() as u64
}

fn cursors(db: &SqliteDb) -> Vec<u64> {
    let mut statement = db
        .conn()
        .prepare("SELECT cursor FROM chain_events ORDER BY cursor")
        .unwrap();
    statement
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap() as u64)
        .collect()
}

fn floor(db: &SqliteDb) -> u64 {
    db.conn()
        .query_row(
            "SELECT floor FROM chain_event_state WHERE id = 1",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap() as u64
}

fn payload_of(db: &SqliteDb, kind: &str) -> String {
    db.conn()
        .query_row(
            "SELECT payload FROM chain_events WHERE json_extract(payload, '$.kind') = ?1 ORDER BY cursor DESC LIMIT 1",
            [kind],
            |r| r.get(0),
        )
        .unwrap()
}

const DAY: u64 = 86_400;

/// Witness step 5 (CF2): chain rows older than 90 days and `tipAge` rows
/// older than 24 hours go, the floor rises to the greatest deleted chain
/// cursor in the same transaction, a cursor below it reads 410 and a cursor
/// at it reads the page the full journal gives (a deleted `tipAge` is a
/// skipped number); a row of an unknown kind and the head row stay.
#[tokio::test]
async fn the_retention_deletes_old_rows_and_raises_the_floor_with_them() {
    let (db, _) = served_chain().await;
    let now = events::now_secs();
    let tip = payload_of(&db, "tip");
    let age = payload_of(&db, "tipAge");
    db.conn()
        .execute(
            "UPDATE chain_events SET created_at = ?1",
            [(now - 100 * DAY) as i64],
        )
        .unwrap();
    let opening = cursors(&db);
    // created_at rises with the cursor, as unixepoch() writes it.
    let old_tip = insert_row(&db, &tip, None, now - 100 * DAY);
    let unknown = insert_row(&db, r#"{"v":3,"kind":"later"}"#, None, now - 95 * DAY);
    let young_tip = insert_row(&db, &tip, None, now - 10 * DAY);
    let old_age = insert_row(&db, &age, Some("tipAge:a:1"), now - 2 * DAY);
    let young_age = insert_row(&db, &age, Some("tipAge:a:2"), now - 3_600);
    let head = insert_row(&db, &tip, None, now);
    let whole = events::read_wake(&db, unknown - 1, 100).await.unwrap();
    assert_eq!(floor(&db), 0);
    events::retain(&db, now).await.unwrap();
    assert_eq!(floor(&db), old_tip, "the greatest deleted chain row");
    assert_eq!(cursors(&db), vec![unknown, young_tip, young_age, head]);
    assert!(opening.iter().all(|c| *c < old_tip));
    assert!(old_age > young_tip, "a deleted tipAge above the floor");
    // Below the floor: 410 with the floor and the served tip.
    let expired = events::read_wake(&db, old_tip - 1, 100).await.unwrap();
    assert!(expired.expired(old_tip - 1));
    let body: serde_json::Value = serde_json::from_str(&expired.expired_json()).unwrap();
    let served = storage::served_tip(&db).await.unwrap().unwrap();
    assert_eq!(
        body,
        serde_json::json!({"v": 1, "error": "cursorExpired", "floor": old_tip.to_string(),
            "tip": {"height": served.height, "hash": served.hash}})
    );
    assert!(
        expired.expired(0),
        "a replay from 0 past a floor is told so"
    );
    // At the floor: the page the full journal gives from there.
    let at = events::read_wake(&db, old_tip, 100).await.unwrap();
    assert!(!at.expired(old_tip));
    assert_eq!(
        at.page.events.iter().map(|e| e.cursor).collect::<Vec<_>>(),
        vec![unknown, young_tip, young_age, head]
    );
    assert_eq!(
        whole
            .page
            .events
            .iter()
            .map(|e| e.cursor)
            // the one difference: the deleted tipAge, a skipped number
            .filter(|c| *c >= unknown && *c != old_age)
            .collect::<Vec<_>>(),
        at.page.events.iter().map(|e| e.cursor).collect::<Vec<_>>()
    );
    // A second tick a day later: the young tipAge goes, a skipped number above
    // the floor; the floor stays, the page from the floor has no chain hole.
    events::retain(&db, now + DAY).await.unwrap();
    assert_eq!(floor(&db), old_tip);
    assert_eq!(cursors(&db), vec![unknown, young_tip, head]);
    // Eighty-one days on, the young tip is old; the head row stays always.
    events::retain(&db, now + 81 * DAY).await.unwrap();
    assert_eq!(floor(&db), young_tip);
    assert_eq!(cursors(&db), vec![unknown, head]);
    events::retain(&db, now + 1_000 * DAY).await.unwrap();
    assert_eq!(cursors(&db), vec![unknown, head], "never the head row");
    assert_eq!(floor(&db), young_tip);
}

/// At most 1,000 rows a tick; the backlog of the minute's `tipAge` rows
/// written since 0009 drains over the ticks after the release.
#[tokio::test]
async fn the_retention_deletes_at_most_1000_rows_a_tick() {
    let (db, _) = served_chain().await;
    let now = events::now_secs();
    let age = payload_of(&db, "tipAge");
    for minute in 0..1_500 {
        insert_row(
            &db,
            &age,
            Some(&format!("tipAge:backlog:{minute}")),
            now - 2 * DAY,
        );
    }
    insert_row(&db, &payload_of(&db, "tip"), None, now);
    let before = cursors(&db).len();
    events::retain(&db, now).await.unwrap();
    assert_eq!(before - cursors(&db).len(), 1_000);
    events::retain(&db, now).await.unwrap();
    assert_eq!(before - cursors(&db).len(), 1_500);
    assert_eq!(floor(&db), 0, "a tipAge row never raises the floor");
}

/// Change 4, the outbox under feed 2: one read of the head and of every
/// delivery cursor a tick for both targets; the caught-up target costs
/// nothing more, the behind one its page and its POSTs; a quiet tick is the
/// one read; the 0.6.0 outbox reads three queries a target.
#[tokio::test]
async fn the_outbox_reads_the_head_once_a_tick_for_every_target() {
    use crate::d1::Query;
    use crate::host_harness::{Handed, Recorder};
    let (db, _) = served_chain().await;
    let head = events::head(&db).await.unwrap();
    db.conn()
        .execute(
            "INSERT INTO chain_event_deliveries(target, cursor) VALUES ('A=https://a.example/events', ?1)",
            [head as i64],
        )
        .unwrap();
    let mut hooks = RecordedWebhooks::new(
        "A=https://a.example/events,B=https://b.example/events",
        "synthetic-bearer",
    );
    hooks.feed = Feed::V2;
    let rec = Recorder::new(&db);
    events::deliver(&rec, &hooks).await.unwrap();
    let sent = hooks.take();
    assert!(!sent.is_empty());
    assert!(sent
        .iter()
        .all(|p| p.target.binding.as_deref() == Some("B")));
    let reads: Vec<Handed> = rec
        .handed()
        .into_iter()
        .filter(|h| !matches!(h, Handed::Execute(_)))
        .collect();
    assert_eq!(
        reads,
        vec![
            Handed::All(Query::new(events::SQL_OUTBOX)),
            Handed::All(Query::new(events::SQL_ROWS).bind(0u64).bind(16u32)),
        ]
    );
    assert_eq!(
        rec.handed().len() - reads.len(),
        sent.len(),
        "one cursor upsert per accepted POST"
    );
    // Both at the head: the tick is the one read.
    let quiet = Recorder::new(&db);
    events::deliver(&quiet, &hooks).await.unwrap();
    assert!(hooks.take().is_empty());
    assert_eq!(
        quiet.handed(),
        vec![Handed::All(Query::new(events::SQL_OUTBOX))]
    );
    // 0.6.0 on the same quiet journal: per target its cursor, the head, the rows.
    hooks.feed = Feed::V1;
    let old = Recorder::new(&db);
    events::deliver(&old, &hooks).await.unwrap();
    assert_eq!(old.handed().len(), 6);
}

/// A stored cursor below the floor or ahead of the journal stops that target
/// alone, logged; a new target starts at the floor; the bytes are the page's.
#[tokio::test]
async fn the_outbox_holds_an_expired_target_and_starts_a_new_one_at_the_floor() {
    let (db, _) = served_chain().await;
    let whole = events::read_page(&db, 0, 100).await.unwrap();
    let floor = whole.events[0].cursor;
    db.conn()
        .execute("UPDATE chain_event_state SET floor = ?1", [floor as i64])
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO chain_event_deliveries(target, cursor) VALUES ('A=https://a.example/events', 0), ('C=https://c.example/events', 999999)",
            [],
        )
        .unwrap();
    let mut hooks = RecordedWebhooks::new(
        "A=https://a.example/events,B=https://b.example/events,C=https://c.example/events",
        "synthetic-bearer",
    );
    hooks.feed = Feed::V2;
    events::deliver(&db, &hooks).await.unwrap();
    let sent = hooks.take();
    assert!(sent
        .iter()
        .all(|p| p.target.binding.as_deref() == Some("B")));
    assert_eq!(
        sent.iter().map(|p| p.body.clone()).collect::<Vec<_>>(),
        whole.events[1..]
            .iter()
            .map(|e| e.payload.clone())
            .collect::<Vec<_>>()
    );
}
