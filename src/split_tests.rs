//! The two-rung witness of the 2026-10-16 most-work ruling.
use crate::couriers::CourierLadder;
use crate::host_harness::{block_hash, header, regtest, RecordedWebhooks, ScriptedChain, SqliteDb};
use crate::storage::{get_info, insert_header};
use crate::sync::run_cron;
use crate::types::{BlockHeader, Chain};

fn branch(root: &BlockHeader, label: &str) -> Vec<BlockHeader> {
    let mut rows = vec![root.clone()];
    for depth in 1..=4 {
        rows.push(header(
            root.height + depth,
            &format!("split {label} {depth}"),
            &rows.last().unwrap().hash,
        ));
    }
    rows
}

fn source(rows: &[BlockHeader]) -> ScriptedChain {
    let source = ScriptedChain::new();
    for h in rows {
        source.publish(h);
    }
    let tip = rows.last().unwrap();
    source.tip(tip.height, &tip.hash);
    source
}

async fn tick(db: &SqliteDb, a: &[BlockHeader], b: &[BlockHeader]) {
    let ladder = CourierLadder::new(
        vec![("woc", source(a)), ("arcade", source(b))],
        0,
        regtest().clone(),
    );
    run_cron(db, regtest(), &ladder, None, &RecordedWebhooks::new("", ""))
        .await
        .unwrap();
}

fn splits(db: &SqliteDb) -> Vec<serde_json::Value> {
    let mut statement = db.conn().prepare(
        "SELECT payload FROM chain_events WHERE json_extract(payload, '$.kind') IN ('split', 'splitResolved') ORDER BY cursor",
    ).unwrap();
    statement
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| serde_json::from_str(&r.unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn rung_tips_and_split_depth_key_resolution() {
    let db = SqliteDb::migrated();
    let root = header(965_875, "x", &block_hash("the block below x"));
    insert_header(&db, regtest(), &root).await.unwrap();
    let a = branch(&root, "a");
    let b = branch(&root, "b");
    tick(&db, &a[..=2], &b[..=2]).await;
    let info = serde_json::to_value(get_info(&db, &Chain::Main).await.unwrap()).unwrap();
    assert!(
        info["rungTips"].is_array(),
        "getInfo must retain each rung's tip"
    );
    assert_eq!(info["rungTips"].as_array().unwrap().len(), 2);
    for (name, expected, label) in [("woc", &a[2], "reference"), ("arcade", &b[2], "teranode")] {
        let tip = info["rungTips"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == name)
            .unwrap();
        let stored = crate::storage::find_header_for_hash(&db, &expected.hash)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tip["hash"], expected.hash);
        assert_eq!(tip["height"], expected.height);
        assert_eq!(tip["chainWork"], stored.chain_work);
        assert_eq!(tip["time"], expected.time);
        assert_eq!(tip["isActive"], stored.is_active);
        assert_eq!(tip["implementation"], label);
        assert!(tip["observedAt"].as_u64().unwrap() > 0);
    }
    assert!(splits(&db).is_empty(), "depth two is below the hold");
    tick(&db, &a[..=3], &b[..=2]).await;
    assert!(splits(&db).is_empty(), "both branches must reach the depth");
    tick(&db, &a[..=3], &b[..=3]).await;
    let events = splits(&db);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["v"], 2);
    assert_eq!(events[0]["kind"], "split");
    assert_eq!(events[0]["key"], format!("split:{}", root.hash));
    assert_eq!(events[0]["forkHeight"], root.height + 1);
    assert_eq!(events[0]["tips"].as_array().unwrap().len(), 2);
    tick(&db, &a[..=4], &b[..=3]).await;
    assert_eq!(splits(&db).len(), 1, "growth deduplicates by fork point");
    tick(&db, &a[..=4], &a[..=3]).await;
    let events = splits(&db);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["kind"], "splitResolved");
    assert_eq!(events[1]["key"], events[0]["key"]);
    tick(&db, &a[..=4], &a[..=4]).await;
    assert_eq!(splits(&db).len(), 2, "resolution is emitted once");
    tick(&db, &a[..=4], &b[..=4]).await;
    assert_eq!(splits(&db).len(), 2, "one lifecycle per fork point");
}

fn observed(
    name: &str,
    tip: Option<&BlockHeader>,
    at: u64,
) -> (crate::rungs::Definition, Option<crate::rungs::Observation>) {
    (
        crate::rungs::Definition {
            name: name.into(),
            implementation: crate::rungs::default_implementation(name).into(),
        },
        tip.map(|h| crate::rungs::Observation {
            hash: h.hash.clone(),
            height: h.height,
            observed_at: at,
        }),
    )
}

async fn stored_fork() -> (SqliteDb, Vec<BlockHeader>, Vec<BlockHeader>) {
    let db = SqliteDb::migrated();
    let root = header(965_875, "x", &block_hash("the block below x"));
    let a = branch(&root, "a");
    let b = branch(&root, "b");
    for h in a.iter().chain(b.iter().skip(1)) {
        insert_header(&db, regtest(), h).await.unwrap();
    }
    crate::sync::revalidate(&db, regtest()).await;
    (db, a, b)
}

#[tokio::test]
async fn failed_unknown_and_late_observations_do_not_resolve_a_split() {
    let (db, a, b) = stored_fork().await;
    crate::rungs::record(
        &db,
        &[
            observed("woc", Some(&a[3]), 10),
            observed("arcade", Some(&b[3]), 10),
        ],
        3,
    )
    .await
    .unwrap();
    crate::rungs::record(
        &db,
        &[
            observed("woc", Some(&a[4]), 20),
            observed("arcade", None, 20),
        ],
        3,
    )
    .await
    .unwrap();
    assert_eq!(
        splits(&db).len(),
        1,
        "a failed rung retains its observation"
    );
    crate::rungs::record(
        &db,
        &[
            observed("woc", Some(&b[3]), 9),
            observed("arcade", None, 20),
        ],
        3,
    )
    .await
    .unwrap();
    let tips = crate::rungs::read(&db).await.unwrap();
    assert_eq!(
        tips.iter()
            .find(|t| t.name == "woc")
            .unwrap()
            .hash
            .as_deref(),
        Some(a[4].hash.as_str())
    );
    let missing = header(a[4].height + 1, "unavailable tip", &a[4].hash);
    crate::rungs::record(
        &db,
        &[
            observed("woc", Some(&missing), 30),
            observed("arcade", None, 30),
        ],
        3,
    )
    .await
    .unwrap();
    let tips = crate::rungs::read(&db).await.unwrap();
    assert!(tips
        .iter()
        .find(|t| t.name == "woc")
        .unwrap()
        .chain_work
        .is_none());
    assert_eq!(
        splits(&db).len(),
        1,
        "unknown ancestry cannot prove resolution"
    );
    crate::rungs::record(&db, &[observed("woc", Some(&a[4]), 40)], 3)
        .await
        .unwrap();
    assert_eq!(crate::rungs::read(&db).await.unwrap().len(), 1);
    assert_eq!(
        splits(&db)[1]["kind"],
        "splitResolved",
        "a removed rung no longer holds its branch"
    );
}

#[tokio::test]
async fn split_snapshot_and_journal_commit_together() {
    let (db, a, b) = stored_fork().await;
    db.conn().execute_batch("CREATE TRIGGER reject_split BEFORE INSERT ON chain_events WHEN json_extract(NEW.payload, '$.kind') = 'split' BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;").unwrap();
    let snapshot = [
        observed("woc", Some(&a[3]), 1),
        observed("arcade", Some(&b[3]), 1),
    ];
    assert!(crate::rungs::record(&db, &snapshot, 3)
        .await
        .unwrap_err()
        .to_string()
        .contains("injected journal failure"));
    assert!(crate::rungs::read(&db).await.unwrap().is_empty());
    assert!(splits(&db).is_empty());
    db.conn()
        .execute_batch("DROP TRIGGER reject_split;")
        .unwrap();
    crate::rungs::record(&db, &snapshot, 3).await.unwrap();
    assert_eq!(splits(&db).len(), 1);
    db.conn().execute_batch("CREATE TRIGGER reject_resolution BEFORE INSERT ON chain_events WHEN json_extract(NEW.payload, '$.kind') = 'splitResolved' BEGIN SELECT RAISE(ABORT, 'injected resolution failure'); END;").unwrap();
    let resolution = [
        observed("woc", Some(&a[3]), 2),
        observed("arcade", Some(&a[3]), 2),
    ];
    assert!(crate::rungs::record(&db, &resolution, 3).await.is_err());
    assert_eq!(splits(&db).len(), 1);
    assert_eq!(
        crate::rungs::read(&db).await.unwrap()[0].hash.as_deref(),
        Some(b[3].hash.as_str())
    );
    db.conn()
        .execute_batch("DROP TRIGGER reject_resolution;")
        .unwrap();
    crate::rungs::record(&db, &resolution, 3).await.unwrap();
    assert_eq!(splits(&db).len(), 2);
}

#[tokio::test]
async fn configured_depth_and_shared_tip_rung_names_reach_the_feed() {
    let (db, a, b) = stored_fork().await;
    let snapshot = [
        observed("woc", Some(&a[2]), 1),
        observed("arcade", Some(&b[2]), 1),
        observed("bitails", Some(&a[2]), 1),
    ];
    crate::rungs::record(&db, &snapshot, 3).await.unwrap();
    assert!(splits(&db).is_empty());
    let ladder = CourierLadder::new(
        vec![
            ("woc", source(&a[..=2])),
            ("arcade", source(&b[..=2])),
            ("bitails", source(&a[..=2])),
        ],
        0,
        regtest().clone(),
    )
    .with_configuration(&[], Some("2".into()));
    run_cron(
        &db,
        regtest(),
        &ladder,
        None,
        &RecordedWebhooks::new("", ""),
    )
    .await
    .unwrap();
    let event = splits(&db).pop().unwrap();
    let tip = event["tips"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["hash"] == a[2].hash)
        .unwrap();
    assert_eq!(tip["rungs"], serde_json::json!(["bitails", "woc"]));
    let wire = event.to_string();
    let envelope = crate::events::decode(&wire).unwrap();
    assert_eq!(serde_json::to_value(&envelope).unwrap(), event);
    let frame = crate::events::sse_frame(
        &crate::events::JournalEvent {
            cursor: 42,
            payload: wire.clone(),
            fault: None,
        },
        crate::events::View::Envelope,
    )
    .unwrap();
    assert_eq!(frame, format!("id: 42\ndata: {wire}\n\n"));
    let hooks = RecordedWebhooks::new("https://consumer.example/events", "local");
    crate::events::deliver(&db, &hooks).await.unwrap();
    assert!(hooks
        .take()
        .iter()
        .any(|p| p.body == wire
            || serde_json::from_str::<serde_json::Value>(&p.body).unwrap() == event));
    let mut invalid = event.clone();
    invalid["v"] = 1.into();
    assert!(crate::events::decode(&invalid.to_string())
        .unwrap_err()
        .to_string()
        .contains("unknownKind"));
    for bad in [
        {
            let mut v = event.clone();
            v["key"] = "split:invalid".into();
            v
        },
        {
            let mut v = event.clone();
            v["tips"][0]["rungs"] = serde_json::json!([]);
            v
        },
        {
            let mut v = event.clone();
            v["extra"] = true.into();
            v
        },
        {
            let mut v = event.clone();
            v["tips"][0]["height"] = 0.into();
            v
        },
    ] {
        assert!(crate::events::decode(&bad.to_string())
            .unwrap_err()
            .to_string()
            .contains("unknownShape"));
    }
}

#[tokio::test]
async fn split_walk_follows_bulk_parent_hashes_without_numeric_links() {
    let (db, a, b) = stored_fork().await;
    // The bulk importer can leave active rows without previous_header_id.
    db.conn()
        .execute(
            "UPDATE headers SET previous_header_id = NULL WHERE is_active = 1",
            [],
        )
        .unwrap();
    crate::rungs::record(
        &db,
        &[
            observed("woc", Some(&a[3]), 1),
            observed("arcade", Some(&b[3]), 1),
        ],
        3,
    )
    .await
    .unwrap();
    let events = splits(&db);
    assert_eq!(
        events.len(),
        1,
        "stored parent hashes must establish the fork across bulk rows"
    );
    assert_eq!(events[0]["key"], format!("split:{}", a[0].hash));
    crate::rungs::record(
        &db,
        &[
            observed("woc", Some(&a[3]), 2),
            observed("arcade", Some(&a[2]), 2),
        ],
        3,
    )
    .await
    .unwrap();
    assert_eq!(splits(&db)[1]["kind"], "splitResolved");
}
