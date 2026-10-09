//! #32: the scenario of record on the Worker's real storage paths.
//! [SRC] go-chaintracks@c7eeda1 chaintracks/types.go:44-57;
//! [SRC] ts-stack@fb1b2da packages/wallet/wallet-toolbox/src/services/
//! chaintracker/chaintracks/GoChaintracksServiceClient.ts:266-307,689-714.

use crate::events::{self, ChainEvent, Envelope, EventHeader, Outpoint, View};
use crate::host_harness::{block_hash, header, regtest, SqliteDb};
use crate::host_harness::{header_at, RecordedWebhooks, ScriptedChain, EQUAL_WORK_BITS};
use crate::storage;
use crate::sync::{self, WebhookDelivery};

fn fixture() -> (
    SqliteDb,
    crate::consensus::ChainParams,
    Vec<crate::types::BlockHeader>,
) {
    let db = SqliteDb::migrated();
    let root = header(100, "#32 checkpoint", &block_hash("#32 parent"));
    let a1 = header(101, "#32 A1 contains our transaction", &root.hash);
    let b1 = header(101, "#32 B1 excludes it", &root.hash);
    let b2 = header(102, "#32 B2 excludes it", &b1.hash);
    let params = regtest()
        .clone()
        .with_checkpoints(&format!("{}:{}", root.height, root.hash))
        .unwrap();
    (db, params, vec![root, a1, b1, b2])
}

async fn seed(
    db: &SqliteDb,
    params: &crate::consensus::ChainParams,
    headers: &[crate::types::BlockHeader],
) {
    for h in headers {
        storage::insert_header(db, params, h).await.unwrap();
    }
}

fn public_header(h: &crate::types::BlockHeader) -> serde_json::Value {
    let mut h = serde_json::to_value(EventHeader::from(h)).unwrap();
    h.as_object_mut().unwrap().remove("chainWork");
    h
}

#[tokio::test]
async fn scenario_announced_nobody_hears_has_a_versioned_reorg() {
    let db = SqliteDb::migrated();
    let root = header(100, "#32 checkpoint", &block_hash("#32 parent"));
    let a1 = header(101, "#32 A1 contains our transaction", &root.hash);
    let b1 = header(101, "#32 B1 excludes it", &root.hash);
    let b2 = header(102, "#32 B2 excludes it", &b1.hash);
    let params = regtest()
        .clone()
        .with_checkpoints(&format!("{}:{}", root.height, root.hash))
        .unwrap();
    for h in [&root, &a1, &b1, &b2] {
        storage::insert_header(&db, &params, h).await.unwrap();
    }
    assert_eq!(
        storage::served_tip(&db).await.unwrap().unwrap().hash,
        b2.hash
    );
    assert_eq!(
        storage::check_root_for_height(&db, &a1.merkle_root, a1.height)
            .await
            .unwrap(),
        Some(false)
    );
    let mut statement = db
        .conn()
        .prepare("SELECT payload FROM chain_events ORDER BY cursor")
        .expect("the Worker must journal the versioned reorg for push and cursor replay");
    let events: Vec<serde_json::Value> = statement
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| serde_json::from_str(&r.unwrap()).unwrap())
        .collect();
    let event = events
        .iter()
        .find(|e| e["kind"] == "reorg")
        .expect("the announced reorg must reach a client, including the deactivated header");
    assert_eq!(event["v"], 1);
    assert_eq!(event["depth"], 1);
    assert_eq!(event["forkHeight"], 101);
    assert_eq!(event["deactivatedHeaders"][0]["hash"], a1.hash);
    assert_eq!(event["newTip"]["hash"], b2.hash);
    let envelope = events::decode(&event.to_string()).unwrap();
    let view = events::compatibility_view(&envelope, View::Reorg).unwrap();
    assert_eq!(view["oldTip"]["hash"], a1.hash);
    assert_eq!(view.as_object().unwrap().len(), 4);
    assert_eq!(view["oldTip"].as_object().unwrap().len(), 8);
    if let Ok(path) = std::env::var("LANE32_WITNESS_FILE") {
        let expected = serde_json::json!({"depth":1,"oldTip":public_header(&a1),"newTip":public_header(&b2),"deactivatedHeaders":[public_header(&a1)]});
        assert_eq!(view, expected);
        // Codec control for the unmodified production reference. These real
        // headers test the wire reader, independently of the regtest fork.
        // [SRC] Teranode@4edb60a4 services/blockchain/886001_888000_headers.bin;
        // the P0-4 copy and sha256 are in src/testdata/README.md.
        let raw = include_bytes!("testdata/main_886001_888000.bin");
        let real: Vec<_> = raw[..240]
            .chunks_exact(80)
            .enumerate()
            .map(|(i, bytes)| {
                let mut h =
                    crate::types::BlockHeader::from_bytes(bytes, 886001 + i as u32).unwrap();
                h.chain_work = crate::types::calculate_work(h.bits);
                h.check_pow(&crate::consensus::ChainParams::main()).unwrap();
                h
            })
            .collect();
        let main_event = Envelope {
            v: 1,
            event: ChainEvent::Reorg {
                fork_height: real[1].height,
                depth: 1,
                deactivated_headers: vec![EventHeader::from(&real[1])],
                new_tip: EventHeader::from(&real[2]),
            },
        };
        let main_view = events::compatibility_view(&main_event, View::Reorg).unwrap();
        let main_control = serde_json::json!({"depth":1,"oldTip":public_header(&real[1]),"newTip":public_header(&real[2]),"deactivatedHeaders":[public_header(&real[1])]});
        assert_eq!(main_view, main_control);
        std::fs::write(path, serde_json::to_string(&serde_json::json!({
            "envelope":event,"view":view,"control":expected,"oldTip":public_header(&a1),"ancestor":public_header(&root),
            "headers":[public_header(&root),public_header(&a1),public_header(&b1),public_header(&b2)],
            "mainnet":{"envelope":main_event,"view":main_view,"control":main_control,"oldTip":public_header(&real[1]),"ancestor":public_header(&real[0])}
        })).unwrap()).unwrap();
    }
}

#[tokio::test]
async fn a_tied_competitor_emits_a_fork_before_the_reorg() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h[..3]).await;
    assert_eq!(
        storage::served_tip(&db).await.unwrap().unwrap().hash,
        h[1].hash
    );
    let page = events::read_page(&db, 0, 100).await.unwrap();
    let fork = page
        .events
        .iter()
        .find_map(|e| match events::decode(&e.payload).unwrap().event {
            ChainEvent::Fork {
                height,
                competing_tips,
                depth,
            } => Some((height, competing_tips, depth)),
            _ => None,
        })
        .unwrap();
    assert_eq!(fork.0, 101);
    assert_eq!(fork.2, 1);
    assert_eq!(
        fork.1.iter().map(|h| h.hash.as_str()).collect::<Vec<_>>(),
        vec![h[1].hash.as_str(), h[2].hash.as_str()]
    );
    assert_eq!(fork.1[0].chain_work, fork.1[1].chain_work);
    assert!(!page
        .events
        .iter()
        .any(|e| e.payload.contains("\"kind\":\"reorg\"")));
    storage::insert_header(&db, &params, &h[3]).await.unwrap();
    let next = events::read_page(&db, page.cursor, 100).await.unwrap();
    assert!(next.events.iter().any(|e| matches!(
        events::decode(&e.payload).unwrap().event,
        ChainEvent::Reorg { depth: 1, .. }
    )));
}

#[tokio::test]
async fn poll_sse_and_webhook_replay_identical_envelope_bytes() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h).await;
    let whole = events::read_page(&db, 0, 100).await.unwrap();
    let mut cursor = 0;
    let mut replay = Vec::new();
    loop {
        let page = events::read_page(&db, cursor, 2).await.unwrap();
        assert!(page
            .json()
            .contains(&format!("\"cursor\":\"{}\"", page.cursor)));
        for event in &page.events {
            assert!(page.json().contains(&event.payload));
            assert_eq!(
                events::sse_frame(event, View::Envelope).unwrap(),
                format!("id: {}\ndata: {}\n\n", event.cursor, event.payload)
            );
            replay.push(event.payload.clone());
        }
        cursor = page.cursor;
        if !page.has_more {
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
    assert!(events::read_page(&db, cursor, 100)
        .await
        .unwrap()
        .events
        .is_empty());
    let hooks = RecordedWebhooks::new(
        "CONSUMER=https://consumer.example/events",
        "synthetic-bearer",
    );
    events::deliver(&db, &hooks).await.unwrap();
    let sent = hooks.take();
    assert_eq!(
        sent.iter().map(|p| p.body.clone()).collect::<Vec<_>>(),
        replay
    );
    assert!(sent
        .iter()
        .all(|p| p.target.binding.as_deref() == Some("CONSUMER")));
    events::deliver(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty());
}

#[tokio::test]
async fn a_refused_webhook_keeps_its_cursor_and_other_targets_progress() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h).await;
    let hooks = RecordedWebhooks::new(
        "A=https://a.example/events,B=https://b.example/events",
        "synthetic-bearer",
    );
    hooks.reply_for(
        "https://a.example/events",
        WebhookDelivery::Failed("offline".into()),
    );
    events::deliver(&db, &hooks).await.unwrap();
    let sent = hooks.take();
    let a = sent
        .iter()
        .filter(|p| p.target.binding.as_deref() == Some("A"))
        .collect::<Vec<_>>();
    assert_eq!(a.len(), 1);
    let failed = a[0].body.clone();
    let acked: i64 = db
        .conn()
        .query_row("SELECT COUNT(*) FROM chain_event_deliveries", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(acked, 1);
    hooks.reply_for("https://a.example/events", WebhookDelivery::Accepted(204));
    events::deliver(&db, &hooks).await.unwrap();
    let retry = hooks.take();
    assert_eq!(retry[0].body, failed);
    assert!(retry
        .iter()
        .all(|p| p.target.binding.as_deref() == Some("A")));
    events::deliver(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty());
}

#[tokio::test]
async fn unknown_version_kind_or_shape_is_reported_and_not_acknowledged() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h[..1]).await;
    for (bad, expected) in [
        ("{\"v\":2,\"kind\":\"tip\"}", "unknownVersion"),
        ("{\"v\":1,\"kind\":\"future\"}", "unknownKind"),
        ("{\"v\":1,\"kind\":\"tip\"}", "unknownShape"),
    ] {
        assert!(events::decode(bad)
            .unwrap_err()
            .to_string()
            .contains(expected));
    }
    let before = events::head(&db).await.unwrap();
    db.conn()
        .execute(
            "INSERT INTO chain_events(payload) VALUES (?)",
            ["{\"v\":2,\"kind\":\"future\"}"],
        )
        .unwrap();
    let error = events::read_page(&db, before, 100).await.err().unwrap();
    assert!(error.to_string().contains("unknownVersion"));
    let hooks = RecordedWebhooks::new("https://consumer.example/events", "synthetic-bearer");
    assert!(events::deliver(&db, &hooks).await.is_err());
    assert!(hooks.take().is_empty());
    assert_eq!(
        db.conn()
            .query_row("SELECT COUNT(*) FROM chain_event_deliveries", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn reconnect_cursor_is_validated_and_last_event_id_takes_precedence() {
    let url = url::Url::parse("https://headers.example/events/stream?since=7").unwrap();
    assert_eq!(events::parse_cursor(&url, Some("12")).unwrap(), Some(12));
    assert_eq!(events::parse_cursor(&url, None).unwrap(), Some(7));
    for bad in ["", "-1", "1.0", " 1", "+1", "1\n", "9007199254740992"] {
        assert!(events::parse_cursor(&url, Some(bad)).is_err());
    }
    let duplicate = url::Url::parse("https://headers.example/events?since=1&since=2").unwrap();
    assert!(events::parse_cursor(&duplicate, None).is_err());
}

#[tokio::test]
async fn all_six_kinds_round_trip_and_future_cursors_are_errors() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h).await;
    let page = events::read_page(&db, 0, 100).await.unwrap();
    let mut envelopes = page
        .events
        .iter()
        .map(|e| events::decode(&e.payload).unwrap())
        .collect::<Vec<_>>();
    envelopes.push(Envelope {
        v: 1,
        event: ChainEvent::Invalidated {
            block_hash: h[1].hash.clone(),
        },
    });
    envelopes.push(Envelope {
        v: 1,
        event: ChainEvent::Frozen {
            outpoint: Outpoint {
                txid: block_hash("synthetic txid"),
                vout: 2,
            },
        },
    });
    for envelope in envelopes {
        assert_eq!(
            events::decode(&serde_json::to_string(&envelope).unwrap()).unwrap(),
            envelope
        );
    }
    assert!(events::read_page(&db, page.cursor + 1, 100)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("cursorAhead"));
}

#[tokio::test]
async fn cron_emits_age_during_source_outage_and_clamps_future_tip_time() {
    let db = SqliteDb::migrated();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    let root = header_at(
        100,
        "future timestamp",
        &block_hash("future parent"),
        EQUAL_WORK_BITS,
        now + 120,
    );
    let params = regtest()
        .clone()
        .with_checkpoints(&format!("100:{}", root.hash))
        .unwrap();
    seed(&db, &params, &[root]).await;
    let before = events::head(&db).await.unwrap();
    // Age the dedup key to model another minute without waiting on a clock.
    db.conn()
        .execute(
            "UPDATE chain_events SET event_key = 'previous-minute' WHERE event_key IS NOT NULL",
            [],
        )
        .unwrap();
    let source = ScriptedChain::new();
    source.set_unavailable(true);
    sync::run_cron(&db, &params, &source, None, &RecordedWebhooks::new("", ""))
        .await
        .unwrap();
    assert_eq!(source.calls(), 1);
    let page = events::read_page(&db, before, 100).await.unwrap();
    assert_eq!(page.events.len(), 1);
    assert!(matches!(
        events::decode(&page.events[0].payload).unwrap().event,
        ChainEvent::TipAge { seconds: 0, .. }
    ));
    events::timer(&db).await.unwrap();
    assert_eq!(events::head(&db).await.unwrap(), page.cursor);
}

#[tokio::test]
async fn revalidation_gates_the_journal_and_the_legacy_webhook() {
    let (_, params, h) = fixture();
    let db = SqliteDb::migrated_through("0008");
    seed(&db, &params, &h[..2]).await;
    let a2 = header(102, "A2", &h[1].hash);
    seed(&db, &params, &[a2.clone()]).await;
    db.conn()
        .execute(storage::SQL_RESTART_VALIDATION, [])
        .unwrap();
    db.apply_migrations_after("0008");
    assert_eq!(events::head(&db).await.unwrap(), 0);
    let hooks = RecordedWebhooks::new("https://consumer.example/tip", "synthetic-bearer");
    sync::announce_tip(&db, &hooks, Some(a2.clone())).await;
    assert!(hooks.take().is_empty());
    storage::revalidate_step(&db, &params, 1).await.unwrap();
    sync::announce_tip(&db, &hooks, Some(a2.clone())).await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&hooks.take()[0].body).unwrap()["height"],
        101
    );
    let page = events::read_page(&db, 0, 100).await.unwrap();
    for e in &page.events {
        match events::decode(&e.payload).unwrap().event {
            ChainEvent::Tip { height, .. } => assert!(height <= 101),
            ChainEvent::TipAge { tip, .. } => assert!(tip.height <= 101),
            other => panic!("unexpected event: {other:?}"),
        }
    }
    storage::revalidate_step(&db, &params, 1).await.unwrap();
    assert!(events::read_page(&db, page.cursor, 100)
        .await
        .unwrap()
        .events
        .iter()
        .any(|e| matches!(
            events::decode(&e.payload).unwrap().event,
            ChainEvent::Tip { height: 102, .. }
        )));
}

#[tokio::test]
async fn bulk_and_operator_replacements_emit_the_same_reorg_view() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h[..2]).await;
    let before = events::head(&db).await.unwrap();
    storage::insert_headers_batch(&db, &params, &h[2..])
        .await
        .unwrap();
    storage::update_chain_tip_to_highest(&db).await.unwrap();
    let page = events::read_page(&db, before, 100).await.unwrap();
    assert!(page.events.iter().any(|e| matches!(
        events::decode(&e.payload).unwrap().event,
        ChainEvent::Reorg { depth: 1, .. }
    )));
    // An operator replaces a below-tip ancestor with a valid tied branch.
    // Its old descendants must be disconnected as well.
    storage::canonicalize_heights(&db, &h[1..2]).await.unwrap();
    assert_eq!(
        storage::served_tip(&db).await.unwrap().unwrap().hash,
        h[1].hash
    );
    assert!(storage::find_active_header_for_hash(&db, &h[3].hash)
        .await
        .unwrap()
        .is_none());
    let after = events::read_page(&db, page.cursor, 100).await.unwrap();
    assert!(after.events.iter().any(|e| matches!(
        events::decode(&e.payload).unwrap().event,
        ChainEvent::Reorg { depth: 2, .. }
    )));
}

#[tokio::test]
async fn bulk_extending_an_inactive_parent_emits_the_reorg() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h[..3]).await;
    let before = events::head(&db).await.unwrap();
    storage::insert_headers_batch(&db, &params, &h[3..])
        .await
        .unwrap();
    storage::update_chain_tip_to_highest(&db).await.unwrap();
    let page = events::read_page(&db, before, 100).await.unwrap();
    assert!(page.events.iter().any(|e| matches!(
        events::decode(&e.payload).unwrap().event,
        ChainEvent::Reorg { depth: 1, .. }
    )));
    assert_eq!(
        storage::check_root_for_height(&db, &h[1].merkle_root, 101)
            .await
            .unwrap(),
        Some(false)
    );
}

#[tokio::test]
async fn a_journal_fault_rolls_back_the_whole_deep_reorg() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h[..1]).await;
    let mut a = h[0].clone();
    let mut b = h[0].clone();
    for height in 101..=220 {
        a = header(height, &format!("old {height}"), &a.hash);
        storage::insert_header(&db, &params, &a).await.unwrap();
    }
    for height in 101..=220 {
        b = header(height, &format!("new {height}"), &b.hash);
        storage::insert_header(&db, &params, &b).await.unwrap();
    }
    let winner = header(221, "new winning tip", &b.hash);
    db.conn().execute_batch("CREATE TRIGGER fail_reorg_journal BEFORE INSERT ON chain_events WHEN json_extract(NEW.payload, '$.kind') = 'reorg' BEGIN SELECT RAISE(ABORT, 'injected journal fault'); END;").unwrap();
    let error = storage::insert_header(&db, &params, &winner)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected journal fault"));
    assert_eq!(
        storage::served_tip(&db).await.unwrap().unwrap().hash,
        a.hash
    );
    assert!(
        !storage::find_header_for_hash(&db, &winner.hash)
            .await
            .unwrap()
            .unwrap()
            .is_active
    );
    db.conn()
        .execute_batch("DROP TRIGGER fail_reorg_journal;")
        .unwrap();
    storage::relink_orphan_and_reevaluate(&db, &params, &winner.hash)
        .await
        .unwrap();
    let payload: String = db.conn().query_row("SELECT payload FROM chain_events WHERE json_extract(payload, '$.kind') = 'reorg' ORDER BY cursor DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    assert!(matches!(
        events::decode(&payload).unwrap().event,
        ChainEvent::Reorg { depth: 120, .. }
    ));
    assert_eq!(
        storage::served_tip(&db).await.unwrap().unwrap().hash,
        winner.hash
    );
}

#[tokio::test]
async fn a_journal_fault_rolls_back_a_live_or_read_through_extension() {
    let (db, params, h) = fixture();
    seed(&db, &params, &h[..1]).await;
    let before = events::head(&db).await.unwrap();
    db.conn().execute_batch("CREATE TRIGGER fail_tip_journal BEFORE INSERT ON chain_events WHEN json_extract(NEW.payload, '$.kind') = 'tip' BEGIN SELECT RAISE(ABORT, 'injected tip journal fault'); END;").unwrap();
    assert!(storage::insert_header(&db, &params, &h[1])
        .await
        .unwrap_err()
        .to_string()
        .contains("injected tip journal fault"));
    assert!(storage::find_header_for_hash(&db, &h[1].hash)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        storage::served_tip(&db).await.unwrap().unwrap().hash,
        h[0].hash
    );
    assert_eq!(events::head(&db).await.unwrap(), before);
}
