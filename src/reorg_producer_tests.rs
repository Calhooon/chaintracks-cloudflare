//! The reorg producer driven through its REAL code path on the host (a private program
//! M19B-G2, 2026-09-08): `storage::insert_header` (and the `handle_reorg` it
//! runs) then `sync::notify_if_tip_advanced`, and since round 4 the cron
//! itself (`sync::run_cron` on a scripted chain), on a real SQLite carrying
//! the real migrations, with the webhook transport recorded. Before this the
//! producer was pinned at the statement tier only (each SQL constant executed
//! by hand) and the round-3 gate accepted that as a pre-existing gap (plan doc
//! R2 round 3, MED-1).
//!
//! The shape under test is the 2026-09-07 reorg. chaintracks activates a
//! branch only on MORE cumulative work, so the canonical 58-tx block at
//! 965771 (equal work to the 34 MB block already active there) landed
//! INACTIVE, and the tip flipped only when its child at 965772 arrived. The
//! announce for that child has to carry `reorgFrom: 965771`, or the overlay
//! reads a plain extension and never re-verifies 965771.
//!
//! Rounds 2 to 4: a new tip is CLAIMED atomically before the POST (one
//! isolate announces), each target's delivery is recorded on its own with
//! the fork its body carried, and a refused, failed or partial delivery
//! consumes nothing: the claim stays, the owed targets stay owed, and once
//! the claim's in-flight window has passed the next announce (every cron,
//! idle or not) re-claims and re-sends the same body to the owed targets
//! alone. A fork rides to each target until that target has accepted a body
//! carrying it. The failure run is counted and every `ANNOUNCE_STUCK_AFTER`
//! of them is a stuck event. On the host no time passes between two crons,
//! so the tests age the claim with `age_claim` where a real cron would be a
//! minute later.
//!
//! Each test names, in its doc, the one line that reds it.

use crate::d1::{HeaderDb, Query};
use crate::host_harness::{
    age_claim, announce_counters, announced, block_hash, deliveries, flags, freshness, header,
    header_at, header_id, heavy_root, min_difficulty_rule, pending, regtest, set_freshness, Handed,
    RecordedWebhooks, Recorder, ScriptedChain, SqliteDb, EQUAL_WORK_BITS, HEAVY_BITS,
};
use crate::statement_pins::{
    D5_COURIER_HEALTH, MAIN_ACTIVATE_HASH, MAIN_CHAIN_TIP, MAIN_CLEAR_CHAIN_TIP,
    MAIN_CLEAR_PENDING_REORG, MAIN_COUNT_ACTIVE_ABOVE, MAIN_DEACTIVATE_ABOVE, MAIN_HEADER_FOR_HASH,
    MAIN_HEADER_FOR_ID, MAIN_INSERT_HEADER, MAIN_READ_PENDING_REORG, MAIN_RECORD_PENDING_REORG,
    MAIN_SET_CHAIN_TIP_ACTIVE, MAIN_VOCABULARY, P04_VALIDATION_STATE, ROUND2_ANNOUNCED_TIP,
    ROUND2_COUNT_ANNOUNCE_STUCK, ROUND2_SET_ANNOUNCE_FAILURES, ROUND2_VOCABULARY,
    ROUND3_READ_DELIVERIES, ROUND3_RECLAIM_ANNOUNCE, ROUND3_RECORD_DELIVERY, ROUND3_VOCABULARY,
    ROUND4_TIP_ANNOUNCE, ROUND4_VOCABULARY,
};
use crate::storage::{find_chain_tip, find_header_for_height, get_info, insert_header};
use crate::sync::{
    notify_if_tip_advanced, run_cron, WebhookDelivery, WebhookTarget, ANNOUNCE_STUCK_AFTER,
    CLAIM_IN_FLIGHT_S, READ_PENDING_REORG_SQL, TIP_ANNOUNCE_SQL,
};
use crate::types::{add_work, calculate_work, BlockHeader, Chain};

/// The deployed `TIP_WEBHOOK_URLS` (wrangler.toml): the beta app-layer over
/// its service binding.
const BETA_HOOK: &str =
    "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed";
const BETA_URL: &str = "https://your-worker.your-account.workers.dev/internal/tip-changed";
/// A second consumer on another zone, reached by a public fetch (the shape
/// the prod app-layer takes at promotion).
const PUBLIC_URL: &str = "https://consumer.example/tip-changed";
const TWO_HOOKS: &str = "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed, https://consumer.example/tip-changed";
const BEARER: &str = "tip-webhook-bearer";

/// The incident height.
const H: u32 = 965_771;

/// The blocks of the incident.
struct Incident {
    /// H-2, the bootstrap row (its parent is not in the store).
    x: BlockHeader,
    /// H-1.
    y: BlockHeader,
    /// H, the 34 MB block: first seen, active, later orphaned.
    a: BlockHeader,
    /// H, the canonical 58-tx block: equal work, lands inactive.
    a2: BlockHeader,
    /// H+1, the child of a2: flips the tip.
    b2: BlockHeader,
}

fn incident() -> Incident {
    let x = header(H - 2, "x", &block_hash("the block below x"));
    let y = header(H - 1, "y", &x.hash);
    let a = header(H, "a: the 34 MB block", &y.hash);
    let a2 = header(H, "a prime: the 58-tx block", &y.hash);
    let b2 = header(H + 1, "b prime: the child of a prime", &a2.hash);
    Incident { x, y, a, a2, b2 }
}

/// The deeper competitor: C at H-1 off X, D at H, E at H+1 (each ties its
/// height and lands inactive), F at H+2 (outworks B', flips the tip, forks at
/// H-1).
fn deeper_branch(inc: &Incident) -> Vec<BlockHeader> {
    let c = header(H - 1, "c", &inc.x.hash);
    let d = header(H, "d", &c.hash);
    let e = header(H + 1, "e", &d.hash);
    let f = header(H + 2, "f", &e.hash);
    vec![c, d, e, f]
}

fn plain(height: u32, hash: &str) -> String {
    format!(r#"{{"height":{height},"hash":"{hash}"}}"#)
}

fn with_fork(height: u32, hash: &str, fork: u32) -> String {
    format!(r#"{{"height":{height},"hash":"{hash}","reorgFrom":{fork}}}"#)
}

fn bodies(hooks: &RecordedWebhooks) -> Vec<String> {
    hooks.take().into_iter().map(|p| p.body).collect()
}

fn refused() -> WebhookDelivery {
    WebhookDelivery::Refused {
        status: 404,
        server: "cloudflare".to_string(),
        excerpt: "<!DOCTYPE html>".to_string(),
    }
}

/// A row of `deliveries`.
fn delivered(
    url: &str,
    height: u32,
    hash: &str,
    fork: Option<u32>,
) -> (String, i64, String, Option<i64>) {
    (
        url.to_string(),
        i64::from(height),
        hash.to_string(),
        fork.map(i64::from),
    )
}

/// A cron a minute later: the claim's in-flight window has passed.
fn next_cron(db: &SqliteDb) {
    age_claim(db, 2);
}

/// The chain as the incident found it: X, Y, A inserted and each tip
/// announced through the real path, so the announce state is the live one.
async fn orphan_active(db: &impl HeaderDb, hooks: &RecordedWebhooks, inc: &Incident) {
    for h in [&inc.x, &inc.y, &inc.a] {
        let r = insert_header(db, regtest(), h).await.unwrap();
        assert!(r.added && r.is_active_tip && r.reorg_depth == 0, "{r:?}");
        notify_if_tip_advanced(db, hooks).await.unwrap();
    }
    assert_eq!(
        bodies(hooks),
        vec![
            plain(H - 2, &inc.x.hash),
            plain(H - 1, &inc.y.hash),
            plain(H, &inc.a.hash)
        ]
    );
}

/// `orphan_active`, then A' and B' inserted (the fork at H recorded), NOT yet announced.
async fn incident_reorged(db: &impl HeaderDb, hooks: &RecordedWebhooks, inc: &Incident) {
    orphan_active(db, hooks, inc).await;
    insert_header(db, regtest(), &inc.a2).await.unwrap();
    insert_header(db, regtest(), &inc.b2).await.unwrap();
}

/// `incident_reorged`, then B' announced: the incident fully played and heard.
async fn incident_announced(db: &impl HeaderDb, hooks: &RecordedWebhooks, inc: &Incident) {
    incident_reorged(db, hooks, inc).await;
    notify_if_tip_advanced(db, hooks).await.unwrap();
    assert_eq!(bodies(hooks), vec![with_fork(H + 1, &inc.b2.hash, H)]);
}

// ─── (1) The 2026-09-07 shape ───────────────────────────────────────────────

/// Chain at H-1; the orphan A at H becomes active; the canonical A' at H
/// arrives with EQUAL work and lands inactive (no announce, no fork); B' at
/// H+1 arrives, the tip flips to B', `handle_reorg` records the fork at H, and
/// the next announce carries `{height: H+1, hash: B', reorgFrom: H}` over the
/// service binding, records the delivery with the fork it carried, and clears
/// the fork. To red: `handle_reorg`, delete the
/// `batch.add(RECORD_PENDING_REORG_SQL, ...)` line (the announce loses its
/// `reorgFrom` and the overlay reads a plain extension).
#[tokio::test]
async fn an_equal_work_sibling_lands_inactive_and_its_child_announces_the_fork() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    orphan_active(&db, &hooks, &inc).await;

    // A' (equal work) lands INACTIVE: the tip stays A, nothing is announced,
    // no fork is recorded. This is exactly why the child has to carry it.
    let r = insert_header(&db, regtest(), &inc.a2).await.unwrap();
    assert!(r.added && !r.is_active_tip && r.reorg_depth == 0, "{r:?}");
    assert_eq!(flags(&db, &inc.a2.hash), (false, false));
    assert_eq!(flags(&db, &inc.a.hash), (true, true));
    assert_eq!(
        find_header_for_height(&db, H).await.unwrap().unwrap().hash,
        inc.a.hash
    );
    assert_eq!(pending(&db), None);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(
        hooks.take().is_empty(),
        "an inactive sibling is not a tip change"
    );

    // B' flips the tip: handle_reorg orphans A, activates A' and B', records the fork at H.
    let r = insert_header(&db, regtest(), &inc.b2).await.unwrap();
    assert!(r.added && r.is_active_tip, "{r:?}");
    assert_eq!(r.reorg_depth, 1, "one header orphaned: A");
    assert_eq!(pending(&db), Some(i64::from(H)));
    assert_eq!(flags(&db, &inc.a.hash), (false, false));
    assert_eq!(flags(&db, &inc.a2.hash), (true, false));
    assert_eq!(flags(&db, &inc.b2.hash), (true, true));
    assert_eq!(flags(&db, &inc.y.hash), (true, false));
    assert_eq!(
        find_header_for_height(&db, H).await.unwrap().unwrap().hash,
        inc.a2.hash,
        "the served header at H flipped"
    );
    assert_eq!(
        find_chain_tip(&db).await.unwrap().unwrap().hash,
        inc.b2.hash
    );

    // The announce carries the fork; accepted, it records the delivery (with
    // the fork) and clears.
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0].target,
        WebhookTarget {
            binding: Some("APP_LAYER_BETA".to_string()),
            url: BETA_URL.to_string()
        }
    );
    assert_eq!(posts[0].bearer, BEARER);
    assert_eq!(posts[0].body, with_fork(H + 1, &inc.b2.hash, H));
    assert_eq!(
        pending(&db),
        None,
        "carried and accepted by every target: cleared"
    );
    assert_eq!(
        announced(&db),
        (i64::from(H + 1), Some(inc.b2.hash.clone()))
    );
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H))]
    );
    assert_eq!(announce_counters(&db), (0, 0));

    // The same tip again (the cron after a read-through): silent.
    next_cron(&db);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty());
}

/// The same shape through the REAL CRON on a scripted chain (round 4): the
/// store holds X, Y, A; cron 1 finds WoC at the same tip and is idle; cron 2
/// finds WoC switched to A' at the same height (the audit-C2 competitor
/// path), fetches it by hash, lands it INACTIVE and announces nothing; cron 3
/// finds WoC at H+1, the live path inserts B', the reorg runs and
/// `finish_sync` announces `{H+1, b', reorgFrom: H}`. To red:
/// `run_cron`, the `if woc_height <= our_height` guard to `<` (the competitor
/// path skipped: A' never lands, cron 3 backfills it and the trace differs).
#[tokio::test]
async fn the_cron_plays_the_2026_09_07_shape_on_a_scripted_chain() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let inc = incident();
    orphan_active(&db, &hooks, &inc).await;
    for h in [&inc.x, &inc.y, &inc.a] {
        chain.publish(h);
    }
    // cron 1: idle
    chain.tip(H, &inc.a.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert!(hooks.take().is_empty(), "idle: nothing to announce");
    // cron 2: WoC switched to A' at the same height (equal work)
    chain.publish(&inc.a2);
    chain.tip(H, &inc.a2.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        flags(&db, &inc.a2.hash),
        (false, false),
        "ingested by hash, inactive"
    );
    assert_eq!(
        flags(&db, &inc.a.hash),
        (true, true),
        "the tip did not flip"
    );
    assert!(hooks.take().is_empty(), "no tip change, no announce");
    assert_eq!(pending(&db), None);
    // cron 3: WoC at H+1 with B'
    chain.publish(&inc.b2);
    chain.tip(H + 1, &inc.b2.hash);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(bodies(&hooks), vec![with_fork(H + 1, &inc.b2.hash, H)]);
    assert_eq!(flags(&db, &inc.b2.hash), (true, true));
    assert_eq!(flags(&db, &inc.a.hash), (false, false));
    assert_eq!(pending(&db), None);
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H))]
    );
}

/// A HEAVIER competitor at the tip's height (WoC switched branches at our
/// height) flips the tip in the cron that ingests it, and round 4 announces
/// it in that same cron (the idle path announces too); before round 4 it
/// stayed unannounced until the next block or a read-through. To red:
/// `run_cron`, delete the `announce_tip(db, hooks).await;` before the idle
/// `return Ok(())`.
#[tokio::test]
async fn the_cron_announces_a_heavier_same_height_competitor_the_cron_it_ingests_it() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    // the node's minimum-difficulty rule (`min_difficulty_rule`): A, 21
    // minutes after Y, carries the limit's bits; its heavier sibling, a
    // minute after Y, carries Y's
    let rule = min_difficulty_rule();
    let x = heavy_root();
    let y = header_at(H - 1, "y, prompt", &x.hash, HEAVY_BITS, x.time + 60);
    let a = header_at(H, "a, late", &y.hash, EQUAL_WORK_BITS, y.time + 21 * 60);
    for h in [&x, &y, &a] {
        let r = insert_header(&db, rule, h).await.unwrap();
        assert!(r.added && r.is_active_tip && r.reorg_depth == 0, "{r:?}");
        notify_if_tip_advanced(&db, &hooks).await.unwrap();
        chain.publish(h);
    }
    assert_eq!(
        bodies(&hooks),
        vec![
            plain(H - 2, &x.hash),
            plain(H - 1, &y.hash),
            plain(H, &a.hash)
        ]
    );
    let heavy = header_at(H, "a heavy, prompt", &y.hash, HEAVY_BITS, y.time + 60);
    chain.publish(&heavy);
    chain.tip(H, &heavy.hash);
    run_cron(&db, rule, &chain, None, &hooks).await.unwrap();
    assert_eq!(flags(&db, &heavy.hash), (true, true), "more work: the tip");
    assert_eq!(flags(&db, &a.hash), (false, false), "A orphaned");
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H, &heavy.hash, H)],
        "announced by the cron that ingested it"
    );
    assert_eq!(pending(&db), None);
}

// ─── (2) MIN-accumulate and the CAS clear ───────────────────────────────────

/// Two reorgs before one announce: the second, deeper one (forking at H-1)
/// wins the record, a shallower third (forking at H+2) does not raise it, and
/// the announce carries H-1 once. To red: `RECORD_PENDING_REORG_SQL`,
/// `pending_reorg_from > ?1` to `pending_reorg_from < ?1`.
#[tokio::test]
async fn two_reorgs_before_one_announce_carry_the_deeper_fork() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&db, &hooks, &inc).await;
    assert_eq!(pending(&db), Some(i64::from(H)));

    let deeper = deeper_branch(&inc);
    for h in &deeper[..3] {
        let r = insert_header(&db, regtest(), h).await.unwrap();
        assert!(r.added && !r.is_active_tip, "{r:?}");
    }
    let r = insert_header(&db, regtest(), &deeper[3]).await.unwrap();
    assert!(r.is_active_tip, "{r:?}");
    assert_eq!(r.reorg_depth, 3, "Y, A prime, B prime");
    assert_eq!(
        pending(&db),
        Some(i64::from(H - 1)),
        "MIN-accumulated: the deeper fork"
    );

    let g = header(H + 2, "g: a sibling of f", &deeper[2].hash);
    let g2 = header(H + 3, "g two: the child of g", &g.hash);
    insert_header(&db, regtest(), &g).await.unwrap();
    let r = insert_header(&db, regtest(), &g2).await.unwrap();
    assert_eq!(r.reorg_depth, 1, "F");
    assert_eq!(
        pending(&db),
        Some(i64::from(H - 1)),
        "a shallower fork does not raise it"
    );

    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(bodies(&hooks), vec![with_fork(H + 3, &g2.hash, H - 1)]);
    assert_eq!(pending(&db), None);

    for h in [&inc.y, &inc.a, &inc.a2, &inc.b2, &deeper[3]] {
        assert_eq!(flags(&db, &h.hash), (false, false), "{}", h.hash);
    }
    for h in [&inc.x, &deeper[0], &deeper[1], &deeper[2], &g] {
        assert_eq!(flags(&db, &h.hash), (true, false), "{}", h.hash);
    }
    assert_eq!(flags(&db, &g2.hash), (true, true));
}

/// A deeper reorg landing through the REAL `insert_header` between the
/// announce's read of the pending fork and its clear (the POST sits in that
/// window): the clear is a CAS on the height read, so the deeper fork
/// survives and the NEXT announce carries it. To red:
/// `CLEAR_PENDING_REORG_SQL`, delete ` AND pending_reorg_from = ?1`.
#[tokio::test]
async fn a_deeper_fork_landing_between_the_read_and_the_clear_survives_to_the_next_announce() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&rec, &hooks, &inc).await;
    assert_eq!(pending(&db), Some(i64::from(H)));

    let deeper = deeper_branch(&inc);
    let f_hash = deeper[3].hash.clone();
    rec.after_first_of(READ_PENDING_REORG_SQL, move |db| async move {
        for h in &deeper {
            insert_header(db, regtest(), h).await.unwrap();
        }
    });
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "what was read is what was sent"
    );
    assert_eq!(
        pending(&db),
        Some(i64::from(H - 1)),
        "the deeper fork survived the clear"
    );
    assert!(
        rec.handed().contains(&Handed::Execute(
            Query::new(MAIN_CLEAR_PENDING_REORG).bind(i64::from(H))
        )),
        "the clear ran on the height read, and no-oped"
    );

    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    assert_eq!(bodies(&hooks), vec![with_fork(H + 2, &f_hash, H - 1)]);
    assert_eq!(pending(&db), None);
}

// ─── (3) A plain extension ──────────────────────────────────────────────────

/// A header extending the tip announces `{height, hash}` and nothing else. To
/// red: `tip_webhook_body`, the `None` arm to the `Some` arm's shape.
#[tokio::test]
async fn a_plain_extension_announces_height_and_hash_only() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_announced(&db, &hooks, &inc).await;
    let c = header(H + 2, "c: a plain extension of b prime", &inc.b2.hash);
    let r = insert_header(&db, regtest(), &c).await.unwrap();
    assert!(r.added && r.is_active_tip && r.reorg_depth == 0, "{r:?}");
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].body, plain(H + 2, &c.hash));
    let v: serde_json::Value = serde_json::from_str(&posts[0].body).unwrap();
    let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
    assert_eq!(keys, vec!["hash", "height"], "no reorgFrom key at all");
    assert_eq!(pending(&db), None);
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 2, &c.hash, Some(H))],
        "the fork heard stays recorded (COALESCE)"
    );
}

// ─── (4) Suppressed announces ───────────────────────────────────────────────

/// The same tip again, even after the claim's window, is three reads and
/// nothing else (round 4, LOW-2: a fully delivered tip re-claims nothing);
/// a tip BELOW the claimed height (the row moved ahead by another isolate or
/// an operator repair) is silent too: nothing is posted, nothing is written,
/// the height is never moved backwards, and the pending fork is neither read
/// nor cleared. To red: `notify_if_tip_advanced`, `claim_is_tip &&
/// !owed.is_empty()` to `claim_is_tip` (a retry claim on a delivered tip), or
/// the `else { false }` arm to `true`.
#[tokio::test]
async fn a_repeated_or_stale_tip_never_announces_and_consumes_nothing() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    orphan_active(&rec, &hooks, &inc).await;
    let reads = vec![
        Handed::First(Query::new(MAIN_CHAIN_TIP)),
        Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
        Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
    ];
    next_cron(&db);
    let before = rec.handed().len();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "the same tip again");
    assert_eq!(
        rec.handed()[before..].to_vec(),
        reads,
        "delivered to every target: no retry claim, no write"
    );

    insert_header(&rec, regtest(), &inc.a2).await.unwrap();
    insert_header(&rec, regtest(), &inc.b2).await.unwrap();
    assert_eq!(pending(&db), Some(i64::from(H)));
    db.conn()
        .execute(
            "UPDATE sync_state SET last_synced_height = ?1, last_announced_hash = 'ahead' WHERE id = 1",
            [i64::from(H + 5)],
        )
        .unwrap();
    let before = rec.handed().len();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "a lower height never fires");
    assert_eq!(
        announced(&db),
        (i64::from(H + 5), Some("ahead".to_string())),
        "never written backwards"
    );
    assert_eq!(pending(&db), Some(i64::from(H)), "not consumed");
    assert_eq!(
        rec.handed()[before..].to_vec(),
        reads,
        "three reads, no claim, no fork read, no write at all"
    );
}

// ─── (5) The transport ──────────────────────────────────────────────────────

/// The announce reaches EVERY configured target, in order, each with the
/// trimmed bearer and the same body: the Worker on this account over its
/// service binding (Cloudflare refuses a plain fetch between two Workers on
/// one zone, error 1042), a consumer on another zone over a public fetch. To
/// red: `notify_tip_webhooks`, `for (target, carry) in owed` to
/// `for (target, carry) in owed.iter().take(1)`.
#[tokio::test]
async fn the_announce_reaches_every_configured_target_with_the_bearer_over_its_binding() {
    let db = SqliteDb::migrated();
    let inc = incident();
    incident_announced(&db, &RecordedWebhooks::new(BETA_HOOK, BEARER), &inc).await;
    let hooks = RecordedWebhooks::new(TWO_HOOKS, "  tip-webhook-bearer  ");
    let c = header(H + 2, "c", &inc.b2.hash);
    insert_header(&db, regtest(), &c).await.unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    let targets: Vec<WebhookTarget> = posts.iter().map(|p| p.target.clone()).collect();
    assert_eq!(
        targets,
        vec![
            WebhookTarget {
                binding: Some("APP_LAYER_BETA".to_string()),
                url: BETA_URL.to_string()
            },
            WebhookTarget {
                binding: None,
                url: PUBLIC_URL.to_string()
            },
        ]
    );
    for p in &posts {
        assert_eq!(p.bearer, "tip-webhook-bearer", "trimmed");
        assert_eq!(p.body, plain(H + 2, &c.hash));
    }
    assert_eq!(announced(&db), (i64::from(H + 2), Some(c.hash.clone())));
    assert_eq!(
        deliveries(&db),
        vec![
            delivered(PUBLIC_URL, H + 2, &c.hash, None),
            delivered(BETA_URL, H + 2, &c.hash, Some(H)),
        ]
    );
}

/// No bearer: nothing is sent (loudly logged) and NOTHING is consumed: the
/// claim is made (so no other isolate re-tries inside the window), the marker
/// stays, the target stays owed, the failure run counts it, and the announce
/// after the secret lands delivers the same body. To red:
/// `notify_tip_webhooks`, `round.failed = owed.len()` in the empty-bearer
/// guard deleted.
#[tokio::test]
async fn a_missing_bearer_sends_nothing_and_consumes_nothing() {
    let db = SqliteDb::migrated();
    let inc = incident();
    incident_reorged(&db, &RecordedWebhooks::new(BETA_HOOK, BEARER), &inc).await;
    let unset = RecordedWebhooks::new(BETA_HOOK, "   ");
    notify_if_tip_advanced(&db, &unset).await.unwrap();
    assert!(unset.take().is_empty(), "no bearer, no post");
    assert_eq!(
        announced(&db),
        (i64::from(H + 1), Some(inc.b2.hash.clone())),
        "the claim"
    );
    assert_eq!(pending(&db), Some(i64::from(H)), "the marker stayed");
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H, &inc.a.hash, None)]
    );
    assert_eq!(announce_counters(&db), (1, 0));
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    next_cron(&db);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(bodies(&hooks), vec![with_fork(H + 1, &inc.b2.hash, H)]);
    assert_eq!(pending(&db), None);
    assert_eq!(announce_counters(&db), (0, 0));
}

/// A refused delivery (an edge 404, the consumer's own refusal) keeps the
/// claim and the target owed, never fails the sync, and consumes nothing:
/// inside the claim's in-flight window a second announce stays silent (the
/// burst dedup), the announce after the window re-claims and re-sends the
/// same `{height, hash, reorgFrom}`; once accepted the delivery is recorded,
/// the marker consumed, the failure run reset, and the tip is silent from
/// then on. To red: `notify_tip_webhooks`, `round.failed += 1` in the
/// `Refused` arm deleted (a refusal reads as delivered).
#[tokio::test]
async fn a_refused_delivery_keeps_the_claim_owed_and_the_next_announce_re_sends_the_same_body() {
    let db = SqliteDb::migrated();
    let inc = incident();
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    incident_reorged(&db, &hooks, &inc).await;
    hooks.reply = refused();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "posted, refused"
    );
    assert_eq!(pending(&db), Some(i64::from(H)), "the marker stayed");
    assert_eq!(
        announced(&db),
        (i64::from(H + 1), Some(inc.b2.hash.clone())),
        "the claim stayed"
    );
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H, &inc.a.hash, None)],
        "no delivery recorded"
    );
    assert_eq!(announce_counters(&db), (1, 0));

    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(
        hooks.take().is_empty(),
        "inside the in-flight window: silent"
    );
    assert_eq!(announce_counters(&db), (1, 0));

    next_cron(&db);
    hooks.reply = WebhookDelivery::Failed("service binding APP_LAYER_BETA unavailable".to_string());
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "the same body again"
    );
    assert_eq!(pending(&db), Some(i64::from(H)));
    assert_eq!(announce_counters(&db), (2, 0));

    next_cron(&db);
    hooks.reply = WebhookDelivery::Accepted(200);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "the same body, accepted"
    );
    assert_eq!(pending(&db), None, "consumed on delivery");
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H))]
    );
    assert_eq!(announce_counters(&db), (0, 0), "the failure run reset");
    next_cron(&db);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "delivered once, silent after");
}

/// MED-2: one dead target never re-delivers to a healthy one. Two targets,
/// the public one refuses: both are posted, the healthy one's delivery is
/// recorded and it is never posted this tip again; every later announce
/// re-sends the same body to the owed target ALONE until it accepts; then the
/// marker is consumed and the run reset. To red: `notify_if_tip_advanced`,
/// the `owed` filter to every target (every target, every time).
#[tokio::test]
async fn one_dead_target_is_retried_alone_and_never_re_delivers_to_a_healthy_one() {
    let db = SqliteDb::migrated();
    let inc = incident();
    incident_reorged(&db, &RecordedWebhooks::new(BETA_HOOK, BEARER), &inc).await;
    let hooks = RecordedWebhooks::new(TWO_HOOKS, BEARER);
    hooks.reply_for(PUBLIC_URL, refused());
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(
        posts
            .iter()
            .map(|p| p.target.url.clone())
            .collect::<Vec<_>>(),
        vec![BETA_URL.to_string(), PUBLIC_URL.to_string()],
        "both owed, both posted"
    );
    assert!(posts
        .iter()
        .all(|p| p.body == with_fork(H + 1, &inc.b2.hash, H)));
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H))],
        "the healthy one recorded"
    );
    assert_eq!(
        pending(&db),
        Some(i64::from(H)),
        "the marker stayed: the dead target has not heard it"
    );
    assert_eq!(announce_counters(&db), (1, 0));

    for cron in 2..=3 {
        next_cron(&db);
        notify_if_tip_advanced(&db, &hooks).await.unwrap();
        let posts = hooks.take();
        assert_eq!(
            posts
                .iter()
                .map(|p| p.target.url.clone())
                .collect::<Vec<_>>(),
            vec![PUBLIC_URL.to_string()],
            "cron {cron}: the owed target alone, the healthy one never re-delivered"
        );
        assert_eq!(posts[0].body, with_fork(H + 1, &inc.b2.hash, H));
        assert_eq!(announce_counters(&db), (cron, 0));
    }

    next_cron(&db);
    hooks.reply_for(PUBLIC_URL, WebhookDelivery::Accepted(200));
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(posts.len(), 1, "the owed target alone");
    assert_eq!(posts[0].target.url, PUBLIC_URL);
    assert_eq!(pending(&db), None, "every target has heard it: consumed");
    assert_eq!(
        deliveries(&db),
        vec![
            delivered(PUBLIC_URL, H + 1, &inc.b2.hash, Some(H)),
            delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H)),
        ]
    );
    assert_eq!(announce_counters(&db), (0, 0));
    next_cron(&db);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty());
}

/// LOW-3 (round 4): a pending fork rides to EACH target until THAT target
/// has accepted a body carrying it. While the public target is dead, the
/// next block's announce carries the fork to the public target only; the
/// healthy target, which already heard it, gets a plain body and is not made
/// to re-verify on every block; the marker clears when the dead target
/// accepts. To red: `notify_if_tip_advanced`, `reorg_from.filter(|v|
/// heard_before(&t.url) != Some(*v))` to `reorg_from` (every owed target
/// carries it again).
#[tokio::test]
async fn a_fork_rides_to_each_target_until_that_target_has_accepted_it() {
    let db = SqliteDb::migrated();
    let inc = incident();
    incident_reorged(&db, &RecordedWebhooks::new(BETA_HOOK, BEARER), &inc).await;
    let hooks = RecordedWebhooks::new(TWO_HOOKS, BEARER);
    hooks.reply_for(PUBLIC_URL, refused());
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![
            with_fork(H + 1, &inc.b2.hash, H),
            with_fork(H + 1, &inc.b2.hash, H)
        ],
        "both carry the fork the first time"
    );
    assert_eq!(pending(&db), Some(i64::from(H)));

    // the next block, the public target still dead
    let c = header(H + 2, "c", &inc.b2.hash);
    insert_header(&db, regtest(), &c).await.unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(posts.len(), 2, "a new tip: both owed");
    assert_eq!(posts[0].target.url, BETA_URL);
    assert_eq!(
        posts[0].body,
        plain(H + 2, &c.hash),
        "the healthy target already heard the fork: a plain body"
    );
    assert_eq!(posts[1].target.url, PUBLIC_URL);
    assert_eq!(
        posts[1].body,
        with_fork(H + 2, &c.hash, H),
        "the dead target is still owed the fork"
    );
    assert_eq!(
        pending(&db),
        Some(i64::from(H)),
        "not every target has heard it"
    );
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 2, &c.hash, Some(H))],
        "the fork heard is kept on a plain delivery"
    );

    // the public target recovers
    next_cron(&db);
    hooks.reply_for(PUBLIC_URL, WebhookDelivery::Accepted(200));
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 2, &c.hash, H)],
        "the owed target alone, with the fork"
    );
    assert_eq!(
        pending(&db),
        None,
        "every target has heard the fork: cleared"
    );
    assert_eq!(
        deliveries(&db),
        vec![
            delivered(PUBLIC_URL, H + 2, &c.hash, Some(H)),
            delivered(BETA_URL, H + 2, &c.hash, Some(H)),
        ]
    );
    assert_eq!(announce_counters(&db), (0, 0));
}

/// MED-2, the promotion shape: a target added to `TIP_WEBHOOK_URLS` later is
/// owed the current tip and receives it alone (after the claim's window);
/// the target that already has it is not posted again; the next block goes to
/// both. To red: `notify_if_tip_advanced`, `claim_is_tip && !owed.is_empty()`
/// to `false` (an added target waits for the next block).
#[tokio::test]
async fn a_target_added_later_is_owed_the_current_tip_and_gets_it_alone() {
    let db = SqliteDb::migrated();
    let inc = incident();
    incident_announced(&db, &RecordedWebhooks::new(BETA_HOOK, BEARER), &inc).await;
    let hooks = RecordedWebhooks::new(TWO_HOOKS, BEARER);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "inside the claim's window: silent");
    next_cron(&db);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].target.url, PUBLIC_URL, "the added target alone");
    assert_eq!(
        posts[0].body,
        plain(H + 1, &inc.b2.hash),
        "the current tip; the fork was consumed when every target of the time had heard it"
    );
    assert_eq!(announce_counters(&db), (0, 0));
    let c = header(H + 2, "c", &inc.b2.hash);
    insert_header(&db, regtest(), &c).await.unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(hooks.take().len(), 2, "the next block goes to both");
}

/// A dead consumer is visible, never silent: every `ANNOUNCE_STUCK_AFTER`
/// consecutive undelivered announces count one stuck event on
/// `tip_announce_stuck_total` (with a loud log line), the claim and the
/// marker still stay, the same body is re-sent every announce after the
/// window, `/getInfo` shows the run and the count, and a delivery resets the
/// run but never the lifetime count. To red: `notify_if_tip_advanced`,
/// `failures % ANNOUNCE_STUCK_AFTER == 0` to `failures == u32::MAX`.
#[tokio::test]
async fn the_stuck_counter_fires_after_n_consecutive_undelivered_announces() {
    let db = SqliteDb::migrated();
    let inc = incident();
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    incident_reorged(&db, &hooks, &inc).await;
    hooks.reply = WebhookDelivery::Failed("connection refused".to_string());
    for cron in 1..=ANNOUNCE_STUCK_AFTER * 2 {
        next_cron(&db);
        notify_if_tip_advanced(&db, &hooks).await.unwrap();
        assert_eq!(
            bodies(&hooks),
            vec![with_fork(H + 1, &inc.b2.hash, H)],
            "cron {cron}"
        );
        assert_eq!(pending(&db), Some(i64::from(H)), "cron {cron}");
        assert_eq!(
            announced(&db),
            (i64::from(H + 1), Some(inc.b2.hash.clone())),
            "cron {cron}: the claim"
        );
        let stuck = i64::from(cron / ANNOUNCE_STUCK_AFTER);
        assert_eq!(
            announce_counters(&db),
            (i64::from(cron), stuck),
            "cron {cron}"
        );
    }
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.tip_announce_failures, Some(ANNOUNCE_STUCK_AFTER * 2));
    assert_eq!(info.tip_announce_stuck_total, Some(2));
    assert_eq!(info.last_synced_height, Some(H + 1), "the claim");

    next_cron(&db);
    hooks.reply = WebhookDelivery::Accepted(200);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(bodies(&hooks), vec![with_fork(H + 1, &inc.b2.hash, H)]);
    assert_eq!(pending(&db), None);
    assert_eq!(
        announce_counters(&db),
        (0, 2),
        "the run reset, the lifetime count kept"
    );
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(
        (info.tip_announce_failures, info.tip_announce_stuck_total),
        (Some(0), Some(2))
    );
}

// ─── (6) The idle cron (round 4, MED-1) ─────────────────────────────────────

/// An IDLE cron (WoC not above us) after a refused delivery retries it: the
/// cron inside the window is silent, the cron after it re-claims and re-sends
/// the same body, and a cron that cannot reach WoC at all still announces. The
/// REAL cron body on a scripted chain. To red: `run_cron`, delete the
/// `announce_tip(db, hooks).await;` before the idle `return Ok(())`.
#[tokio::test]
async fn an_idle_cron_after_a_refused_delivery_re_announces_the_same_body() {
    let db = SqliteDb::migrated();
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let inc = incident();
    incident_reorged(&db, &hooks, &inc).await;
    chain.tip(H + 1, &inc.b2.hash);
    hooks.reply = refused();
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "cron 1: claimed, posted, refused"
    );
    assert_eq!(announce_counters(&db), (1, 0));
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert!(hooks.take().is_empty(), "cron 2, inside the window: silent");
    next_cron(&db);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "cron 3, idle: the same body again"
    );
    assert_eq!(announce_counters(&db), (2, 0));
    // WoC down: the announce still runs
    next_cron(&db);
    chain.set_unavailable(true);
    hooks.reply = WebhookDelivery::Accepted(200);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "cron 4, WoC unavailable: delivered"
    );
    assert_eq!(pending(&db), None);
    assert_eq!(announce_counters(&db), (0, 0));
    chain.set_unavailable(false);
    next_cron(&db);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert!(hooks.take().is_empty(), "delivered: silent");
}

/// On an idle chain the stuck counter fires after `ANNOUNCE_STUCK_AFTER`
/// crons past the window, not after five blocks. To red: `run_cron`, the
/// idle-path `announce_tip` call deleted (the run never grows).
#[tokio::test]
async fn the_stuck_counter_fires_after_five_undelivered_idle_crons() {
    let db = SqliteDb::migrated();
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let inc = incident();
    incident_reorged(&db, &hooks, &inc).await;
    chain.tip(H + 1, &inc.b2.hash);
    hooks.reply = WebhookDelivery::Failed("connection refused".to_string());
    for cron in 1..=ANNOUNCE_STUCK_AFTER {
        next_cron(&db);
        run_cron(&db, regtest(), &chain, None, &hooks)
            .await
            .unwrap();
        assert_eq!(bodies(&hooks).len(), 1, "cron {cron}");
        assert_eq!(
            announce_counters(&db),
            (i64::from(cron), i64::from(cron / ANNOUNCE_STUCK_AFTER)),
            "cron {cron}"
        );
    }
    assert_eq!(announce_counters(&db), (i64::from(ANNOUNCE_STUCK_AFTER), 1));
    assert_eq!(
        pending(&db),
        Some(i64::from(H)),
        "the marker is kept throughout"
    );
}

/// A retry never moves the /getInfo freshness signal (`updated_at`, audit
/// M6): a dead target retried every minute cannot make a stalled sync look
/// fresh; only a NEW tip's claim moves it. To red: `RECLAIM_ANNOUNCE_SQL`,
/// `SET claimed_at = datetime('now')` to `SET claimed_at = datetime('now'),
/// updated_at = datetime('now')`.
#[tokio::test]
async fn a_retry_never_moves_the_freshness_read() {
    let db = SqliteDb::migrated();
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let inc = incident();
    incident_reorged(&db, &hooks, &inc).await;
    chain.tip(H + 1, &inc.b2.hash);
    hooks.reply = refused();
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(bodies(&hooks).len(), 1);
    let stalled = "2026-09-08 00:00:00";
    set_freshness(&db, stalled);
    for _ in 0..3 {
        next_cron(&db);
        run_cron(&db, regtest(), &chain, None, &hooks)
            .await
            .unwrap();
        assert_eq!(bodies(&hooks).len(), 1, "retried");
        assert_eq!(
            freshness(&db).as_deref(),
            Some(stalled),
            "the freshness signal did not move"
        );
    }
    let info = get_info(&db, &Chain::Main).await.unwrap();
    assert_eq!(info.last_synced_at.as_deref(), Some(stalled));
    assert_eq!(info.tip_announce_failures, Some(4));
    // a new block: the claim moves it
    let c = header(H + 2, "c", &inc.b2.hash);
    chain.publish(&c);
    chain.tip(H + 2, &c.hash);
    hooks.reply = WebhookDelivery::Accepted(200);
    run_cron(&db, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert_eq!(bodies(&hooks), vec![with_fork(H + 2, &c.hash, H)]);
    assert_ne!(
        freshness(&db).as_deref(),
        Some(stalled),
        "a new tip's claim moves it"
    );
}

// ─── (7) The claim: one isolate announces ───────────────────────────────────

/// MED-1: two isolates read the same unannounced tip; the one whose claim
/// lands first announces, the other's claim answers 0 changes and it POSTs
/// nothing (block 965077 was announced five times before the claim). The
/// second isolate runs, whole, between the first's announce-row read and its
/// claim. To red: `notify_if_tip_advanced`, `run_changes(db).await? == 1` on
/// the claim to `>= 0`.
#[tokio::test]
async fn two_isolates_reading_the_same_unannounced_tip_announce_it_once() {
    let db = SqliteDb::migrated();
    let hooks_b = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let rec = Recorder::new(&db);
    let hooks_a = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&rec, &hooks_a, &inc).await;
    let b = &hooks_b;
    rec.after_first_of(ROUND2_ANNOUNCED_TIP, move |db| async move {
        notify_if_tip_advanced(db, b).await.unwrap();
    });
    let before = rec.handed().len();
    notify_if_tip_advanced(&rec, &hooks_a).await.unwrap();
    assert!(hooks_a.take().is_empty(), "the loser posts nothing");
    assert_eq!(
        bodies(&hooks_b),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "the winner announced once"
    );
    assert_eq!(pending(&db), None);
    assert_eq!(announce_counters(&db), (0, 0));
    assert_eq!(
        rec.handed()[before..].to_vec(),
        vec![
            Handed::First(Query::new(MAIN_CHAIN_TIP)),
            Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
            Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
            Handed::Execute(
                Query::new(ROUND4_TIP_ANNOUNCE)
                    .bind(H + 1)
                    .bind(inc.b2.hash.as_str())
            ),
        ],
        "the loser: the claim lost, nothing after it"
    );
}

/// MED-1: an isolate arriving while the announce is IN FLIGHT in another
/// (the claim made, no delivery recorded yet) stays silent: the retry claim
/// is gated on the claim's age. The second isolate runs, whole, between the
/// first's claim and its POST. To red: `CLAIM_IN_FLIGHT_S` to `0`.
#[tokio::test]
async fn an_isolate_arriving_while_the_announce_is_in_flight_stays_silent() {
    let db = SqliteDb::migrated();
    let hooks_b = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let rec = Recorder::new(&db);
    let hooks_a = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&rec, &hooks_a, &inc).await;
    let b = &hooks_b;
    rec.after_first_of(READ_PENDING_REORG_SQL, move |db| async move {
        notify_if_tip_advanced(db, b).await.unwrap();
    });
    notify_if_tip_advanced(&rec, &hooks_a).await.unwrap();
    assert!(hooks_b.take().is_empty(), "in flight elsewhere: silent");
    assert_eq!(
        bodies(&hooks_a),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "the first isolate delivered"
    );
    assert_eq!(pending(&db), None);
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H))]
    );
}

/// MED-1: a retry claim never touches a newer claim. The first isolate's
/// delivery was refused (the claim owed); by its next cron another isolate
/// has claimed and delivered a NEWER tip (with the fork still pending, so it
/// carries `reorgFrom`); the retry claim for the old tip answers 0 changes
/// and posts nothing; the old announce is superseded, the fork was heard. To
/// red: `RECLAIM_ANNOUNCE_SQL`, delete ` AND last_synced_height = ?1 AND
/// last_announced_hash = ?2`.
#[tokio::test]
async fn a_retry_claim_never_touches_a_newer_claim() {
    let db = SqliteDb::migrated();
    let hooks_b = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let rec = Recorder::new(&db);
    let mut hooks_a = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&rec, &hooks_a, &inc).await;
    hooks_a.reply = refused();
    notify_if_tip_advanced(&rec, &hooks_a).await.unwrap();
    assert_eq!(bodies(&hooks_a).len(), 1, "refused");
    assert_eq!(pending(&db), Some(i64::from(H)));
    hooks_a.reply = WebhookDelivery::Accepted(200);
    next_cron(&db);
    let c = header(H + 2, "c: the next block", &inc.b2.hash);
    let c_hash = c.hash.clone();
    let b = &hooks_b;
    rec.after_first_of(ROUND2_ANNOUNCED_TIP, move |db| async move {
        insert_header(db, regtest(), &c).await.unwrap();
        notify_if_tip_advanced(db, b).await.unwrap();
    });
    notify_if_tip_advanced(&rec, &hooks_a).await.unwrap();
    assert!(
        hooks_a.take().is_empty(),
        "the stale retry lost to the newer claim"
    );
    assert_eq!(
        bodies(&hooks_b),
        vec![with_fork(H + 2, &c_hash, H)],
        "the newer tip carried the fork"
    );
    assert_eq!(
        announced(&db),
        (i64::from(H + 2), Some(c_hash.clone())),
        "the newer claim untouched"
    );
    assert_eq!(pending(&db), None);
    assert!(rec.handed().contains(&Handed::Execute(
        Query::new(ROUND3_RECLAIM_ANNOUNCE)
            .bind(H + 1)
            .bind(inc.b2.hash.as_str())
            .bind(CLAIM_IN_FLIGHT_S)
    )));
}

/// LOW-1 (round 4): a slow delivery of an OLDER tip never rewrites a newer
/// record. Isolate A claims T1 and its POST is slow; meanwhile isolate B
/// claims and delivers T2 to the same target; A's POST is then accepted, but
/// its delivery record is guarded on the claim row (now T2) and writes
/// nothing, so the next announce does not re-post T2. To red:
/// `RECORD_DELIVERY_SQL`, delete ` AND last_synced_height = ?2 AND
/// last_announced_hash = ?3` (the record becomes unconditional).
#[tokio::test]
async fn a_late_delivery_never_rewrites_a_newer_record() {
    let db = SqliteDb::migrated();
    let hooks_b = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let rec = Recorder::new(&db);
    let hooks_a = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&rec, &hooks_a, &inc).await;
    let c = header(H + 2, "c: the next block", &inc.b2.hash);
    let c_hash = c.hash.clone();
    let b = &hooks_b;
    rec.after_first_of(READ_PENDING_REORG_SQL, move |db| async move {
        insert_header(db, regtest(), &c).await.unwrap();
        notify_if_tip_advanced(db, b).await.unwrap();
    });
    notify_if_tip_advanced(&rec, &hooks_a).await.unwrap();
    assert_eq!(
        bodies(&hooks_a),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "A's slow T1 delivery, accepted late"
    );
    assert_eq!(
        bodies(&hooks_b),
        vec![with_fork(H + 2, &c_hash, H)],
        "B delivered T2 meanwhile"
    );
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 2, &c_hash, Some(H))],
        "the newer record stands"
    );
    assert_eq!(pending(&db), None);
    next_cron(&db);
    notify_if_tip_advanced(&rec, &hooks_a).await.unwrap();
    assert!(
        hooks_a.take().is_empty(),
        "nothing owed: T2 is not re-posted"
    );
}

/// MED-1 residual, closed: a claim left by an isolate evicted mid-POST (the
/// claim made, no delivery, no failure recorded) is silent inside its
/// window and re-claimed and re-sent by the announce after it. The state is
/// planted with the claim statement itself. To red: `RECLAIM_ANNOUNCE_SQL`,
/// `>= ?3` to `< ?3`.
#[tokio::test]
async fn a_claim_left_by_an_evicted_isolate_is_re_sent_after_its_window() {
    let db = SqliteDb::migrated();
    let inc = incident();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    incident_reorged(&db, &hooks, &inc).await;
    db.conn()
        .execute(
            TIP_ANNOUNCE_SQL,
            rusqlite::params![i64::from(H + 1), inc.b2.hash],
        )
        .unwrap();
    assert_eq!(
        announced(&db),
        (i64::from(H + 1), Some(inc.b2.hash.clone()))
    );
    assert_eq!(announce_counters(&db), (0, 0));
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(
        hooks.take().is_empty(),
        "inside the window: presumed in flight"
    );
    next_cron(&db);
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "re-claimed and re-sent"
    );
    assert_eq!(pending(&db), None);
    assert_eq!(
        deliveries(&db),
        vec![delivered(BETA_URL, H + 1, &inc.b2.hash, Some(H))]
    );
}

/// The claim the PRE-0006 build left (no `claimed_at`) counts as old: the
/// current tip is re-sent once by the first announce after the deploy (the
/// deploy-time residual named in the build log). To red:
/// `RECLAIM_ANNOUNCE_SQL`, delete `claimed_at IS NULL OR `.
#[tokio::test]
async fn a_claim_without_a_clock_is_re_sent_by_the_first_announce_after_the_deploy() {
    let db = SqliteDb::migrated();
    let inc = incident();
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    incident_announced(&db, &hooks, &inc).await;
    // the pre-0006 build's state: a claim, no clock, no delivery rows
    db.conn()
        .execute("UPDATE sync_state SET claimed_at = NULL WHERE id = 1", [])
        .unwrap();
    db.conn()
        .execute("DELETE FROM announce_deliveries", [])
        .unwrap();
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![plain(H + 1, &inc.b2.hash)],
        "re-sent once"
    );
    notify_if_tip_advanced(&db, &hooks).await.unwrap();
    assert!(hooks.take().is_empty(), "then silent");
}

/// LOW-1 (round 5): no premature clear while a target has not heard the
/// fork. The rule is `heard_by_all`: the marker is CAS-cleared only when
/// EVERY configured target's recorded delivery carries it, never merely
/// because this round's owed targets all accepted. The state that tells the
/// two apart is reachable through the fork-read race: a target accepts a
/// PLAIN body while the fork is pending (the fork read faulted, best-effort
/// `None`), so it has the tip but not the fork; the next round's owed target
/// accepts with the fork and `round.failed == 0` would clear. To red:
/// `notify_if_tip_advanced`, `if heard_by_all {` to `if round.failed == 0 {`.
#[tokio::test]
async fn no_premature_clear_while_a_target_has_not_heard_the_fork() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let inc = incident();
    incident_reorged(&rec, &RecordedWebhooks::new(BETA_HOOK, BEARER), &inc).await;
    let hooks = RecordedWebhooks::new(TWO_HOOKS, BEARER);
    // round 1: the fork read faults; the public target accepts a plain body,
    // the beta target refuses
    rec.fault_first_of(READ_PENDING_REORG_SQL);
    hooks.reply_for(BETA_URL, refused());
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![plain(H + 1, &inc.b2.hash), plain(H + 1, &inc.b2.hash)],
        "the fork read faulted: plain bodies"
    );
    assert_eq!(
        deliveries(&db),
        vec![
            delivered(PUBLIC_URL, H + 1, &inc.b2.hash, None),
            delivered(BETA_URL, H, &inc.a.hash, None),
        ],
        "the public target has the tip without the fork; the beta target still has A"
    );
    assert_eq!(pending(&db), Some(i64::from(H)));
    // round 2: the beta target, the only one owed, accepts WITH the fork
    next_cron(&db);
    hooks.reply_for(BETA_URL, WebhookDelivery::Accepted(200));
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    assert_eq!(
        bodies(&hooks),
        vec![with_fork(H + 1, &inc.b2.hash, H)],
        "the owed target alone, with the fork"
    );
    assert_eq!(announce_counters(&db), (0, 0), "every owed target accepted");
    assert_eq!(
        pending(&db),
        Some(i64::from(H)),
        "NOT cleared: the public target has the tip but has not heard the fork"
    );
    // the next tip carries the fork to the public target; then every target has heard it
    let c = header(H + 2, "c", &inc.b2.hash);
    insert_header(&rec, regtest(), &c).await.unwrap();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    let posts = hooks.take();
    assert_eq!(
        posts.iter().map(|p| p.body.clone()).collect::<Vec<_>>(),
        vec![plain(H + 2, &c.hash), with_fork(H + 2, &c.hash, H)]
    );
    assert_eq!(pending(&db), None, "cleared once every target has heard it");
}

/// LOW-4 (round 5): an idle cron reads the tip ONCE (the height decides the
/// sync mode, the competitor check compares its hash, the announce reuses
/// it), the courier record once (a private program loop 10 D5; the seen height is
/// written ONCE when the record is behind, see `courier_tests`), then the
/// announce row and the deliveries, and writes nothing when the record is
/// current and every target has the tip. #32 adds the durable age write and
/// the served-ceiling read. To red:
/// `run_cron`, `announce_tip(db, hooks, tip)` on the idle path to
/// `announce_tip(db, hooks, None)`, or the D5 `if seen != Some(woc_height)`
/// guard dropped (a write every tick).
#[tokio::test]
async fn an_idle_cron_reads_the_tip_once_and_emits_age() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let chain = ScriptedChain::new();
    let inc = incident();
    incident_announced(&rec, &hooks, &inc).await;
    chain.tip(H + 1, &inc.b2.hash);
    next_cron(&db);
    // the first idle cron after an ingest that bypassed the cron seeds the
    // courier record (one write); the pin is on the steady state after it
    run_cron(&rec, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert!(hooks.take().is_empty());
    let before = rec.handed().len();
    run_cron(&rec, regtest(), &chain, None, &hooks)
        .await
        .unwrap();
    assert!(hooks.take().is_empty());
    assert_eq!(
        rec.handed()[before..].to_vec(),
        vec![
            Handed::Execute(Query::new(
                "UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1"
            )),
            Handed::Execute(Query::new(crate::events::SQL_TIP_AGE)),
            Handed::First(Query::new(MAIN_CHAIN_TIP)),
            Handed::First(Query::new(D5_COURIER_HEALTH)),
            // P0-4: the re-validation state, one read a tick (complete here)
            Handed::First(Query::new(P04_VALIDATION_STATE)),
            // #32: the legacy announce reads the verification ceiling.
            Handed::First(Query::new(P04_VALIDATION_STATE)),
            Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
            Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
        ],
        "one tip read, bounded age journal, validation and announce reads"
    );
}

// ─── The SQL the real path hands the database ───────────────────────────────

/// The B' insert and its announce hand the database EXACTLY main's statements
/// plus this branch's, in this order, with these binds, in these transactions
/// (the goldens are literals extracted from main's bytes, never the constants
/// under test; the claim is main's literal plus `claimed_at`, round 4). Round
/// 3 put the claim back before the fork read (as main), added the deliveries
/// read before the claim and the delivery record after the POST, and writes
/// the failure run to 0 on every delivery; round 4 records the fork carried
/// with the delivery. To red: any byte of any statement on the path, or the
/// order of any read; e.g. `handle_reorg`, swap the `SQL_DEACTIVATE_ABOVE`
/// and the first `SQL_ACTIVATE_HASH` in the batch.
#[tokio::test]
async fn the_producer_path_hands_the_database_exactly_mains_statements_in_order() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    orphan_active(&rec, &hooks, &inc).await;
    insert_header(&rec, regtest(), &inc.a2).await.unwrap();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    let before = rec.handed().len();
    insert_header(&rec, regtest(), &inc.b2).await.unwrap();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    hooks.take();

    let a2_id = header_id(&db, &inc.a2.hash);
    let y_id = header_id(&db, &inc.y.hash);
    let g = calculate_work(EQUAL_WORK_BITS);
    let b2_work = add_work(&add_work(&add_work(&g, &g), &g), &g);
    let by_hash = |h: &str| Handed::First(Query::new(MAIN_HEADER_FOR_HASH).bind(h));
    let by_id = |id: i64| Handed::First(Query::new(MAIN_HEADER_FOR_ID).bind(id));
    let tip = || Handed::First(Query::new(MAIN_CHAIN_TIP));
    let expected = vec![
        // insert_header(B'): the dupe check, the parent, the tip
        by_hash(&inc.b2.hash),
        by_hash(&inc.a2.hash),
        tip(),
        // the row, INACTIVE: it does not extend the tip (A)
        Handed::Execute(
            Query::new(MAIN_INSERT_HEADER)
                .bind(a2_id)
                .bind(inc.b2.previous_hash.as_str())
                .bind(H + 1)
                .bind(false)
                .bind(false)
                .bind(inc.b2.hash.as_str())
                .bind(b2_work.as_str())
                .bind(0x2000_0000u32)
                .bind(inc.b2.merkle_root.as_str())
                .bind(inc.b2.time)
                .bind(EQUAL_WORK_BITS)
                .bind(inc.b2.nonce),
        ),
        // handle_reorg: the common ancestor. B' (the input row, no id link)
        // steps back by hash to A'; A' and A (the stored tip) step back by id
        // to Y, which is the ancestor.
        by_hash(&inc.a2.hash),
        by_id(y_id),
        by_id(y_id),
        // the branch to activate: B' by hash to A', A' by id to Y (stop)
        by_hash(&inc.a2.hash),
        by_id(y_id),
        // what the deactivate will touch, then ONE transaction: deactivate
        // above the ancestor, activate B' and A', record the fork at H
        Handed::First(Query::new(MAIN_COUNT_ACTIVE_ABOVE).bind(H - 1)),
        Handed::Batch(vec![
            Query::new(MAIN_DEACTIVATE_ABOVE).bind(i64::from(H - 1)),
            Query::new(MAIN_ACTIVATE_HASH).bind(inc.b2.hash.clone()),
            Query::new(MAIN_ACTIVATE_HASH).bind(inc.a2.hash.clone()),
            Query::new(MAIN_RECORD_PENDING_REORG).bind(i64::from(H)),
            // #32: the tip and its trigger-written event share this batch.
            Query::new(MAIN_CLEAR_CHAIN_TIP),
            Query::new(MAIN_SET_CHAIN_TIP_ACTIVE).bind(inc.b2.hash.clone()),
        ]),
        // notify_if_tip_advanced: the tip, the announce row, the deliveries;
        // the CLAIM (main's position: before the fork read; main's text plus
        // claimed_at); the fork read; the POST; the delivery record with the
        // fork carried; the CAS clear; the run to 0
        tip(),
        Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
        Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
        Handed::Execute(
            Query::new(ROUND4_TIP_ANNOUNCE)
                .bind(H + 1)
                .bind(inc.b2.hash.as_str()),
        ),
        Handed::First(Query::new(MAIN_READ_PENDING_REORG)),
        Handed::Batch(vec![Query::new(ROUND3_RECORD_DELIVERY)
            .bind(BETA_URL)
            .bind(i64::from(H + 1))
            .bind(inc.b2.hash.clone())
            .bind(Some(i64::from(H)))]),
        Handed::Execute(Query::new(MAIN_CLEAR_PENDING_REORG).bind(i64::from(H))),
        Handed::Execute(Query::new(ROUND2_SET_ANNOUNCE_FAILURES).bind(0u32)),
    ];
    let handed = rec.handed()[before..].to_vec();
    for (i, (got, want)) in handed.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got, want, "statement {i}");
    }
    assert_eq!(handed.len(), expected.len());
}

/// An undelivered announce hands the database the reads and the claim, the
/// fork read, then only the failure-run write (and, at the stuck threshold,
/// the stuck count): never a delivery record, never the clear. Inside the
/// window a retry is the three reads and the retry claim that loses; after
/// the window it is the retry claim that wins. To red: `notify_if_tip_advanced`,
/// the `round.failed == 0` branch condition to `true`.
#[tokio::test]
async fn an_undelivered_announce_writes_only_the_failure_run() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_reorged(&rec, &hooks, &inc).await;
    hooks.reply = WebhookDelivery::Failed("connection refused".to_string());
    let reads = || {
        vec![
            Handed::First(Query::new(MAIN_CHAIN_TIP)),
            Handed::First(Query::new(ROUND2_ANNOUNCED_TIP)),
            Handed::All(Query::new(ROUND3_READ_DELIVERIES)),
        ]
    };
    let reclaim = || {
        Handed::Execute(
            Query::new(ROUND3_RECLAIM_ANNOUNCE)
                .bind(H + 1)
                .bind(inc.b2.hash.as_str())
                .bind(CLAIM_IN_FLIGHT_S),
        )
    };
    let before = rec.handed().len();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    let mut expected = reads();
    expected.extend([
        Handed::Execute(
            Query::new(ROUND4_TIP_ANNOUNCE)
                .bind(H + 1)
                .bind(inc.b2.hash.as_str()),
        ),
        Handed::First(Query::new(MAIN_READ_PENDING_REORG)),
        Handed::Execute(Query::new(ROUND2_SET_ANNOUNCE_FAILURES).bind(1u32)),
    ]);
    assert_eq!(
        rec.handed()[before..].to_vec(),
        expected,
        "the first, a new tip"
    );

    let before = rec.handed().len();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    let mut expected = reads();
    expected.push(reclaim());
    assert_eq!(
        rec.handed()[before..].to_vec(),
        expected,
        "inside the window: the retry claim loses"
    );

    for _ in 2..ANNOUNCE_STUCK_AFTER {
        next_cron(&db);
        notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    }
    next_cron(&db);
    let before = rec.handed().len();
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    let mut expected = reads();
    expected.extend([
        reclaim(),
        Handed::First(Query::new(MAIN_READ_PENDING_REORG)),
        Handed::Execute(Query::new(ROUND2_SET_ANNOUNCE_FAILURES).bind(ANNOUNCE_STUCK_AFTER)),
        Handed::Execute(Query::new(ROUND2_COUNT_ANNOUNCE_STUCK)),
    ]);
    assert_eq!(
        rec.handed()[before..].to_vec(),
        expected,
        "the stuck threshold"
    );
    hooks.take();
}

/// Everything the producer path runs, over the whole incident, the deeper
/// competitor, a plain extension, a run of undelivered announces up to the
/// stuck threshold and the delivery that ends it, is one of main's thirteen
/// statements on that path (the claim with round 4's column), round 2's
/// three or round 3's three, and every one of those is in the pinned
/// vocabulary. To red: add any statement to the path.
#[tokio::test]
async fn everything_the_producer_path_runs_is_in_the_pinned_vocabulary() {
    let db = SqliteDb::migrated();
    let rec = Recorder::new(&db);
    let mut hooks = RecordedWebhooks::new(BETA_HOOK, BEARER);
    let inc = incident();
    incident_announced(&rec, &hooks, &inc).await;
    for h in deeper_branch(&inc) {
        insert_header(&rec, regtest(), &h).await.unwrap();
    }
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    insert_header(
        &rec,
        regtest(),
        &header(H + 3, "a plain extension", &deeper_branch(&inc)[3].hash),
    )
    .await
    .unwrap();
    hooks.reply = WebhookDelivery::Failed("connection refused".to_string());
    for _ in 0..ANNOUNCE_STUCK_AFTER {
        next_cron(&db);
        notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    }
    hooks.reply = WebhookDelivery::Accepted(200);
    next_cron(&db);
    notify_if_tip_advanced(&rec, &hooks).await.unwrap();
    hooks.take();

    let mut expected: Vec<String> = [
        MAIN_HEADER_FOR_HASH,
        MAIN_CHAIN_TIP,
        MAIN_HEADER_FOR_ID,
        MAIN_INSERT_HEADER,
        MAIN_COUNT_ACTIVE_ABOVE,
        MAIN_DEACTIVATE_ABOVE,
        MAIN_ACTIVATE_HASH,
        MAIN_RECORD_PENDING_REORG,
        MAIN_CLEAR_CHAIN_TIP,
        MAIN_SET_CHAIN_TIP_ACTIVE,
        ROUND4_TIP_ANNOUNCE,
        MAIN_READ_PENDING_REORG,
        MAIN_CLEAR_PENDING_REORG,
        ROUND2_ANNOUNCED_TIP,
        ROUND2_SET_ANNOUNCE_FAILURES,
        ROUND2_COUNT_ANNOUNCE_STUCK,
        ROUND3_RECLAIM_ANNOUNCE,
        ROUND3_READ_DELIVERIES,
        ROUND3_RECORD_DELIVERY,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    expected.sort();
    assert_eq!(rec.vocabulary(), expected);
    for text in rec.vocabulary() {
        assert!(
            MAIN_VOCABULARY.contains(&text.as_str())
                || ROUND2_VOCABULARY.contains(&text.as_str())
                || ROUND3_VOCABULARY.contains(&text.as_str())
                || ROUND4_VOCABULARY.contains(&text.as_str()),
            "{text}"
        );
    }
}
