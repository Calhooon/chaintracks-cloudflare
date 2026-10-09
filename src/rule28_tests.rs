//! Rule 28 (bsv-stack-lean, the owner's ruling of 2026-10-09; the judgement
//! `docs/p0/rule-28-explorer-calls.md` sections 3 and 5): a third-party chain
//! explorer is break-glass. A call that re-derives an answer this service
//! already holds is deleted; a call nothing we hold can answer stays, behind
//! the courier ladder, with its reason named at the site. Each witness below
//! was RED at `62cf619` and is green at the commit that names it.
use crate::couriers::{CourierLadder, RUNG_FAULT_CAP};
use crate::host_harness::{
    block_hash, flags, header, regtest, RecordedWebhooks, ScriptedChain, SqliteDb,
};
use crate::storage::insert_header;
use crate::sync::{
    bootstrap_from_peer, fetch_span, read_through, Bootstrap, ChainSource, HeaderPeer,
    BOOTSTRAP_BATCH,
};
use crate::types::BlockHeader;

/// The text of one function of a source file: from its signature to the
/// first line that closes it at column 0.
fn fn_text<'a>(src: &'a str, signature: &str) -> &'a str {
    let start = src
        .find(signature)
        .unwrap_or_else(|| panic!("{signature} is not in the source"));
    let end = src[start..]
        .find("\n}\n")
        .unwrap_or_else(|| panic!("{signature} never closes"));
    &src[start..start + end]
}

const ROUTES: &str = include_str!("routes.rs");

// ─── H11: `/getPresentHeight` ───────────────────────────────────────────────

/// H11 (`routes.rs:267-270` at `62cf619`): every request to the public route
/// `/getPresentHeight` was one request out to WhatsOnChain's `/chain/info`,
/// and the explorer's unchecked number was served with `status: success`.
/// The service holds the answer: its served tip, and `last_seen_height`, the
/// highest tip the ladder answered in the last cron tick. The route asks no one.
#[test]
fn h11_the_present_height_route_asks_no_explorer() {
    let route = fn_text(ROUTES, "async fn get_present_height(");
    for call in ["WocClient", "get_chain_info", "Fetch::", "CourierLadder"] {
        assert!(
            !route.contains(call),
            "/getPresentHeight is answered from the store, never by a request out: found {call}"
        );
    }
}

/// The harness's checkpoint root (`host_harness::regtest`), where a store starts.
const ROOT: u32 = 965_875;

/// `n` rows stored as one linked chain from the checkpoint root; answers the
/// rows, the last of them the tip.
async fn chain_of(db: &SqliteDb, n: u32) -> Vec<BlockHeader> {
    let mut rows = vec![header(ROOT, "x", &block_hash("the block below x"))];
    for h in ROOT + 1..ROOT + n {
        let parent = rows.last().unwrap().hash.clone();
        rows.push(header(h, &format!("rule 28 row {h}"), &parent));
    }
    for row in &rows {
        let r = insert_header(db, regtest(), row).await.unwrap();
        assert!(r.added && r.is_active_tip, "{r:?}");
    }
    rows
}

fn set_seen(db: &SqliteDb, height: Option<u32>) {
    db.conn()
        .execute(
            "UPDATE sync_state SET last_seen_height = ?1 WHERE id = 1",
            [height],
        )
        .unwrap();
}

/// The answer: the larger of the served tip and `last_seen_height`.
#[tokio::test]
async fn h11_the_present_height_is_the_larger_of_the_served_tip_and_the_last_seen_height() {
    let db = SqliteDb::migrated();
    assert_eq!(
        crate::storage::present_height(&db).await.unwrap(),
        None,
        "an empty store has no present height: the route answers 503, never 0"
    );
    let tip = chain_of(&db, 3).await.last().unwrap().height;
    assert_eq!(tip, ROOT + 2);

    // no courier record yet (the cron has not run since migration 0007)
    set_seen(&db, None);
    assert_eq!(
        crate::storage::present_height(&db).await.unwrap(),
        Some(tip)
    );
    // the couriers answered a tip two above ours in the last tick
    set_seen(&db, Some(tip + 2));
    assert_eq!(
        crate::storage::present_height(&db).await.unwrap(),
        Some(tip + 2)
    );
    // a stale record below our tip (an ingest that bypassed the cron)
    set_seen(&db, Some(tip - 12));
    assert_eq!(
        crate::storage::present_height(&db).await.unwrap(),
        Some(tip)
    );
}

/// The courier record alone is never a height: with nothing served (the
/// re-validation has no anchor, or the store is empty) the route is degraded.
#[tokio::test]
async fn h11_a_seen_height_with_nothing_served_is_not_a_present_height() {
    let db = SqliteDb::migrated();
    set_seen(&db, Some(970_000));
    assert_eq!(crate::storage::present_height(&db).await.unwrap(), None);
}

/// Before migration 0007 the record's column is missing: the read fault is
/// loud and the served tip answers; the route never fails on it.
#[tokio::test]
async fn h11_a_store_before_migration_0007_answers_its_served_tip() {
    let db = SqliteDb::migrated();
    chain_of(&db, 2).await;
    db.conn()
        .execute("ALTER TABLE sync_state DROP COLUMN last_seen_height", [])
        .unwrap();
    assert_eq!(
        crate::storage::present_height(&db).await.unwrap(),
        Some(ROOT + 1)
    );
}

// ─── H12, H13: the ladder on the read-through and the backfill ──────────────

/// H12 (`routes.rs:349-358` at `62cf619`): the read-through for a block up to
/// six above the tip asked WhatsOnChain alone, by height and for the parent
/// walk. One courier's refusal was the 965877 lag the ladder was written for
/// (`couriers.rs`). The read-through reads the ladder the cron reads.
#[test]
fn h12_the_read_through_reads_the_ladder_the_cron_reads() {
    let route = fn_text(ROUTES, "async fn ensure_fresh_header(");
    assert!(
        route.contains("crate::sync::courier_ladder("),
        "the read-through builds the cron's ladder"
    );
    for one_courier in ["WocClient::new", "get_header_by_height"] {
        assert!(
            !route.contains(one_courier),
            "the read-through never asks one courier: found {one_courier}"
        );
    }
}

/// H13 (`routes.rs:704-708` at `62cf619`): the operator's backfill of a gap
/// below the tip asked WhatsOnChain alone, up to 800 times a call.
#[test]
fn h13_the_backfill_reads_the_ladder_the_cron_reads() {
    let route = fn_text(ROUTES, "async fn admin_backfill(");
    assert!(
        route.contains("crate::sync::courier_ladder("),
        "the backfill builds the cron's ladder"
    );
    for one_courier in ["WocClient::new", "get_header_by_height"] {
        assert!(
            !route.contains(one_courier),
            "the backfill never asks one courier: found {one_courier}"
        );
    }
}

/// The cron's own ladder is built by the same constructor, so the three
/// paths cannot drift apart on which couriers they ask.
#[test]
fn the_cron_the_read_through_and_the_backfill_build_one_ladder() {
    let cron = fn_text(include_str!("sync.rs"), "pub async fn poll_for_new_blocks(");
    assert!(cron.contains("courier_ladder(env, &chain)"));
    assert!(!cron.contains("CourierLadder::for_chain("));
}

const HOOK: &str =
    "APP_LAYER_BETA=https://your-worker.your-account.workers.dev/internal/tip-changed";

/// A two-rung ladder of scripted couriers, the first asked first.
fn ladder_of(first: ScriptedChain, second: ScriptedChain) -> CourierLadder<ScriptedChain> {
    CourierLadder::new(
        vec![("woc", first), ("arcade", second)],
        0,
        regtest().clone(),
    )
}

fn refusing() -> ScriptedChain {
    let chain = ScriptedChain::new();
    chain.set_unavailable(true);
    chain
}

/// The 965877 shape on the read-through: the courier asked first refuses, the
/// next one serves. On one courier (the path at `62cf619`) the request
/// answers "unable to verify" for a block the network holds; on the ladder
/// the block lands, checked, and the tip moves.
#[tokio::test]
async fn h12_a_fresh_block_does_not_depend_on_one_courier() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, "bearer");
    let rows = chain_of(&db, 2).await;
    let tip = rows.last().unwrap();
    let fresh = header(tip.height + 1, "the fresh block", &tip.hash);
    let fresher = header(tip.height + 2, "the block above it", &fresh.hash);

    let serving = ScriptedChain::new();
    serving.publish(&fresh);
    serving.publish(&fresher);

    // one courier, and it refuses: could not look, so nothing is stored
    let alone = read_through(
        &db,
        regtest(),
        &refusing(),
        &hooks,
        tip.height,
        fresher.height,
    )
    .await
    .unwrap();
    assert_eq!(alone, None, "one refusing courier: unable to verify");
    assert_eq!(flags(&db, &tip.hash), (true, true), "the held tip stands");

    // the ladder: the second rung serves both, by height
    let ladder = ladder_of(refusing(), serving);
    let landed = read_through(&db, regtest(), &ladder, &hooks, tip.height, fresher.height)
        .await
        .unwrap();
    assert_eq!(landed, Some(()), "the next courier's answer lands");
    assert_eq!(flags(&db, &fresher.hash), (true, true), "the tip moved");
    assert_eq!(flags(&db, &fresh.hash), (true, false));
    assert!(
        ladder.summary().contains("woc ok 0 faults 2") && ladder.summary().contains("arcade ok 2"),
        "the refusals are counted, never silent: {}",
        ladder.summary()
    );
}

/// The parent walk of the read-through reads the ladder too: the fresh block
/// builds on a competitor of our tip, which only the second courier holds by
/// hash. On one courier that was the wedge; on the ladder the branch lands.
#[tokio::test]
async fn h12_the_parent_walk_of_the_read_through_reads_the_ladder() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, "bearer");
    let rows = chain_of(&db, 3).await;
    let (below, tip) = (&rows[1], &rows[2]);
    let competitor = header(tip.height, "the competitor of our tip", &below.hash);
    let fresh = header(
        tip.height + 1,
        "the block on the competitor",
        &competitor.hash,
    );

    let by_height_only = ScriptedChain::new();
    by_height_only.publish(&fresh);
    // this courier never serves the competitor by hash (loop 9's refusal)
    let holds_the_parent = ScriptedChain::new();
    holds_the_parent.publish(&competitor);

    let ladder = ladder_of(by_height_only, holds_the_parent);
    let landed = read_through(&db, regtest(), &ladder, &hooks, tip.height, fresh.height)
        .await
        .unwrap();
    assert_eq!(landed, Some(()));
    assert_eq!(
        flags(&db, &fresh.hash),
        (true, true),
        "the heavier branch won"
    );
    assert_eq!(flags(&db, &competitor.hash), (true, false));
    assert_eq!(
        flags(&db, &tip.hash),
        (false, false),
        "our old tip is off the chain"
    );
}

/// A header a courier serves is believed for its work and its link, never for
/// who served it: a block that links to nothing we hold and to nothing any
/// courier holds is refused, and the request answers "unable to verify".
#[tokio::test]
async fn h12_a_header_no_parent_vouches_for_is_refused_whoever_served_it() {
    let db = SqliteDb::migrated();
    let hooks = RecordedWebhooks::new(HOOK, "bearer");
    let rows = chain_of(&db, 2).await;
    let tip = rows.last().unwrap();
    let foreign = header(
        tip.height + 1,
        "a foreign block",
        &block_hash("no such parent"),
    );
    let a = ScriptedChain::new();
    let b = ScriptedChain::new();
    a.publish(&foreign);
    b.publish(&foreign);
    let ladder = ladder_of(a, b);
    let landed = read_through(&db, regtest(), &ladder, &hooks, tip.height, foreign.height)
        .await
        .unwrap();
    assert_eq!(landed, None, "unable to verify");
    assert_eq!(flags(&db, &tip.hash), (true, true), "the held tip stands");
}

/// The backfill of a span: the courier asked first refuses, the next serves
/// every height; the refusing rung is asked three times and then skipped for
/// the rest of the call (the subrequest budget of 800 is not spent on it).
#[tokio::test]
async fn h13_the_backfill_takes_each_header_from_the_next_courier() {
    let rows = {
        let db = SqliteDb::migrated();
        chain_of(&db, 6).await
    };
    let serving = ScriptedChain::new();
    for r in &rows {
        serving.publish(r);
    }
    let (from, to) = (rows[0].height, rows[5].height);

    assert!(
        fetch_span(&refusing(), from, to).await.is_empty(),
        "one refusing courier: nothing fetched"
    );

    let ladder = ladder_of(refusing(), serving);
    let fetched = fetch_span(&ladder, from, to).await;
    assert_eq!(
        fetched.iter().map(|h| h.hash.clone()).collect::<Vec<_>>(),
        rows.iter().map(|h| h.hash.clone()).collect::<Vec<_>>(),
        "every height, in order, from the courier that serves"
    );
    let tally = ladder.summary();
    assert!(
        tally.contains(&format!("woc ok 0 faults {RUNG_FAULT_CAP} (skipped)"))
            && tally.contains("arcade ok 6 faults 0"),
        "a rung that faults three times is skipped for the rest of the call: {tally}"
    );
}

/// A span ends at the first height no courier serves; what was fetched below
/// it is kept (the operator re-runs from the gap).
#[tokio::test]
async fn h13_the_backfill_ends_at_the_first_height_no_courier_serves() {
    let rows = {
        let db = SqliteDb::migrated();
        chain_of(&db, 4).await
    };
    let a = ScriptedChain::new();
    let b = ScriptedChain::new();
    a.publish(&rows[0]);
    b.publish(&rows[1]);
    // rows[2] is served by no one; rows[3] by both
    a.publish(&rows[3]);
    b.publish(&rows[3]);
    let ladder = ladder_of(a, b);
    let fetched = fetch_span(&ladder, rows[0].height, rows[3].height).await;
    assert_eq!(fetched.len(), 2, "the two below the gap");
    assert_eq!(fetched[1].hash, rows[1].hash);
}

// ─── H14, H15: the bootstrap ────────────────────────────────────────────────

fn bulk_file(name: &str, source_url: Option<&str>) -> crate::woc::BulkHeaderFileInfo {
    crate::woc::BulkHeaderFileInfo {
        file_name: name.to_string(),
        first_height: Some(0),
        source_url: source_url.map(str::to_string),
    }
}

/// H15 (`woc.rs:235-239` at `62cf619`): the URL of a bulk file was taken from
/// the listing's `sourceUrl` unchecked, so the listing could send the
/// bootstrap to any host. The file host is pinned: a `sourceUrl` that names
/// another host is ignored and the file is read from the pinned one.
#[test]
fn h15_a_source_url_naming_another_host_is_ignored() {
    let pinned = "https://cdn.projectbabbage.com/blockheaders/mainNet_0.headers";
    for foreign in [
        "https://bsv-headers.example.net",
        "https://cdn.projectbabbage.com.example.net/blockheaders",
        "https://example.net/cdn.projectbabbage.com/blockheaders",
        "http://cdn.projectbabbage.com/blockheaders",
        "https://user@example.net/blockheaders",
        "not a url",
        "",
    ] {
        assert_eq!(
            crate::woc::bulk_file_url(&bulk_file("mainNet_0.headers", Some(foreign))),
            pinned,
            "a sourceUrl naming {foreign:?} is ignored"
        );
    }
    // the pinned host's own sourceUrl (what the listing names today) and none
    for own in [Some("https://cdn.projectbabbage.com/blockheaders"), None] {
        assert_eq!(
            crate::woc::bulk_file_url(&bulk_file("mainNet_0.headers", own)),
            pinned
        );
    }
}

/// H14 (`routes.rs:753` at `62cf619`): the operator's bootstrap read the file
/// host and nothing else, though the upstream peer the cron already catches
/// up from (`getHeaders`, H10) answers the same need. The peer is asked
/// first; the file host is what remains when the peer cannot be had.
#[test]
fn h14_the_bootstrap_asks_the_upstream_peer_before_the_file_host() {
    let route = fn_text(ROUTES, "async fn admin_bulk_sync(");
    let peer = route
        .find("crate::sync::bootstrap_from_peer(")
        .expect("the bootstrap asks the upstream peer");
    let file_host = route
        .find("get_bulk_file_listing(")
        .expect("the file host stays as the fallback");
    assert!(peer < file_host, "the peer is asked ahead of the file host");
}

/// A scripted peer header service: it serves the rows it holds by height, at
/// most `cap` a request, or faults at and above `faults_from`.
struct ScriptedPeer {
    rows: Vec<BlockHeader>,
    faults_from: Option<u32>,
    asked: std::cell::RefCell<Vec<(u32, u32, Option<String>)>>,
}

impl ScriptedPeer {
    fn holding(rows: &[BlockHeader]) -> Self {
        Self {
            rows: rows.to_vec(),
            faults_from: None,
            asked: Default::default(),
        }
    }
}

impl HeaderPeer for ScriptedPeer {
    async fn headers(
        &self,
        start: u32,
        count: u32,
        expected_prev: Option<&str>,
    ) -> worker::Result<Vec<BlockHeader>> {
        self.asked
            .borrow_mut()
            .push((start, count, expected_prev.map(str::to_string)));
        if self.faults_from.is_some_and(|f| start >= f) {
            return Err(worker::Error::RustError("Production HTTP 503".into()));
        }
        Ok(self
            .rows
            .iter()
            .filter(|r| r.height >= start && r.height < start + count)
            .cloned()
            .collect())
    }
}

/// The harness's highest checkpoint root: a long run starts here, so it
/// crosses no other checkpoint of `host_harness::regtest`.
const TOP_ROOT: u32 = 965_898;

/// A linked run of `n` mined rows from the harness's highest checkpoint root.
fn run_of(n: u32) -> Vec<BlockHeader> {
    let mut rows = vec![header(TOP_ROOT, "x", &"1".repeat(64))];
    for h in TOP_ROOT + 1..TOP_ROOT + n {
        let parent = rows.last().unwrap().hash.clone();
        rows.push(header(h, &format!("rule 28 row {h}"), &parent));
    }
    rows
}

fn hashes(rows: &[BlockHeader]) -> Vec<String> {
    rows.iter().map(|h| h.hash.clone()).collect()
}

/// The peer serves the span: a batch at a time, each batch asked with the
/// hash it must link to, ending at the peer's own tip. The file host is not
/// read, and the run the peer served lands through the node's rules.
#[tokio::test]
async fn h14_the_peer_serves_the_bootstrap_and_the_file_host_is_not_read() {
    let rows = run_of(BOOTSTRAP_BATCH + 3);
    let peer = ScriptedPeer::holding(&rows);
    let got = bootstrap_from_peer(Some(&peer), TOP_ROOT, 100_000, None).await;
    let Bootstrap::Peer(headers) = got else {
        panic!("the peer served it: {got:?}");
    };
    assert_eq!(hashes(&headers), hashes(&rows));
    assert_eq!(
        *peer.asked.borrow(),
        vec![
            (TOP_ROOT, BOOTSTRAP_BATCH, None),
            (
                TOP_ROOT + BOOTSTRAP_BATCH,
                BOOTSTRAP_BATCH,
                Some(rows[BOOTSTRAP_BATCH as usize - 1].hash.clone())
            ),
        ],
        "two requests: a full batch, then the short one that is the peer's tip"
    );
    let db = SqliteDb::migrated();
    let inserted = crate::storage::insert_headers_batch(&db, regtest(), &headers)
        .await
        .unwrap();
    assert_eq!(inserted, BOOTSTRAP_BATCH + 3);
}

/// The first header the peer serves must link to our stored header below the
/// span; a peer on another branch is a fault and the file host is what remains.
#[tokio::test]
async fn h14_a_peer_that_does_not_link_to_our_store_hands_the_span_to_the_file_host() {
    let rows = run_of(4);
    let peer = ScriptedPeer::holding(&rows[1..]);
    let ours = block_hash("our header below the span, on another branch");
    let got = bootstrap_from_peer(Some(&peer), TOP_ROOT + 1, 100_000, Some(&ours)).await;
    let Bootstrap::FileHost { peer_fault } = got else {
        panic!("a foreign branch is never taken: {got:?}");
    };
    let fault = peer_fault.expect("the reason is kept");
    assert!(fault.contains("does not link"), "{fault}");
}

/// A peer fault, at the start or in the middle, hands the WHOLE span to the
/// file host (one span is never stitched from two sources), and the fault is
/// reported; no peer configured is no fault.
#[tokio::test]
async fn h14_a_peer_fault_hands_the_whole_span_to_the_file_host_with_its_reason() {
    let rows = run_of(BOOTSTRAP_BATCH + 3);
    for faults_from in [TOP_ROOT, TOP_ROOT + BOOTSTRAP_BATCH] {
        let mut peer = ScriptedPeer::holding(&rows);
        peer.faults_from = Some(faults_from);
        let got = bootstrap_from_peer(Some(&peer), TOP_ROOT, 100_000, None).await;
        let Bootstrap::FileHost { peer_fault } = got else {
            panic!("a faulting peer serves nothing: {got:?}");
        };
        let fault = peer_fault.expect("could not look is reported, never dropped");
        assert!(
            fault.contains(&format!("getHeaders at {faults_from}")) && fault.contains("503"),
            "{fault}"
        );
    }
    let empty = ScriptedPeer::holding(&[]);
    let got = bootstrap_from_peer(Some(&empty), TOP_ROOT, 100_000, None).await;
    assert!(
        matches!(&got, Bootstrap::FileHost { peer_fault: Some(f) } if f.contains("holds no header")),
        "{got:?}"
    );
    let none: Option<&ScriptedPeer> = None;
    let got = bootstrap_from_peer(none, TOP_ROOT, 100_000, None).await;
    assert!(
        matches!(got, Bootstrap::FileHost { peer_fault: None }),
        "no peer configured: the file host, and no fault to report"
    );
}

// ─── The ladder's reason, at the site ───────────────────────────────────────

/// The words of the rule, carried where the ladder is defined: why these
/// reads leave the service at all, and the shape that makes them safe.
#[test]
fn the_ladder_names_its_reason_at_the_site_in_the_words_of_the_rule() {
    let site = include_str!("couriers.rs");
    let doc: String = site
        .lines()
        .take_while(|l| l.starts_with("//!"))
        .map(|l| l.trim_start_matches("//!").trim())
        .collect::<Vec<_>>()
        .join(" ");
    for words in [
        "Rule 28",
        "headers come from outside the service",
        "the irreducible case of the whole stack",
        "re-derived locally",
        "proof of work",
        "the difficulty rule",
        "the checkpoints",
        "ancestry",
        "before it counts",
        "a negative needs a second provider",
        "\"could not look\" is never \"nothing there\"",
        "the start rotates",
        "break-glass",
    ] {
        assert!(doc.contains(words), "the site does not say: {words}");
    }
}

/// "A negative needs a second provider", and "could not look" is never
/// "nothing there": a header one courier does not serve is asked of the next;
/// when no courier serves it the answer is an error that names every rung's
/// fault. The ladder has no answer that means "no such block". (The shape was
/// already the ladder's at `62cf619`; this pins it under the rule's words.)
#[tokio::test]
async fn a_negative_is_asked_of_a_second_courier_and_every_negative_is_could_not_look() {
    let rows = run_of(2);
    let lacks = ScriptedChain::new();
    let holds = ScriptedChain::new();
    holds.publish(&rows[1]);
    let ladder = ladder_of(lacks, holds);
    let served = ladder.header_by_height(rows[1].height).await.unwrap();
    assert_eq!(served.hash, rows[1].hash, "the second courier answered");

    let absent = TOP_ROOT + 50;
    let e = ladder
        .header_by_height(absent)
        .await
        .expect_err("no courier serves it: an error, never an absence");
    let text = e.to_string();
    assert!(
        text.contains("every courier faulted")
            && text.contains(&format!("woc: header by height {absent}"))
            && text.contains(&format!("arcade: header by height {absent}")),
        "each rung's fault is named: {text}"
    );
    let e = ladder
        .header_by_hash(&block_hash("a block no courier holds"))
        .await
        .expect_err("by hash too");
    assert!(e.to_string().contains("every courier faulted"));
}

/// "The start rotates": the rung asked first is the start rung, so no courier
/// is the one everything leans on. (`for_chain` takes the start from the minute.)
#[tokio::test]
async fn the_start_rotates() {
    let rows = run_of(1);
    for (start, tally) in [
        (0, "woc ok 1 faults 0 · arcade ok 0 faults 0"),
        (1, "woc ok 0 faults 0 · arcade ok 1 faults 0"),
        (2, "woc ok 1 faults 0 · arcade ok 0 faults 0"),
    ] {
        let (a, b) = (ScriptedChain::new(), ScriptedChain::new());
        a.publish(&rows[0]);
        b.publish(&rows[0]);
        let ladder = CourierLadder::new(vec![("woc", a), ("arcade", b)], start, regtest().clone());
        ladder.header_by_height(rows[0].height).await.unwrap();
        assert_eq!(ladder.summary(), tally, "start {start}");
    }
}

// ─── The checkpoint at 965000, derived ──────────────────────────────────────

/// The owner's checkpoint entry in `wrangler.toml`, as `(height, hash)`.
fn configured_checkpoint() -> (u32, String) {
    let toml = include_str!("../wrangler.toml");
    let line = toml
        .lines()
        .find(|l| l.starts_with("CHECKPOINTS"))
        .expect("wrangler.toml carries CHECKPOINTS");
    let spec = line.split('"').nth(1).expect("a quoted value");
    let (height, hash) = spec.split_once(':').expect("height:hash");
    (height.parse().unwrap(), hash.to_string())
}

/// The long pass (Rule 28, the header service's seventh fix item): the hash
/// at the owner's checkpoint height derived by this service's own rules from
/// the node's last listed checkpoint (530359), with `CHECKPOINTS` unset, on a
/// local store; never the production store. The headers are a file of
/// concatenated 80-byte headers exported from the service's own `getHeaders`
/// (not copied here: about 35 MB). Run by hand:
///
/// `RULE28_HEADERS_FILE=<path> RULE28_HEADERS_START=530000 cargo test --release --lib -- --ignored --nocapture the_long_pass`
///
/// Two derivations, each under the node's rules alone (proof of work, the
/// difficulty rule, the node's checkpoints, ancestry):
/// 1. the ingest, through `insert_headers_batch` (the writer every bulk path
///    uses), 2,000 headers a batch as the cron's re-validation chunk;
/// 2. the pass itself, `revalidate_step` restarted and run to `Complete`, as
///    `/admin/revalidate?restart=1` and the cron run it.
#[tokio::test]
#[ignore]
async fn the_long_pass_from_the_nodes_last_checkpoint_derives_the_hash_at_965000() {
    use crate::consensus::ChainParams;
    use crate::storage::{
        find_header_for_height, insert_headers_batch, read_validation_state, revalidate_step,
        update_chain_tip_to_highest, Revalidation, REVALIDATE_CHUNK, SQL_RESTART_VALIDATION,
    };
    use std::time::Instant;

    let path = std::env::var("RULE28_HEADERS_FILE").expect("RULE28_HEADERS_FILE");
    let start: u32 = std::env::var("RULE28_HEADERS_START")
        .expect("RULE28_HEADERS_START")
        .parse()
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len() % 80, 0, "whole headers");
    let chain: Vec<BlockHeader> = bytes
        .chunks(80)
        .enumerate()
        .map(|(i, c)| BlockHeader::from_bytes(c, start + i as u32).unwrap())
        .collect();
    let tip = chain.last().unwrap().clone();

    // CHECKPOINTS unset: the node's own list and nothing else.
    let params = ChainParams::main();
    let (cp_height, cp_hash) = configured_checkpoint();
    let node_last = params.checkpoints.last().unwrap().0;
    assert_eq!(node_last, 530_359, "the node's last listed checkpoint");
    assert!(
        params.checkpoint_at(cp_height).is_none(),
        "the owner's checkpoint is NOT in the rules this pass runs under"
    );
    assert!(start <= node_last && tip.height >= cp_height);
    eprintln!(
        "long pass: {} headers, {start}..={}, rules: the node's {} checkpoints (last {node_last}), CHECKPOINTS unset",
        chain.len(),
        tip.height,
        params.checkpoints.len()
    );

    // 1. the ingest, every batch through the node's rules
    let db = SqliteDb::migrated();
    let t = Instant::now();
    let mut inserted = 0u32;
    for batch in chain.chunks(REVALIDATE_CHUNK as usize) {
        inserted += insert_headers_batch(&db, &params, batch)
            .await
            .unwrap_or_else(|e| panic!("the batch at {} was refused: {e}", batch[0].height));
        update_chain_tip_to_highest(&db).await.unwrap();
    }
    let ingest = t.elapsed();
    assert_eq!(inserted as usize, chain.len());
    let by_ingest = find_header_for_height(&db, cp_height)
        .await
        .unwrap()
        .expect("a row at the checkpoint height")
        .hash;
    eprintln!(
        "long pass: ingest: {inserted} headers in {:.1} s; hash at {cp_height}: {by_ingest}",
        ingest.as_secs_f64()
    );

    // 2. the pass, restarted and run to the tip
    crate::d1::Query::new(SQL_RESTART_VALIDATION)
        .run(&db)
        .await
        .unwrap();
    let t = Instant::now();
    let mut steps = 0u32;
    loop {
        steps += 1;
        match revalidate_step(&db, &params, REVALIDATE_CHUNK)
            .await
            .unwrap()
        {
            Revalidation::Advanced { .. } => {}
            Revalidation::Complete => break,
            Revalidation::Faulted { fault } => panic!("the pass stopped: {fault}"),
        }
    }
    let pass = t.elapsed();
    let state = read_validation_state(&db).await.unwrap();
    assert!(state.complete && state.fault.is_none(), "{state:?}");
    assert_eq!(state.validated_height, Some(tip.height));
    assert_eq!(state.validated_hash.as_deref(), Some(tip.hash.as_str()));
    let by_pass = find_header_for_height(&db, cp_height)
        .await
        .unwrap()
        .unwrap()
        .hash;
    eprintln!(
        "long pass: pass: {} rows above {node_last} in {steps} steps of {REVALIDATE_CHUNK}, {:.1} s; complete at {} {}",
        tip.height - node_last,
        pass.as_secs_f64(),
        tip.height,
        tip.hash
    );
    eprintln!("long pass: DERIVED hash at {cp_height}: {by_pass}");
    eprintln!("long pass: wrangler.toml entry  {cp_height}: {cp_hash}");
    assert_eq!(by_ingest, by_pass);
    assert_eq!(
        by_pass, cp_hash,
        "the derived hash is not the configured checkpoint: STOP, the captain and the owner decide"
    );
    eprintln!("long pass: MATCH");
}

/// The entry the long pass derives is the owner's addition, not the node's:
/// the node's list ends at 530359, which is where the derivation starts.
#[test]
fn the_configured_checkpoint_is_above_the_nodes_list_and_cites_its_derivation() {
    let (height, hash) = configured_checkpoint();
    let node = crate::consensus::ChainParams::main();
    assert_eq!(node.checkpoints.last().unwrap().0, 530_359);
    assert!(height > 530_359 && node.checkpoint_at(height).is_none());
    let with = node
        .with_checkpoints(&format!("{height}:{hash}"))
        .expect("the entry parses");
    assert_eq!(with.checkpoint_at(height), Some(hash.as_str()));
    let toml = include_str!("../wrangler.toml");
    let comment: String = toml
        .lines()
        .take_while(|l| !l.starts_with("CHECKPOINTS"))
        .filter(|l| l.starts_with('#'))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        comment.contains("OUR OWN DERIVATION") && comment.contains(&hash),
        "the entry cites the run that derived it"
    );
    for explorer in ["JungleBus", "Bitails", "WhatsOnChain"] {
        assert!(
            !comment.contains(explorer),
            "no explorer's word stands behind the entry: {explorer}"
        );
    }
}
