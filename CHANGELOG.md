# Changelog

## [0.7.1] - unreleased

- `/getInfo` carries `lastPassAt` (#27, asked by bsv-low #877): the end of the
  last completed sync pass, idle passes and ticks the push covers included,
  from the new column `sync_state.last_pass_at` (migration
  `0013_last_pass_at.sql`, additive, one column). A consumer tells a quiet
  chain (a fresh `lastPassAt`, an aging `lastSyncedAt`) from a stalled tracker
  (both aging). A pass no courier answered, or one that failed, records
  `lastSyncError` as before and does not stamp. `lastSyncedAt` stays the last
  advance (M6) and `lastSeenAt` the couriers' answer: neither meaning moves.
- The load: one `UPDATE` of the single `sync_state` row a pass, 1,440 D1
  writes a day at the minute cron, bounded by the cron and not by the chain;
  the one deliberate exception to "the idle cron writes nothing" (round 5,
  LOW-4). `/getInfo` adds one read of the same row. Before the migration the
  stamp logs and the cron still syncs; `/getInfo` names the column in
  `syncSchemaFault`.
- The compiled-Worker gate: a quiet tick under both feeds answers `/getInfo`
  with a newer `lastPassAt` and the same `lastSyncedAt` and `lastSeenAt`.

## [0.7.0] - 2026-10-23

The chain-event feed's four changes and the edge half of #16, from
bsv-stack-lean `docs/p0/chaintracks-reorg-feed.md` (ruled 2026-10-24), each
behind a var; with both vars unset the feed is 0.6.0's and the header routes
change only in the `no-cache` below.

- `CHAIN_EVENT_FEED`: unset or `"1"` the 0.6.0 feed exactly; `"2"` the four
  changes. Read once per invocation.
- Change 1 (CF3): under `"2"` the minute's timer journals no `tipAge` row on
  an unchanged tip; the `tipAge` row at each tip change stays. The SSE
  heartbeat comment carries `tipAge=<s>`.
- Change 2 (CF4): under `"2"` the page, the stream's open and each wake read
  the rows, the head and the served tip in one D1 query; the stream ends with
  `retry: 1000` and EOF at 900 queries in one invocation, never
  `feedUnavailable` for the bound. `CHAIN_EVENT_STREAM_BOUND` is a test hook
  that can only lower the bound.
- Change 3 (CF2): migration `0012_chain_event_floor.sql` adds
  `chain_event_state.floor` (default 0) and nothing else. Under `"2"` the tick
  deletes chain rows older than 90 days and `tipAge` rows older than 24 h, at
  most 1,000 a tick, never the head row, and raises the floor in the same
  transaction; a page or a stream below the floor answers HTTP 410
  `{"v":1,"error":"cursorExpired","floor":"<n>","tip":{height,hash}}`.
- Change 4: under `"2"` the outbox reads the head and every target's cursor
  once a tick; a caught-up target costs nothing more.
- The edge (#16, CF5): every height within 400 of the served tip answers
  `Cache-Control: no-cache` on the height, hash, range and root routes,
  replacing 0.6.0's `public, max-age=3600` there, whatever the vars.
  `HEADER_EDGE_DEPTH` (a number, at least 400) keeps settled answers in
  `caches.default` with `public, max-age=86400` and `Cache-Tag: h-<height>,
  hb-<height/1000>`; `/admin/ingest`'s replacement and the new
  `POST /admin/edge-purge?from=&to=` purge them locally and by tag through the
  zone (`HEADER_EDGE_ZONE_ID`, the secret `HEADER_EDGE_PURGE_TOKEN`).
- The compiled-Worker gate runs every script under both feeds; the events
  script carries the witness scenario.

## [0.6.0] - 2026-10-22

- bsv-rs from `=0.7.2` to `0.8.1` (the caret; one bsv-rs per lock with the
  middleware, on ^0.8 since 2026-10-22). 0.8.1 reads atomic BEEFs on the
  subject's graph, types the wallet's busy answer and marks `Error`
  non_exhaustive; this crate uses only `block` and `primitives`, whose
  source is the same at 0.8.1 as at 0.7.2, and matches no `bsv_rs::Error`.
  No wire change. A minor version because `ChainParams::pow` and
  `BlockHeader::wire` carry bsv-rs types.

## [0.5.0] - 2026-10-19

- bsv-rs from `=0.6.0` to `=0.7.2`, the same `default-features = false` and
  `block` feature. Every varint read was re-read against 0.7's canonical
  rule (`Reader::read_var_int` refuses a non-canonical encoding): this crate
  reads none. It parses only the 80-byte header frame, through
  `bsv_rs::block::BlockHeader::from_binary`, so nothing moved and no lenient
  read was added.
- The difficulty rule's copy is deleted. `next_work_required`, the legacy
  retarget with the emergency adjustment, testnet's minimum-difficulty rule,
  the DAA, `GetMedianTimePast` and the chain-work sums were this crate's port
  of bitcoin-sv v1.2.3 `src/pow.cpp`; bsv-rs 0.7.2 carries that port
  (`bsv_rs::block::{next_work_required, ChainParams, HeaderAncestry,
  DifficultyError}`) and `src/consensus.rs` now calls it. The host's
  `ChainParams` keeps the network, the genesis hash and the checkpoints and
  holds bsv-rs's parameters as `pow`; `Window` is the store's
  `HeaderAncestry`, answering the chain work between two heights from its
  prefix sums. `check_checkpoint` stays here.
- A header the rule cannot answer where the node asserts or throws (a parent
  too low for its regime, no work between the DAA's suitable blocks) is
  refused as `no-difficulty-answer`; before, the first was
  `ancestry-missing` at a height below the genesis and the second a zero
  target. A missing stored ancestor is `ancestry-missing` as before. Neither
  case arises on main or test past their first 2016 headers.
- The 8 consensus tests run against bsv-rs's rule through the service's
  `Window` (the node's `pow_tests.cpp` tables); the whole-EDA-era replay stays
  ignored. `src/consensus.rs` is 909 lines, from 1122.

## [0.4.3] - 2026-10-18

- Publishable to crates.io as `bsv-chaintracks-cloudflare`; the library keeps
  `rust_chaintracks`, so no `use` path moves. The manifest gains the
  description, repository, readme and an `include` list (the sources and
  their header fixtures, the migrations, `wrangler.toml`, the docs, the
  README, the licence and this changelog).
- `.github/workflows/release.yml` publishes on a `v*` tag through crates.io
  trusted publishing: the tag must equal the manifest version and sit on
  `main`, the CI gate runs (`ci.yml` is now callable), and the package is
  verified with `cargo publish --dry-run` on the host before the publish.
- README refreshed to the code at this tag, with a Releases section. The
  Worker's behaviour, routes, bindings and wrangler config are unchanged.

## [0.4.2] - 2026-10-18

- The page and stream routes deliver every stored chain event at its cursor.
  A stored row the service cannot decode goes out in its place as
  `{"v":1,"kind":"unreadable","cursor":<n>,"fault":"<the decode fault>"}`
  (poll page, SSE stream and webhook body alike), never dropped and never an
  HTTP 500 for the page that holds it; `hasMore`, `cursorAhead` and
  `feedUnavailable` are unchanged. A version-1 reader stops at that cursor.
  The compatibility views send its `id:` line alone (chaintracks-cloudflare
  #8: ten such rows on the beta answered 500 to every page holding them).
- Migration 0011 rewrites the two fork triggers of 0009 with one condition:
  no fork is journaled when the competitor's nearest active ancestor stands
  at or above the served tip. A bulk span writes its rows active before it
  moves the tip flag, so the cron's next header landed inactive above a
  lagging tip and 0009 wrote `fork` with depth -1 or 0, which the decoder
  refuses. Existing rows are not rewritten.

## [0.4.1] - 2026-10-16

- Add `rungTips` to `/getInfo`: each configured courier's latest highest tip,
  implementation label, observation time, and current stored work and active
  membership. Polls ingest advertised competing tips through the existing
  validation path, including tips below the highest provider's height.
- Add version-2 `split` and `splitResolved` chain events, keyed by the common
  ancestor hash. Both branches must reach `SPLIT_DEPTH` (default 3); the D1
  snapshot and event commit atomically, once per fork point. Existing
  version-1 events and transport shapes stay readable. Strict consumers must
  support version 2 before deployment. See `docs/SPLITS.md` for the schema,
  bounded ancestry walk, lifecycle, migration 0010 and replay implications.
- Add the critical `checks.rungSpan` configuration check to `/health`, with
  the labels present and `warn` unless `reference` and `teranode` or `peer`
  are represented. Label overrides use Worker vars. Warnings keep serving.
- Keep the store's most-work tip rule and the crate version unchanged in
  this change; the release commit sets the version.

## [0.4.0] - 2026-10-14

- **The primitives from bsv-rs 0.6.0.** `bsv-rs = "=0.6.0"` (default features
  off, `block`) is the home of the 80-byte header frame and its hash, the
  compact target, the proof-of-work check, the per-block and cumulative chain
  work, `U256` and the pow limits. The service's own copies are deleted (the
  256-bit arithmetic, `SetCompact` and `GetCompact`, `compute_block_hash`, the
  hand-written frame) and `sha2` is no longer a direct dependency. The
  difficulty rule (the legacy retarget, the emergency adjustment, the DAA)
  stays here, on the bsv-rs types. Stored and served hashes, the 64-hex chain
  work and the refusal texts are unchanged; `/health` now names `bsv-rs` in
  `build.crates`, as the lock carries it. One stricter edge: a header frame is
  exactly 80 bytes (every caller already hands it an 80-byte chunk).
- **The deny file.** `deny.toml` is the stack's template byte for byte;
  `cargo deny check` passes advisories, bans, licenses and sources.

## 0.3.1

- Keep host all-target clippy clean: document Worker helpers retained for host
  tests and simplify test assertions, borrowing and item order. Worker behavior
  and validation rules are unchanged.

- Fix bulk bootstrap across a legacy retarget: load the stored ancestry
  needed by every header in the batch, accounting for ancestors supplied
  by the batch itself. The span at 100000 now reaches the stored interval
  needed at 100800. Proof of work, difficulty and checkpoint rules are
  unchanged; a truly missing ancestor still refuses the entire batch.
- Add a compiled-Worker Miniflare witness with the two cached public
  100,000-header files, plus host tests for split retarget windows and a
  missing stored ancestor. No migration or caller change.

## 0.3.0

### L2: the readable build and isolated beta

- **The build.** `GET /health` answers the service, status and build revision,
  dirty flag, UTC build time and `crates`: this crate's own version, and the
  lockfile's `bsv-rs` only when the lock carries it (this service has no such
  dependency today, so the key is absent, never a guess). An export without
  git answers `unknown` for the revision. `/getInfo` stays as it is.
- **The beta.** Its own D1, R2 and `TipStream`, the minute cron, no tip webhook
  or app-layer service binding. Resource and endpoint placeholders are rendered
  privately by the captain; `docs/BETA.md` records bootstrap and deploy steps.
- **The proof.** A host shape test and a compiled-Worker health check without
  a database binding or outbound access.


### E5: the push source (Rule 28), 2026-10-09

Rule 28 (the owner, 2026-10-09): a routine poll is the smell of a proof or a
feed we should hold. The minute poll of the couriers for the tip was the one
routine read of this service; the owner's ruling E5 (a) puts a push ahead of
it.

- **The one door.** `push::ingest_announced`: every pushed header is refused
  at once on its own proof of work, `known` when it is already served,
  otherwise its missing parents are fetched by hash through the courier
  ladder (at most 36) and the run goes through `storage::ingest_pushed`, the
  operator's ingest of #33. A refusal writes nothing and is recorded in
  `sync_state.last_error`; an accepted push records the seen height and
  announces the tip.
- **The Durable Object.** `TipStream` holds the outbound SSE tip stream of
  `ARCADE_URL` + `/chaintracks/v2/tip/stream` (Arcade's chaintracks v2), the
  last event id in its storage, an alarm that runs 10-minute sessions,
  reconnects on a drop with a backoff (1 s doubling to 60 s) and drops a
  stream silent for 45 s. The cron wakes it if it is gone.
- **The poll stays as the fallback.** The minute cron asks the couriers while
  the stream is down, and once in 10 minutes behind a live stream with no
  push; otherwise the tick runs everything that reads no courier (the tip's
  age, the re-validation step, the announce, the work repair). The poll's
  courier requests are counted in the object.
- **Deploy.** `wrangler.toml` gains the `TIP_STREAM` binding and the
  `e5-tip-stream` migration (`new_sqlite_classes`). No D1 migration. The
  deploy is the owner's go.
- **Tests.** `src/push_tests.rs` (8), `src/sse.rs` (6), `src/tip_stream.rs`
  (5); `tests/worker_e5.mjs` on the compiled Worker in Miniflare with a
  scripted SSE peer. The suite: 253 at `a849f4f`, 272 here.
