# Changelog

## [0.7.6] - 2026-11-28

The bsv-rs floor at 0.8.14. No source line of this crate moves, and no migration is added (the last is `0015_writer_lease.sql`):

- The bsv-rs requirement is `0.8.14` (it was `0.8.12`; a caret requirement still, `default-features = false`, `features = ["block"]`). The tracked `Cargo.lock` moves bsv-rs alone (0.8.12 to 0.8.14, its version and checksum, with this crate's own version): 0.8.14 requires no dependency differently. `/health`'s `build` names 0.8.14.
- 0.8.14 closes ten advisories bsv-rs has published (its Security tab, read 2026-11-28), each affecting every 0.8 release up to 0.8.13. What this build compiles of bsv-rs is the `block` feature alone, which is `block` and `primitives` (bsv-rs's feature table at 0.8.14: `block = ["primitives"]`), with `error` and `util`. Every file of `src/block`, `src/primitives` and `src/util`, and `src/lib.rs`, is byte for byte the same in the 0.8.12 and 0.8.14 crate files; the two additions to `src/error.rs` are gated on `transaction` and `compat`. So none of the ten reaches this build, and no answer of this service moves:
  - `transaction`: GHSA-fm3p-cmwv-rf6h (`MerklePath`, `src/transaction/merkle_path.rs`), GHSA-grrj-gwrr-v7r3 and GHSA-63w6-rpmj-r9gg (`Transaction::verify`).
  - `script`: GHSA-56fp-4337-6vfh and GHSA-7vx5-cf5p-5c48 (script evaluation, `src/script/spend.rs`).
  - `wallet`, `auth` and `overlay`: GHSA-4gp5-q4cj-4qjf (the wallet wire, certificates and lookup answers).
  - `compat`: GHSA-43mf-m7fg-m74w (ECIES).
  - `messages`: GHSA-mqr8-3gvc-hhg3 (`messages::verify`).
  - `overlay`: GHSA-33px-hj7f-4cpj (`LookupResolver::query`).
  - `auth`: GHSA-wh67-gjx6-4qqh (`Peer::start`).
- No other dependency's requirement moves.

## [0.7.5] - 2026-11-17

The bsv-rs floor at 0.8.12. No source line of this crate moves, and no migration is added (the last is `0015_writer_lease.sql`):

- The bsv-rs requirement is `0.8.12` (it was `0.8.11`; a caret requirement still, `default-features = false`, `features = ["block"]`), the release that closes GHSA-33fm-jcm5-4m3m. The tracked `Cargo.lock` moves bsv-rs alone (0.8.11 to 0.8.12, with this crate's own version): 0.8.12 requires no dependency differently. `/health`'s `build` names 0.8.12.
- What 0.8.12 changes is the BEEF linker of the `transaction` feature alone (`src/transaction/beef.rs`, `src/transaction/beef/link.rs` and the new `src/transaction/beef/link_limits.rs`: a heap bound on linking, `BeefLinkLimits::max_link_heap_bytes`, 56 MiB by default, beside the wire bound). This build compiles the `block` feature (`block` and `primitives`), not `transaction`, and this Worker reads headers, not BEEFs: every file of `src/block`, `src/primitives`, `error` and `util` is byte for byte the same in the 0.8.11 and 0.8.12 crate files, so no answer of this service moves.
- No other dependency's requirement moves.

## [0.7.4] - 2026-11-17

The bsv-rs floor (#44). No source line of this crate moves, and no migration is added (the last is `0015_writer_lease.sql`):

- The bsv-rs requirement is `0.8.11` (it was `0.8.1`; a caret requirement still, `default-features = false`, `features = ["block"]`), the newest 0.8 release on the crates.io index that is not yanked (read 2026-11-17 13:42Z; none newer there). `Cargo.lock` is not tracked here, so the requirement is the floor of every build: the 0.7.3 build resolved 0.8.3, and a build of this version resolves 0.8.11 or later. `/health`'s `build` names the version it resolved.
- What this build compiles of bsv-rs is the `block` feature alone, which is `block` and `primitives` (bsv-rs's feature table: `block = ["primitives"]`), with `error` and `util`. What this Worker calls is `src/block` (the header frame, the compact target, chain work, the difficulty rule) and, of `primitives`, `hash.rs` and `u256.rs`: each of those files is byte for byte the same in the 0.8.3 and 0.8.11 crate files, so no answer of this service moves.
- Of the nineteen advisories bsv-rs has published (its Security tab, read 2026-11-17), three name code under `primitives`, which this build compiles, and all three are closed by the new floor. This Worker calls none of the three doors:
  - GHSA-9c8r-76f3-m426 (`Signature::from_der` read malformed encodings of a valid signature as that signature; `primitives::ec`), patched in 0.8.6.
  - GHSA-cwxv-4866-5gq2 (a Shamir backup share at x = 0 accepted at recovery, `KeyShares::from_backup_format`; `primitives::bsv::shamir`), patched in 0.8.7.
  - GHSA-67vc-6c8g-7x48 (`sighash::parse_transaction` read a transaction from the front of a longer buffer; `primitives::bsv::sighash`), patched in 0.8.11.
- The other sixteen name code under features this build does not compile (`transaction`, `script`, `auth`, `overlay`, `kvstore`, `registry`, `identity`), so none of that code is in the Worker at any version: GHSA-j8hr-pjhm-6h27, GHSA-84xq-3662-pvx5, GHSA-gg5p-wgff-v3hr, GHSA-mj2m-jxgv-r446, GHSA-89w7-x33x-6m9c, GHSA-4mcm-g5xq-jv3r, GHSA-w6jf-m9c7-6599, GHSA-crr5-gh8p-q2h2, GHSA-x3m6-jp5x-236c, GHSA-vmgj-w2jr-c52x, GHSA-qhj8-j8mj-9cgc, GHSA-fr33-7jw7-mwwh, GHSA-h4m7-c6p8-725m, GHSA-cgp9-rj5h-jxw6, GHSA-gvmh-f5gf-5vjj and GHSA-82g9-h6jw-r99f. Fifteen of them are patched at or below 0.8.11 all the same.
- The sixteenth, GHSA-82g9-h6jw-r99f (`Transaction::from_ef` and `from_hex_ef` allocate without a bound over the transaction), has no patched version: its remedy is the caller's, `from_ef_bounded` and `from_hex_ef_bounded`. Outside this entry the tree holds 0 occurrences of `from_ef` or `from_hex_ef` (the whole tree searched, tests and docs included), and the `transaction` feature that carries them is not compiled: there is nothing to bound here.
- No other dependency's requirement moves.

## [0.7.3] - 2026-11-04

The follow-ups of the orphan-tip fix (#31) under the writer lease (#34, PR #36), with the findings of its reads: Read A (the hand-off's ask count, the read-through's courier reads under the lease), Read B (the read-through holding the cron's lease, the fence, the admin writes outside the lease, the heal's yield over rival rows, the seen height), Read A2 (a heal witness that depended on the clock, the webhook sends inside the lease, a lapsed pass stamped as completed, bulk-sync's lapse answered 422) and Read B2 (the hand-off's retries per header past `waitUntil`'s 30 s, #38); #37 closes inside the heal, and #39 stays open:

- The public read-through holds no lease across its own life: it reads and judges the blocks without a write (`storage::judge_above`), answers from the judged header, and hands a run that passed to the push's door after its answer (`waitUntil`; asked again 9 times 2 s apart while the lease is held, then dropped with its line), so no courier is read under the lease; a block that does not stand on the stored tip answers "unable to verify" for now and the pass lands it. Before, it held the cron's 120 s lease around its courier reads, and a caller that went away left it held for the term.
- The hand-off runs under one budget (#38): 20 s for the whole run, its asks again (9 over the run, not per header), its waits and its sends together, inside `waitUntil`'s 30 s; every courier read and webhook POST in it is cut at 5 s or at the budget's end; a run stopped short logs and records (`lastSyncError`) the heights it leaves, and the poll lands them.
- The push's door gives the writer lease back before it records the seen height and sends the tip's webhooks (the tip stream's push and the hand-off alike): the lease holds the write alone, and a slow consumer no longer lengthens the hold.
- `/admin/bulk-sync`, `POST /admin/ingest` and `/admin/backfill` take the writer lease (the backfill after its courier reads); held elsewhere they answer 503 with the reason and write nothing. A bulk-sync whose lease lapses under its write answers 503 with the lapse as its reason (it answered 422, as a refused batch); the chunks written before stand until it runs again (#39).
- The fence: every write of the `headers` table a lease holder makes carries one guard statement in its own transaction (`sync::FENCE_SQL`, failing on `sync_state`'s `CHECK (id = 1)`), so a holder past its term, or one whose lease another writer took, writes nothing more; it logs one line, records the fault in `lastSyncError` and stops, and the next writer goes on from the rows it wrote. A pass longer than the term now stops at it instead of writing on unserialized. A pass or tick whose lease lapsed under it (the fence's refusal, or the heal finding the term spent before its write) is no completed pass: `lastPassAt` is not stamped, the lapse is the last fault recorded, and the pass answers it as an error.
- A poll the lease turned away is no poll: the object's quiet interval restarts only for a pass that ran (`/polled?ran=0`). A pass that ends in an error counts as not run too, so a store faulting every tick polls every tick instead of once a quiet interval.
- A push still turned away after its 16 asks (15 asked again) is dropped with one line naming the header (`last_fault`), and `busy` counts events, not asks.
- A push turned away through its asks, or refused because its ancestry could not be had (`ancestry-missing`, the parent walk's refusals), leaves its height on the object as a hint to poll (`hintHeight`): the behind rule counts it from the next tick until a poll runs. It never raises `lastSeenHeight` or `/getPresentHeight`.
- The heal walks back by hash from the row above each broken link, from the highest break down, through the store where the rows are held and the ladder where they are not, and activates the run the chain names whatever is active at its heights: a band of rival active rows and a hole two or more heights deep (#37) heal, where the forward walk read the same heights every pass for no progress.
- A heal stays inside the lease's term: the remaining term read before every 10 courier reads, yielding when less than half the term plus twice its longest step is left, and keeping the rows it read (inactive, never served) so the next pass goes on from them; a run that reaches no active row within 400 heights stops with that reason recorded.
- The README names the writers, the fence, the read-through's hand-off and the hint.

## [0.7.2] - 2026-11-03

- An orphan or broken tip heals without an operator (#30, the production stall
  of 2026-11-02 18:48Z to 2026-11-03 06:03Z at 973791). A bulk write
  (`insert_headers_batch`: the upstream catch-up, `/admin/backfill`, a push)
  activates the rows of its run it finds stored inactive where no row is
  active at their height, instead of skipping them under the active rows it
  writes above them; a dupe the poll reads that is stored inactive resumes
  the parent walk as an unlinked one did, and the catch-up's courier fallback
  goes through that walk; every pass checks the links of the top 400 active
  heights and a poll heals a broken one forward by height through the ladder
  (100 heights a pass) and the activating path of `POST /admin/ingest`.
  `/getInfo` carries the result as `linkState` (migration
  `0014_link_state.sql`, additive, one column, written only when it changes);
  `/admin/backfill` answers `leftInactive`, the heights where another row
  stays active.
- The fix round of #31 (its two reads; issue #32, the trigger read from the
  production logs). A tick the push covers heals a broken chain through the
  courier ladder too: every tick of the 2026-11-02 stall took that branch,
  and a linked chain asks no courier there. Only a push that advanced the
  active chain (`active`, `activated`) holds the poll off: a re-sent tip the
  store keeps inactive (`storedInactive`) no longer keeps the push "live"
  past the quiet interval, and a store whose tip sat below `lastSeenHeight`
  for more than one tick is polled behind a live push. The cron's pass, the
  tick the push covers and a push through the door write under one writer
  lease (migration `0015_writer_lease.sql`, additive, two columns, a 120 s
  term); a writer that finds it held writes nothing (the next tick; the
  object asks again, 15 times 2 s apart). The fast path of the activating
  path sets the parent link of every active row of the run and of the child
  above it, so no healed row keeps a null `previous_header_id`.
- What it costs and how far it sees. Two D1 writes (the lease's take and
  give-back) for each cron pass, each tick the push covers, and each push
  that is not `known`: 2,880 a day at the minute cron. One read a tick (the
  seen height against the tip, for the object's `/wake`). The link check
  covers the top 400 active heights a pass; a break deeper is seen by no
  pass, only by `/admin/linkcheck` (a slow full pass over the validated range
  is issue #33 of this repository). A heal reads one header a height through
  the ladder, at most one request a rung (three on main), at most 100
  heights a pass, and only while the chain is broken: at most 300 courier
  requests a pass. The catch-up's courier fallback goes through the parent
  walk, so a header whose parent is not stored costs at most 36 more reads
  by hash. The bulk writer's activation of a row stored inactive rewrites
  that row's `chain_work` to the run's cumulative work, on every bulk writer
  (the catch-up, `/admin/backfill`, `/admin/ingest`, a push, a heal); an
  activation that replaced rows records `pending_reorg_from`, so the next
  announce asks consumers to re-verify from it. `/admin/ingest` and
  `/admin/backfill` (the operator's) do not take the lease.

## [0.7.1] - 2026-10-25

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
