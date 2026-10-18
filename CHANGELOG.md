# Changelog

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
