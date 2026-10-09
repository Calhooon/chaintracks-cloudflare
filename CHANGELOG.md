# Changelog

## Unreleased

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
