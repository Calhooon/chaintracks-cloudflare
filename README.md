# chaintracks-cloudflare

A BSV block-header service on Cloudflare Workers, with D1 header storage, R2
bulk export and a Durable Object for tip-stream ingestion. **0.5.0** is the
version in [Cargo.toml](Cargo.toml); the crate is `bsv-chaintracks-cloudflare`
and its library keeps the name `rust_chaintracks`. The header frame, compact
target, proof-of-work check, chain work and the difficulty rule come from
`bsv-rs` 0.7.2 (exact); the checkpoints and the store's ancestry window are in
[src/consensus.rs](src/consensus.rs).

## Headers, proofs and events

Headers pass proof-of-work, difficulty, checkpoint and ancestry checks before
they can be served as the active chain. Merkle-root queries use that verified
header view. During revalidation, the served tip is limited to the validated
cursor. [src/storage.rs](src/storage.rs) owns insertion and chain selection;
[src/routes.rs](src/routes.rs) owns the query and admin routes.

The E5 push source is the `TipStream` Durable Object in
[src/tip_stream.rs](src/tip_stream.rs). It reads a peer's tip stream, checks
announced headers through [src/push.rs](src/push.rs), and retrieves missing
parents through the courier ladder in [src/couriers.rs](src/couriers.rs).
The minute cron wakes the object and polls when push coverage is absent or
quiet. A courier supplies bytes, not authority over chain selection.

Consumers can read the persisted chain-event feed through polling and SSE;
[docs/CHAIN-EVENTS.md](docs/CHAIN-EVENTS.md) defines the envelopes, cursor and
reconnect behavior; [docs/SPLITS.md](docs/SPLITS.md) defines the version-2
`split` and `splitResolved` events. The service also provides header/root
queries (`/findHeaderHexForHeight`, `/isValidRootForHeight` and the
`/chaintracks/v2` and `/v2` views), bulk sync, link checks and controlled
repair routes under `/admin/`. It does not require clients to poll the whole
chain. `/getInfo` carries `rungTips`, each configured courier's latest tip.

## Freshness on `/getInfo`

Three times answer three questions, each from the single `sync_state` row:

| field | moves when | means |
|---|---|---|
| `lastSyncedAt` | a new tip's claim (audit M6) | the last advance |
| `lastSeenAt` | a cron with work; an idle cron only when the record is behind the couriers' answer | the couriers' highest answer |
| `lastPassAt` | the end of every completed sync pass, idle passes and ticks the push covers included (migration 0013) | the liveness signal: a pass ran |

On a quiet chain `lastSyncedAt` ages while `lastPassAt` stays a minute old; a
stalled tracker ages both. A pass no courier answered, or one that failed,
records `lastSyncError` and `lastSyncErrorAt` and leaves `lastPassAt`.

The load: one `UPDATE` of the `sync_state` row a pass, 1,440 D1 writes a day
at the minute cron, bounded by the cron and not by the chain. It is the one
deliberate exception to "the idle cron writes nothing" (round 5, LOW-4), which
still holds for the courier record and the tip announce; the minute's age
writes of the chain-event feed are separate. `/getInfo` reads the stamp with
one more read of the same row; before migration 0013 it omits `lastPassAt`
and names the missing column in `syncSchemaFault`, and the cron still syncs.

## The link state on `/getInfo`

Every pass (idle passes and ticks the push covers included) checks the links
of the top 400 heights of the active chain: each active row must be the
parent the active row above it names. `/getInfo` carries the answer as
`linkState` (migration 0014): `from`, `to`, `linked`, `broken` (the first 32
heights whose active row the next height does not name) and `brokenCount`.
`linked: false` is a served chain with a hole, a read and not a stall a
consumer finds (#30, the stall of 2026-11-02).

A poll that finds the chain broken heals it with no operator: from the
highest linked active row it walks forward by height through the courier
ladder (at most 100 heights a pass) and hands the run to the path
`POST /admin/ingest` runs, which activates it under the stored child that
names it. A height no courier serves is recorded as `lastSyncError` and
`linkState` stays `linked: false`. A tick the push covers heals the same
way through the same ladder (#31); a linked chain asks no courier there. A
heal costs at most one courier request a rung a height (three on main),
at most 300 a pass. A heal stays inside the writer lease's term (#34): it
reads 10 heights at a time and the lease's remaining term before each
span, goes on while half the term and twice its longest span are left,
and otherwise writes what it read and stops; `linkState` records where it
stopped and the next pass goes on from there. A break more than 400 heights under the tip is seen by
no pass, only by `/admin/linkcheck` (a slow full pass is issue #33 of this
repository).

## One writer at a time

The cron's pass, a tick the push covers, a push through the tip stream's
door, the public read-through (a request up to six blocks above the tip)
and `/admin/bulk-sync` are five writers of one chain state; on 2026-11-02
18:48Z a reorg sync and a push of the same headers interleaved (#32). Each
now takes the writer lease (migration 0015: `sync_state.lease_owner`, `lease_until`) with one
`UPDATE` before it writes and gives it back after; a writer that finds it
held writes nothing, and a holder that died frees it after 120 s. A pass
turned away waits for the next tick and is no poll (the quiet interval is
not restarted by it); a push turned away answers `busy` and the object asks
again, 15 times 2 s apart, then drops it with one line naming the header
(`tip stream: DROPPED the push of ...`, kept as `last_fault` on the
object's `/status`; `busy` counts events, not asks). A read-through turned
away answers "unable to verify" for now (the holder is landing the chain);
`/admin/bulk-sync` turned away answers 503 and writes nothing, run it
again. A push turned away, or refused because its ancestry could not be
had, records its height as `lastSeenHeight` (within 37 of the served tip),
so the behind rule counts from the next tick. The load: two `sync_state`
writes per pass, per tick, per read-through, per bulk-sync call and per push
that is not `known`, and one read per 10 heights a heal reads. Before
migration 0015 every writer logs the missing column and writes
unserialized, as before. `/admin/ingest` and `/admin/backfill` do not take
it.

Behind a live push the cron still polls when the stream is down, when no
push that advanced the active chain and no poll landed in the quiet
interval (a re-sent tip the store keeps inactive is not heard), or when the
store's tip sat below `lastSeenHeight` for more than one tick.

The load: four reads a pass (three bounded by the window, about 1,200 rows,
and the stored state), and a write of the `sync_state` row only when the answer changed (a new tip), so an
idle pass on a linked chain writes `lastPassAt` and the writer lease's two
writes (its take and its give-back) alone. Before migration
0014 the leg logs, the cron still syncs, and `/getInfo` names the column in
`syncSchemaFault`.

## Vars

The deploy's `[vars]` and secrets that shape the feed and the header routes
(the rest are in [wrangler.toml](wrangler.toml)):

| name | kind | unset | set |
|---|---|---|---|
| `CHAIN_EVENT_FEED` | var | the 0.6.0 feed | `"2"`: no minute `tipAge`, one query a stream wake and a clean end at 900, the 90-day retention and 410 `cursorExpired`, the outbox's one head read ([docs/CHAIN-EVENTS.md](docs/CHAIN-EVENTS.md)) |
| `HEADER_EDGE_DEPTH` | var | no cache; 0.6.0 lifetimes more than 400 below the tip, `no-cache` within | the settled line (at least 400): settled answers from `caches.default` for a day, tagged |
| `HEADER_EDGE_ZONE_ID` | var | no zone purge (the colo's `cache.delete` only) | the zone whose tags a purge names |
| `HEADER_EDGE_PURGE_TOKEN` | secret | as above | the purge token, read by `env.secret` alone |
| `CHAIN_EVENT_WEBHOOK_URLS`, `CHAIN_EVENT_WEBHOOK_TOKEN` | var, secret | no outbox | the outbox targets and their bearer |
| `CHAIN_EVENT_STREAM_BOUND` | var | 900 | a test hook: a lower stream bound, never a higher one |

## Health and beta

`GET /health` returns the service status plus `build.lib_rev`, `build.dirty`,
`build.built_at` and `build.crates` (the package version under the key
`rust-chaintracks`, and `bsv-rs`), plus the `checks.rungSpan` configuration
check. It does no database or outbound work. It identifies the compiled artifact;
`/getInfo` supplies readiness, validation and lag information.

[wrangler.toml](wrangler.toml) includes a separate beta environment with its own
D1, R2 and Durable Object bindings. [docs/BETA.md](docs/BETA.md) covers private
configuration, migrations, bootstrap, readiness and the public-tag build.
Build and deploy from a clean public release checkout, configure resources
privately, apply migrations, seed the header store and check validation and
lag before treating beta as ready. Deployments remain operator actions.

## Build and gate

With Rust 1.95.0, its wasm target and worker-build 0.7.5 available, the cargo
gates [ci.yml](.github/workflows/ci.yml) runs are:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings
cargo test --no-fail-fast
worker-build --release
```

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs `test`, `fmt`, `clippy`,
`wasm32` and `worker` on PR heads and main pushes. It also checks host clippy
and generates the chain-event witness before running every compiled-Worker
script. Those scripts in [tests](tests) cover bootstrap, link repair, E5 push,
event/SSE delivery, health and the outbound fallback rules. The event test
requires the generated witness; the bootstrap test obtains bulk header fixtures.
Host tests alone do not establish the compiled Worker's behavior.

## Releases

The tag (`v0.4.3` and on) is the release of record. From this release on, the
crate `bsv-chaintracks-cloudflare` on crates.io carries the same version as the
tag: [.github/workflows/release.yml](.github/workflows/release.yml) checks the
tag against `Cargo.toml` and its ancestry on `main`, runs the CI gate, then
publishes through crates.io trusted publishing. The Worker in production is
deployed from the tag by the stack's infra, not by this workflow. There are no
earlier crates.io entries under this name or the old `rust-chaintracks`.

## Records and license

[CHANGELOG.md](CHANGELOG.md) records changes, [docs/A3-32-REPORT.md](docs/A3-32-REPORT.md)
records event-feed witnesses, and [src/testdata/README.md](src/testdata/README.md)
describes header fixtures. See [LICENSE](LICENSE).
