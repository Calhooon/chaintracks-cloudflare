# chaintracks-cloudflare

A BSV block-header service on Cloudflare Workers, with D1 header storage, R2
bulk export and a Durable Object for tip-stream ingestion. **0.3.1** is the
version in [Cargo.toml](Cargo.toml). This service has **no bsv-rs dependency**;
its header validation code is in [src/consensus.rs](src/consensus.rs).

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
reconnect behavior. The service also provides header/root queries, bulk sync,
link checks and controlled repair routes. It does not require clients to poll
the whole chain.

## Health and beta

`GET /health` returns the service status plus `build.lib_rev`, `build.dirty`,
`build.built_at` and `build.crates`, including this service's package version.
It does no database or outbound work. It identifies the compiled artifact;
`/getInfo` supplies readiness, validation and lag information.

[wrangler.toml](wrangler.toml) includes a separate beta environment with its own
D1, R2 and Durable Object bindings. [docs/BETA.md](docs/BETA.md) covers private
configuration, migrations, bootstrap, readiness and the public-tag build.
Build and deploy from a clean public release checkout, configure resources
privately, apply migrations, seed the header store and check validation and
lag before treating beta as ready. Deployments remain operator actions.

## Build and gate

With Rust's wasm target and worker-build 0.7.5 available, the five local gates
are:

```sh
cargo fmt --all -- --check
cargo clippy --target wasm32-unknown-unknown -- -D warnings
cargo check --target wasm32-unknown-unknown
cargo test --lib
worker-build --release
```

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs `test`, `fmt`, `clippy`,
`wasm32` and `worker` on PR heads and main pushes. It also checks host clippy
and generates the chain-event witness before running every compiled-Worker
script. Those scripts in [tests](tests) cover bootstrap, link repair, E5 push,
event/SSE delivery, health and the outbound fallback rules. The event test
requires the generated witness; the bootstrap test obtains bulk header fixtures.
Host tests alone do not establish the compiled Worker's behavior.

## Records and license

[CHANGELOG.md](CHANGELOG.md) records changes, [docs/A3-32-REPORT.md](docs/A3-32-REPORT.md)
records event-feed witnesses, and [src/testdata/README.md](src/testdata/README.md)
describes header fixtures. See [LICENSE](LICENSE).
