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
