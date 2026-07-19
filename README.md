# chaintracks-cloudflare

BSV block header tracking service on Cloudflare Workers. Rust compiled to WASM.

Reimplementation of the Node.js [`chaintracks-server`](https://github.com/bsv-blockchain/chaintracks-server) as a single Cloudflare Worker, replacing a 2× VPS + Docker deployment. Serves block header queries and merkle root validation for SPV consumers.

## What it does

- Polls WhatsOnChain every minute (Workers cron) for new block headers
- Stores headers in Cloudflare D1 (~945k rows), serves bulk headers from R2
- Exposes 12 HTTP endpoints matching the original TS server's public API
- Detects and handles reorgs up to 400 blocks deep
- Validates merkle roots for SPV consumers

## Differences from Node.js chaintracks-server

Cloudflare Workers are stateless and request-based, so several TS server features are intentionally dropped or reshaped:

- **No event subscriptions.** TS exposes `subscribeHeaders()` / `subscribeReorgs()` for live callbacks. Workers cannot hold persistent channels — consumers should poll `/getInfo` or `/currentHeight`.
- **No WebSocket ingestion.** TS uses `LiveIngestorWhatsOnChainWs` for push ingest. Workers cannot hold outbound WebSocket connections. Rust uses HTTP polling via 1-minute cron.
- **Manual bulk header export.** TS auto-exports CDN bulk header files every 30 min. Rust exposes `/admin/export-r2` as a manual endpoint — trigger it directly or set up an external scheduler to call it.
- **No in-process watchdog.** TS runs a self-check loop that restarts its Docker container on stall. Workers handle restarts at the infra layer; redundant.
- **Simpler multi-source fallback.** TS reads Babbage CDN + WhatsOnChain with watchdog failover. Rust falls back to a configurable upstream chaintracks URL during catch-up, then direct WhatsOnChain calls.

None of these are missing features — they're architectural trade-offs for serverless. All public HTTP endpoints are at parity with TS.

## 2026-07 hardening release (v0.2.0)

This snapshot brings the public repo up to the fully hardened implementation
(audited against TS wallet-toolbox v2.4.0 chaintracks + the Go BHS tracker,
then adversarially reviewed; the chain-work math is verified against a bigint
harness on 10k randomized cases):

- **Chain-truth correctness**: exact 256-bit cumulative chain work
  (`2^256/(target+1)`, genesis vector `0x100010001`) with more-work tip
  selection; equal-height/competing-branch reorgs detected via
  `bestblockhash` and repaired with by-hash parent backfill (bounded 36,
  TS `addLiveRecursionLimit` parity); a refused reorg (no common ancestor)
  leaves the tip untouched; reorg walks apply as batched transactions.
- **No lies to consumers**: `/currentHeight` answers 503 (never a fake 0)
  when degraded; `/isValidRootForHeight` distinguishes "unable to verify"
  (404 — hole/above-tip/reorg window) from a factual root mismatch
  (`false`); `/getInfo` exposes `lastSyncedAt`/`lastSyncedHeight` freshness.
- **Ingest integrity**: header hashes recomputed from the 80 bytes and
  mismatches rejected; `badPrev` height-linkage check; linkage guards on
  every bulk path; R2 exports refuse misaligned files.
- **Fresh-block grace**: lookups within `tip+6` trigger a verified live
  read-through from WhatsOnChain (full validation, never blind acceptance),
  so fail-closed SPV consumers don't bounce proofs from just-mined blocks.
- **`/admin/*` is token-gated and fail-closed**: set the `ADMIN_TOKEN`
  worker secret (`npx wrangler secret put ADMIN_TOKEN`); until it exists,
  admin routes answer 503. Calls use `Authorization: Bearer <token>`.
- **`/v2` wire shim**: the go-chaintracks/Arcade v2 contract
  (`/v2/network`, `/v2/tip[.bin]`, `/v2/header/height/{h}[.bin]`,
  `/v2/header/hash/{hash}[.bin]`, `/v2/headers[.bin]`), validated against
  the ts-stack conformance vectors (20/20 testable vectors pass). SSE
  streams are not implemented (cron-pull architecture); v2 clients poll.

## Bulk/live storage split, /health, self-populating R2

Layers a canonical bulk/live storage split plus operational monitoring on top of
the above, so D1 stays well under its size ceiling as the chain grows (storing
the whole chain in D1 hits the free-tier size cap around ~795k rows).

- **Bulk/live split.** Immutable headers below `BULK_TOP_HEIGHT` (default
  `942761`, the projectbabbage CDN snapshot top) live as 80-byte records in the
  R2 bulk store; recent heights stay in the mutable D1 live window. Reads route
  by height (`findHeaderHexForHeight`, `isValidRootForHeight`, and the `/v2`
  header paths), with a CDN read-through fallback that refuses a non-80-byte
  response — so historical reads never fail while R2 is being populated. The
  per-tick full-table sweep then only ever touches the small live window.

- **`/health`.** A machine-readable readiness probe (200 / 503 + JSON), answered
  entirely from local D1 (no upstream call) so an uptime watchdog can poll it
  every minute. Returns 200 only when the tracked tip is within a small gap of
  the last-observed network tip, the cron heartbeat is fresh, and (at equal
  height) the tip hash matches the network's best block; otherwise 503 with a
  reason (`no-data` / `stale` / `behind` / `forked`). `/getInfo` also surfaces
  `wocTip` / `behindBy` / `isSyncing`, and each cron tick records its heartbeat
  before any CPU-heavy work so the freshness signal stays truthful even if a
  catch-up tick is cut short.

- **Self-populating R2** (optional, off by default). With `SELFPOP_CRON=on`,
  idle caught-up ticks stream the bulk header files from the CDN into R2 and
  verify them lazily in bounded chunks (full linkage + the CDN index last-hash,
  self-healing by delete on any mismatch) — removing the manual
  `wrangler r2 object put` step and the permanent per-read CDN dependency. It is
  **off by default** because a populate/verify unit measures ~16–50 ms CPU,
  above the Workers Free-plan *scheduled* (cron) ~10 ms limit; the operator
  `/admin/self-pop` route (fetch handler, higher budget) populates fine, or set
  `SELFPOP_CRON=on` on a paid plan for automatic self-population. Reads always
  fall back to the CDN, so an unpopulated or partially-populated R2 is safe.

## HTTP API

| Endpoint | Description |
|---|---|
| `GET /` | Health check (plain text) |
| `GET /getChain` | Returns `"main"` or `"test"` |
| `GET /getInfo` | Service status (height, header count, sync state) |
| `GET /currentHeight` | Current chain tip height |
| `GET /findChainTipHashHex` | Chain tip block hash |
| `GET /findChainTipHeaderHex` | Chain tip header (JSON object) |
| `GET /findHeaderHexForHeight?height=N` | Header at height N |
| `GET /findHeaderHexForBlockHash?hash=H` | Header by block hash (active chain only) |
| `GET /getHeaders?height=N&count=M` | M headers starting at N (concatenated hex) |
| `GET /isValidRootForHeight?root=R&height=N` | Validate merkle root at height |
| `GET /admin/bulk-sync?file=IDX` | Admin: ingest bulk header file from CDN |
| `GET /admin/export-r2` | Admin: export D1 headers to R2 bulk files |

## Architecture

```
Request  → lib.rs → routes.rs → storage.rs → D1
Cron 1m  → lib.rs (scheduled) → sync.rs → WhatsOnChain → D1
Bulk     → R2 bucket (CDN replacement)
```

- **lib.rs** — Worker entry: `#[event(fetch)]` + `#[event(scheduled)]`
- **routes.rs** — HTTP routing, 12 endpoints
- **storage.rs** — D1 read/write operations
- **sync.rs** — Cron-triggered chain sync, reorg detection
- **d1.rs** — Parameterized D1 query builder
- **types.rs** — `BlockHeader`, `Chain`, `ChaintracksInfo`, 80-byte serialization

## Cloudflare Bindings

| Binding | Type | Purpose |
|---|---|---|
| `DB` | D1 | Block header storage |
| `BULK_HEADERS` | R2 | Bulk header binary files |
| `CHAIN` | Var | `"main"` or `"test"` |
| `WHATSONCHAIN_API_KEY` | Var/Secret | Optional WoC API key |

## Build and Deploy

```bash
npm install
npm run dev              # local dev (D1 emulated)
worker-build --release   # build WASM
npm run deploy           # deploy to Cloudflare Workers
```

### Initial Cloudflare setup

1. Create a Cloudflare account, note your `account_id`
2. `npx wrangler d1 create chaintracks-cloudflare` — record the returned `database_id`
3. `npx wrangler r2 bucket create chaintracks-cloudflare-headers`
4. Fill `wrangler.toml` with your `account_id` and `database_id`
5. Apply migrations: `npx wrangler d1 migrations apply chaintracks-cloudflare --remote`
6. Deploy: `npm run deploy`
7. (Optional) Set WhatsOnChain key: `echo "<key>" | npx wrangler secret put WHATSONCHAIN_API_KEY`

## Testing

Quality gates — run all five before shipping:

```bash
cargo fmt --all
cargo clippy --target wasm32-unknown-unknown -- -D warnings
cargo check --target wasm32-unknown-unknown
cargo test --lib
worker-build --release
```

- **Unit tests:** `cargo test --lib` (79 tests covering serialization, storage logic, reorg detection)
- **Comparison:** `tests/e2e/compare.sh` (13-test parity check against a reference chaintracks instance)

## Consumers

Any service implementing the `ChainTracker` trait from bsv-rs can point at this worker. Known consumers:

- `rust-wallet-infra` — merkle root validation
- `rust-overlay` — `WorkerChainTracker` for `/findHeaderHexForHeight`, `/currentHeight`

## License

MIT — see [LICENSE](LICENSE).
