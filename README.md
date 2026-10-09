# rust-chaintracks

BSV block header tracking service on Cloudflare Workers. Rust compiled to WASM.

Reimplementation of the Node.js [`chaintracks-server`](https://github.com/bsv-blockchain/chaintracks-server) as a single Cloudflare Worker, replacing a 2× VPS + Docker deployment. Serves block header queries and merkle root validation for SPV consumers.

## What it does

- Polls the chain every minute (Workers cron) for new block headers through a COURIER LADDER: WhatsOnChain, Arcade's chaintracks v2 and Bitails as each other's fallbacks (see below)
- Stores headers in Cloudflare D1 (~945k rows), serves bulk headers from R2
- Exposes 12 HTTP endpoints matching the original TS server's public API
- Detects and handles reorgs up to 400 blocks deep
- Validates merkle roots for SPV consumers

## The live path's courier ladder (2026-09-08)

A same-height competition at 965877 (2026-09-08) left this store 23 minutes behind the network: one courier fed the live path, its refusal of the competitor parent aborted the cron on `?` before the announce and the repair, and with observability off nothing recorded why. Since then:

- **Three couriers, one ladder** (`src/couriers.rs`): WhatsOnChain, Arcade's chaintracks v2 (`/chaintracks/v2/tip|header/height|header/hash`) and Bitails (`/network/info`, `/block/height/{h}`, `/block/{hash}`, the raw header hex). The start rung rotates per minute; a rung that faults three times in a tick is skipped for the rest of it and counted; `chain_info` asks every rung and follows the HIGHEST tip (a lagging courier never reads as the chain), and that rung is asked first for the tick's headers. The reference (`Chaintracks.addLiveHeader` → `getMissingBlockHeader` across every live ingestor) does the same for a missing parent.
- **Availability, not authority.** Every answer is bound to the question (the height or the hash asked), to its own bytes (the hash is recomputed from the fields) and to its own proof of work (`BlockHeader::check_pow`, the node's `CheckProofOfWork`; see the next section). The store's most-work rule still decides the tip. A wrong answer is a faulting rung, never a header.
- **A poll fault is recorded, never an abort.** `sync_state.last_error` / `last_error_at` hold the LAST fault (never cleared; judge it by its age); `last_seen_height` / `last_seen_at` the highest tip any courier answered on the last cron with work. The cron's tail (the tip announce, the cumulative-work repair) runs on every pass.
- **`/getInfo`** serves `liveLagBlocks` (the seen height minus the stored tip; absent until the first cron after migration 0007, and absence is not health), `lastSeenHeight`, `lastSeenAt`, `lastSyncError`, `lastSyncErrorAt`, and `syncSchemaFault` when the health read itself fails (the migration not applied). bsv-low's fleet launcher refuses a launch on a lag above one block or an unknown lag.
- **Observability on** (`wrangler.toml [observability]`): every cron line, every courier fault, the per-tick tally (`Cron: couriers: woc ok 3 faults 0 · arcade ok 0 faults 1 (skipped) · …`).
- **Tests** (`src/courier_tests.rs`, the host harness): the loop-9 shape on one courier (recorded, the cron finishes), the ladder taking the parent from the next rung, the highest tip winning, the per-tick skip, the proof-of-work bind on real mainnet headers (genesis, 965900) and a lying courier, the lag and the fault on `/getInfo`. Each pin names its mutation.

## The node's header rules on every path (P0-4, 2026-10-08)

A header enters the store only through the node's own rules, ported from bitcoin-sv v1.2.3 (`src/consensus.rs` names each function and line it ports): the compact `bits` refused when negative, overflowing, zero or above the chain's `powLimit`, then the hash at most the target (`CheckProofOfWork`; a bits exponent past 256 bits used to decode to the maximum target); the bits equal to `GetNextWorkRequired`'s answer for the header's parent and time (the DAA on mainnet since 504032, the legacy retarget with the emergency adjustment before it, testnet's 20-minute rule, regtest's constant bits); the checkpoint at its height, and no new header below a checkpoint the store holds (`CheckIndexAgainstCheckpoint`). The checks run in `storage::insert_header` and `storage::insert_headers_batch`, the two writers of header rows, so every path runs them: the live cron, the backfill walk, the read-through grace path, the upstream catch-up, `/admin/ingest`, `/admin/backfill`, `/admin/bulk-sync`. A refusal names the node's reject reason (`high-hash`, `bad-diffbits`, `checkpoint mismatch`, `bad-fork-prior-to-checkpoint`, `prev-blk-not-found`) plus ours (`bad-hash`, `bad-genesis`, `ancestry-missing`); the cron records it and runs its tail, the read-through answers unable to verify, the admin routes answer 422.

- **Anchors.** A store starts from the genesis or a checkpoint: a batch with no stored parent must carry one (its hash vouches for the rows below it, which are the window the rows above it are checked against); a single header with no stored parent on an empty store is refused. On a store that holds a chain it is an orphan, stored inactive, never the tip, and checked when the backfill links it.
- **`CHECKPOINTS`** (var, optional): `"height:hash,..."` added to the node's list (a listed height takes the new hash). Malformed, the cron skips and the routes answer 503.
- **The re-validation after the deploy** (migration 0008): a store that held rows when the migration ran is re-checked from the last checkpoint it holds (mainnet's last is 530359), 2000 rows a cron tick, resumed from `sync_state.validated_height` / `validated_hash`. Until it reaches the tip, roots and headers above the cursor are not served (`isValidRootForHeight` answers unable to verify, the header routes 404, the tip routes the cursor's header); a refused row stops it (`validation_fault`) and nothing from that row up is served. `/admin/revalidate?steps=N` drives up to 50 chunks a call; `/admin/revalidate?restart=1` restarts it after a repair. `/getInfo` serves `validatedHeight`, `validationComplete`, `validationFault`. Rows at or below the anchor are vouched for by its hash and are not re-read.
- **Tests**: `src/pow_tests.rs` and `src/retarget_tests.rs` (RED on the base, each named), the node's own vectors in `src/consensus.rs` (`setcompact_test`, `bignum_SetCompact`, `get_next_work*`, `retargeting_test`, `cash_difficulty_test`), and real chain runs from Teranode v0.16.0 (`src/testdata/README.md`). The host fixtures are mined under regtest's `powLimit`.

## The 956433 class: a row the chain does not extend (#33, 2026-10-09)

On 2026-07-02 two blocks competed at 956433; the store kept the orphan active and the row at 956434 named a parent no row held. Three things answer that class.

- **`POST /admin/ingest?start=H` activates the row the stored child commits to.** The pushed headers meet the node's rules first (a refusal answers 422 and writes nothing). Then the stored chain decides, never the operator's word: when the active row at the next height names the last pushed header as its parent, and the push stands on the active row below it, the pushed rows become the active rows at their heights, every other row there inactive, the child's `previous_header_id` set, in one transaction (at most 400 headers a push). The tip and the rows above do not move. A push no stored child commits to is a competitor: stored inactive, and the answer says so. A push that outworks the tip reorgs to it like any courier's header. The answer:

  ```json
  {"status":"success","value":{"start":956433,"parsed":1,"inserted":1,"canonicalized":1,
    "outcome":"activated","deactivated":["<hash of the row that was active>"],
    "childRelinked":"<hash of the row at 956434>","reason":null}}
  ```

  `outcome` is `active` (the pushed rows are the only active rows at their heights: a tip extension, a filled gap, or what the store already serves), `activated`, or `storedInactive` (with `reason`). `canonicalized` counts the pushed heights whose single active row is the pushed header. `inserted` is the writer's count as it always was: a push of headers the store already holds, with no competing row, still counts them. After an `activated` answer run `/admin/revalidate?restart=1`: the rows above were last checked against the row that was replaced. A header below a checkpoint the store holds is still refused unless it is the active row already.

- **The sweep of dual-active heights prefers the extended row** (`storage::dedupe_active_heights`, run by the cron's catch-up and by `/admin/bulk-sync`): where two rows are active at one height it keeps the one the active row at the next height names as its parent, and only where no row is so named the newest ingest. Heights are settled from the top down, 1,000 a sweep, so two competing branches resolve to one. The statement it replaces kept the newest ingest whatever the chain extended, which is how the orphan at 956433 stayed served.

- **`GET /admin/linkcheck?from=H&to=T` answers "is the chain linked"** (`storage::link_check`). It walks the active rows from `from` (the genesis when not given) to `to` (the highest active row when not given), 50,000 heights a read, and reports every height whose active row the next height does not name as its parent. One pass, no checkpoint, no rule but the link, no write. The re-validation (`/admin/revalidate`) is the slow question, "does every header pass the node's rules": it anchors at a checkpoint and stops at its first fault; the link check is the fast one and reads below every checkpoint. The answer:

  ```json
  {"status":"success","value":{"from":0,"to":970341,"rows":970342,"linked":false,
    "broken":[{"height":956433,"active":"<hash of the active row at 956433>",
      "nextNames":"<the parent the row at 956434 commits to>","next":"<hash of the row at 956434>"}],
    "truncated":false,"checkedThrough":970341}}
  ```

  `rows` is the count of active rows in `[from, to]` (`to - from + 1` on a store with one row a height). An entry with `"active":null` is a height with no active row under an active one (a hole, or the height below the store's lowest row when the store does not start at the genesis). A height with two active rows reports the one the next height does not name. `linked` is true when `broken` is empty. An answer carries at most 1,000 entries; `truncated` then is true and `checkedThrough` is the height to ask again from. To repair an entry: push the header `nextNames` hashes to through `POST /admin/ingest?start=<height>`.

## What leaves the service, and why (Rule 28, 2026-10-09)

A third-party chain explorer is break-glass: a read that leaves the service is kept only when no header, proof or index of ours can answer it. For a header service that case is irreducible: headers come from outside the service, so nothing we hold can answer for a block we have not yet been told about. Every answer is re-derived locally (proof of work, the difficulty rule, the checkpoints, ancestry) before it counts, so a header is believed for its work and its ancestry, never for who served it. The reason and the fallback shape are named at the site (`src/couriers.rs`).

| read | who is asked | when |
|---|---|---|
| the tip, as it is announced | Arcade's chaintracks v2 tip stream (a peer, `ARCADE_URL` + `/chaintracks/v2/tip/stream`), held by the `TipStream` Durable Object | always (E5): one outbound stream, each tip through the ingest door |
| the tip, a header by height, a header by hash | the courier ladder: WhatsOnChain and Bitails (explorers), Arcade's chaintracks v2 (a peer) | the minute cron when the push does not cover the tick (the stream down, or no push nor poll in 10 minutes); the read-through for a block up to six above the tip; `/admin/backfill`; the push's parent walk by hash |
| up to 1,000 headers from a height | the upstream peer, `UPSTREAM_CHAINTRACKS_URL` (`getHeaders`) | the cron's catch-up (a gap over 10); `/admin/bulk-sync`, asked first |
| a file of 100,000 headers | the bulk file host, pinned (`woc::BULK_FILE_HOST`; neither peer nor explorer) | `/admin/bulk-sync` when the peer cannot serve the span |

- **The shape.** A negative needs a second provider (a rung that does not serve a header is a faulting rung and the next is asked; for the tip every rung is asked and the highest wins). "Could not look" is never "nothing there" (every rung faulted is an error recorded with its time, and the routes answer unable to verify). The start rotates per minute. Testnet's ladder is WhatsOnChain alone.
- **Nothing else goes out.** `/getPresentHeight` answers from the store (it was one explorer request per request in). No route proxies an explorer.
- **The push source (E5).** The routine read was the minute poll of the tip. The `TipStream` Durable Object (`src/tip_stream.rs`) now holds the peer's tip stream and hands each tip to the one ingest door, `push::ingest_announced`: the header's own proof of work first, its missing parents fetched by hash through the ladder (at most 36), then `storage::ingest_pushed`, the operator's ingest of #33, so a pushed header is stored and activated exactly as an operator's is. A wrong push is a refused header and a logged fault (`/getInfo` `lastError`), never a row. The object reads inside its alarm handler in sessions of 10 minutes, drops a stream silent for 45 s (three of Arcade's 15 s keepalives) and reconnects with the last event id; the minute cron wakes it if it is gone (`POST /wake`) and keeps the courier poll as the fallback: every minute while the stream is down, once in 10 minutes behind a live one with no push. Each poll's courier requests are counted in the object (`GET /status` on the object: `polls`). Design: bsv-stack-lean `docs/p0/rule-28-chaintracks.md`; report: `docs/p0/e5-push-source.md`.
- **Tests** (`src/rule28_tests.rs`): each fix red at the commit before it, then the behavior on scripted couriers and a scripted peer.

## Differences from Node.js chaintracks-server

Cloudflare Workers are stateless and request-based, so several TS server features are intentionally dropped or reshaped:

- **No event subscriptions.** TS exposes `subscribeHeaders()` / `subscribeReorgs()` for live callbacks. Workers cannot hold persistent channels — consumers should poll `/getInfo` or `/currentHeight`.
- **No WebSocket ingestion.** TS uses `LiveIngestorWhatsOnChainWs` for push ingest. Workers cannot hold outbound WebSocket connections. Rust uses HTTP polling via 1-minute cron.
- **Manual bulk header export.** TS auto-exports CDN bulk header files every 30 min. Rust exposes `/admin/export-r2` as a manual endpoint — trigger it directly or set up an external scheduler to call it.
- **No in-process watchdog.** TS runs a self-check loop that restarts its Docker container on stall. Workers handle restarts at the infra layer; redundant.
- **Multi-source fallback.** TS reads Babbage CDN + WhatsOnChain with watchdog failover. Rust reads a configurable upstream chaintracks URL (a peer) during catch-up and the courier ladder everywhere else (see "What leaves the service").

None of these are missing features — they're architectural trade-offs for serverless. All public HTTP endpoints are at parity with TS.

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
| `GET /getPresentHeight` | The larger of the served tip and the highest tip the couriers answered in the last cron tick (from the store; no request goes out) |
| `POST /admin/ingest?start=H` | Admin: push 80-byte headers (hex body) from height H; the answer says whether they are `active`, `activated` or `storedInactive` (the 956433 section below) |
| `GET /admin/linkcheck?from=H&to=T` | Admin: every height whose active row the next height does not name, from the genesis to the tip by default; one pass, no checkpoint (the 956433 section below) |
| `GET /admin/bulk-sync?file=IDX` | Admin: bootstrap span IDX (100,000 headers) from the upstream peer's `getHeaders`, else from the pinned bulk file host (`&source=file` asks the file host alone) |
| `GET /admin/backfill?from=A&to=B` | Admin: fill a gap below the tip from the courier ladder, 800 heights a call |
| `GET /admin/export-r2` | Admin: export D1 headers to R2 bulk files |

## Architecture

```
Request  → lib.rs → routes.rs → storage.rs → D1
Cron 1m  → lib.rs (scheduled) → sync.rs → the courier ladder → D1
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
| `TIP_STREAM` | Durable Object (`TipStream`, migration `e5-tip-stream`, SQLite-backed) | The push source: the peer's tip stream (E5) |
| `ARCADE_URL` | Var | The peer: a courier rung, and the tip stream's host |
| `TIP_STREAM_HEARTBEAT_S`, `TIP_STREAM_SESSION_S`, `TIP_STREAM_QUIET_S` | Var, optional | The stream's heartbeat (45), session (600) and the poll's quiet interval (600), in seconds |

## Build and Deploy

```bash
npm install
npm run dev              # local dev (D1 emulated)
worker-build --release   # build WASM
npm run deploy           # deploy to Cloudflare Workers
```

### Initial Cloudflare setup

1. Create a Cloudflare account, note your `account_id`
2. `npx wrangler d1 create rust-chaintracks` — record the returned `database_id`
3. `npx wrangler r2 bucket create rust-chaintracks-headers`
4. Fill `wrangler.toml` with your `account_id` and `database_id`
5. Apply migrations: `npx wrangler d1 migrations apply rust-chaintracks --remote`
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

- **Unit tests:** `cargo test` (272 pass, two runs by hand ignored: the extended header run, and the long pass from the node's checkpoint 530359 that derives the `CHECKPOINTS` entry; includes the chain-event witness and the Rule 28 witnesses on the host harness)
- **The compiled Worker, local:** `node tests/worker_events.mjs <witness.json>` (D1, the event feed, SSE), `node tests/worker_ct33.mjs` (the 956433 class through `/admin/linkcheck` and `/admin/ingest`) `node tests/worker_rule28.mjs` (what leaves the Worker and to whom: every outbound request is caught and answered in the harness, none reaches the network) and `node tests/worker_e5.mjs` (the push source: a scripted SSE peer, the object's reconnects and heartbeat, a refused and an accepted push, the poll's requests counted)
- **Comparison:** `tests/e2e/compare.sh` (13-test parity check against a reference chaintracks instance)

## Consumers

The versioned chain-event feed, cursor polling, SSE routes, Worker webhooks,
TypeScript compatibility view and client contract are in
[docs/CHAIN-EVENTS.md](docs/CHAIN-EVENTS.md). Apply migration 0009 after 0008;
the events follow P0-4's verification ceiling. Canonical webhook targets opt
in through `CHAIN_EVENT_WEBHOOK_URLS` and `CHAIN_EVENT_WEBHOOK_TOKEN`.

Any service implementing the `ChainTracker` trait from bsv-rs can point at this worker. Known consumers:

- `rust-wallet-infra` — merkle root validation
- `rust-overlay` — `WorkerChainTracker` for `/findHeaderHexForHeight`, `/currentHeight`

## License

MIT — see [LICENSE](LICENSE).
