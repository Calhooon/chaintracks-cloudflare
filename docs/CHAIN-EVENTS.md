# Chain events, version 1 (#32)

[D] This is the Worker contract selected by R1 section 2a of the lane's
`docs/RULINGS-2026-10.md`, and the tracker input contract in
`docs/charters/tracker.md` section 3. The implementation is in `src/events.rs`
and migration `0009_chain_events.sql`. P0-4 is the base, `461dc39` on
`f86752b`; the Worker is unpublished, version `0.1.0`. This is an additive
HTTP capability with a minor-version contract impact. No deployment is part
of the lane.

## Schema and version rule

[D] Integers below are JSON integers, hashes are 64 hex characters in display
byte order, time is UNIX seconds, and work is a 256-bit hex integer. An
outpoint is `{txid, vout}`. `H` carries the eight header fields and cumulative
work, independent of storage flags or row ids.

```ts
type H = {
  version: number; previousHash: string; merkleRoot: string;
  time: number; bits: number; nonce: number; height: number;
  hash: string; chainWork: string
}
type ChainEvent =
  | { v: 1; kind: 'tip'; height: number; hash: string; time: number; header: H }
  | { v: 1; kind: 'fork'; height: number; competingTips: [H, H]; depth: number }
  | { v: 1; kind: 'reorg'; forkHeight: number; depth: number; deactivatedHeaders: H[]; newTip: H }
  | { v: 1; kind: 'invalidated'; blockHash: string }
  | { v: 1; kind: 'frozen'; outpoint: { txid: string; vout: number } }
  | { v: 1; kind: 'tipAge'; seconds: number; tip: H }
```

[D] Version 1 has exactly these kinds and fields. Changing a kind, field,
meaning, or accepted shape requires another version and a release note.
Unknown versions, kinds and malformed known shapes are reported errors.
`src/events.rs::decode` implements this rule and validates the relationships
inside the event. Consumers independently check header hashes, work and proof
roots; parsing a shape is not proof of inclusion.

[D] `fork.height` and `reorg.forkHeight` are the lowest affected height,
one above the common ancestor. A fork carries the currently served tip and
the checked competitor, including their competing work; its depth is the
current tip height minus the ancestor height. A reorg's depth counts removed
headers; they run from the prior served tip down to the fork height, with
no gaps. `newTip` is the verified new tip. A tied competitor emits `fork`
before the winner emits `reorg`, `tip`, and `tipAge`. The existing 400-step
ancestor limit bounds the transition; an unresolved ancestor is an error.
`tipAge.seconds` is `max(0, now - tip.time)` at emission, never a negative age.

[SRC] The alert vocabulary belongs to sibling bsv-script-lean S7
[#84](https://github.com/Calgooon/bsv-script-lean/issues/84),
[#85](https://github.com/Calgooon/bsv-script-lean/issues/85), S6
[#70](https://github.com/Calgooon/bsv-script-lean/issues/70) and S8
[#94](https://github.com/Calgooon/bsv-script-lean/issues/94), with node pins in
its `docs/PROVENANCE.md` section 1: bitcoin-sv `6504a3af` (v1.2.3), Teranode
`4edb60a4` (v0.16.0). The source paths read for this lane are Teranode
`services/alert/node.go:160-175,274`,
`stores/utxo/aerospike/alert_system.go:88-95`. Their meanings are cited there,
not implemented as a new node policy here. [X] There is no alert intake in
this Worker at the base or in this lane; `invalidated` and `frozen` have
codec and transport coverage, and require a verified alert producer before
live emission. An integration must commit its stored-state transition and
its envelope in the same D1 transaction.

## Delivery and cursors

[D] `GET /events?since=<cursor>` returns:

```json
{"v":1,"cursor":"42","events":[{"cursor":"42","event":{"v":1,"kind":"invalidated","blockHash":"0000000000000000000000000000000000000000000000000000000000000001"}}],"hasMore":false}
```

[D] Omit `since` or use `0` to replay the journal. Each page holds at most
100 events in cursor order. Continue from the returned cursor while
`hasMore` is true. An empty page returns the supplied cursor. Cursors are
opaque decimal strings, scoped to one chain/database, within D1's exact
integer range; skipped numbers are allowed. A malformed cursor is HTTP 400;
a cursor ahead of this journal is HTTP 409 `cursorAhead`, an incident the
client reports. No retention deletion is implemented in this migration.

[D] `GET /events/stream?since=<cursor>` is a long-lived SSE stream:

```text
id: 42
data: {"v":1,"kind":"invalidated","blockHash":"0000000000000000000000000000000000000000000000000000000000000001"}

: heartbeat

```

[D] `Last-Event-ID` takes precedence over `since` on reconnect. The stream
replays the journal, checks D1 every 15 seconds when caught up, and sends
comment heartbeats during quiet intervals. Dropping the response cancels its
timer. Pages and streams use `Cache-Control: no-store` and
`X-Chain-Event-Version: 1`. A journal/decoder fault before the stream opens
is an HTTP error; after opening it is an SSE `event: error` with a reported
`feedUnavailable` body and no id, followed by EOF. That fault is not a chain
envelope and cannot acknowledge the failed record.

[D] Workers opt in through `CHAIN_EVENT_WEBHOOK_URLS`, a comma-separated list
of `[BINDING=]URL`, and the `CHAIN_EVENT_WEBHOOK_TOKEN` secret. The existing
service-binding transport is reused. [SRC] The pattern is
`rust-chaintracks@461dc39 wrangler.toml:16-35` (the existing tip targets and
bindings); no existing deployment target was added or changed. Each POST
body is the original envelope, with `X-Chain-Event-Cursor` and the bearer
authorization. Each target has its own durable cursor, acknowledged only on
2xx. Refusals stop that target and are logged with the cursor; the next cron
retries it while other targets progress. There are at most 32 total POSTs
per tick, divided among at most 32 targets. The journal remains the recovery
path during a long target outage.

[D] Delivery is at least once. A failure after the receiver accepts but
before D1 records the acknowledgment can repeat the event. A consumer makes
its own processing and cursor persistence atomic, deduplicates by cursor,
and reports a reset or an uninterpretable payload before resuming. It must
not assume cursor values are contiguous. The JSON envelope bytes stored by
D1 are reused unchanged inside the poll page, the SSE data line and webhook
body. The transport cursor is outside the envelope.

## Emitter and verification ceiling

[X] The journal is written by D1 triggers in the same transaction as a tip
move or a verification cursor update. It covers live ingest, the
read-through, competing-branch backfill/relink, upstream catch-up, operator
ingest/canonicalization, bulk sync, and re-validation. Extensions commit
the new row, tip and envelope together; reorgs commit the entire bounded
branch, pending legacy marker, tip and envelope in one batch, including
reorgs deeper than 100 statements. Bulk callers following an inactive parent
or replacing an active row use the live branch selection path. An operator
replacement disconnects the old suffix before publishing its new tip.
The 120-header rollback witness injects a journal fault and proves the branch
and tip roll back together.

[D] The event view selects the same served tip as P0-4's `served_tip`, and
emits no tip, fork or age above the verification ceiling. During re-validation,
the announced tip advances with the verified cursor; lowering that cursor
is a tip change, not a declaration that still-active headers were orphaned.
When no verified tip is available, tip delivery waits. Readers fail closed
through the existing header routes. The legacy tip webhook also uses this
ceiling, including on read-through notifications.

[X] `tipAge` accompanies each distinct served tip and runs on the existing
minute cron even when every courier is unreachable. Overlapping ticks for
the same tip/minute deduplicate. [SRC] The timer is
`rust-chaintracks@461dc39 src/lib.rs:68-74`, `wrangler.toml:53-54`. Migration
0009 does no additional header scan or re-validation; P0-4's bounded pass
and checkpoint choice remain the verification work.

## TypeScript compatibility view

[D] The pinned client's paths are `GET /v2/tip/stream` and
`GET /v2/reorg/stream`, also mounted under `/chaintracks/v2`. Tip data is an
exact eight-field public header. Reorg data is
`{depth, oldTip, newTip, deactivatedHeaders}`, derived from the envelope;
`oldTip` is its highest deactivated header. The view removes `chainWork`,
because the reference permits the eight base fields or all its storage
fields, and refuses a partial set. Unmatched kinds advance the stream cursor
without a data event. On a connection without a cursor, the tip stream
starts with the served tip and the reorg stream starts at the journal head;
an explicit cursor requests replay. The HTTP version header declares the
view's version while the pinned legacy payload remains exactly its expected
shape.

[SRC] `ts-stack@fb1b2da
packages/wallet/wallet-toolbox/src/services/chaintracker/chaintracks/GoChaintracksServiceClient.ts:259-307,346-364,642-714`:
the exact keys, header validation, and reconnect behavior;
`infra/chaintracks-server/src/v2-routes.ts:109-142,149-184`: the matching TS
server view and heartbeat. The actual Go server is the sibling clone
`go-chaintracks@c7eeda1` (`c7eeda1f15bdf89095e1c73a8e9f80ca22aa9132`),
`chaintracks/types.go:44-57`, `cmd/server/api.go:111-164`, which emits
`{orphanedHashes, commonAncestor, newTip, depth}`. The lane's named
`chaintracks-server` directory is a TypeScript server, HEAD
`4dc4acde89a572416cb93bbcb5353e4d9266ee4f`, not the Go server; that HEAD was
recorded and its `src/v2-routes.ts:120-185` read.

[X] `src/chain_event_tests.rs` replays the scenario of record through the
Worker's storage harness. `tests/reference_client.ts` executes the actual
pinned SSE client with its modules loaded through `git show`. The Go payload
causes zero listener calls and one silent reconnect. The TS control and our
derived view each call the listener once. For the mined regtest fork, the
harness changes only the reference utility's hardcoded mainnet PoW limit to
regtest's limit; the parser, SSE loop, computed hashes and work comparison
still run. The unmodified production client separately accepts a codec
control with real mainnet headers from P0-4's pinned Teranode data. That
control tests the shape, not a mainnet reorg or a complete wallet/monitor.
[X] `tests/worker_events.mjs` also runs the compiled release Worker in local
Miniflare with D1: migrations and triggers, poll/SSE byte equality, both TS
route aliases, reconnect cursors, live delivery and the 15-second heartbeat.
It seeds the same checked regtest fixture directly into the local database;
the native harness separately exercises header validation and ingest. Its
synthetic frozen event proves transport, not a verified alert intake.
[SRC] The reason for the adapter is
`GoChaintracksServiceClient.ts:642-674`, `util/blockHeaderUtilities.ts:589-618`
at `ts-stack@fb1b2da`. The reference monitor/task are
`src/monitor/Monitor.ts:208-236`, `src/monitor/tasks/TaskReorg.ts:46-56` under
the same wallet package. See the lane report for commands and tails.

## Client contract and consumers

[D] An unknown version or kind, or a malformed known shape, is an error the
client reports through a status, metric, or error to its listener. It stops
processing at the offending cursor, with no acknowledgment, skip, or silent
reconnect. A client with no chain-event feed polls the header service only
at the heights of proofs it holds, rechecks their roots, and reports lookup
failures as unable to verify. It performs no transaction, address, output or
chain scan.

[D] Tracker `ChainEvent` maps directly onto all six kinds. On a fork it marks
only proofs at or above `height` suspect. On a reorg it marks only those at
or above `forkHeight` stale and schedules a re-ask; on invalidation it targets
the checked block hash. A tip rechecks the suspect set. Frozen inputs and tip
age go to the host's spend/freshness guard; they do not alone write a mined
word. Each host chooses its freshness threshold and treats an old or
unreachable source as unable to verify.

[D] Zanaadu's app layer must consume and persist the cursor, rewrite affected
stored heights after re-verifying their proofs, invalidate block-time and
served-fact caches, withdraw stale confirmed views, and show only the
tracker's checked word. `app_layer.reorg_done` and `app_layer.tip_stale` move
when that consumer runs, not when this emitter branch is committed. Wallet
and overlay consumers owe the same targeted proof re-check; the broadcaster
remains a source of hints. The header service stores no application proofs
and cannot retract a proof on the holder's behalf.

[D] Sentence for `docs/APP-CONTRACT.md` rule 1: "A chain-event client reports
an unknown version, kind or shape through a status, metric or listener error
without acknowledging the cursor or silently reconnecting; a client with
no feed polls only the heights of the proofs it holds."

## Upgrade, rollback and owner gate

[D] Apply migration 0008, complete the owner's P0-4 soak, apply 0009, and deploy
this branch only on the owner's beta go. Configure canonical webhook targets
and their token only when consumers implement the contract. Existing height
webhook targets retain `{height, hash, reorgFrom?}`. The schema is additive;
0009 starts its journal from the verified served tip and retains all events.

[D] To roll back, disable the 0009 triggers (the SQL below), then restore the
P0-4 Worker. Keep the journal and delivery tables for investigation or replay;
P0-4 reads neither. This rollback procedure has not been executed.

```sql
DROP TRIGGER chain_event_fork_insert;
DROP TRIGGER chain_event_fork_relink;
DROP TRIGGER chain_event_validation;
DROP TRIGGER chain_event_initial_tip;
DROP TRIGGER chain_event_tip;
DROP TRIGGER chain_event_publish;
```

[D] The owner's pre-beta risk is the new D1 journal's operational cost and
behavior on a populated service: measure trigger CPU, a deep atomic batch,
SSE longevity, and backlog delivery during the beta soak, then decide on
retention with an explicit cursor-expiry contract. The production D1/beta
soak, live verified alert intake, complete wallet/tracker/app integration,
and the owner's mirror export remain unverified by this lane.

## One example per kind

[D] These are wire examples using the same mined regtest headers as the
witness, not production chain data. The age example's clock is tip time plus
600 seconds. The frozen outpoint is synthetic.

```json
{"v":1,"kind":"tip","height":102,"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","time":1757280102,"header":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912}}
```

```json
{"v":1,"kind":"fork","height":101,"competingTips":[{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"merkleRoot":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912},{"bits":545259519,"hash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","height":101,"merkleRoot":"0000000000000000ed99394f49f49e75b96e4deb070e14301c2e57cfa4bc902d","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004"}],"depth":1}
```

```json
{"deactivatedHeaders":[{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"merkleRoot":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912}],"depth":1,"forkHeight":101,"kind":"reorg","newTip":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912},"v":1}
```

```json
{"v":1,"kind":"invalidated","blockHash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b"}
```

```json
{"v":1,"kind":"frozen","outpoint":{"txid":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","vout":0}}
```

```json
{"v":1,"kind":"tipAge","seconds":600,"tip":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912}}
```
