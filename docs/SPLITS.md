# Rung tips and persistent branch splits (0.4.1)

The service continues to choose the most-work chain of valid headers it has
seen. It does not emulate a node's hold. The 2026-10-16 ruling in
`bsv-stack-lean/docs/RULINGS-2026-10-12.md` and the pinned client contract,
`bsv-script-lean@e08acd20:spec/node-client-contract.md`, row `tl.soft-freeze`,
motivate observing when providers follow different branches. A split event
reports that observation; it does not identify a cause or change the tip.

## The snapshot on `/getInfo`

The existing response gains one key, `rungTips`, an array. Each entry has:

| field | meaning |
|---|---|
| `name` | configured rung name |
| `implementation` | `reference`, `teranode`, or `peer` |
| `hash`, `height` | the highest tip returned by that rung's latest successful tip query |
| `chainWork` | cumulative work from the stored header, 64 hexadecimal digits |
| `time` | stored header time in UNIX seconds |
| `observedAt` | time the tip query started, UNIX milliseconds |
| `isActive` | whether that header currently belongs to the store's active chain |

This is the latest tip, including a lower replacement after a provider
reorganizes; it is not an all-time height maximum. A failed query retains
the preceding observation and its timestamp. No observation yet means
null hash, height, work and times, with `isActive: false`. A claimed tip
that is absent, unlinked or above the verification ceiling has null work
and header time and `isActive: false`. Its hash and height remain a provider
claim. Work is never copied from a provider. The D1 sync table has one row
per configured rung; a view joins current header work and active membership,
so a later reorg cannot leave stale flags on `/getInfo`. Removed rungs are
pruned on the next poll. Existing `lastSeenHeight` semantics are unchanged.

Each poll still asks every ladder rung for its tip. After normal catch-up,
any advertised tip absent from the store is requested from that rung and
ingested through the existing validation and parent-backfill path. Parent
requests retain the ladder's fallbacks and 36-header backfill limit. This
includes a tip below the highest rung's height. Already stored tips need no
extra header request. The push path's quiet poll interval is unchanged;
observations and detection advance at the next ladder poll, not every push.

## Configuration and `/health`

| rung | default label | override |
|---|---|---|
| `woc` | `reference` | `WOC_IMPLEMENTATION` |
| `arcade` | `teranode` | `ARCADE_IMPLEMENTATION` |
| `bitails` | `reference` | `BITAILS_IMPLEMENTATION` |

The label describes the chain the provider follows. An alternate header
service can use `peer`. Mainnet configures all three rungs; testnet configures
`woc`. These defaults live with the rung definitions and the same definition
reader supplies the sync, `/getInfo`, and `/health`. Overrides use ordinary
Worker vars, including the existing beta vars block in `wrangler.toml`.

`/health` adds `checks.rungSpan`, with `critical: true`, the sorted distinct
`labels`, and `status: "ok"` when they include `reference` and either
`teranode` or `peer`. A missing span or an unrecognized label yields `warn`.
The HTTP response and overall status remain 200 and `ok`; the service keeps
serving. This check reads configuration only, with no D1 or outbound request.
It describes configured coverage, not provider uptime.

`SPLIT_DEPTH` defaults to 3 and accepts integers from 1 through 400. Invalid
values produce a recorded sync error; header serving continues. The value
is a detection threshold, not a finality or spend-depth rule.

## Version 2 envelopes

Version 1's six kinds and field meanings remain unchanged. The two new
kinds use version 2. The decoder accepts both versions, validates known
shapes strictly, and rejects a new kind presented as version 1. Unknown
versions, kinds and fields remain errors that cannot be acknowledged.

```ts
type SplitTip = {
  hash: string; height: number; chainWork: string; rungs: string[]
}
type SplitEvent =
  | { v: 2; kind: 'split'; key: string; forkHeight: number;
      tips: [SplitTip, SplitTip] }
  | { v: 2; kind: 'splitResolved'; key: string; forkHeight: number }
```

Two tips qualify when neither is an ancestor of the other and **each** is
at least `SPLIT_DEPTH` blocks above their common ancestor. `forkHeight` is
the lowest affected height, one above that ancestor, matching the existing
fork/reorg vocabulary. `key` is `split:<common-ancestor-hash>` in display
order. Different fork points at the same height therefore have different
keys. The event contains a deterministic witness pair and all rung names
reporting each of those exact tips, sorted by name. It need not enumerate
every descendant tip on either branch.

There is one `split` per fork point, including across restarts, overlapping
polls and later tip growth. `splitResolved` uses the same public `key` when
no pair of configured rungs still straddles that fork point. Resolution
does not require the depth threshold: dropping below depth three on two
branches alone is not resolution. A provider failure alone is not resolution.
Equal tips or tips on one ancestry resolve it. Removing a provider can also
resolve it. One lifecycle is retained per fork point, so a later recurrence
of an already resolved fork does not emit another `split` for that key.

Detection walks stored parent links, at most 400 per rung, and never scans
the chain or calls a provider. Missing ancestry or a verification ceiling
can leave a pair unclassified; it cannot resolve an open split. Complete
coverage of a split beyond the bounded ancestry window is not inferred.
The snapshot updates, split state and journal inserts share one D1 batch.
A journal failure rolls the snapshot and transition back together. Split
state and event history are retained, while tip rows stay bounded by rungs.

The poll page keeps its version-1 carrier shape and decimal cursors. Each
envelope selects its own schema version. Poll and SSE responses advertise
`X-Chain-Event-Version: 2`, the newest envelope version the feed can emit.
Existing tip/fork/reorg/age producers keep their version-1 envelopes. SSE
data, webhook bodies, and any downstream Queue forwarding use the original
journal bytes; there is no new transport envelope. This Worker has no Queue
producer or consumer of its own. Legacy tip/reorg views ignore the new
kinds while advancing their cursors. Upgrade strict feed consumers before
enabling the new producer; version-1-only consumers correctly stop at a
version-2 record.

## Migration and verification

Apply `0010_rung_tips.sql` after 0009. It adds the bounded sync snapshot,
ancestry views, split state and triggers, without changing header selection
or revalidating historical headers. No historical provider tips can be
reconstructed; observations start on the next poll. To roll back a Worker,
retain these tables for diagnosis and account for version-2 records already
in the journal: a version-1-only Worker cannot replay them. Do not delete
records or reuse cursors to hide that incompatibility.

`src/split_tests.rs` drives the in-memory store and ladder through depths two
and three, growth, failed/late/unknown observations, same-branch resolution,
configuration changes, carrier bytes and transaction rollback. The compiled
Worker check `tests/worker_split.mjs` drives the cron against local D1 and
intercepts every outbound request. `tests/worker_health.mjs` checks both
allowed label spans and warnings without a database binding.
