# A3 #32 lane report, 2026-10-09

[X] Repository: `/Users/johncalhoun/bsv/rust-chaintracks`, origin
`https://github.com/Calgooon/rust-chaintracks.git`. Worktree:
`/Users/johncalhoun/bsv/targets/a3/32/chain-event-envelope`, branch
`a3-32/chain-event-envelope`. Base is exactly
`461dc39911c40bac8e954db33dabe29fa09a0484`, P0-4
`p0-4/headers-pow-retarget`, on `f86752b`. The branch was made from that
commit. The canonical checkout and P0-4 worktree were not changed.

[X] The lane implementation and local proofs are complete. Nothing was
pushed, deployed, broadcast, signed or paid; no PR, stash, sub-agent or
background job was used. All runtime requests were local or injected test
responses. The separate stack report and scenario are uncommitted captain
inputs. The public mirror remains the captain's job.

## Contract and delivery

[D] The complete schema, six examples, version rule, delivery details,
reference citations and client obligations are in [CHAIN-EVENTS.md](CHAIN-EVENTS.md).
One schema block follows; `H` is the eight public header fields plus
256-bit hex `chainWork`, and an outpoint is `{txid, vout}`.

```ts
type ChainEvent =
  | {v: 1; kind: 'tip'; height: number; hash: string; time: number; header: H}
  | {v: 1; kind: 'fork'; height: number; competingTips: [H, H]; depth: number}
  | {v: 1; kind: 'reorg'; forkHeight: number; depth: number; deactivatedHeaders: H[]; newTip: H}
  | {v: 1; kind: 'invalidated'; blockHash: string}
  | {v: 1; kind: 'frozen'; outpoint: {txid: string; vout: number}}
  | {v: 1; kind: 'tipAge'; seconds: number; tip: H}
```

[X] Routes: `/events?since=<cursor>`, `/events/stream?since=<cursor>`,
`/v2/tip/stream`, `/v2/reorg/stream`, with both compatibility streams also
under `/chaintracks/v2`. The canonical envelope bytes are reused inside
poll, SSE and webhook delivery; cursor metadata stays outside the envelope.
SSE has a 15-second heartbeat and Last-Event-ID reconnect support. Opt-in
Worker webhooks use `CHAIN_EVENT_WEBHOOK_URLS`,
`CHAIN_EVENT_WEBHOOK_TOKEN`, service bindings and a per-target durable
2xx acknowledgment cursor. The existing height webhook shape is retained.

[X] Migration `0009_chain_events.sql` journals the served chain through D1
triggers in the transaction that publishes its tip. Live ingest,
read-through, backfill/relink, upstream catch-up, operator canonicalization,
bulk sync and re-validation use that journal and P0-4's verification ceiling.
A tip has an age event, and the existing minute cron emits age during
courier outage. Reorg activation, tip publication and the legacy pending
marker now commit in one bounded batch, including a 120-header witness;
an extension's inserted row and tip also roll back if journaling fails.

[SRC] Existing transport and timer: `rust-chaintracks@461dc39
wrangler.toml:16-35,53-54`, `src/lib.rs:68-74`. No alert intake exists at
the base or was added here (`git grep` of the base source returned only a
frozen-clock comment). [X] `invalidated` and `frozen` have codec and transport
tests. [D] Their live emission requires a verified producer that commits
its transition and event atomically.

## Reference pins and witness scope

[SRC] `ts-stack@fb1b2dad56d207b21aa5f763f0c56113cacedd5d`,
`packages/wallet/wallet-toolbox/src/services/chaintracker/chaintracks/GoChaintracksServiceClient.ts:259-307,346-364,642-714`
requires the TS keys and silently reconnects after a shape exception;
`infra/chaintracks-server/src/v2-routes.ts:109-142,149-184` provides the TS
control. The Go server is `go-chaintracks@c7eeda1f15bdf89095e1c73a8e9f80ca22aa9132`,
`chaintracks/types.go:44-57`, `cmd/server/api.go:111-164`.

[SRC] The brief's `chaintracks-server` clone is TypeScript, HEAD
`4dc4acde89a572416cb93bbcb5353e4d9266ee4f`; its
`src/v2-routes.ts:120-185` was read. The scenario's actual file is
`2026-10-08-reorg-announced-nobody-hears-go-server-ts-client-shape.md`.
These naming corrections do not change the required witness.

[SRC] Alert words remain the sibling's: bsv-script-lean S6 #70, S7 #84/#85,
S8 #94 and its provenance pins, bitcoin-sv v1.2.3 `6504a3af`, Teranode
v0.16.0 `4edb60a4`; Teranode `services/alert/node.go:160-175,274`,
`stores/utxo/aerospike/alert_system.go:88-95`. No node policy was copied.

[X] The native witness uses mined regtest headers: checkpoint 100, A1 at
101, tied B1 at 101 and winning B2 at 102. A1's root is refused after the
reorg; the journal carries A1 and B2; its compatibility view round-trips.
The actual pinned TS SSE client, not a duplicate parser, gives zero listener
calls and one silent reconnect for the Go shape, and one listener call with
no reconnect for the TS control and our derived view.

[X] The regtest reference run changes exactly one PoW-limit literal in
the pinned utility; SSE, parsing, hashes and work checks still execute.
[SRC] That mainnet-only limit is `util/blockHeaderUtilities.ts:589-618`
under the same pinned chaintracks directory. [X] A separate codec control
with real mainnet headers runs the reference unmodified. [D] That control
does not represent a mainnet reorg. No complete wallet, monitor, spend,
tracker or application replay is claimed.

[X] The compiled release Worker also passed local Miniflare checks of D1
migrations/triggers, poll/SSE bytes, both route aliases, reconnect cursors,
live delivery, heartbeat and malformed/future cursor statuses. Its rows
are seeded from the checked regtest fixture; native tests separately run
validation and storage entry points. Its frozen event is a transport sample.

## Files and tests

[X] Files changed, one purpose per file:

| file | change |
|---|---|
| `Cargo.toml` | add the stream combinator dependency |
| `migrations/0009_chain_events.sql` | journal, delivery cursors, served-tip views and atomic publishing triggers |
| `src/events.rs` | strict six-kind codec, poll, SSE, compatibility and webhook outbox |
| `src/chain_event_tests.rs` | thirteen storage/delivery/codec/re-validation/rollback witnesses |
| `src/d1.rs` | add a bounded atomic batch method for reorg transitions |
| `src/storage.rs` | keep tip publication and its transition atomic, handle competing bulk/operator paths |
| `src/sync.rs` | cron age/outbox, verified legacy notifications, shared transport |
| `src/routes.rs` | canonical routes, compatibility aliases, gated read-through notification |
| `src/lib.rs` | register the event implementation and witnesses |
| `src/courier_tests.rs` | update statement expectations for the existing timer and verification reads |
| `src/reorg_producer_tests.rs` | update expectations for one atomic transition and the timer |
| `tests/reference_client.ts` | execute the pinned TS SSE client using read-only git-show modules |
| `tests/pinned_client_entry.ts` | entry point for that pinned client bundle |
| `tests/worker_events.mjs` | local compiled-Wasm Worker and D1/SSE checks |
| `docs/CHAIN-EVENTS.md` | contract, examples, pin citations, client responsibilities and rollout |
| `docs/A3-32-REPORT.md` | commands, results, scope and captain handoff |
| `README.md` | event entry point, migration/config and current host-test count |

[X] `cargo fmt --all` normalized inherited formatting in touched modules.
The thirteen added Rust tests cover the announced-nobody-hears scenario,
tied fork, byte equality and replay, refused targets, unknown payloads,
cursor validation, all six kinds, age during outage/future timestamp,
verification ceiling, bulk/operator replacements, a bulk inactive parent,
deep reorg rollback and extension rollback.

## Proof commands and tails

[X] All Cargo commands below run in the named worktree with:

```sh
export CARGO_TARGET_DIR=/Users/johncalhoun/bsv/targets/a3/32/target
export CARGO_BUILD_JOBS=4
```

[X] Logs and exported fixture are local artifacts under
`/Users/johncalhoun/bsv/targets/a3/32/`. They are not repository inputs.
The relevant proof commands and output tails follow. Exit 101 for the
pre-fix Rust witnesses and exit 1 for the Go-client delivery assertion are
the intended red results; all final gates exited 0.

```text
cargo test --lib chain_event_tests::scenario_announced_nobody_hears_has_a_versioned_reorg -- --nocapture
RED: no such table: chain_events
test result: FAILED. 0 passed; 1 failed; 205 filtered out
GREEN: test result: ok. 1 passed; 0 failed; 205 filtered out
Logs: witness-red.log, witness-green-initial.log

cargo test --lib chain_event_tests::bulk_extending_an_inactive_parent_emits_the_reorg -- --nocapture
RED: no depth-1 reorg was emitted
test result: FAILED. 0 passed; 1 failed; 217 filtered out
GREEN: test result: ok. 1 passed; 0 failed; 217 filtered out
Logs: bulk-parent-red.log, bulk-parent-green.log

LANE32_WITNESS_FILE=/Users/johncalhoun/bsv/targets/a3/32/witness.json cargo test --lib chain_event_tests -- --nocapture
test result: ok. 13 passed; 0 failed; 0 ignored; 205 filtered out; finished in 1.00s
Log: witness-final.log

bun tests/reference_client.ts /Users/johncalhoun/bsv/targets/a3/32/witness.json --regtest --expect-go-delivery
AssertionError: Go shape must reach the listener
0 !== 1
Log: reference-red.log

bun tests/reference_client.ts /Users/johncalhoun/bsv/targets/a3/32/witness.json --regtest
Go shape RED: listener=0, silent reconnect=1 (actual pinned SSE client)
TS control GREEN; Worker envelope -> compatibility view GREEN: listener=1, reconnect=0
pin=fb1b2dad56d207b21aa5f763f0c56113cacedd5d; 197 pinned modules; explicit regtest PoW-limit adapter; format, hash and PoW checked
Log: reference-green.log

bun tests/reference_client.ts /Users/johncalhoun/bsv/targets/a3/32/witness.json
Go shape RED: listener=0, silent reconnect=1 (actual pinned SSE client)
TS control GREEN; Worker envelope -> compatibility view GREEN: listener=1, reconnect=0
pin=fb1b2dad56d207b21aa5f763f0c56113cacedd5d; 197 pinned modules; unmodified mainnet codec control; format, hash and PoW checked
Log: reference-mainnet.log

LANE32_WITNESS_FILE=/Users/johncalhoun/bsv/targets/a3/32/witness.json cargo test
test result: ok. 217 passed; 0 failed; 1 ignored; finished in 29.95s
Doc-tests rust_chaintracks: 0 passed; 0 failed
Log: cargo-test.log

cargo clippy --target wasm32-unknown-unknown -- -D warnings
Checking rust-chaintracks v0.1.0
Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.52s
Log: clippy-wasm.log

cargo check --target wasm32-unknown-unknown
Checking rust-chaintracks v0.1.0
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.71s
Log: check-wasm.log

/tmp/p04-wb/bin/worker-build --release
Finished `release` profile [optimized] target(s) in 28.01s
Optimizing wasm binaries with `wasm-opt`...
Done in 34.76s
index.js 26.9kb; Done in 8ms
Log: worker-build.log

MINIFLARE_MODULE=/Users/johncalhoun/.npm/_npx/32026684e21afda6/node_modules/miniflare/dist/src/index.js /Users/johncalhoun/.nvm/versions/node/v24.11.1/bin/node tests/worker_events.mjs /Users/johncalhoun/bsv/targets/a3/32/witness.json
Local compiled Worker GREEN: D1 migrations/triggers, poll/SSE byte equality, both TS route aliases, Last-Event-ID, live delivery, 15 s heartbeat, bad/future cursor statuses
Log: worker-runtime.log

cargo fmt --all -- --check
(no output; exit 0)
Log: fmt-check.log

git diff --check
(no output; exit 0)
```

[X] Build/test tools: Bun 1.3.11, worker-build 0.7.5 (the P0-4 executable),
worker crate 0.7.5, wasm-bindgen 0.2.126 from this lane's ignored local lock,
Node 24.11.1 and cached Miniflare 4.20260426.0. The global worker-build
0.8.7 was not used. No version bump: this Worker is unpublished 0.1.0.

[X] Earlier iterations are retained rather than counted as passing proof:
`compile-events.log` recorded use of a nonexistent D1 clone method, fixed
by transferring ownership; `suite-initial.log` was 202 pass/3 fail/1 ignored
on inherited statement snapshots, updated for the timer, ceiling reads and
atomic transaction; `suite-final.log` was 216 pass/1 ignored before adding
the bulk-parent witness. `witness-green.log` was the earlier 11-test pass.
`witness-export.log` records the corrected `mainnet()` helper name (`main()`
is the API). The initial fixture helper call was also corrected before the
proper missing-journal red run. Wasm clippy found an unused legacy raw-tip
helper, now restricted to its existing tests. The TS loader's initial
namespace setup was corrected. Its API source was
[Bun's official plugin documentation](https://bun.com/docs/bundler/plugins).
The local runtime's module rule, outbound-service callback and migration
statement loading were corrected before the final green; D1.exec's line
splitting cannot apply a multiline trigger, so the harness uses SQLite's
complete-statement parser and D1.batch. These were harness errors, not
runtime passes.

[SRC] Required readings were opened in the requested order: the A3 brief,
shared P0 brief, then `docs/p0/p0-4.md`; followed by the constitution,
rules, north-star tests, P0 reference/upgrade table, R1 2a, tracker section
3, app rule 1, P0-4 headers brief, matrix and both corpus scenarios.
Reference reads used `git show <pin>:<path>`, never mutable source files.
Read-only issue commands were `gh api repos/Calgooon/bsv-stack-lean/issues/32`
and `/26`, and the sibling alert issues #78 through #86 and #94. Preflight
and final checks used `git rev-parse`, `git branch --show-current`,
`git remote -v`, `git status --short`, `git diff --check`, `rg`, and `git
grep` over the named repositories. No forbidden file or private program
was read. The full file/line source citations are also in CHAIN-EVENTS.md.

## Upgrade, rollback, consumers and open work

[D] Apply 0009 after 0008 and after the owner's P0-4 soak, then deploy only
on the owner's beta go. The migration starts from the verified served tip;
it adds no header scan. Re-validation remains P0-4's bounded checkpoint pass.
Canonical consumers persist/deduplicate cursors atomically with handling,
report unknown versions/kinds/shapes without acknowledging them, and
interpret cursor reset/ahead as an incident. A no-feed client polls only
the heights of its own proofs.

[D] Sentence for `docs/APP-CONTRACT.md` rule 1: "A chain-event client reports
an unknown version, kind or shape through a status, metric or listener error
without acknowledging the cursor or silently reconnecting; a client with
no feed polls only the heights of the proofs it holds."

[D] Tracker `ChainEvent` must map the six kinds, target the affected proof
heights/hash, mark them suspect/stale and re-ask. Zanaadu must re-verify and
rewrite stored heights, invalidate block-time/served-fact caches, withdraw
stale confirmed views, and display the tracker's checked word. Tip age feeds
its freshness guard. Wallet/overlay consumers need their own targeted proof
re-check. No application integration was made in this lane.

[D] Matrix recommendations, scoped to the tested Worker: the captain can
move `headers_proofs.wire_change` unknown -> rule,
`headers_proofs.fork_in_progress` gap -> rule, and
`headers_proofs.tip_stale` gap -> rule with these proofs. Keep
`headers_proofs.proof_stale` gap and `wallet.reorg_done` gap until proof
holders consume and re-check; keep `app_layer.reorg_done` unknown and
`app_layer.tip_stale` unknown until Zanaadu runs. No matrix file was edited.

[D] Rollback: drop the six named 0009 triggers as shown in CHAIN-EVENTS.md,
restore the P0-4 Worker, and retain the journal/cursors for investigation.
Rollback has not been executed. The owner's pre-beta risk is journal cost
and behavior on a populated D1: measure trigger CPU, deep atomic batches,
SSE longevity and backlog delivery, then decide retention with a cursor
expiry contract. No retention deletion is implemented.

[D] Unverified: the inherited ignored full EDA fixture, production/beta D1
and soak/load, live service-binding webhooks, authenticated alert production,
complete wallet/tracker/app behavior, rollback and public mirror export.
Next: the captain reviews this branch, completes the P0-4 soak, applies 0009
on beta only with the owner's go, and coordinates those consumer lanes.
