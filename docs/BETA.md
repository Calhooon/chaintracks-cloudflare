# Beta from the public tag

The captain syncs the mirror and tags `v0.3.0`, then builds a clean checkout
of that tag with worker-build 0.7.5 in a private tool root and a separate
`CARGO_TARGET_DIR`. Compare `/health.build.lib_rev` with that checkout's
`git rev-parse HEAD`, not the canonical revision. Require `dirty: false`.
`crates` names this crate's own version, and `bsv-rs` only when Cargo.lock
carries it (it does not today, so the key is absent). Cargo.lock is currently
ignored by the repository, so retain the release lockfile with the build evidence.

Beta has its own D1 and R2. The captain renders endpoint placeholders to the
same values as production in the private config, with `TIP_WEBHOOK_URLS = ""`.
No beta service binding may target an app layer. Never copy production data
or resource identifiers into the template. Leave `CHECKPOINTS` unset, as in
production. The minute cron and `TIP_STREAM_QUIET_S = "60"` match production.

Wrangler's installed `config-schema.json` documents `durable_objects`, `vars`
and `services` as non-inherited. Its `wrangler-dist/cli.js` normalizer uses
`inheritable(..., "migrations", ...)`: the top-level `e5-tip-stream` migration
applies to beta; do not duplicate its history. The explicit beta `TIP_STREAM`
binding has no `script_name`, so it refers to beta's own `TipStream` class.
Cloudflare creation and environment isolation remain [D] until deployed.

Captain commands (only the captain runs these against Cloudflare):

```sh
wrangler d1 create rust-chaintracks-beta
wrangler r2 bucket create '<your-bulk-headers-beta-bucket>'
```

Render the returned resource values and the production endpoint values into
`~/bsv/infra/workers/chaintracks/beta/wrangler.toml`. Set `main` in that private
config to the absolute `build/worker/shim.mjs` in the public tag checkout and
`migrations_dir` on beta's D1 binding to its absolute `migrations` directory.
Strip the build command into another private config before any deploy:

```sh
beta_config="$HOME/bsv/infra/workers/chaintracks/beta/wrangler.toml"
beta_deploy="$HOME/bsv/infra/workers/chaintracks/beta/wrangler.deploy.toml"
sed '/^command = /d' "$beta_config" > "$beta_deploy"
wrangler d1 migrations apply DB --env beta --remote --config "$beta_deploy"
wrangler deploy --env beta --config "$beta_deploy"
wrangler secret put ADMIN_TOKEN --env beta --config "$beta_deploy"
curl --fail --silent --show-error "$BETA_URL/health"
curl --fail --silent --show-error "$BETA_URL/getInfo"
```

`BETA_URL` and `BETA_ADMIN_TOKEN` below are supplied privately by the captain;
no real endpoint or credential belongs in this document. The secret set above
must match the latter. `/health` needs no database and is a build identity,
not a statement that headers are caught up.

An empty database does not bootstrap just by waiting: `sync::run_cron` starts
at height 1 and the storage gate needs genesis or a checkpoint in a linked
batch. Seed the genesis span through the authenticated bootstrap route:

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer $BETA_ADMIN_TOKEN" \
  "$BETA_URL/admin/bulk-sync?file=0"
```

The route asks the configured peer for batches of 1,000 headers, falling back
to the pinned bulk file host if the peer fails, then validates and inserts the
100,000-header span through the storage rules (D1 writes use bounded batches). Continue spans in
ascending order (`file=1`, `file=2`, etc.) as needed, checking each response;
never infer success from a request sent. The built-in mainnet checkpoint list
ends at 530359; each linked batch must satisfy the applicable checkpoint and
difficulty rules. No new operator checkpoint is needed.

The cron then catches up in at most 5,000 headers per tick using the upstream
peer, falling back to the rotating WhatsOnChain/Arcade/Bitails courier ladder;
near the tip, the push source and courier fallback keep it current. [D] At a
tip around 966,000 after the genesis span, the nominal bulk-cron lower bound
is ceil((966000-99999)/5000) = 174 minutes. Real catch-up may take longer from
Worker limits, courier faults, push/poll scheduling and validation. No live
bootstrap duration has been measured here. Additional admin spans can shorten
that time, but their duration is also [D].

Repeat `/getInfo` until `heightLive` reaches the seen tip, `liveLagBlocks` is
zero, `validationComplete` is true, `validatedHeight` reaches the tip and
`validationFault` is clear; inspect `lastSyncError` and its age. An existing
fault is not automatically cleared by a later successful poll. Re-check
`/health` against the public tag revision throughout the soak.

R2 is an output, not an empty-store seed: after D1 catches up, the authenticated
`/admin/export-r2` writes bulk files and the index to beta's bucket. It does
not fetch a seed from that bucket. Export separately if bulk clients need it:

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer $BETA_ADMIN_TOKEN" "$BETA_URL/admin/export-r2"
```

The captain records bootstrap timing, the deploy's build JSON and the soak.
Production deployment, mirror retirement and archive require the owner's word.
