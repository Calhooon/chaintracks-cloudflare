-- 0006: per-target announce deliveries and the claim clock (bsv-low M19B-G2
-- rounds 3 and 4, MED-2 / MED-1, 2026-09-08). ADDITIVE (a new table, one
-- nullable column) and idempotent under wrangler's migration ledger; apply
-- with `npx wrangler d1 migrations apply rust-chaintracks --remote` BEFORE
-- deploying the build that ships with it (that build reads the table and
-- stamps the column on every announce).
--
-- Why the table: the announce kept ONE delivered state for every target on
-- the shared sync_state row, so one dead target kept the record owed and
-- the healthy target was re-POSTed the same tip every minute until the dead
-- one recovered (at promotion the prod app-layer would re-broadcast the tip
-- to every prod client each minute while beta was down). The CLAIM (which
-- tip is being announced) stays on sync_state; WHICH target has it, and
-- which pending reorg fork that target has been told about (reorg_from,
-- the `reorgFrom` of the body it accepted), is recorded here, keyed by the
-- target URL: a retry goes only to the targets still owed, a fork rides to
-- each target until that target has accepted a body carrying it, and a
-- target added later (the prod app-layer at promotion) is owed the current
-- tip and receives it alone. A row for a target no longer configured is
-- ignored.
--
-- Why the column: the retry claim waits out the claim's in-flight window,
-- and that clock must not be `updated_at`, the /getInfo freshness signal
-- (audit M6), or a dead target retried every minute would keep
-- `lastSyncedAt` fresh through a stalled sync. `claimed_at` is stamped by
-- the claim and by every retry claim; NULL (a claim made by the pre-0006
-- build) counts as old, so the current tip is re-sent once after the deploy.
CREATE TABLE IF NOT EXISTS announce_deliveries (
    target TEXT PRIMARY KEY,
    height INTEGER NOT NULL,
    hash TEXT NOT NULL,
    reorg_from INTEGER,
    delivered_at TEXT NOT NULL DEFAULT (datetime('now'))
);
ALTER TABLE sync_state ADD COLUMN claimed_at TEXT;
