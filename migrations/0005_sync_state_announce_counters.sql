-- 0005: the announce delivery counters (bsv-low M19B-G2 round 2, 2026-09-08).
-- ADDITIVE and idempotent under wrangler's migration ledger; apply with
-- `npx wrangler d1 migrations apply rust-chaintracks --remote` BEFORE
-- deploying the build that ships with it (that build reads both columns on
-- every announce; the pre-0005 build never reads them).
--
-- Why: the announce row and the pending-reorg clear were written BEFORE the
-- webhook POST, so a refused or failed delivery consumed the `reorgFrom`
-- marker unretried and the overlay never got its targeted re-verify (only
-- the slow sweep healed). The announce now records the row and clears the
-- marker only after EVERY target accepted; otherwise both stay untouched and
-- the next cron re-announces the same body (a repeat is idempotent on the
-- overlay side). announce_failures counts the consecutive undelivered
-- announces (reset on delivery); tip_announce_stuck_total counts every
-- ANNOUNCE_STUCK_AFTER (5) of them, each with a loud log line, so a dead
-- consumer is visible on /getInfo rather than silent.
ALTER TABLE sync_state ADD COLUMN announce_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_state ADD COLUMN tip_announce_stuck_total INTEGER NOT NULL DEFAULT 0;
