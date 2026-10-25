-- 0.7.1, the liveness stamp (chaintracks #27, bsv-low #877): the end of the
-- last completed sync pass, idle passes included, served on /getInfo as
-- `lastPassAt`. NULL until the first pass after the migration. `updated_at`
-- (the last advance, audit M6) and the courier record (`last_seen_*`) keep
-- their meanings. Additive: a deploy before it still syncs (the stamp is
-- loud and best-effort) and /getInfo names the missing column. Apply after
-- 0012.
ALTER TABLE sync_state ADD COLUMN last_pass_at TEXT;
