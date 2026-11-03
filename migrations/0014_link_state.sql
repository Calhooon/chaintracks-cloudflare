-- #30, the link state (the stall of 2026-11-02 18:48Z): the link check of
-- the top of the active chain (`storage::LINK_WINDOW` heights under the
-- tip), as the last pass read it, a JSON object served on /getInfo as
-- `linkState`. Written only when the answer changes, so an idle pass on a
-- linked chain still moves `last_pass_at` alone (#27). NULL until the first
-- pass after the migration. Additive: a deploy before it still syncs (the
-- write is loud and best-effort) and /getInfo names the missing column.
-- Apply after 0013.
ALTER TABLE sync_state ADD COLUMN link_state TEXT;
