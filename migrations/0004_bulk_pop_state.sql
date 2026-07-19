-- Self-populating R2 bulk store: per-file progress + a soft lease so the cron
-- can fill the R2 bulk files from the CDN on idle ticks without a manual
-- `wrangler r2 object put`, then verify them lazily in bounded chunks.
--
-- Presence is authoritative in R2 (checked via head()); this table only tracks
-- verification progress and a lease. A file is "verified" only after Phase 2
-- has read every header back from R2 and confirmed full linkage + last hash.
CREATE TABLE IF NOT EXISTS bulk_pop_state (
    file_idx          INTEGER PRIMARY KEY,          -- 0..N (100k-header file index)
    verified          INTEGER NOT NULL DEFAULT 0,   -- 0 = present_unverified/absent, 1 = fully verified
    verify_offset     INTEGER NOT NULL DEFAULT 0,   -- next header index to verify (Phase 2 resume point)
    verify_prev_hash  TEXT    NOT NULL DEFAULT '',  -- last verified header's hash (linkage across chunks)
    lease_until       INTEGER NOT NULL DEFAULT 0,   -- epoch seconds; a tick holds the lease while working
    lease_token       TEXT    NOT NULL DEFAULT '',  -- claimant token: compare-and-claim so a cron tick and an
                                                    -- /admin/self-pop call can never both work the same key
    updated_at        TEXT    NOT NULL DEFAULT (datetime('now'))
);
