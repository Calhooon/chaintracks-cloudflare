-- #31, the writer lease (#32 ask 1: the cron's poll, the tip stream's push
-- and the link heal were three writers of one chain state with nothing
-- between them; on 2026-11-02 18:48Z a reorg sync and a push of the same
-- headers interleaved). One row's lease: the owner (a role and a random
-- token, `sync::take_lease`) and its expiry in Unix seconds. A cron pass, a
-- tick the push covers and a push through the door each take it before
-- they write and give it back after; a writer that finds it held writes
-- nothing (the cron's next tick, the object's retry ask again), and a
-- holder that died frees it at the expiry (`sync::LEASE_S`). NULL: no
-- writer. Additive: a deploy before it still syncs, unserialized, and every
-- writer logs the missing column. Apply after 0014.
ALTER TABLE sync_state ADD COLUMN lease_owner TEXT;
ALTER TABLE sync_state ADD COLUMN lease_until INTEGER;
