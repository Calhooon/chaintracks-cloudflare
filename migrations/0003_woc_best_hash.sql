-- Persist WhatsOnChain's best block hash so /health can detect an unresolved
-- equal-height fork. When our tip sits at the same height as the network tip
-- but on a different block, a gap-only check reads "caught up" (gap 0) while
-- SPV reads are actually serving a losing branch (wrong merkleRoot). Storing
-- the network best hash lets /health compare it to our tip hash and report 503.
ALTER TABLE sync_state ADD COLUMN woc_best_hash TEXT NOT NULL DEFAULT '';
