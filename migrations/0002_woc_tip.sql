-- Persist the last-observed network tip (from WhatsOnChain) so /getInfo can
-- report how far behind the tracked chain is. Drives the self-monitoring
-- `behindBy` / `isSyncing` fields and lets an external alert notice a stall.
ALTER TABLE sync_state ADD COLUMN woc_tip_height INTEGER NOT NULL DEFAULT 0;
