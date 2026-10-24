-- 0.7.0, the retention's floor (bsv-stack-lean docs/p0/chaintracks-reorg-feed.md
-- section 6, CF2): the greatest cursor of a deleted chain row, 0 until one is
-- deleted. A page or a stream from a cursor below it answers 410
-- cursorExpired; at or above it, the page the full journal would give.
-- Additive: feed "1" never reads it and deletes nothing. Apply after 0011.
ALTER TABLE chain_event_state ADD COLUMN floor INTEGER NOT NULL DEFAULT 0;
