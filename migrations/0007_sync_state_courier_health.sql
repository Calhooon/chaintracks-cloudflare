-- bsv-low loop 10 D5 (2026-09-08): the courier health the cron records on
-- every pass, served on /getInfo. `last_seen_height` is the highest tip any
-- courier answered (with its time); `last_error` the LAST poll fault, with its
-- time, never cleared (a reader judges it by its age). The 965877 same-height
-- competition of loop 9 left the store 23 min behind with nothing recorded.
ALTER TABLE sync_state ADD COLUMN last_seen_height INTEGER;
ALTER TABLE sync_state ADD COLUMN last_seen_at TEXT;
ALTER TABLE sync_state ADD COLUMN last_error TEXT;
ALTER TABLE sync_state ADD COLUMN last_error_at TEXT;
