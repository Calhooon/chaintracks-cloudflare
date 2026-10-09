-- #32: durable envelopes and per-target webhook cursors. Apply after 0008.
-- JSON is constructed once here and delivered unchanged by every transport.
CREATE TABLE chain_events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    event_key TEXT UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE TABLE chain_event_deliveries (
    target TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0 CHECK(cursor >= 0)
);
CREATE TABLE chain_event_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    tip_hash TEXT
);
INSERT INTO chain_event_state(id) VALUES (1);
CREATE TABLE chain_event_signal (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    tick INTEGER NOT NULL DEFAULT 0
);
INSERT INTO chain_event_signal(id) VALUES (1);

-- Public header bytes plus cumulative work; the TS view removes chainWork.
CREATE VIEW chain_event_headers AS
SELECT headers.*, json_object('version', version, 'previousHash', previous_hash, 'merkleRoot', merkle_root, 'time', time, 'bits', bits, 'nonce', nonce, 'height', height, 'hash', hash, 'chainWork', chain_work) AS payload FROM headers;

-- Identical ceiling and tip selection to storage::served_tip (P0-4).
CREATE VIEW chain_event_served_tip AS
SELECT h.* FROM chain_event_headers h, sync_state s
WHERE s.id = 1 AND h.is_active = 1 AND (
    (s.validation_complete = 1 AND h.is_chain_tip = 1) OR
    (s.validation_complete = 0 AND s.validated_height IS NOT NULL AND (
        (h.is_chain_tip = 1 AND h.height <= s.validated_height) OR
        (h.height = s.validated_height AND NOT EXISTS (
            SELECT 1 FROM headers t WHERE t.is_chain_tip = 1 AND t.height <= s.validated_height
        ))
    ))
)
ORDER BY h.height DESC, h.header_id DESC LIMIT 1;

-- The previously served branch, tip to ancestor, bounded by the storage walk.
-- A lowered verification ceiling does not orphan a stored header.
CREATE VIEW chain_event_old_branch AS
WITH RECURSIVE branch AS (
    SELECT h.*, 0 AS distance FROM chain_event_headers h
    JOIN chain_event_state s ON h.hash = s.tip_hash WHERE s.id = 1
    UNION ALL
    SELECT p.*, b.distance + 1 FROM chain_event_headers p
    JOIN branch b ON p.hash = b.previous_hash
    WHERE b.is_active = 0 AND b.distance < 400
)
SELECT * FROM branch WHERE is_active = 0;

-- Publishing runs inside the transaction that changes the served tip.
-- No network and no second transaction can lose the event after a tip move.
CREATE TRIGGER chain_event_publish AFTER UPDATE OF tick ON chain_event_signal
WHEN EXISTS(SELECT 1 FROM chain_event_served_tip)
BEGIN
    INSERT INTO chain_events(payload)
    SELECT json_object('v', 1, 'kind', 'reorg',
        'forkHeight', (SELECT MIN(height) FROM chain_event_old_branch),
        'depth', (SELECT COUNT(*) FROM chain_event_old_branch),
        'deactivatedHeaders', json((SELECT json_group_array(json(payload)) FROM (
            SELECT payload FROM chain_event_old_branch ORDER BY distance
        ))),
        'newTip', json(t.payload))
    FROM chain_event_served_tip t, chain_event_state s
    WHERE s.id = 1 AND s.tip_hash IS NOT NULL AND s.tip_hash != t.hash
      AND EXISTS(SELECT 1 FROM chain_event_old_branch);

    INSERT INTO chain_events(payload)
    SELECT json_object('v', 1, 'kind', 'tip', 'height', t.height,
        'hash', t.hash, 'time', t.time, 'header', json(t.payload))
    FROM chain_event_served_tip t, chain_event_state s
    WHERE s.id = 1 AND (s.tip_hash IS NULL OR s.tip_hash != t.hash);

    INSERT OR IGNORE INTO chain_events(payload, event_key)
    SELECT json_object('v', 1, 'kind', 'tipAge', 'seconds', MAX(0, unixepoch() - t.time),
        'tip', json(t.payload)), 'tipAge:' || t.hash || ':' || (unixepoch() / 60)
    FROM chain_event_served_tip t, chain_event_state s
    WHERE s.id = 1 AND (s.tip_hash IS NULL OR s.tip_hash != t.hash);

    UPDATE chain_event_state SET tip_hash = (SELECT hash FROM chain_event_served_tip) WHERE id = 1;
END;
CREATE TRIGGER chain_event_tip AFTER UPDATE OF is_chain_tip ON headers
WHEN NEW.is_chain_tip = 1
BEGIN
    UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1;
END;
CREATE TRIGGER chain_event_initial_tip AFTER INSERT ON headers
WHEN NEW.is_chain_tip = 1
BEGIN
    UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1;
END;
CREATE TRIGGER chain_event_validation
AFTER UPDATE OF validated_height, validated_hash, validation_complete ON sync_state
WHEN OLD.validated_height IS NOT NEW.validated_height
  OR OLD.validated_hash IS NOT NEW.validated_hash
  OR OLD.validation_complete IS NOT NEW.validation_complete
BEGIN
    UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1;
END;

CREATE TRIGGER chain_event_fork_insert AFTER INSERT ON headers
WHEN NEW.is_active = 0 AND EXISTS(SELECT 1 FROM headers p WHERE p.hash = NEW.previous_hash)
BEGIN
    INSERT OR IGNORE INTO chain_events(payload, event_key)
    SELECT json_object('v', 1, 'kind', 'fork', 'height', a.height + 1,
        'depth', t.height - a.height,
        'competingTips', json_array(json(t.payload), json(n.payload))),
        'fork:' || n.hash || ':' || t.hash
    FROM chain_event_served_tip t, chain_event_headers n, (
        WITH RECURSIVE ancestry AS (
            SELECT h.hash, h.previous_hash, h.height, h.is_active, 0 AS distance
            FROM headers h WHERE h.hash = NEW.hash
            UNION ALL
            SELECT p.hash, p.previous_hash, p.height, p.is_active, a.distance + 1
            FROM headers p JOIN ancestry a ON p.hash = a.previous_hash
            WHERE a.is_active = 0 AND a.distance < 400
        )
        SELECT height FROM ancestry WHERE is_active = 1 ORDER BY distance LIMIT 1
    ) a, sync_state s
    WHERE n.hash = NEW.hash AND s.id = 1 AND n.previous_hash != t.hash
      AND (s.validation_complete = 1 OR n.height <= s.validated_height);
END;

CREATE TRIGGER chain_event_fork_relink AFTER UPDATE OF previous_header_id, chain_work ON headers
WHEN NEW.is_active = 0 AND EXISTS(SELECT 1 FROM headers p WHERE p.hash = NEW.previous_hash)
BEGIN
    INSERT OR IGNORE INTO chain_events(payload, event_key)
    SELECT json_object('v', 1, 'kind', 'fork', 'height', a.height + 1,
        'depth', t.height - a.height,
        'competingTips', json_array(json(t.payload), json(n.payload))),
        'fork:' || n.hash || ':' || t.hash
    FROM chain_event_served_tip t, chain_event_headers n, (
        WITH RECURSIVE ancestry AS (
            SELECT h.hash, h.previous_hash, h.height, h.is_active, 0 AS distance
            FROM headers h WHERE h.hash = NEW.hash
            UNION ALL
            SELECT p.hash, p.previous_hash, p.height, p.is_active, a.distance + 1
            FROM headers p JOIN ancestry a ON p.hash = a.previous_hash
            WHERE a.is_active = 0 AND a.distance < 400
        )
        SELECT height FROM ancestry WHERE is_active = 1 ORDER BY distance LIMIT 1
    ) a, sync_state s
    WHERE n.hash = NEW.hash AND s.id = 1 AND n.previous_hash != t.hash
      AND (s.validation_complete = 1 OR n.height <= s.validated_height);
END;

-- Start with the verified tip, including on an already populated store.
UPDATE chain_event_signal SET tick = tick + 1 WHERE id = 1;
