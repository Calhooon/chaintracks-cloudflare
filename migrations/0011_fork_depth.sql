-- chaintracks-cloudflare #8: a fork is measured from the served tip, and
-- its depth is the served tip's height minus the competitor's nearest
-- active ancestor. A bulk span writes its rows active before it moves the
-- tip flag, so a competitor's active ancestor can stand at or above the
-- served tip; 0009 then wrote depth 0 or below, a row the decoder refuses.
-- A fork point at or above the served tip is above what was served: no
-- fork is journaled there (the tip and reorg triggers still publish when
-- the tip moves). The two triggers are 0009's with that one condition.
DROP TRIGGER chain_event_fork_insert;
DROP TRIGGER chain_event_fork_relink;

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
      AND (s.validation_complete = 1 OR n.height <= s.validated_height)
      AND a.height < t.height;
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
      AND (s.validation_complete = 1 OR n.height <= s.validated_height)
      AND a.height < t.height;
END;
