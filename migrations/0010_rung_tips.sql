-- One latest observation per configured rung, never a history of polls.
CREATE TABLE sync_rung_tips (
    name TEXT PRIMARY KEY,
    implementation TEXT NOT NULL,
    hash TEXT,
    height INTEGER,
    observed_at INTEGER
);

-- Work and active membership are always read from the current store. A
-- claimed tip that is missing, unlinked or above the verification ceiling
-- has no checked work/time. No courier-supplied work enters the snapshot.
CREATE VIEW sync_rung_headers AS
SELECT r.*, h.chain_work, h.time, COALESCE(h.is_active, 0) AS is_active
FROM sync_rung_tips r
LEFT JOIN headers h ON h.hash = r.hash AND h.height = r.height
    AND (h.is_active = 1 OR h.previous_header_id IS NOT NULL)
    AND EXISTS(SELECT 1 FROM sync_state s WHERE s.id = 1
        AND (s.validation_complete = 1 OR h.height <= s.validated_height));

-- The store already bounds its branch walks at 400. Missing ancestry is
-- unknown, never evidence that a previously observed split has resolved.
CREATE VIEW chain_rung_ancestry AS
WITH RECURSIVE ancestry(name, hash, previous_header_id, height, distance) AS (
    SELECT r.name, h.hash, h.previous_header_id, h.height, 0
    FROM sync_rung_headers r JOIN headers h ON h.hash = r.hash
    WHERE r.chain_work IS NOT NULL
    UNION ALL
    SELECT a.name, p.hash, p.previous_header_id, p.height, a.distance + 1
    FROM ancestry a JOIN headers p ON p.header_id = a.previous_header_id
    WHERE a.distance < 400 AND p.height + 1 = a.height
)
SELECT * FROM ancestry;

-- One common ancestor per pair, including equal or ancestor/descendant tips.
CREATE VIEW chain_rung_pairs AS
SELECT left_name, right_name, fork_hash, ancestor_height FROM (
    SELECT a.name AS left_name, b.name AS right_name,
        a.hash AS fork_hash, a.height AS ancestor_height,
        ROW_NUMBER() OVER (
            PARTITION BY a.name, b.name ORDER BY a.height DESC
        ) AS position
    FROM chain_rung_ancestry a JOIN chain_rung_ancestry b
        ON a.hash = b.hash AND a.name < b.name
) WHERE position = 1;

CREATE TABLE chain_splits (
    fork_hash TEXT PRIMARY KEY,
    fork_height INTEGER NOT NULL,
    tips TEXT NOT NULL CHECK(json_valid(tips)),
    resolved INTEGER NOT NULL DEFAULT 0 CHECK(resolved IN (0, 1))
);
CREATE TABLE chain_split_signal (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    depth INTEGER NOT NULL DEFAULT 3 CHECK(depth BETWEEN 1 AND 400),
    tick INTEGER NOT NULL DEFAULT 0
);
INSERT INTO chain_split_signal(id) VALUES (1);

CREATE TRIGGER chain_split_open AFTER INSERT ON chain_splits
BEGIN
    INSERT INTO chain_events(payload, event_key)
    VALUES (json_object('v', 2, 'kind', 'split',
        'key', 'split:' || NEW.fork_hash, 'forkHeight', NEW.fork_height,
        'tips', json(NEW.tips)), 'split:' || NEW.fork_hash);
END;

CREATE TRIGGER chain_split_resolve AFTER UPDATE OF resolved ON chain_splits
WHEN OLD.resolved = 0 AND NEW.resolved = 1
BEGIN
    INSERT INTO chain_events(payload, event_key)
    VALUES (json_object('v', 2, 'kind', 'splitResolved',
        'key', 'split:' || NEW.fork_hash, 'forkHeight', NEW.fork_height),
        'splitResolved:' || NEW.fork_hash);
END;

CREATE TRIGGER chain_split_detect AFTER UPDATE OF tick ON chain_split_signal
BEGIN
    INSERT OR IGNORE INTO chain_splits(fork_hash, fork_height, tips)
    SELECT p.fork_hash, p.ancestor_height + 1,
        json_array(
            json_object('hash', l.hash, 'height', l.height, 'chainWork', l.chain_work,
                'rungs', json((SELECT json_group_array(name) FROM (
                    SELECT name FROM sync_rung_tips WHERE hash = l.hash ORDER BY name)))),
            json_object('hash', r.hash, 'height', r.height, 'chainWork', r.chain_work,
                'rungs', json((SELECT json_group_array(name) FROM (
                    SELECT name FROM sync_rung_tips WHERE hash = r.hash ORDER BY name))))
        )
    FROM chain_rung_pairs p
    JOIN sync_rung_headers l ON l.name = p.left_name
    JOIN sync_rung_headers r ON r.name = p.right_name
    WHERE l.height - p.ancestor_height >= NEW.depth
        AND r.height - p.ancestor_height >= NEW.depth
    ORDER BY p.left_name, p.right_name;

    UPDATE chain_splits SET resolved = 1
    WHERE resolved = 0
        AND (SELECT COUNT(*) FROM chain_rung_pairs) = (
            SELECT COUNT(*) * (COUNT(*) - 1) / 2 FROM sync_rung_tips
        )
        AND NOT EXISTS (
            SELECT 1 FROM chain_rung_pairs p
            JOIN sync_rung_tips l ON l.name = p.left_name
            JOIN sync_rung_tips r ON r.name = p.right_name
            WHERE p.fork_hash = chain_splits.fork_hash
                AND l.height > p.ancestor_height AND r.height > p.ancestor_height
        );
END;
