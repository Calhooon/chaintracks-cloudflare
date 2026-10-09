-- P0-4 (bsv-stack-lean #35, 2026-10-08): the re-validation of the stored chain
-- under the node's header rules (proof of work, the difficulty rule, the
-- checkpoints), from the last checkpoint the store holds, after the deploy
-- that brings the rules. `validated_height` and `validated_hash` are the
-- cursor (the last row passed); `validation_fault` the refusal that stopped
-- it (rows above the cursor are not served until the operator restarts it);
-- `validation_complete` 1 once the cursor reached the tip, after which every
-- row written has passed the same checks on the way in. A store with no rows
-- has nothing to re-validate; a store with rows starts the pass.
ALTER TABLE sync_state ADD COLUMN validated_height INTEGER;
ALTER TABLE sync_state ADD COLUMN validated_hash TEXT;
ALTER TABLE sync_state ADD COLUMN validation_fault TEXT;
ALTER TABLE sync_state ADD COLUMN validation_complete INTEGER NOT NULL DEFAULT 0;
UPDATE sync_state
SET validation_complete = CASE WHEN EXISTS (SELECT 1 FROM headers) THEN 0 ELSE 1 END
WHERE id = 1;
