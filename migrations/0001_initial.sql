-- Chaintracks D1 Schema
-- Block header storage for BSV chain tracking service.
-- Based on bsv-wallet-toolbox-rs SqliteStorage schema, adapted for Cloudflare D1.

CREATE TABLE IF NOT EXISTS headers (
    header_id INTEGER PRIMARY KEY,
    previous_header_id INTEGER,
    previous_hash TEXT NOT NULL,
    height INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0,
    is_chain_tip INTEGER NOT NULL DEFAULT 0,
    hash TEXT NOT NULL UNIQUE,
    chain_work TEXT NOT NULL,
    version INTEGER NOT NULL,
    merkle_root TEXT NOT NULL,
    time INTEGER NOT NULL,
    bits INTEGER NOT NULL,
    nonce INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Primary lookup indexes
CREATE INDEX IF NOT EXISTS idx_headers_height ON headers(height);
CREATE INDEX IF NOT EXISTS idx_headers_active ON headers(is_active);
CREATE INDEX IF NOT EXISTS idx_headers_tip ON headers(is_chain_tip);
CREATE INDEX IF NOT EXISTS idx_headers_prev_hash ON headers(previous_hash);

-- Partial index: only index merkle roots for active chain headers
CREATE INDEX IF NOT EXISTS idx_headers_merkle_active
    ON headers(merkle_root) WHERE is_active = 1;

-- Sync state: tracks last-known chain height and sync progress
CREATE TABLE IF NOT EXISTS sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    chain TEXT NOT NULL DEFAULT 'main',
    last_synced_height INTEGER NOT NULL DEFAULT 0,
    bulk_sync_complete INTEGER NOT NULL DEFAULT 0,
    live_sync_active INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO sync_state (id, chain) VALUES (1, 'main');
