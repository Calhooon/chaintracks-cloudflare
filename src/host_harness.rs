//! The host harness (test-only): the storage and announce paths driven in
//! `cargo test` through their real code, against a real SQLite carrying the
//! real migrations, with the tip-webhook transport recorded instead of sent.
//!
//! bsv-low M19B-G2 (2026-09-08). Before this, `insert_header` and
//! `handle_reorg` took the D1 binding, so the reorg producer was pinned only at
//! the statement tier (each SQL constant executed by hand under rusqlite) and
//! the pure tier; the round-3 gate accepted that as a pre-existing gap (plan
//! doc R2 round 3, MED-1). `HeaderDb` (d1.rs) closes it: `SqliteDb` here is
//! the host implementation, `Recorder` wraps it to pin the statements the real
//! path hands the database and to script an interleaving, and
//! `RecordedWebhooks` stands in for the worker `Env` on the announce side.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::ToSql;
use serde::de::DeserializeOwned;
use sha2::Digest;

use crate::consensus::ChainParams;
use crate::d1::{HeaderDb, QVal, Query};
use crate::sync::{
    parse_webhook_targets, ChainSource, TipWebhooks, WebhookDelivery, WebhookTarget,
};
use crate::types::BlockHeader;
use crate::woc::WocChainInfo;

/// A `QVal` binds the way D1 binds it: `Int` as INTEGER, `Text` as TEXT,
/// `Bool` as the number 1/0 (`QVal::to_js` hands D1 a JS number), `Float`
/// as REAL, `Null` as NULL.
impl ToSql for QVal {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            QVal::Null => ToSqlOutput::Owned(Value::Null),
            QVal::Int(i) => ToSqlOutput::Owned(Value::Integer(*i)),
            QVal::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
            QVal::Bool(b) => ToSqlOutput::Owned(Value::Integer(i64::from(*b))),
            QVal::Float(f) => ToSqlOutput::Owned(Value::Real(*f)),
        })
    }
}

fn sql_err(e: rusqlite::Error) -> worker::Error {
    worker::Error::RustError(format!("sqlite: {e}"))
}

fn row_err(e: serde_json::Error) -> worker::Error {
    worker::Error::RustError(format!("row: {e}"))
}

/// The migration files as wrangler applies them: every `*.sql` in
/// `migrations/`, in name order, as `(file name, sql)`. Read at run time so a
/// migration added later is applied here without touching this file.
pub(crate) fn migrations() -> Vec<(String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .expect("migrations/ next to Cargo.toml")
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".sql"))
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let sql = std::fs::read_to_string(dir.join(&name)).unwrap();
            (name, sql)
        })
        .collect()
}

/// A real SQLite carrying the real schema: every migration applied in order
/// on an in-memory database.
pub(crate) struct SqliteDb {
    conn: rusqlite::Connection,
}

impl SqliteDb {
    pub fn migrated() -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for (name, sql) in migrations() {
            conn.execute_batch(&sql)
                .unwrap_or_else(|e| panic!("applying {name}: {e}"));
        }
        Self { conn }
    }

    /// The migrations whose file name sorts at or below `last` (a prefix such
    /// as "0007"), as a store written before a later deploy carries them.
    pub fn migrated_through(last: &str) -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for (name, sql) in migrations() {
            if name.as_str() <= last || name.starts_with(last) {
                conn.execute_batch(&sql)
                    .unwrap_or_else(|e| panic!("applying {name}: {e}"));
            }
        }
        Self { conn }
    }

    /// The migrations after `last`, in order: the deploy that lands on a
    /// store `migrated_through(last)` wrote.
    pub fn apply_migrations_after(&self, last: &str) {
        for (name, sql) in migrations() {
            if name.as_str() > last && !name.starts_with(last) {
                self.conn
                    .execute_batch(&sql)
                    .unwrap_or_else(|e| panic!("applying {name}: {e}"));
            }
        }
    }

    /// The raw connection, for a test's preconditions and assertions.
    pub fn conn(&self) -> &rusqlite::Connection {
        &self.conn
    }

    /// Run a read and shape each row the way D1 hands rows to serde: one
    /// object per row keyed by column name, INTEGER and REAL as JSON numbers
    /// (a JS number; the row structs read them as `f64`), TEXT as a string,
    /// NULL as null.
    fn rows(&self, q: &Query) -> worker::Result<Vec<serde_json::Value>> {
        let mut stmt = self.conn.prepare(q.sql()).map_err(sql_err)?;
        let names: Vec<String> = stmt.column_names().iter().map(|n| n.to_string()).collect();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(q.params()))
            .map_err(sql_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(sql_err)? {
            let mut object = serde_json::Map::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                let value = match row.get_ref(i).map_err(sql_err)? {
                    ValueRef::Null => serde_json::Value::Null,
                    ValueRef::Integer(n) => serde_json::Value::from(n),
                    ValueRef::Real(f) => serde_json::Value::from(f),
                    ValueRef::Text(t) => {
                        serde_json::Value::from(String::from_utf8_lossy(t).into_owned())
                    }
                    ValueRef::Blob(b) => serde_json::Value::from(b.to_vec()),
                };
                object.insert(name.clone(), value);
            }
            out.push(serde_json::Value::Object(object));
        }
        Ok(out)
    }
}

impl HeaderDb for SqliteDb {
    async fn first<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Option<T>> {
        self.rows(&q)?
            .into_iter()
            .next()
            .map(|row| serde_json::from_value(row).map_err(row_err))
            .transpose()
    }

    async fn all<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Vec<T>> {
        self.rows(&q)?
            .into_iter()
            .map(|row| serde_json::from_value(row).map_err(row_err))
            .collect()
    }

    async fn execute(&self, q: Query) -> worker::Result<u32> {
        self.conn
            .execute(q.sql(), rusqlite::params_from_iter(q.params()))
            .map(|changed| changed as u32)
            .map_err(sql_err)
    }

    async fn batch(&self, stmts: Vec<Query>) -> worker::Result<()> {
        // One batch is one transaction, as a D1 `batch` is.
        let tx = self.conn.unchecked_transaction().map_err(sql_err)?;
        for q in &stmts {
            tx.execute(q.sql(), rusqlite::params_from_iter(q.params()))
                .map_err(sql_err)?;
        }
        tx.commit().map_err(sql_err)
    }
}

/// What the real path handed the database, in order, with the batch
/// boundaries kept (one `Batch` is one transaction).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Handed {
    First(Query),
    All(Query),
    Execute(Query),
    Batch(Vec<Query>),
}

impl Handed {
    /// The statement texts in this entry.
    pub fn sql(&self) -> Vec<&str> {
        match self {
            Handed::First(q) | Handed::All(q) | Handed::Execute(q) => vec![q.sql()],
            Handed::Batch(qs) => qs.iter().map(Query::sql).collect(),
        }
    }
}

/// One scripted interleaving: a future run against the inner database.
type Interleave<'a> = Box<dyn FnOnce(&'a SqliteDb) -> Pin<Box<dyn Future<Output = ()> + 'a>> + 'a>;

/// A `HeaderDb` over a `SqliteDb` that records every statement it is handed
/// (the SQL pins) and can run ONE scripted interleaving right after a chosen
/// read answers (the race pins: "another isolate wrote between this read and
/// the write that follows it").
pub(crate) struct Recorder<'a> {
    inner: &'a SqliteDb,
    handed: RefCell<Vec<Handed>>,
    interleave: RefCell<Option<(String, Interleave<'a>)>>,
    fault: RefCell<Option<String>>,
}

impl<'a> Recorder<'a> {
    pub fn new(inner: &'a SqliteDb) -> Self {
        Self {
            inner,
            handed: RefCell::new(Vec::new()),
            interleave: RefCell::new(None),
            fault: RefCell::new(None),
        }
    }

    /// Answer the next `first` of exactly `sql` with an error (a D1 fault on
    /// that read), once.
    pub fn fault_first_of(&self, sql: &str) {
        *self.fault.borrow_mut() = Some(sql.to_string());
    }

    /// Everything handed so far, in order.
    pub fn handed(&self) -> Vec<Handed> {
        self.handed.borrow().clone()
    }

    /// Every distinct statement text handed so far, sorted.
    pub fn vocabulary(&self) -> Vec<String> {
        let mut texts: Vec<String> = self
            .handed
            .borrow()
            .iter()
            .flat_map(|h| h.sql().into_iter().map(str::to_string).collect::<Vec<_>>())
            .collect();
        texts.sort();
        texts.dedup();
        texts
    }

    /// Run `hook` once, right after the next `first` of exactly `sql` answers.
    pub fn after_first_of<F, Fut>(&self, sql: &str, hook: F)
    where
        F: FnOnce(&'a SqliteDb) -> Fut + 'a,
        Fut: Future<Output = ()> + 'a,
    {
        let boxed: Interleave<'a> = Box::new(move |db| Box::pin(hook(db)));
        *self.interleave.borrow_mut() = Some((sql.to_string(), boxed));
    }

    fn due_after(&self, sql: &str) -> Option<Interleave<'a>> {
        let mut slot = self.interleave.borrow_mut();
        match slot.take() {
            Some((wanted, hook)) if wanted == sql => Some(hook),
            other => {
                *slot = other;
                None
            }
        }
    }
}

impl<'a> HeaderDb for Recorder<'a> {
    async fn first<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Option<T>> {
        self.handed.borrow_mut().push(Handed::First(q.clone()));
        let faulted = {
            let mut slot = self.fault.borrow_mut();
            match slot.take() {
                Some(wanted) if wanted == q.sql() => true,
                other => {
                    *slot = other;
                    false
                }
            }
        };
        if faulted {
            return Err(worker::Error::RustError(format!(
                "injected fault: {}",
                q.sql()
            )));
        }
        let answer = self.inner.first::<T>(q.clone()).await;
        if let Some(hook) = self.due_after(q.sql()) {
            hook(self.inner).await;
        }
        answer
    }

    async fn all<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Vec<T>> {
        self.handed.borrow_mut().push(Handed::All(q.clone()));
        self.inner.all::<T>(q).await
    }

    async fn execute(&self, q: Query) -> worker::Result<u32> {
        self.handed.borrow_mut().push(Handed::Execute(q.clone()));
        self.inner.execute(q).await
    }

    async fn batch(&self, stmts: Vec<Query>) -> worker::Result<()> {
        self.handed.borrow_mut().push(Handed::Batch(stmts.clone()));
        self.inner.batch(stmts).await
    }
}

/// One recorded tip-webhook POST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Post {
    pub target: WebhookTarget,
    pub bearer: String,
    pub body: String,
}

/// A `TipWebhooks` standing in for the worker `Env`: the targets come from a
/// real `TIP_WEBHOOK_URLS` string through the real parser, the token from a
/// real `TIP_WEBHOOK_TOKEN` value, and every POST is recorded, never sent,
/// and answered with `reply`.
pub(crate) struct RecordedWebhooks {
    urls: String,
    token: String,
    pub reply: WebhookDelivery,
    replies_by_url: RefCell<HashMap<String, WebhookDelivery>>,
    posts: RefCell<Vec<Post>>,
}

impl RecordedWebhooks {
    pub fn new(urls: &str, token: &str) -> Self {
        Self {
            urls: urls.to_string(),
            token: token.to_string(),
            reply: WebhookDelivery::Accepted(200),
            replies_by_url: RefCell::new(HashMap::new()),
            posts: RefCell::new(Vec::new()),
        }
    }

    /// Answer `delivery` to this one URL (the others keep `reply`): a partial
    /// delivery.
    pub fn reply_for(&self, url: &str, delivery: WebhookDelivery) {
        self.replies_by_url
            .borrow_mut()
            .insert(url.to_string(), delivery);
    }

    /// The posts recorded since the last `take`, oldest first.
    pub fn take(&self) -> Vec<Post> {
        std::mem::take(&mut *self.posts.borrow_mut())
    }
}

impl TipWebhooks for RecordedWebhooks {
    fn targets(&self) -> Vec<WebhookTarget> {
        parse_webhook_targets(&self.urls)
    }

    fn token(&self) -> String {
        self.token.clone()
    }

    async fn post(&self, target: &WebhookTarget, token: &str, body: &str) -> WebhookDelivery {
        self.posts.borrow_mut().push(Post {
            target: target.clone(),
            bearer: token.to_string(),
            body: body.to_string(),
        });
        self.replies_by_url
            .borrow()
            .get(&target.url)
            .cloned()
            .unwrap_or_else(|| self.reply.clone())
    }
}

// ─── Chain fixtures ─────────────────────────────────────────────────────────

/// A 64-hex block hash, unique per label: the real shape, so nothing on the
/// path is fooled by a short id. Used for a parent the store never holds (a
/// fixture chain's root links to one) and for merkle roots; a fixture's own
/// hash is the real hash of its fields (`header_with_bits`).
pub(crate) fn block_hash(label: &str) -> String {
    let digest = hex::encode(sha2::Sha256::digest(label.as_bytes()));
    format!("{}{}", "0".repeat(16), &digest[16..])
}

/// The rules the host tests run under: the node's regtest (P0-4), which
/// never retargets (every header carries its parent's bits). Every fixture is
/// MINED under regtest's `powLimit`, so every path that checks proof of work
/// checks a real one; the mainnet and testnet rules run on real headers
/// (`pow_tests`, `retarget_tests`, `courier_tests`). A store starts only from
/// the genesis or a checkpoint, so the fixture chains' roots (each test
/// file's `x`) are the owner-configured checkpoints of these params, as an
/// operator's `CHECKPOINTS` would anchor a store.
pub(crate) fn regtest() -> &'static ChainParams {
    static PARAMS: std::sync::OnceLock<ChainParams> = std::sync::OnceLock::new();
    PARAMS.get_or_init(|| {
        let roots = [
            header(965_769, "x", &block_hash("the block below x")),
            header(965_875, "x", &block_hash("the block below x")),
            header(965_898, "x", &"1".repeat(64)),
        ];
        let spec = roots
            .iter()
            .map(|r| format!("{}:{}", r.height, r.hash))
            .collect::<Vec<_>>()
            .join(",");
        ChainParams::regtest().with_checkpoints(&spec).unwrap()
    })
}

/// The node's minimum-difficulty rule (testnet's `fPowAllowMinDifficultyBlocks`
/// in `GetNextEDAWorkRequired`, src/pow.cpp:44-62 at v1.2.3) on regtest's
/// `powLimit`: a header more than 20 minutes after its parent carries the
/// limit's bits, a prompt one the last bits that were not the limit's. It is
/// the node's own mechanism by which two children of ONE parent carry
/// different work, so the store's most-work rule (a heavier same-height
/// sibling takes the tip) is tested under it; regtest's rule (every header
/// its parent's bits) admits no such pair. The root is `heavy_root()`.
pub(crate) fn min_difficulty_rule() -> &'static ChainParams {
    static PARAMS: std::sync::OnceLock<ChainParams> = std::sync::OnceLock::new();
    PARAMS.get_or_init(|| {
        let mut p = ChainParams::regtest()
            .with_checkpoints(&format!("{}:{}", HEAVY_ROOT_HEIGHT, heavy_root().hash))
            .unwrap();
        p.no_retargeting = false;
        p.allow_min_difficulty_blocks = true;
        p.daa_height = u32::MAX;
        p
    })
}

/// The root of the minimum-difficulty fixtures: at `HEAVY_BITS`, so a prompt
/// child carries them and a late one the limit's.
pub(crate) const HEAVY_ROOT_HEIGHT: u32 = 965_769;

pub(crate) fn heavy_root() -> BlockHeader {
    header_with_bits(
        HEAVY_ROOT_HEIGHT,
        "x heavy root",
        &block_hash("the block below x"),
        HEAVY_BITS,
    )
}

/// Regtest's proof-of-work limit alone, for mining the fixtures (the anchors
/// of `regtest()` are mined fixtures themselves).
fn mining() -> &'static ChainParams {
    static PARAMS: std::sync::OnceLock<ChainParams> = std::sync::OnceLock::new();
    PARAMS.get_or_init(ChainParams::regtest)
}

/// Regtest's minimum difficulty (`GetCompact(powLimit)`): every block carries
/// the same work, so an equal-height sibling ties and only a longer branch
/// outworks the tip. About two hashes to mine.
pub(crate) const EQUAL_WORK_BITS: u32 = 0x207f_ffff;

/// A heavier block under regtest's limit: a target 128 times smaller than
/// `EQUAL_WORK_BITS`, so about 128 times the work (and about 256 hashes to mine).
pub(crate) const HEAVY_BITS: u32 = 0x2000_ffff;

/// A header at `height` linking to `parent`, at `EQUAL_WORK_BITS`.
pub(crate) fn header(height: u32, label: &str, parent: &str) -> BlockHeader {
    header_with_bits(height, label, parent, EQUAL_WORK_BITS)
}

/// A header at `height` linking to `parent`, at `bits`, mined: the nonce is
/// the first from 0 whose hash meets the target, and `hash` is that hash. The
/// label fixes the merkle root, so the fixture is the same on every run.
pub(crate) fn header_with_bits(height: u32, label: &str, parent: &str, bits: u32) -> BlockHeader {
    header_at(height, label, parent, bits, 1_757_280_000 + height)
}

/// `header_with_bits` at a given `time`.
pub(crate) fn header_at(
    height: u32,
    label: &str,
    parent: &str,
    bits: u32,
    time: u32,
) -> BlockHeader {
    let mut h = BlockHeader {
        version: 0x2000_0000,
        previous_hash: parent.to_string(),
        merkle_root: block_hash(&format!("root of {label}")),
        time,
        bits,
        nonce: 0,
        height,
        ..Default::default()
    };
    loop {
        h.hash = crate::types::compute_block_hash(&h.to_bytes());
        if h.check_pow(mining()).is_ok() {
            return h;
        }
        h.nonce += 1;
    }
}

/// `sync_state.pending_reorg_from`, read raw.
pub(crate) fn pending(db: &SqliteDb) -> Option<i64> {
    db.conn()
        .query_row(
            "SELECT pending_reorg_from FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// `(last_synced_height, last_announced_hash)`, read raw.
pub(crate) fn announced(db: &SqliteDb) -> (i64, Option<String>) {
    db.conn()
        .query_row(
            "SELECT last_synced_height, last_announced_hash FROM sync_state WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

/// `(is_active, is_chain_tip)` of the row with this hash, read raw.
pub(crate) fn flags(db: &SqliteDb, hash: &str) -> (bool, bool) {
    db.conn()
        .query_row(
            "SELECT is_active, is_chain_tip FROM headers WHERE hash = ?1",
            [hash],
            |r| Ok((r.get::<_, i64>(0)? == 1, r.get::<_, i64>(1)? == 1)),
        )
        .unwrap_or_else(|e| panic!("no row for {hash}: {e}"))
}

/// The row id of this hash, read raw.
pub(crate) fn header_id(db: &SqliteDb, hash: &str) -> i64 {
    db.conn()
        .query_row(
            "SELECT header_id FROM headers WHERE hash = ?1",
            [hash],
            |r| r.get(0),
        )
        .unwrap_or_else(|e| panic!("no row for {hash}: {e}"))
}

/// Move the claim's clock back by `minutes` (the in-flight window is read
/// from `sync_state.updated_at` by the retry claim; on the host no time
/// passes between two crons, so the test ages the claim itself).
pub(crate) fn age_claim(db: &SqliteDb, minutes: u32) {
    db.conn()
        .execute(
            "UPDATE sync_state SET claimed_at = datetime('now', ?1) WHERE id = 1",
            [format!("-{minutes} minutes")],
        )
        .unwrap();
}

/// `sync_state.updated_at`, the /getInfo freshness signal, read raw.
pub(crate) fn freshness(db: &SqliteDb) -> Option<String> {
    db.conn()
        .query_row("SELECT updated_at FROM sync_state WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap()
}

/// Plant a distinctive `updated_at` (a second's resolution would hide a
/// rewrite made within the same second).
pub(crate) fn set_freshness(db: &SqliteDb, value: &str) {
    db.conn()
        .execute(
            "UPDATE sync_state SET updated_at = ?1 WHERE id = 1",
            [value],
        )
        .unwrap();
}

/// Every per-target delivery, `(target, height, hash, reorg_from)`, by
/// target, read raw.
pub(crate) fn deliveries(db: &SqliteDb) -> Vec<(String, i64, String, Option<i64>)> {
    let mut stmt = db
        .conn()
        .prepare("SELECT target, height, hash, reorg_from FROM announce_deliveries ORDER BY target")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// `(announce_failures, tip_announce_stuck_total)`, read raw.
pub(crate) fn announce_counters(db: &SqliteDb) -> (i64, i64) {
    db.conn()
        .query_row(
            "SELECT announce_failures, tip_announce_stuck_total FROM sync_state WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

// ─── The scripted chain (round 4) ───────────────────────────────────────────

/// The chain the cron reads on the host: a scripted WhatsOnChain. `tip` is
/// what `/chain/info` answers, `publish` what the header routes serve (the
/// latest published at a height wins, as WoC serves the active chain; every
/// published header stays reachable by hash), `unavailable` makes every call
/// fail.
pub(crate) struct ScriptedChain {
    blocks: Cell<u32>,
    best: RefCell<Option<String>>,
    by_height: RefCell<HashMap<u32, BlockHeader>>,
    by_hash: RefCell<HashMap<String, BlockHeader>>,
    unavailable: Cell<bool>,
    /// Every call, answered or refused (bsv-low loop 10 D5: the ladder's
    /// per-tick skip of a faulting rung is pinned on this count).
    calls: Cell<u32>,
}

impl ScriptedChain {
    pub fn new() -> Self {
        Self {
            blocks: Cell::new(0),
            best: RefCell::new(None),
            by_height: RefCell::new(HashMap::new()),
            by_hash: RefCell::new(HashMap::new()),
            unavailable: Cell::new(false),
            calls: Cell::new(0),
        }
    }
    /// How many calls this chain has taken (answered or refused).
    pub fn calls(&self) -> u32 {
        self.calls.get()
    }

    /// WoC's view of the tip: its height and best block hash.
    pub fn tip(&self, blocks: u32, best: &str) {
        self.blocks.set(blocks);
        *self.best.borrow_mut() = Some(best.to_string());
    }

    /// A header WoC serves.
    pub fn publish(&self, h: &BlockHeader) {
        self.by_height.borrow_mut().insert(h.height, h.clone());
        self.by_hash.borrow_mut().insert(h.hash.clone(), h.clone());
    }

    pub fn set_unavailable(&self, down: bool) {
        self.unavailable.set(down);
    }

    fn down(&self) -> worker::Result<()> {
        self.calls.set(self.calls.get() + 1);
        if self.unavailable.get() {
            Err(worker::Error::RustError(
                "scripted chain: unavailable".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

impl ChainSource for ScriptedChain {
    async fn chain_info(&self) -> worker::Result<WocChainInfo> {
        self.down()?;
        Ok(WocChainInfo {
            blocks: self.blocks.get(),
            best_block_hash: self.best.borrow().clone(),
        })
    }

    async fn header_by_height(&self, height: u32) -> worker::Result<BlockHeader> {
        self.down()?;
        self.by_height
            .borrow()
            .get(&height)
            .cloned()
            .ok_or_else(|| {
                worker::Error::RustError(format!("scripted chain: no header at {height}"))
            })
    }

    async fn header_by_hash(&self, hash: &str) -> worker::Result<BlockHeader> {
        self.down()?;
        self.by_hash
            .borrow()
            .get(hash)
            .cloned()
            .ok_or_else(|| worker::Error::RustError(format!("scripted chain: no header {hash}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The host schema is the deployed schema: every migration, in the order
    /// wrangler applies them, the four that exist today first. A migration
    /// added later is applied without touching this list; one renamed or
    /// deleted reds here.
    #[test]
    fn the_host_schema_is_every_migration_in_order() {
        let names: Vec<String> = migrations().into_iter().map(|(n, _)| n).collect();
        assert!(
            names.starts_with(&[
                "0001_initial.sql".to_string(),
                "0002_active_height_index.sql".to_string(),
                "0003_sync_state_announced_hash.sql".to_string(),
                "0004_sync_state_pending_reorg.sql".to_string(),
            ]),
            "{names:?}"
        );
        for (i, name) in names.iter().enumerate() {
            assert!(
                name.starts_with(&format!("{:04}_", i + 1)),
                "migrations are numbered without a gap: {names:?}"
            );
        }
        let db = SqliteDb::migrated();
        let columns: Vec<String> = db
            .conn()
            .prepare("SELECT * FROM sync_state")
            .unwrap()
            .column_names()
            .iter()
            .map(|c| c.to_string())
            .collect();
        assert!(
            columns.contains(&"last_announced_hash".to_string()),
            "{columns:?}"
        );
        assert!(
            columns.contains(&"pending_reorg_from".to_string()),
            "{columns:?}"
        );
    }

    /// Rows reach the row structs the way D1 hands them over: numbers as
    /// numbers the `f64` fields accept, NULL as `None`, one object per row;
    /// an empty read is `None` / an empty vec; a write answers its changes.
    #[tokio::test]
    async fn the_host_db_keeps_the_d1_row_convention() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Row {
            height: Option<f64>,
            hash: Option<String>,
            previous_header_id: Option<f64>,
        }
        let db = SqliteDb::migrated();
        let none: Option<Row> =
            Query::new("SELECT height, hash, previous_header_id FROM headers WHERE hash = ?")
                .bind("nothing")
                .first(&db)
                .await
                .unwrap();
        assert_eq!(none, None);
        let changed = Query::new(
            "INSERT INTO headers (previous_header_id, previous_hash, height, is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(Option::<i64>::None)
        .bind("p")
        .bind(7u32)
        .bind(true)
        .bind(false)
        .bind("h")
        .bind("w")
        .bind(1u32)
        .bind("r")
        .bind(2u32)
        .bind(3u32)
        .bind(4u32)
        .run_changes(&db)
        .await
        .unwrap();
        assert_eq!(changed, 1);
        let row: Option<Row> =
            Query::new("SELECT height, hash, previous_header_id FROM headers WHERE hash = ?")
                .bind("h")
                .first(&db)
                .await
                .unwrap();
        assert_eq!(
            row,
            Some(Row {
                height: Some(7.0),
                hash: Some("h".to_string()),
                previous_header_id: None
            })
        );
        let all: Vec<Row> = Query::new("SELECT height, hash, previous_header_id FROM headers")
            .all(&db)
            .await
            .unwrap();
        assert_eq!(all.len(), 1);
        // A `Bool` bind lands as the integer D1 stores for a JS 1/0.
        let active: i64 = db
            .conn()
            .query_row("SELECT is_active FROM headers WHERE hash = 'h'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(active, 1);
    }

    /// A batch is one transaction: a failing statement rolls the whole batch
    /// back, so a reorg can never half-apply on the host any more than on D1.
    #[tokio::test]
    async fn a_batch_is_one_transaction() {
        let db = SqliteDb::migrated();
        let err = db
            .batch(vec![
                Query::new("UPDATE sync_state SET last_synced_height = 5 WHERE id = 1"),
                Query::new("UPDATE no_such_table SET x = 1"),
            ])
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no such table"), "{err}");
        let height: i64 = db
            .conn()
            .query_row(
                "SELECT last_synced_height FROM sync_state WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            height, 0,
            "the first statement was rolled back with the batch"
        );
    }
}
