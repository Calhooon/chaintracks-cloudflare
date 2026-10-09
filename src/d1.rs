//! D1 database helpers ; parameterized queries for Cloudflare D1.
//!
//! Adapted from ~/bsv/rust-overlay/crates/overlay-cloudflare/src/d1/mod.rs.
//!
//! bsv-low M19B-G2 (2026-09-08): `HeaderDb` is the statement seam. Every read
//! and write the storage layer runs is a `Query` (statement text + binds)
//! handed to a `HeaderDb`, so the reorg producer (`insert_header`,
//! `handle_reorg`, `notify_if_tip_advanced`) is driven on a host against real
//! SQLite carrying the real migrations (`host_harness.rs`, test-only). The
//! worker's `HeaderDb` is `D1Database`, a passthrough: D1 receives the same
//! statement strings and binds it received before the seam existed (pinned by
//! `reorg_producer_tests`, which compares what the real path hands the database
//! with the literals from main `d2317f2`).

use serde::de::DeserializeOwned;
use worker::wasm_bindgen::JsValue;
use worker::{D1Database, D1PreparedStatement};

/// A value that can be bound to a D1 prepared statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QVal {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(f64),
}

impl QVal {
    pub fn to_js(&self) -> JsValue {
        match self {
            Self::Null => JsValue::null(),
            Self::Int(i) => JsValue::from_f64(*i as f64),
            Self::Text(s) => JsValue::from_str(s),
            Self::Bool(b) => JsValue::from_f64(if *b { 1.0 } else { 0.0 }),
            Self::Float(f) => JsValue::from_f64(*f),
        }
    }
}

impl From<i64> for QVal {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<i32> for QVal {
    fn from(v: i32) -> Self {
        Self::Int(v as i64)
    }
}
impl From<u32> for QVal {
    fn from(v: u32) -> Self {
        Self::Int(v as i64)
    }
}
impl From<u64> for QVal {
    fn from(v: u64) -> Self {
        Self::Int(v as i64)
    }
}
impl From<String> for QVal {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<&str> for QVal {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}
impl From<bool> for QVal {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<f64> for QVal {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl<T: Into<QVal>> From<Option<T>> for QVal {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

/// One statement with its binds: the value the storage layer hands a
/// [`HeaderDb`]. Built with `new` + `bind`; run with `first` / `all` / `run` /
/// `run_changes` against any `HeaderDb`.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    sql: String,
    params: Vec<QVal>,
}

impl Query {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn bind(mut self, val: impl Into<QVal>) -> Self {
        self.params.push(val.into());
        self
    }

    /// The statement text, exactly as it reaches the database (the host
    /// harness runs and records it; the worker only ever `prepare`s).
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The binds, in order (host harness, as `sql`).
    #[cfg(test)]
    pub fn params(&self) -> &[QVal] {
        &self.params
    }

    /// The D1 prepared statement (the worker's `HeaderDb` runs this).
    pub fn prepare(self, db: &D1Database) -> worker::Result<D1PreparedStatement> {
        let stmt = db.prepare(&self.sql);
        if self.params.is_empty() {
            return Ok(stmt);
        }
        let js_values: Vec<JsValue> = self.params.iter().map(|v| v.to_js()).collect();
        stmt.bind(&js_values)
    }

    pub async fn run(self, db: &impl HeaderDb) -> worker::Result<()> {
        db.execute(self).await?;
        Ok(())
    }

    /// Run a write and return how many rows it changed (`meta.changes`), the
    /// one number a conditional UPDATE needs to be an atomic "did I win?".
    pub async fn run_changes(self, db: &impl HeaderDb) -> worker::Result<u32> {
        db.execute(self).await
    }

    pub async fn first<T: DeserializeOwned>(self, db: &impl HeaderDb) -> worker::Result<Option<T>> {
        db.first(self).await
    }

    pub async fn all<T: DeserializeOwned>(self, db: &impl HeaderDb) -> worker::Result<Vec<T>> {
        db.all(self).await
    }
}

// ─── The statement seam ─────────────────────────────────────────────────────

/// The four statement shapes the storage layer runs. The worker's
/// implementation is [`D1Database`] (a passthrough to D1, below); the host
/// harness implements it over rusqlite with the real migrations, so the reorg
/// producer runs through its real code in `cargo test`.
///
/// Rows come back the way D1 hands them to serde: one object per row, every
/// number a JS number (an f64), NULL a null. The row structs are written for
/// that (`HeaderRow` is all `Option<f64>`), and the host implementation keeps
/// the same convention, so a row struct that deserializes on one deserializes
/// on the other.
pub trait HeaderDb {
    /// The first row of a read, or `None` when the read answers no row.
    async fn first<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Option<T>>;
    /// Every row of a read.
    async fn all<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Vec<T>>;
    /// One write; answers the rows it changed (D1 `meta.changes`).
    async fn execute(&self, q: Query) -> worker::Result<u32>;
    /// Up to 100 statements as ONE transaction (D1 `batch`).
    async fn batch(&self, stmts: Vec<Query>) -> worker::Result<()>;
}

/// The worker's database: each shape is the D1 call the storage layer made
/// before the seam, on the same statement text and the same binds.
impl HeaderDb for D1Database {
    async fn first<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Option<T>> {
        q.prepare(self)?.first::<T>(None).await
    }

    async fn all<T: DeserializeOwned>(&self, q: Query) -> worker::Result<Vec<T>> {
        q.prepare(self)?.all().await?.results::<T>()
    }

    async fn execute(&self, q: Query) -> worker::Result<u32> {
        let res = q.prepare(self)?.run().await?;
        Ok(res
            .meta()
            .ok()
            .flatten()
            .and_then(|m| m.changes)
            .unwrap_or(0) as u32)
    }

    async fn batch(&self, stmts: Vec<Query>) -> worker::Result<()> {
        let mut prepared = Vec::with_capacity(stmts.len());
        for q in stmts {
            prepared.push(q.prepare(self)?);
        }
        D1Database::batch(self, prepared).await?;
        Ok(())
    }
}

// ─── Batch Collector ────────────────────────────────────────────────────────

/// Collects statements for atomic batch execution via `HeaderDb::batch`.
///
/// D1 has no BEGIN/COMMIT. Instead, `db.batch(stmts)` executes all statements
/// atomically (up to 100 per batch). If the batch exceeds 100 statements,
/// it is split into sequential sub-batches.
pub struct BatchCollector<'a, D: HeaderDb> {
    db: &'a D,
    statements: Vec<Query>,
}

impl<'a, D: HeaderDb> BatchCollector<'a, D> {
    pub fn new(db: &'a D) -> Self {
        Self {
            db,
            statements: Vec::new(),
        }
    }

    /// Add a parameterized statement to the batch.
    pub fn add(&mut self, sql: &str, params: Vec<QVal>) {
        self.statements.push(Query {
            sql: sql.to_string(),
            params,
        });
    }

    /// Number of statements in the batch.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    /// Execute all statements atomically.
    ///
    /// Splits into chunks of 100 (D1 limit). Each sub-batch is atomic
    /// internally, but failures in later batches won't roll back earlier ones.
    pub async fn execute(self) -> worker::Result<()> {
        let mut stmts = self.statements;
        while !stmts.is_empty() {
            let chunk: Vec<Query> = stmts.drain(..stmts.len().min(100)).collect();
            self.db.batch(chunk).await?;
        }
        Ok(())
    }

    /// A chain transition must commit together with its trigger-written
    /// envelope. Bulk callers can keep using the chunked execute method;
    /// the reorg caller bounds this batch with its 400-step ancestor walk.
    pub async fn execute_atomic(self) -> worker::Result<()> {
        self.db.batch(self.statements).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: to_js() tests require WASM target and can't run natively.
    // We test the From impls and Query builder which are pure Rust.

    #[test]
    fn test_from_i64() {
        let val: QVal = 42i64.into();
        assert!(matches!(val, QVal::Int(42)));
    }

    #[test]
    fn test_from_i32() {
        let val: QVal = 42i32.into();
        assert!(matches!(val, QVal::Int(42)));
    }

    #[test]
    fn test_from_u32() {
        let val: QVal = 100u32.into();
        assert!(matches!(val, QVal::Int(100)));
    }

    #[test]
    fn test_from_u64() {
        let val: QVal = 999u64.into();
        assert!(matches!(val, QVal::Int(999)));
    }

    #[test]
    fn test_from_string() {
        let val: QVal = String::from("hello").into();
        assert!(matches!(val, QVal::Text(s) if s == "hello"));
    }

    #[test]
    fn test_from_str() {
        let val: QVal = "test".into();
        assert!(matches!(val, QVal::Text(s) if s == "test"));
    }

    #[test]
    fn test_from_bool() {
        let val: QVal = true.into();
        assert!(matches!(val, QVal::Bool(true)));

        let val: QVal = false.into();
        assert!(matches!(val, QVal::Bool(false)));
    }

    #[test]
    fn test_from_f64() {
        let val: QVal = 2.5f64.into();
        assert!(matches!(val, QVal::Float(f) if (f - 2.5).abs() < f64::EPSILON));
    }

    #[test]
    fn test_from_option_some() {
        let val: QVal = Some(42i64).into();
        assert!(matches!(val, QVal::Int(42)));
    }

    #[test]
    fn test_from_option_none() {
        let val: QVal = Option::<i64>::None.into();
        assert!(matches!(val, QVal::Null));
    }

    #[test]
    fn test_query_builder_bind() {
        let q = Query::new("SELECT * FROM headers WHERE height = ? AND hash = ?")
            .bind(100u32)
            .bind("abc123");
        assert_eq!(q.sql, "SELECT * FROM headers WHERE height = ? AND hash = ?");
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn test_query_builder_no_params() {
        let q = Query::new("SELECT COUNT(*) FROM headers");
        assert_eq!(q.params.len(), 0);
    }

    #[test]
    fn test_query_builder_many_params() {
        let q = Query::new("INSERT INTO headers VALUES (?, ?, ?, ?, ?)")
            .bind(1u32)
            .bind("hash")
            .bind(true)
            .bind(2.5f64)
            .bind(Option::<i64>::None);
        assert_eq!(q.params.len(), 5);
    }
}
