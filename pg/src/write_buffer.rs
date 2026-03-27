//! Per-table write buffer for coalescing single-row INSERTs into batch gRPC calls.
//!
//! PostgreSQL FDW `ExecForeignInsert` is called per-row for individual INSERT statements.
//! Without buffering, each row incurs a full gRPC round-trip (~1-2ms). This module
//! accumulates rows in-process and flushes them in batches, reducing the number of
//! network round-trips by up to `FLUSH_THRESHOLD`×.
//!
//! ## Safety
//! PostgreSQL backends are single-threaded, so the global buffer requires no
//! synchronization beyond a `RefCell`-style guard. We use `Mutex` for Rust's
//! Send/Sync requirements but contention is impossible.
//!
//! ## Flush triggers
//! - Buffer reaches `FLUSH_THRESHOLD` rows → immediate flush
//! - `EndForeignModify` → flush remaining rows for this table
//! - `PRE_COMMIT` xact callback → flush ALL pending writes before commit
//! - `BeginForeignScan` on same table → flush for read-your-writes consistency

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kalam_pg_api::{InsertRequest, KalamBackendExecutor, TenantContext};
use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::{TableId, TableType};

/// Maximum rows to buffer before auto-flushing.
const FLUSH_THRESHOLD: usize = 256;

/// A pending batch of rows for a specific table + user context.
struct PendingBatch {
    table_id: TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    rows: Vec<Row>,
    executor: Arc<dyn KalamBackendExecutor>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl PendingBatch {
    /// Flush all buffered rows in a single gRPC batch call.
    fn flush(&mut self) -> Result<(), KalamPgError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let request = InsertRequest::new(
            self.table_id.clone(),
            self.table_type,
            TenantContext::new(None, self.user_id.clone()),
            rows,
        );
        self.runtime
            .block_on(async { self.executor.insert(request).await })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Global write buffer keyed by table full name.
///
/// This is a per-process singleton. PG backends are single-threaded,
/// so the Mutex is uncontended.
static WRITE_BUFFER: Mutex<Option<HashMap<String, PendingBatch>>> = Mutex::new(None);

/// Initialize the write buffer (idempotent).
fn ensure_buffer() {
    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
}

/// Buffer a row for later batch insertion. Automatically flushes when the
/// buffer reaches `FLUSH_THRESHOLD`.
pub fn buffer_insert(
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    row: Row,
    executor: &Arc<dyn KalamBackendExecutor>,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<(), KalamPgError> {
    ensure_buffer();
    let key = table_id.full_name();
    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.as_mut().unwrap();

    let batch = map.entry(key).or_insert_with(|| PendingBatch {
        table_id: table_id.clone(),
        table_type,
        user_id: user_id.clone(),
        rows: Vec::with_capacity(FLUSH_THRESHOLD),
        executor: Arc::clone(executor),
        runtime: Arc::clone(runtime),
    });

    batch.rows.push(row);

    if batch.len() >= FLUSH_THRESHOLD {
        batch.flush()?;
    }

    Ok(())
}

/// Flush buffered writes for a specific table. Called from `EndForeignModify`.
pub fn flush_table(table_id: &TableId) -> Result<(), KalamPgError> {
    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(map) = guard.as_mut() else {
        return Ok(());
    };
    let key = table_id.full_name();
    if let Some(batch) = map.get_mut(&key) {
        batch.flush()?;
    }
    Ok(())
}

/// Flush ALL buffered writes across all tables. Called from xact PRE_COMMIT.
pub fn flush_all() -> Result<(), KalamPgError> {
    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(map) = guard.as_mut() else {
        return Ok(());
    };
    for batch in map.values_mut() {
        batch.flush()?;
    }
    // Clear all entries after flush
    map.clear();
    Ok(())
}

/// Discard all buffered writes (called on ABORT).
pub fn discard_all() {
    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(map) = guard.as_mut() {
        map.clear();
    }
}


