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
//! - Buffer reaches `FLUSH_THRESHOLD` rows in autocommit mode → immediate flush
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

/// Fast check: returns true if the global write buffer has any entries at all.
/// Uses a try_lock to avoid blocking — if the lock is contended, conservatively
/// returns true so the caller proceeds with the full flush path.
pub fn has_any_pending_writes() -> bool {
    match WRITE_BUFFER.try_lock() {
        Ok(guard) => guard.as_ref().is_some_and(|map| !map.is_empty()),
        Err(_) => true, // conservatively assume writes pending
    }
}

/// A pending batch of rows for a specific table + user context.
struct PendingBatch {
    session_id: String,
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
        self.runtime.block_on(async { self.executor.insert(request).await })?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

fn pending_batch_key(
    session_id: &str,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
) -> String {
    let user_scope = user_id.map(UserId::as_str).unwrap_or("_");
    format!("{}|{}|{}|{}", session_id, table_type.as_str(), table_id.full_name(), user_scope)
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
    session_id: &str,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    row: Row,
    executor: &Arc<dyn KalamBackendExecutor>,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<(), KalamPgError> {
    ensure_buffer();
    let key = pending_batch_key(session_id, table_id, table_type, user_id.as_ref());
    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.as_mut().unwrap();

    let batch = map.entry(key).or_insert_with(|| PendingBatch {
        session_id: session_id.to_string(),
        table_id: table_id.clone(),
        table_type,
        user_id: user_id.clone(),
        rows: Vec::with_capacity(FLUSH_THRESHOLD),
        executor: Arc::clone(executor),
        runtime: Arc::clone(runtime),
    });

    batch.rows.push(row);

    if batch.len() >= FLUSH_THRESHOLD
        && !crate::fdw_xact::is_explicit_transaction_block_active()
        && !crate::fdw_xact::has_active_transaction()
    {
        batch.flush()?;
    }

    Ok(())
}

/// Flush buffered writes for a specific table. Called from `EndForeignModify`.
pub fn flush_table(
    session_id: &str,
    table_id: &TableId,
    table_type: TableType,
) -> Result<(), KalamPgError> {
    // Fast path: skip mutex + iteration when no writes are buffered at all
    if !has_any_pending_writes() {
        return Ok(());
    }

    let mut guard = WRITE_BUFFER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(map) = guard.as_mut() else {
        return Ok(());
    };

    let table_full_name = table_id.full_name();
    let matching_keys = map
        .iter()
        .filter(|(_, batch)| {
            batch.session_id == session_id
                && batch.table_type.as_str() == table_type.as_str()
                && batch.table_id.full_name() == table_full_name
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();

    for key in matching_keys {
        if let Some(batch) = map.get_mut(&key) {
            batch.flush()?;
        }
    }

    map.retain(|_, batch| !batch.rows.is_empty());
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::LazyLock;

    use async_trait::async_trait;
    use datafusion_common::ScalarValue;
    use kalam_pg_api::{DeleteRequest, MutationResponse, ScanRequest, ScanResponse, UpdateRequest};
    use kalamdb_commons::models::{NamespaceId, TableName};

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[derive(Default)]
    struct RecordingExecutor {
        inserts: Mutex<Vec<InsertRequest>>,
    }

    impl RecordingExecutor {
        fn take_inserts(&self) -> Vec<InsertRequest> {
            std::mem::take(&mut *self.inserts.lock().unwrap_or_else(|e| e.into_inner()))
        }
    }

    #[async_trait]
    impl KalamBackendExecutor for RecordingExecutor {
        async fn scan(&self, _request: ScanRequest) -> Result<ScanResponse, KalamPgError> {
            Ok(ScanResponse::new(Vec::new()))
        }

        async fn insert(&self, request: InsertRequest) -> Result<MutationResponse, KalamPgError> {
            self.inserts.lock().unwrap_or_else(|e| e.into_inner()).push(request.clone());
            Ok(MutationResponse {
                affected_rows: request.rows.len() as u64,
            })
        }

        async fn update(&self, _request: UpdateRequest) -> Result<MutationResponse, KalamPgError> {
            Ok(MutationResponse { affected_rows: 0 })
        }

        async fn delete(&self, _request: DeleteRequest) -> Result<MutationResponse, KalamPgError> {
            Ok(MutationResponse { affected_rows: 0 })
        }
    }

    fn test_runtime() -> Arc<tokio::runtime::Runtime> {
        Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime"),
        )
    }

    fn test_table_id() -> TableId {
        TableId::new(NamespaceId::new("app"), TableName::new("profiles"))
    }

    fn test_row(id: &str) -> Row {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Utf8(Some(id.to_string())));
        Row::new(values)
    }

    #[test]
    fn buffer_insert_separates_batches_by_user_scope() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        discard_all();

        let table_id = test_table_id();
        let executor = Arc::new(RecordingExecutor::default());
        let executor_dyn: Arc<dyn KalamBackendExecutor> = executor.clone();
        let runtime = test_runtime();

        buffer_insert(
            "sess-a",
            &table_id,
            TableType::User,
            Some(UserId::new("user-a")),
            test_row("a1"),
            &executor_dyn,
            &runtime,
        )
        .expect("buffer user-a row");
        buffer_insert(
            "sess-a",
            &table_id,
            TableType::User,
            Some(UserId::new("user-b")),
            test_row("b1"),
            &executor_dyn,
            &runtime,
        )
        .expect("buffer user-b row");

        flush_all().expect("flush all batches");

        let mut inserts = executor.take_inserts();
        inserts.sort_by(|left, right| {
            left.tenant_context
                .effective_user_id()
                .map(UserId::as_str)
                .cmp(&right.tenant_context.effective_user_id().map(UserId::as_str))
        });

        assert_eq!(inserts.len(), 2);
        assert_eq!(
            inserts[0].tenant_context.effective_user_id().map(UserId::as_str),
            Some("user-a")
        );
        assert_eq!(
            inserts[1].tenant_context.effective_user_id().map(UserId::as_str),
            Some("user-b")
        );
        assert_eq!(inserts[0].rows.len(), 1);
        assert_eq!(inserts[1].rows.len(), 1);

        discard_all();
    }

    #[test]
    fn flush_table_only_flushes_matching_session_scope() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        discard_all();

        let table_id = test_table_id();
        let executor = Arc::new(RecordingExecutor::default());
        let executor_dyn: Arc<dyn KalamBackendExecutor> = executor.clone();
        let runtime = test_runtime();

        buffer_insert(
            "sess-a",
            &table_id,
            TableType::Shared,
            None,
            test_row("a1"),
            &executor_dyn,
            &runtime,
        )
        .expect("buffer session-a row");
        buffer_insert(
            "sess-b",
            &table_id,
            TableType::Shared,
            None,
            test_row("b1"),
            &executor_dyn,
            &runtime,
        )
        .expect("buffer session-b row");

        flush_table("sess-a", &table_id, TableType::Shared).expect("flush matching table scope");

        let inserts = executor.take_inserts();
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].rows.len(), 1);

        flush_all().expect("flush remaining batches");

        let remaining = executor.take_inserts();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].rows.len(), 1);

        discard_all();
    }
}
