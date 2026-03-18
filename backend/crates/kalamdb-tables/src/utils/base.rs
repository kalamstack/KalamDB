//! Base trait for table providers with unified DML operations
//!
//! This module provides:
//! - BaseTableProvider<K, V> trait for generic table operations
//! - TableProviderCore shared structure for common services
//!
//! **Design Rationale**:
//! - Eliminates ~1200 lines of duplicate code across User/Shared/Stream providers
//! - Generic over storage key (K) and value (V) types
//! - No separate handlers - DML logic implemented directly in providers
//! - Shared core reduces memory overhead (Arc<TableProviderCore> vs per-provider fields)
//!
//! ## Streaming vs MVCC Constraints
//!
//! **Why full iterator-based streaming is NOT possible for User/Shared tables:**
//!
//! User and Shared tables use MVCC (Multi-Version Concurrency Control) with version
//! resolution. This means:
//!
//! 1. Multiple versions of the same row may exist (each INSERT/UPDATE creates a new _seq)
//! 2. To return the "current" row, we must find MAX(_seq) per primary key
//! 3. Tombstones (_deleted = true) must hide older versions
//!
//! This inherently requires seeing ALL rows before returning ANY results, making
//! true streaming impossible. The flow is:
//!
//! ```text
//! Hot Storage (RocksDB) ─┐
//!                        ├──> Merge ──> Version Resolution ──> Filter Deleted ──> Result
//! Cold Storage (Parquet) ┘       (requires ALL rows to find MAX(_seq) per PK)
//! ```
//!
//! **Stream tables ARE streamable** because they:
//! - Are append-only (no updates, no version resolution needed)
//! - Use TTL-based eviction instead of tombstones
//! - Can return rows as they're scanned with early termination on LIMIT
//!
//! ## Architecture
//!
//! ```text
//! TableProvider::scan()
//!        │
//!        ▼
//! base_scan() ── combines filters, calls scan_rows()
//!        │
//!        ▼
//! scan_rows() ── extracts user context, calls scan_with_version_resolution_to_kvs()
//!        │
//!        ▼
//! scan_with_version_resolution_to_kvs() ── provider-specific implementation:
//!   • User: user-scoped RocksDB prefix + Parquet, MVCC merge
//!   • Shared: full RocksDB + Parquet, MVCC merge
//!   • Stream: user-scoped RocksDB only, TTL filter, streamable
//! ```

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;

use crate::manifest::ManifestAccessPlanner;
use crate::utils::unified_dml;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{utils::expr_to_columns, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::arrow_json_conversion::coerce_rows;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{NamespaceId, TableName, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::{StorageKey, TableId};
use kalamdb_filestore::registry::ListResult;
use kalamdb_system::ClusterCoordinator as ClusterCoordinatorTrait;
use kalamdb_system::Manifest;
use kalamdb_system::SchemaRegistry as SchemaRegistryTrait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Re-export types moved to submodules
pub use crate::utils::core::TableProviderCore;
pub(crate) use crate::utils::parquet::scan_parquet_files_as_batch_async;
pub use crate::utils::row_utils::{
    extract_full_user_context, extract_seq_bounds_from_filter, resolve_user_scope, system_user_id,
};
pub use crate::utils::row_utils::{inject_system_columns, rows_to_arrow_batch, ScanRow};

/// Unified trait for all table providers with generic storage abstraction
///
/// **Key Design Decisions**:
/// - Generic K: StorageKey (UserTableRowId, SharedTableRowId, StreamTableRowId)
/// - Generic V: Row type (UserTableRow, SharedTableRow, StreamTableRow)
/// - Extends DataFusion::TableProvider (same struct serves both custom DML + SQL)
/// - No separate handlers - all DML logic in provider implementations
/// - Stateless providers - user_id passed per-operation, not stored per-user
///
/// **Architecture**:
/// ```text
/// ExecutionContext → SessionState.extensions (SessionUserContext)
///                 ↓
/// Provider.scan_rows(state) → extract_user_context(state)
///                           ↓
/// Provider.scan_with_version_resolution_to_kvs(user_id, filter)
/// ```
#[async_trait]
pub trait BaseTableProvider<K: StorageKey, V>: Send + Sync + TableProvider {
    // ===========================
    // Core Access (required)
    // ===========================

    /// Get the TableProviderCore for low-level access (storage, manifest, etc.)
    /// All other metadata accessors have default implementations that delegate here.
    fn core(&self) -> &TableProviderCore;

    // ===========================
    // Core Metadata (default implementations via core())
    // ===========================

    /// Table identifier (namespace + table name)
    fn table_id(&self) -> &TableId {
        self.core().table_id()
    }

    /// Memoized Arrow schema (Phase 10 optimization: 50-100× faster than recomputation)
    fn schema_ref(&self) -> SchemaRef {
        self.core().schema_ref()
    }

    /// Logical table type (User, Shared, Stream)
    ///
    /// Named differently from DataFusion's TableProvider::table_type to avoid ambiguity.
    fn provider_table_type(&self) -> TableType {
        self.core().table_type()
    }

    /// Cluster coordinator for leader checks (read routing).
    fn cluster_coordinator(&self) -> &Arc<dyn ClusterCoordinatorTrait> {
        self.core().cluster_coordinator()
    }

    /// Access to schema registry for table metadata
    fn schema_registry(&self) -> &Arc<dyn SchemaRegistryTrait<Error = KalamDbError>> {
        self.core().schema_registry()
    }

    /// Primary key field name from schema definition (e.g., "id", "email")
    fn primary_key_field_name(&self) -> &str {
        self.core().primary_key_field_name()
    }

    /// Get namespace ID from table_id (default implementation)
    fn namespace_id(&self) -> &NamespaceId {
        self.table_id().namespace_id()
    }

    /// Get table name from table_id (default implementation)
    fn table_name(&self) -> &TableName {
        self.table_id().table_name()
    }

    /// Get RocksDB column family name (default implementation)
    fn column_family_name(&self) -> String {
        format!(
            "{}:{}",
            match <Self as BaseTableProvider<K, V>>::provider_table_type(self) {
                TableType::User => "user_table",
                TableType::Shared => "shared_table",
                TableType::Stream => "stream_table",
                _ => "table",
            },
            self.table_id() // TableId Display: "namespace:table"
        )
    }

    // ===========================
    // Row Construction (required per provider)
    // ===========================

    /// Construct (K, V) from ParquetRowData for cold storage lookups.
    /// Providers should override this to create their specific key and value types.
    fn construct_row_from_parquet_data(
        &self,
        user_id: &UserId,
        row_data: &crate::utils::version_resolution::ParquetRowData,
    ) -> Result<Option<(K, V)>, KalamDbError>;

    // ===========================
    // DML Operations (Synchronous - No Handlers)
    // ===========================

    /// Insert a single row (auto-generates system columns: _seq, _deleted)
    ///
    /// **Implementation**: Calls unified_dml helpers directly
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS (User/Stream use it, Shared ignores it)
    /// * `row_data` - Row containing user-defined columns
    ///
    /// # Returns
    /// Generated storage key (UserTableRowId, SharedTableRowId, or StreamTableRowId)
    ///
    /// # Architecture Note
    /// Providers are stateless. The user_id is passed per-operation by the SQL executor
    /// from ExecutionContext, enabling:
    /// - AS USER impersonation (executor passes subject_user_id)
    /// - Per-request user scoping without per-user provider instances
    /// - Clean separation: executor handles auth/context, provider handles storage
    async fn insert(&self, user_id: &UserId, row_data: Row) -> Result<K, KalamDbError>;

    /// Insert multiple rows in a batch (optimized for bulk operations)
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `rows` - Vector of Row objects
    ///
    /// # Default Implementation
    /// Iterates over rows and calls insert() for each. Providers may override
    /// with batch-optimized implementation.
    async fn insert_batch(&self, user_id: &UserId, rows: Vec<Row>) -> Result<Vec<K>, KalamDbError> {
        // Coerce rows to match schema types (e.g. String -> Timestamp)
        // This ensures real-time events match the storage format
        let coerced_rows = coerce_rows(rows, &self.schema_ref()).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
        })?;

        let mut results = Vec::with_capacity(coerced_rows.len());
        for row in coerced_rows {
            results.push(self.insert(user_id, row).await?);
        }
        Ok(results)
    }

    /// Update a row by key (appends new version with incremented _seq)
    ///
    /// **Implementation**: Uses version_resolution helpers + unified_dml
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `key` - Storage key identifying the row
    /// * `updates` - Row object with column updates
    ///
    /// # Returns
    /// New storage key (new SeqId for versioning)
    async fn update(&self, user_id: &UserId, key: &K, updates: Row) -> Result<K, KalamDbError>;

    /// Delete a row by key (appends tombstone with _deleted=true)
    ///
    /// **Implementation**: Uses version_resolution helpers + unified_dml
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `key` - Storage key identifying the row
    async fn delete(&self, user_id: &UserId, key: &K) -> Result<(), KalamDbError>;

    /// Update multiple rows in a batch (default implementation)
    async fn update_batch(
        &self,
        user_id: &UserId,
        updates: Vec<(K, Row)>,
    ) -> Result<Vec<K>, KalamDbError> {
        let mut results = Vec::with_capacity(updates.len());
        for (key, update) in updates {
            results.push(BaseTableProvider::update(self, user_id, &key, update).await?);
        }
        Ok(results)
    }

    /// Delete multiple rows in a batch (default implementation)
    async fn delete_batch(&self, user_id: &UserId, keys: Vec<K>) -> Result<Vec<()>, KalamDbError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.delete(user_id, &key).await?);
        }
        Ok(results)
    }

    // ===========================
    // Convenience Methods (with default implementations)
    // ===========================

    /// Find row key by ID field value
    ///
    /// Scans rows with version resolution and returns the key of the first row
    /// where `fields.id == id_value`. The returned key K already contains user_id
    /// for user/stream tables (embedded in UserTableRowId/StreamTableRowId).
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS scoping
    /// * `id_value` - Value to search for in the ID field
    ///
    /// # Performance
    /// - User tables: Override uses PK index for O(1) lookup
    /// - Shared tables: Override uses PK index for O(1) lookup
    /// - Stream tables: Uses default implementation (full scan)
    ///
    /// # Note
    /// Providers with PK indexes should override this method for efficient lookups.
    /// Uses async I/O for cold storage access.
    async fn find_row_key_by_id_field(
        &self,
        user_id: &UserId,
        id_value: &str,
    ) -> Result<Option<K>, KalamDbError>;

    /// Update a row by primary key value directly (no key lookup needed)
    ///
    /// This is more efficient than `update()` because it doesn't need to load
    /// the prior row just to extract the PK value - we already have it.
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `pk_value` - Primary key value (e.g., "user123")
    /// * `updates` - Row object with column updates
    ///
    /// # Returns
    /// New storage key (new SeqId for versioning)
    async fn update_by_pk_value(
        &self,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<K, KalamDbError>;

    /// Update a row by searching for matching ID field value
    async fn update_by_id_field(
        &self,
        user_id: &UserId,
        id_value: &str,
        updates: Row,
    ) -> Result<K, KalamDbError> {
        // Directly update by PK value - no need to find key first, then load row to extract PK
        self.update_by_pk_value(user_id, id_value, updates).await
    }

    /// Delete a row by primary key value directly (no key lookup needed)
    ///
    /// This is more efficient than `delete()` and works for both hot and cold storage.
    /// It finds the row by PK value (using find_row_by_pk for cold storage),
    /// then writes a tombstone.
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `pk_value` - Primary key value (e.g., "user123")
    ///
    /// # Returns
    /// `Ok(true)` if row was deleted, `Ok(false)` if row was not found
    async fn delete_by_pk_value(
        &self,
        user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError>;

    /// Delete a row by searching for matching ID field value.
    ///
    /// Returns `true` if a row was deleted, `false` if the row did not exist.
    async fn delete_by_id_field(
        &self,
        user_id: &UserId,
        id_value: &str,
    ) -> Result<bool, KalamDbError> {
        // Directly delete by PK value - handles both hot and cold storage
        self.delete_by_pk_value(user_id, id_value).await
    }

    // ===========================
    // DataFusion TableProvider Default Implementations
    // ===========================

    /// Default implementation for supports_filters_pushdown
    fn base_supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // We support Inexact pushdown for all filters because:
        // 1. We use them for partition pruning (Parquet)
        // 2. We use them for prefix scan / range scan (RocksDB)
        // But we still need DataFusion to apply the filter afterwards to be safe/exact.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    /// Default implementation for statistics
    fn base_statistics(&self) -> Option<Statistics> {
        // TODO: Implement row count estimation from Manifest + RocksDB stats
        None
    }

    /// Default implementation for scan
    async fn base_scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.ensure_leader_read(state)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Combine filters (AND) for pruning and pass to scan_rows
        let combined_filter: Option<Expr> = if filters.is_empty() {
            None
        } else {
            let first = filters[0].clone();
            Some(filters[1..].iter().cloned().fold(first, |acc, e| acc.and(e)))
        };

        // Optimization: Pass projection to scan_rows ONLY if filters is empty.
        // If filters exist, we need all columns involved in the filter.
        // Since we return Inexact for pushdown, DataFusion adds a FilterExec after the Scan.
        // So the Scan must return columns needed for the filter.
        // Safest approach: If filters are present, fetch all columns.
        let effective_projection = if filters.is_empty() { projection } else { None };

        let batch = self
            .scan_rows(state, effective_projection, combined_filter.as_ref(), limit)
            .await
            .map_err(|e| DataFusionError::Execution(format!("scan_rows failed: {}", e)))?;

        let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

        // If filters are empty, batch is already projected, so we scan all columns of MemTable.
        // If filters are present, batch has all columns, so we apply projection in MemTable scan.
        let final_projection = if filters.is_empty() { None } else { projection };

        mem.scan(state, final_projection, filters, limit).await
    }

    /// Enforce leader-only reads for client contexts in cluster mode.
    async fn ensure_leader_read(&self, state: &dyn Session) -> Result<(), KalamDbError> {
        let (_user_id, _role, read_context) = extract_full_user_context(state)?;
        if !read_context.requires_leader() {
            return Ok(());
        }

        let coordinator = self.cluster_coordinator();
        if !coordinator.is_cluster_mode().await {
            return Ok(());
        }

        match self.provider_table_type() {
            TableType::User | TableType::Stream => {
                let (user_id, _role, _read_context) = extract_full_user_context(state)?;
                if !coordinator.is_leader_for_user(user_id).await {
                    let leader_addr = coordinator.leader_addr_for_user(user_id).await;
                    return Err(KalamDbError::NotLeader { leader_addr });
                }
            },
            TableType::Shared => {
                if !coordinator.is_leader_for_shared().await {
                    let leader_addr = coordinator.leader_addr_for_shared().await;
                    return Err(KalamDbError::NotLeader { leader_addr });
                }
            },
            TableType::System => {},
        }

        Ok(())
    }

    // ===========================
    // Scan Operations (with version resolution)
    // ===========================

    /// Scan rows with optional filter (merges hot + cold storage with version resolution)
    ///
    /// **Called by DataFusion during query execution via TableProvider::scan()**
    ///
    /// The `state` parameter contains SessionUserContext in extensions,
    /// which providers extract to apply RLS filtering.
    ///
    /// **User/Shared Tables**:
    /// 1. Extract user_id from SessionState.config().options().extensions
    /// 2. Scan RocksDB (hot storage)
    /// 3. Scan Parquet files (cold storage)
    /// 4. Apply version resolution (MAX(_seq) per primary key) via DataFusion
    /// 5. Filter _deleted = false
    /// 6. Apply user filter expression
    /// 7. For User tables: Apply RLS (user_id = subject)
    ///
    /// **Stream Tables**:
    /// 1. Extract user_id from SessionState
    /// 2. Scan ONLY RocksDB (hot storage, no Parquet)
    /// 3. Apply TTL filtering
    /// 4. Filter _deleted = false (if applicable)
    /// 5. Apply user filter expression
    /// 6. Apply RLS (user_id = subject)
    ///
    /// # Arguments
    /// * `state` - DataFusion SessionState (contains SessionUserContext)
    /// * `projection` - Optional column projection
    /// * `filter` - Optional DataFusion expression for filtering
    /// * `limit` - Optional limit on number of rows
    ///
    /// # Returns
    /// RecordBatch with resolved, filtered rows
    ///
    /// # Note
    /// Called by DataFusion's TableProvider::scan(). For direct DML operations,
    /// use scan_with_version_resolution_to_kvs_async().
    async fn scan_rows(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, KalamDbError>;

    /// Async scan with version resolution returning key-value pairs (for internal DML use)
    ///
    /// Used by UPDATE/DELETE to find current version before appending new version.
    /// Unlike scan_rows(), this is called directly by DML operations with user_id
    /// passed explicitly.
    ///
    /// Uses `spawn_blocking` internally to prevent blocking the async runtime.
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS scoping
    /// * `filter` - Optional DataFusion expression for filtering
    /// * `since_seq` - Optional sequence number to start scanning from (optimization)
    /// * `limit` - Optional limit on number of rows
    /// * `keep_deleted` - Whether to include soft-deleted rows (tombstones) in the result
    async fn scan_with_version_resolution_to_kvs_async(
        &self,
        user_id: &UserId,
        filter: Option<&Expr>,
        since_seq: Option<SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
    ) -> Result<Vec<(K, V)>, KalamDbError>;

    /// Extract row fields from provider-specific value type
    ///
    /// Each provider implements this to access the internal `Row` stored on their row type.
    fn extract_row(row: &V) -> &Row;
}

/// Check if a filter expression references the _deleted column
pub fn filter_uses_deleted_column(filter: &Expr) -> bool {
    let mut columns = HashSet::new();
    if expr_to_columns(filter, &mut columns).is_ok() {
        columns.iter().any(|c| c.name == SystemColumnNames::DELETED)
    } else {
        false
    }
}

/// Extract a PK equality literal from a simple `pk_col = literal` filter.
///
/// Supports both `col = literal` and `literal = col` forms, including
/// AND-conjunctions where the PK equality is one term.
/// Returns `Some(ScalarValue)` if a PK equality is found, `None` otherwise.
pub fn extract_pk_equality_literal(filter: &Expr, pk_name: &str) -> Option<ScalarValue> {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
            // col = literal
            if let (Expr::Column(col), Expr::Literal(val, _)) =
                (binary.left.as_ref(), binary.right.as_ref())
            {
                if col.name.eq_ignore_ascii_case(pk_name) {
                    return Some(val.clone());
                }
            }
            // literal = col
            if let (Expr::Literal(val, _), Expr::Column(col)) =
                (binary.left.as_ref(), binary.right.as_ref())
            {
                if col.name.eq_ignore_ascii_case(pk_name) {
                    return Some(val.clone());
                }
            }
            None
        },
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            // Recursively check AND branches
            extract_pk_equality_literal(&binary.left, pk_name)
                .or_else(|| extract_pk_equality_literal(&binary.right, pk_name))
        },
        _ => None,
    }
}

/// Locate the latest non-deleted row matching the provided primary-key value (async).
///
/// This function scans cold storage (Parquet files) to find a row by its primary key.
/// For UPDATE/DELETE operations on cold storage data, this is needed to:
/// 1. Get the current row data to merge with updates
/// 2. Verify the row exists before creating a tombstone (delete)
///
/// Uses async I/O to avoid blocking the tokio runtime.
/// For hot storage lookups, providers should use their own O(1) PK index first.
pub async fn find_row_by_pk<P, K, V>(
    provider: &P,
    scope: Option<&UserId>,
    pk_value: &str,
) -> Result<Option<(K, V)>, KalamDbError>
where
    P: BaseTableProvider<K, V>,
    K: StorageKey,
{
    use crate::utils::version_resolution::{parquet_batch_to_rows, ParquetRowData};
    use datafusion::prelude::{col, lit};

    let pk_name = provider.primary_key_field_name();
    let user_scope = resolve_user_scope(scope);

    // Build filter for the specific PK value
    let filter: Expr = col(pk_name).eq(lit(pk_value));

    // Get core from provider (we need schema, table_id, table_type, and storage access)
    let core = provider.core();
    let table_id = provider.table_id();
    let table_type = provider.provider_table_type();
    let schema = provider.schema_ref();

    // Scan cold storage for this PK value using async I/O
    let batch =
        scan_parquet_files_as_batch_async(core, table_id, table_type, scope, schema, Some(&filter))
            .await?;

    if batch.num_rows() == 0 {
        return Ok(None);
    }

    // Parse rows from Parquet batch
    let rows_data: Vec<ParquetRowData> = parquet_batch_to_rows(&batch)?;

    // Find the latest non-deleted version with matching PK
    // Rows should already be filtered by PK, but we need version resolution
    let mut latest: Option<ParquetRowData> = None;

    for row_data in rows_data {
        // Skip deleted rows
        if row_data.deleted {
            continue;
        }

        // Check if this row matches the PK value
        if let Some(row_pk) = row_data.fields.values.get(pk_name) {
            let row_pk_str = match row_pk {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                ScalarValue::Int64(Some(n)) => n.to_string(),
                ScalarValue::Int32(Some(n)) => n.to_string(),
                ScalarValue::Int16(Some(n)) => n.to_string(),
                ScalarValue::UInt64(Some(n)) => n.to_string(),
                ScalarValue::UInt32(Some(n)) => n.to_string(),
                ScalarValue::Boolean(Some(b)) => b.to_string(),
                _ => continue,
            };

            if row_pk_str != pk_value {
                continue;
            }

            // Keep the row with highest _seq (latest version)
            if latest.as_ref().map(|l| row_data.seq_id > l.seq_id).unwrap_or(true) {
                latest = Some(row_data);
            }
        }
    }

    // Convert ParquetRowData to the provider's (K, V) types
    if let Some(row_data) = latest {
        let result = provider.construct_row_from_parquet_data(user_scope, &row_data)?;
        return Ok(result);
    }

    Ok(None)
}

/// Check if a PK value exists in cold storage (Parquet files) using manifest-based pruning (async).
///
/// **Optimized for PK existence checks during INSERT**:
/// 1. Load manifest from cache (no disk I/O if cached)
/// 2. Use column_stats min/max to prune segments that definitely don't contain the PK
/// 3. Only scan relevant Parquet files (if any)
/// 4. Scan with version resolution to handle MVCC (latest non-deleted wins)
///
/// This is much faster than `find_row_by_pk` which scans ALL cold storage rows.
///
/// # Arguments
/// * `core` - TableProviderCore for app_context access
/// * `table_id` - Table identifier
/// * `table_type` - TableType (User, Shared, Stream)
/// * `user_id` - Optional user ID for scoping (User tables)
/// * `pk_column` - Name of the primary key column
/// * `pk_column_id` - Column ID of the primary key column (for manifest column_stats lookup)
/// * `pk_value` - The PK value to check for
///
/// # Returns
/// * `Ok(true)` - PK exists in cold storage (non-deleted)
/// * `Ok(false)` - PK does not exist in cold storage
pub async fn pk_exists_in_cold(
    core: &TableProviderCore,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    pk_column: &str,
    pk_column_id: u64,
    pk_value: &str,
) -> Result<bool, KalamDbError> {
    let namespace = table_id.namespace_id();
    let table = table_id.table_name();
    let scope_label = user_id
        .map(|uid| format!("user={}", uid.as_str()))
        .unwrap_or_else(|| format!("scope={}", table_type.as_str()));

    // 1. Get storage_id from schema registry
    let storage_id = match core.services.schema_registry.get_storage_id(table_id) {
        Ok(id) => id,
        Err(_) => {
            log::trace!(
                "[pk_exists_in_cold] No storage id for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(false);
        },
    };

    // 2. Get StorageCached from registry
    let storage_registry = core.services.storage_registry.as_ref().ok_or_else(|| {
        KalamDbError::InvalidOperation("Storage registry not configured".to_string())
    })?;
    let storage_cached = storage_registry.get_cached(&storage_id)?.ok_or_else(|| {
        KalamDbError::InvalidOperation(format!("Storage '{}' not found", storage_id.as_str()))
    })?;

    // 4. Load manifest from cache
    let manifest_service = core.services.manifest_service.clone();
    let cache_result = manifest_service.get_or_load(table_id, user_id);

    let manifest: Option<Manifest> = match &cache_result {
        Ok(Some(entry)) => Some(entry.manifest.clone()),
        Ok(None) => {
            log::trace!(
                "[pk_exists_in_cold] No manifest for {}.{} {} - checking all files",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            None
        },
        Err(e) => {
            log::warn!(
                "[pk_exists_in_cold] Manifest cache error for {}.{} {}: {}",
                namespace.as_str(),
                table.as_str(),
                scope_label,
                e
            );
            None
        },
    };

    // Fast path: manifest loaded and has no cold segments.
    // Avoid storage listing on hot-only write paths.
    if let Some(ref m) = manifest {
        if m.segments.is_empty() {
            log::trace!(
                "[pk_exists_in_cold] Manifest has no segments for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(false);
        }
    }

    // 5. Use manifest to prune segments or list all Parquet files
    let planner = ManifestAccessPlanner::new();
    let files_to_scan: Vec<String> = if let Some(ref m) = manifest {
        let pruned_paths = planner.plan_by_pk_value(m, pk_column_id, pk_value);
        if pruned_paths.is_empty() {
            log::trace!(
                "[pk_exists_in_cold] Manifest pruning returned no candidate segments for PK {} on {}.{} {} - PK not in cold",
                pk_value,
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(false);
        } else {
            log::trace!(
                "[pk_exists_in_cold] Manifest pruning: {} of {} segments may contain PK {} for {}.{} {}",
                pruned_paths.len(),
                m.segments.len(),
                pk_value,
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            pruned_paths
        }
    } else {
        // No manifest - use all Parquet files from listing
        let list_result = match storage_cached.list(table_type, table_id, user_id).await {
            Ok(result) => result,
            Err(_) => {
                log::trace!(
                    "[pk_exists_in_cold] No storage dir for {}.{} {} - PK not in cold",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(false);
            },
        };
        if list_result.is_empty() {
            log::trace!(
                "[pk_exists_in_cold] No files in storage for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(false);
        }
        collect_parquet_files_from_list(&list_result)
    };

    if files_to_scan.is_empty() {
        return Ok(false);
    }

    // 6. Scan pruned Parquet files and check for PK using StorageCached
    // Manifest paths are just filenames (e.g., "batch-0.parquet"), so prepend storage_path
    for file_name in files_to_scan {
        if pk_exists_in_parquet_via_storage_cache(
            &storage_cached,
            table_type,
            table_id,
            user_id,
            &file_name,
            pk_column,
            pk_value,
        )
        .await?
        {
            log::trace!(
                "[pk_exists_in_cold] Found PK {} in {} for {}.{} {}",
                pk_value,
                file_name,
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(true);
        }
    }

    Ok(false)
}

fn collect_parquet_files_from_list(list_result: &ListResult) -> Vec<String> {
    let prefix = list_result.prefix.trim_end_matches('/');
    list_result
        .paths
        .iter()
        .filter_map(|path| {
            let stripped = strip_list_prefix(path, prefix).unwrap_or(path);
            if stripped.ends_with(".parquet") {
                Some(stripped.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Batch check if any PK values exist in cold storage (Parquet files) (async).
///
/// **OPTIMIZED for batch INSERT**: Checks multiple PK values in a single pass through cold storage.
/// This is O(files) instead of O(files × N) where N is the number of PK values.
///
/// # Arguments
/// * `core` - TableProviderCore for app_context access
/// * `table_id` - Table identifier
/// * `table_type` - TableType (User, Shared, Stream)
/// * `user_id` - Optional user ID for scoping (User tables)
/// * `pk_column` - Name of the primary key column
/// * `pk_column_id` - Column ID of the primary key column (for manifest column_stats lookup)
/// * `pk_values` - The PK values to check for
///
/// # Returns
/// * `Ok(Some(pk))` - First PK that exists in cold storage (non-deleted)
/// * `Ok(None)` - None of the PKs exist in cold storage
pub async fn pk_exists_batch_in_cold(
    core: &TableProviderCore,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    pk_column: &str,
    pk_column_id: u64,
    pk_values: &[String],
) -> Result<Option<String>, KalamDbError> {
    if pk_values.is_empty() {
        return Ok(None);
    }

    let namespace = table_id.namespace_id();
    let table = table_id.table_name();
    let scope_label = user_id
        .map(|uid| format!("user={}", uid.as_str()))
        .unwrap_or_else(|| format!("scope={}", table_type.as_str()));

    // 1. Get storage_id from schema registry
    let storage_id = match core.services.schema_registry.get_storage_id(table_id) {
        Ok(id) => id,
        Err(_) => {
            log::trace!(
                "[pk_exists_batch_in_cold] No storage id for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        },
    };

    // 2. Get StorageCached from registry
    let storage_registry = core.services.storage_registry.as_ref().ok_or_else(|| {
        KalamDbError::InvalidOperation("Storage registry not configured".to_string())
    })?;
    let storage_cached = storage_registry.get_cached(&storage_id)?.ok_or_else(|| {
        KalamDbError::InvalidOperation(format!("Storage '{}' not found", storage_id.as_str()))
    })?;

    // 4. Load manifest from cache
    let manifest_service = core.services.manifest_service.clone();
    let cache_result = manifest_service.get_or_load(table_id, user_id);

    let manifest: Option<Manifest> = match &cache_result {
        Ok(Some(entry)) => Some(entry.manifest.clone()),
        Ok(None) => {
            log::trace!(
                "[pk_exists_batch_in_cold] No manifest for {}.{} {} - checking all files",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            None
        },
        Err(e) => {
            log::warn!(
                "[pk_exists_batch_in_cold] Manifest cache error for {}.{} {}: {}",
                namespace.as_str(),
                table.as_str(),
                scope_label,
                e
            );
            None
        },
    };

    // Fast path: manifest loaded and has no cold segments.
    // Avoid storage listing on hot-only write paths.
    if let Some(ref m) = manifest {
        if m.segments.is_empty() {
            log::trace!(
                "[pk_exists_batch_in_cold] Manifest has no segments for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        }
    }

    // 5. Determine files to scan - union of files that may contain any of the PK values
    let planner = ManifestAccessPlanner::new();
    let files_to_scan: Vec<String> = if let Some(ref m) = manifest {
        // Collect all potentially relevant files for any PK value
        let mut relevant_files: HashSet<String> = HashSet::new();
        for pk_value in pk_values {
            let pruned_paths = planner.plan_by_pk_value(m, pk_column_id, pk_value);
            relevant_files.extend(pruned_paths);
        }
        if relevant_files.is_empty() {
            log::trace!(
                "[pk_exists_batch_in_cold] Manifest pruning returned no candidate segments for {}.{} {} - PKs not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        } else {
            log::trace!(
                "[pk_exists_batch_in_cold] Manifest pruning: {} of {} segments may contain {} PKs for {}.{} {}",
                relevant_files.len(),
                m.segments.len(),
                pk_values.len(),
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            relevant_files.into_iter().collect()
        }
    } else {
        // No manifest - use all Parquet files from listing
        let list_result = match storage_cached.list(table_type, table_id, user_id).await {
            Ok(result) => result,
            Err(_) => {
                log::trace!(
                    "[pk_exists_batch_in_cold] No storage dir for {}.{} {} - PK not in cold",
                    namespace.as_str(),
                    table.as_str(),
                    scope_label
                );
                return Ok(None);
            },
        };
        if list_result.is_empty() {
            log::trace!(
                "[pk_exists_batch_in_cold] No files in storage for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        }
        collect_parquet_files_from_list(&list_result)
    };

    if files_to_scan.is_empty() {
        return Ok(None);
    }

    // 6. Create a HashSet for O(1) PK lookups
    let pk_set: HashSet<&str> = pk_values.iter().map(|s| s.as_str()).collect();

    // 7. Scan Parquet files and check for PKs (batch version)
    for file_name in files_to_scan {
        if let Some(found_pk) = pk_exists_batch_in_parquet_via_storage_cache(
            &storage_cached,
            table_type,
            table_id,
            user_id,
            &file_name,
            pk_column,
            &pk_set,
        )
        .await?
        {
            log::trace!(
                "[pk_exists_batch_in_cold] Found PK {} in {} for {}.{} {}",
                found_pk,
                file_name,
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(Some(found_pk));
        }
    }

    Ok(None)
}

/// Batch check if any PK values exist in a single Parquet file via StorageCached (async).
///
/// **Phase 13.7: Uses bloom filter fast-fail + column projection for optimal batch checking.**
/// Returns the first matching PK found (with non-deleted latest version).
async fn pk_exists_batch_in_parquet_via_storage_cache(
    storage_cached: &kalamdb_filestore::StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    parquet_filename: &str,
    pk_column: &str,
    pk_values: &std::collections::HashSet<&str>,
) -> Result<Option<String>, KalamDbError> {
    // Get file data once
    let result = storage_cached
        .get(table_type, table_id, user_id, parquet_filename)
        .await
        .into_kalamdb_error("Failed to read Parquet file")?;

    // Phase 1: Bloom filter fast-fail for each PK (O(metadata) checks)
    let mut maybe_present = Vec::new();
    for &pk in pk_values {
        let definitely_absent =
            kalamdb_filestore::bloom_filter_check_absent(result.data.clone(), pk_column, &pk)
                .unwrap_or(false);

        if !definitely_absent {
            maybe_present.push(pk);
        }
    }

    // Early exit if all PKs are definitely absent
    if maybe_present.is_empty() {
        return Ok(None);
    }

    // Phase 2: Read with column projection (only pk, _seq, _deleted)
    let columns_to_read = [
        pk_column,
        SystemColumnNames::SEQ,
        SystemColumnNames::DELETED,
    ];
    let batches = kalamdb_filestore::parse_parquet_projected(result.data, &columns_to_read)
        .into_kalamdb_error("Failed to parse projected Parquet")?;

    // Track latest version per PK value: pk_value -> (max_seq, is_deleted)
    let mut versions: HashMap<String, (i64, bool)> = HashMap::new();

    for batch in batches {
        let pk_idx = batch.schema().index_of(pk_column).ok();
        let seq_idx = batch.schema().index_of(SystemColumnNames::SEQ).ok();
        let deleted_idx = batch.schema().index_of(SystemColumnNames::DELETED).ok();

        let (Some(pk_i), Some(seq_i)) = (pk_idx, seq_idx) else {
            continue;
        };

        let pk_col = batch.column(pk_i);
        let seq_col = batch.column(seq_i);
        let deleted_col = deleted_idx.map(|i| batch.column(i));

        for row_idx in 0..batch.num_rows() {
            let row_pk = extract_pk_as_string(pk_col.as_ref(), row_idx);
            let Some(row_pk_str) = row_pk else { continue };

            // Only check rows matching target PKs (O(1) lookup)
            if !pk_values.contains(row_pk_str.as_str()) {
                continue;
            }

            let seq = if let Some(arr) = seq_col.as_any().downcast_ref::<Int64Array>() {
                arr.value(row_idx)
            } else if let Some(arr) = seq_col.as_any().downcast_ref::<UInt64Array>() {
                arr.value(row_idx) as i64
            } else {
                continue;
            };

            let deleted = if let Some(del_col) = &deleted_col {
                if let Some(arr) = del_col.as_any().downcast_ref::<BooleanArray>() {
                    arr.value(row_idx)
                } else {
                    false
                }
            } else {
                false
            };

            // Update version tracking (keep only latest _seq)
            versions
                .entry(row_pk_str)
                .and_modify(|(max_seq, del)| {
                    if seq > *max_seq {
                        *max_seq = seq;
                        *del = deleted;
                    }
                })
                .or_insert((seq, deleted));
        }
    }

    // Return first non-deleted PK found
    for (pk, (_, is_deleted)) in versions {
        if !is_deleted && pk_values.contains(pk.as_str()) {
            return Ok(Some(pk));
        }
    }

    Ok(None)
}

/// Check if a PK value exists in a single Parquet file via StorageCached (async, with MVCC version resolution).
///
/// **Phase 13.7: Uses bloom filter-based reading for optimal performance.**
/// Fast-fails if bloom filter proves absence (O(metadata)), then projects only pk/_seq/_deleted columns.
async fn pk_exists_in_parquet_via_storage_cache(
    storage_cached: &kalamdb_filestore::StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    parquet_filename: &str,
    pk_column: &str,
    pk_value: &str,
) -> Result<bool, KalamDbError> {
    // Step 1: Get file data
    let result = storage_cached
        .get(table_type, table_id, user_id, parquet_filename)
        .await
        .into_kalamdb_error("Failed to read Parquet file")?;

    // Step 2: Bloom filter fast-fail (O(metadata))
    let definitely_absent =
        kalamdb_filestore::bloom_filter_check_absent(result.data.clone(), pk_column, &pk_value)
            .unwrap_or(false);

    if definitely_absent {
        return Ok(false);
    }

    // Step 3: Read with column projection (only pk, _seq, _deleted)
    let columns_to_read = [
        pk_column,
        SystemColumnNames::SEQ,
        SystemColumnNames::DELETED,
    ];
    let batches = kalamdb_filestore::parse_parquet_projected(result.data, &columns_to_read)
        .into_kalamdb_error("Failed to parse projected Parquet")?;

    // Track latest version: pk_value -> (max_seq, is_deleted)
    let mut versions: HashMap<String, (i64, bool)> = HashMap::new();

    for batch in batches {
        let pk_idx = batch.schema().index_of(pk_column).ok();
        let seq_idx = batch.schema().index_of(SystemColumnNames::SEQ).ok();
        let deleted_idx = batch.schema().index_of(SystemColumnNames::DELETED).ok();

        let (Some(pk_i), Some(seq_i)) = (pk_idx, seq_idx) else {
            continue;
        };

        let pk_col = batch.column(pk_i);
        let seq_col = batch.column(seq_i);
        let deleted_col = deleted_idx.map(|i| batch.column(i));

        for row_idx in 0..batch.num_rows() {
            let row_pk = extract_pk_as_string(pk_col.as_ref(), row_idx);
            let Some(row_pk_str) = row_pk else { continue };

            if row_pk_str != pk_value {
                continue;
            }

            let seq = if let Some(arr) = seq_col.as_any().downcast_ref::<Int64Array>() {
                arr.value(row_idx)
            } else if let Some(arr) = seq_col.as_any().downcast_ref::<UInt64Array>() {
                arr.value(row_idx) as i64
            } else {
                continue;
            };

            let deleted = if let Some(del_col) = &deleted_col {
                if let Some(arr) = del_col.as_any().downcast_ref::<BooleanArray>() {
                    arr.value(row_idx)
                } else {
                    false
                }
            } else {
                false
            };

            // Update version tracking (keep only latest _seq)
            versions
                .entry(row_pk_str)
                .and_modify(|(max_seq, del)| {
                    if seq > *max_seq {
                        *max_seq = seq;
                        *del = deleted;
                    }
                })
                .or_insert((seq, deleted));
        }
    }

    // Return true if latest version is not deleted
    Ok(versions.get(pk_value).map(|(_, is_deleted)| !*is_deleted).unwrap_or(false))
}

/// Extract PK value as string from an Arrow array at given index (delegates to shared utility).
fn extract_pk_as_string(col: &dyn Array, idx: usize) -> Option<String> {
    crate::utils::pk_utils::extract_pk_as_string(col, idx)
}

fn strip_list_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let trimmed_prefix = prefix.trim_end_matches('/');
    if trimmed_prefix.is_empty() {
        return Some(path.trim_start_matches('/'));
    }
    if path == trimmed_prefix {
        return None;
    }
    path.strip_prefix(trimmed_prefix)
        .map(|stripped| stripped.trim_start_matches('/'))
}

/// Ensure an INSERT payload either auto-generates or provides a unique primary-key value
///
/// This uses find_row_key_by_id_field which providers can override to use PK indexes
/// for O(1) lookup instead of scanning all rows.
///
/// **Optimization**: If the PK column is AUTO_INCREMENT or SNOWFLAKE_ID, this check
/// is skipped since the system guarantees unique values.
///
/// **Cold Storage Check**: After checking hot storage (RocksDB), this also checks
/// cold storage (Parquet files) using PkExistenceChecker for full PK uniqueness validation.
pub async fn ensure_unique_pk_value<P, K, V>(
    provider: &P,
    scope: Option<&UserId>,
    row_data: &Row,
) -> Result<(), KalamDbError>
where
    P: BaseTableProvider<K, V>,
    K: StorageKey,
{
    let table_id = provider.table_id();

    // Fast path: Skip uniqueness check if PK is auto-increment (O(1) cached value)
    if provider.core().is_auto_increment_pk() {
        log::trace!(
            "[ensure_unique_pk_value] Skipping PK check for {} - PK is auto-increment",
            table_id
        );
        return Ok(());
    }

    let pk_name = provider.primary_key_field_name();
    if let Some(pk_value) = row_data.get(pk_name) {
        if !matches!(pk_value, ScalarValue::Null) {
            let pk_str = unified_dml::extract_user_pk_value(row_data, pk_name)?;
            let user_scope = resolve_user_scope(scope);

            //Step 1: Check hot storage (RocksDB) - fast PK index lookup
            if provider.find_row_key_by_id_field(user_scope, &pk_str).await?.is_some() {
                return Err(KalamDbError::AlreadyExists(format!(
                    "Primary key violation: value '{}' already exists in column '{}' (hot storage)",
                    pk_str, pk_name
                )));
            }

            // Step 2: Check cold storage (Parquet files) using PkExistenceChecker
            let core = provider.core();

            // Skip cold storage check if storage registry is not available
            let Some(storage_registry) = core.services.storage_registry.clone() else {
                return Ok(()); // No cold storage to check
            };

            let pk_checker = crate::utils::pk::PkExistenceChecker::new(
                core.services.schema_registry.clone(),
                storage_registry,
                core.services.manifest_service.clone(),
            );

            let check_result = pk_checker.check_pk_exists(core, scope, &pk_str).await?;

            if let crate::utils::pk::PkCheckResult::FoundInCold { segment_path } = check_result {
                return Err(KalamDbError::AlreadyExists(format!(
                    "Primary key violation: value '{}' already exists in column '{}' (cold storage: {})",
                    pk_str, pk_name, segment_path
                )));
            }
        }
    }
    Ok(())
}

/// Log a warning when scanning version resolution without filter or limit.
///
/// This helps identify potential performance issues where full table scans are happening.
/// Called by `scan_with_version_resolution_to_kvs` implementations.
///
/// # Arguments
/// * `table_id` - Table identifier for logging
/// * `filter` - Optional filter expression
/// * `limit` - Optional limit
/// * `table_type` - Type of table (User, Shared, Stream)
pub fn warn_if_unfiltered_scan(
    _table_id: &TableId,
    _filter: Option<&Expr>,
    _limit: Option<usize>,
    _table_type: TableType,
) {
    // if filter.is_none() && limit.is_none() {
    //     log::warn!(
    //         "⚠️  [UNFILTERED SCAN] table={} type={} | No filter or limit provided - scanning ALL rows. \
    //          This may cause performance issues for large tables.",
    //         table_id,
    //         table_type.as_str()
    //     );
    // }
}

/// Validate that an UPDATE operation doesn't change the PK to an existing value
///
/// This is called when an UPDATE includes the PK column in the SET clause.
/// If the new PK value already exists (for a different row), returns an error.
///
/// **Skip conditions**:
/// - PK value is not being changed (new value == old value)
/// - PK column is not in the updates
/// - PK column has AUTO_INCREMENT (not allowed to be updated)
///
/// # Arguments
/// * `provider` - The table provider to check against
/// * `scope` - Optional user ID for scoping (User tables)
/// * `updates` - The Row containing update values
/// * `current_pk_value` - The current PK value of the row being updated
///
/// # Returns
/// * `Ok(())` if the update is valid
/// * `Err(AlreadyExists)` if the new PK value already exists
/// * `Err(InvalidOperation)` if trying to change an auto-increment PK
pub async fn validate_pk_update<P, K, V>(
    provider: &P,
    scope: Option<&UserId>,
    updates: &Row,
    current_pk_value: &ScalarValue,
) -> Result<(), KalamDbError>
where
    P: BaseTableProvider<K, V>,
    K: StorageKey,
{
    let table_id = provider.table_id();
    let pk_name = provider.primary_key_field_name();

    // Check if PK is in the update values
    let new_pk_value = match updates.get(pk_name) {
        Some(v) if !matches!(v, ScalarValue::Null) => v,
        _ => return Ok(()), // PK not being updated, nothing to validate
    };

    // Check if the value is actually changing
    if new_pk_value == current_pk_value {
        return Ok(()); // Same value, no change
    }

    // Fast path: Reject PK modification if it's auto-increment (O(1) cached value)
    if provider.core().is_auto_increment_pk() {
        return Err(KalamDbError::InvalidOperation(format!(
            "Cannot modify auto-increment primary key column '{}' in table {}",
            pk_name, table_id
        )));
    }

    // Check if the new PK value already exists
    let new_pk_str = unified_dml::extract_user_pk_value(updates, pk_name)?;
    let user_scope = resolve_user_scope(scope);

    if provider.find_row_key_by_id_field(user_scope, &new_pk_str).await?.is_some() {
        return Err(KalamDbError::AlreadyExists(format!(
            "Primary key violation: value '{}' already exists in column '{}' (UPDATE would create duplicate)",
            new_pk_str, pk_name
        )));
    }

    log::trace!(
        "[validate_pk_update] PK change validated: {} -> {} for {}",
        current_pk_value,
        new_pk_str,
        table_id
    );

    Ok(())
}

/// Apply limit to a vector of results after version resolution.
///
/// Common helper used by both User and Shared table providers.
pub fn apply_limit<T>(result: &mut Vec<T>, limit: Option<usize>) {
    if let Some(l) = limit {
        if result.len() > l {
            result.truncate(l);
        }
    }
}

/// Calculate scan limit for RocksDB based on user-provided limit.
///
/// We scan more than the limit to account for version resolution and tombstones.
/// Default is 100,000 if no limit is provided.
pub fn calculate_scan_limit(limit: Option<usize>) -> usize {
    limit.map(|l| std::cmp::max(l * 2, 1000)).unwrap_or(100_000)
}
