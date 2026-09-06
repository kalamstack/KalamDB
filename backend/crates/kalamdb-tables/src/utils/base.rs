//! Base trait for table providers with unified DML operations
//!
//! This module provides:
//! - BaseTableProvider<K, V> trait for generic table operations
//! - TableProviderCore shared structure for common services
//! - Shared MVCC-oriented scan helpers for user and shared tables
//!
//! **Design Rationale**:
//! - Eliminates most of the historic duplicate code across User/Shared/Stream providers
//! - Generic over storage key (K) and value (V) types
//! - No separate handlers - DML logic implemented directly in providers
//! - Shared core reduces memory overhead (Arc<TableProviderCore> vs per-provider fields)
//! - New planning-only helpers are moving into `kalamdb-datafusion-sources`
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
//! Stream tables ARE streamable** because they:
//! - Are append-only (no updates, no version resolution needed)
//! - Use TTL-based eviction instead of tombstones
//! - Can return rows as they're scanned with early termination on LIMIT
//! - They now use the provider-family-specific exec-backed path in `stream_table_provider.rs`
//!   instead of sharing the MVCC-oriented flow below
//!
//! ## Architecture
//!
//! ```text
//! User/Shared TableProvider::scan()
//!             │
//!             ▼
//! base_scan() ── combines filters, remaps projections, and builds the deferred
//!                MVCC execution plan used by user/shared providers
//!             │
//!             ▼
//! scan_rows() ── extracts scan context, applies fast paths, and calls
//!                scan_kvs_with_context()
//!             │
//!             ▼
//! scan_kvs_with_context() ── provider-specific hot/cold scan implementation:
//!   • User: user-scoped RocksDB prefix + Parquet, MVCC merge
//!   • Shared: full RocksDB + Parquet, MVCC merge
//!             │
//!             ▼
//! resolve_latest_scan_from_futures() ── shared concurrent hot/cold fetch +
//!                                       MVCC winner selection
//!
//! Stream tables bypass this path and build deferred execution descriptors in
//! `stream_table_provider.rs` so the hot-store scan runs at execute time.
//! ```

use std::{collections::HashSet, future::Future, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, Float32Array},
        datatypes::SchemaRef,
        record_batch::RecordBatch,
    },
    catalog::Session,
    common::DFSchema,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{utils::expr_to_columns, Expr, TableProviderFilterPushDown},
    physical_expr::PhysicalExpr,
    physical_plan::{ExecutionPlan, Statistics},
    scalar::ScalarValue,
};
use kalamdb_commons::{
    constants::SystemColumnNames,
    conversions::arrow_json_conversion::coerce_rows,
    ids::SeqId,
    models::{
        datatypes::KalamDataType,
        rows::{Row, RowMetadata},
        schemas::TableDefinition,
        NamespaceId, TableName, UserId,
    },
    schemas::TableType,
    try_pk_bucket_key, NotLeaderError, StorageKey, TableId,
};
use kalamdb_datafusion_sources::{
    exec::{
        count_resolved_from_metadata, finalize_deferred_batch, prefers_version,
        resolve_latest_kvs_from_cold_batch, DeferredBatchExec, DeferredBatchOutput,
        DeferredBatchSource, DeferredScanDiagnostics, ParquetRowData, VersionedRow,
    },
    provider::{
        combined_filter, pushdown_results_for_filters, remap_projection_indices, SourceProvider,
    },
    pruning::mvcc_filter_evaluation,
};
use kalamdb_filestore::registry::{ListResult, StorageCached};
use kalamdb_session_datafusion::ScanDiagnosticsContext;
use kalamdb_store::IndexedEntityStore;
use kalamdb_system::{
    ClusterCoordinator as ClusterCoordinatorTrait, Manifest, ManifestCacheEntry,
    SchemaRegistry as SchemaRegistryTrait,
};
use kalamdb_transactions::{
    extract_transaction_query_context, TransactionAccessError, TransactionOverlay,
    TransactionOverlayExec,
};

// Re-export types moved to submodules
pub use crate::utils::core::TableProviderCore;
pub(crate) use crate::utils::parquet::{
    scan_parquet_files_as_batch_async, scan_parquet_files_as_result_async,
    scan_parquet_files_with_stats_async, ParquetScanResult,
};
pub use crate::utils::row_utils::{
    extract_full_user_context, extract_seq_bounds_from_filter, inject_system_columns,
    resolve_user_scope, rows_to_arrow_batch, system_user_id, ScanRow,
};
use crate::{error::KalamDbError, manifest::ManifestAccessPlanner, utils::unified_dml};

pub struct MvccScanResult<K, V> {
    pub rows:        Vec<(K, V)>,
    pub diagnostics: DeferredScanDiagnostics,
}

pub(crate) fn scan_diagnostics_enabled(state: &dyn Session) -> bool {
    state
        .config()
        .options()
        .extensions
        .get::<ScanDiagnosticsContext>()
        .map(ScanDiagnosticsContext::is_enabled)
        .unwrap_or(false)
}

#[async_trait]
pub trait DeferredMvccScanProvider<K: StorageKey, V>:
    BaseTableProvider<K, V> + Clone + Send + Sync + 'static
where
    K: StorageKey + Clone + Send + Sync + 'static,
    V: ScanRow + Send + Sync + 'static,
{
    type ScanContext: Clone + Send + Sync + 'static;

    fn scan_source_name(&self) -> &'static str;

    fn build_scan_context(&self, state: &dyn Session) -> Result<Self::ScanContext, KalamDbError>;

    fn scan_snapshot_commit_seq(&self, scan_context: &Self::ScanContext) -> Option<u64>;

    fn allow_pk_fast_path(&self, scan_context: &Self::ScanContext) -> bool {
        self.scan_snapshot_commit_seq(scan_context).is_none()
    }

    fn allow_count_only_fast_path(&self, _scan_context: &Self::ScanContext) -> bool {
        true
    }

    fn authorization_plan_details(
        &self,
        _scan_context: &Self::ScanContext,
        _filter: Option<&Expr>,
    ) -> Option<String> {
        None
    }

    /// Return false only when a leakproof authorization guard proves that the
    /// scan cannot produce an authorized row.
    async fn pre_authorize_scan(
        &self,
        _scan_context: &Self::ScanContext,
        _filter: Option<&Expr>,
    ) -> Result<bool, KalamDbError> {
        Ok(true)
    }

    /// When false, resolved MVCC winners are returned without per-row RLS work.
    fn requires_row_authorization(&self, _scan_context: &Self::ScanContext) -> bool {
        true
    }

    async fn authorize_resolved_rows(
        &self,
        _scan_context: &Self::ScanContext,
        rows: Vec<(K, V)>,
    ) -> Result<Vec<(K, V)>, KalamDbError> {
        Ok(rows)
    }

    fn scan_scope_label(&self, _scan_context: &Self::ScanContext) -> &'static str {
        "default"
    }

    fn scan_cold_scope<'a>(&self, scan_context: &'a Self::ScanContext) -> Option<&'a UserId>;

    /// Return the newest hot-storage version, including tombstones.
    ///
    /// The resolver must inspect the deletion flag from this same lookup so a
    /// point read does not repeat the RocksDB index seek and entity fetch.
    async fn scan_latest_hot_pk_entry(
        &self,
        scan_context: &Self::ScanContext,
        pk_value: &ScalarValue,
    ) -> Result<Option<(K, V)>, KalamDbError>;

    async fn count_rows_with_context(
        &self,
        scan_context: &Self::ScanContext,
    ) -> Result<usize, KalamDbError>;

    async fn scan_kvs_with_context(
        &self,
        scan_context: &Self::ScanContext,
        filter: Option<&Expr>,
        since_seq: Option<SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
        cold_columns: Option<&[String]>,
    ) -> Result<Vec<(K, V)>, KalamDbError>;

    async fn scan_kvs_with_diagnostics(
        &self,
        scan_context: &Self::ScanContext,
        filter: Option<&Expr>,
        since_seq: Option<SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
        cold_columns: Option<&[String]>,
    ) -> Result<MvccScanResult<K, V>, KalamDbError> {
        let rows = self
            .scan_kvs_with_context(
                scan_context,
                filter,
                since_seq,
                limit,
                keep_deleted,
                cold_columns,
            )
            .await?;
        Ok(MvccScanResult {
            rows,
            diagnostics: DeferredScanDiagnostics::default(),
        })
    }

    async fn scan_rows_with_context(
        &self,
        scan_context: &Self::ScanContext,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, KalamDbError> {
        Ok(self
            .scan_rows_output(scan_context, projection, filter, filter, limit, false)
            .await?
            .batch)
    }

    async fn scan_rows_with_diagnostics(
        &self,
        scan_context: &Self::ScanContext,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        limit: Option<usize>,
    ) -> Result<DeferredBatchOutput, KalamDbError> {
        self.scan_rows_output(scan_context, projection, filter, filter, limit, true)
            .await
    }

    async fn scan_rows_output(
        &self,
        scan_context: &Self::ScanContext,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        authorization_filter: Option<&Expr>,
        limit: Option<usize>,
        include_diagnostics: bool,
    ) -> Result<DeferredBatchOutput, KalamDbError> {
        let schema = self.schema_ref();
        let pk_name = self.primary_key_field_name();
        let _scope_label = self.scan_scope_label(scan_context);
        let _subject_user = self.scan_cold_scope(scan_context).map(UserId::as_str).unwrap_or("-");

        if !self.pre_authorize_scan(scan_context, authorization_filter).await? {
            let batch = rows_to_arrow_batch(&schema, Vec::<(K, V)>::new(), projection, |_, _| {})?;
            return Ok(DeferredBatchOutput::new(batch));
        }

        if self.allow_pk_fast_path(scan_context) {
            if let Some(pk_scalar) = typed_pk_literal_from_filter(&schema, filter, pk_name) {
                let resolved = resolve_pk_point_lookup(self, scan_context, &pk_scalar).await?;
                let resolved = if self.requires_row_authorization(scan_context) {
                    self.authorize_resolved_rows(scan_context, resolved.into_iter().collect())
                        .await?
                } else {
                    resolved.into_iter().collect()
                };
                let batch = rows_to_arrow_batch(&schema, resolved, projection, |_, _| {})?;
                return Ok(DeferredBatchOutput::new(batch));
            }
        }

        if self.allow_count_only_fast_path(scan_context)
            && is_count_only_projection(projection, filter)
        {
            let count = self.count_rows_with_context(scan_context).await?;
            return Ok(DeferredBatchOutput::new(build_count_only_batch(count)?));
        }

        let (since_seq, _until_seq) = if let Some(expr) = filter {
            extract_seq_bounds_from_filter(expr)
        } else {
            (None, None)
        };

        let keep_deleted = filter.map(filter_uses_deleted_column).unwrap_or(false);
        let cold_columns = compute_cold_columns(projection, &schema, pk_name);
        let scan_result = if include_diagnostics {
            self.scan_kvs_with_diagnostics(
                scan_context,
                filter,
                since_seq,
                limit,
                keep_deleted,
                cold_columns.as_deref(),
            )
            .await?
        } else {
            MvccScanResult {
                rows:        self
                    .scan_kvs_with_context(
                        scan_context,
                        filter,
                        since_seq,
                        limit,
                        keep_deleted,
                        cold_columns.as_deref(),
                    )
                    .await?,
                diagnostics: DeferredScanDiagnostics::default(),
            }
        };

        // log::trace!(
        //     "[MvccScan] scan_rows resolved {} row(s) for table={} scope={} subject={}",
        //     kvs.len(),
        //     self.table_id(),
        //     scope_label,
        //     subject_user
        // );

        let authorized_rows = if self.requires_row_authorization(scan_context) {
            self.authorize_resolved_rows(scan_context, scan_result.rows).await?
        } else {
            scan_result.rows
        };
        let batch = rows_to_arrow_batch(&schema, authorized_rows, projection, |_, _| {})?;
        Ok(DeferredBatchOutput::new(batch).with_diagnostics(scan_result.diagnostics))
    }
}

struct DeferredMvccScanSource<P, K, V>
where
    P: DeferredMvccScanProvider<K, V>,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: ScanRow + Send + Sync + 'static,
{
    provider:             P,
    scan_context:         P::ScanContext,
    projection:           Option<Vec<usize>>,
    filter:               Option<Expr>,
    authorization_filter: Option<Expr>,
    physical_filter:      Option<Arc<dyn PhysicalExpr>>,
    output_projection:    Option<Vec<usize>>,
    limit:                Option<usize>,
    output_schema:        SchemaRef,
    _marker:              std::marker::PhantomData<(K, V)>,
}

impl<P, K, V> std::fmt::Debug for DeferredMvccScanSource<P, K, V>
where
    P: DeferredMvccScanProvider<K, V>,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: ScanRow + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredMvccScanSource")
            .field("source", &self.provider.scan_source_name())
            .field("projection", &self.projection)
            .field("has_filter", &self.filter.is_some())
            .field("has_authorization_filter", &self.authorization_filter.is_some())
            .finish()
    }
}

impl<P, K, V> DeferredMvccScanSource<P, K, V>
where
    P: DeferredMvccScanProvider<K, V>,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: ScanRow + Send + Sync + 'static,
{
    async fn produce_output(
        &self,
        include_diagnostics: bool,
    ) -> DataFusionResult<DeferredBatchOutput> {
        let source_limit = if self.physical_filter.is_none() {
            self.limit
        } else {
            None
        };
        let output = self
            .provider
            .scan_rows_output(
                &self.scan_context,
                self.projection.as_ref(),
                self.filter.as_ref(),
                self.authorization_filter.as_ref(),
                source_limit,
                include_diagnostics,
            )
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "{} failed: {}",
                    self.provider.scan_source_name(),
                    error
                ))
            })?;

        let batch = finalize_deferred_batch(
            output.batch,
            self.physical_filter.as_ref(),
            self.output_projection.as_deref(),
            self.limit,
            self.provider.scan_source_name(),
        )?;
        Ok(DeferredBatchOutput::new(batch).with_diagnostics(output.diagnostics))
    }
}

#[async_trait]
impl<P, K, V> DeferredBatchSource for DeferredMvccScanSource<P, K, V>
where
    P: DeferredMvccScanProvider<K, V>,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: ScanRow + Send + Sync + 'static,
{
    fn source_name(&self) -> &'static str {
        self.provider.scan_source_name()
    }

    fn plan_details(&self) -> Option<String> {
        let mut details = "storage_tiers=[hot=rocksdb,cold=parquet], mvcc=true".to_string();
        if let Some(authorization) = self.provider.authorization_plan_details(
            &self.scan_context,
            self.authorization_filter.as_ref().or(self.filter.as_ref()),
        ) {
            details.push_str(", ");
            details.push_str(&authorization);
        }
        Some(details)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch> {
        Ok(self.produce_output(false).await?.batch)
    }

    async fn produce_batch_with_diagnostics(&self) -> DataFusionResult<DeferredBatchOutput> {
        self.produce_output(true).await
    }
}

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
/// Provider.scan_kvs_with_context(scan_context, filter)
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
    /// - Strict subject scoping from the effective execution context
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
    /// `Ok(Some(key))` if the row was updated, `Ok(None)` if the row was unchanged (no-op).
    async fn update(
        &self,
        user_id: &UserId,
        key: &K,
        updates: Row,
    ) -> Result<Option<K>, KalamDbError>;

    /// Delete a row by key (appends tombstone with _deleted=true)
    ///
    /// **Implementation**: Uses version_resolution helpers + unified_dml
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `key` - Storage key identifying the row
    async fn delete(&self, user_id: &UserId, key: &K) -> Result<(), KalamDbError>;

    /// Update multiple rows in a batch (default implementation).
    /// Returns only the keys that were actually modified (skips no-op updates).
    async fn update_batch(
        &self,
        user_id: &UserId,
        updates: Vec<(K, Row)>,
    ) -> Result<Vec<K>, KalamDbError> {
        let mut results = Vec::with_capacity(updates.len());
        for (key, update) in updates {
            if let Some(k) = BaseTableProvider::update(self, user_id, &key, update).await? {
                results.push(k);
            }
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
    /// `Ok(Some(key))` if the row was updated, `Ok(None)` if the row was unchanged (no-op).
    async fn update_by_pk_value(
        &self,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<Option<K>, KalamDbError>;

    /// Update a row by searching for matching ID field value
    async fn update_by_id_field(
        &self,
        user_id: &UserId,
        id_value: &str,
        updates: Row,
    ) -> Result<Option<K>, KalamDbError> {
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
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>>
    where
        Self: SourceProvider,
    {
        Ok(pushdown_results_for_filters(filters, |filter| self.filter_capability(filter)))
    }

    /// Manifest-backed table statistics for the DataFusion optimizer.
    ///
    /// DataFusion's mainline planner does not consume [`TableProvider::statistics`] yet,
    /// but KalamDB exposes cold-segment estimates here for future optimizer rules and
    /// downstream tooling.
    fn statistics(&self) -> Option<Statistics> {
        crate::utils::table_statistics::compute_manifest_table_statistics(
            self.core(),
            self.provider_table_type(),
        )
    }

    /// Default implementation for scan
    async fn base_scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>>
    where
        Self: SourceProvider + DeferredMvccScanProvider<K, V> + Sized,
        K: Clone + Send + Sync + 'static,
        V: ScanRow + Send + Sync + 'static,
    {
        self.validate_transaction_table_access(state)?;
        self.ensure_leader_read(state).await.map_err(kalam_error_to_datafusion)?;

        let descriptor = self.scan_descriptor(projection, filters, limit);
        let pruning = descriptor.pruning_request();
        let filter_evaluation =
            mvcc_filter_evaluation(pruning.filters.filters.as_ref(), self.primary_key_field_name());
        let _ = pruning.limit.limit;
        let source_filter = combined_filter(filter_evaluation.inexact.filters.as_ref());
        let authorization_filter = combined_filter(filters);
        let exact_filter = combined_filter(filter_evaluation.exact.filters.as_ref());
        let effective_projection =
            pruning.projection.columns.as_ref().map(|indices| indices.as_ref().to_vec());

        let merged_schema = match effective_projection.as_ref() {
            Some(indices) => descriptor
                .schema
                .project(indices)
                .map(Arc::new)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
            None => Arc::clone(&descriptor.schema),
        };
        let output_projection = if pruning.filters.filters.is_empty() {
            None
        } else {
            projection.map(|indices| {
                remap_projection_indices(&descriptor.schema, &merged_schema, indices)
            })
        };
        let output_schema = match projection {
            Some(indices) => descriptor
                .schema
                .project(indices)
                .map(Arc::new)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
            None => Arc::clone(&descriptor.schema),
        };
        let physical_filter = if let Some(filter) = exact_filter.clone() {
            let df_schema = DFSchema::try_from(Arc::clone(&merged_schema))?;
            Some(state.create_physical_expr(filter, &df_schema)?)
        } else {
            None
        };
        let scan_context = self.build_scan_context(state).map_err(kalam_error_to_datafusion)?;
        let source = Arc::new(DeferredMvccScanSource::<Self, K, V> {
            provider: self.clone(),
            scan_context,
            projection: effective_projection,
            filter: source_filter,
            authorization_filter,
            physical_filter,
            output_projection,
            limit,
            output_schema,
            _marker: std::marker::PhantomData,
        });

        if scan_diagnostics_enabled(state) {
            Ok(Arc::new(DeferredBatchExec::new_with_scan_diagnostics(source)))
        } else {
            Ok(Arc::new(DeferredBatchExec::new(source)))
        }
    }

    async fn base_scan_with_overlay(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        overlay: Option<TransactionOverlay>,
        overlay_user: Option<UserId>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>>
    where
        Self: SourceProvider + DeferredMvccScanProvider<K, V> + Sized,
        K: Clone + Send + Sync + 'static,
        V: ScanRow + Send + Sync + 'static,
    {
        let Some(overlay) = overlay else {
            return self.base_scan(state, projection, filters, limit).await;
        };

        let overlay_projection = crate::utils::datafusion_dml::prepare_overlay_scan_projection(
            &self.schema_ref(),
            projection,
            self.primary_key_field_name(),
        )?;
        let base_plan = self
            .base_scan(state, overlay_projection.effective_projection.as_ref(), filters, limit)
            .await?;

        Ok(Arc::new(TransactionOverlayExec::try_new(
            base_plan,
            self.table_id().clone(),
            self.primary_key_field_name().to_string(),
            overlay,
            overlay_user,
            overlay_projection.final_projection,
            None,
        )?))
    }

    fn validate_transaction_table_access(&self, state: &dyn Session) -> DataFusionResult<()> {
        let Some(transaction_query_context) = extract_transaction_query_context(state) else {
            return Ok(());
        };

        let user_id = match self.provider_table_type() {
            TableType::User | TableType::Stream => {
                let (user_id, _role, _read_context) = extract_full_user_context(state)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                Some(user_id)
            },
            TableType::Shared | TableType::System => None,
        };

        transaction_query_context
            .access_validator
            .validate_table_access(
                &transaction_query_context.transaction_id,
                self.table_id(),
                self.provider_table_type(),
                user_id,
            )
            .map_err(transaction_access_error_to_datafusion)
    }

    /// Enforce leader-only reads for client contexts in cluster mode.
    async fn ensure_leader_read(&self, state: &dyn Session) -> Result<(), KalamDbError> {
        let (user_id, _role, read_context) = extract_full_user_context(state)?;
        if !read_context.requires_leader() {
            return Ok(());
        }

        let coordinator = self.cluster_coordinator();
        if !coordinator.is_cluster_mode().await {
            return Ok(());
        }

        match self.provider_table_type() {
            TableType::User | TableType::Stream => {
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
        cold_columns: Option<&[String]>,
        snapshot_commit_seq: Option<u64>,
    ) -> Result<Vec<(K, V)>, KalamDbError>;

    /// Extract row fields from provider-specific value type
    ///
    /// Each provider implements this to access the internal `Row` stored on their row type.
    fn extract_row(row: &V) -> &Row;
}

pub(crate) fn transaction_access_error_to_datafusion(
    error: TransactionAccessError,
) -> DataFusionError {
    match error {
        TransactionAccessError::NotLeader { leader_addr } => {
            DataFusionError::External(Box::new(NotLeaderError::new(leader_addr)))
        },
        TransactionAccessError::InvalidOperation(message) => DataFusionError::Execution(message),
    }
}

fn kalam_error_to_datafusion(error: KalamDbError) -> DataFusionError {
    match error {
        KalamDbError::NotLeader { leader_addr } => {
            DataFusionError::External(Box::new(NotLeaderError::new(leader_addr)))
        },
        other => DataFusionError::Execution(other.to_string()),
    }
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

pub fn typed_pk_literal_from_filter(
    schema: &SchemaRef,
    filter: Option<&Expr>,
    pk_name: &str,
) -> Option<ScalarValue> {
    let pk_literal = filter.and_then(|expr| extract_pk_equality_literal(expr, pk_name))?;
    let pk_scalar = if let Ok(field) = schema.field_with_name(pk_name) {
        kalamdb_commons::conversions::parse_string_as_scalar(
            &pk_literal.to_string(),
            field.data_type(),
        )
        .ok()
        .unwrap_or(pk_literal)
    } else {
        pk_literal
    };

    Some(pk_scalar)
}

pub fn is_count_only_projection(projection: Option<&Vec<usize>>, filter: Option<&Expr>) -> bool {
    projection.is_some_and(|proj| proj.is_empty()) && filter.is_none()
}

fn prefers_scan_row_version<V: ScanRow>(candidate: &V, current: &V) -> bool {
    prefers_version(
        candidate.commit_seq_value(),
        SeqId::from_i64(candidate.seq_value()),
        current.commit_seq_value(),
        SeqId::from_i64(current.seq_value()),
    )
}

fn prefers_scan_row_pair<K, V: ScanRow>(candidate: &(K, V), current: &(K, V)) -> bool {
    prefers_scan_row_version(&candidate.1, &current.1)
}

fn visible_hot_entry<K, V: ScanRow>(entry: Option<(K, V)>) -> Option<(K, V)> {
    entry.filter(|(_, row)| !row.deleted_flag())
}

/// Cached empty manifests mean there is no Parquet to merge.
///
/// Missing or failed loads return `false` so we still scan cold (manual
/// `STORAGE FLUSH` and degraded listings can have files without a cached empty
/// manifest).
fn cached_manifest_is_hot_only(entry: Option<&ManifestCacheEntry>) -> bool {
    entry.is_some_and(|cached| cached.manifest.segments.is_empty())
}

/// Resolve a single PK equality lookup by merging the latest hot and cold versions.
async fn resolve_pk_point_lookup<P, K, V>(
    provider: &P,
    scan_context: &P::ScanContext,
    pk_scalar: &ScalarValue,
) -> Result<Option<(K, V)>, KalamDbError>
where
    P: DeferredMvccScanProvider<K, V>,
    K: StorageKey + Send + Sync + 'static,
    V: ScanRow + Send + Sync + 'static,
{
    let latest_hot = provider.scan_latest_hot_pk_entry(scan_context, pk_scalar).await?;
    if latest_hot.as_ref().is_some_and(|(_, row)| row.deleted_flag()) {
        return Ok(None);
    }
    let hot = visible_hot_entry(latest_hot);

    let skip_cold = match provider
        .core()
        .services
        .manifest_service
        .get_or_load_async(provider.table_id(), provider.scan_cold_scope(scan_context))
        .await
    {
        Ok(entry) => cached_manifest_is_hot_only(entry.as_deref()),
        Err(_) => false,
    };
    if skip_cold {
        return Ok(hot);
    }

    // Merge cold when the manifest has segments, is missing, or failed to load.
    // Manual `STORAGE FLUSH` can materialize Parquet even without FLUSH_POLICY.
    let cold =
        find_row_by_pk(provider, provider.scan_cold_scope(scan_context), &pk_scalar.to_string())
            .await?;

    Ok(match (hot, cold) {
        (Some(candidate), Some(current)) if prefers_scan_row_pair(&current, &candidate) => {
            Some(current)
        },
        (Some(candidate), Some(_)) => Some(candidate),
        (Some(row), None) | (None, Some(row)) => Some(row),
        (None, None) => None,
    })
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
    use datafusion::prelude::{col, lit};

    use crate::utils::version_resolution::{parquet_batch_to_rows, ParquetRowData};

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
    let batch = scan_parquet_files_as_batch_async(
        core,
        table_id,
        table_type,
        scope,
        schema,
        Some(&filter),
        None,
    )
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
            let Ok(row_pk_key) = try_pk_bucket_key(row_pk) else {
                continue;
            };
            if row_pk_key.to_string() != pk_value {
                continue;
            }

            // Keep the row with the highest (commit_seq, seq) version.
            if latest
                .as_ref()
                .map(|current| {
                    prefers_version(
                        row_data.commit_seq,
                        row_data.seq_id,
                        current.commit_seq,
                        current.seq_id,
                    )
                })
                .unwrap_or(true)
            {
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

pub(crate) struct ResolvedMvccScan<K, V> {
    pub rows:               Vec<(K, V)>,
    pub hot_rows_scanned:   usize,
    pub cold_rows_scanned:  usize,
    pub cold_files_total:   usize,
    pub cold_files_skipped: usize,
    pub cold_files_scanned: usize,
    pub cold_files:         Vec<String>,
}

pub(crate) async fn resolve_latest_scan_from_futures<K, R, HotFuture, ColdFuture, Build>(
    pk_name: &str,
    limit: Option<usize>,
    keep_deleted: bool,
    snapshot_commit_seq: Option<u64>,
    hot_future: HotFuture,
    cold_future: ColdFuture,
    build_cold_row: Build,
) -> Result<ResolvedMvccScan<K, R>, KalamDbError>
where
    K: Clone,
    R: VersionedRow,
    HotFuture: Future<Output = Result<Vec<(K, R)>, KalamDbError>>,
    ColdFuture: Future<Output = Result<ParquetScanResult, KalamDbError>>,
    Build: Fn(ParquetRowData) -> DataFusionResult<(K, R)>,
{
    let (hot_result, cold_result) = tokio::join!(hot_future, cold_future);
    let hot_rows = hot_result?;
    let hot_rows_scanned = hot_rows.len();
    let cold_result = cold_result?;
    let cold_rows_scanned = cold_result.batch.num_rows();

    let mut rows = resolve_latest_kvs_from_cold_batch(
        pk_name,
        hot_rows,
        &cold_result.batch,
        keep_deleted,
        snapshot_commit_seq,
        build_cold_row,
    )
    .map_err(|error| KalamDbError::DataFusion(error.to_string()))?;

    apply_limit(&mut rows, limit);

    Ok(ResolvedMvccScan {
        rows,
        hot_rows_scanned,
        cold_rows_scanned,
        cold_files_total: cold_result.stats.total_files,
        cold_files_skipped: cold_result.stats.skipped_files,
        cold_files_scanned: cold_result.stats.scanned_files,
        cold_files: cold_result.stats.visited_files,
    })
}

pub(crate) async fn count_resolved_rows_from_futures<HotFuture, ColdFuture>(
    pk_name: &str,
    snapshot_commit_seq: Option<u64>,
    hot_future: HotFuture,
    cold_future: ColdFuture,
) -> Result<usize, KalamDbError>
where
    HotFuture: Future<Output = Result<Vec<RowMetadata>, KalamDbError>>,
    ColdFuture: Future<Output = Result<RecordBatch, KalamDbError>>,
{
    let (hot_result, cold_result) = tokio::join!(hot_future, cold_future);
    let hot_metadata = hot_result?;
    let parquet_batch = cold_result?;

    count_resolved_from_metadata(pk_name, hot_metadata, &parquet_batch, snapshot_commit_seq)
        .map_err(|error| KalamDbError::DataFusion(error.to_string()))
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

    // 1. Load manifest through the centralized memory -> RocksDB -> storage path.
    let manifest_service = core.services.manifest_service.clone();
    let cache_result = manifest_service.get_or_load_async(table_id, user_id).await;

    let manifest: Option<Manifest> = match &cache_result {
        Ok(Some(entry)) => Some(entry.manifest.clone()),
        Ok(None) => {
            // log::trace!(
            //     "[pk_exists_in_cold] No manifest for {}.{} {} - checking all files",
            //     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            None
        },
        Err(kalamdb_store::StorageError::SerializationError(e)) => {
            return Err(KalamDbError::InvalidOperation(format!(
                "Failed to load manifest for {} {}: {}",
                table_id, scope_label, e
            )));
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
            // log::trace!(
            //     "[pk_exists_in_cold] Manifest has no segments for {}.{} {} - PK not in cold",
            //     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            return Ok(false);
        }
    }

    // 2. Use manifest to prune segments or list all Parquet files.
    let planner = ManifestAccessPlanner::new();
    let mut storage_cached_for_scan: Option<Arc<StorageCached>> = None;
    let files_to_scan: Vec<String> = if let Some(ref m) = manifest {
        let pruned_paths = planner.plan_by_pk_value(m, pk_column_id, pk_value);
        if pruned_paths.is_empty() {
            // log::trace!(
            //     "[pk_exists_in_cold] Manifest pruning returned no candidate segments for PK {} on
            // {}.{} {} - PK not in cold",     pk_value,
            //     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            return Ok(false);
        } else {
            // log::trace!(
            //     "[pk_exists_in_cold] Manifest pruning: {} of {} segments may contain PK {} for
            // {}.{} {}",     pruned_paths.len(),
            //     m.segments.len(),
            //     pk_value,
            //     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            pruned_paths
        }
    } else {
        // No manifest - use all Parquet files from listing.
        let Some(storage_cached) = resolve_storage_cached_for_pk(core, table_id)? else {
            return Ok(false);
        };
        let list_result = match storage_cached.list(table_type, table_id, user_id).await {
            Ok(result) => result,
            Err(_) => {
                // log::trace!(
                //     "[pk_exists_in_cold] No storage dir for {}.{} {} - PK not in cold",
                //     namespace.as_str(),
                //     table.as_str(),
                //     scope_label
                // );
                return Ok(false);
            },
        };
        if list_result.is_empty() {
            // log::trace!(
            //     "[pk_exists_in_cold] No files in storage for {}.{} {} - PK not in cold",
            //     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            return Ok(false);
        }
        storage_cached_for_scan = Some(storage_cached);
        collect_parquet_files_from_list(&list_result)
    };

    if files_to_scan.is_empty() {
        return Ok(false);
    }

    let storage_cached = match storage_cached_for_scan {
        Some(cached) => cached,
        None => match resolve_storage_cached_for_pk(core, table_id)? {
            Some(cached) => cached,
            None => return Ok(false),
        },
    };

    // 3. Scan pruned Parquet files and check for PK using StorageCached.
    // Manifest paths are just filenames (e.g., "batch-0.parquet"), so prepend storage_path
    for file_name in files_to_scan {
        if crate::utils::pk::pk_exists_in_parquet_file(
            storage_cached.as_ref(),
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

fn resolve_storage_cached_for_pk(
    core: &TableProviderCore,
    table_id: &TableId,
) -> Result<Option<Arc<StorageCached>>, KalamDbError> {
    let storage_id = match core.services.schema_registry.get_storage_id(table_id) {
        Ok(id) => id,
        Err(_) => return Ok(None),
    };

    let storage_registry = core.services.storage_registry.as_ref().ok_or_else(|| {
        KalamDbError::InvalidOperation("Storage registry not configured".to_string())
    })?;
    storage_registry.get_cached(&storage_id).map_err(KalamDbError::from)
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

    // 1. Load manifest through the centralized memory -> RocksDB -> storage path.
    let manifest_service = core.services.manifest_service.clone();
    let cache_result = manifest_service.get_or_load_async(table_id, user_id).await;

    let manifest: Option<Manifest> = match &cache_result {
        Ok(Some(entry)) => Some(entry.manifest.clone()),
        Ok(None) => {
            // log::trace!(
            //     "[pk_exists_batch_in_cold] No manifest for {}.{} {} - checking all files",
            //     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            None
        },
        Err(kalamdb_store::StorageError::SerializationError(e)) => {
            return Err(KalamDbError::InvalidOperation(format!(
                "Failed to load manifest for {} {}: {}",
                table_id, scope_label, e
            )));
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
            // log::trace!(
            //     "[pk_exists_batch_in_cold] Manifest has no segments for {}.{} {} - PK not in
            // cold",     namespace.as_str(),
            //     table.as_str(),
            //     scope_label
            // );
            return Ok(None);
        }
    }

    // 2. Determine files to scan - union of files that may contain any PK value.
    let planner = ManifestAccessPlanner::new();
    let mut storage_cached_for_scan: Option<Arc<StorageCached>> = None;
    let files_to_scan: Vec<String> = if let Some(ref m) = manifest {
        // Collect all potentially relevant files for any PK value
        let mut relevant_files: HashSet<String> = HashSet::new();
        for pk_value in pk_values {
            let pruned_paths = planner.plan_by_pk_value(m, pk_column_id, pk_value);
            relevant_files.extend(pruned_paths);
        }
        if relevant_files.is_empty() {
            log::trace!(
                "[pk_exists_batch_in_cold] Manifest pruning returned no candidate segments for \
                 {}.{} {} - PKs not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        } else {
            log::trace!(
                "[pk_exists_batch_in_cold] Manifest pruning: {} of {} segments may contain {} PKs \
                 for {}.{} {}",
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
        let Some(storage_cached) = resolve_storage_cached_for_pk(core, table_id)? else {
            log::trace!(
                "[pk_exists_batch_in_cold] No storage id for {}.{} {} - PK not in cold",
                namespace.as_str(),
                table.as_str(),
                scope_label
            );
            return Ok(None);
        };
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
        storage_cached_for_scan = Some(storage_cached);
        collect_parquet_files_from_list(&list_result)
    };

    if files_to_scan.is_empty() {
        return Ok(None);
    }

    let storage_cached = match storage_cached_for_scan {
        Some(cached) => cached,
        None => match resolve_storage_cached_for_pk(core, table_id)? {
            Some(cached) => cached,
            None => return Ok(None),
        },
    };

    // 3. Create a HashSet for O(1) PK lookups.
    let pk_set: HashSet<&str> = pk_values.iter().map(|s| s.as_str()).collect();

    // 4. Scan Parquet files and check for PKs (batch version).
    for file_name in files_to_scan {
        if let Some(found_pk) = crate::utils::pk::first_existing_pk_in_parquet_file(
            storage_cached.as_ref(),
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

            // Step 1: Check hot storage (RocksDB) - fast PK index lookup
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
                    "Primary key violation: value '{}' already exists in column '{}' (cold \
                     storage: {})",
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
/// If `filter` matches a prefix index, return `(index_idx, prefix)` for `scan_by_index`.
pub(crate) fn hot_index_seek<K, V>(
    store: &IndexedEntityStore<K, V>,
    filter: Option<&Expr>,
    user_id: Option<&UserId>,
) -> Option<(usize, Vec<u8>)>
where
    K: StorageKey + Clone + Send + Sync + 'static,
    V: kalamdb_commons::KSerializable + Clone + Send + Sync + 'static,
{
    store.find_best_index_for_filter_expr(user_id, filter?)
}

/// Called by provider-side MVCC scan implementations.
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
    //         "⚠️  [UNFILTERED SCAN] table={} type={} | No filter or limit provided - scanning ALL
    // rows. \          This may cause performance issues for large tables.",
    //         table_id,
    //         table_type.as_str()
    //     );
    // }
}

/// Compute the minimal set of column names needed from the Parquet cold path.
///
/// When a query projects specific columns, we only need those columns plus
/// system columns (`_seq`, `_deleted`) and the primary key for version resolution.
/// Returns `None` when all columns should be read (projection is None).
pub fn compute_cold_columns(
    projection: Option<&Vec<usize>>,
    schema: &SchemaRef,
    pk_name: &str,
) -> Option<Vec<String>> {
    let proj = projection?;
    let mut col_set: HashSet<String> =
        proj.iter().map(|&i| schema.field(i).name().clone()).collect();
    // Always include columns required for version resolution
    for sys_col in [
        SystemColumnNames::SEQ,
        SystemColumnNames::COMMIT_SEQ,
        SystemColumnNames::DELETED,
    ] {
        col_set.insert(sys_col.to_string());
    }
    col_set.insert(pk_name.to_string());
    Some(col_set.into_iter().collect())
}

/// Compute the minimal cold-path columns needed for metadata-only MVCC work.
///
/// Used by count-only paths that only need the primary key and MVCC system
/// columns to choose visible winners without decoding full user payloads.
pub fn compute_metadata_only_cold_columns(pk_name: &str) -> Vec<String> {
    vec![
        pk_name.to_string(),
        SystemColumnNames::SEQ.to_string(),
        SystemColumnNames::COMMIT_SEQ.to_string(),
        SystemColumnNames::DELETED.to_string(),
    ]
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
            "Primary key violation: value '{}' already exists in column '{}' (UPDATE would create \
             duplicate)",
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

/// Return embedding columns that need vector hot-staging support.
///
/// Shared between user and shared table providers so provider constructors
/// don't each repeat the schema walk and dimension filtering.
pub fn embedding_columns(table_def: &TableDefinition) -> Vec<(String, u32)> {
    table_def
        .columns
        .iter()
        .filter_map(|column| match &column.data_type {
            KalamDataType::Embedding(dim) if *dim > 0 => {
                Some((column.column_name.clone(), *dim as u32))
            },
            _ => None,
        })
        .collect()
}

/// Extract an embedding vector from a ScalarValue, validating dimensions.
///
/// Shared between SharedTableProvider and UserTableProvider for vector
/// column upsert operations.
pub fn extract_embedding_vector(value: &ScalarValue, expected_dimensions: u32) -> Option<Vec<f32>> {
    let parse_inner = |array: &dyn Array| -> Option<Vec<f32>> {
        let float_array = array.as_any().downcast_ref::<Float32Array>()?;
        if float_array.len() != expected_dimensions as usize
            || float_array.null_count() == float_array.len()
        {
            return None;
        }
        Some(
            (0..float_array.len())
                .map(|idx| {
                    if float_array.is_null(idx) {
                        0.0
                    } else {
                        float_array.value(idx)
                    }
                })
                .collect(),
        )
    };

    match value {
        ScalarValue::FixedSizeList(list) => {
            if list.is_empty() || list.is_null(0) {
                return None;
            }
            parse_inner(list.value(0).as_ref())
        },
        ScalarValue::List(list) => {
            if list.is_empty() || list.is_null(0) {
                return None;
            }
            parse_inner(list.value(0).as_ref())
        },
        ScalarValue::LargeList(list) => {
            if list.is_empty() || list.is_null(0) {
                return None;
            }
            parse_inner(list.value(0).as_ref())
        },
        ScalarValue::Utf8(Some(json)) | ScalarValue::LargeUtf8(Some(json)) => {
            let parsed = serde_json::from_str::<Vec<f32>>(json).ok()?;
            if parsed.len() != expected_dimensions as usize {
                return None;
            }
            Some(parsed)
        },
        _ => None,
    }
}

/// Build a notification row from any entity that has common MVCC fields.
///
/// Both SharedTableRow and UserTableRow have `_seq`, `_commit_seq`, `_deleted`, and `fields`.
/// This function avoids duplicating the notification row building logic.
pub fn build_notification_row(fields: &Row, seq: SeqId, commit_seq: u64, deleted: bool) -> Row {
    let mut values = fields.values.clone();
    values.insert(SystemColumnNames::SEQ.to_string(), ScalarValue::Int64(Some(seq.as_i64())));
    values.insert(SystemColumnNames::COMMIT_SEQ.to_string(), ScalarValue::UInt64(Some(commit_seq)));
    values.insert(SystemColumnNames::DELETED.to_string(), ScalarValue::Boolean(Some(deleted)));
    Row::new(values)
}

/// Build a notification row for append-only stream tables.
pub fn build_stream_notification_row(fields: &Row, seq: SeqId) -> Row {
    let mut values = fields.values.clone();
    values.insert(SystemColumnNames::SEQ.to_string(), ScalarValue::Int64(Some(seq.as_i64())));
    Row::new(values)
}

/// Build a count-only RecordBatch with no columns but the given row count.
///
/// Used by the COUNT(*) fast-path in both SharedTableProvider and UserTableProvider
/// when projection is empty.
pub fn build_count_only_batch(count: usize) -> Result<RecordBatch, KalamDbError> {
    let empty_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(Vec::<
        datafusion::arrow::datatypes::Field,
    >::new()));
    if count == 0 {
        return Ok(RecordBatch::new_empty(empty_schema));
    }
    let options =
        datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(count));
    RecordBatch::try_new_with_options(empty_schema, vec![], &options).map_err(|e| {
        KalamDbError::InvalidOperation(format!("Failed to build count-only batch: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[derive(Clone)]
    struct TestScanRow {
        row:     Row,
        deleted: bool,
    }

    impl ScanRow for TestScanRow {
        fn row(&self) -> &Row {
            &self.row
        }

        fn into_row(self) -> Row {
            self.row
        }

        fn seq_value(&self) -> i64 {
            1
        }

        fn commit_seq_value(&self) -> u64 {
            1
        }

        fn deleted_flag(&self) -> bool {
            self.deleted
        }
    }

    #[test]
    fn cached_empty_manifest_is_hot_only() {
        let table_id = TableId::new(NamespaceId::new("ns"), TableName::new("t"));
        let entry = ManifestCacheEntry::new(
            Manifest::new(table_id, None),
            None,
            0,
            kalamdb_system::SyncState::InSync,
        );
        assert!(cached_manifest_is_hot_only(Some(&entry)));
    }

    #[test]
    fn missing_manifest_is_not_hot_only() {
        assert!(!cached_manifest_is_hot_only(None));
    }

    #[test]
    fn visible_hot_entry_hides_latest_tombstone() {
        let tombstone = TestScanRow {
            row:     Row::from_vec(vec![]),
            deleted: true,
        };

        assert!(visible_hot_entry(Some((1_u64, tombstone))).is_none());
    }

    #[test]
    fn visible_hot_entry_keeps_latest_live_row() {
        let live = TestScanRow {
            row:     Row::from_vec(vec![]),
            deleted: false,
        };

        assert!(visible_hot_entry(Some((1_u64, live))).is_some());
    }

    #[test]
    fn compute_metadata_only_cold_columns_returns_pk_and_mvcc_columns() {
        let columns = compute_metadata_only_cold_columns("id");

        assert_eq!(
            columns,
            vec![
                "id".to_string(),
                SystemColumnNames::SEQ.to_string(),
                SystemColumnNames::COMMIT_SEQ.to_string(),
                SystemColumnNames::DELETED.to_string(),
            ]
        );
    }

    #[test]
    fn compute_cold_columns_adds_pk_and_system_columns_to_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        let projection = vec![1usize];
        let columns = compute_cold_columns(Some(&projection), &schema, "id")
            .expect("projection should produce cold columns");

        assert!(columns.iter().any(|column| column == "id"));
        assert!(columns.iter().any(|column| column == "name"));
        assert!(columns.iter().any(|column| column == SystemColumnNames::SEQ));
        assert!(columns.iter().any(|column| column == SystemColumnNames::COMMIT_SEQ));
        assert!(columns.iter().any(|column| column == SystemColumnNames::DELETED));
    }
}
