//! User table provider implementation with RLS
//!
//! This module provides UserTableProvider implementing BaseTableProvider<UserTableRowId, UserTableRow>
//! with Row-Level Security (RLS) enforced via user_id parameter.
//!
//! **Key Features**:
//! - Direct fields (no UserTableShared wrapper)
//! - Shared core via Arc<TableProviderCore>
//! - No handlers - all DML logic inline
//! - RLS via user_id parameter in DML methods
//! - SessionState extraction for scan_rows()
//! - PK Index for efficient row lookup (Phase 14)

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::manifest::manifest_helpers::{ensure_manifest_ready, load_row_from_parquet_by_seq};
use crate::user_tables::{UserTableIndexedStore, UserTablePkIndex, UserTableRow};
use crate::utils::base::{self, BaseTableProvider, TableProviderCore};
use crate::utils::row_utils::{extract_full_user_context, extract_user_context};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::arrow_json_conversion::coerce_rows;
use kalamdb_commons::ids::{SeqId, UserTableRowId};
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::UserId;
use kalamdb_commons::NotLeaderError;
use kalamdb_commons::StorageKey;
use kalamdb_session::{can_read_all_users, check_user_table_access, check_user_table_write_access};
use kalamdb_store::EntityStore;
use kalamdb_system::VectorMetric;
use kalamdb_vector::{
    new_indexed_user_vector_hot_store, UserVectorHotOpId, UserVectorHotStore, VectorHotOp,
    VectorHotOpType,
};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

// Arrow <-> JSON helpers
use crate::utils::version_resolution::{merge_versioned_rows, parquet_batch_to_rows};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::websocket::ChangeNotification;

/// User table provider with RLS
///
/// **Architecture**:
/// - Stateless provider (user context passed per-operation)
/// - Direct fields (no wrapper layer)
/// - Shared core via Arc<TableProviderCore> (holds schema, pk_name, column_defaults, non_null_columns)
/// - RLS enforced via user_id parameter
/// - PK Index for efficient row lookup (Phase 14)
pub struct UserTableProvider {
    /// Shared core (services, schema, pk_name, column_defaults, non_null_columns)
    core: Arc<TableProviderCore>,

    /// IndexedEntityStore with PK index for DML operations (public for flush jobs)
    pub(crate) store: Arc<UserTableIndexedStore>,

    /// PK index for efficient lookups
    pk_index: UserTablePkIndex,

    /// Embedding columns tracked by vector hot staging: (column_name, dimensions).
    vector_columns: Vec<(String, u32)>,

    /// Cached vector staging stores keyed by embedding column name.
    vector_stores: HashMap<String, Arc<UserVectorHotStore>>,
}

impl UserTableProvider {
    /// Create a new user table provider
    ///
    /// # Arguments
    /// * `core` - Shared core with services, schema, pk_name, etc.
    /// * `store` - IndexedEntityStore with PK index for this table
    pub fn new(core: Arc<TableProviderCore>, store: Arc<UserTableIndexedStore>) -> Self {
        let pk_index = UserTablePkIndex::new(core.table_id(), core.primary_key_field_name());
        let vector_columns: Vec<(String, u32)> = core
            .table_def()
            .columns
            .iter()
            .filter_map(|column| match &column.data_type {
                KalamDataType::Embedding(dim) if *dim > 0 => {
                    Some((column.column_name.clone(), *dim as u32))
                },
                _ => None,
            })
            .collect();
        let backend = store.backend().clone();
        let vector_stores: HashMap<String, Arc<UserVectorHotStore>> = vector_columns
            .iter()
            .map(|(column_name, _)| {
                (
                    column_name.clone(),
                    Arc::new(new_indexed_user_vector_hot_store(
                        backend.clone(),
                        core.table_id(),
                        column_name,
                    )),
                )
            })
            .collect();

        if log::log_enabled!(log::Level::Debug) {
            let field_names: Vec<_> = core.schema().fields().iter().map(|f| f.name()).collect();
            log::debug!(
                "UserTableProvider: Created for {} with schema fields: {:?}",
                core.table_id(),
                field_names
            );
        }

        Self {
            core,
            store,
            pk_index,
            vector_columns,
            vector_stores,
        }
    }

    /// Backward-compatible constructors that accept pk_field/column_defaults as separate args.
    /// These are used by production code and tests that still pass them separately.
    /// The values are expected to already be set in the core.
    pub fn try_new(
        core: Arc<TableProviderCore>,
        store: Arc<UserTableIndexedStore>,
        _primary_key_field_name: String,
    ) -> Result<Self, KalamDbError> {
        Ok(Self::new(core, store))
    }

    pub fn try_new_with_defaults(
        core: Arc<TableProviderCore>,
        store: Arc<UserTableIndexedStore>,
        _primary_key_field_name: String,
        _column_defaults: HashMap<String, Expr>,
    ) -> Result<Self, KalamDbError> {
        Ok(Self::new(core, store))
    }

    /// Get the primary key field name
    pub fn primary_key_field_name(&self) -> &str {
        self.core.primary_key_field_name()
    }

    /// Access the underlying indexed store (used by flush jobs)
    pub fn store(&self) -> Arc<UserTableIndexedStore> {
        Arc::clone(&self.store)
    }

    fn extract_embedding_vector(value: &ScalarValue, expected_dimensions: u32) -> Option<Vec<f32>> {
        let parse_inner = |array: &dyn Array| -> Option<Vec<f32>> {
            let float_array = array.as_any().downcast_ref::<Float32Array>()?;
            if float_array.len() != expected_dimensions as usize {
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

    async fn stage_vector_upsert(
        &self,
        user_id: &UserId,
        seq: SeqId,
        row: &Row,
    ) -> Result<(), KalamDbError> {
        if self.vector_columns.is_empty() {
            return Ok(());
        }

        let pk =
            crate::utils::unified_dml::extract_user_pk_value(row, self.primary_key_field_name())?;
        for (column_name, dimensions) in &self.vector_columns {
            let Some(value) = row.get(column_name.as_str()) else {
                continue;
            };
            let Some(vector) = Self::extract_embedding_vector(value, *dimensions) else {
                continue;
            };

            let store = self.vector_stores.get(column_name).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Missing cached vector store for column '{}'",
                    column_name
                ))
            })?;
            let key = UserVectorHotOpId::new(user_id.clone(), seq, pk.clone());
            let op = VectorHotOp::new(
                self.core.table_id().clone(),
                column_name.clone(),
                pk.clone(),
                VectorHotOpType::Upsert,
                Some(vector),
                None,
                *dimensions,
                VectorMetric::Cosine,
            );
            store.insert_async(key, op).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to stage vector upsert op: {}", e))
            })?;
        }

        Ok(())
    }

    async fn stage_vector_upsert_batch(
        &self,
        user_id: &UserId,
        entries: &[(UserTableRowId, UserTableRow)],
    ) -> Result<(), KalamDbError> {
        if self.vector_columns.is_empty() || entries.is_empty() {
            return Ok(());
        }

        let mut ops_by_column: HashMap<String, Vec<(UserVectorHotOpId, VectorHotOp)>> =
            HashMap::new();

        for (row_key, entity) in entries {
            let pk = crate::utils::unified_dml::extract_user_pk_value(
                &entity.fields,
                self.primary_key_field_name(),
            )?;

            for (column_name, dimensions) in &self.vector_columns {
                let Some(value) = entity.fields.get(column_name.as_str()) else {
                    continue;
                };
                let Some(vector) = Self::extract_embedding_vector(value, *dimensions) else {
                    continue;
                };

                ops_by_column.entry(column_name.clone()).or_default().push((
                    UserVectorHotOpId::new(user_id.clone(), row_key.seq, pk.clone()),
                    VectorHotOp::new(
                        self.core.table_id().clone(),
                        column_name.clone(),
                        pk.clone(),
                        VectorHotOpType::Upsert,
                        Some(vector),
                        None,
                        *dimensions,
                        VectorMetric::Cosine,
                    ),
                ));
            }
        }

        for (column_name, ops) in ops_by_column {
            let store = self.vector_stores.get(&column_name).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Missing cached vector store for column '{}'",
                    column_name
                ))
            })?;
            store.insert_batch_async(ops).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to batch stage vector upsert ops for column '{}': {}",
                    column_name, e
                ))
            })?;
        }

        Ok(())
    }

    async fn stage_vector_delete(
        &self,
        user_id: &UserId,
        seq: SeqId,
        pk: &str,
    ) -> Result<(), KalamDbError> {
        if self.vector_columns.is_empty() {
            return Ok(());
        }

        for (column_name, dimensions) in &self.vector_columns {
            let store = self.vector_stores.get(column_name).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Missing cached vector store for column '{}'",
                    column_name
                ))
            })?;
            let key = UserVectorHotOpId::new(user_id.clone(), seq, pk.to_string());
            let op = VectorHotOp::new(
                self.core.table_id().clone(),
                column_name.clone(),
                pk.to_string(),
                VectorHotOpType::Delete,
                None,
                None,
                *dimensions,
                VectorMetric::Cosine,
            );
            store.insert_async(key, op).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to stage vector delete op: {}", e))
            })?;
        }

        Ok(())
    }

    /// Build a complete Row from UserTableRow including system columns (_seq, _deleted)
    ///
    /// This ensures live query notifications include all columns, not just user-defined fields.
    fn build_notification_row(entity: &UserTableRow) -> Row {
        let mut values = entity.fields.values.clone();
        values.insert(
            SystemColumnNames::SEQ.to_string(),
            ScalarValue::Int64(Some(entity._seq.as_i64())),
        );
        values.insert(
            SystemColumnNames::DELETED.to_string(),
            ScalarValue::Boolean(Some(entity._deleted)),
        );
        Row::new(values)
    }

    /// Find a row by primary key value using the PK index
    ///
    /// Returns the latest non-deleted version of the row with the given PK.
    /// This is more efficient than scanning all rows.
    ///
    /// # Arguments
    /// * `user_id` - User scope for RLS
    /// * `pk_value` - Primary key value to search for
    ///
    /// # Returns
    /// Option<(UserTableRowId, UserTableRow)> if found
    async fn latest_hot_pk_entry(
        &self,
        user_id: &UserId,
        pk_value: &ScalarValue,
    ) -> Result<Option<(UserTableRowId, UserTableRow)>, KalamDbError> {
        let prefix = self.pk_index.build_prefix_for_pk(user_id, pk_value);
        self.store
            .get_latest_by_index_prefix_async(0, prefix)
            .await
            .into_kalamdb_error("PK index scan failed")
    }

    pub async fn find_by_pk(
        &self,
        user_id: &UserId,
        pk_value: &ScalarValue,
    ) -> Result<Option<(UserTableRowId, UserTableRow)>, KalamDbError> {
        Ok(self.latest_hot_pk_entry(user_id, pk_value).await?.and_then(|(row_id, row)| {
            if row._deleted {
                None
            } else {
                Some((row_id, row))
            }
        }))
    }

    /// Returns true if the latest hot-storage version of this PK is a tombstone
    /// (`_deleted = true`).  Returns false if the PK is absent from hot storage
    /// or if the latest version is active.
    ///
    /// Used in the PK fast-path of `scan_rows` to prevent cold storage (Parquet)
    /// from surfacing a row that has already been deleted in hot storage.
    async fn pk_tombstoned_in_hot(
        &self,
        user_id: &UserId,
        pk_value: &ScalarValue,
    ) -> Result<bool, KalamDbError> {
        Ok(self
            .latest_hot_pk_entry(user_id, pk_value)
            .await?
            .map(|(_, row)| row._deleted)
            .unwrap_or(false))
    }

    /// Scan Parquet files from cold storage for a specific user (async version).
    ///
    /// Lists all *.parquet files in the user's storage directory and merges them into a single RecordBatch.
    /// Returns an empty batch if no Parquet files exist.
    ///
    /// **Phase 4 (US6, T082-T084)**: Integrated with ManifestService for manifest caching.
    /// Logs cache hits/misses and updates last_accessed timestamp. Full query optimization
    /// (batch file pruning based on manifest metadata) implemented in Phase 5 (US2, T119-T123).
    async fn scan_parquet_files_as_batch_async(
        &self,
        user_id: &UserId,
        filter: Option<&Expr>,
    ) -> Result<RecordBatch, KalamDbError> {
        base::scan_parquet_files_as_batch_async(
            &self.core,
            self.core.table_id(),
            self.core.table_type(),
            Some(user_id),
            self.schema_ref(),
            filter,
        )
        .await
    }

    /// Async version of scan_all_users_with_version_resolution to avoid blocking the async runtime.
    async fn scan_all_users_with_version_resolution_async(
        &self,
        filter: Option<&Expr>,
        limit: Option<usize>,
        keep_deleted: bool,
        fallback_user_id: Option<&UserId>,
    ) -> Result<Vec<(UserTableRowId, UserTableRow)>, KalamDbError> {
        use kalamdb_store::EntityStoreAsync;

        let table_id = self.core.table_id();
        base::warn_if_unfiltered_scan(table_id, filter, limit, self.core.table_type());

        let scan_limit = base::calculate_scan_limit(limit);
        // Use async version to avoid blocking the runtime
        let hot_rows = self
            .store
            .scan_typed_with_prefix_and_start_async(None, None, scan_limit)
            .await
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to scan user table hot storage: {}",
                    e
                ))
            })?;

        let mut user_ids = HashSet::new();
        for (_row_id, row) in &hot_rows {
            user_ids.insert(row.user_id.clone());
        }

        if let Ok(scopes) = self.core.services.manifest_service.get_manifest_user_ids(table_id) {
            user_ids.extend(scopes);
        }

        if let Some(user_id) = fallback_user_id {
            user_ids.insert(user_id.clone());
        }

        let mut cold_rows = Vec::new();
        for user_id in user_ids {
            // Use async version to avoid blocking the runtime
            let parquet_batch = self.scan_parquet_files_as_batch_async(&user_id, filter).await?;
            for row_data in parquet_batch_to_rows(&parquet_batch)? {
                let seq_id = row_data.seq_id;
                let row = UserTableRow {
                    user_id: user_id.clone(),
                    _seq: seq_id,
                    _deleted: row_data.deleted,
                    fields: row_data.fields,
                };
                cold_rows.push((UserTableRowId::new(user_id.clone(), seq_id), row));
            }
        }

        let pk_name = self.primary_key_field_name().to_string();
        let mut result = merge_versioned_rows(&pk_name, hot_rows, cold_rows, keep_deleted);
        base::apply_limit(&mut result, limit);

        Ok(result)
    }

    async fn collect_matching_rows_for_subject(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> DataFusionResult<Vec<Row>> {
        crate::utils::datafusion_dml::collect_matching_rows(self, state, filters).await
    }
}

#[async_trait]
impl BaseTableProvider<UserTableRowId, UserTableRow> for UserTableProvider {
    fn core(&self) -> &base::TableProviderCore {
        &self.core
    }

    fn construct_row_from_parquet_data(
        &self,
        user_id: &UserId,
        row_data: &crate::utils::version_resolution::ParquetRowData,
    ) -> Result<Option<(UserTableRowId, UserTableRow)>, KalamDbError> {
        let row_key = UserTableRowId::new(user_id.clone(), row_data.seq_id);
        let row = UserTableRow {
            user_id: user_id.clone(),
            _seq: row_data.seq_id,
            _deleted: row_data.deleted,
            fields: row_data.fields.clone(),
        };
        Ok(Some((row_key, row)))
    }

    /// Override find_row_key_by_id_field to use PK index for efficient lookup
    ///
    /// This avoids scanning all rows and instead uses the secondary index.
    /// For hot storage (RocksDB), uses fast existence check. If not found in hot storage,
    /// falls back to checking cold storage using manifest-based pruning.
    ///
    /// OPTIMIZED: Uses `pk_exists_in_hot` for fast hot-path check (single index lookup + 1 entity fetch max).
    /// OPTIMIZED: Uses `pk_exists_in_cold` with manifest-based segment pruning for cold storage.
    async fn find_row_key_by_id_field(
        &self,
        user_id: &UserId,
        id_value: &str,
    ) -> Result<Option<UserTableRowId>, KalamDbError> {
        // Use shared helper to parse PK value
        let pk_value = crate::utils::pk::parse_pk_value(id_value);

        if let Some((row_id, row)) = self.latest_hot_pk_entry(user_id, &pk_value).await? {
            if row._deleted {
                log::trace!("[UserTableProvider] PK {} latest hot version is tombstoned", id_value);
                return Ok(None);
            }
            log::trace!("[UserTableProvider] PK collision in hot storage: id={}", id_value);
            return Ok(Some(row_id));
        }

        log::trace!("[UserTableProvider] PK {} not in hot storage, checking cold", id_value);

        // Not found in hot storage - check cold storage using optimized manifest-based lookup
        // This uses column_stats to prune segments that can't contain the PK
        let pk_name = self.primary_key_field_name();
        let pk_column_id = self.core.primary_key_column_id();
        let exists_in_cold = base::pk_exists_in_cold(
            &self.core,
            self.core.table_id(),
            self.core.table_type(),
            Some(user_id),
            pk_name,
            pk_column_id,
            id_value,
        )
        .await?;

        if exists_in_cold {
            log::trace!("[UserTableProvider] PK {} exists in cold storage", id_value);
            // Return a sentinel key to signal existence in cold storage.
            // Callers that only check `is_some()` (PK uniqueness guards) will reject duplicates.
            return Ok(Some(UserTableRowId::new(user_id.clone(), SeqId::new(0))));
        }

        Ok(None)
    }

    async fn insert(
        &self,
        user_id: &UserId,
        row_data: Row,
    ) -> Result<UserTableRowId, KalamDbError> {
        let span = tracing::debug_span!(
            "table.insert",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            column_count = row_data.values.len()
        );
        async move {
            ensure_manifest_ready(
                &self.core,
                self.core.table_type(),
                Some(user_id),
                "UserTableProvider",
            )?;

        // Validate PRIMARY KEY uniqueness if user provided PK value
        base::ensure_unique_pk_value(self, Some(user_id), &row_data).await?;

        // Generate new SeqId via SystemColumnsService
        let sys_cols = self.core.services.system_columns.clone();
        let seq_id = sys_cols.generate_seq_id().map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;

        // Create UserTableRow directly
        let entity = UserTableRow {
            user_id: user_id.clone(),
            _seq: seq_id,
            _deleted: false,
            fields: row_data,
        };

        // Create composite key
        let row_key = UserTableRowId::new(user_id.clone(), seq_id);

        // log::info!("🔍 [AS_USER_DEBUG] Inserting row for user_id='{}' _seq={}",
        //            user_id.as_str(), seq_id);

        // Store the entity in RocksDB (hot storage) with PK index maintenance
        self.store.insert_async(row_key.clone(), entity.clone()).await.map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to insert user table row: {}", e))
        })?;

        log::debug!("Inserted user table row for user {} with _seq {}", user_id.as_str(), seq_id);

        if let Err(e) = self.stage_vector_upsert(user_id, seq_id, &entity.fields).await {
            log::warn!(
                "Failed to stage vector upsert for table={}, user={}, seq={}: {}",
                self.core.table_id(),
                user_id.as_str(),
                seq_id.as_i64(),
                e
            );
        }

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id)) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        // Fire live query + topic notification (INSERT)
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(Some(&user_id), &table_id);
        if has_topics || has_live_subs {
            // Build complete row including system columns (_seq, _deleted)
            let row = Self::build_notification_row(&entity);
            if has_topics {
                self.core
                    .publish_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Insert,
                        &row,
                        Some(&user_id),
                    )
                    .await;
            }
            if has_live_subs {
                let notification = ChangeNotification::insert(table_id.clone(), row);
                notification_service.notify_table_change(
                    Some(user_id.clone()),
                    table_id,
                    notification,
                );
            }
        }

            Ok(row_key)
        }
        .instrument(span)
        .await
    }

    /// Optimized batch insert using single RocksDB WriteBatch
    ///
    /// **Performance**: This method is significantly faster than calling insert() N times:
    /// - Single mutex acquisition for all SeqId generation
    /// - Single RocksDB WriteBatch for all rows (one disk write vs N)
    /// - Batch PK validation (single scan + HashSet lookup instead of N individual checks)
    ///
    /// # Arguments
    /// * `user_id` - Subject user ID for RLS
    /// * `rows` - Vector of Row objects to insert
    ///
    /// # Returns
    /// Vector of generated UserTableRowIds
    async fn insert_batch(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
    ) -> Result<Vec<UserTableRowId>, KalamDbError> {
        let row_count = rows.len();
        let span = tracing::debug_span!(
            "table.insert_batch",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            row_count
        );
        async move {
            if rows.is_empty() {
                return Ok(Vec::new());
            }

        // Ensure manifest is ready
        ensure_manifest_ready(
            &self.core,
            self.core.table_type(),
            Some(user_id),
            "UserTableProvider",
        )?;

        // Coerce rows to match schema types (e.g. String -> Timestamp)
        let coerced_rows = {
            let _coerce_span = tracing::debug_span!("insert_batch.coerce_rows").entered();
            coerce_rows(rows, &self.schema_ref()).map_err(|e| {
                KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
            })?
        };

        // VALIDATE NOT NULL CONSTRAINTS (per ADR-016: must occur before any RocksDB write)
        crate::utils::datafusion_dml::validate_not_null_with_set(
            self.core.non_null_columns(),
            &coerced_rows,
        )
        .map_err(|e| KalamDbError::ConstraintViolation(e.to_string()))?;

        let row_count = coerced_rows.len();

        // Batch PK validation: collect all user-provided PK values and their prefixes
        tracing::debug!(row_count, "insert_batch.pk_validation start");
        let pk_name = self.primary_key_field_name();
        let mut pk_values_to_check: Vec<(String, ScalarValue)> = Vec::new();
        let mut seen_batch_pks = HashSet::new();
        {
            for row_data in &coerced_rows {
                if let Some(pk_value) = row_data.get(pk_name) {
                    if !matches!(pk_value, ScalarValue::Null) {
                        let pk_str =
                            crate::utils::unified_dml::extract_user_pk_value(row_data, pk_name)?;
                        if !seen_batch_pks.insert(pk_str.clone()) {
                            return Err(KalamDbError::AlreadyExists(format!(
                                "Primary key violation: value '{}' appears multiple times in the insert batch for column '{}'",
                                pk_str, pk_name
                            )));
                        }
                        pk_values_to_check.push((pk_str, pk_value.clone()));
                    }
                }
            }

            // Check only the latest hot version for each requested PK. Tombstoned
            // latest versions are reusable and should not fail the insert.
            if !pk_values_to_check.is_empty() {
                for (pk_str, pk_value) in &pk_values_to_check {
                    if let Some((_row_id, row)) =
                        self.latest_hot_pk_entry(user_id, pk_value).await?
                    {
                        if !row._deleted {
                            return Err(KalamDbError::AlreadyExists(format!(
                                "Primary key violation: value '{}' already exists in column '{}'",
                                pk_str, pk_name
                            )));
                        }
                    }
                }

                let pk_column_id = self.core.primary_key_column_id();
                let pk_values: Vec<String> =
                    pk_values_to_check.iter().map(|(pk, _)| pk.clone()).collect();
                if let Some(found_pk) = base::pk_exists_batch_in_cold(
                    &self.core,
                    self.core.table_id(),
                    self.core.table_type(),
                    Some(user_id),
                    pk_name,
                    pk_column_id,
                    &pk_values,
                )
                .await?
                {
                    return Err(KalamDbError::AlreadyExists(format!(
                        "Primary key violation: value '{}' already exists in column '{}'",
                        found_pk, pk_name
                    )));
                }
            }
        }
        tracing::debug!(row_count, "insert_batch.pk_validation done");

        // Generate all SeqIds in single mutex acquisition
        let sys_cols = self.core.services.system_columns.clone();
        let seq_ids = sys_cols.generate_seq_ids(row_count).map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId batch generation failed: {}", e))
        })?;

        // Build all entities and keys
        let mut user_rows: Vec<UserTableRow> = Vec::with_capacity(row_count);
        let mut row_keys: Vec<UserTableRowId> = Vec::with_capacity(row_count);

        for (row_data, seq_id) in coerced_rows.into_iter().zip(seq_ids.into_iter()) {
            row_keys.push(UserTableRowId::new(user_id.clone(), seq_id));
            user_rows.push(UserTableRow {
                user_id: user_id.clone(),
                _seq: seq_id,
                _deleted: false,
                fields: row_data,
            });
        }

        // Batch-encode all rows with FlatBufferBuilder reuse, then write atomically.
        // This reuses the inner FlatBufferBuilder across rows (reset() retains capacity),
        // saving N-1 builder allocations and eliminating per-row .to_vec() copies.
        let encoded_values = {
            let _encode_span = tracing::debug_span!("insert_batch.encode", row_count).entered();
            kalamdb_commons::serialization::row_codec::batch_encode_user_table_rows(&user_rows)
                .map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to batch encode user table rows: {}",
                        e
                    ))
                })?
        };

        // Combine keys + entities for index key extraction
        let entries: Vec<(UserTableRowId, UserTableRow)> =
            row_keys.iter().cloned().zip(user_rows.into_iter()).collect();

        self.store
            .insert_batch_preencoded_async(entries.clone(), encoded_values)
            .await
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to batch insert user table rows: {}",
                    e
                ))
            })?;

        if let Err(e) = self.stage_vector_upsert_batch(user_id, &entries).await {
            log::warn!(
                "Failed to batch stage vector upserts for table={}, user={}: {}",
                self.core.table_id(),
                user_id.as_str(),
                e
            );
        }

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id)) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        log::debug!(
            "Batch inserted {} user table rows for user {} with _seq range [{}, {}]",
            row_count,
            user_id.as_str(),
            row_keys.first().map(|k| k.seq.as_i64()).unwrap_or(0),
            row_keys.last().map(|k| k.seq.as_i64()).unwrap_or(0)
        );

        // Fire live query + topic notifications (one per row)
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();
        log::debug!(
            "UserTableProvider::insert_batch: Sending {} notifications for user={}, table={}",
            entries.len(),
            user_id.as_str(),
            table_id
        );

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(Some(&user_id), &table_id);
        if has_topics || has_live_subs {
            // Build notification rows
            let rows: Vec<_> = entries
                .iter()
                .map(|(_row_key, entity)| Self::build_notification_row(entity))
                .collect();

            // Batch publish to topics (single RocksDB WriteBatch + single lock per partition)
            if has_topics {
                self.core
                    .publish_batch_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Insert,
                        &rows,
                        Some(&user_id),
                    )
                    .await;
            }
            if has_live_subs {
                for row in rows {
                    let notification = ChangeNotification::insert(table_id.clone(), row);
                    notification_service.notify_table_change(
                        Some(user_id.clone()),
                        table_id.clone(),
                        notification,
                    );
                }
            }
        }

            Ok(row_keys)
        }
        .instrument(span)
        .await
    }

    async fn update(
        &self,
        user_id: &UserId,
        key: &UserTableRowId,
        updates: Row,
    ) -> Result<UserTableRowId, KalamDbError> {
        // Load referenced version to extract PK, then delegate to update_by_pk_value
        let prior_opt = self.store.get(key).into_kalamdb_error("Failed to load prior version")?;

        let prior = if let Some(p) = prior_opt {
            p
        } else {
            load_row_from_parquet_by_seq(
                &self.core,
                self.core.table_type(),
                self.core.schema(),
                Some(user_id),
                key.seq,
                |row_data| UserTableRow {
                    user_id: user_id.clone(),
                    _seq: row_data.seq_id,
                    _deleted: row_data.deleted,
                    fields: row_data.fields,
                },
            )
            .await?
            .ok_or_else(|| KalamDbError::NotFound("Row not found for update".to_string()))?
        };

        let pk_name = self.primary_key_field_name().to_string();
        let pk_value_scalar = prior.fields.get(&pk_name).cloned().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Prior row missing PK {}", pk_name))
        })?;

        // Validate PK update (check if new PK value already exists) — only needed when updating by key
        base::validate_pk_update(self, Some(user_id), &updates, &pk_value_scalar).await?;

        // Delegate to the canonical implementation
        let pk_value_str = pk_value_scalar.to_string();
        self.update_by_pk_value(user_id, &pk_value_str, updates).await
    }

    async fn update_by_pk_value(
        &self,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<UserTableRowId, KalamDbError> {
        let span = tracing::debug_span!(
            "table.update",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            pk = pk_value,
            update_columns = updates.values.len()
        );
        async move {
            let pk_name = self.primary_key_field_name().to_string();

        // Get PK column data type from schema for proper type coercion
        let schema = self.schema();
        let pk_field = schema.field_with_name(&pk_name).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "PK column '{}' not found in schema: {}",
                pk_name, e
            ))
        })?;
        let pk_column_type = pk_field.data_type();

        // Convert string PK value to proper ScalarValue based on column type
        use kalamdb_commons::conversions::parse_string_as_scalar;
        let pk_value_scalar = parse_string_as_scalar(pk_value, pk_column_type)
            .map_err(|e| KalamDbError::InvalidOperation(e))?;

        // Find latest resolved row for this PK under same user
        // First try hot storage (O(1) via PK index), then fall back to cold storage (Parquet scan)
        let (_latest_key, latest_row) =
            if let Some(result) = self.find_by_pk(user_id, &pk_value_scalar).await? {
                result
            } else if self.pk_tombstoned_in_hot(user_id, &pk_value_scalar).await? {
                return Err(KalamDbError::NotFound(format!(
                    "Row with {}={} was deleted",
                    pk_name, pk_value
                )));
            } else {
                // Not in hot storage, check cold storage
                log::debug!(
                "[UPDATE] PK {} not found in hot storage, querying cold storage for user={}, pk={}",
                pk_name,
                user_id.as_str(),
                pk_value
            );
                base::find_row_by_pk(self, Some(user_id), pk_value).await?.ok_or_else(|| {
                    KalamDbError::NotFound(format!(
                        "Row with {}={} not found (checked both hot and cold storage)",
                        pk_name, pk_value
                    ))
                })?
            };

        // Merge updates onto latest
        let mut merged = latest_row.fields.values.clone();
        for (k, v) in &updates.values {
            merged.insert(k.clone(), v.clone());
        }

        let new_fields = Row::new(merged);

        // VALIDATE NOT NULL CONSTRAINTS on the merged row (per ADR-016)
        crate::utils::datafusion_dml::validate_not_null_with_set(
            self.core.non_null_columns(),
            &[new_fields.clone()],
        )
        .map_err(|e| KalamDbError::ConstraintViolation(e.to_string()))?;

        let sys_cols = self.core.services.system_columns.clone();
        let seq_id = sys_cols.generate_seq_id().map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;
        let entity = UserTableRow {
            user_id: user_id.clone(),
            _seq: seq_id,
            _deleted: false,
            fields: new_fields,
        };
        let row_key = UserTableRowId::new(user_id.clone(), seq_id);
        // Insert new version (MVCC - all writes are inserts with new SeqId)
        self.store.insert_async(row_key.clone(), entity.clone()).await.map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to update user table row: {}", e))
        })?;

        if let Err(e) = self.stage_vector_upsert(user_id, seq_id, &entity.fields).await {
            log::warn!(
                "Failed to stage vector upsert for table={}, user={}, seq={}: {}",
                self.core.table_id(),
                user_id.as_str(),
                seq_id.as_i64(),
                e
            );
        }

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id)) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        // Fire live query + topic notification (UPDATE)
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(Some(&user_id), &table_id);
        if has_topics || has_live_subs {
            let new_row = Self::build_notification_row(&entity);
            if has_topics {
                self.core
                    .publish_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Update,
                        &new_row,
                        Some(&user_id),
                    )
                    .await;
            }
            if has_live_subs {
                let old_row = Self::build_notification_row(&latest_row);
                let pk_col = self.primary_key_field_name().to_string();
                let notification =
                    ChangeNotification::update(table_id.clone(), old_row, new_row, vec![pk_col]);
                notification_service.notify_table_change(
                    Some(user_id.clone()),
                    table_id,
                    notification,
                );
            }
        }
            Ok(row_key)
        }
        .instrument(span)
        .await
    }

    async fn delete(&self, user_id: &UserId, key: &UserTableRowId) -> Result<(), KalamDbError> {
        // Load referenced version to extract PK, then delegate to delete_by_pk_value
        let prior_opt = self.store.get(key).into_kalamdb_error("Failed to load prior version")?;

        let prior = if let Some(p) = prior_opt {
            p
        } else {
            load_row_from_parquet_by_seq(
                &self.core,
                self.core.table_type(),
                self.core.schema(),
                Some(user_id),
                key.seq,
                |row_data| UserTableRow {
                    user_id: user_id.clone(),
                    _seq: row_data.seq_id,
                    _deleted: row_data.deleted,
                    fields: row_data.fields,
                },
            )
            .await?
            .ok_or_else(|| KalamDbError::NotFound("Row not found for delete".to_string()))?
        };

        let pk_name = self.primary_key_field_name().to_string();
        let pk_value_scalar = prior.fields.get(&pk_name).cloned().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Prior row missing PK {}", pk_name))
        })?;
        let pk_value_str = pk_value_scalar.to_string();

        self.delete_by_pk_value(user_id, &pk_value_str).await?;
        Ok(())
    }

    async fn delete_by_pk_value(
        &self,
        user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        let span = tracing::debug_span!(
            "table.delete",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            pk = pk_value
        );
        async move {
            let pk_name = self.primary_key_field_name().to_string();
        let schema = self.schema();
        let pk_field = schema.field_with_name(&pk_name).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "PK column '{}' not found in schema: {}",
                pk_name, e
            ))
        })?;
        let pk_column_type = pk_field.data_type();
        let pk_value_scalar =
            kalamdb_commons::conversions::parse_string_as_scalar(pk_value, pk_column_type)
                .map_err(KalamDbError::InvalidOperation)?;

        // Find latest resolved row for this PK under same user
        // First try hot storage (O(1) via PK index), then fall back to cold storage (Parquet scan)
        let latest_row =
            if let Some((_key, row)) = self.find_by_pk(user_id, &pk_value_scalar).await? {
                row
            } else if self.pk_tombstoned_in_hot(user_id, &pk_value_scalar).await? {
                return Ok(false);
            } else {
                // Not in hot storage, check cold storage
                match base::find_row_by_pk(self, Some(user_id), pk_value).await? {
                    Some((_key, row)) => row,
                    None => {
                        log::trace!(
                            "[UserProvider DELETE_BY_PK] Row with {}={} not found",
                            pk_name,
                            pk_value
                        );
                        return Ok(false);
                    },
                }
            };

        let sys_cols = self.core.services.system_columns.clone();
        let seq_id = sys_cols.generate_seq_id().map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;

        // Preserve ALL fields in the tombstone so they can be queried if _deleted=true
        // This allows "undo" functionality and auditing of deleted records
        let values = latest_row.fields.values.clone();

        let entity = UserTableRow {
            user_id: user_id.clone(),
            _seq: seq_id,
            _deleted: true,
            fields: Row::new(values),
        };
        let row_key = UserTableRowId::new(user_id.clone(), seq_id);
        log::debug!(
            "[UserProvider DELETE_BY_PK] Writing tombstone: user={}, pk={}, _seq={}",
            user_id.as_str(),
            pk_value,
            seq_id.as_i64()
        );
        // Insert tombstone version (MVCC - all writes are inserts with new SeqId)
        self.store.insert_async(row_key.clone(), entity.clone()).await.map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to delete user table row: {}", e))
        })?;

        if let Err(e) = self.stage_vector_delete(user_id, seq_id, pk_value).await {
            log::warn!(
                "Failed to stage vector delete for table={}, user={}, seq={}, pk={}: {}",
                self.core.table_id(),
                user_id.as_str(),
                seq_id.as_i64(),
                pk_value,
                e
            );
        }

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id)) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        // Fire live query + topic notification (DELETE soft)
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(Some(&user_id), &table_id);
        if has_topics || has_live_subs {
            // Provide tombstone entity with system columns for filter matching
            let row = Self::build_notification_row(&entity);
            if has_topics {
                self.core
                    .publish_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Delete,
                        &row,
                        Some(&user_id),
                    )
                    .await;
            }
            if has_live_subs {
                let notification = ChangeNotification::delete_soft(table_id.clone(), row);
                notification_service.notify_table_change(
                    Some(user_id.clone()),
                    table_id,
                    notification,
                );
            }
        }
            Ok(true)
        }
        .instrument(span)
        .await
    }

    async fn scan_rows(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, KalamDbError> {
        // Extract user_id and role from SessionState for RLS
        let (user_id, role) = extract_user_context(state)?;
        let allow_all_users = can_read_all_users(role);

        let schema = self.schema_ref();
        let pk_name = self.primary_key_field_name();

        // ── PK equality fast-path ────────────────────────────────────────────
        // If the filter is `pk_col = <literal>`, use the PK index for O(1)
        // lookup instead of full table scan + MVCC resolution.
        // Only for single-user scope (not admin cross-user queries).
        if !allow_all_users {
            if let Some(expr) = filter {
                if let Some(pk_literal) = base::extract_pk_equality_literal(expr, pk_name) {
                    let pk_field = schema.field_with_name(pk_name).ok();
                    let pk_scalar = if let Some(field) = pk_field {
                        kalamdb_commons::conversions::parse_string_as_scalar(
                            &pk_literal.to_string(),
                            field.data_type(),
                        )
                        .ok()
                        .unwrap_or(pk_literal)
                    } else {
                        pk_literal
                    };

                    // Hot storage PK index (O(1))
                    let found = self.find_by_pk(user_id, &pk_scalar).await?;
                    if let Some((row_id, row)) = found {
                        log::debug!(
                            "[UserProvider] PK fast-path hit for {}={}, user={}",
                            pk_name,
                            pk_scalar,
                            user_id.as_str()
                        );
                        return crate::utils::base::rows_to_arrow_batch(
                            &schema,
                            vec![(row_id, row)],
                            projection,
                            |_, _| {},
                        );
                    }

                    // Check if PK is tombstoned (deleted) in hot storage.
                    // If so, the row has been deleted — do NOT fall back to cold storage.
                    // Returning the Parquet version would surface a row whose latest
                    // version is a tombstone, violating MVCC visibility rules.
                    if self.pk_tombstoned_in_hot(user_id, &pk_scalar).await? {
                        log::debug!(
                            "[UserProvider] PK fast-path tombstone for {}={}, user={}",
                            pk_name,
                            pk_scalar,
                            user_id.as_str()
                        );
                        return crate::utils::base::rows_to_arrow_batch(
                            &schema,
                            Vec::<(UserTableRowId, UserTableRow)>::new(),
                            projection,
                            |_, _| {},
                        );
                    }

                    // Cold storage fallback (PK absent from hot storage entirely)
                    let cold_found =
                        base::find_row_by_pk(self, Some(user_id), &pk_scalar.to_string()).await?;
                    if let Some((row_id, row)) = cold_found {
                        log::debug!(
                            "[UserProvider] PK fast-path cold hit for {}={}, user={}",
                            pk_name,
                            pk_scalar,
                            user_id.as_str()
                        );
                        return crate::utils::base::rows_to_arrow_batch(
                            &schema,
                            vec![(row_id, row)],
                            projection,
                            |_, _| {},
                        );
                    }

                    // PK not found — return empty batch
                    return crate::utils::base::rows_to_arrow_batch(
                        &schema,
                        Vec::<(UserTableRowId, UserTableRow)>::new(),
                        projection,
                        |_, _| {},
                    );
                }
            }
        }

        // ── Full scan path (no PK equality filter or admin cross-user) ──────
        // Extract sequence bounds from filter to optimize RocksDB scan
        let (since_seq, _until_seq) = if let Some(expr) = filter {
            base::extract_seq_bounds_from_filter(expr)
        } else {
            (None, None)
        };

        // Privileged roles can scan across all users for read access; others remain scoped to
        // their own user_id for RLS.
        let keep_deleted = filter.map(base::filter_uses_deleted_column).unwrap_or(false);

        let kvs = if allow_all_users {
            self.scan_all_users_with_version_resolution_async(
                filter,
                limit,
                keep_deleted,
                Some(user_id),
            )
            .await?
        } else {
            self.scan_with_version_resolution_to_kvs_async(
                user_id,
                filter,
                since_seq,
                limit,
                keep_deleted,
            )
            .await?
        };

        let table_id = self.core.table_id();
        log::trace!(
            "[UserTableProvider] scan_rows resolved {} row(s) for user={} role={:?} table={}",
            kvs.len(),
            user_id.as_str(),
            role,
            table_id
        );

        // Convert rows to JSON values aligned with schema
        crate::utils::base::rows_to_arrow_batch(&schema, kvs, projection, |_, _| {})
    }

    async fn scan_with_version_resolution_to_kvs_async(
        &self,
        user_id: &UserId,
        filter: Option<&Expr>,
        since_seq: Option<kalamdb_commons::ids::SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
    ) -> Result<Vec<(UserTableRowId, UserTableRow)>, KalamDbError> {
        use kalamdb_store::EntityStoreAsync;

        let table_id = self.core.table_id();

        // Warn if no filter or limit - potential performance issue
        base::warn_if_unfiltered_scan(table_id, filter, limit, self.core.table_type());

        // 1) Scan hot storage (RocksDB) scoped to the user using a storekey prefix.
        let user_prefix = UserTableRowId::user_prefix(user_id);

        // Construct start_key if since_seq is provided
        let start_key_bytes = if let Some(seq) = since_seq {
            // since_seq is exclusive, so start at seq + 1
            let start_seq = kalamdb_commons::ids::SeqId::from(seq.as_i64() + 1);
            let key = UserTableRowId::new(user_id.clone(), start_seq);
            Some(key.storage_key())
        } else {
            None
        };

        // Calculate scan limit using common helper
        //  Need to scan more than requested limit because we'll filter by user_id
        let scan_limit = base::calculate_scan_limit(limit) * 10; // Buffer for filtering

        // Run hot storage (RocksDB) and cold storage (Parquet) scans concurrently
        let hot_future = self
            .store
            .scan_with_raw_prefix_async(&user_prefix, start_key_bytes.as_deref(), scan_limit);
        let cold_future = self.scan_parquet_files_as_batch_async(user_id, filter);

        let (hot_result, cold_result) = tokio::join!(hot_future, cold_future);

        let hot_rows = hot_result.map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to scan user table hot storage: {}",
                e
            ))
        })?;

        log::trace!(
            "[UserProvider] Hot scan: {} rows for user={} (table={})",
            hot_rows.len(),
            user_id.as_str(),
            table_id
        );

        // 2) Scan cold storage (Parquet files)
        let parquet_batch = cold_result?;

        let cold_rows: Vec<(UserTableRowId, UserTableRow)> = parquet_batch_to_rows(&parquet_batch)?
            .into_iter()
            .map(|row_data| {
                let seq_id = row_data.seq_id;
                let row = UserTableRow {
                    user_id: user_id.clone(),
                    _seq: seq_id,
                    _deleted: row_data.deleted,
                    fields: row_data.fields,
                };
                (UserTableRowId::new(user_id.clone(), seq_id), row)
            })
            .collect();

        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "[UserProvider] Cold scan: {} Parquet rows (table={}; user={})",
                cold_rows.len(),
                table_id,
                user_id.as_str()
            );
        }

        // 3) Version resolution: keep MAX(_seq) per primary key; honor tombstones
        let pk_name = self.primary_key_field_name().to_string();
        let mut result = merge_versioned_rows(&pk_name, hot_rows, cold_rows, keep_deleted);

        // Apply limit after resolution using common helper
        base::apply_limit(&mut result, limit);

        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "[UserProvider] Final version-resolved (post-tombstone): {} rows (table={}; user={})",
                result.len(),
                table_id,
                user_id.as_str()
            );
        }

        Ok(result)
    }

    fn extract_row(row: &UserTableRow) -> &Row {
        &row.fields
    }
}

// Manual Debug to satisfy DataFusion's TableProvider: Debug bound
impl std::fmt::Debug for UserTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserTableProvider")
            .field("table_id", self.core.table_id())
            .field("table_type", &self.core.table_type())
            .field("primary_key_field_name", &self.core.primary_key_field_name())
            .finish()
    }
}

// Implement DataFusion TableProvider trait
#[async_trait]
impl TableProvider for UserTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema_ref()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.core.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.base_statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // SECURITY: Enforce user table access rules (namespace isolation)
        check_user_table_access(state, self.core.table_id()).map_err(DataFusionError::from)?;

        // Extract user context including read_context for leader check
        let (_user_id, _role, read_context) = extract_full_user_context(state).map_err(|e| {
            DataFusionError::Execution(format!("Failed to extract user context: {}", e))
        })?;

        // Check if this is a client read that requires leader
        // Skip check for internal reads (jobs, live query notifications, etc.)
        // Route to the Meta group leader where ALL DML data lives
        // (DML writes go through forward_sql_if_follower to Meta leader)
        if read_context.requires_leader()
            && self.core.services.cluster_coordinator.is_cluster_mode().await
        {
            let is_leader = self.core.services.cluster_coordinator.is_meta_leader().await;
            if !is_leader {
                // Get Meta leader address for client redirection
                let leader_addr = self.core.services.cluster_coordinator.meta_leader_addr().await;
                return Err(DataFusionError::External(Box::new(NotLeaderError::new(leader_addr))));
            }
        }

        self.base_scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!(table_id = %self.core.table_id(), "table.insert_into");
        check_user_table_write_access(state, self.core.table_id())
            .map_err(DataFusionError::from)?;

        if insert_op != InsertOp::Append {
            return Err(DataFusionError::Plan(format!(
                "{} is not supported for user tables",
                insert_op
            )));
        }

        let (user_id, _role) =
            extract_user_context(state).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let rows = crate::utils::datafusion_dml::collect_input_rows(state, input).await?;
        let inserted = self
            .insert_batch(user_id, rows)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        crate::utils::datafusion_dml::rows_affected_plan(state, inserted.len() as u64).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        check_user_table_write_access(state, self.core.table_id())
            .map_err(DataFusionError::from)?;
        crate::utils::datafusion_dml::validate_where_clause(&filters, "DELETE")?;

        let (user_id, _role) =
            extract_user_context(state).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let rows = self.collect_matching_rows_for_subject(state, &filters).await?;
        if rows.is_empty() {
            return crate::utils::datafusion_dml::rows_affected_plan(state, 0).await;
        }

        let pk_column = self.primary_key_field_name().to_string();
        let mut seen = HashSet::new();
        let mut deleted: u64 = 0;

        for row in rows {
            let pk_value = crate::utils::datafusion_dml::extract_pk_value(&row, &pk_column)?;
            if !seen.insert(pk_value.clone()) {
                continue;
            }

            if self
                .delete_by_pk_value(user_id, &pk_value)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
            {
                deleted += 1;
            }
        }

        crate::utils::datafusion_dml::rows_affected_plan(state, deleted).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        check_user_table_write_access(state, self.core.table_id())
            .map_err(DataFusionError::from)?;
        crate::utils::datafusion_dml::validate_where_clause(&filters, "UPDATE")?;

        let pk_column = self.primary_key_field_name().to_string();
        crate::utils::datafusion_dml::validate_update_assignments(&assignments, &pk_column)?;

        let (user_id, _role) =
            extract_user_context(state).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let rows = self.collect_matching_rows_for_subject(state, &filters).await?;
        if rows.is_empty() {
            return crate::utils::datafusion_dml::rows_affected_plan(state, 0).await;
        }

        let schema = self.schema_ref();
        let mut seen = HashSet::new();
        let mut updated: u64 = 0;

        for row in rows {
            let pk_value = crate::utils::datafusion_dml::extract_pk_value(&row, &pk_column)?;
            if !seen.insert(pk_value.clone()) {
                continue;
            }

            let mut values = BTreeMap::new();
            for (column, expr) in &assignments {
                let value = crate::utils::datafusion_dml::evaluate_assignment_expr(
                    state, &schema, &row, expr,
                )?;
                values.insert(column.clone(), value);
            }

            self.update_by_pk_value(user_id, &pk_value, Row::new(values))
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            updated += 1;
        }

        crate::utils::datafusion_dml::rows_affected_plan(state, updated).await
    }
}

// KalamTableProvider: extends TableProvider with KalamDB-specific DML
#[async_trait]
impl crate::utils::dml_provider::KalamTableProvider for UserTableProvider {
    async fn insert_rows(&self, user_id: &UserId, rows: Vec<Row>) -> Result<usize, KalamDbError> {
        let keys = self.insert_batch(user_id, rows).await?;
        Ok(keys.len())
    }

    async fn insert_rows_returning(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
    ) -> Result<Vec<ScalarValue>, KalamDbError> {
        let keys = self.insert_batch(user_id, rows).await?;
        Ok(keys.into_iter().map(|k| ScalarValue::Int64(Some(k.seq.as_i64()))).collect())
    }

    async fn update_row_by_pk(
        &self,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<bool, KalamDbError> {
        match self.update_by_pk_value(user_id, pk_value, updates).await {
            Ok(_) => Ok(true),
            Err(KalamDbError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn delete_row_by_pk(
        &self,
        user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        self.delete_by_pk_value(user_id, pk_value).await
    }
}
