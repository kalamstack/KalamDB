//! Shared table provider implementation without RLS
//!
//! This module provides SharedTableProvider implementing BaseTableProvider<SharedTableRowId, SharedTableRow>
//! for cross-user shared tables (no Row-Level Security).
//!
//! **Key Features**:
//! - Direct fields (no wrapper layer)
//! - Shared core via Arc<TableProviderCore>
//! - No handlers - all DML logic inline
//! - NO RLS - ignores user_id parameter (operates on all rows)
//! - SessionState NOT extracted in scan_rows() (scans all rows)
//! - PK Index: Uses SharedTableIndexedStore for efficient O(1) lookups by PK value

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::manifest::manifest_helpers::{ensure_manifest_ready, load_row_from_parquet_by_seq};
use crate::shared_tables::{SharedTableIndexedStore, SharedTablePkIndex, SharedTableRow};
use crate::utils::base::{self, BaseTableProvider, TableProviderCore};
use crate::utils::row_utils::extract_full_user_context;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::arrow_json_conversion::coerce_rows;
use kalamdb_commons::ids::SharedTableRowId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::websocket::ChangeNotification;
use kalamdb_commons::NotLeaderError;
use kalamdb_session::{check_shared_table_access, check_shared_table_write_access};
use kalamdb_store::EntityStore;
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

// Arrow <-> JSON helpers
use crate::utils::version_resolution::{merge_versioned_rows, parquet_batch_to_rows};

/// Shared table provider without RLS
///
/// **Architecture**:
/// - Stateless provider (user context passed but ignored)
/// - Direct fields (no wrapper layer)
/// - Shared core via Arc<TableProviderCore> (holds schema, pk_name, column_defaults, non_null_columns, table_def)
/// - NO RLS - user_id parameter ignored in all operations
/// - Uses SharedTableIndexedStore for efficient PK lookups
pub struct SharedTableProvider {
    /// Shared core (services, schema, pk_name, column_defaults, non_null_columns, table_def)
    core: Arc<TableProviderCore>,

    /// SharedTableIndexedStore for DML operations with PK index
    pub(crate) store: Arc<SharedTableIndexedStore>,

    /// PK index for efficient lookups
    pk_index: SharedTablePkIndex,
}

impl SharedTableProvider {
    /// Create a new shared table provider
    ///
    /// # Arguments
    /// * `core` - Shared core with services, schema, pk_name, table_def, etc.
    /// * `store` - SharedTableIndexedStore for this table
    pub fn new(core: Arc<TableProviderCore>, store: Arc<SharedTableIndexedStore>) -> Self {
        let pk_index = SharedTablePkIndex::new(core.table_id(), core.primary_key_field_name());

        Self {
            core,
            store,
            pk_index,
        }
    }

    /// Backward-compatible constructor that accepts pk_field/column_defaults as separate args.
    pub fn new_with_defaults(
        core: Arc<TableProviderCore>,
        store: Arc<SharedTableIndexedStore>,
        _primary_key_field_name: String,
        _column_defaults: HashMap<String, Expr>,
    ) -> Self {
        Self::new(core, store)
    }

    /// Build a complete Row for live query/topic notifications including system columns (_seq, _deleted)
    ///
    /// This ensures notifications include all columns, not just user-defined fields.
    fn build_notification_row(entity: &SharedTableRow) -> Row {
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

    /// Access the underlying indexed store (used by flush jobs)
    pub fn store(&self) -> Arc<SharedTableIndexedStore> {
        Arc::clone(&self.store)
    }

    /// Scan Parquet files from cold storage for shared table
    ///
    /// Lists all *.parquet files in the table's storage directory and merges them into a single RecordBatch.
    /// Returns an empty batch if no Parquet files exist.
    ///
    /// **Difference from user tables**: Shared tables have NO user_id partitioning,
    /// so all Parquet files are in the same directory (no subdirectories per user).
    ///
    /// **Phase 4 (US6, T082-T084)**: Integrated with ManifestService for manifest caching.
    /// Logs cache hits/misses and updates last_accessed timestamp. Full query optimization
    /// (batch file pruning based on manifest metadata) implemented in Phase 5 (US2, T119-T123).
    ///
    /// **Manifest-Driven Pruning**: Uses ManifestAccessPlanner to select files based on filter predicates,
    /// enabling row-group level pruning when row_group metadata is available.
    async fn scan_parquet_files_as_batch_async(
        &self,
        filter: Option<&Expr>,
    ) -> Result<RecordBatch, KalamDbError> {
        base::scan_parquet_files_as_batch_async(
            &self.core,
            self.core.table_id(),
            self.core.table_type(),
            None,
            self.schema_ref(),
            filter,
        )
        .await
    }

    /// Find a row by PK value using the PK index for efficient O(1) lookup.
    ///
    /// This method uses the PK index to find all versions of a row with the given PK value,
    /// then returns the latest non-deleted version.
    fn find_by_pk(
        &self,
        pk_value: &ScalarValue,
    ) -> Result<Option<(SharedTableRowId, SharedTableRow)>, KalamDbError> {
        // Build index prefix for this PK value
        let prefix = self.pk_index.build_prefix_for_pk(pk_value);

        // Use scan_by_index on the IndexedEntityStore (index 0 is the PK index)
        // scan_by_index returns (key, entity) pairs directly
        let results = self
            .store
            .scan_by_index(0, Some(&prefix), None)
            .into_kalamdb_error("PK index scan failed")?;

        if results.is_empty() {
            return Ok(None);
        }

        if let Some((row_id, row)) = results.into_iter().max_by_key(|(row_id, _)| row_id.as_i64()) {
            if row._deleted {
                Ok(None)
            } else {
                Ok(Some((row_id, row)))
            }
        } else {
            Ok(None)
        }
    }

    /// Returns true if the latest hot-storage version of this PK is a tombstone
    /// (`_deleted = true`).  Returns false if the PK is absent from hot storage
    /// or if the latest version is active.
    ///
    /// Used in the PK fast-path of `scan_rows` to prevent cold storage (Parquet)
    /// from surfacing a row that has already been deleted in hot storage.
    fn pk_tombstoned_in_hot(&self, pk_value: &ScalarValue) -> Result<bool, KalamDbError> {
        let prefix = self.pk_index.build_prefix_for_pk(pk_value);
        let results = self
            .store
            .scan_by_index(0, Some(&prefix), None)
            .into_kalamdb_error("PK index scan failed")?;
        Ok(results
            .into_iter()
            .max_by_key(|(row_id, _)| row_id.as_i64())
            .map(|(_, row)| row._deleted)
            .unwrap_or(false))
    }
}

#[async_trait]
impl BaseTableProvider<SharedTableRowId, SharedTableRow> for SharedTableProvider {
    fn core(&self) -> &base::TableProviderCore {
        &self.core
    }

    fn construct_row_from_parquet_data(
        &self,
        _user_id: &UserId,
        row_data: &crate::utils::version_resolution::ParquetRowData,
    ) -> Result<Option<(SharedTableRowId, SharedTableRow)>, KalamDbError> {
        // Shared tables use SeqId as the key (no user_id scoping)
        let row_key = row_data.seq_id;
        let row = SharedTableRow {
            _seq: row_data.seq_id,
            _deleted: row_data.deleted,
            fields: row_data.fields.clone(),
        };
        Ok(Some((row_key, row)))
    }

    /// Find row by PK value using the PK index for O(1) lookup.
    ///
    /// OPTIMIZED: Uses `pk_exists_in_hot` for fast hot-path check.
    /// OPTIMIZED: Uses `pk_exists_in_cold` with manifest-based segment pruning for cold storage.
    /// For shared tables, user_id is ignored (no RLS).
    async fn find_row_key_by_id_field(
        &self,
        _user_id: &UserId,
        id_value: &str,
    ) -> Result<Option<SharedTableRowId>, KalamDbError> {
        // Use shared helper to parse PK value
        let pk_value = crate::utils::pk::parse_pk_value(id_value);

        // Fast path: return the latest non-deleted row from hot storage if it exists.
        if let Some((row_id, _row)) = self.find_by_pk(&pk_value)? {
            return Ok(Some(row_id));
        }

        // If hot storage has entries but they're all deleted, allow PK reuse.
        let prefix = self.pk_index.build_prefix_for_pk(&pk_value);
        let hot_has_versions = self
            .store
            .exists_by_index(0, &prefix)
            .into_kalamdb_error("PK index scan failed")?;
        if hot_has_versions {
            return Ok(None);
        }

        // Not found in hot storage - check cold storage using optimized manifest-based lookup
        // This uses column_stats to prune segments that can't contain the PK
        let pk_name = self.primary_key_field_name();
        let pk_column_id = self.core.primary_key_column_id();
        let exists_in_cold = base::pk_exists_in_cold(
            &self.core,
            self.core.table_id(),
            self.core.table_type(),
            None, // No user scoping for shared tables
            pk_name,
            pk_column_id,
            id_value,
        )
        .await?;

        if exists_in_cold {
            log::trace!("[SharedTableProvider] PK {} exists in cold storage", id_value);
            // For PK uniqueness check, we just need to know it exists
            // Return None to indicate "exists but key not available synchronously"
            return Ok(None);
        }

        Ok(None)
    }

    async fn insert(
        &self,
        _user_id: &UserId,
        row_data: Row,
    ) -> Result<SharedTableRowId, KalamDbError> {
        ensure_manifest_ready(&self.core, self.core.table_type(), None, "SharedTableProvider")?;

        // IGNORE user_id parameter - no RLS for shared tables
        base::ensure_unique_pk_value(self, None, &row_data).await?;

        // Generate new SeqId via SystemColumnsService
        let sys_cols = self.core.services.system_columns.clone();
        let seq_id = sys_cols.generate_seq_id().map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;

        // Create SharedTableRow directly
        let entity = SharedTableRow {
            _seq: seq_id,
            _deleted: false,
            fields: row_data,
        };

        // Key is just the SeqId (SharedTableRowId is type alias for SeqId)
        let row_key = seq_id;

        // Store the entity in RocksDB (hot storage) using insert() to update PK index
        self.store.insert(&row_key, &entity).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to insert shared table row: {}", e))
        })?;

        log::debug!("Inserted shared table row with _seq {}", seq_id);

        // Fire topic/CDC notification (INSERT) - no user_id for shared tables
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(None, &table_id);
        if has_topics || has_live_subs {
            let row = Self::build_notification_row(&entity);
            if has_topics {
                self.core
                    .publish_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Insert,
                        &row,
                        None,
                    )
                    .await;
            }
            if has_live_subs {
                let notification = ChangeNotification::insert(table_id.clone(), row);
                notification_service.notify_table_change(None, table_id, notification);
            }
        }

        Ok(row_key)
    }

    /// Optimized batch insert using single RocksDB WriteBatch
    ///
    /// **Performance**: This method is significantly faster than calling insert() N times:
    /// - Single mutex acquisition for all SeqId generation
    /// - Single RocksDB WriteBatch for all rows (one disk write vs N)
    /// - Batch PK validation (single scan for large batches, O(1) lookups for small batches)
    ///
    /// # Arguments
    /// * `_user_id` - Ignored for shared tables (no RLS)
    /// * `rows` - Vector of Row objects to insert
    ///
    /// # Returns
    /// Vector of generated SharedTableRowIds (SeqIds)
    async fn insert_batch(
        &self,
        _user_id: &UserId,
        rows: Vec<Row>,
    ) -> Result<Vec<SharedTableRowId>, KalamDbError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Ensure manifest is ready
        ensure_manifest_ready(&self.core, self.core.table_type(), None, "SharedTableProvider")?;

        // Coerce rows to match schema types
        let coerced_rows = coerce_rows(rows, &self.schema_ref()).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
        })?;

        // VALIDATE NOT NULL CONSTRAINTS (per ADR-016: must occur before any RocksDB write)
        crate::utils::datafusion_dml::validate_not_null_with_set(
            self.core.non_null_columns(),
            &coerced_rows,
        )
        .map_err(|e| KalamDbError::ConstraintViolation(e.to_string()))?;

        let row_count = coerced_rows.len();

        // Batch PK validation: collect all user-provided PK values
        let pk_name = self.primary_key_field_name();
        let mut pk_values_to_check: Vec<String> = Vec::new();
        for row_data in &coerced_rows {
            if let Some(pk_value) = row_data.get(pk_name) {
                if !matches!(pk_value, ScalarValue::Null) {
                    let pk_str =
                        crate::utils::unified_dml::extract_user_pk_value(row_data, pk_name)?;
                    pk_values_to_check.push(pk_str);
                }
            }
        }

        // OPTIMIZED: Check PK existence in hot storage only (cold storage handled below)
        // For small batches (≤2 rows), use direct lookups. For larger batches, use batch scan.
        if !pk_values_to_check.is_empty() {
            if pk_values_to_check.len() <= 2 {
                // Small batch: individual O(1) lookups are efficient
                for pk_str in &pk_values_to_check {
                    let prefix = self.pk_index.build_pk_prefix(pk_str);
                    if self
                        .store
                        .exists_by_index(0, &prefix)
                        .into_kalamdb_error("PK index check failed")?
                    {
                        return Err(KalamDbError::AlreadyExists(format!(
                            "Primary key violation: value '{}' already exists in column '{}'",
                            pk_str, pk_name
                        )));
                    }
                }
            } else {
                // Larger batch: build all prefixes and use batch scan
                let prefixes: Vec<Vec<u8>> = pk_values_to_check
                    .iter()
                    .map(|pk_str| self.pk_index.build_pk_prefix(pk_str))
                    .collect();

                // Use empty common prefix for shared tables (no user scoping)
                let existing = self
                    .store
                    .exists_batch_by_index(0, &[], &prefixes)
                    .into_kalamdb_error("Batch PK index check failed")?;

                if !existing.is_empty() {
                    // Find which PK matched
                    for pk_str in &pk_values_to_check {
                        let prefix = self.pk_index.build_pk_prefix(pk_str);
                        if existing.contains(&prefix) {
                            return Err(KalamDbError::AlreadyExists(format!(
                                "Primary key violation: value '{}' already exists in column '{}'",
                                pk_str, pk_name
                            )));
                        }
                    }
                    // Fallback if we couldn't match the prefix back (shouldn't happen)
                    return Err(KalamDbError::AlreadyExists(format!(
                        "Primary key violation: a value already exists in column '{}'",
                        pk_name
                    )));
                }
            }

            // OPTIMIZED: Batch cold storage check - O(files) instead of O(files × N)
            // This reads Parquet files ONCE for all PK values instead of N times
            let pk_column_id = self.core.primary_key_column_id();
            if let Some(found_pk) = base::pk_exists_batch_in_cold(
                &self.core,
                self.core.table_id(),
                self.core.table_type(),
                None, // No user scoping for shared tables
                pk_name,
                pk_column_id,
                &pk_values_to_check,
            )
            .await?
            {
                return Err(KalamDbError::AlreadyExists(format!(
                    "Primary key violation: value '{}' already exists in column '{}'",
                    found_pk, pk_name
                )));
            }
        }

        // Generate all SeqIds in single mutex acquisition
        let sys_cols = self.core.services.system_columns.clone();
        let seq_ids = sys_cols.generate_seq_ids(row_count).map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId batch generation failed: {}", e))
        })?;

        // Build all entities and keys
        let mut shared_rows: Vec<SharedTableRow> = Vec::with_capacity(row_count);
        let mut row_keys: Vec<SharedTableRowId> = Vec::with_capacity(row_count);

        for (row_data, seq_id) in coerced_rows.into_iter().zip(seq_ids.into_iter()) {
            row_keys.push(seq_id);
            shared_rows.push(SharedTableRow {
                _seq: seq_id,
                _deleted: false,
                fields: row_data,
            });
        }

        // Batch-encode all rows with FlatBufferBuilder reuse, then write atomically.
        let encode_input: Vec<(
            kalamdb_commons::ids::SeqId,
            bool,
            &kalamdb_commons::models::rows::Row,
        )> = shared_rows.iter().map(|r| (r._seq, r._deleted, &r.fields)).collect();
        let encoded_values =
            kalamdb_commons::serialization::row_codec::batch_encode_shared_table_rows(
                &encode_input,
            )
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to batch encode shared table rows: {}",
                    e
                ))
            })?;

        // Combine keys + entities for index key extraction
        let entries: Vec<(SharedTableRowId, SharedTableRow)> =
            row_keys.iter().copied().zip(shared_rows.into_iter()).collect();

        self.store.insert_batch_preencoded(&entries, encoded_values).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to batch insert shared table rows: {}",
                e
            ))
        })?;

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), None) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        log::debug!(
            "Batch inserted {} shared table rows with _seq range [{}, {}]",
            row_count,
            row_keys.first().map(|k| k.as_i64()).unwrap_or(0),
            row_keys.last().map(|k| k.as_i64()).unwrap_or(0)
        );

        // Fire topic/CDC notifications (INSERT) - no user_id for shared tables
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(None, &table_id);
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
                        None,
                    )
                    .await;
            }
            if has_live_subs {
                for row in rows {
                    let notification = ChangeNotification::insert(table_id.clone(), row);
                    notification_service.notify_table_change(None, table_id.clone(), notification);
                }
            }
        }

        Ok(row_keys)
    }

    async fn update(
        &self,
        _user_id: &UserId,
        key: &SharedTableRowId,
        updates: Row,
    ) -> Result<SharedTableRowId, KalamDbError> {
        // IGNORE user_id parameter - no RLS for shared tables
        // Extract PK from prior row, then delegate to update_by_pk_value
        let pk_name = self.primary_key_field_name().to_string();

        // Load referenced prior version to derive PK value
        let prior_opt = self.store.get(key).into_kalamdb_error("Failed to load prior version")?;

        let prior = if let Some(p) = prior_opt {
            p
        } else {
            load_row_from_parquet_by_seq(
                &self.core,
                self.core.table_type(),
                &self.core.schema_ref(),
                None,
                *key,
                |row_data| SharedTableRow {
                    _seq: row_data.seq_id,
                    _deleted: row_data.deleted,
                    fields: row_data.fields,
                },
            )
            .await?
            .ok_or_else(|| KalamDbError::NotFound("Row not found for update".to_string()))?
        };

        let pk_value_scalar = prior.fields.get(&pk_name).cloned().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Prior row missing PK {}", pk_name))
        })?;

        // Validate PK is not being changed to a value that already exists
        base::validate_pk_update(self, None, &updates, &pk_value_scalar).await?;

        let pk_value_str = pk_value_scalar.to_string();
        self.update_by_pk_value(_user_id, &pk_value_str, updates).await
    }

    async fn update_by_pk_value(
        &self,
        _user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<SharedTableRowId, KalamDbError> {
        // IGNORE user_id parameter - no RLS for shared tables
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

        // Resolve latest per PK - first try hot storage (O(1) via PK index),
        // then fall back to cold storage (Parquet scan)
        let (_latest_key, latest_row) = if let Some(result) = self.find_by_pk(&pk_value_scalar)? {
            result
        } else {
            // Not in hot storage, check cold storage
            log::debug!(
                "[UPDATE] PK {} not found in hot storage, querying cold storage for pk={}",
                pk_name,
                pk_value
            );
            base::find_row_by_pk(self, None, pk_value).await?.ok_or_else(|| {
                KalamDbError::NotFound(format!(
                    "Row with {}={} not found (checked both hot and cold storage)",
                    pk_name, pk_value
                ))
            })?
        };

        let mut merged = latest_row.fields.values.clone();
        for (k, v) in &updates.values {
            merged.insert(k.clone(), v.clone());
        }
        let new_fields = Row::new(merged);

        let sys_cols = self.core.services.system_columns.clone();
        let seq_id = sys_cols.generate_seq_id().map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;
        let entity = SharedTableRow {
            _seq: seq_id,
            _deleted: false,
            fields: new_fields,
        };
        let row_key = seq_id;
        // Use insert() to update PK index for the new MVCC version
        self.store.insert(&row_key, &entity).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to update shared table row: {}", e))
        })?;

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), None) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        // Fire topic/CDC notification (UPDATE) - no user_id for shared tables
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(None, &table_id);
        if has_topics || has_live_subs {
            let new_row = Self::build_notification_row(&entity);
            if has_topics {
                self.core
                    .publish_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Update,
                        &new_row,
                        None,
                    )
                    .await;
            }
            if has_live_subs {
                let old_row = Self::build_notification_row(&latest_row);
                let pk_col = self.primary_key_field_name().to_string();
                let notification = ChangeNotification::update(table_id.clone(), old_row, new_row, vec![pk_col]);
                notification_service.notify_table_change(None, table_id, notification);
            }
        }

        Ok(row_key)
    }

    async fn delete(&self, _user_id: &UserId, key: &SharedTableRowId) -> Result<(), KalamDbError> {
        // IGNORE user_id parameter - no RLS for shared tables
        // Extract PK from prior row, then delegate to delete_by_pk_value
        let pk_name = self.primary_key_field_name().to_string();

        let prior_opt = self.store.get(key).into_kalamdb_error("Failed to load prior version")?;

        let prior = if let Some(p) = prior_opt {
            p
        } else {
            load_row_from_parquet_by_seq(
                &self.core,
                self.core.table_type(),
                &self.core.schema_ref(),
                None,
                *key,
                |row_data| SharedTableRow {
                    _seq: row_data.seq_id,
                    _deleted: row_data.deleted,
                    fields: row_data.fields,
                },
            )
            .await?
            .ok_or_else(|| KalamDbError::NotFound("Row not found for delete".to_string()))?
        };

        let pk_value_scalar = prior.fields.get(&pk_name).cloned().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Prior row missing PK {}", pk_name))
        })?;
        let pk_value_str = pk_value_scalar.to_string();

        self.delete_by_pk_value(_user_id, &pk_value_str).await?;
        Ok(())
    }

    async fn delete_by_pk_value(
        &self,
        _user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        // IGNORE user_id parameter - no RLS for shared tables
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

        // Find latest resolved row for this PK
        // First try hot storage (O(1) via PK index), then fall back to cold storage (Parquet scan)
        let latest_row = if let Some((_key, row)) = self.find_by_pk(&pk_value_scalar)? {
            row
        } else {
            // Not in hot storage, check cold storage
            match base::find_row_by_pk(self, None, pk_value).await? {
                Some((_key, row)) => row,
                None => {
                    log::trace!(
                        "[SharedProvider DELETE_BY_PK] Row with {}={} not found",
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

        // Preserve ALL fields in the tombstone
        let values = latest_row.fields.values.clone();

        let entity = SharedTableRow {
            _seq: seq_id,
            _deleted: true,
            fields: Row::new(values),
        };
        let row_key = seq_id;
        log::info!(
            "[SharedProvider DELETE_BY_PK] Writing tombstone: pk={}, _seq={}",
            pk_value,
            seq_id.as_i64()
        );
        // Use insert() to update PK index for the tombstone record
        self.store.insert(&row_key, &entity).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to delete shared table row: {}", e))
        })?;

        // Mark manifest as having pending writes (hot data needs to be flushed)
        let manifest_service = self.core.services.manifest_service.clone();
        if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), None) {
            log::warn!(
                "Failed to mark manifest as pending_write for {}: {}",
                self.core.table_id(),
                e
            );
        }

        // Fire topic/CDC notification (DELETE) - no user_id for shared tables
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = notification_service.has_subscribers(None, &table_id);
        if has_topics || has_live_subs {
            let row = Self::build_notification_row(&entity);
            if has_topics {
                self.core
                    .publish_to_topics(
                        &table_id,
                        kalamdb_commons::models::TopicOp::Delete,
                        &row,
                        None,
                    )
                    .await;
            }
            if has_live_subs {
                let notification = ChangeNotification::delete_soft(table_id.clone(), row);
                notification_service.notify_table_change(None, table_id, notification);
            }
        }

        Ok(true)
    }

    async fn scan_rows(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, KalamDbError> {
        let schema = self.schema_ref();
        let pk_name = self.primary_key_field_name();

        // ── PK equality fast-path ────────────────────────────────────────────
        // If the filter is `pk_col = <literal>`, use the PK index for O(1)
        // lookup instead of scanning the entire table + MVCC resolution.
        if let Some(expr) = filter {
            if let Some(pk_literal) = base::extract_pk_equality_literal(expr, pk_name) {
                // Coerce the literal to the PK column's Arrow data type
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

                // Try hot storage PK index (O(1))
                let found = self.find_by_pk(&pk_scalar)?;
                if let Some((row_id, row)) = found {
                    log::debug!(
                        "[SharedProvider] PK fast-path hit for {}={}, _seq={}",
                        pk_name,
                        pk_scalar,
                        row_id.as_i64()
                    );
                    return crate::utils::base::rows_to_arrow_batch(
                        &schema,
                        vec![(row_id, row)],
                        projection,
                        |_, _| {},
                    );
                }

                // Not in hot storage — check if it is tombstoned before trying cold storage.
                // A tombstone in hot storage means the row was deleted; falling back to Parquet
                // would surface a stale version and violate MVCC visibility rules.
                if self.pk_tombstoned_in_hot(&pk_scalar)? {
                    log::debug!(
                        "[SharedProvider] PK fast-path tombstone for {}={}",
                        pk_name,
                        pk_scalar
                    );
                    return crate::utils::base::rows_to_arrow_batch(
                        &schema,
                        Vec::<(SharedTableRowId, SharedTableRow)>::new(),
                        projection,
                        |_, _| {},
                    );
                }

                // Not in hot storage — check cold storage via manifest-based lookup
                let cold_found = base::find_row_by_pk(self, None, &pk_scalar.to_string()).await?;
                if let Some((row_id, row)) = cold_found {
                    log::debug!(
                        "[SharedProvider] PK fast-path cold hit for {}={}",
                        pk_name,
                        pk_scalar
                    );
                    return crate::utils::base::rows_to_arrow_batch(
                        &schema,
                        vec![(row_id, row)],
                        projection,
                        |_, _| {},
                    );
                }

                // PK not found anywhere — return empty batch
                log::debug!("[SharedProvider] PK fast-path miss for {}={}", pk_name, pk_scalar);
                return crate::utils::base::rows_to_arrow_batch(
                    &schema,
                    Vec::<(SharedTableRowId, SharedTableRow)>::new(),
                    projection,
                    |_, _| {},
                );
            }
        }

        // ── Full scan path (no PK equality filter) ──────────────────────────
        // Extract sequence bounds from filter to optimize RocksDB scan
        let (since_seq, _until_seq) = if let Some(expr) = filter {
            base::extract_seq_bounds_from_filter(expr)
        } else {
            (None, None)
        };

        let keep_deleted = filter.map(base::filter_uses_deleted_column).unwrap_or(false);

        // NO user_id extraction - shared tables scan ALL rows
        let kvs = self
            .scan_with_version_resolution_to_kvs_async(
                base::system_user_id(),
                filter,
                since_seq,
                limit,
                keep_deleted,
            )
            .await?;

        // Convert to JSON rows aligned with schema
        crate::utils::base::rows_to_arrow_batch(&schema, kvs, projection, |_, _| {})
    }

    async fn scan_with_version_resolution_to_kvs_async(
        &self,
        _user_id: &UserId,
        filter: Option<&Expr>,
        since_seq: Option<kalamdb_commons::ids::SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
    ) -> Result<Vec<(SharedTableRowId, SharedTableRow)>, KalamDbError> {
        use kalamdb_store::EntityStoreAsync;

        // Warn if no filter or limit - potential performance issue
        base::warn_if_unfiltered_scan(self.core.table_id(), filter, limit, self.core.table_type());

        // IGNORE user_id parameter - scan ALL rows (hot storage)

        // Construct start_key if since_seq is provided
        let start_key = if let Some(seq) = since_seq {
            // since_seq is exclusive, so start at seq + 1
            Some(kalamdb_commons::ids::SeqId::from(seq.as_i64() + 1))
        } else {
            None
        };

        // Calculate scan limit using common helper
        let scan_limit = base::calculate_scan_limit(limit);

        // Use async version to avoid blocking the runtime
        let hot_rows = self
            .store
            .scan_typed_with_prefix_and_start_async(None, start_key.as_ref(), scan_limit)
            .await
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to scan shared table hot storage: {}",
                    e
                ))
            })?;
        log::debug!("[SharedProvider] RocksDB scan returned {} rows", hot_rows.len());

        // Scan cold storage (Parquet files) - pass filter for pruning
        // Use async version to avoid blocking the runtime
        let parquet_batch = self.scan_parquet_files_as_batch_async(filter).await?;

        let pk_name = self.primary_key_field_name().to_string();

        let cold_rows: Vec<(SharedTableRowId, SharedTableRow)> =
            parquet_batch_to_rows(&parquet_batch)?
                .into_iter()
                .map(|row_data| {
                    let seq_id = row_data.seq_id;
                    (
                        seq_id,
                        SharedTableRow {
                            _seq: seq_id,
                            _deleted: row_data.deleted,
                            fields: row_data.fields,
                        },
                    )
                })
                .collect();

        let mut result = merge_versioned_rows(&pk_name, hot_rows, cold_rows, keep_deleted);

        // Apply limit after resolution using common helper
        base::apply_limit(&mut result, limit);

        log::debug!(
            "[SharedProvider] Version-resolved rows (post-tombstone filter): {}",
            result.len()
        );
        Ok(result)
    }

    fn extract_row(row: &SharedTableRow) -> &Row {
        &row.fields
    }
}

// Manual Debug to satisfy DataFusion's TableProvider: Debug bound
impl std::fmt::Debug for SharedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table_id = self.core.table_id_arc();
        f.debug_struct("SharedTableProvider")
            .field("table_id", &table_id)
            .field("table_type", &self.core.table_type())
            .field("primary_key_field_name", &self.core.primary_key_field_name())
            .finish()
    }
}

// Implement DataFusion TableProvider trait
#[async_trait]
impl TableProvider for SharedTableProvider {
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // SECURITY: Enforce shared table access rules (access_level + role)
        check_shared_table_access(state, self.core.table_def()).map_err(DataFusionError::from)?;

        // Extract user context including read_context for leader check
        // SharedTableProvider ignores user_id for data access (no RLS) but uses read_context
        let (_user_id, _role, read_context) = extract_full_user_context(state).map_err(|e| {
            DataFusionError::Execution(format!("Failed to extract user context: {}", e))
        })?;

        // Check if this is a client read that requires leader (shared data shard)
        // Skip check for internal reads (jobs, live query notifications, etc.)
        if read_context.requires_leader()
            && self.core.services.cluster_coordinator.is_cluster_mode().await
        {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                // Include leader address for auto-forwarding by the SQL handler
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
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
        check_shared_table_write_access(state, self.core.table_def())
            .map_err(DataFusionError::from)?;

        if insert_op != InsertOp::Append {
            return Err(DataFusionError::Plan(format!(
                "{} is not supported for shared tables",
                insert_op
            )));
        }

        // In cluster mode, ensure we're on the shared shard leader
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(DataFusionError::External(Box::new(NotLeaderError::new(leader_addr))));
            }
        }

        let rows = crate::utils::datafusion_dml::collect_input_rows(state, input).await?;
        let inserted = self
            .insert_batch(base::system_user_id(), rows)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        crate::utils::datafusion_dml::rows_affected_plan(state, inserted.len() as u64).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        check_shared_table_write_access(state, self.core.table_def())
            .map_err(DataFusionError::from)?;
        crate::utils::datafusion_dml::validate_where_clause(&filters, "DELETE")?;

        // In cluster mode, ensure we're on the shared shard leader
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(DataFusionError::External(Box::new(NotLeaderError::new(leader_addr))));
            }
        }

        let rows =
            crate::utils::datafusion_dml::collect_matching_rows(self, state, &filters).await?;
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
                .delete_by_pk_value(base::system_user_id(), &pk_value)
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
        check_shared_table_write_access(state, self.core.table_def())
            .map_err(DataFusionError::from)?;
        crate::utils::datafusion_dml::validate_where_clause(&filters, "UPDATE")?;

        // In cluster mode, ensure we're on the shared shard leader
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(DataFusionError::External(Box::new(NotLeaderError::new(leader_addr))));
            }
        }

        let pk_column = self.primary_key_field_name().to_string();
        crate::utils::datafusion_dml::validate_update_assignments(&assignments, &pk_column)?;

        let rows =
            crate::utils::datafusion_dml::collect_matching_rows(self, state, &filters).await?;
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

            self.update_by_pk_value(base::system_user_id(), &pk_value, Row::new(values))
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            updated += 1;
        }

        crate::utils::datafusion_dml::rows_affected_plan(state, updated).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        self.base_statistics()
    }
}

// KalamTableProvider: extends TableProvider with KalamDB-specific DML
#[async_trait]
impl crate::utils::dml_provider::KalamTableProvider for SharedTableProvider {
    async fn insert_rows(&self, user_id: &UserId, rows: Vec<Row>) -> Result<usize, KalamDbError> {
        // In cluster mode, ensure we're on the shared shard leader.
        // This is needed because fast_insert bypasses DataFusion's insert_into(),
        // which has its own leadership check.
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(KalamDbError::NotLeader { leader_addr });
            }
        }
        let keys = self.insert_batch(user_id, rows).await?;
        Ok(keys.len())
    }

    async fn insert_rows_returning(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
    ) -> Result<Vec<ScalarValue>, KalamDbError> {
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(KalamDbError::NotLeader { leader_addr });
            }
        }
        let keys = self.insert_batch(user_id, rows).await?;
        Ok(keys
            .into_iter()
            .map(|k| ScalarValue::Int64(Some(k.as_i64())))
            .collect())
    }
}
