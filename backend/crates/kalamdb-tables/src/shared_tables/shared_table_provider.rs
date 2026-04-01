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

use kalamdb_commons::conversions::arrow_json_conversion::{coerce_rows, coerce_updates};
use kalamdb_commons::ids::SharedTableRowId;
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::websocket::ChangeNotification;
use kalamdb_commons::NotLeaderError;
use kalamdb_commons::StorageKey;
use kalamdb_session_datafusion::{
    check_shared_table_access, check_shared_table_write_access, session_error_to_datafusion,
};
use kalamdb_store::EntityStore;
use kalamdb_system::VectorMetric;
use kalamdb_vector::{
    new_indexed_shared_vector_hot_store, SharedVectorHotOpId, SharedVectorHotStore, VectorHotOp,
    VectorHotOpType,
};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

// Arrow <-> JSON helpers
use crate::utils::version_resolution::resolve_latest_kvs_from_cold_batch;

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

    /// Embedding columns tracked by vector hot staging: (column_name, dimensions).
    vector_columns: Vec<(String, u32)>,

    /// Cached vector staging stores keyed by embedding column name.
    vector_stores: HashMap<String, Arc<SharedVectorHotStore>>,
}

impl SharedTableProvider {
    /// Create a new shared table provider
    ///
    /// # Arguments
    /// * `core` - Shared core with services, schema, pk_name, table_def, etc.
    /// * `store` - SharedTableIndexedStore for this table
    pub fn new(core: Arc<TableProviderCore>, store: Arc<SharedTableIndexedStore>) -> Self {
        let pk_index = SharedTablePkIndex::new(core.table_id(), core.primary_key_field_name());
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
        let vector_stores: HashMap<String, Arc<SharedVectorHotStore>> = vector_columns
            .iter()
            .map(|(column_name, _)| {
                (
                    column_name.clone(),
                    Arc::new(new_indexed_shared_vector_hot_store(
                        backend.clone(),
                        core.table_id(),
                        column_name,
                    )),
                )
            })
            .collect();

        Self {
            core,
            store,
            pk_index,
            vector_columns,
            vector_stores,
        }
    }

    pub async fn collect_live_string_primary_keys_before_async(
        &self,
        cutoff_exclusive: String,
        limit: usize,
    ) -> Result<Vec<String>, KalamDbError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let store = Arc::clone(&self.store);
        let pk_name = self.primary_key_field_name().to_string();

        tokio::task::spawn_blocking(move || {
            collect_live_string_primary_keys_before_from_store(
                store.as_ref(),
                &pk_name,
                &cutoff_exclusive,
                limit,
            )
        })
        .await
        .map_err(|error| {
            KalamDbError::InvalidOperation(format!(
                "collect_live_string_primary_keys_before_async join error: {}",
                error
            ))
        })?
    }

    pub async fn hard_delete_string_primary_keys_async(
        &self,
        pk_values: Vec<String>,
    ) -> Result<usize, KalamDbError> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let store = Arc::clone(&self.store);
        let table_id = self.core.table_id().clone();
        let pk_name = self.primary_key_field_name().to_string();

        tokio::task::spawn_blocking(move || {
            let pk_index = SharedTablePkIndex::new(&table_id, &pk_name);
            hard_delete_string_primary_keys_from_store(store.as_ref(), &pk_index, &pk_values)
        })
        .await
        .map_err(|error| {
            KalamDbError::InvalidOperation(format!(
                "hard_delete_string_primary_keys_async join error: {}",
                error
            ))
        })?
    }

    /// Build a complete Row for live query/topic notifications including system columns (_seq, _deleted)
    ///
    /// This ensures notifications include all columns, not just user-defined fields.
    fn build_notification_row(entity: &SharedTableRow) -> Row {
        base::build_notification_row(&entity.fields, entity._seq, entity._deleted)
    }

    /// Access the underlying indexed store (used by flush jobs)
    pub fn store(&self) -> Arc<SharedTableIndexedStore> {
        Arc::clone(&self.store)
    }

    fn extract_embedding_vector(value: &ScalarValue, expected_dimensions: u32) -> Option<Vec<f32>> {
        base::extract_embedding_vector(value, expected_dimensions)
    }

    async fn stage_vector_upsert(
        &self,
        seq: SharedTableRowId,
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
            let key = SharedVectorHotOpId::new(seq, pk.clone());
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
        entries: &[(SharedTableRowId, SharedTableRow)],
    ) -> Result<(), KalamDbError> {
        if self.vector_columns.is_empty() || entries.is_empty() {
            return Ok(());
        }

        let mut ops_by_column: HashMap<String, Vec<(SharedVectorHotOpId, VectorHotOp)>> =
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
                    SharedVectorHotOpId::new(*row_key, pk.clone()),
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
        seq: SharedTableRowId,
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
            let key = SharedVectorHotOpId::new(seq, pk.to_string());
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
        columns: Option<&[String]>,
    ) -> Result<RecordBatch, KalamDbError> {
        base::scan_parquet_files_as_batch_async(
            &self.core,
            self.core.table_id(),
            self.core.table_type(),
            None,
            self.schema_ref(),
            filter,
            columns,
        )
        .await
    }

    /// Find a row by PK value using the PK index for efficient O(1) lookup.
    ///
    /// This method uses the PK index to find all versions of a row with the given PK value,
    /// then returns the latest non-deleted version.
    async fn latest_hot_pk_entry(
        &self,
        pk_value: &ScalarValue,
    ) -> Result<Option<(SharedTableRowId, SharedTableRow)>, KalamDbError> {
        let prefix = self.pk_index.build_prefix_for_pk(pk_value);
        self.store
            .get_latest_by_index_prefix_async(0, prefix)
            .await
            .into_kalamdb_error("PK index scan failed")
    }

    pub async fn find_by_pk(
        &self,
        pk_value: &ScalarValue,
    ) -> Result<Option<(SharedTableRowId, SharedTableRow)>, KalamDbError> {
        Ok(self.latest_hot_pk_entry(pk_value).await?.and_then(|(row_id, row)| {
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
    async fn pk_tombstoned_in_hot(&self, pk_value: &ScalarValue) -> Result<bool, KalamDbError> {
        Ok(self
            .latest_hot_pk_entry(pk_value)
            .await?
            .map(|(_, row)| row._deleted)
            .unwrap_or(false))
    }
}

fn collect_live_string_primary_keys_before_from_store(
    store: &SharedTableIndexedStore,
    pk_name: &str,
    cutoff_exclusive: &str,
    limit: usize,
) -> Result<Vec<String>, KalamDbError> {
    let iter = store
        .scan_by_index_iter(0, None, None)
        .into_kalamdb_error("PK index scan failed")?;
    let mut expired = Vec::with_capacity(limit.min(1024));
    let mut current_pk: Option<String> = None;
    let mut current_deleted = false;

    for entry in iter {
        let (_row_id, row) = entry.into_kalamdb_error("PK index scan failed")?;
        let Some(pk_value) = extract_string_primary_key(&row, pk_name) else {
            continue;
        };

        if pk_value.as_str() >= cutoff_exclusive {
            finalize_primary_key_group(&mut expired, &mut current_pk, current_deleted);
            return Ok(expired);
        }

        match current_pk.as_deref() {
            Some(existing) if existing == pk_value.as_str() => {
                current_deleted = row._deleted;
            },
            Some(_) => {
                finalize_primary_key_group(&mut expired, &mut current_pk, current_deleted);
                if expired.len() >= limit {
                    return Ok(expired);
                }
                current_pk = Some(pk_value);
                current_deleted = row._deleted;
            },
            None => {
                current_pk = Some(pk_value);
                current_deleted = row._deleted;
            },
        }
    }

    finalize_primary_key_group(&mut expired, &mut current_pk, current_deleted);
    Ok(expired)
}

fn hard_delete_string_primary_keys_from_store(
    store: &SharedTableIndexedStore,
    pk_index: &SharedTablePkIndex,
    pk_values: &[String],
) -> Result<usize, KalamDbError> {
    let mut deleted = 0usize;

    for pk_value in pk_values {
        let prefix = pk_index.build_pk_prefix(pk_value);
        let rows = store
            .scan_by_index(0, Some(&prefix), None)
            .into_kalamdb_error("PK index scan failed")?;

        for (row_id, _) in rows {
            store.delete(&row_id).into_kalamdb_error("Hard delete by PK failed")?;
            deleted += 1;
        }
    }

    Ok(deleted)
}

fn extract_string_primary_key(row: &SharedTableRow, pk_name: &str) -> Option<String> {
    match row.fields.get(pk_name)? {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn finalize_primary_key_group(
    expired: &mut Vec<String>,
    current_pk: &mut Option<String>,
    current_deleted: bool,
) {
    if !current_deleted {
        if let Some(pk_value) = current_pk.take() {
            expired.push(pk_value);
        }
    } else {
        current_pk.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_tables::{new_indexed_shared_table_store, SharedTableRow};
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::ids::SeqId;
    use kalamdb_commons::models::{NamespaceId, TableId, TableName};
    use kalamdb_store::test_utils::InMemoryBackend;
    use kalamdb_store::StorageBackend;
    use std::collections::BTreeMap;

    fn create_store(pk_field_name: &str) -> Arc<SharedTableIndexedStore> {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let table_id = TableId::new(NamespaceId::new("dba"), TableName::new("stats"));
        Arc::new(new_indexed_shared_table_store(backend, &table_id, pk_field_name))
    }

    fn build_row(
        seq: i64,
        pk_name: &str,
        pk_value: &str,
        deleted: bool,
    ) -> (SeqId, SharedTableRow) {
        let mut fields = BTreeMap::new();
        fields.insert(pk_name.to_string(), ScalarValue::Utf8(Some(pk_value.to_string())));

        let row_id = SeqId::new(seq);
        (
            row_id,
            SharedTableRow {
                _seq: row_id,
                _deleted: deleted,
                fields: Row::new(fields),
            },
        )
    }

    #[test]
    fn collect_live_string_primary_keys_before_filters_tombstoned_latest_rows() {
        let store = create_store("id");

        let (old_live_key, old_live_row) =
            build_row(1, "id", "1700000000000:node-a:memory_usage_mb", false);
        let (tombstone_old_key, tombstone_old_row) =
            build_row(2, "id", "1700000000001:node-a:cpu_usage_percent", false);
        let (tombstone_new_key, tombstone_new_row) =
            build_row(3, "id", "1700000000001:node-a:cpu_usage_percent", true);
        let (recent_key, recent_row) =
            build_row(4, "id", "1700000001000:node-a:memory_usage_mb", false);

        store.insert(&old_live_key, &old_live_row).unwrap();
        store.insert(&tombstone_old_key, &tombstone_old_row).unwrap();
        store.insert(&tombstone_new_key, &tombstone_new_row).unwrap();
        store.insert(&recent_key, &recent_row).unwrap();

        let expired = collect_live_string_primary_keys_before_from_store(
            store.as_ref(),
            "id",
            "1700000000500:",
            10,
        )
        .unwrap();

        assert_eq!(expired, vec!["1700000000000:node-a:memory_usage_mb".to_string()]);
    }

    #[test]
    fn hard_delete_string_primary_keys_removes_all_versions_for_pk() {
        let store = create_store("id");
        let pk_index = SharedTablePkIndex::new(
            &TableId::new(NamespaceId::new("dba"), TableName::new("stats")),
            "id",
        );

        let (first_key, first_row) =
            build_row(10, "id", "1700000000000:node-a:memory_usage_mb", false);
        let (second_key, second_row) =
            build_row(11, "id", "1700000000000:node-a:memory_usage_mb", true);
        let (other_key, other_row) =
            build_row(12, "id", "1700000000001:node-a:cpu_usage_percent", false);

        store.insert(&first_key, &first_row).unwrap();
        store.insert(&second_key, &second_row).unwrap();
        store.insert(&other_key, &other_row).unwrap();

        let deleted = hard_delete_string_primary_keys_from_store(
            store.as_ref(),
            &pk_index,
            &["1700000000000:node-a:memory_usage_mb".to_string()],
        )
        .unwrap();

        assert_eq!(deleted, 2);
        let deleted_prefix = pk_index.build_pk_prefix("1700000000000:node-a:memory_usage_mb");
        let remaining_deleted = store.scan_by_index(0, Some(&deleted_prefix), None).unwrap();
        assert!(remaining_deleted.is_empty());

        let other_prefix = pk_index.build_pk_prefix("1700000000001:node-a:cpu_usage_percent");
        let remaining_other = store.scan_by_index(0, Some(&other_prefix), None).unwrap();
        assert_eq!(remaining_other.len(), 1);
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

        if let Some((row_id, row)) = self.latest_hot_pk_entry(&pk_value).await? {
            if row._deleted {
                return Ok(None);
            }
            return Ok(Some(row_id));
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
        let span = tracing::debug_span!(
            "table.insert",
            table_id = %self.core.table_id(),
            scope = "shared",
            column_count = row_data.values.len()
        );
        async move {
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
            self.store.insert_async(row_key, entity.clone()).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to insert shared table row: {}", e))
            })?;

            log::debug!("Inserted shared table row with _seq {}", seq_id);

            if let Err(e) = self.stage_vector_upsert(seq_id, &entity.fields).await {
                log::warn!(
                    "Failed to stage vector upsert for table={}, seq={}: {}",
                    self.core.table_id(),
                    seq_id.as_i64(),
                    e
                );
            }

            // Mark manifest as having pending writes (hot data needs to be flushed)
            let manifest_service = self.core.services.manifest_service.clone();
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), None) {
                log::warn!(
                    "Failed to mark manifest as pending_write for {}: {}",
                    self.core.table_id(),
                    e
                );
            }

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
        .instrument(span)
        .await
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
        let row_count = rows.len();
        let span = tracing::debug_span!(
            "table.insert_batch",
            table_id = %self.core.table_id(),
            scope = "shared",
            row_count
        );
        async move {
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
        let mut pk_values_to_check: Vec<(String, ScalarValue)> = Vec::new();
        let mut seen_batch_pks = HashSet::new();
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

        // Check only the latest hot version for each PK. Tombstoned latest
        // versions are reusable and should not fail the insert.
        if !pk_values_to_check.is_empty() {
            // Single spawn_blocking for ALL hot PK checks (avoids N round-trips)
            let pk_prefixes: Vec<(String, Vec<u8>)> = pk_values_to_check
                .iter()
                .map(|(pk_str, pk_value)| {
                    (pk_str.clone(), self.pk_index.build_prefix_for_pk(pk_value))
                })
                .collect();

            let store = self.store.clone();
            let hot_duplicate = tokio::task::spawn_blocking(move || -> Result<Option<String>, KalamDbError> {
                for (pk_str, prefix) in &pk_prefixes {
                    if let Some((_row_id, row)) = store
                        .get_latest_by_index_prefix(0, prefix)
                        .map_err(|e| KalamDbError::InvalidOperation(format!("PK index scan failed: {}", e)))?
                    {
                        if !row._deleted {
                            return Ok(Some(pk_str.clone()));
                        }
                    }
                }
                Ok(None)
            })
            .await
            .map_err(|e| KalamDbError::InvalidOperation(format!("spawn_blocking error: {}", e)))??;

            if let Some(dup_pk) = hot_duplicate {
                return Err(KalamDbError::AlreadyExists(format!(
                    "Primary key violation: value '{}' already exists in column '{}'",
                    dup_pk, pk_name
                )));
            }

            // OPTIMIZED: Batch cold storage check - O(files) instead of O(files × N)
            // This reads Parquet files ONCE for all PK values instead of N times
            let pk_column_id = self.core.primary_key_column_id();
            let pk_values: Vec<String> =
                pk_values_to_check.iter().map(|(pk, _)| pk.clone()).collect();
            if let Some(found_pk) = base::pk_exists_batch_in_cold(
                &self.core,
                self.core.table_id(),
                self.core.table_type(),
                None, // No user scoping for shared tables
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

        // Combine keys + entities for index key extraction
        let entries: Vec<(SharedTableRowId, SharedTableRow)> =
            row_keys.iter().copied().zip(shared_rows.into_iter()).collect();

        // Encode + write in single spawn_blocking (avoids separate encode + write hops)
        let store = self.store.clone();
        let entries_for_write = entries.clone();

        tokio::task::spawn_blocking(move || -> Result<(), KalamDbError> {
            let encode_input: Vec<(
                kalamdb_commons::ids::SeqId,
                bool,
                &kalamdb_commons::models::rows::Row,
            )> = entries_for_write.iter().map(|(_, r)| (r._seq, r._deleted, &r.fields)).collect();
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
            store
                .insert_batch_preencoded(&entries_for_write, encoded_values)
                .map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to batch insert shared table rows: {}",
                        e
                    ))
                })
        })
        .await
        .map_err(|e| KalamDbError::InvalidOperation(format!("spawn_blocking error: {}", e)))??;

        if let Err(e) = self.stage_vector_upsert_batch(&entries).await {
            log::warn!(
                "Failed to batch stage vector upserts for table={}: {}",
                self.core.table_id(),
                e
            );
        }

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
        .instrument(span)
        .await
    }

    async fn update(
        &self,
        _user_id: &UserId,
        key: &SharedTableRowId,
        updates: Row,
    ) -> Result<Option<SharedTableRowId>, KalamDbError> {
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
    ) -> Result<Option<SharedTableRowId>, KalamDbError> {
        let span = tracing::debug_span!(
            "table.update",
            table_id = %self.core.table_id(),
            scope = "shared",
            pk = pk_value,
            update_columns = updates.values.len()
        );
        async move {
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
            let (_latest_key, latest_row) =
                if let Some(result) = self.find_by_pk(&pk_value_scalar).await? {
                    result
                } else if self.pk_tombstoned_in_hot(&pk_value_scalar).await? {
                    return Err(KalamDbError::NotFound(format!(
                        "Row with {}={} was deleted",
                        pk_name, pk_value
                    )));
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

            // Coerce update values to match schema types (e.g., Utf8 → TimestampMicrosecond).
            // Without this, the no-op comparison would fail for any column where the
            // SQL literal type differs from the stored Arrow type (TIMESTAMP, INT, etc.).
            let coerced = coerce_updates(updates, &self.schema_ref()).map_err(|e| {
                KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
            })?;

            // Merge coerced updates onto latest
            let mut merged = latest_row.fields.values.clone();
            for (k, v) in coerced.values {
                merged.insert(k, v);
            }
            let new_fields = Row::new(merged);

            // Skip write if the merged row is identical to the existing row.
            // Like PostgreSQL / MySQL, a no-op UPDATE should not create a new
            // MVCC version, fire notifications, or count as a row affected.
            if new_fields == latest_row.fields {
                tracing::debug!(
                    table_id = %self.core.table_id(),
                    pk = pk_value,
                    "table.update_noop: row unchanged, skipping write"
                );
                return Ok(None);
            }

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
            self.store.insert_async(row_key, entity.clone()).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to update shared table row: {}", e))
            })?;

            if let Err(e) = self.stage_vector_upsert(seq_id, &entity.fields).await {
                log::warn!(
                    "Failed to stage vector upsert for table={}, seq={}: {}",
                    self.core.table_id(),
                    seq_id.as_i64(),
                    e
                );
            }

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
                    let notification = ChangeNotification::update(
                        table_id.clone(),
                        old_row,
                        new_row,
                        vec![pk_col],
                    );
                    notification_service.notify_table_change(None, table_id, notification);
                }
            }

            Ok(Some(row_key))
        }
        .instrument(span)
        .await
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
        let span = tracing::debug_span!(
            "table.delete",
            table_id = %self.core.table_id(),
            scope = "shared",
            pk = pk_value
        );
        async move {
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
            let latest_row = if let Some((_key, row)) = self.find_by_pk(&pk_value_scalar).await? {
                row
            } else if self.pk_tombstoned_in_hot(&pk_value_scalar).await? {
                return Ok(false);
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
            log::debug!(
                "[SharedProvider DELETE_BY_PK] Writing tombstone: pk={}, _seq={}",
                pk_value,
                seq_id.as_i64()
            );
            // Use insert() to update PK index for the tombstone record
            self.store.insert_async(row_key, entity.clone()).await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Failed to delete shared table row: {}", e))
            })?;

            if let Err(e) = self.stage_vector_delete(seq_id, pk_value).await {
                log::warn!(
                    "Failed to stage vector delete for table={}, seq={}, pk={}: {}",
                    self.core.table_id(),
                    seq_id.as_i64(),
                    pk_value,
                    e
                );
            }

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
        .instrument(span)
        .await
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
                let found = self.find_by_pk(&pk_scalar).await?;
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
                if self.pk_tombstoned_in_hot(&pk_scalar).await? {
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

        // ── Count-only fast-path ─────────────────────────────────────────────
        // When projection is empty (e.g., COUNT(*)), avoid loading full row data.
        // Only decode metadata (seq, deleted, pk) for version resolution.
        if let Some(proj) = projection {
            if proj.is_empty() && filter.is_none() {
                let count = self.count_resolved_rows_async().await?;
                return base::build_count_only_batch(count);
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

        // Compute cold-path column projection: when DataFusion provides a projection,
        // we only need to decode the projected columns + system columns + PK from Parquet.
        let cold_columns = base::compute_cold_columns(projection, &schema, pk_name);

        // NO user_id extraction - shared tables scan ALL rows
        let kvs = self
            .scan_with_version_resolution_to_kvs_async(
                base::system_user_id(),
                filter,
                since_seq,
                limit,
                keep_deleted,
                cold_columns.as_deref(),
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
        cold_columns: Option<&[String]>,
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

        // Run hot storage (RocksDB) and cold storage (Parquet) scans concurrently
        let hot_future =
            self.store
                .scan_typed_with_prefix_and_start_async(None, start_key.as_ref(), scan_limit);
        let cold_future = self.scan_parquet_files_as_batch_async(filter, cold_columns);

        let (hot_result, cold_result) = tokio::join!(hot_future, cold_future);

        let hot_rows = hot_result.map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to scan shared table hot storage: {}",
                e
            ))
        })?;
        log::trace!("[SharedProvider] RocksDB scan returned {} rows", hot_rows.len());

        let parquet_batch = cold_result?;

        let pk_name = self.primary_key_field_name().to_string();

        let cold_rows_scanned = parquet_batch.num_rows();
        log::trace!("[SharedProvider] Cold scan returned {} Parquet rows", cold_rows_scanned);

        let mut result = resolve_latest_kvs_from_cold_batch(
            &pk_name,
            hot_rows,
            &parquet_batch,
            keep_deleted,
            |row_data| {
                let seq_id = row_data.seq_id;
                Ok((
                    seq_id,
                    SharedTableRow {
                        _seq: seq_id,
                        _deleted: row_data.deleted,
                        fields: row_data.fields,
                    },
                ))
            },
        )?;

        // Apply limit after resolution using common helper
        base::apply_limit(&mut result, limit);

        log::trace!(
            "[SharedProvider] Version-resolved rows (post-tombstone filter): {}",
            result.len()
        );
        Ok(result)
    }

    fn extract_row(row: &SharedTableRow) -> &Row {
        &row.fields
    }
}

impl SharedTableProvider {
    /// Count resolved rows without materializing full row data.
    ///
    /// Used for COUNT(*) queries where projection is empty. Instead of deserializing
    /// all row fields (which can consume ~600-800 bytes per row in HashMap allocations),
    /// this only decodes seq, deleted, and the PK field for version resolution.
    ///
    /// For 100K rows, this saves ~80MB of memory compared to the full scan path.
    async fn count_resolved_rows_async(&self) -> Result<usize, KalamDbError> {
        use kalamdb_commons::serialization::row_codec::decode_shared_table_row_metadata;

        let pk_name = self.primary_key_field_name().to_string();
        let store = Arc::clone(&self.store);
        let pk_name_clone = pk_name.clone();

        // Hot storage: scan raw bytes and decode metadata only (skip full field deserialization)
        let hot_future = tokio::task::spawn_blocking(move || {
            let partition = store.partition();
            let iter =
                store.backend().scan(&partition, None, None, Some(1_000_000)).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to scan shared table hot storage for count: {}",
                        e
                    ))
                })?;

            let mut hot_metadata = Vec::new();
            for (key_bytes, value_bytes) in iter {
                let key = kalamdb_commons::ids::SharedTableRowId::from_storage_key(&key_bytes)
                    .map_err(|e| {
                        KalamDbError::InvalidOperation(format!("Failed to decode row key: {}", e))
                    })?;
                match decode_shared_table_row_metadata(&value_bytes, &pk_name_clone) {
                    Ok(metadata) => hot_metadata.push((key, metadata)),
                    Err(e) => {
                        log::warn!("Skipping row with malformed metadata: {}", e);
                        continue;
                    },
                }
            }
            Ok::<_, KalamDbError>(hot_metadata)
        });

        // Cold storage: scan Parquet files (get full batch, but extract only metadata)
        let cold_future = self.scan_parquet_files_as_batch_async(None, None);

        let (hot_result, cold_result): (_, _) = tokio::join!(hot_future, cold_future);

        let hot_metadata: Vec<(
            kalamdb_commons::ids::SharedTableRowId,
            kalamdb_commons::serialization::row_codec::RowMetadata,
        )> = hot_result.map_err(|e| {
            KalamDbError::InvalidOperation(format!("spawn_blocking join error: {}", e))
        })??;

        let parquet_batch = cold_result?;
        let count = crate::utils::version_resolution::count_resolved_from_metadata(
            &pk_name,
            hot_metadata.into_iter().map(|(_, m)| m).collect(),
            &parquet_batch,
        )?;
        Ok(count)
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
        check_shared_table_access(state, self.core.table_def())
            .map_err(session_error_to_datafusion)?;

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
            .map_err(session_error_to_datafusion)?;

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
            .map_err(session_error_to_datafusion)?;
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

        let pk_column = self.primary_key_field_name().to_string();
        let schema = self.schema_ref();
        let projection = crate::utils::datafusion_dml::dml_scan_projection(
            &schema,
            &filters,
            &[],
            &[&pk_column],
        )?;
        let rows = crate::utils::datafusion_dml::collect_matching_rows_with_projection(
            self,
            state,
            &filters,
            projection.as_ref(),
        )
        .await?;
        if rows.is_empty() {
            return crate::utils::datafusion_dml::rows_affected_plan(state, 0).await;
        }

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
            .map_err(session_error_to_datafusion)?;
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

        let schema = self.schema_ref();
        let projection = crate::utils::datafusion_dml::dml_scan_projection(
            &schema,
            &filters,
            &assignments,
            &[&pk_column],
        )?;
        let rows = crate::utils::datafusion_dml::collect_matching_rows_with_projection(
            self,
            state,
            &filters,
            projection.as_ref(),
        )
        .await?;
        if rows.is_empty() {
            return crate::utils::datafusion_dml::rows_affected_plan(state, 0).await;
        }

        let mut seen = HashSet::new();
        let mut updated: u64 = 0;

        for row in rows {
            let pk_value = crate::utils::datafusion_dml::extract_pk_value(&row, &pk_column)?;
            if !seen.insert(pk_value.clone()) {
                continue;
            }

            let result = self
                .update_by_pk_value(
                    base::system_user_id(),
                    &pk_value,
                    crate::utils::datafusion_dml::evaluate_assignment_values(
                        state,
                        &schema,
                        &row,
                        &assignments,
                    )?,
                )
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            if result.is_some() {
                updated += 1;
            }
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

    async fn update_row_by_pk(
        &self,
        _user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<bool, KalamDbError> {
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(KalamDbError::NotLeader { leader_addr });
            }
        }

        match self.update_by_pk_value(base::system_user_id(), pk_value, updates).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false), // no-op: row unchanged
            Err(KalamDbError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn delete_row_by_pk(
        &self,
        _user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        if self.core.services.cluster_coordinator.is_cluster_mode().await {
            let is_leader = self.core.services.cluster_coordinator.is_leader_for_shared().await;
            if !is_leader {
                let leader_addr =
                    self.core.services.cluster_coordinator.leader_addr_for_shared().await;
                return Err(KalamDbError::NotLeader { leader_addr });
            }
        }

        self.delete_by_pk_value(base::system_user_id(), pk_value).await
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
        Ok(keys.into_iter().map(|k| ScalarValue::Int64(Some(k.as_i64()))).collect())
    }
}
