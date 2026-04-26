//! User table provider implementation with RLS
//!
//! This module provides UserTableProvider implementing BaseTableProvider<UserTableRowId,
//! UserTableRow> with Row-Level Security (RLS) enforced via user_id parameter.
//!
//! **Key Features**:
//! - Direct fields (no UserTableShared wrapper)
//! - Shared core via Arc<TableProviderCore>
//! - No handlers - all DML logic inline
//! - RLS via user_id parameter in DML methods
//! - SessionState extraction for scan_rows()
//! - PK Index for efficient row lookup (Phase 14)

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    catalog::Session,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{dml::InsertOp, Expr, TableProviderFilterPushDown},
    physical_plan::{ExecutionPlan, Statistics},
    scalar::ScalarValue,
};
use kalamdb_commons::{
    conversions::arrow_json_conversion::{coerce_rows, coerce_updates},
    ids::{SeqId, UserTableRowId},
    models::{datatypes::KalamDataType, rows::Row, OperationKind, UserId},
    websocket::ChangeNotification,
    StorageKey, TableType,
};
use kalamdb_datafusion_sources::{
    exec::{resolve_latest_kvs_from_cold_batch, VersionedRow},
    provider::{
        merged_projection_scan_descriptor, mvcc_filter_capability, FilterCapability,
        ScanDescriptor, SourceProvider,
    },
};
use kalamdb_session::can_read_all_users;
use kalamdb_session_datafusion::{
    check_user_table_access, check_user_table_write_access, session_error_to_datafusion,
};
use kalamdb_store::EntityStore;
use kalamdb_transactions::{extract_transaction_query_context, StagedMutation};
use kalamdb_vector::{new_indexed_user_vector_hot_store, UserVectorHotOpId, UserVectorHotStore};
use tracing::Instrument;

use crate::{
    error::KalamDbError,
    error_extensions::KalamDbResultExt,
    manifest::manifest_helpers::{ensure_manifest_ready, load_row_from_parquet_by_seq},
    user_tables::{UserTableIndexedStore, UserTablePkIndex, UserTableRow},
    utils::{
        base::{self, BaseTableProvider, DeferredMvccScanProvider, TableProviderCore},
        row_utils::extract_user_context,
    },
};

/// User table provider with RLS
///
/// **Architecture**:
/// - Stateless provider (user context passed per-operation)
/// - Direct fields (no wrapper layer)
/// - Shared core via Arc<TableProviderCore> (holds schema, pk_name, column_defaults,
///   non_null_columns)
/// - RLS enforced via user_id parameter
/// - PK Index for efficient row lookup (Phase 14)
#[derive(Clone)]
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

struct UserMvccRow(UserTableRow);

impl VersionedRow for UserMvccRow {
    fn seq_id(&self) -> SeqId {
        self.0._seq
    }

    fn commit_seq(&self) -> u64 {
        self.0._commit_seq
    }

    fn deleted(&self) -> bool {
        self.0._deleted
    }

    fn pk_value(&self, pk_name: &str) -> Option<String> {
        self.0.fields.get(pk_name).and_then(|val| {
            if val.is_null() {
                None
            } else {
                match val {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
                    _ => Some(val.to_string()),
                }
            }
        })
    }
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

    /// Get the primary key field name
    pub fn primary_key_field_name(&self) -> &str {
        self.core.primary_key_field_name()
    }

    /// Access the underlying indexed store (used by flush jobs)
    pub fn store(&self) -> Arc<UserTableIndexedStore> {
        Arc::clone(&self.store)
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

        let ops_by_column = crate::utils::vector_staging::build_vector_upsert_batch_ops(
            self.core.table_id(),
            self.primary_key_field_name(),
            &self.vector_columns,
            std::iter::once((seq, row)),
            |(_, row)| row,
            |(seq, _), pk| UserVectorHotOpId::new(user_id.clone(), *seq, pk.to_string()),
        )?;
        crate::utils::vector_staging::stage_vector_ops_by_column(
            &self.vector_stores,
            ops_by_column,
            "stage vector upsert",
        )
        .await
    }

    async fn stage_vector_upsert_batch(
        &self,
        user_id: &UserId,
        entries: &[(UserTableRowId, UserTableRow)],
    ) -> Result<(), KalamDbError> {
        if self.vector_columns.is_empty() || entries.is_empty() {
            return Ok(());
        }

        let ops_by_column = crate::utils::vector_staging::build_vector_upsert_batch_ops(
            self.core.table_id(),
            self.primary_key_field_name(),
            &self.vector_columns,
            entries.iter(),
            |(_, entity)| &entity.fields,
            |(row_key, _), pk| UserVectorHotOpId::new(user_id.clone(), row_key.seq, pk.to_string()),
        )?;
        crate::utils::vector_staging::stage_vector_ops_by_column(
            &self.vector_stores,
            ops_by_column,
            "batch stage vector upsert",
        )
        .await
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

        let ops_by_column = crate::utils::vector_staging::build_vector_delete_ops(
            self.core.table_id(),
            &self.vector_columns,
            pk,
            |primary_key| UserVectorHotOpId::new(user_id.clone(), seq, primary_key.to_string()),
        );
        crate::utils::vector_staging::stage_vector_ops_by_column(
            &self.vector_stores,
            ops_by_column,
            "stage vector delete",
        )
        .await
    }

    async fn append_hot_row(
        &self,
        row_key: &UserTableRowId,
        entity: &UserTableRow,
        error_context: &str,
    ) -> Result<(), KalamDbError> {
        let store = self.store.clone();
        let row_key = row_key.clone();
        let entity = entity.clone();
        let error_context = error_context.to_string();

        tokio::task::spawn_blocking(move || -> Result<(), KalamDbError> {
            let encoded_values =
                kalamdb_commons::serialization::row_codec::batch_encode_user_table_rows(
                    std::slice::from_ref(&entity),
                )
                .map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to batch encode user table row: {}",
                        e
                    ))
                })?;

            let entries = vec![(row_key, entity)];
            store
                .insert_batch_preencoded(&entries, encoded_values)
                .map_err(|e| KalamDbError::InvalidOperation(format!("{}: {}", error_context, e)))
        })
        .await
        .map_err(|e| KalamDbError::InvalidOperation(format!("spawn_blocking error: {}", e)))??;

        Ok(())
    }

    pub async fn validate_insert_batch_rows<'a, I>(
        &self,
        user_id: &UserId,
        rows: I,
    ) -> Result<(), KalamDbError>
    where
        I: IntoIterator<Item = &'a Row>,
    {
        let pk_name = self.primary_key_field_name();
        let mut pk_values_to_check: Vec<(String, ScalarValue)> = Vec::new();
        let mut seen_batch_pks = HashSet::new();

        for row_data in rows {
            if let Some(pk_value) = row_data.get(pk_name) {
                if !matches!(pk_value, ScalarValue::Null) {
                    let pk_str =
                        crate::utils::unified_dml::extract_user_pk_value(row_data, pk_name)?;
                    if !seen_batch_pks.insert(pk_str.clone()) {
                        return Err(KalamDbError::AlreadyExists(format!(
                            "Primary key violation: value '{}' appears multiple times in the \
                             insert batch for column '{}'",
                            pk_str, pk_name
                        )));
                    }
                    pk_values_to_check.push((pk_str, pk_value.clone()));
                }
            }
        }

        if pk_values_to_check.is_empty() {
            return Ok(());
        }

        let pk_prefixes: Vec<(String, Vec<u8>)> = pk_values_to_check
            .iter()
            .map(|(pk_str, pk_value)| {
                (pk_str.clone(), self.pk_index.build_prefix_for_pk(user_id, pk_value))
            })
            .collect();

        let store = self.store.clone();
        let hot_duplicate =
            tokio::task::spawn_blocking(move || -> Result<Option<String>, KalamDbError> {
                for (pk_str, prefix) in &pk_prefixes {
                    if let Some((_row_id, row)) =
                        store.get_latest_by_index_prefix(0, prefix).map_err(|e| {
                            KalamDbError::InvalidOperation(format!("PK index scan failed: {}", e))
                        })?
                    {
                        if !row._deleted {
                            return Ok(Some(pk_str.clone()));
                        }
                    }
                }
                Ok(None)
            })
            .await
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!("spawn_blocking error: {}", e))
            })??;

        if let Some(dup_pk) = hot_duplicate {
            return Err(KalamDbError::AlreadyExists(format!(
                "Primary key violation: value '{}' already exists in column '{}'",
                dup_pk, pk_name
            )));
        }

        let pk_column_id = self.core.primary_key_column_id();
        let pk_values: Vec<String> = pk_values_to_check.iter().map(|(pk, _)| pk.clone()).collect();
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

        Ok(())
    }

    async fn persist_insert_batch_rows(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
        validate_unique_pk: bool,
        commit_seq: u64,
    ) -> Result<Vec<(UserTableRowId, UserTableRow)>, KalamDbError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        ensure_manifest_ready(
            &self.core,
            self.core.table_type(),
            Some(user_id),
            "UserTableProvider",
        )?;

        let coerced_rows = coerce_rows(rows, &self.schema_ref()).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
        })?;

        crate::utils::datafusion_dml::validate_not_null_with_set(
            self.core.non_null_columns(),
            &coerced_rows,
        )
        .map_err(|e| KalamDbError::ConstraintViolation(e.to_string()))?;

        let row_count = coerced_rows.len();

        if validate_unique_pk {
            self.validate_insert_batch_rows(user_id, coerced_rows.iter()).await?;
        }

        let sys_cols = self.core.services.system_columns.clone();
        let seq_ids = sys_cols.generate_seq_ids(row_count).map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId batch generation failed: {}", e))
        })?;

        let mut user_rows: Vec<UserTableRow> = Vec::with_capacity(row_count);
        let mut row_keys: Vec<UserTableRowId> = Vec::with_capacity(row_count);

        for (row_data, seq_id) in coerced_rows.into_iter().zip(seq_ids.into_iter()) {
            row_keys.push(UserTableRowId::new(user_id.clone(), seq_id));
            user_rows.push(UserTableRow {
                user_id: user_id.clone(),
                _seq: seq_id,
                _commit_seq: commit_seq,
                _deleted: false,
                fields: row_data,
            });
        }

        let store = self.store.clone();
        let entries: Vec<(UserTableRowId, UserTableRow)> =
            row_keys.iter().cloned().zip(user_rows.into_iter()).collect();

        let entries = tokio::task::spawn_blocking(
            move || -> Result<Vec<(UserTableRowId, UserTableRow)>, KalamDbError> {
                let encode_input: Vec<&UserTableRow> = entries.iter().map(|(_, row)| row).collect();
                let encoded_values =
                    kalamdb_commons::serialization::row_codec::batch_encode_user_table_row_refs(
                        &encode_input,
                    )
                    .map_err(|e| {
                        KalamDbError::InvalidOperation(format!(
                            "Failed to batch encode user table rows: {}",
                            e
                        ))
                    })?;
                store.insert_batch_preencoded(&entries, encoded_values).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to batch insert user table rows: {}",
                        e
                    ))
                })?;
                Ok(entries)
            },
        )
        .await
        .map_err(|e| KalamDbError::InvalidOperation(format!("spawn_blocking error: {}", e)))??;

        if let Err(e) = self.stage_vector_upsert_batch(user_id, &entries).await {
            log::warn!(
                "Failed to batch stage vector upserts for table={}, user={}: {}",
                self.core.table_id(),
                user_id.as_str(),
                e
            );
        }

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

        Ok(entries)
    }

    /// Build a complete Row from UserTableRow including system columns (_seq, _deleted)
    ///
    /// This ensures live query notifications include all columns, not just user-defined fields.
    fn build_notification_row(entity: &UserTableRow) -> Row {
        base::build_notification_row(
            &entity.fields,
            entity._seq,
            entity._commit_seq,
            entity._deleted,
        )
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
            .scan_by_index_async(0, Some(prefix), None)
            .await
            .into_kalamdb_error("PK index scan failed")
            .map(|entries| entries.into_iter().max_by_key(|(row_id, _)| row_id.seq))
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

    pub async fn patch_commit_seq_for_row_key(
        &self,
        row_key: &UserTableRowId,
        commit_seq: u64,
    ) -> Result<(), KalamDbError> {
        let mut row = self
            .store
            .get(row_key)
            .into_kalamdb_error("Failed to load row for commit_seq patch")?
            .ok_or_else(|| {
                KalamDbError::NotFound(format!(
                    "row '{}' not found while patching commit_seq",
                    row_key.seq
                ))
            })?;
        row._commit_seq = commit_seq;
        self.store.insert_async(row_key.clone(), row).await.map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to patch commit_seq: {}", e))
        })
    }

    pub async fn patch_latest_commit_seq_by_pk(
        &self,
        user_id: &UserId,
        pk_value: &str,
        commit_seq: u64,
    ) -> Result<bool, KalamDbError> {
        let schema = self.schema_ref();
        let pk_field = schema.field_with_name(self.primary_key_field_name()).map_err(|e| {
            KalamDbError::InvalidOperation(format!("PK column lookup failed: {}", e))
        })?;
        let pk_scalar =
            kalamdb_commons::conversions::parse_string_as_scalar(pk_value, pk_field.data_type())
                .map_err(KalamDbError::InvalidOperation)?;

        let Some((row_key, _)) = self.latest_hot_pk_entry(user_id, &pk_scalar).await? else {
            return Ok(false);
        };

        self.patch_commit_seq_for_row_key(&row_key, commit_seq).await?;
        Ok(true)
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
    /// Lists all *.parquet files in the user's storage directory and merges them into a single
    /// RecordBatch. Returns an empty batch if no Parquet files exist.
    ///
    /// **Phase 4 (US6, T082-T084)**: Integrated with ManifestService for manifest caching.
    /// Logs cache hits/misses and updates last_accessed timestamp. Full query optimization
    /// (batch file pruning based on manifest metadata) implemented in Phase 5 (US2, T119-T123).
    async fn scan_parquet_files_as_batch_async(
        &self,
        user_id: &UserId,
        filter: Option<&Expr>,
        columns: Option<&[String]>,
    ) -> Result<RecordBatch, KalamDbError> {
        base::scan_parquet_files_as_batch_async(
            &self.core,
            self.core.table_id(),
            self.core.table_type(),
            Some(user_id),
            self.schema_ref(),
            filter,
            columns,
        )
        .await
    }

    fn construct_mvcc_row_from_parquet_data(
        &self,
        user_id: &UserId,
        row_data: crate::utils::version_resolution::ParquetRowData,
    ) -> DataFusionResult<(UserTableRowId, UserMvccRow)> {
        self.construct_row_from_parquet_data(user_id, &row_data)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .map(|(row_id, row)| (row_id, UserMvccRow(row)))
            .ok_or_else(|| {
                DataFusionError::Execution("missing user row from parquet data".to_string())
            })
    }

    /// Async version of scan_all_users_with_version_resolution to avoid blocking the async runtime.
    async fn scan_all_users_with_version_resolution_async(
        &self,
        filter: Option<&Expr>,
        limit: Option<usize>,
        keep_deleted: bool,
        snapshot_commit_seq: Option<u64>,
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

        let mut hot_rows_by_user: HashMap<UserId, Vec<(UserTableRowId, UserTableRow)>> =
            HashMap::new();
        let mut user_ids = HashSet::new();
        for (row_id, row) in hot_rows {
            user_ids.insert(row.user_id.clone());
            hot_rows_by_user.entry(row.user_id.clone()).or_default().push((row_id, row));
        }

        if let Ok(scopes) = self.core.services.manifest_service.get_manifest_user_ids(table_id) {
            user_ids.extend(scopes);
        }

        if let Some(user_id) = fallback_user_id {
            user_ids.insert(user_id.clone());
        }

        let pk_name = self.primary_key_field_name().to_string();
        let mut result = Vec::new();

        for user_id in user_ids {
            // Use async version to avoid blocking the runtime
            let parquet_batch =
                self.scan_parquet_files_as_batch_async(&user_id, filter, None).await?;
            let hot_rows = hot_rows_by_user.remove(&user_id).unwrap_or_default();
            let resolved: Vec<(UserTableRowId, UserMvccRow)> = resolve_latest_kvs_from_cold_batch(
                &pk_name,
                hot_rows.into_iter().map(|(row_id, row)| (row_id, UserMvccRow(row))),
                &parquet_batch,
                keep_deleted,
                snapshot_commit_seq,
                |row_data| self.construct_mvcc_row_from_parquet_data(&user_id, row_data),
            )
            .map_err(|error| KalamDbError::DataFusion(error.to_string()))?;
            result.extend(resolved.into_iter().map(|(row_id, row)| (row_id, row.0)));
        }

        base::apply_limit(&mut result, limit);

        Ok(result)
    }

    async fn collect_matching_rows_for_subject(
        &self,
        state: &dyn Session,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
    ) -> DataFusionResult<Vec<Row>> {
        crate::utils::datafusion_dml::collect_matching_rows_with_projection(
            self, state, filters, projection,
        )
        .await
    }
}

#[derive(Clone)]
pub struct UserScanContext {
    user_id: UserId,
    allow_all_users: bool,
    snapshot_commit_seq: Option<u64>,
}

#[async_trait]
impl DeferredMvccScanProvider<UserTableRowId, UserTableRow> for UserTableProvider {
    type ScanContext = UserScanContext;

    fn scan_source_name(&self) -> &'static str {
        "user_table_scan"
    }

    fn build_scan_context(&self, state: &dyn Session) -> Result<Self::ScanContext, KalamDbError> {
        let (user_id, role) = extract_user_context(state)?;
        Ok(UserScanContext {
            user_id: user_id.clone(),
            allow_all_users: can_read_all_users(role),
            snapshot_commit_seq: extract_transaction_query_context(state)
                .map(|context| context.snapshot_commit_seq),
        })
    }

    fn scan_snapshot_commit_seq(&self, scan_context: &Self::ScanContext) -> Option<u64> {
        scan_context.snapshot_commit_seq
    }

    fn allow_pk_fast_path(&self, scan_context: &Self::ScanContext) -> bool {
        !scan_context.allow_all_users && scan_context.snapshot_commit_seq.is_none()
    }

    fn allow_count_only_fast_path(&self, scan_context: &Self::ScanContext) -> bool {
        !scan_context.allow_all_users
    }

    fn scan_scope_label(&self, scan_context: &Self::ScanContext) -> &'static str {
        if scan_context.allow_all_users {
            "all-users"
        } else {
            "subject"
        }
    }

    fn scan_cold_scope<'a>(&self, scan_context: &'a Self::ScanContext) -> Option<&'a UserId> {
        Some(&scan_context.user_id)
    }

    async fn scan_hot_pk_row(
        &self,
        scan_context: &Self::ScanContext,
        pk_value: &ScalarValue,
    ) -> Result<Option<(UserTableRowId, UserTableRow)>, KalamDbError> {
        self.find_by_pk(&scan_context.user_id, pk_value).await
    }

    async fn hot_pk_tombstoned(
        &self,
        scan_context: &Self::ScanContext,
        pk_value: &ScalarValue,
    ) -> Result<bool, KalamDbError> {
        self.pk_tombstoned_in_hot(&scan_context.user_id, pk_value).await
    }

    async fn count_rows_with_context(
        &self,
        scan_context: &Self::ScanContext,
    ) -> Result<usize, KalamDbError> {
        self.count_resolved_rows_async(&scan_context.user_id, scan_context.snapshot_commit_seq)
            .await
    }

    async fn scan_kvs_with_context(
        &self,
        scan_context: &Self::ScanContext,
        filter: Option<&Expr>,
        since_seq: Option<SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
        cold_columns: Option<&[String]>,
    ) -> Result<Vec<(UserTableRowId, UserTableRow)>, KalamDbError> {
        let user_id = &scan_context.user_id;
        if scan_context.allow_all_users {
            self.scan_all_users_with_version_resolution_async(
                filter,
                limit,
                keep_deleted,
                scan_context.snapshot_commit_seq,
                Some(user_id),
            )
            .await
        } else {
            self.scan_with_version_resolution_to_kvs_async(
                user_id,
                filter,
                since_seq,
                limit,
                keep_deleted,
                cold_columns,
                scan_context.snapshot_commit_seq,
            )
            .await
        }
    }
}

impl SourceProvider for UserTableProvider {
    fn filter_capability(&self, filter: &Expr) -> FilterCapability {
        mvcc_filter_capability(filter, self.primary_key_field_name())
    }

    fn scan_descriptor(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> ScanDescriptor {
        merged_projection_scan_descriptor(self.schema_ref(), projection, filters, limit)
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
            _commit_seq: row_data.commit_seq,
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
    /// OPTIMIZED: Uses `pk_exists_in_hot` for fast hot-path check (single index lookup + 1 entity
    /// fetch max). OPTIMIZED: Uses `pk_exists_in_cold` with manifest-based segment pruning for
    /// cold storage.
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
                _commit_seq: 0,
                _deleted: false,
                fields: row_data,
            };

            // Create composite key
            let row_key = UserTableRowId::new(user_id.clone(), seq_id);

            // log::info!("🔍 [AS_USER_DEBUG] Inserting row for user_id='{}' _seq={}",
            //            user_id.as_str(), seq_id);

            // Use the same preencoded append path as batch inserts so single-row
            // MVCC writes and batch writes stay consistent.
            self.append_hot_row(&row_key, &entity, "Failed to insert user table row")
                .await?;

            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "Inserted user table row for user {} with _seq {}",
                    user_id.as_str(),
                    seq_id
                );
            }

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
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id))
            {
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
        let commit_seq = self.core.services.commit_sequence_source.allocate_next();
        self.insert_batch_with_commit_seq(user_id, rows, commit_seq).await
    }

    async fn update(
        &self,
        user_id: &UserId,
        key: &UserTableRowId,
        updates: Row,
    ) -> Result<Option<UserTableRowId>, KalamDbError> {
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
                    _commit_seq: row_data.commit_seq,
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

        // Validate PK update (check if new PK value already exists) — only needed when updating by
        // key
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
    ) -> Result<Option<UserTableRowId>, KalamDbError> {
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
            // First try hot storage (O(1) via PK index), then fall back to cold storage (Parquet
            // scan)
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
                    if log::log_enabled!(log::Level::Debug) {
                        log::debug!(
                            "[UPDATE] PK {} not found in hot storage, querying cold storage for \
                             user={}, pk={}",
                            pk_name,
                            user_id.as_str(),
                            pk_value
                        );
                    }
                    base::find_row_by_pk(self, Some(user_id), pk_value).await?.ok_or_else(|| {
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
                // if log::log_enabled!(log::Level::Debug) {
                //     log::debug!(
                //         table_id = %self.core.table_id(),
                //         pk = pk_value,
                //         "table.update_noop: row unchanged, skipping write"
                //     );
                // }
                return Ok(None);
            }

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
                _commit_seq: 0,
                _deleted: false,
                fields: new_fields,
            };
            let row_key = UserTableRowId::new(user_id.clone(), seq_id);
            self.append_hot_row(&row_key, &entity, "Failed to update user table row")
                .await?;

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
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id))
            {
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
                    let notification = ChangeNotification::update(
                        table_id.clone(),
                        old_row,
                        new_row,
                        vec![pk_col],
                    );
                    notification_service.notify_table_change(
                        Some(user_id.clone()),
                        table_id,
                        notification,
                    );
                }
            }
            Ok(Some(row_key))
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
                    _commit_seq: row_data.commit_seq,
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
            // First try hot storage (O(1) via PK index), then fall back to cold storage (Parquet
            // scan)
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
                _commit_seq: 0,
                _deleted: true,
                fields: Row::new(values),
            };
            let row_key = UserTableRowId::new(user_id.clone(), seq_id);

            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "[UserProvider DELETE_BY_PK] Writing tombstone: user={}, pk={}, _seq={}",
                    user_id.as_str(),
                    pk_value,
                    seq_id.as_i64()
                );
            }
            self.append_hot_row(&row_key, &entity, "Failed to delete user table row")
                .await?;

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
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id))
            {
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
        let scan_context = self.build_scan_context(state)?;
        self.scan_rows_with_context(&scan_context, projection, filter, limit).await
    }

    async fn scan_with_version_resolution_to_kvs_async(
        &self,
        user_id: &UserId,
        filter: Option<&Expr>,
        since_seq: Option<kalamdb_commons::ids::SeqId>,
        limit: Option<usize>,
        keep_deleted: bool,
        cold_columns: Option<&[String]>,
        snapshot_commit_seq: Option<u64>,
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

        // Calculate scan limit using common helper.
        // The scan is already prefix-scoped to user_id, so the 2× buffer inside
        // calculate_scan_limit is sufficient (covers version duplicates + tombstones).
        let scan_limit = base::calculate_scan_limit(limit);

        // Run hot storage (RocksDB) and cold storage (Parquet) scans concurrently
        let hot_future = async {
            self.store
                .scan_with_raw_prefix_async(&user_prefix, start_key_bytes.as_deref(), scan_limit)
                .await
                .map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to scan user table hot storage: {}",
                        e
                    ))
                })
                .map(|rows| {
                    rows.into_iter()
                        .map(|(row_id, row)| (row_id, UserMvccRow(row)))
                        .collect::<Vec<_>>()
                })
        };
        let cold_future = self.scan_parquet_files_as_batch_async(user_id, filter, cold_columns);
        let pk_name = self.primary_key_field_name().to_string();
        let resolved = base::resolve_latest_scan_from_futures(
            &pk_name,
            limit,
            keep_deleted,
            snapshot_commit_seq,
            hot_future,
            cold_future,
            |row_data| self.construct_mvcc_row_from_parquet_data(user_id, row_data),
        )
        .await?;

        // log::trace!(
        //     "[UserProvider] Hot scan: {} rows for user={} (table={})",
        //     resolved.hot_rows_scanned,
        //     user_id.as_str(),
        //     table_id
        // );

        // if log::log_enabled!(log::Level::Trace) {
        //     log::trace!(
        //         "[UserProvider] Cold scan: {} Parquet rows (table={}; user={})",
        //         resolved.cold_rows_scanned,
        //         table_id,
        //         user_id.as_str()
        //     );
        // }

        let result: Vec<(UserTableRowId, UserTableRow)> =
            resolved.rows.into_iter().map(|(row_id, row)| (row_id, row.0)).collect();

        if log::log_enabled!(log::Level::Trace) {
            log::trace!(
                "[UserProvider] Final version-resolved (post-tombstone): {} rows (table={}; \
                 user={})",
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

impl UserTableProvider {
    /// Count resolved rows without materializing full row data (single user).
    ///
    /// Used for COUNT(*) queries where projection is empty. Only decodes
    /// metadata (seq, deleted, pk) to perform version resolution.
    async fn count_resolved_rows_async(
        &self,
        user_id: &UserId,
        snapshot_commit_seq: Option<u64>,
    ) -> Result<usize, KalamDbError> {
        use kalamdb_commons::serialization::row_codec::decode_user_table_row_metadata;

        let pk_name = self.primary_key_field_name().to_string();
        let store = Arc::clone(&self.store);
        let user_prefix = UserTableRowId::user_prefix(user_id);
        let pk_name_clone = pk_name.clone();

        // Hot storage: scan raw bytes with user prefix, decode metadata only
        let hot_future = tokio::task::spawn_blocking(move || {
            let partition = store.partition();
            let iter = store
                .backend()
                .scan(&partition, Some(&user_prefix), None, Some(1_000_000))
                .map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to scan user table hot storage for count: {}",
                        e
                    ))
                })?;

            let mut hot_metadata = Vec::new();
            for (_key_bytes, value_bytes) in iter {
                match decode_user_table_row_metadata(&value_bytes, &pk_name_clone) {
                    Ok((_uid, metadata)) => hot_metadata.push(metadata),
                    Err(e) => {
                        log::warn!("Skipping row with malformed metadata: {}", e);
                        continue;
                    },
                }
            }
            Ok::<_, KalamDbError>(hot_metadata)
        });

        // Cold storage: project only the PK + MVCC metadata needed for counting.
        let cold_columns = base::compute_metadata_only_cold_columns(&pk_name);
        let cold_future =
            self.scan_parquet_files_as_batch_async(user_id, None, Some(cold_columns.as_slice()));

        let hot_future = async {
            hot_future.await.map_err(|e| {
                KalamDbError::InvalidOperation(format!("spawn_blocking join error: {}", e))
            })?
        };

        base::count_resolved_rows_from_futures(
            &pk_name,
            snapshot_commit_seq,
            hot_future,
            cold_future,
        )
        .await
    }

    async fn insert_deferred_internal(
        &self,
        user_id: &UserId,
        row_data: Row,
        validate_unique_pk: bool,
    ) -> Result<(UserTableRowId, Option<ChangeNotification>), KalamDbError> {
        let span = tracing::debug_span!(
            "table.insert",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            column_count = row_data.values.len(),
            deferred_side_effects = true
        );
        async move {
            ensure_manifest_ready(
                &self.core,
                self.core.table_type(),
                Some(user_id),
                "UserTableProvider",
            )?;

            if validate_unique_pk {
                base::ensure_unique_pk_value(self, Some(user_id), &row_data).await?;
            }

            let sys_cols = self.core.services.system_columns.clone();
            let seq_id = sys_cols.generate_seq_id().map_err(|e| {
                KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
            })?;

            let entity = UserTableRow {
                user_id: user_id.clone(),
                _seq: seq_id,
                _commit_seq: 0,
                _deleted: false,
                fields: row_data,
            };

            let row_key = UserTableRowId::new(user_id.clone(), seq_id);
            self.append_hot_row(&row_key, &entity, "Failed to insert user table row")
                .await?;

            if let Err(e) = self.stage_vector_upsert(user_id, seq_id, &entity.fields).await {
                log::warn!(
                    "Failed to stage vector upsert for table={}, user={}, seq={}: {}",
                    self.core.table_id(),
                    user_id.as_str(),
                    seq_id.as_i64(),
                    e
                );
            }

            let manifest_service = self.core.services.manifest_service.clone();
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id))
            {
                log::warn!(
                    "Failed to mark manifest as pending_write for {}: {}",
                    self.core.table_id(),
                    e
                );
            }

            let notification_service = self.core.services.notification_service.clone();
            let table_id = self.core.table_id().clone();
            let has_topics = self.core.has_topic_routes(&table_id);
            let has_live_subs = notification_service.has_subscribers(Some(user_id), &table_id);
            let notification = if has_topics || has_live_subs {
                Some(ChangeNotification::insert(table_id, Self::build_notification_row(&entity)))
            } else {
                None
            };

            Ok((row_key, notification))
        }
        .instrument(span)
        .await
    }

    pub async fn insert_deferred(
        &self,
        user_id: &UserId,
        row_data: Row,
    ) -> Result<(UserTableRowId, Option<ChangeNotification>), KalamDbError> {
        self.insert_deferred_internal(user_id, row_data, true).await
    }

    pub async fn insert_deferred_prevalidated(
        &self,
        user_id: &UserId,
        row_data: Row,
    ) -> Result<(UserTableRowId, Option<ChangeNotification>), KalamDbError> {
        self.insert_deferred_internal(user_id, row_data, false).await
    }

    pub async fn insert_batch_with_commit_seq(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
        commit_seq: u64,
    ) -> Result<Vec<UserTableRowId>, KalamDbError> {
        let row_count = rows.len();
        let span = tracing::debug_span!(
            "table.insert_batch",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            row_count
        );
        async move {
            let entries = self.persist_insert_batch_rows(user_id, rows, true, commit_seq).await?;
            let row_keys: Vec<UserTableRowId> =
                entries.iter().map(|(row_key, _)| row_key.clone()).collect();

            let notification_service = self.core.services.notification_service.clone();
            let table_id = self.core.table_id().clone();

            let has_topics = self.core.has_topic_routes(&table_id);
            let has_live_subs = notification_service.has_subscribers(Some(user_id), &table_id);
            if has_topics || has_live_subs {
                let rows: Vec<_> = entries
                    .iter()
                    .map(|(_row_key, entity)| Self::build_notification_row(entity))
                    .collect();

                if has_topics {
                    self.core
                        .publish_batch_to_topics(
                            &table_id,
                            kalamdb_commons::models::TopicOp::Insert,
                            &rows,
                            Some(user_id),
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

    pub async fn insert_batch_deferred_prevalidated(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
    ) -> Result<Vec<(UserTableRowId, Option<ChangeNotification>)>, KalamDbError> {
        let commit_seq = self.core.services.commit_sequence_source.allocate_next();
        self.insert_batch_deferred_prevalidated_with_commit_seq(user_id, rows, commit_seq)
            .await
    }

    pub async fn insert_batch_deferred_prevalidated_with_commit_seq(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
        commit_seq: u64,
    ) -> Result<Vec<(UserTableRowId, Option<ChangeNotification>)>, KalamDbError> {
        let row_count = rows.len();
        let span = tracing::debug_span!(
            "table.insert_batch",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            row_count,
            deferred_side_effects = true
        );
        async move {
            let entries = self.persist_insert_batch_rows(user_id, rows, false, commit_seq).await?;

            let notification_service = self.core.services.notification_service.clone();
            let table_id = self.core.table_id().clone();
            let has_topics = self.core.has_topic_routes(&table_id);
            let has_live_subs = notification_service.has_subscribers(Some(user_id), &table_id);

            Ok(entries
                .into_iter()
                .map(|(row_key, entity)| {
                    let notification = if has_topics || has_live_subs {
                        Some(ChangeNotification::insert(
                            table_id.clone(),
                            Self::build_notification_row(&entity),
                        ))
                    } else {
                        None
                    };
                    (row_key, notification)
                })
                .collect())
        }
        .instrument(span)
        .await
    }

    pub async fn update_by_pk_value_deferred(
        &self,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
        commit_seq: u64,
    ) -> Result<Option<(UserTableRowId, Option<ChangeNotification>)>, KalamDbError> {
        let span = tracing::debug_span!(
            "table.update",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            pk = pk_value,
            deferred_side_effects = true
        );
        async move {
            let schema = self.schema();
            let updates = coerce_updates(updates, &schema).map_err(|e| {
                KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
            })?;

            let pk_name = self.primary_key_field_name().to_string();
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

            let (_latest_key, latest_row) =
                if let Some(result) = self.find_by_pk(user_id, &pk_value_scalar).await? {
                    result
                } else if self.pk_tombstoned_in_hot(user_id, &pk_value_scalar).await? {
                    return Err(KalamDbError::NotFound(format!(
                        "Row with {}={} was deleted",
                        pk_name, pk_value
                    )));
                } else {
                    base::find_row_by_pk(self, Some(user_id), pk_value).await?.ok_or_else(|| {
                        KalamDbError::NotFound(format!(
                            "Row with {}={} not found (checked both hot and cold storage)",
                            pk_name, pk_value
                        ))
                    })?
                };

            let mut merged = latest_row.fields.values.clone();
            for (key, value) in updates.values {
                merged.insert(key, value);
            }
            let new_fields = Row::new(merged);

            crate::utils::datafusion_dml::validate_not_null_with_set(
                self.core.non_null_columns(),
                &[new_fields.clone()],
            )
            .map_err(|e| KalamDbError::ConstraintViolation(e.to_string()))?;

            if new_fields == latest_row.fields {
                tracing::debug!(
                    table_id = %self.core.table_id(),
                    user_id = %user_id.as_str(),
                    pk = pk_value,
                    "table.update_noop: row unchanged, skipping write"
                );
                return Ok(None);
            }

            let sys_cols = self.core.services.system_columns.clone();
            let seq_id = sys_cols.generate_seq_id().map_err(|e| {
                KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
            })?;
            let entity = UserTableRow {
                user_id: user_id.clone(),
                _seq: seq_id,
                _commit_seq: commit_seq,
                _deleted: false,
                fields: new_fields,
            };
            let row_key = UserTableRowId::new(user_id.clone(), seq_id);
            self.append_hot_row(&row_key, &entity, "Failed to update user table row")
                .await?;

            if let Err(e) = self.stage_vector_upsert(user_id, seq_id, &entity.fields).await {
                log::warn!(
                    "Failed to stage vector upsert for table={}, user={}, seq={}: {}",
                    self.core.table_id(),
                    user_id.as_str(),
                    seq_id.as_i64(),
                    e
                );
            }

            let manifest_service = self.core.services.manifest_service.clone();
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id))
            {
                log::warn!(
                    "Failed to mark manifest as pending_write for {}: {}",
                    self.core.table_id(),
                    e
                );
            }

            let notification_service = self.core.services.notification_service.clone();
            let table_id = self.core.table_id().clone();
            let has_topics = self.core.has_topic_routes(&table_id);
            let has_live_subs = notification_service.has_subscribers(Some(user_id), &table_id);
            let notification = if has_topics || has_live_subs {
                let old_row = Self::build_notification_row(&latest_row);
                let new_row = Self::build_notification_row(&entity);
                Some(ChangeNotification::update(
                    table_id,
                    old_row,
                    new_row,
                    vec![self.primary_key_field_name().to_string()],
                ))
            } else {
                None
            };

            Ok(Some((row_key, notification)))
        }
        .instrument(span)
        .await
    }

    pub async fn delete_by_pk_value_deferred(
        &self,
        user_id: &UserId,
        pk_value: &str,
        commit_seq: u64,
    ) -> Result<Option<(UserTableRowId, Option<ChangeNotification>)>, KalamDbError> {
        let span = tracing::debug_span!(
            "table.delete",
            table_id = %self.core.table_id(),
            user_id = %user_id.as_str(),
            pk = pk_value,
            deferred_side_effects = true
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

            let latest_row =
                if let Some((_key, row)) = self.find_by_pk(user_id, &pk_value_scalar).await? {
                    row
                } else if self.pk_tombstoned_in_hot(user_id, &pk_value_scalar).await? {
                    return Ok(None);
                } else {
                    match base::find_row_by_pk(self, Some(user_id), pk_value).await? {
                        Some((_key, row)) => row,
                        None => return Ok(None),
                    }
                };

            let sys_cols = self.core.services.system_columns.clone();
            let seq_id = sys_cols.generate_seq_id().map_err(|e| {
                KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
            })?;

            let values = latest_row.fields.values.clone();
            let entity = UserTableRow {
                user_id: user_id.clone(),
                _seq: seq_id,
                _commit_seq: commit_seq,
                _deleted: true,
                fields: Row::new(values),
            };
            let row_key = UserTableRowId::new(user_id.clone(), seq_id);
            self.append_hot_row(&row_key, &entity, "Failed to delete user table row")
                .await?;

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

            let manifest_service = self.core.services.manifest_service.clone();
            if let Err(e) = manifest_service.mark_pending_write(self.core.table_id(), Some(user_id))
            {
                log::warn!(
                    "Failed to mark manifest as pending_write for {}: {}",
                    self.core.table_id(),
                    e
                );
            }

            let notification_service = self.core.services.notification_service.clone();
            let table_id = self.core.table_id().clone();
            let has_topics = self.core.has_topic_routes(&table_id);
            let has_live_subs = notification_service.has_subscribers(Some(user_id), &table_id);
            let notification = if has_topics || has_live_subs {
                Some(ChangeNotification::delete_soft(
                    table_id,
                    Self::build_notification_row(&entity),
                ))
            } else {
                None
            };

            Ok(Some((row_key, notification)))
        }
        .instrument(span)
        .await
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
        check_user_table_access(state, self.core.table_id())
            .map_err(session_error_to_datafusion)?;

        let table_overlay = extract_transaction_query_context(state)
            .and_then(|context| context.overlay_view.overlay_for_table(self.core.table_id()));
        let (user_id, _role) = extract_user_context(state)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;

        self.base_scan_with_overlay(
            state,
            projection,
            filters,
            limit,
            table_overlay,
            Some(user_id.clone()),
        )
        .await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        tracing::debug!(table_id = %self.core.table_id(), "table.insert_into");
        check_user_table_write_access(state, self.core.table_id())
            .map_err(session_error_to_datafusion)?;

        if insert_op != InsertOp::Append {
            return Err(DataFusionError::Plan(format!(
                "{} is not supported for user tables",
                insert_op
            )));
        }

        let (user_id, _role) =
            extract_user_context(state).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let rows = crate::utils::datafusion_dml::collect_input_rows(state, input).await?;
        if let Some(transaction_query_context) = extract_transaction_query_context(state) {
            let inserted = crate::utils::datafusion_dml::stage_insert_rows(
                transaction_query_context,
                self.core.table_id(),
                TableType::User,
                Some(user_id.clone()),
                self.primary_key_field_name(),
                rows,
            )?;

            return crate::utils::datafusion_dml::rows_affected_plan(state, inserted).await;
        }

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
            .map_err(session_error_to_datafusion)?;
        crate::utils::datafusion_dml::validate_where_clause(&filters, "DELETE")?;

        let (user_id, _role) =
            extract_user_context(state).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let pk_column = self.primary_key_field_name().to_string();
        let schema = self.schema_ref();
        let projection = crate::utils::datafusion_dml::dml_scan_projection(
            &schema,
            &filters,
            &[],
            &[&pk_column],
        )?;
        let rows = self
            .collect_matching_rows_for_subject(state, &filters, projection.as_ref())
            .await?;
        if rows.is_empty() {
            return crate::utils::datafusion_dml::rows_affected_plan(state, 0).await;
        }

        let transaction_query_context = extract_transaction_query_context(state);

        let mut seen = HashSet::new();
        let mut deleted: u64 = 0;
        let commit_seq = transaction_query_context
            .is_none()
            .then(|| self.core.services.commit_sequence_source.allocate_next());
        let mut staged_mutations =
            transaction_query_context.map(|_| Vec::with_capacity(rows.len()));

        for row in rows {
            let pk_value = crate::utils::datafusion_dml::extract_pk_value(&row, &pk_column)?;
            if !seen.insert(pk_value.clone()) {
                continue;
            }

            if let Some(staged_mutations) = staged_mutations.as_mut() {
                staged_mutations.push(StagedMutation::new(
                    transaction_query_context
                        .expect("transaction_query_context must exist when staging DELETE")
                        .transaction_id
                        .clone(),
                    self.core.table_id().clone(),
                    TableType::User,
                    Some(user_id.clone()),
                    OperationKind::Delete,
                    pk_value,
                    Row::new(std::collections::BTreeMap::new()),
                    true,
                ));
                deleted += 1;
                continue;
            }

            if self
                .delete_by_pk_value(user_id, &pk_value)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
            {
                let commit_seq = commit_seq.expect("commit_seq must exist for direct DELETE");
                let patched = self
                    .patch_latest_commit_seq_by_pk(user_id, &pk_value, commit_seq)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                if !patched {
                    return Err(DataFusionError::Execution(format!(
                        "Deleted row '{}' but failed to stamp commit sequence",
                        pk_value
                    )));
                }
                deleted += 1;
            }
        }

        if let (Some(transaction_query_context), Some(staged_mutations)) =
            (transaction_query_context, staged_mutations)
        {
            crate::utils::datafusion_dml::stage_transaction_mutations(
                transaction_query_context,
                staged_mutations,
            )?;
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
            .map_err(session_error_to_datafusion)?;
        crate::utils::datafusion_dml::validate_where_clause(&filters, "UPDATE")?;

        let pk_column = self.primary_key_field_name().to_string();
        crate::utils::datafusion_dml::validate_update_assignments(&assignments, &pk_column)?;

        let (user_id, _role) =
            extract_user_context(state).map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let schema = self.schema_ref();
        let projection = crate::utils::datafusion_dml::dml_scan_projection(
            &schema,
            &filters,
            &assignments,
            &[&pk_column],
        )?;
        let rows = self
            .collect_matching_rows_for_subject(state, &filters, projection.as_ref())
            .await?;
        if rows.is_empty() {
            return crate::utils::datafusion_dml::rows_affected_plan(state, 0).await;
        }

        let transaction_query_context = extract_transaction_query_context(state);

        let mut seen = HashSet::new();
        let mut updated: u64 = 0;
        let commit_seq = transaction_query_context
            .is_none()
            .then(|| self.core.services.commit_sequence_source.allocate_next());
        let mut staged_mutations =
            transaction_query_context.map(|_| Vec::with_capacity(rows.len()));

        for row in rows {
            let pk_value = crate::utils::datafusion_dml::extract_pk_value(&row, &pk_column)?;
            if !seen.insert(pk_value.clone()) {
                continue;
            }

            let evaluated_updates = crate::utils::datafusion_dml::evaluate_assignment_values(
                state,
                &schema,
                &row,
                &assignments,
            )?;
            if crate::utils::datafusion_dml::update_assignments_noop(
                &schema,
                &row,
                &evaluated_updates,
            )? {
                continue;
            }

            if let Some(staged_mutations) = staged_mutations.as_mut() {
                staged_mutations.push(StagedMutation::new(
                    transaction_query_context
                        .expect("transaction_query_context must exist when staging UPDATE")
                        .transaction_id
                        .clone(),
                    self.core.table_id().clone(),
                    TableType::User,
                    Some(user_id.clone()),
                    OperationKind::Update,
                    pk_value,
                    evaluated_updates,
                    false,
                ));
                updated += 1;
                continue;
            }

            let result = self
                .update_by_pk_value(user_id, &pk_value, evaluated_updates)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            if let Some(row_key) = result {
                let commit_seq = commit_seq.expect("commit_seq must exist for direct UPDATE");
                self.patch_commit_seq_for_row_key(&row_key, commit_seq)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                updated += 1;
            }
        }

        if let (Some(transaction_query_context), Some(staged_mutations)) =
            (transaction_query_context, staged_mutations)
        {
            crate::utils::datafusion_dml::stage_transaction_mutations(
                transaction_query_context,
                staged_mutations,
            )?;
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
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false), // no-op: row unchanged
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
