//! Stream table provider implementation with RLS + TTL
//!
//! This module provides StreamTableProvider implementing BaseTableProvider<StreamTableRowId, StreamTableRow>
//! for ephemeral event streams with Row-Level Security and TTL-based eviction.
//!
//! **Key Features**:
//! - Direct fields (no wrapper layer)
//! - Shared core via Arc<TableProviderCore>
//! - No handlers - all DML logic inline
//! - RLS via user_id parameter in DML methods
//! - Commit log-backed storage (append-only, no Parquet)
//! - TTL-based eviction in scan operations

use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::stream_tables::{StreamTableRow, StreamTableStore};
use crate::utils::base::{extract_seq_bounds_from_filter, BaseTableProvider, TableProviderCore};
use crate::utils::row_utils::extract_user_context;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::ids::{SeqId, StreamTableRowId};
use kalamdb_commons::models::UserId;
use kalamdb_session_datafusion::{check_user_table_write_access, session_error_to_datafusion};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// Arrow <-> JSON helpers
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::websocket::ChangeNotification;

/// Stream table provider with RLS and TTL filtering
///
/// **Architecture**:
/// - Stateless provider (user context passed per-operation)
/// - Direct fields (no wrapper layer)
/// - Shared core via Arc<TableProviderCore> (holds schema, pk_name, column_defaults, non_null_columns)
/// - RLS enforced via user_id parameter
/// - HOT-ONLY storage (ephemeral data, no Parquet)
/// - TTL-based eviction
pub struct StreamTableProvider {
    /// Shared core (services, schema, pk_name, column_defaults, non_null_columns)
    core: Arc<TableProviderCore>,

    /// StreamTableStore for DML operations (commit log-backed)
    store: Arc<StreamTableStore>,

    /// TTL in seconds (optional)
    ttl_seconds: Option<u64>,
}

impl StreamTableProvider {
    /// Create a new stream table provider
    ///
    /// # Arguments
    /// * `core` - Shared core with services, schema, pk_name, etc.
    /// * `store` - StreamTableStore for this table (hot-only)
    /// * `ttl_seconds` - Optional TTL for event eviction
    pub fn new(
        core: Arc<TableProviderCore>,
        store: Arc<StreamTableStore>,
        ttl_seconds: Option<u64>,
    ) -> Self {
        Self {
            core,
            store,
            ttl_seconds,
        }
    }

    /// Build a complete Row from StreamTableRow including system column (_seq)
    ///
    /// This ensures live query notifications include all columns, not just user-defined fields.
    /// Stream tables don't have _deleted column.
    fn build_notification_row(entity: &StreamTableRow) -> Row {
        let mut values = entity.fields.values.clone();
        values.insert(
            SystemColumnNames::SEQ.to_string(),
            ScalarValue::Int64(Some(entity._seq.as_i64())),
        );
        Row::new(values)
    }

    fn now_millis() -> Result<u64, KalamDbError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .into_invalid_operation("System time error")
            .map(|d| d.as_millis() as u64)
    }

    /// Expose the underlying store (used by maintenance jobs such as stream eviction)
    pub fn store_arc(&self) -> Arc<StreamTableStore> {
        self.store.clone()
    }
}

#[async_trait]
impl BaseTableProvider<StreamTableRowId, StreamTableRow> for StreamTableProvider {
    fn core(&self) -> &crate::utils::base::TableProviderCore {
        &self.core
    }

    fn construct_row_from_parquet_data(
        &self,
        user_id: &UserId,
        row_data: &crate::utils::version_resolution::ParquetRowData,
    ) -> Result<Option<(StreamTableRowId, StreamTableRow)>, KalamDbError> {
        let row_key = StreamTableRowId::new(user_id.clone(), row_data.seq_id);
        let row = StreamTableRow {
            user_id: user_id.clone(),
            _seq: row_data.seq_id,
            fields: row_data.fields.clone(),
        };
        Ok(Some((row_key, row)))
    }

    /// Stream tables are append-only and don't support UPDATE/DELETE by PK.
    /// This always returns None - DML operations other than INSERT are not supported.
    async fn find_row_key_by_id_field(
        &self,
        _user_id: &UserId,
        _id_value: &str,
    ) -> Result<Option<StreamTableRowId>, KalamDbError> {
        // Stream tables are append-only - no PK-based lookups for DML
        Ok(None)
    }

    async fn insert(
        &self,
        user_id: &UserId,
        row_data: Row,
    ) -> Result<StreamTableRowId, KalamDbError> {
        let table_id = self.core.table_id();

        // Call SystemColumnsService to generate SeqId
        let sys_cols = self.core.services.system_columns.clone();
        let seq_id = sys_cols.generate_seq_id().map_err(|e| {
            KalamDbError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;

        // Create StreamTableRow (no _deleted field for stream tables)
        let user_id = user_id.clone();
        let entity = StreamTableRow {
            user_id: user_id.clone(),
            _seq: seq_id,
            fields: row_data,
        };

        // Create composite key
        let row_key = StreamTableRowId::new(user_id.clone(), seq_id);

        // Store in commit log (append-only, no Parquet)
        self.store.put(&row_key, &entity).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to insert stream event: {}", e))
        })?;

        log::debug!(
            "[StreamProvider] Inserted event: table={} seq={} user={}",
            table_id,
            seq_id.as_i64(),
            user_id.as_str()
        );

        // Fire live query + topic notification (INSERT)
        let manager = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        let has_topics = self.core.has_topic_routes(&table_id);
        let has_live_subs = manager.has_subscribers(Some(&user_id), &table_id);
        if has_topics || has_live_subs {
            let table_name = table_id.full_name();

            // Build complete row including system column (_seq)
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
                log::debug!(
                    "[StreamProvider] Notifying change: table={} type=INSERT user={} seq={}",
                    table_name,
                    user_id.as_str(),
                    seq_id.as_i64()
                );
                manager.notify_table_change(Some(user_id.clone()), table_id, notification);
            }
        }

        Ok(row_key)
    }

    async fn update(
        &self,
        user_id: &UserId,
        _key: &StreamTableRowId,
        updates: Row,
    ) -> Result<Option<StreamTableRowId>, KalamDbError> {
        // TODO: Implement full UPDATE logic for stream tables
        // 1. Scan in-memory hot storage (no Parquet)
        // 2. Find row by key (user-scoped)
        // 3. Merge updates
        // 4. Append new version

        // Placeholder: Just append as new version (incomplete implementation)
        self.insert(user_id, updates).await.map(Some)
    }

    async fn update_by_pk_value(
        &self,
        user_id: &UserId,
        _pk_value: &str,
        updates: Row,
    ) -> Result<Option<StreamTableRowId>, KalamDbError> {
        // TODO: Implement full UPDATE logic for stream tables
        // Stream tables are typically append-only, so UPDATE just inserts a new event
        self.insert(user_id, updates).await.map(Some)
    }

    async fn delete(&self, user_id: &UserId, key: &StreamTableRowId) -> Result<(), KalamDbError> {
        // TODO: Implement DELETE logic for stream tables
        // Stream tables may use hard delete or tombstone depending on requirements

        // Placeholder: Delete from hot storage directly
        self.store.delete(key).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to delete stream event: {}", e))
        })?;

        // Fire live query notification (DELETE hard)
        let notification_service = self.core.services.notification_service.clone();
        let table_id = self.core.table_id().clone();

        if notification_service.has_subscribers(Some(&user_id), &table_id) {
            let row_id_str = format!("{}:{}", key.user_id().as_str(), key.seq().as_i64());
            let notification = ChangeNotification::delete_hard(table_id.clone(), row_id_str);
            notification_service.notify_table_change(Some(user_id.clone()), table_id, notification);
        }

        Ok(())
    }

    async fn delete_by_pk_value(
        &self,
        user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        // STREAM tables support DELETE by PK value for hard delete
        // PK column is typically an auto-generated ID (e.g., ULID(), event_id, etc.)

        // Scan all rows for this user to find matching PK values
        // Note: This is O(n) but STREAM tables are typically append-only with TTL eviction
        let rows = self.store.scan_user(user_id, None, usize::MAX).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to scan stream table keys: {}", e))
        })?;

        let pk_name = self.primary_key_field_name();
        let mut deleted_count = 0;

        for (key, entity) in rows {
            // Check if PK column matches the target value
            if let Some(row_pk_value) = entity.fields.get(pk_name) {
                let row_pk_str = match row_pk_value {
                    ScalarValue::Utf8(Some(s)) => s.clone(),
                    ScalarValue::Int64(Some(i)) => i.to_string(),
                    ScalarValue::Int32(Some(i)) => i.to_string(),
                    _ => continue,
                };

                if row_pk_str == pk_value {
                    // Delete this row
                    self.store.delete(&key).map_err(|e| {
                        KalamDbError::InvalidOperation(format!(
                            "Failed to delete stream event: {}",
                            e
                        ))
                    })?;

                    deleted_count += 1;

                    // Fire live query notification (DELETE hard)
                    let notification_service = self.core.services.notification_service.clone();
                    let table_id = self.core.table_id().clone();

                    if notification_service.has_subscribers(Some(&user_id), &table_id) {
                        let row_id_str =
                            format!("{}:{}", key.user_id().as_str(), key.seq().as_i64());
                        let notification =
                            ChangeNotification::delete_hard(table_id.clone(), row_id_str);
                        notification_service.notify_table_change(
                            Some(user_id.clone()),
                            table_id,
                            notification,
                        );
                    }
                }
            }
        }

        Ok(deleted_count > 0)
    }

    async fn scan_rows(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filter: Option<&Expr>,
        limit: Option<usize>,
    ) -> Result<RecordBatch, KalamDbError> {
        // Extract user_id from SessionState for RLS
        let (user_id, _role) = extract_user_context(state)?;

        // Extract sequence bounds from filter to optimize scan
        let (since_seq, _until_seq) = if let Some(expr) = filter {
            extract_seq_bounds_from_filter(expr)
        } else {
            (None, None)
        };

        // Perform KV scan (hot-only) and convert to batch
        let keep_deleted = false; // Stream tables don't support soft delete yet
        let kvs = self
            .scan_with_version_resolution_to_kvs_async(
                user_id,
                filter,
                since_seq,
                limit,
                keep_deleted,
                None,
                None,
            )
            .await?;
        let table_id = self.core.table_id();
        log::debug!(
            "[StreamProvider] scan_rows: table={} rows={} user={} ttl={:?}",
            table_id,
            kvs.len(),
            user_id.as_str(),
            self.ttl_seconds
        );

        let schema = self.schema_ref();
        crate::utils::base::rows_to_arrow_batch(&schema, kvs, projection, |row_values, row| {
            if self.core.schema_ref().field_with_name("user_id").is_ok() {
                row_values.values.insert(
                    "user_id".to_string(),
                    ScalarValue::Utf8(Some(row.user_id.as_str().to_string())),
                );
            }
        })
    }

    async fn scan_with_version_resolution_to_kvs_async(
        &self,
        user_id: &UserId,
        _filter: Option<&Expr>,
        since_seq: Option<SeqId>,
        limit: Option<usize>,
        _keep_deleted: bool,
        _cold_columns: Option<&[String]>,
        _snapshot_commit_seq: Option<u64>,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>, KalamDbError> {
        let table_id = self.core.table_id();

        // since_seq is exclusive, so start at seq + 1
        let start_seq = since_seq.map(|seq| SeqId::from_i64(seq.as_i64().saturating_add(1)));

        let ttl_ms = self.ttl_seconds.map(|s| s * 1000);
        let now_ms = Self::now_millis()?;

        log::debug!(
            "[StreamProvider] streaming scan: table={} user={} ttl_ms={:?} limit={:?}",
            table_id,
            user_id.as_str(),
            ttl_ms,
            limit
        );

        // Use streaming scan with TTL filtering and early termination
        // This is more efficient than the pagination loop for LIMIT queries
        let scan_limit = limit.unwrap_or(100_000);

        // Use async version to avoid blocking the runtime
        let results = self
            .store
            .scan_user_streaming_async(user_id, start_seq, scan_limit, ttl_ms, now_ms)
            .await
            .map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to scan stream table hot storage: {}",
                    e
                ))
            })?;

        log::debug!(
            "[StreamProvider] streaming scan complete: table={} user={} rows={}",
            table_id,
            user_id.as_str(),
            results.len()
        );

        // TODO(phase 13.6): Apply filter expression for simple predicates if provided
        Ok(results)
    }

    fn extract_row(row: &StreamTableRow) -> &Row {
        &row.fields
    }
}

// Manual Debug to satisfy DataFusion's TableProvider: Debug bound
impl std::fmt::Debug for StreamTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table_id = self.core.table_id_arc();
        f.debug_struct("StreamTableProvider")
            .field("table_id", &table_id)
            .field("table_type", &self.core.table_type())
            .field("ttl_seconds", &self.ttl_seconds)
            .field("primary_key_field_name", &self.core.primary_key_field_name())
            .finish()
    }
}

// Implement DataFusion TableProvider trait
#[async_trait]
impl TableProvider for StreamTableProvider {
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
        self.base_scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_supports_filters_pushdown(filters)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        check_user_table_write_access(state, self.core.table_id())
            .map_err(session_error_to_datafusion)?;

        if insert_op != InsertOp::Append {
            return Err(DataFusionError::Plan(format!(
                "{} is not supported for stream tables",
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
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan("UPDATE not supported for STREAM tables".to_string()))
    }

    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        self.base_statistics()
    }
}

// KalamTableProvider: extends TableProvider with KalamDB-specific DML
#[async_trait]
impl crate::utils::dml_provider::KalamTableProvider for StreamTableProvider {
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
}
