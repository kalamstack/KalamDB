use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, ReadContext, Role, TransactionId, TransactionOrigin, UserId};
use kalamdb_commons::models::pg_operations::{
    DeleteRequest, InsertRequest, MutationResult, ScanRequest, ScanResult, UpdateRequest,
};
use kalamdb_commons::{NamespaceId, TableType};
use kalamdb_pg::OperationExecutor;
use kalamdb_session_datafusion::SessionUserContext;
use kalamdb_transactions::{TransactionQueryContext, TransactionQueryExtension};
use std::collections::BTreeMap;
use tonic::Status;

use super::scan;
use crate::app_context::AppContext;
use crate::sql::ExecutionContext;
use crate::transactions::{
    CoordinatorAccessValidator, CoordinatorOverlayView, ExecutionOwnerKey, StagedMutation,
};

/// Domain-typed operation executor for Tier-2 (typed) callers.
///
/// Typed callers (PG extension, future transports) skip SQL parsing and DataFusion
/// logical planning entirely:
/// - **Scans**: `TableProvider::scan()` → `collect(plan, task_ctx)` — physical execution only
/// - **Mutations**: `UnifiedApplier` → Raft → `DmlExecutor` — no DataFusion at all
pub struct OperationService {
    app_context: Arc<AppContext>,
}

impl OperationService {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn session_with_query_context(
        &self,
        user_id: Option<&UserId>,
        role: Role,
        transaction_query_context: Option<TransactionQueryContext>,
    ) -> SessionContext {
        let base = self.app_context.base_session_context();
        let mut state = base.state().clone();

        let ctx = match user_id {
            Some(uid) => SessionUserContext::new(uid.clone(), role, ReadContext::Client),
            None => SessionUserContext::new(UserId::anonymous(), role, ReadContext::Client),
        };
        state.config_mut().options_mut().extensions.insert(ctx);

        if let Some(transaction_query_context) = transaction_query_context {
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(TransactionQueryExtension::new(transaction_query_context));
        }

        SessionContext::new_with_state(state)
    }

    /// Determine the appropriate role for a scan based on the table type.
    ///
    /// User/Stream tables use `Role::User` to enforce RLS (row-level security).
    /// Shared tables use `Role::Service` to allow access to Restricted/Private tables.
    fn role_for_table_type(table_type: TableType) -> Role {
        match table_type {
            TableType::User | TableType::Stream => Role::User,
            _ => Role::Service,
        }
    }

    fn active_transaction_for_session(&self, session_id: Option<&str>) -> Result<Option<TransactionId>, Status> {
        // Autocommit typed DML stays on the hot path here: no transaction handle means
        // one session-id parse plus one coordinator owner-key lookup, with no overlay,
        // query-context, or staged-write allocation.
        let Some(session_id) = session_id.map(str::trim).filter(|session_id| !session_id.is_empty()) else {
            return Ok(None);
        };

        let owner_key = ExecutionOwnerKey::from_pg_session_id(session_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(self.app_context.transaction_coordinator().active_for_owner(&owner_key))
    }

    fn transaction_query_context_for_session(
        &self,
        session_id: Option<&str>,
    ) -> Result<Option<TransactionQueryContext>, Status> {
        // Autocommit reads return from this helper without constructing an overlay view
        // or mutation sink unless an active transaction handle is actually present.
        let Some(session_id) = session_id.map(str::trim).filter(|session_id| !session_id.is_empty()) else {
            return Ok(None);
        };

        let owner_key = ExecutionOwnerKey::from_pg_session_id(session_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let coordinator = self.app_context.transaction_coordinator();
        let Some(transaction_id) = coordinator.active_for_owner(&owner_key) else {
            return Ok(None);
        };

        let handle = coordinator
            .get_handle(&transaction_id)
            .ok_or_else(|| Status::failed_precondition(format!(
                "active transaction '{}' has no handle",
                transaction_id
            )))?;

        if !handle.state.is_open() {
            return Err(Status::failed_precondition(format!(
                "transaction '{}' is {}",
                transaction_id, handle.state
            )));
        }

        Ok(Some(TransactionQueryContext::new(
            transaction_id.clone(),
            handle.snapshot_commit_seq,
            Arc::new(CoordinatorOverlayView::new(
                Arc::clone(&coordinator),
                transaction_id.clone(),
            )),
            Arc::new(crate::transactions::CoordinatorMutationSink::new(coordinator)),
            Arc::new(CoordinatorAccessValidator::new(
                self.app_context.transaction_coordinator(),
            )),
        )))
    }

    async fn stage_insert(
        &self,
        transaction_id: &TransactionId,
        request: InsertRequest,
    ) -> Result<MutationResult, Status> {
        let coordinator = self.app_context.transaction_coordinator();
        let affected_rows = request.rows.len() as u64;

        for row in request.rows {
            let primary_key = primary_key_from_row(&row)?;
            let mutation = StagedMutation::new(
                transaction_id.clone(),
                request.table_id.clone(),
                request.table_type,
                request.user_id.clone(),
                OperationKind::Insert,
                primary_key,
                row,
                false,
            );

            coordinator
                .stage(transaction_id, mutation)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
        }

        Ok(MutationResult { affected_rows })
    }

    async fn stage_update(
        &self,
        transaction_id: &TransactionId,
        request: UpdateRequest,
    ) -> Result<MutationResult, Status> {
        let coordinator = self.app_context.transaction_coordinator();
        let payload = request
            .updates
            .into_iter()
            .next()
            .unwrap_or_else(|| Row::new(BTreeMap::new()));
        let mutation = StagedMutation::new(
            transaction_id.clone(),
            request.table_id,
            request.table_type,
            request.user_id,
            OperationKind::Update,
            request.pk_value,
            payload,
            false,
        );

        coordinator
            .stage(transaction_id, mutation)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;

        Ok(MutationResult { affected_rows: 1 })
    }

    async fn stage_delete(
        &self,
        transaction_id: &TransactionId,
        request: DeleteRequest,
    ) -> Result<MutationResult, Status> {
        let coordinator = self.app_context.transaction_coordinator();
        let mutation = StagedMutation::new(
            transaction_id.clone(),
            request.table_id,
            request.table_type,
            request.user_id,
            OperationKind::Delete,
            request.pk_value,
            Row::new(BTreeMap::new()),
            true,
        );

        coordinator
            .stage(transaction_id, mutation)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;

        Ok(MutationResult { affected_rows: 1 })
    }
}

#[async_trait]
impl OperationExecutor for OperationService {
    async fn begin_transaction(&self, session_id: &str) -> Result<Option<String>, Status> {
        let owner_key = ExecutionOwnerKey::from_pg_session_id(session_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let transaction_id = self
            .app_context
            .transaction_coordinator()
            .begin(owner_key, session_id.to_string().into(), TransactionOrigin::PgRpc)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Some(transaction_id.to_string()))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, Status> {
        let transaction_id = TransactionId::try_new(transaction_id.to_string())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let result = self
            .app_context
            .transaction_coordinator()
            .commit(&transaction_id)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Some(result.transaction_id.to_string()))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, Status> {
        let transaction_id = TransactionId::try_new(transaction_id.to_string())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.app_context
            .transaction_coordinator()
            .rollback(&transaction_id)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Some(transaction_id.to_string()))
    }

    async fn execute_scan(&self, request: ScanRequest) -> Result<ScanResult, Status> {
        let role = Self::role_for_table_type(request.table_type);
        // Non-transactional scans pay only the idle-session lookup above. The
        // transaction query extension is attached only when a live transaction exists.
        let transaction_query_context =
            self.transaction_query_context_for_session(request.session_id.as_deref())?;
        let session = self.session_with_query_context(
            request.user_id.as_ref(),
            role,
            transaction_query_context,
        );
        let batches = scan::execute_scan(
            &self.app_context.schema_registry(),
            &session,
            &request.table_id,
            &request.columns,
            request.limit,
        )
        .await
        .map_err(|e| -> Status { e.into() })?;
        Ok(ScanResult { batches })
    }

    async fn execute_insert(&self, request: InsertRequest) -> Result<MutationResult, Status> {
        // Autocommit requests pay only the owner-key lookup here; we do not allocate
        // transaction overlays or staged write buffers unless an explicit transaction is active.
        if let Some(transaction_id) =
            self.active_transaction_for_session(request.session_id.as_deref())?
        {
            return self.stage_insert(&transaction_id, request).await;
        }

        let applier = self.app_context.applier();
        let affected = match request.table_type {
            TableType::User | TableType::Stream => {
                let user_id = require_user_id(request.user_id, "inserts")?;
                let resp = applier
                    .insert_user_data(request.table_id, user_id, request.rows)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .insert_shared_data(request.table_id, request.rows)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied("cannot insert into system tables"));
            },
        };
        Ok(MutationResult {
            affected_rows: affected as u64,
        })
    }

    async fn execute_update(&self, request: UpdateRequest) -> Result<MutationResult, Status> {
        // Preserve the autocommit fast path: one presence check, then go straight to the applier.
        if let Some(transaction_id) =
            self.active_transaction_for_session(request.session_id.as_deref())?
        {
            return self.stage_update(&transaction_id, request).await;
        }

        let applier = self.app_context.applier();
        let affected = match request.table_type {
            TableType::User | TableType::Stream => {
                let user_id = require_user_id(request.user_id, "updates")?;
                let resp = applier
                    .update_user_data(
                        request.table_id,
                        user_id,
                        request.updates,
                        Some(request.pk_value),
                    )
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .update_shared_data(request.table_id, request.updates, Some(request.pk_value))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied("cannot update system tables"));
            },
        };
        Ok(MutationResult {
            affected_rows: affected as u64,
        })
    }

    async fn execute_delete(&self, request: DeleteRequest) -> Result<MutationResult, Status> {
        // Preserve the autocommit fast path: avoid transaction-specific allocations when absent.
        if let Some(transaction_id) =
            self.active_transaction_for_session(request.session_id.as_deref())?
        {
            return self.stage_delete(&transaction_id, request).await;
        }

        let applier = self.app_context.applier();
        let affected = match request.table_type {
            TableType::User | TableType::Stream => {
                let user_id = require_user_id(request.user_id, "deletes")?;
                let resp = applier
                    .delete_user_data(request.table_id, user_id, Some(vec![request.pk_value]))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .delete_shared_data(request.table_id, Some(vec![request.pk_value]))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied("cannot delete from system tables"));
            },
        };
        Ok(MutationResult {
            affected_rows: affected as u64,
        })
    }

    async fn execute_sql(&self, sql: &str) -> Result<String, Status> {
        let base = self.app_context.base_session_context();
        let exec_ctx = ExecutionContext::with_namespace(
            UserId::new("pg-extension"),
            Role::Dba,
            NamespaceId::new("default"),
            base,
        );

        let sql_executor = self.app_context.sql_executor();
        let result = sql_executor
            .execute(sql, &exec_ctx, Vec::new())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let message = match result {
            crate::sql::ExecutionResult::Success { message } => message,
            other => format!("OK (affected: {})", other.affected_rows()),
        };
        Ok(message)
    }

    async fn execute_query(&self, sql: &str) -> Result<(String, Vec<bytes::Bytes>), Status> {
        let base = self.app_context.base_session_context();
        let exec_ctx = ExecutionContext::with_namespace(
            UserId::new("pg-extension"),
            Role::Dba,
            NamespaceId::new("default"),
            base,
        );

        let sql_executor = self.app_context.sql_executor();
        let result = sql_executor
            .execute(sql, &exec_ctx, Vec::new())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            crate::sql::ExecutionResult::Rows {
                batches, row_count, ..
            } => {
                let (ipc_batches, _) = kalamdb_pg::encode_batches(&batches)?;
                Ok((format!("{} row(s)", row_count), ipc_batches))
            },
            crate::sql::ExecutionResult::Success { message } => Ok((message, Vec::new())),
            other => Ok((format!("OK (affected: {})", other.affected_rows()), Vec::new())),
        }
    }
}

fn primary_key_from_row(row: &Row) -> Result<String, Status> {
    row.values
        .get("id")
        .map(|value| value.to_string())
        .ok_or_else(|| Status::invalid_argument("transactional inserts require an 'id' primary key in the typed pg path"))
}

fn require_user_id(user_id: Option<UserId>, operation: &str) -> Result<UserId, Status> {
    user_id.ok_or_else(|| {
        Status::invalid_argument(format!("user_id required for user/stream table {}", operation))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema_registry::cached_table_data::CachedTableData;
    use crate::test_helpers::test_app_context_simple;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::ScalarValue;
    use datafusion::datasource::MemTable;
    use kalamdb_commons::datatypes::KalamDataType;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
    use kalamdb_commons::models::{NamespaceId, TableId, TableName};
    use kalamdb_commons::TableType;
    use kalamdb_pg::OperationExecutor;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn empty_row() -> Row {
        Row {
            values: BTreeMap::new(),
        }
    }

    /// Helper: create an AppContext and OperationService for tests.
    fn setup() -> (Arc<AppContext>, OperationService) {
        let app_ctx = test_app_context_simple();
        let svc = OperationService::new(Arc::clone(&app_ctx));
        (app_ctx, svc)
    }

    /// Helper: register an in-memory table with two columns (id INT64, name UTF8)
    /// and optional seed data into the SchemaRegistry.
    fn register_mem_table(
        app_ctx: &AppContext,
        table_id: &TableId,
        batches: Vec<RecordBatch>,
    ) -> Arc<Schema> {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create a TableDefinition so the CachedTableData can be built
        let table_def = TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .expect("table definition");

        let cached = Arc::new(CachedTableData::new(Arc::new(table_def)));

        // Wrap seed data in a MemTable provider
        let mem =
            MemTable::try_new(Arc::clone(&arrow_schema), vec![batches]).expect("MemTable creation");
        cached.set_provider(Arc::new(mem));

        app_ctx.schema_registry().insert_cached(table_id.clone(), cached);
        arrow_schema
    }

    // ---------------------------------------------------------------
    // Scan tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn scan_nonexistent_table_returns_not_found() {
        let (_app_ctx, svc) = setup();
        let req = ScanRequest {
            table_id: TableId::new(NamespaceId::new("no_ns"), TableName::new("no_table")),
            table_type: TableType::Shared,
            session_id: None,
            columns: vec![],
            limit: None,
            user_id: None,
        };
        let err = svc.execute_scan(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn scan_empty_table_returns_zero_batches() {
        let (app_ctx, svc) = setup();
        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("empty_tbl"));
        register_mem_table(&app_ctx, &table_id, vec![]);

        let res = svc
            .execute_scan(ScanRequest {
                table_id,
                table_type: TableType::Shared,
                session_id: None,
                columns: vec![],
                limit: None,
                user_id: None,
            })
            .await
            .expect("scan should succeed");
        let total_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn scan_with_data_returns_rows() {
        let (app_ctx, svc) = setup();
        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("data_tbl"));
        let schema = register_mem_table(
            &app_ctx,
            &table_id,
            vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap()],
        );

        let res = svc
            .execute_scan(ScanRequest {
                table_id,
                table_type: TableType::Shared,
                session_id: None,
                columns: vec![],
                limit: None,
                user_id: None,
            })
            .await
            .expect("scan should succeed");
        let total_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
        // Verify schema when returning all columns
        assert_eq!(res.batches[0].schema().as_ref(), schema.as_ref());
    }

    #[tokio::test]
    async fn scan_with_column_projection() {
        let (app_ctx, svc) = setup();
        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("proj_tbl"));
        register_mem_table(
            &app_ctx,
            &table_id,
            vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![10])),
                    Arc::new(StringArray::from(vec!["x"])),
                ],
            )
            .unwrap()],
        );

        let res = svc
            .execute_scan(ScanRequest {
                table_id,
                table_type: TableType::Shared,
                session_id: None,
                columns: vec!["name".to_string()],
                limit: None,
                user_id: None,
            })
            .await
            .expect("scan with projection");
        assert_eq!(res.batches[0].num_columns(), 1);
        assert_eq!(res.batches[0].schema().field(0).name(), "name");
    }

    #[tokio::test]
    async fn scan_with_invalid_column_returns_error() {
        let (app_ctx, svc) = setup();
        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("badcol_tbl"));
        register_mem_table(&app_ctx, &table_id, vec![]);

        let err = svc
            .execute_scan(ScanRequest {
                table_id,
                table_type: TableType::Shared,
                session_id: None,
                columns: vec!["nonexistent_col".to_string()],
                limit: None,
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("nonexistent_col"));
    }

    #[tokio::test]
    async fn scan_with_limit() {
        let (app_ctx, svc) = setup();
        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("limit_tbl"));
        register_mem_table(
            &app_ctx,
            &table_id,
            vec![RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                ],
            )
            .unwrap()],
        );

        // Limit is passed as a hint to TableProvider::scan(); MemTable may not
        // enforce it at the physical level, so we only verify the call succeeds.
        let res = svc
            .execute_scan(ScanRequest {
                table_id,
                table_type: TableType::Shared,
                session_id: None,
                columns: vec![],
                limit: Some(2),
                user_id: None,
            })
            .await
            .expect("scan with limit should succeed");
        let total_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "scan should return some rows");
    }

    // ---------------------------------------------------------------
    // DML rejection tests (system tables — no applier needed)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn insert_system_table_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_insert(InsertRequest {
                table_id: TableId::new(NamespaceId::new("system"), TableName::new("users")),
                table_type: TableType::System,
                session_id: None,
                rows: vec![empty_row()],
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn update_system_table_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_update(UpdateRequest {
                table_id: TableId::new(NamespaceId::new("system"), TableName::new("users")),
                table_type: TableType::System,
                session_id: None,
                updates: vec![empty_row()],
                pk_value: "some_pk".to_string(),
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn delete_system_table_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_delete(DeleteRequest {
                table_id: TableId::new(NamespaceId::new("system"), TableName::new("users")),
                table_type: TableType::System,
                session_id: None,
                pk_value: "some_pk".to_string(),
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // ---------------------------------------------------------------
    // Validation tests (user_id required — no applier needed)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn insert_user_table_without_user_id_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_insert(InsertRequest {
                table_id: TableId::new(NamespaceId::new("default"), TableName::new("tbl")),
                table_type: TableType::User,
                session_id: None,
                rows: vec![empty_row()],
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("user_id required"));
    }

    #[tokio::test]
    async fn update_user_table_without_user_id_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_update(UpdateRequest {
                table_id: TableId::new(NamespaceId::new("default"), TableName::new("tbl")),
                table_type: TableType::User,
                session_id: None,
                updates: vec![empty_row()],
                pk_value: "pk".to_string(),
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn delete_user_table_without_user_id_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_delete(DeleteRequest {
                table_id: TableId::new(NamespaceId::new("default"), TableName::new("tbl")),
                table_type: TableType::User,
                session_id: None,
                pk_value: "pk".to_string(),
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn insert_stream_table_without_user_id_rejected() {
        let (_app_ctx, svc) = setup();
        let err = svc
            .execute_insert(InsertRequest {
                table_id: TableId::new(NamespaceId::new("default"), TableName::new("events")),
                table_type: TableType::Stream,
                session_id: None,
                rows: vec![empty_row()],
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("user_id required"));
    }

    #[tokio::test]
    async fn insert_with_active_pg_transaction_stages_into_coordinator() {
        let (app_ctx, svc) = setup();
        let session_id = "pg-321-deadbeef";
        let transaction_id = app_ctx
            .transaction_coordinator()
            .begin(
                crate::transactions::ExecutionOwnerKey::from_pg_session_id(session_id).unwrap(),
                session_id.to_string().into(),
                kalamdb_commons::models::TransactionOrigin::PgRpc,
            )
            .unwrap();

        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(42)));
        values.insert(
            "name".to_string(),
            ScalarValue::Utf8(Some("staged item".to_string())),
        );

        let result = svc
            .execute_insert(InsertRequest {
                table_id: TableId::new(NamespaceId::new("default"), TableName::new("items")),
                table_type: TableType::Shared,
                session_id: Some(session_id.to_string()),
                user_id: None,
                rows: vec![Row::new(values)],
            })
            .await
            .expect("insert should stage successfully");

        assert_eq!(result.affected_rows, 1);

        let overlay = app_ctx
            .transaction_coordinator()
            .get_overlay(&transaction_id)
            .expect("overlay should exist after staging");
        let table_id = TableId::new(NamespaceId::new("default"), TableName::new("items"));
        let entry = overlay
            .latest_visible_entry(&table_id, "42")
            .expect("latest staged row should be visible in overlay");
        assert_eq!(entry.operation_kind, OperationKind::Insert);
        assert!(!entry.tombstone);
    }
}
