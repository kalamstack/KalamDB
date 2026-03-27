use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use kalamdb_commons::models::{ReadContext, Role, UserId};
use kalamdb_commons::{NamespaceId, TableType};
use kalamdb_pg::OperationExecutor;
use kalamdb_session::SessionUserContext;
use tonic::Status;

use super::scan;
use super::types::{DeleteRequest, InsertRequest, MutationResult, ScanRequest, ScanResult, UpdateRequest};
use crate::app_context::AppContext;
use crate::sql::ExecutionContext;

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

    /// Build a per-request SessionContext with user identity and role injected.
    ///
    /// The PG extension acts as a bridge: the `user_id` comes from the FDW
    /// request (derived from the `kalam.user_id` GUC on the Postgres side).
    ///
    /// Role selection:
    /// - **User/Stream tables**: `Role::User` so row-level security (RLS) is enforced
    /// - **Shared tables**: `Role::Service` so Restricted/Private shared tables are accessible
    fn session_with_user(&self, user_id: Option<&UserId>, role: Role) -> SessionContext {
        let base = self.app_context.base_session_context();
        let mut state = base.state().clone();

        let ctx = match user_id {
            Some(uid) => SessionUserContext::new(uid.clone(), role, ReadContext::Client),
            None => SessionUserContext::new(UserId::anonymous(), role, ReadContext::Client),
        };
        state.config_mut().options_mut().extensions.insert(ctx);

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
}

#[async_trait]
impl OperationExecutor for OperationService {
    async fn execute_scan(&self, request: ScanRequest) -> Result<ScanResult, Status> {
        let role = Self::role_for_table_type(request.table_type);
        let session = self.session_with_user(request.user_id.as_ref(), role);
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
                return Err(Status::permission_denied(
                    "cannot insert into system tables",
                ));
            },
        };
        Ok(MutationResult {
            affected_rows: affected as u64,
        })
    }

    async fn execute_update(&self, request: UpdateRequest) -> Result<MutationResult, Status> {
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
                    .update_shared_data(
                        request.table_id,
                        request.updates,
                        Some(request.pk_value),
                    )
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
        let applier = self.app_context.applier();
        let affected = match request.table_type {
            TableType::User | TableType::Stream => {
                let user_id = require_user_id(request.user_id, "deletes")?;
                let resp = applier
                    .delete_user_data(
                        request.table_id,
                        user_id,
                        Some(vec![request.pk_value]),
                    )
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .delete_shared_data(
                        request.table_id,
                        Some(vec![request.pk_value]),
                    )
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied(
                    "cannot delete from system tables",
                ));
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

        let sql_executor = crate::sql::executor::SqlExecutor::new(
            Arc::clone(&self.app_context),
            false, // no password-complexity enforcement for DDL
        );
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

        let sql_executor = crate::sql::executor::SqlExecutor::new(
            Arc::clone(&self.app_context),
            false,
        );
        let result = sql_executor
            .execute(sql, &exec_ctx, Vec::new())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            crate::sql::ExecutionResult::Rows { batches, row_count, .. } => {
                let (ipc_batches, _) = kalamdb_pg::encode_batches(&batches)?;
                Ok((format!("{} row(s)", row_count), ipc_batches))
            }
            crate::sql::ExecutionResult::Success { message } => Ok((message, Vec::new())),
            other => Ok((format!("OK (affected: {})", other.affected_rows()), Vec::new())),
        }
    }
}

fn require_user_id(
    user_id: Option<UserId>,
    operation: &str,
) -> Result<UserId, Status> {
    user_id.ok_or_else(|| {
        Status::invalid_argument(format!(
            "user_id required for user/stream table {}",
            operation
        ))
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
        Row { values: BTreeMap::new() }
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
        let mem = MemTable::try_new(Arc::clone(&arrow_schema), vec![batches])
            .expect("MemTable creation");
        cached.set_system_provider(Arc::new(mem));

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
                rows: vec![empty_row()],
                user_id: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("user_id required"));
    }
}
