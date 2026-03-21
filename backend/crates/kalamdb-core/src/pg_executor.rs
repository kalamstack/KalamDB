use std::str::FromStr;
use std::sync::Arc;

use arrow_ipc::writer::StreamWriter;
use async_trait::async_trait;
use tonic::Status;

use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{NamespaceId, TableName, UserId};
use kalamdb_commons::{TableId, TableType};
use kalamdb_pg::{
    DeleteRpcRequest, DeleteRpcResponse, InsertRpcRequest, InsertRpcResponse, PgQueryExecutor,
    ScanRpcRequest, ScanRpcResponse, UpdateRpcRequest, UpdateRpcResponse,
};

use crate::app_context::AppContext;
use crate::sql::context::ExecutionContext;

/// Server-side implementation of `PgQueryExecutor` that uses `AppContext`
/// to execute scan/mutation requests received from the PostgreSQL FDW.
pub struct CorePgQueryExecutor {
    app_context: Arc<AppContext>,
}

impl CorePgQueryExecutor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn parse_table_type(s: &str) -> Result<TableType, Status> {
        TableType::from_str(s)
            .map_err(|e| Status::invalid_argument(format!("invalid table_type: {}", e)))
    }

    fn parse_table_id(namespace: &str, table_name: &str) -> Result<TableId, Status> {
        if namespace.trim().is_empty() {
            return Err(Status::invalid_argument("namespace must not be empty"));
        }
        if table_name.trim().is_empty() {
            return Err(Status::invalid_argument("table_name must not be empty"));
        }
        Ok(TableId::new(
            NamespaceId::new(namespace.trim()),
            TableName::new(table_name.trim()),
        ))
    }

    fn build_exec_ctx(
        &self,
        namespace: &str,
        user_id: Option<&str>,
    ) -> ExecutionContext {
        let ns = NamespaceId::new(namespace.trim());
        match user_id {
            Some(uid) if !uid.trim().is_empty() => ExecutionContext::with_namespace(
                UserId::new(uid.trim()),
                kalamdb_commons::models::Role::Service,
                ns,
                self.app_context.base_session_context(),
            ),
            _ => ExecutionContext::with_namespace(
                UserId::new("pg-remote"),
                kalamdb_commons::models::Role::Service,
                ns,
                self.app_context.base_session_context(),
            ),
        }
    }

    fn parse_row(json: &str) -> Result<Row, Status> {
        serde_json::from_str::<Row>(json)
            .map_err(|e| Status::invalid_argument(format!("invalid row JSON: {}", e)))
    }

    /// Encode Arrow RecordBatches into IPC bytes for gRPC transport.
    fn encode_batches(
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Result<(Vec<bytes::Bytes>, Option<bytes::Bytes>), Status> {
        if batches.is_empty() {
            return Ok((Vec::new(), None));
        }

        let schema = batches[0].schema();
        let mut ipc_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| {
                    Status::internal(format!("failed to create IPC writer: {}", e))
                })?;
                writer.write(batch).map_err(|e| {
                    Status::internal(format!("failed to write IPC batch: {}", e))
                })?;
                writer.finish().map_err(|e| {
                    Status::internal(format!("failed to finish IPC writer: {}", e))
                })?;
            }
            ipc_batches.push(bytes::Bytes::from(buf));
        }

        Ok((ipc_batches, None))
    }
}

#[async_trait]
impl PgQueryExecutor for CorePgQueryExecutor {
    async fn execute_scan(&self, request: ScanRpcRequest) -> Result<ScanRpcResponse, Status> {
        let table_id = Self::parse_table_id(&request.namespace, &request.table_name)?;
        let _table_type = Self::parse_table_type(&request.table_type)?;
        let exec_ctx = self.build_exec_ctx(&request.namespace, request.user_id.as_deref());

        // Build a SELECT query from the typed request
        let columns = if request.columns.is_empty() {
            "*".to_string()
        } else {
            request
                .columns
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!(
            "SELECT {} FROM \"{}\".\"{}\"",
            columns,
            table_id.namespace_id().as_str().replace('"', "\"\""),
            table_id.table_name().as_str().replace('"', "\"\""),
        );

        if let Some(limit) = request.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        let sql_executor = self.app_context.sql_executor();
        let result = sql_executor
            .execute(&sql, &exec_ctx, vec![])
            .await
            .map_err(|e| Status::internal(format!("scan execution failed: {}", e)))?;

        match result {
            crate::sql::context::ExecutionResult::Rows { batches, .. } => {
                let (ipc_batches, schema_ipc) = Self::encode_batches(&batches)?;
                Ok(ScanRpcResponse {
                    ipc_batches,
                    schema_ipc,
                })
            },
            _ => Ok(ScanRpcResponse {
                ipc_batches: Vec::new(),
                schema_ipc: None,
            }),
        }
    }

    async fn execute_insert(&self, request: InsertRpcRequest) -> Result<InsertRpcResponse, Status> {
        let table_id = Self::parse_table_id(&request.namespace, &request.table_name)?;
        let table_type = Self::parse_table_type(&request.table_type)?;

        let mut rows = Vec::with_capacity(request.rows_json.len());
        for json in &request.rows_json {
            rows.push(Self::parse_row(json)?);
        }

        let applier = self.app_context.applier();
        let affected = match table_type {
            TableType::User | TableType::Stream => {
                let user_id = request
                    .user_id
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| UserId::new(s.trim()))
                    .ok_or_else(|| {
                        Status::invalid_argument("user_id required for user/stream table inserts")
                    })?;
                let resp = applier
                    .insert_user_data(table_id, user_id, rows)
                    .await
                    .map_err(|e| Status::internal(format!("insert failed: {}", e)))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .insert_shared_data(table_id, rows)
                    .await
                    .map_err(|e| Status::internal(format!("insert failed: {}", e)))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied("cannot insert into system tables"));
            },
        };

        Ok(InsertRpcResponse {
            affected_rows: affected as u64,
        })
    }

    async fn execute_update(&self, request: UpdateRpcRequest) -> Result<UpdateRpcResponse, Status> {
        let table_id = Self::parse_table_id(&request.namespace, &request.table_name)?;
        let table_type = Self::parse_table_type(&request.table_type)?;
        let updates = Self::parse_row(&request.updates_json)?;

        let applier = self.app_context.applier();
        let affected = match table_type {
            TableType::User | TableType::Stream => {
                let user_id = request
                    .user_id
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| UserId::new(s.trim()))
                    .ok_or_else(|| {
                        Status::invalid_argument("user_id required for user/stream table updates")
                    })?;
                let resp = applier
                    .update_user_data(
                        table_id,
                        user_id,
                        vec![updates],
                        Some(request.pk_value.clone()),
                    )
                    .await
                    .map_err(|e| Status::internal(format!("update failed: {}", e)))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .update_shared_data(
                        table_id,
                        vec![updates],
                        Some(request.pk_value.clone()),
                    )
                    .await
                    .map_err(|e| Status::internal(format!("update failed: {}", e)))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied("cannot update system tables"));
            },
        };

        Ok(UpdateRpcResponse {
            affected_rows: affected as u64,
        })
    }

    async fn execute_delete(&self, request: DeleteRpcRequest) -> Result<DeleteRpcResponse, Status> {
        let table_id = Self::parse_table_id(&request.namespace, &request.table_name)?;
        let table_type = Self::parse_table_type(&request.table_type)?;

        let applier = self.app_context.applier();
        let affected = match table_type {
            TableType::User | TableType::Stream => {
                let user_id = request
                    .user_id
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| UserId::new(s.trim()))
                    .ok_or_else(|| {
                        Status::invalid_argument("user_id required for user/stream table deletes")
                    })?;
                let resp = applier
                    .delete_user_data(
                        table_id,
                        user_id,
                        Some(vec![request.pk_value.clone()]),
                    )
                    .await
                    .map_err(|e| Status::internal(format!("delete failed: {}", e)))?;
                resp.rows_affected()
            },
            TableType::Shared => {
                let resp = applier
                    .delete_shared_data(table_id, Some(vec![request.pk_value.clone()]))
                    .await
                    .map_err(|e| Status::internal(format!("delete failed: {}", e)))?;
                resp.rows_affected()
            },
            TableType::System => {
                return Err(Status::permission_denied("cannot delete from system tables"));
            },
        };

        Ok(DeleteRpcResponse {
            affected_rows: affected as u64,
        })
    }
}
