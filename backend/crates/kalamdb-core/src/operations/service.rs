use std::sync::Arc;

use async_trait::async_trait;
use kalamdb_commons::models::UserId;
use kalamdb_commons::TableType;
use tonic::Status;

use super::scan;
use crate::app_context::AppContext;

// Re-export domain types from kalamdb-pg (canonical location of the trait)
use kalamdb_pg::{
    DeleteRequest, InsertRequest, MutationResult, OperationExecutor, ScanRequest, ScanResult,
    UpdateRequest,
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
}

#[async_trait]
impl OperationExecutor for OperationService {
    async fn execute_scan(&self, request: ScanRequest) -> Result<ScanResult, Status> {
        let batches = scan::execute_scan(
            &self.app_context.schema_registry(),
            &self.app_context.base_session_context(),
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
