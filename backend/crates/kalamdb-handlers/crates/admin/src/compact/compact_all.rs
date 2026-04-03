//! Typed handler for STORAGE COMPACT ALL statement

use kalamdb_commons::models::{TableId, TableName};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::JobId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_jobs::executors::compact::CompactParams;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::CompactAllTablesStatement;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Handler for STORAGE COMPACT ALL
pub struct CompactAllTablesHandler {
    app_context: Arc<AppContext>,
}

impl CompactAllTablesHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<CompactAllTablesStatement> for CompactAllTablesHandler {
    async fn execute(
        &self,
        statement: CompactAllTablesStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Offload sync RocksDB full table scan to blocking thread
        let app_ctx = self.app_context.clone();
        let all_defs =
            tokio::task::spawn_blocking(move || app_ctx.system_tables().tables().list_tables())
                .await
                .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;
        let ns = statement.namespace.clone();
        let target_tables: Vec<(TableName, TableType)> = all_defs
            .into_iter()
            .filter(|d| d.namespace_id == ns)
            .filter(|d| matches!(d.table_type, TableType::User | TableType::Shared))
            .map(|d| (d.table_name.clone(), d.table_type))
            .collect();

        if target_tables.is_empty() {
            return Err(KalamDbError::NotFound(format!(
                "No compactable tables found in namespace {}",
                ns.as_str()
            )));
        }

        let job_manager = self.app_context.job_manager();
        let mut job_ids: Vec<String> = Vec::new();
        for (table_name, table_type) in &target_tables {
            let table_id = TableId::new(ns.clone(), table_name.clone());
            let params = CompactParams {
                table_id: table_id.clone(),
                table_type: *table_type,
                target_file_size_mb: 128,
            };
            let idempotency_key = format!("compact-{}-{}", ns.as_str(), table_name.as_str());
            let job_id: JobId = job_manager
                .create_job_typed(JobType::Compact, params, Some(idempotency_key), None)
                .await?;
            job_ids.push(job_id.as_str().to_string());
        }

        Ok(ExecutionResult::Success {
            message: format!(
                "Storage compaction started for {} table(s) in namespace '{}'. Job IDs: [{}]",
                job_ids.len(),
                ns.as_str(),
                job_ids.join(", ")
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CompactAllTablesStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::can_execute_maintenance;
        if !can_execute_maintenance(context.user_role()) {
            return Err(KalamDbError::Unauthorized(
                "STORAGE COMPACT ALL requires Service, DBA, or System role".to_string(),
            ));
        }
        Ok(())
    }
}
