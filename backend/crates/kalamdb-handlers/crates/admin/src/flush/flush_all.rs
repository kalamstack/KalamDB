//! Typed handler for STORAGE FLUSH ALL statement

use kalamdb_commons::JobId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_jobs::executors::flush::FlushParams;
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::FlushAllTablesStatement;
use kalamdb_system::JobType;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Handler for STORAGE FLUSH ALL
pub struct FlushAllTablesHandler {
    app_context: Arc<AppContext>,
}

impl FlushAllTablesHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<FlushAllTablesStatement> for FlushAllTablesHandler {
    async fn execute(
        &self,
        statement: FlushAllTablesStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Query manifest pending write index to find tables with unflushed data
        let manifest_service = self.app_context.manifest_service();
        let pending_iter = manifest_service
            .pending_manifest_ids_iter()
            .map_err(|e| KalamDbError::Other(format!("Pending manifest scan failed: {}", e)))?;

        // Filter by namespace if needed
        let ns = statement.namespace.clone();
        let mut any_pending = false;
        let mut join_set = JoinSet::new();
        let mut job_ids: Vec<String> = Vec::new();
        let mut in_flight = 0usize;
        const MAX_IN_FLIGHT: usize = 16;

        for manifest_id in pending_iter {
            let manifest_id = manifest_id
                .map_err(|e| KalamDbError::Other(format!("Pending manifest read failed: {}", e)))?;

            if manifest_id.table_id().namespace_id() != &ns {
                continue;
            }

            any_pending = true;
            let app_context = self.app_context.clone();
            let table_id = manifest_id.table_id().clone();

            join_set.spawn(async move {
                // Offload sync RocksDB read to blocking thread
                let app_ctx_inner = app_context.clone();
                let tid = table_id.clone();
                let table_def = tokio::task::spawn_blocking(move || {
                    app_ctx_inner.system_tables().tables().get_table_by_id(&tid)
                })
                .await
                .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

                let table_def = match table_def {
                    Some(def) => def,
                    None => return Ok::<Option<JobId>, KalamDbError>(None),
                };

                let params = FlushParams {
                    table_id: table_id.clone(),
                    table_type: table_def.table_type,
                    flush_threshold: None,
                };

                let idempotency_key = format!(
                    "flush-{}-{}",
                    table_id.namespace_id().as_str(),
                    table_id.table_name().as_str()
                );

                let job_id: JobId = app_context
                    .job_manager()
                    .create_job_typed(JobType::Flush, params, Some(idempotency_key), None)
                    .await?;

                Ok(Some(job_id))
            });

            in_flight += 1;
            if in_flight >= MAX_IN_FLIGHT {
                if let Some(result) = join_set.join_next().await {
                    let job_id = result.map_err(|e| {
                        KalamDbError::Other(format!("Flush job task failed: {}", e))
                    })??;
                    if let Some(job_id) = job_id {
                        job_ids.push(job_id.as_str().to_string());
                    }
                    in_flight -= 1;
                }
            }
        }

        if !any_pending {
            return Ok(ExecutionResult::Success {
                message: format!(
                    "Storage flush skipped: no pending writes in namespace '{}'",
                    ns.as_str()
                ),
            });
        }

        while let Some(result) = join_set.join_next().await {
            let job_id = result
                .map_err(|e| KalamDbError::Other(format!("Flush job task failed: {}", e)))??;
            if let Some(job_id) = job_id {
                job_ids.push(job_id.as_str().to_string());
            }
        }

        Ok(ExecutionResult::Success {
            message: format!(
                "Storage flush started for {} table(s) in namespace '{}'. Job IDs: [{}]",
                job_ids.len(),
                ns.as_str(),
                job_ids.join(", ")
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &FlushAllTablesStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_session::can_execute_maintenance;
        // Allow Service, DBA, and System roles to flush tables
        if !can_execute_maintenance(context.user_role()) {
            return Err(KalamDbError::Unauthorized(
                "STORAGE FLUSH ALL requires Service, DBA, or System role".to_string(),
            ));
        }
        Ok(())
    }
}
