//! Vector index job executor.
//!
//! Flushes per-column vector hot staging into cold snapshot artifacts and updates
//! vector metadata embedded in manifest.json.

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::vector::{flush_shared_scope_vectors, flush_user_scope_vectors};
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexParams {
    pub table_id: TableId,
    pub table_type: TableType,
    #[serde(default)]
    pub user_id: Option<UserId>,
}

impl JobParams for VectorIndexParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if matches!(self.table_type, TableType::System | TableType::Stream) {
            return Err(KalamDbError::InvalidOperation(
                "Vector indexing is supported only for user/shared tables".to_string(),
            ));
        }

        if matches!(self.table_type, TableType::User) && self.user_id.is_none() {
            return Err(KalamDbError::InvalidOperation(
                "user_id is required for user table vector indexing".to_string(),
            ));
        }

        Ok(())
    }
}

pub struct VectorIndexExecutor;

impl VectorIndexExecutor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for VectorIndexExecutor {
    type Params = VectorIndexParams;

    fn job_type(&self) -> JobType {
        JobType::VectorIndex
    }

    fn name(&self) -> &'static str {
        "VectorIndexExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        self.execute_leader(ctx).await
    }

    async fn execute_local(
        &self,
        _ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        // Local phase is intentionally a no-op. Snapshot persistence happens in leader phase.
        Ok(JobDecision::Completed {
            message: Some("Vector local phase skipped".to_string()),
        })
    }

    async fn execute_leader(
        &self,
        ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        let params = ctx.params();
        let app_ctx = &ctx.app_ctx;
        let schema_registry = app_ctx.schema_registry();

        let cached = schema_registry.get(&params.table_id).ok_or_else(|| {
            KalamDbError::TableNotFound(format!("Table not found: {}", params.table_id))
        })?;

        let schema = schema_registry.get_arrow_schema(&params.table_id)?;

        let storage_cached = cached
            .storage_cached(&app_ctx.storage_registry())
            .map_err(|e| KalamDbError::Other(format!("Failed to resolve storage cache: {}", e)))?;

        match params.table_type {
            TableType::User => {
                let user_id = params.user_id.as_ref().ok_or_else(|| {
                    KalamDbError::InvalidOperation(
                        "user_id is required for user table vector indexing".to_string(),
                    )
                })?;

                flush_user_scope_vectors(
                    app_ctx,
                    &params.table_id,
                    user_id,
                    &schema,
                    &storage_cached,
                )?;
            },
            TableType::Shared => {
                flush_shared_scope_vectors(app_ctx, &params.table_id, &schema, &storage_cached)?;
            },
            TableType::System | TableType::Stream => {
                return Err(KalamDbError::InvalidOperation(
                    "Vector indexing is supported only for user/shared tables".to_string(),
                ));
            },
        }

        Ok(JobDecision::Completed {
            message: Some(format!("Vector index updated for {}", params.table_id.full_name())),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("VectorIndex jobs cannot be safely cancelled once snapshot write begins");
        Ok(())
    }
}

impl Default for VectorIndexExecutor {
    fn default() -> Self {
        Self::new()
    }
}
