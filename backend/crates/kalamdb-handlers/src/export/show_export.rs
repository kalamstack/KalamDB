//! Typed handler for SHOW EXPORT statement

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use arrow::array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_sql::ddl::ShowExportStatement;
use kalamdb_system::providers::jobs::models::{Job, JobFilter, JobSortField, SortOrder};
use kalamdb_system::JobType;
use std::sync::Arc;

/// Handler for SHOW EXPORT
///
/// Lists the user's export jobs with status and download link (when complete).
pub struct ShowExportHandler {
    app_context: Arc<AppContext>,
}

impl ShowExportHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn result_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("job_id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("message", DataType::Utf8, true),
            Field::new("download_url", DataType::Utf8, true),
        ]))
    }

    fn created_at_micros(created_at_millis: i64) -> i64 {
        created_at_millis.saturating_mul(1_000)
    }

    /// Build a download URL for a completed export
    fn build_download_url(&self, user_id: &str, export_id: &str) -> String {
        let config = self.app_context.config();
        let host = &config.server.host;
        let port = config.server.port;
        format!("http://{}:{}/v1/exports/{}/{}", host, port, user_id, export_id)
    }

    /// Extract export_id from job parameters JSON
    fn extract_export_id(job: &Job) -> Option<String> {
        job.parameters.as_ref().and_then(|params_json| {
            serde_json::from_str::<serde_json::Value>(params_json)
                .ok()
                .and_then(|v| v.get("export_id").and_then(|e| e.as_str().map(String::from)))
        })
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<ShowExportStatement> for ShowExportHandler {
    async fn execute(
        &self,
        _statement: ShowExportStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let user_id = context.user_id().to_string();

        // Query jobs for this user's export jobs
        let job_manager = self.app_context.job_manager();
        let filter = JobFilter {
            job_type: Some(JobType::UserExport),
            limit: Some(20),
            sort_by: Some(JobSortField::CreatedAt),
            sort_order: Some(SortOrder::Desc),
            ..Default::default()
        };

        let all_jobs = job_manager.list_jobs(filter).await?;

        // Filter to only this user's exports (check parameters JSON)
        let user_jobs: Vec<&Job> = all_jobs
            .iter()
            .filter(|job| {
                job.parameters
                    .as_ref()
                    .map(|p| p.contains(&format!("\"user_id\":\"{}\"", user_id)))
                    .unwrap_or(false)
            })
            .collect();

        // Build result schema
        let schema = Self::result_schema();

        if user_jobs.is_empty() {
            let batch = RecordBatch::new_empty(schema.clone());
            return Ok(ExecutionResult::Rows {
                batches: vec![batch],
                row_count: 0,
                schema: Some(schema),
            });
        }

        let mut job_ids = Vec::new();
        let mut statuses = Vec::new();
        let mut created_ats = Vec::new();
        let mut messages = Vec::new();
        let mut download_urls = Vec::new();

        for job in &user_jobs {
            job_ids.push(job.job_id.as_str().to_string());
            statuses.push(format!("{}", job.status));
            created_ats.push(Self::created_at_micros(job.created_at));
            messages.push(job.message.clone().unwrap_or_default());

            // Build download URL only for completed jobs
            let url = if job.status == kalamdb_system::JobStatus::Completed {
                Self::extract_export_id(job)
                    .map(|eid| self.build_download_url(&user_id, &eid))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            download_urls.push(url);
        }

        let row_count = user_jobs.len();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(job_ids)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(TimestampMicrosecondArray::from(created_ats)),
                Arc::new(StringArray::from(messages)),
                Arc::new(StringArray::from(download_urls)),
            ],
        )
        .map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to build export results: {}", e))
        })?;

        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count,
            schema: Some(schema),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &ShowExportStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // Any authenticated user can view their own exports
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_export_schema_uses_timestamp_for_created_at() {
        let schema = ShowExportHandler::result_schema();
        let field = schema.field_with_name("created_at").expect("created_at field");

        assert!(matches!(field.data_type(), DataType::Timestamp(TimeUnit::Microsecond, None)));
    }

    #[test]
    fn show_export_created_at_converts_millis_to_micros() {
        assert_eq!(ShowExportHandler::created_at_micros(1_741_900_245_123), 1_741_900_245_123_000);
    }
}
