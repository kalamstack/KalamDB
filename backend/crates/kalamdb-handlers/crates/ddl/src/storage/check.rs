//! Typed DDL handler for STORAGE CHECK statements

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    error_extensions::KalamDbResultExt,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_filestore::{HealthStatus, StorageHealthService};
use kalamdb_sql::ddl::CheckStorageStatement;

use crate::helpers::guards::require_admin;

/// Typed handler for STORAGE CHECK statements
pub struct CheckStorageHandler {
    app_context: Arc<AppContext>,
}

impl CheckStorageHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Build the result schema for STORAGE CHECK
    fn result_schema() -> Schema {
        Schema::new(vec![
            Field::new("storage_id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("readable", DataType::Boolean, false),
            Field::new("writable", DataType::Boolean, false),
            Field::new("listable", DataType::Boolean, false),
            Field::new("deletable", DataType::Boolean, false),
            Field::new("latency_ms", DataType::Int64, false),
            Field::new("total_bytes", DataType::Int64, true),
            Field::new("used_bytes", DataType::Int64, true),
            Field::new("error", DataType::Utf8, true),
            Field::new("tested_at", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        ])
    }
}

impl TypedStatementHandler<CheckStorageStatement> for CheckStorageHandler {
    async fn execute(
        &self,
        statement: CheckStorageStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Get the storage by ID (offload sync RocksDB read)
        let app_ctx = self.app_context.clone();
        let sid = statement.storage_id.clone();
        let storage = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().storages().get_storage_by_id(&sid)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))?
        .into_kalamdb_error("Failed to get storage")?
        .ok_or_else(|| {
            KalamDbError::NotFound(format!("Storage '{}' not found", statement.storage_id.as_str()))
        })?;

        // Run the health check
        let mut health_result = StorageHealthService::run_full_health_check(&storage)
            .await
            .into_kalamdb_error("Health check failed")?;

        if !statement.extended {
            health_result.total_bytes = None;
            health_result.used_bytes = None;
        }

        // Build the result RecordBatch
        let schema = Arc::new(Self::result_schema());

        let mut storage_id_builder = StringBuilder::new();
        let mut status_builder = StringBuilder::new();
        let mut readable_builder = BooleanBuilder::new();
        let mut writable_builder = BooleanBuilder::new();
        let mut listable_builder = BooleanBuilder::new();
        let mut deletable_builder = BooleanBuilder::new();
        let mut latency_builder = Int64Builder::new();
        let mut total_bytes_builder = Int64Builder::new();
        let mut used_bytes_builder = Int64Builder::new();
        let mut error_builder = StringBuilder::new();
        let mut tested_at_builder = TimestampMillisecondBuilder::new();

        storage_id_builder.append_value(statement.storage_id.as_str());
        status_builder.append_value(match health_result.status {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded => "degraded",
            HealthStatus::Unreachable => "unreachable",
        });
        readable_builder.append_value(health_result.readable);
        writable_builder.append_value(health_result.writable);
        listable_builder.append_value(health_result.listable);
        deletable_builder.append_value(health_result.deletable);
        latency_builder.append_value(health_result.latency_ms as i64);

        match health_result.total_bytes {
            Some(total) => total_bytes_builder.append_value(total as i64),
            None => total_bytes_builder.append_null(),
        }
        match health_result.used_bytes {
            Some(used) => used_bytes_builder.append_value(used as i64),
            None => used_bytes_builder.append_null(),
        }
        match health_result.error.as_ref() {
            Some(err) => error_builder.append_value(err),
            None => error_builder.append_null(),
        }
        tested_at_builder.append_value(health_result.tested_at);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(storage_id_builder.finish()),
            Arc::new(status_builder.finish()),
            Arc::new(readable_builder.finish()),
            Arc::new(writable_builder.finish()),
            Arc::new(listable_builder.finish()),
            Arc::new(deletable_builder.finish()),
            Arc::new(latency_builder.finish()),
            Arc::new(total_bytes_builder.finish()),
            Arc::new(used_bytes_builder.finish()),
            Arc::new(error_builder.finish()),
            Arc::new(tested_at_builder.finish()),
        ];

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .into_kalamdb_error("Failed to build result batch")?;

        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count: 1,
            schema: Some(schema),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CheckStorageStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        require_admin(context, "check storage health")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kalamdb_commons::{models::UserId, Role, StorageId};
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};

    use super::*;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context(role: Role) -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), role, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_check_storage_authorization() {
        let app_ctx = init_app_context();
        let handler = CheckStorageHandler::new(app_ctx);
        let stmt = CheckStorageStatement {
            storage_id: StorageId::local(),
            extended: false,
        };

        // User role should be denied
        let user_ctx = create_test_context(Role::User);
        let result = handler.check_authorization(&stmt, &user_ctx).await;
        assert!(result.is_err());

        // DBA role should be allowed
        let dba_ctx = create_test_context(Role::Dba);
        let result = handler.check_authorization(&stmt, &dba_ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_storage_not_found() {
        let app_ctx = init_app_context();
        let handler = CheckStorageHandler::new(app_ctx);
        let stmt = CheckStorageStatement {
            storage_id: StorageId::from("nonexistent_storage"),
            extended: false,
        };
        let ctx = create_test_context(Role::System);

        let result = handler.execute(stmt, vec![], &ctx).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_check_storage_local_success() {
        let app_ctx = init_app_context();
        let handler = CheckStorageHandler::new(app_ctx);
        let stmt = CheckStorageStatement {
            storage_id: StorageId::local(),
            extended: false,
        };
        let ctx = create_test_context(Role::System);

        let result = handler.execute(stmt, vec![], &ctx).await;

        // Should succeed for local storage
        if let Err(err) = &result {
            panic!("STORAGE CHECK local failed: {}", err);
        }
        if let Ok(ExecutionResult::Rows {
            batches, row_count, ..
        }) = result
        {
            assert_eq!(row_count, 1);
            assert!(!batches.is_empty());

            // Verify the batch has the expected columns
            let batch = &batches[0];
            assert_eq!(batch.num_columns(), 11);
            assert_eq!(batch.num_rows(), 1);
        }
    }
}
