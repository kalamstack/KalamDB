//! Typed DDL handler for CREATE STORAGE statements

use std::sync::Arc;

use kalamdb_commons::models::StorageId;
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    error_extensions::KalamDbResultExt,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_filestore::StorageHealthService;
use kalamdb_sql::ddl::CreateStorageStatement;
use kalamdb_system::StorageType;

use crate::helpers::{guards::require_admin, storage::ensure_filesystem_directory};

/// Typed handler for CREATE STORAGE statements
pub struct CreateStorageHandler {
    app_context: Arc<AppContext>,
}

impl CreateStorageHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<CreateStorageStatement> for CreateStorageHandler {
    async fn execute(
        &self,
        statement: CreateStorageStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        // Extract providers from AppContext
        let storage_registry = self.app_context.storage_registry();

        // Check if storage already exists (offload sync RocksDB read)
        let storage_id = StorageId::from(statement.storage_id.as_str());
        let app_ctx = self.app_context.clone();
        let sid = storage_id.clone();
        let existing = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().storages().get_storage_by_id(&sid)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))?
        .into_kalamdb_error("Failed to check storage")?;
        if existing.is_some() {
            return Err(KalamDbError::InvalidOperation(format!(
                "Storage '{}' already exists",
                statement.storage_id
            )));
        }

        // Validate templates using StorageRegistry
        if !statement.shared_tables_template.is_empty() {
            storage_registry.validate_template(&statement.shared_tables_template, false)?;
        }
        if !statement.user_tables_template.is_empty() {
            storage_registry.validate_template(&statement.user_tables_template, true)?;
        }

        // Filesystem storages must eagerly create their base directories
        if statement.storage_type == StorageType::Filesystem {
            ensure_filesystem_directory(&statement.base_directory)?;
        }

        // Validate credentials JSON (if provided)
        let normalized_credentials = if let Some(raw) = statement.credentials.as_ref() {
            let value: serde_json::Value =
                serde_json::from_str(raw).into_invalid_operation("Invalid credentials JSON")?;

            if !value.is_object() {
                return Err(KalamDbError::InvalidOperation(
                    "Credentials must be a JSON object".to_string(),
                ));
            }

            Some(value)
        } else {
            None
        };

        // Validate config JSON (if provided)
        let normalized_config_json = if let Some(raw) = statement.config_json.as_ref() {
            let value: serde_json::Value =
                serde_json::from_str(raw).into_invalid_operation("Invalid CONFIG JSON")?;

            if !value.is_object() {
                return Err(KalamDbError::InvalidOperation(
                    "CONFIG must be a JSON object".to_string(),
                ));
            }

            Some(value)
        } else {
            None
        };

        // Create storage record
        let storage = kalamdb_system::Storage {
            storage_id: statement.storage_id.clone(),
            storage_name: statement.storage_name,
            description: statement.description,
            storage_type: statement.storage_type,
            base_directory: statement.base_directory,
            credentials: normalized_credentials,
            config_json: normalized_config_json,
            shared_tables_template: statement.shared_tables_template,
            user_tables_template: statement.user_tables_template,
            created_at: chrono::Utc::now().timestamp_millis(),
            updated_at: chrono::Utc::now().timestamp_millis(),
        };

        let connectivity = StorageHealthService::test_connectivity(&storage)
            .await
            .into_kalamdb_error("Storage connectivity check failed")?;

        if !connectivity.connected {
            let error =
                connectivity.error.unwrap_or_else(|| "Unknown connectivity error".to_string());
            return Err(KalamDbError::InvalidOperation(format!(
                "Storage connectivity check failed (latency {} ms): {}",
                connectivity.latency_ms, error
            )));
        }

        let health_result = StorageHealthService::run_full_health_check(&storage)
            .await
            .into_kalamdb_error("Storage health check failed")?;

        if !health_result.is_healthy() {
            let error = health_result
                .error
                .unwrap_or_else(|| "Unknown storage health error".to_string());
            return Err(KalamDbError::InvalidOperation(format!(
                "Storage health check failed (status {}, readable={}, writable={}, listable={}, \
                 deletable={}, latency {} ms): {}",
                health_result.status,
                health_result.readable,
                health_result.writable,
                health_result.listable,
                health_result.deletable,
                health_result.latency_ms,
                error
            )));
        }

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .create_storage(storage)
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("CREATE STORAGE failed: {}", e)))?;

        Ok(ExecutionResult::Success {
            message: format!("Storage '{}' created successfully", statement.storage_id),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CreateStorageStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        require_admin(context, "create storage")
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
    async fn test_create_storage_authorization() {
        let app_ctx = init_app_context();
        let handler = CreateStorageHandler::new(app_ctx);
        let stmt = CreateStorageStatement {
            storage_id: StorageId::new("test_storage"),
            storage_name: "Test Storage".to_string(),
            description: None,
            storage_type: kalamdb_system::providers::storages::models::StorageType::from("local"),
            base_directory: "/tmp/storage".to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: String::new(),
            user_tables_template: String::new(),
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

    #[ignore = "Requires Raft for CREATE STORAGE"]
    #[tokio::test]
    async fn test_create_storage_success() {
        let app_ctx = init_app_context();
        let handler = CreateStorageHandler::new(app_ctx);
        let storage_id = format!("test_storage_{}", chrono::Utc::now().timestamp_millis());
        let stmt = CreateStorageStatement {
            storage_id: StorageId::from(storage_id.as_str()),
            storage_name: "Test Storage".to_string(),
            description: Some("Test description".to_string()),
            storage_type: kalamdb_system::providers::storages::models::StorageType::from("local"),
            base_directory: "/tmp/test".to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: String::new(),
            user_tables_template: String::new(),
        };
        let ctx = create_test_context(Role::System);

        let result = handler.execute(stmt, vec![], &ctx).await;

        assert!(result.is_ok());
        if let Ok(ExecutionResult::Success { message }) = result {
            assert!(message.contains(&storage_id));
        }
    }

    #[ignore = "Requires Raft for CREATE STORAGE"]
    #[tokio::test]
    async fn test_create_storage_duplicate() {
        let app_ctx = init_app_context();
        let handler = CreateStorageHandler::new(app_ctx);
        let storage_id = format!("test_dup_{}", chrono::Utc::now().timestamp_millis());
        let stmt = CreateStorageStatement {
            storage_id: StorageId::from(storage_id.as_str()),
            storage_name: "Test Duplicate".to_string(),
            description: None,
            storage_type: kalamdb_system::providers::storages::models::StorageType::from("local"),
            base_directory: "/tmp/test".to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: String::new(),
            user_tables_template: String::new(),
        };
        let ctx = create_test_context(Role::System);

        // First creation should succeed
        let result1 = handler.execute(stmt.clone(), vec![], &ctx).await;
        assert!(result1.is_ok());

        // Second creation should fail
        let result2 = handler.execute(stmt, vec![], &ctx).await;
        assert!(result2.is_err());
    }
}
