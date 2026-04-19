//! Typed DDL handler for ALTER STORAGE statements

use crate::helpers::guards::require_admin;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_filestore::StorageHealthService;
use kalamdb_sql::ddl::AlterStorageStatement;
use std::sync::Arc;

/// Typed handler for ALTER STORAGE statements
pub struct AlterStorageHandler {
    app_context: Arc<AppContext>,
}

impl AlterStorageHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<AlterStorageStatement> for AlterStorageHandler {
    async fn execute(
        &self,
        statement: AlterStorageStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let storage_registry = self.app_context.storage_registry();

        // Get existing storage (offload sync RocksDB read)
        let storage_id = statement.storage_id.clone();
        let app_ctx = self.app_context.clone();
        let sid = storage_id.clone();
        let mut storage = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().storages().get_storage_by_id(&sid)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))?
        .into_kalamdb_error("Failed to get storage")?
        .ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "Storage '{}' not found",
                statement.storage_id.as_str()
            ))
        })?;

        // Update fields if provided
        if let Some(name) = statement.storage_name {
            storage.storage_name = name;
        }

        if let Some(desc) = statement.description {
            storage.description = Some(desc);
        }

        if let Some(shared_template) = statement.shared_tables_template {
            // Validate template before updating
            if !shared_template.is_empty() {
                storage_registry.validate_template(&shared_template, false)?;
            }
            storage.shared_tables_template = shared_template;
        }

        if let Some(user_template) = statement.user_tables_template {
            // Validate template before updating
            if !user_template.is_empty() {
                storage_registry.validate_template(&user_template, true)?;
            }
            storage.user_tables_template = user_template;
        }

        if let Some(raw_config) = statement.config_json {
            let value: serde_json::Value =
                serde_json::from_str(&raw_config).into_invalid_operation("Invalid config_json")?;

            if !value.is_object() {
                return Err(KalamDbError::InvalidOperation(
                    "CONFIG must be a JSON object".to_string(),
                ));
            }

            storage.config_json = Some(value);
        }

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
                "Storage health check failed (status {}, readable={}, writable={}, listable={}, deletable={}, latency {} ms): {}",
                health_result.status,
                health_result.readable,
                health_result.writable,
                health_result.listable,
                health_result.deletable,
                health_result.latency_ms,
                error
            )));
        }

        // Update timestamp
        storage.updated_at = chrono::Utc::now().timestamp_millis();

        // Save updated storage (offload sync RocksDB write)
        let app_ctx = self.app_context.clone();
        tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().storages().update_storage(storage)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))?
        .into_kalamdb_error("Failed to update storage")?;

        // Invalidate storage cache to ensure fresh data on next access
        storage_registry.invalidate(&storage_id);

        Ok(ExecutionResult::Success {
            message: format!("Storage '{}' altered successfully", statement.storage_id),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &AlterStorageStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        require_admin(context, "alter storage")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::{Role, StorageId};
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_system::Storage;
    use std::sync::Arc;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context(role: Role) -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), role, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_alter_storage_authorization() {
        let app_ctx = init_app_context();
        let handler = AlterStorageHandler::new(app_ctx);
        let stmt = AlterStorageStatement {
            storage_id: StorageId::from("test_storage"),
            storage_name: Some("Updated Storage".to_string()),
            description: None,
            shared_tables_template: None,
            user_tables_template: None,
            config_json: None,
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
    async fn test_alter_storage_success() {
        let app_ctx = init_app_context();

        // First create a storage to alter
        let storages_provider = app_ctx.system_tables().storages();
        let storage_id = format!("test_alter_{}", chrono::Utc::now().timestamp_millis());
        let storage = Storage {
            storage_id: StorageId::from(storage_id.as_str()),
            storage_name: "Original Name".to_string(),
            description: None,
            storage_type: kalamdb_system::providers::storages::models::StorageType::Filesystem,
            base_directory: "/tmp/test".to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: String::new(),
            user_tables_template: String::new(),
            created_at: chrono::Utc::now().timestamp_millis(),
            updated_at: chrono::Utc::now().timestamp_millis(),
        };
        storages_provider.insert_storage(storage).unwrap();

        // Now alter it
        let handler = AlterStorageHandler::new(app_ctx);
        let stmt = AlterStorageStatement {
            storage_id: StorageId::from(storage_id.as_str()),
            storage_name: Some("Updated Name".to_string()),
            description: Some("New description".to_string()),
            shared_tables_template: None,
            user_tables_template: None,
            config_json: None,
        };
        let ctx = create_test_context(Role::System);

        let result = handler.execute(stmt, vec![], &ctx).await;

        assert!(result.is_ok());
        if let Ok(ExecutionResult::Success { message }) = result {
            assert!(message.contains(&storage_id));
        }

        // Verify the changes
        let updated = storages_provider
            .get_storage_by_id(&StorageId::from(storage_id.as_str()))
            .unwrap();
        assert!(updated.is_some());
        let updated = updated.unwrap();
        assert_eq!(updated.storage_name, "Updated Name");
        assert_eq!(updated.description, Some("New description".to_string()));
    }
}
