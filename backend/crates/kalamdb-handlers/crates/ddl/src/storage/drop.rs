//! Typed DDL handler for DROP STORAGE statements

use crate::helpers::guards::require_admin;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::DropStorageStatement;
use std::sync::Arc;

/// Typed handler for DROP STORAGE statements
pub struct DropStorageHandler {
    app_context: Arc<AppContext>,
}

impl DropStorageHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<DropStorageStatement> for DropStorageHandler {
    async fn execute(
        &self,
        statement: DropStorageStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let storage_id = statement.storage_id.clone();

        // Check if storage exists and if any tables use it (offload sync RocksDB reads)
        let app_ctx = self.app_context.clone();
        let sid = storage_id.clone();
        let (storage, tables_using_storage_count) = tokio::task::spawn_blocking(move || {
            let storages_provider = app_ctx.system_tables().storages();
            let tables_provider = app_ctx.system_tables().tables();

            let storage = storages_provider.get_storage_by_id(&sid).map_err(|e| {
                KalamDbError::ExecutionError(format!("Failed to get storage: {}", e))
            })?;

            let count = if storage.is_some() {
                let all_tables = tables_provider.list_tables().map_err(|e| {
                    KalamDbError::ExecutionError(format!("Failed to check tables: {}", e))
                })?;
                all_tables
                    .iter()
                    .filter(|t| match &t.table_options {
                        kalamdb_commons::schemas::TableOptions::User(opts) => {
                            opts.storage_id == sid
                        },
                        kalamdb_commons::schemas::TableOptions::Shared(opts) => {
                            opts.storage_id == sid
                        },
                        _ => false,
                    })
                    .count()
            } else {
                0
            };
            Ok::<_, KalamDbError>((storage, count))
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        if storage.is_none() {
            if statement.if_exists {
                return Ok(ExecutionResult::Success {
                    message: format!(
                        "Storage '{}' does not exist (skipped)",
                        statement.storage_id.as_str()
                    ),
                });
            }

            return Err(KalamDbError::InvalidOperation(format!(
                "Storage '{}' not found",
                statement.storage_id.as_str()
            )));
        }

        if tables_using_storage_count > 0 {
            return Err(KalamDbError::InvalidOperation(format!(
                "Cannot drop storage '{}': {} table(s) still using it",
                statement.storage_id, tables_using_storage_count
            )));
        }

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .drop_storage(storage_id.clone())
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("DROP STORAGE failed: {}", e)))?;

        // Invalidate storage cache
        self.app_context.storage_registry().invalidate(&storage_id);

        Ok(ExecutionResult::Success {
            message: format!("Storage '{}' dropped successfully", statement.storage_id),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &DropStorageStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        require_admin(context, "drop storage")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::{Role, StorageId};
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use std::sync::Arc;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context(role: Role) -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), role, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_drop_storage_authorization() {
        let app_ctx = init_app_context();
        let handler = DropStorageHandler::new(app_ctx);
        let stmt = DropStorageStatement {
            storage_id: StorageId::new("test_storage"),
            if_exists: false,
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

    #[ignore = "Requires Raft for CREATE/DROP STORAGE"]
    #[tokio::test]
    async fn test_drop_storage_success() {
        let app_ctx = init_app_context();
        let storages_provider = app_ctx.system_tables().storages();

        // Create a test storage
        let storage_id = format!("test_drop_{}", chrono::Utc::now().timestamp_millis());
        let storage = kalamdb_system::Storage {
            storage_id: StorageId::from(storage_id.as_str()),
            storage_name: "Test Drop".to_string(),
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

        // Drop it
        let handler = DropStorageHandler::new(app_ctx);
        let stmt = DropStorageStatement {
            storage_id: StorageId::from(storage_id.as_str()),
            if_exists: false,
        };
        let ctx = create_test_context(Role::System);
        let result = handler.execute(stmt, vec![], &ctx).await;

        assert!(result.is_ok());
        if let Ok(ExecutionResult::Success { message }) = result {
            assert!(message.contains(&storage_id));
        }

        // Verify deletion
        let deleted = storages_provider
            .get_storage_by_id(&StorageId::from(storage_id.as_str()))
            .unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_drop_storage_not_found() {
        let app_ctx = init_app_context();
        let handler = DropStorageHandler::new(app_ctx);
        let stmt = DropStorageStatement {
            storage_id: StorageId::from("nonexistent_storage"),
            if_exists: false,
        };
        let ctx = create_test_context(Role::System);

        let result = handler.execute(stmt, vec![], &ctx).await;

        assert!(result.is_err());
    }
}
