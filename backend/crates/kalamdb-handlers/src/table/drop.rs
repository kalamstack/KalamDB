//! Typed DDL handler for DROP TABLE statements
//!
//! This module provides both the DROP TABLE handler and reusable cleanup functions
//! for table deletion operations (used by both DDL handler and CleanupExecutor).

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_jobs::executors::cleanup::{CleanupOperation, CleanupParams, StorageCleanupDetails};
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_core::operations::table_cleanup::cleanup_table_data_internal;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use crate::helpers::guards::block_system_namespace_modification;
use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::TableType;
use kalamdb_sql::ddl::DropTableStatement;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Typed handler for DROP TABLE statements
pub struct DropTableHandler {
    app_context: Arc<AppContext>,
}

impl DropTableHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn capture_storage_cleanup_details(
        &self,
        table_id: &TableId,
        table_type: TableType,
    ) -> Result<StorageCleanupDetails, KalamDbError> {
        let registry = self.app_context.schema_registry();
        let cached = registry.get(table_id).ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Table cache entry not found for {}", table_id))
        })?;

        let storage_id = cached.storage_id.clone();

        // Get storage from registry (cached lookup)
        let storage = self
            .app_context
            .storage_registry()
            .get_storage(&storage_id)
            .map_err(|e| KalamDbError::Other(format!("Failed to get storage: {}", e)))?
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Storage '{}' not found",
                    storage_id.as_str()
                ))
            })?;

        // Get the appropriate template for this table type
        let relative_template = match table_type {
            TableType::User => storage.user_tables_template.clone(),
            TableType::Shared | TableType::Stream => storage.shared_tables_template.clone(),
            TableType::System => {
                return Err(KalamDbError::InvalidOperation(
                    "System tables do not use storage templates".to_string(),
                ))
            },
        };

        // Get base directory
        let base_dir = {
            let trimmed = storage.base_directory.trim();
            if trimmed.is_empty() {
                self.app_context.storage_registry().default_storage_path().to_string()
            } else {
                storage.base_directory.clone()
            }
        };

        Ok(StorageCleanupDetails {
            storage_id,
            base_directory: base_dir,
            relative_path_template: relative_template,
        })
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<DropTableStatement> for DropTableHandler {
    async fn execute(
        &self,
        statement: DropTableStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let table_id =
            TableId::from_strings(statement.namespace_id.as_str(), statement.table_name.as_str());

        log::debug!(
            "🗑️  DROP TABLE request: {}.{} (if_exists: {}, user: {}, role: {:?})",
            statement.namespace_id.as_str(),
            statement.table_name.as_str(),
            statement.if_exists,
            context.user_id().as_str(),
            context.user_role()
        );

        // Block DROP on system tables - they are managed internally
        block_system_namespace_modification(
            &statement.namespace_id,
            "DROP",
            "TABLE",
            Some(statement.table_name.as_str()),
        )?;

        // RBAC: authorize based on actual table type if exists
        let registry = self.app_context.schema_registry();
        let registry_def = registry.get_table_if_exists(&table_id)?;
        let actual_type = match registry_def.as_deref() {
            Some(def) => def.table_type,
            None => TableType::from(statement.table_type),
        };
        let is_owner = false;
        if !kalamdb_session::can_delete_table(context.user_role(), actual_type, is_owner) {
            log::error!(
                "❌ DROP TABLE {}.{}: Insufficient privileges (user: {}, role: {:?}, table_type: {:?})",
                statement.namespace_id.as_str(),
                statement.table_name.as_str(),
                context.user_id().as_str(),
                context.user_role(),
                actual_type
            );
            return Err(KalamDbError::Unauthorized(
                "Insufficient privileges to drop this table".to_string(),
            ));
        }

        // Check existence via system.tables provider (for IF EXISTS behavior)
        // Offload sync RocksDB read to blocking thread
        let app_ctx = self.app_context.clone();
        let tid = table_id.clone();
        let table_metadata = tokio::task::spawn_blocking(move || {
            app_ctx.system_tables().tables().get_table_by_id(&tid)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;
        let exists = table_metadata.is_some() || registry_def.is_some();

        if !exists {
            if statement.if_exists {
                log::debug!(
                    "ℹ️  DROP TABLE {}.{}: Table does not exist (IF EXISTS - skipping)",
                    statement.namespace_id.as_str(),
                    statement.table_name.as_str()
                );
                return Ok(ExecutionResult::Success {
                    message: format!(
                        "Table {}.{} does not exist (skipped)",
                        statement.namespace_id.as_str(),
                        statement.table_name.as_str()
                    ),
                });
            } else {
                log::warn!(
                    "⚠️  DROP TABLE failed: Table '{}' not found in namespace '{}'",
                    statement.table_name.as_str(),
                    statement.namespace_id.as_str()
                );
                return Err(KalamDbError::NotFound(format!(
                    "Table '{}' not found in namespace '{}'",
                    statement.table_name.as_str(),
                    statement.namespace_id.as_str()
                )));
            }
        }

        // Log table details before dropping
        if let Some(metadata) = table_metadata {
            log::debug!(
                "📋 Dropping table: type={:?}, columns={}, created_at={:?}",
                actual_type,
                metadata.columns.len(),
                metadata.created_at
            );
        }

        let app_ctx = self.app_context.clone();
        let tid = table_id.clone();
        let at = actual_type;
        let storage_details = tokio::task::spawn_blocking(move || {
            // capture_storage_cleanup_details uses SchemaRegistry + StorageRegistry caches
            // which can fall through to sync RocksDB on cache miss
            let handler = DropTableHandler::new(app_ctx);
            handler.capture_storage_cleanup_details(&tid, at)
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        // Cancel any active flush jobs for this table before dropping
        let job_manager = self.app_context.job_manager();
        let flush_filter = kalamdb_system::JobFilter {
            job_type: Some(kalamdb_system::JobType::Flush),
            status: None, // Check all non-completed statuses
            idempotency_key: None,
            limit: None,
            created_after: None,
            created_before: None,
            ..Default::default()
        };

        let flush_jobs = job_manager.list_jobs(flush_filter).await?;
        let mut cancelled_count = 0;

        // Filter jobs by namespace and table from parameters
        let target_namespace = statement.namespace_id.clone();
        let target_table = statement.table_name.clone();

        for job in flush_jobs {
            // Check if this job is for the target table (namespace_id and table_name in parameters)
            let matches_table = job.namespace_id().as_ref() == Some(&target_namespace)
                && job.table_name().as_ref() == Some(&target_table);

            if !matches_table {
                continue;
            }

            // Only cancel jobs that are not already completed/failed/cancelled
            if matches!(
                job.status,
                kalamdb_system::JobStatus::New
                    | kalamdb_system::JobStatus::Queued
                    | kalamdb_system::JobStatus::Running
            ) {
                match job_manager.cancel_job(&job.job_id).await {
                    Ok(_) => {
                        log::debug!(
                            "🛑 Cancelled flush job {} for table {}.{} before DROP",
                            job.job_id,
                            statement.namespace_id.as_str(),
                            statement.table_name.as_str()
                        );
                        cancelled_count += 1;
                    },
                    Err(e) => {
                        log::warn!(
                            "⚠️  Failed to cancel flush job {} for table {}.{}: {}",
                            job.job_id,
                            statement.namespace_id.as_str(),
                            statement.table_name.as_str(),
                            e
                        );
                    },
                }
            }
        }

        if cancelled_count > 0 {
            log::debug!(
                "🛑 Cancelled {} active flush job(s) for table {}.{}",
                cancelled_count,
                statement.namespace_id.as_str(),
                statement.table_name.as_str()
            );
        }

        // TODO: Check active live queries/subscriptions before dropping (Phase 9 integration)

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .drop_table(table_id.clone())
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("DROP TABLE failed: {}", e)))?;

        // Release RocksDB partitions immediately so DROP TABLE reduces the
        // physical storage footprint and descriptor pressure before the
        // asynchronous cleanup job handles cold-storage cleanup.
        cleanup_table_data_internal(&self.app_context, &table_id, actual_type).await?;

        // Create cleanup job for async data removal (with retry on failure)
        let params = CleanupParams {
            table_id: table_id.clone(),
            table_type: actual_type,
            operation: CleanupOperation::DropTable,
            storage: storage_details,
        };

        let idempotency_key =
            format!("drop-{}-{}", statement.namespace_id.as_str(), statement.table_name.as_str());

        let job_manager = self.app_context.job_manager();
        let job_id = job_manager
            .create_job_typed(JobType::Cleanup, params, Some(idempotency_key), None)
            .await?;

        // Log DDL operation
        use crate::helpers::audit;
        let audit_entry = audit::log_ddl_operation(
            context,
            "DROP",
            "TABLE",
            &format!("{}.{}", statement.namespace_id.as_str(), statement.table_name.as_str()),
            Some(format!("Type: {:?}, Cleanup Job: {}", actual_type, job_id.as_str())),
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        log::debug!(
            "✅ DROP TABLE succeeded: {}.{} (type: {:?}) - Cleanup job: {}",
            statement.namespace_id.as_str(),
            statement.table_name.as_str(),
            actual_type,
            job_id.as_str()
        );

        Ok(ExecutionResult::Success {
            message: format!(
                "Table {}.{} dropped successfully. Cleanup job: {}",
                statement.namespace_id.as_str(),
                statement.table_name.as_str(),
                job_id.as_str()
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &DropTableStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use crate::helpers::guards::block_anonymous_write;

        // T050: Block anonymous users from DDL operations
        block_anonymous_write(context, "DROP TABLE")?;

        // Coarse auth gate (fine-grained check performed in execute using actual table type)
        if context.is_system() || context.is_admin() {
            return Ok(());
        }
        // Allow users to attempt; execute() will enforce per-table RBAC
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{cleanup_table_data_internal, DropTableHandler};
    use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult};
    use crate::table::create::CreateTableHandler;
    use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
    use kalamdb_core::test_helpers::{
        create_test_session_simple, test_app_context, test_app_context_simple,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use kalamdb_commons::models::{NamespaceId, TableName, UserId};
    use kalamdb_commons::schemas::TableType;
    use kalamdb_commons::Role;
    use kalamdb_sql::ddl::{CreateTableStatement, DropTableStatement, TableKind};
    use kalamdb_store::EntityStore;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_context(role: Role) -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), role, create_test_session_simple())
    }

    fn create_user_table_statement(table_name: TableName) -> CreateTableStatement {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
        ]));

        CreateTableStatement {
            namespace_id: NamespaceId::default(),
            table_name,
            table_type: TableType::User,
            schema,
            column_defaults: HashMap::new(),
            primary_key_column: Some("id".to_string()),
            storage_id: None,
            use_user_storage: false,
            flush_policy: None,
            deleted_retention_hours: None,
            ttl_seconds: None,
            if_not_exists: false,
            access_level: None,
        }
    }

    #[tokio::test]
    async fn cleanup_table_data_drops_user_table_partitions() {
        let app_ctx = test_app_context_simple();
        let table_name =
            TableName::new(format!("cleanup_drop_{}", chrono::Utc::now().timestamp_millis()));
        let table_id = kalamdb_commons::models::TableId::new(NamespaceId::default(), table_name);

        let store = kalamdb_tables::new_indexed_user_table_store(
            app_ctx.storage_backend(),
            &table_id,
            "id",
        );
        let main_partition = store.partition();
        let pk_partition = store.indexes()[0].partition();

        assert!(app_ctx.storage_backend().partition_exists(&main_partition));
        assert!(app_ctx.storage_backend().partition_exists(&pk_partition));

        cleanup_table_data_internal(&app_ctx, &table_id, TableType::User)
            .await
            .expect("cleanup table data");

        assert!(!app_ctx.storage_backend().partition_exists(&main_partition));
        assert!(!app_ctx.storage_backend().partition_exists(&pk_partition));
    }

    #[ignore = "Requires Raft for CREATE/DROP TABLE"]
    #[tokio::test]
    async fn drop_table_releases_user_partitions_before_returning() {
        let app_ctx = test_app_context();
        let table_name =
            TableName::new(format!("drop_release_{}", chrono::Utc::now().timestamp_millis()));
        let create_handler = CreateTableHandler::new(app_ctx.clone());
        let drop_handler = DropTableHandler::new(app_ctx.clone());
        let ctx = create_test_context(Role::Dba);

        let create_result = create_handler
            .execute(create_user_table_statement(table_name.clone()), vec![], &ctx)
            .await;
        assert!(create_result.is_ok(), "CREATE TABLE failed: {:?}", create_result);

        let table_id =
            kalamdb_commons::models::TableId::new(NamespaceId::default(), table_name.clone());
        let main_partition =
            kalamdb_store::storage_trait::Partition::new(format!("user_{}", table_id));
        let pk_partition =
            kalamdb_store::storage_trait::Partition::new(format!("user_{}_pk_idx", table_id));

        assert!(app_ctx.storage_backend().partition_exists(&main_partition));
        assert!(app_ctx.storage_backend().partition_exists(&pk_partition));

        let drop_result = drop_handler
            .execute(
                DropTableStatement {
                    namespace_id: NamespaceId::default(),
                    table_name,
                    table_type: TableKind::User,
                    if_exists: false,
                },
                vec![],
                &ctx,
            )
            .await;

        assert!(drop_result.is_ok(), "DROP TABLE failed: {:?}", drop_result);
        assert!(!app_ctx.storage_backend().partition_exists(&main_partition));
        assert!(!app_ctx.storage_backend().partition_exists(&pk_partition));

        if let Ok(ExecutionResult::Success { message }) = drop_result {
            assert!(message.contains("Cleanup job"));
        }
    }
}
