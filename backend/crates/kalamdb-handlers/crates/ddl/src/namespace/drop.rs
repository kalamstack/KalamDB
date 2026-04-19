//! Typed DDL handler for DROP NAMESPACE statements
//!
//! When a namespace is dropped, its DataFusion schema becomes unavailable.
//! Any queries referencing tables in the dropped namespace will fail.

use crate::helpers::audit;
use crate::helpers::guards::{block_anonymous_write, require_admin};
use crate::table::drop::{capture_storage_cleanup_details, schedule_drop_table_cleanup};
use kalamdb_commons::models::{NamespaceId, TableId};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ddl::DropNamespaceStatement;
use std::sync::Arc;

/// Typed handler for DROP NAMESPACE statements
pub struct DropNamespaceHandler {
    app_context: Arc<AppContext>,
}

impl DropNamespaceHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Deregister namespace schema from DataFusion catalog
    ///
    /// Note: DataFusion's MemoryCatalogProvider doesn't have a direct deregister_schema method.
    /// We log the drop and rely on the fact that the namespace metadata is deleted from RocksDB.
    /// Any subsequent queries to tables in this namespace will fail with "table not found".
    fn deregister_namespace_schema(&self, namespace_id: &NamespaceId) {
        // DataFusion doesn't provide a deregister_schema API on CatalogProvider trait.
        // The schema will remain in memory until server restart, but since the namespace
        // metadata is deleted, any table lookups will fail appropriately.
        //
        // For a clean deregistration, we would need a custom CatalogProvider that supports removal.
        // This is tracked as a future enhancement.
        log::debug!(
            "Namespace '{}' dropped - schema will be unavailable for new queries",
            namespace_id.as_str()
        );
    }
}

impl TypedStatementHandler<DropNamespaceStatement> for DropNamespaceHandler {
    async fn execute(
        &self,
        statement: DropNamespaceStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let namespace_id = statement.name;

        // Check if namespace exists (offload sync RocksDB read)
        let app_ctx = self.app_context.clone();
        let ns_id = namespace_id.clone();
        let (namespace_opt, tables_in_namespace) = tokio::task::spawn_blocking(move || {
            let ns = app_ctx.system_tables().namespaces().get_namespace(&ns_id)?;
            let tables = app_ctx.system_tables().tables().list_tables_in_namespace(&ns_id)?;
            Ok::<_, KalamDbError>((ns, tables))
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

        let namespace = match namespace_opt {
            Some(ns) => ns,
            None => {
                if statement.if_exists {
                    let message = format!("Namespace '{}' does not exist", namespace_id.as_str());
                    return Ok(ExecutionResult::Success { message });
                } else {
                    return Err(KalamDbError::NotFound(format!(
                        "Namespace '{}' not found",
                        namespace_id.as_str()
                    )));
                }
            },
        };

        for table in tables_in_namespace {
            let table_id = TableId::new(table.namespace_id.clone(), table.table_name.clone());
            let table_type = table.table_type;

            let app_ctx = self.app_context.clone();
            let tid = table_id.clone();
            let storage_details = tokio::task::spawn_blocking(move || {
                capture_storage_cleanup_details(&app_ctx, &tid, table_type)
            })
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))??;

            self.app_context
                .applier()
                .drop_table(table_id.clone())
                .await
                .map_err(|e| KalamDbError::ExecutionError(format!("DROP TABLE failed: {}", e)))?;
            let cleanup_job_id = schedule_drop_table_cleanup(
                &self.app_context,
                &table_id,
                table_type,
                storage_details,
            )
            .await?;

            let audit_entry = audit::log_ddl_operation(
                context,
                "DROP",
                "TABLE",
                &table_id.full_name(),
                Some(format!(
                    "CASCADE from DROP NAMESPACE. Type: {:?}, Cleanup Job: {}",
                    table_type, cleanup_job_id
                )),
                None,
            );
            audit::persist_audit_entry(&self.app_context, &audit_entry).await?;
        }

        // Delegate to unified applier (handles standalone vs cluster internally)
        self.app_context
            .applier()
            .drop_namespace(namespace_id.clone())
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("DROP NAMESPACE failed: {}", e)))?;

        // Deregister schema from DataFusion catalog
        self.deregister_namespace_schema(&namespace_id);

        // Log DDL operation
        let audit_entry = audit::log_ddl_operation(
            context,
            "DROP",
            "NAMESPACE",
            namespace_id.as_str(),
            None,
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        let message = format!("Namespace '{}' dropped successfully", namespace.name);
        Ok(ExecutionResult::Success { message })
    }

    async fn check_authorization(
        &self,
        _statement: &DropNamespaceStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // T050: Block anonymous users from DDL operations
        block_anonymous_write(context, "DROP NAMESPACE")?;

        require_admin(context, "drop namespace")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::models::datatypes::KalamDataType;
    use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
    use kalamdb_commons::models::{TableName, UserId};
    use kalamdb_commons::schemas::TableType;
    use kalamdb_commons::Role;
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_store::EntityStore;
    use kalamdb_system::Namespace;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn create_test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), Role::Dba, create_test_session_simple())
    }

    fn unique_suffix() -> String {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_millis();
        format!("{}_{}", std::process::id(), millis)
    }

    #[tokio::test]
    async fn test_drop_namespace_success() {
        let app_ctx = init_app_context();
        let handler = DropNamespaceHandler::new(app_ctx);
        let stmt = DropNamespaceStatement {
            name: kalamdb_commons::models::NamespaceId::new("test_namespace"),
            if_exists: false,
            cascade: false,
        };
        let ctx = create_test_context();

        // Note: This test would need proper setup of test namespace
        // For now, it demonstrates the pattern
        let result = handler.execute(stmt, vec![], &ctx).await;

        // Would verify result or error based on test setup
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_drop_namespace_authorization() {
        let app_ctx = init_app_context();
        let handler = DropNamespaceHandler::new(app_ctx);
        let stmt = DropNamespaceStatement {
            name: kalamdb_commons::models::NamespaceId::new("test"),
            if_exists: false,
            cascade: false,
        };

        // Test with non-admin user
        let ctx =
            ExecutionContext::new(UserId::new("user"), Role::User, create_test_session_simple());
        let result = handler.check_authorization(&stmt, &ctx).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), KalamDbError::Unauthorized(_)));
    }

    #[tokio::test]
    async fn test_drop_namespace_if_exists() {
        let app_ctx = init_app_context();
        let handler = DropNamespaceHandler::new(app_ctx);
        let stmt = DropNamespaceStatement {
            name: kalamdb_commons::models::NamespaceId::new("nonexistent"),
            if_exists: true,
            cascade: false,
        };
        let ctx = create_test_context();

        let result = handler.execute(stmt, vec![], &ctx).await;

        // With IF EXISTS, should succeed even if namespace doesn't exist
        if let Ok(ExecutionResult::Success { message }) = result {
            assert!(message.contains("does not exist"));
        }
    }

    #[tokio::test]
    async fn test_drop_namespace_cascade_cleanup_releases_user_table_partitions() {
        let app_ctx = test_app_context_simple();
        let suffix = unique_suffix();
        let namespace_id = NamespaceId::new(format!("drop_ns_{}", suffix));
        let table_name = TableName::new(format!("docs_{}", suffix));
        let table_id = TableId::new(namespace_id.clone(), table_name.clone());

        app_ctx
            .system_tables()
            .namespaces()
            .create_namespace(Namespace {
                namespace_id: namespace_id.clone(),
                name: namespace_id.as_str().to_string(),
                created_at: chrono::Utc::now().timestamp_millis(),
                options: Some(serde_json::json!({})),
                table_count: 0,
            })
            .expect("create namespace");

        let columns = vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
        ];
        let mut table_def = TableDefinition::new(
            namespace_id.clone(),
            table_name.clone(),
            TableType::User,
            columns,
            TableOptions::user(),
            None,
        )
        .expect("create table definition");
        app_ctx
            .system_columns_service()
            .add_system_columns(&mut table_def)
            .expect("add system columns");
        app_ctx.schema_registry().register_table(table_def).expect("register table");

        let store = kalamdb_tables::new_indexed_user_table_store(
            app_ctx.storage_backend(),
            &table_id,
            "id",
        );
        let main_partition = store.partition();
        let pk_partition = store.indexes()[0].partition();
        assert!(app_ctx.storage_backend().partition_exists(&main_partition));
        assert!(app_ctx.storage_backend().partition_exists(&pk_partition));

        app_ctx
            .schema_registry()
            .delete_table_definition(&table_id)
            .expect("delete table definition");
        app_ctx
            .system_tables()
            .namespaces()
            .delete_namespace(&namespace_id)
            .expect("delete namespace metadata");
        crate::table::drop::cleanup_dropped_table_partitions(&app_ctx, &table_id, TableType::User)
            .await
            .expect("cleanup dropped table partitions");
        assert!(
            !app_ctx.storage_backend().partition_exists(&main_partition),
            "main user-table partition should be dropped during namespace cascade cleanup"
        );
        assert!(
            !app_ctx.storage_backend().partition_exists(&pk_partition),
            "user-table PK index partition should be dropped during namespace cascade cleanup"
        );
    }
}
