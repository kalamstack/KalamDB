//! Typed DDL handler for CREATE TABLE statements

use std::sync::Arc;

use kalamdb_commons::{models::TableId, schemas::TableType};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    error_extensions::KalamDbResultExt,
    sql::{
        context::{ExecutionContext, ExecutionResult, ScalarValue},
        executor::handlers::TypedStatementHandler,
    },
};
use kalamdb_sql::ddl::CreateTableStatement;

use crate::helpers::async_blocking::run_blocking;

/// Typed handler for CREATE TABLE statements (all table types: USER, SHARED, STREAM)
pub struct CreateTableHandler {
    app_context: Arc<AppContext>,
}

impl CreateTableHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    async fn maybe_existing_if_not_exists_message(
        &self,
        statement: &CreateTableStatement,
    ) -> Result<Option<String>, KalamDbError> {
        if !statement.if_not_exists {
            return Ok(None);
        }

        let app_ctx = self.app_context.clone();
        let table_id = TableId::new(statement.namespace_id.clone(), statement.table_name.clone());

        run_blocking(move || {
            let schema_registry = app_ctx.schema_registry();
            let existing_def = schema_registry
                .get_table_if_exists(&table_id)
                .into_kalamdb_error("Failed to check table existence")?;

            let Some(existing_def) = existing_def else {
                return Ok::<Option<String>, KalamDbError>(None);
            };

            if schema_registry.get_provider(&table_id).is_none() {
                log::info!("Table {} exists but provider missing - registering now", table_id);
                schema_registry.put((*existing_def).clone())?;
            }

            Ok(Some(format!("Table {} already exists (IF NOT EXISTS)", table_id)))
        })
        .await
    }

    fn resolve_table_type(
        statement: &CreateTableStatement,
        context: &ExecutionContext,
    ) -> TableType {
        use kalamdb_session::can_downgrade_shared_to_user;

        if statement.table_type == TableType::Shared
            && can_downgrade_shared_to_user(context.user_role())
            && statement.namespace_id.as_str() == context.user_id().as_str()
        {
            TableType::User
        } else {
            statement.table_type
        }
    }
}

impl TypedStatementHandler<CreateTableStatement> for CreateTableHandler {
    async fn execute(
        &self,
        statement: CreateTableStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        use crate::helpers::{audit, table_creation};

        let mut statement = statement;
        let effective_type = Self::resolve_table_type(&statement, context);
        if effective_type != statement.table_type {
            log::debug!(
                "Inferring USER table type for {}.{} issued by {} (role {:?})",
                statement.namespace_id.as_str(),
                statement.table_name.as_str(),
                context.user_id().as_str(),
                context.user_role()
            );
            statement.table_type = effective_type;
        }

        // Capture details for audit log
        let namespace_id = statement.namespace_id.clone();
        let table_name = statement.table_name.clone();
        let table_type = statement.table_type;
        let table_id = TableId::new(namespace_id.clone(), table_name.clone());

        if let Some(message) = self.maybe_existing_if_not_exists_message(&statement).await? {
            return Ok(ExecutionResult::Success { message });
        }

        // Build TableDefinition (validate + build, no execution yet)
        // Offload sync RocksDB reads (namespace existence, storage lookup) to blocking thread
        let app_ctx = self.app_context.clone();
        let stmt_clone = statement.clone();
        let user_id = context.user_id().clone();
        let user_role = context.user_role();
        let table_def = run_blocking(move || {
            table_creation::build_table_definition(app_ctx, &stmt_clone, &user_id, user_role)
        })
        .await?;

        let implicit_def = table_def.clone();

        // Delegate to unified applier - pass raw parameters
        let message = self
            .app_context
            .applier()
            .create_table(table_id.clone(), table_type, table_def)
            .await
            .map_err(|e| KalamDbError::ExecutionError(format!("CREATE TABLE failed: {}", e)))?;

        // Normalize success message to keep tests stable and surface raft path
        let message = format!("CREATE TABLE via Raft consensus: {}", message);

        // Log DDL operation
        let audit_entry = audit::log_ddl_operation(
            context,
            "CREATE",
            "TABLE",
            &table_id.full_name(),
            Some(format!("Type: {}", table_type)),
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        let implicit_table_id = table_id.clone();
        let app_for_type = Arc::clone(&self.app_context);
        if let Err(error) = run_blocking(move || {
            crate::catalog_type::ensure_implicit_row_type(
                &app_for_type.system_tables().catalog_stores(),
                &implicit_table_id,
                &implicit_def,
            )
        })
        .await
        {
            log::error!(
                "failed to catalog implicit row type for {}: {error}",
                table_id.full_name()
            );
        }

        Ok(ExecutionResult::Success { message })
    }

    async fn check_authorization(
        &self,
        statement: &CreateTableStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use crate::helpers::guards::block_anonymous_write;

        // T050: Block anonymous users from DDL operations
        block_anonymous_write(context, "CREATE TABLE")?;

        // Authorization check is handled inside table_creation helpers
        // (they call can_create_table for each table type)
        // This allows unified error messages
        use kalamdb_session::can_create_table;

        let effective_type = Self::resolve_table_type(statement, context);

        if !can_create_table(context.user_role(), effective_type) {
            return Err(KalamDbError::Unauthorized(format!(
                "Insufficient privileges to create {} tables",
                effective_type
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use kalamdb_commons::{
        models::{NamespaceId, UserId},
        schemas::TableType,
        Role,
    };
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_system::Storage;

    use super::*;

    fn create_test_context(role: Role) -> ExecutionContext {
        ExecutionContext::new(UserId::new("test_user"), role, create_test_session_simple())
    }

    fn ensure_default_storage() {
        let app_ctx = test_app_context_simple();
        let storages_provider = app_ctx.system_tables().storages();
        let storage_id = kalamdb_commons::models::StorageId::from("local");

        // Check if "local" storage exists, create if not
        if storages_provider.get_storage_by_id(&storage_id).unwrap().is_none() {
            let storage = Storage {
                storage_id:             storage_id.clone(),
                storage_name:           "Local Storage".to_string(),
                description:            Some("Default local storage".to_string()),
                storage_type:
                    kalamdb_system::providers::storages::models::StorageType::Filesystem,
                base_directory:         "/tmp/kalamdb_test".to_string(),
                credentials:            None,
                config_json:            None,
                shared_tables_template: "shared/{namespace}/{table}".to_string(),
                user_tables_template:   "user/{namespace}/{table}/{userId}".to_string(),
                created_at:             chrono::Utc::now().timestamp_millis(),
                updated_at:             chrono::Utc::now().timestamp_millis(),
            };
            storages_provider.create_storage(storage).unwrap();
        }
    }

    fn create_test_statement(table_type: TableType) -> CreateTableStatement {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("age", DataType::Int32, true)),
        ]));

        CreateTableStatement {
            namespace_id: NamespaceId::default(),
            table_name: format!("test_table_{}", chrono::Utc::now().timestamp_millis()).into(),
            table_type,
            schema,
            column_defaults: std::collections::HashMap::new(),
            primary_key_column: Some("id".to_string()),
            storage_id: None,
            use_user_storage: false,
            flush_policy: None,
            deleted_retention_hours: None,
            ttl_seconds: if table_type == TableType::Stream {
                Some(3600)
            } else {
                None
            },
            compression: None,
            eviction_strategy: None,
            max_stream_size_bytes: None,
            if_not_exists: false,
        }
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists_existing_table_skips_audit() {
        let app_ctx = test_app_context_simple();
        let namespaces_provider = app_ctx.system_tables().namespaces();
        let namespace_id = NamespaceId::default();
        if namespaces_provider.get_namespace(&namespace_id).unwrap().is_none() {
            let namespace = kalamdb_system::Namespace {
                namespace_id: namespace_id.clone(),
                name:         "default".to_string(),
                created_at:   chrono::Utc::now().timestamp_millis(),
                options:      None,
                table_count:  0,
            };
            namespaces_provider.create_namespace(namespace).unwrap();
        }

        let handler = CreateTableHandler::new(app_ctx.clone());
        let ctx = create_test_context(Role::Dba);
        let mut stmt = create_test_statement(TableType::User);
        stmt.table_name = format!("test_ine_{}", chrono::Utc::now().timestamp_millis()).into();
        stmt.if_not_exists = true;
        let table_id = TableId::new(stmt.namespace_id.clone(), stmt.table_name.clone());

        let table_def = crate::helpers::table_creation::build_table_definition(
            app_ctx.clone(),
            &stmt,
            ctx.user_id(),
            ctx.user_role(),
        )
        .unwrap();
        app_ctx.schema_registry().register_table(table_def).unwrap();

        let before_audit_count = app_ctx.system_tables().audit_logs().scan_all().unwrap().len();
        let result = handler.execute(stmt, vec![], &ctx).await;
        let after_audit_count = app_ctx.system_tables().audit_logs().scan_all().unwrap().len();

        assert!(result.is_ok(), "CREATE TABLE IF NOT EXISTS failed: {:?}", result);
        if let Ok(ExecutionResult::Success { message }) = result {
            assert_eq!(message, format!("Table {} already exists (IF NOT EXISTS)", table_id));
        }
        assert_eq!(after_audit_count, before_audit_count);
    }

    #[tokio::test]
    async fn test_create_table_authorization_user() {
        let app_ctx = test_app_context_simple();
        let handler = CreateTableHandler::new(app_ctx);
        let stmt = create_test_statement(TableType::User);

        // User role CANNOT create tables (DML only)
        let user_ctx = create_test_context(Role::User);
        let result = handler.check_authorization(&stmt, &user_ctx).await;
        assert!(result.is_err(), "User role should NOT be able to create USER tables");

        // User role CANNOT create SHARED tables
        let stmt_shared = create_test_statement(TableType::Shared);
        let result = handler.check_authorization(&stmt_shared, &user_ctx).await;
        assert!(result.is_err(), "User role should NOT be able to create SHARED tables");

        // Dba role CAN create USER tables
        let dba_ctx = create_test_context(Role::Dba);
        let result = handler.check_authorization(&stmt, &dba_ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_table_authorization_shared_denied() {
        let app_ctx = test_app_context_simple();
        let handler = CreateTableHandler::new(app_ctx);
        let stmt = create_test_statement(TableType::Shared);

        // User role CANNOT create SHARED tables
        let user_ctx = create_test_context(Role::User);
        let result = handler.check_authorization(&stmt, &user_ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_table_authorization_stream_dba() {
        let app_ctx = test_app_context_simple();
        let handler = CreateTableHandler::new(app_ctx);
        let stmt = create_test_statement(TableType::Stream);

        // DBA role can create STREAM tables
        let dba_ctx = create_test_context(Role::Dba);
        let result = handler.check_authorization(&stmt, &dba_ctx).await;
        assert!(result.is_ok());
    }

    #[ignore = "Requires Raft for CREATE TABLE"]
    #[tokio::test]
    async fn test_create_user_table_success() {
        let app_ctx = test_app_context_simple();

        // Ensure default storage and namespace exist
        ensure_default_storage();
        let namespaces_provider = app_ctx.system_tables().namespaces();
        let namespace_id = NamespaceId::default();
        if namespaces_provider.get_namespace(&namespace_id).unwrap().is_none() {
            let namespace = kalamdb_system::Namespace {
                namespace_id: namespace_id.clone(),
                name:         "default".to_string(),
                created_at:   chrono::Utc::now().timestamp_millis(),
                options:      None,
                table_count:  0,
            };
            namespaces_provider.create_namespace(namespace).unwrap();
        }

        let handler = CreateTableHandler::new(app_ctx);
        let stmt = create_test_statement(TableType::User);
        let ctx = create_test_context(Role::Dba);

        let result = handler.execute(stmt, vec![], &ctx).await;

        assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
        if let Ok(ExecutionResult::Success { message }) = result {
            let msg = message.to_lowercase();
            assert!(msg.contains("create table"));
            assert!(msg.contains("raft consensus"));
        }
    }

    #[ignore = "Requires Raft for CREATE TABLE"]
    #[tokio::test]
    async fn test_create_table_if_not_exists() {
        let app_ctx = test_app_context_simple();

        // Ensure default storage and namespace exist
        ensure_default_storage();
        let namespaces_provider = app_ctx.system_tables().namespaces();
        let namespace_id = NamespaceId::default();
        if namespaces_provider.get_namespace(&namespace_id).unwrap().is_none() {
            let namespace = kalamdb_system::Namespace {
                namespace_id: namespace_id.clone(),
                name:         "default".to_string(),
                created_at:   chrono::Utc::now().timestamp_millis(),
                options:      None,
                table_count:  0,
            };
            namespaces_provider.create_namespace(namespace).unwrap();
        }

        let handler = CreateTableHandler::new(app_ctx);
        let table_name = format!("test_ine_{}", chrono::Utc::now().timestamp_millis());

        let mut stmt = create_test_statement(TableType::User);
        stmt.table_name = table_name.clone().into();
        let ctx = create_test_context(Role::Dba);

        // First creation should succeed
        let result1 = handler.execute(stmt.clone(), vec![], &ctx).await;
        assert!(result1.is_ok());

        // Second creation without IF NOT EXISTS should fail
        let result2 = handler.execute(stmt.clone(), vec![], &ctx).await;
        assert!(result2.is_err());

        // Third creation with IF NOT EXISTS should succeed with message
        let mut stmt_ine = stmt.clone();
        stmt_ine.if_not_exists = true;
        let result3 = handler.execute(stmt_ine, vec![], &ctx).await;
        assert!(result3.is_ok());
        if let Ok(ExecutionResult::Success { message }) = result3 {
            let msg = message.to_lowercase();
            assert!(msg.contains("already exists"));
            assert!(msg.contains("if not exists"));
        }
    }
}
