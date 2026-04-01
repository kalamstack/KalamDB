//! Namespace DDL Handlers
//!
//! Handlers for CREATE NAMESPACE and DROP NAMESPACE statements.

use datafusion::execution::context::SessionContext;
use kalamdb_commons::models::NamespaceId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult};
use kalamdb_sql::ddl::{CreateNamespaceStatement, DropNamespaceStatement};
use kalamdb_system::Namespace;
use std::sync::Arc;

/// Execute CREATE NAMESPACE statement
///
/// Creates a new namespace in the system. Namespaces provide logical isolation
/// for tables and other database objects.
///
/// # Arguments
/// * `app_context` - Application context with system table providers
/// * `_session` - DataFusion session context (reserved for future use)
/// * `sql` - Raw SQL statement
/// * `_exec_ctx` - Execution context with user information
///
/// # Returns
/// Success message indicating namespace creation status
///
/// # Example SQL
/// ```sql
/// CREATE NAMESPACE production;
/// CREATE NAMESPACE IF NOT EXISTS staging;
/// ```
pub async fn execute_create_namespace(
    app_context: &Arc<AppContext>,
    _session: &SessionContext,
    sql: &str,
    _exec_ctx: &ExecutionContext,
) -> Result<ExecutionResult, KalamDbError> {
    let namespaces_provider = app_context.system_tables().namespaces();

    // Parse CREATE NAMESPACE statement
    let stmt = CreateNamespaceStatement::parse(sql).map_err(|e| {
        KalamDbError::InvalidSql(format!("Failed to parse CREATE NAMESPACE: {}", e))
    })?;

    let name = stmt.name.as_str();

    // Validate namespace name
    kalamdb_sql::validation::validate_namespace_name(name).map_err(|e| e.to_string())?;

    // Check if namespace already exists
    let namespace_id = NamespaceId::new(name);
    let existing = namespaces_provider.get_namespace(&namespace_id)?;

    if existing.is_some() {
        if stmt.if_not_exists {
            let message = format!("Namespace '{}' already exists", name);
            return Ok(ExecutionResult::Success { message });
        } else {
            return Err(KalamDbError::AlreadyExists(format!(
                "Namespace '{}' already exists",
                name
            )));
        }
    }

    // Create namespace entity
    let namespace = Namespace::new(name);

    // Insert namespace via provider
    namespaces_provider.create_namespace(namespace)?;

    let message = format!("Namespace '{}' created successfully", name);
    Ok(ExecutionResult::Success { message })
}

/// Execute DROP NAMESPACE statement
///
/// Drops a namespace from the system. Prevents dropping namespaces that contain tables.
///
/// # Arguments
/// * `app_context` - Application context with system table providers
/// * `_session` - DataFusion session context (reserved for future use)
/// * `sql` - Raw SQL statement
/// * `_exec_ctx` - Execution context with user information
///
/// # Returns
/// Success message indicating namespace deletion status
///
/// # Example SQL
/// ```sql
/// DROP NAMESPACE production;
/// DROP NAMESPACE IF EXISTS staging;
/// ```
pub async fn execute_drop_namespace(
    app_context: &Arc<AppContext>,
    _session: &SessionContext,
    sql: &str,
    _exec_ctx: &ExecutionContext,
) -> Result<ExecutionResult, KalamDbError> {
    let namespaces_provider = app_context.system_tables().namespaces();

    // Parse DROP NAMESPACE statement
    let stmt = DropNamespaceStatement::parse(sql)?;

    let name = stmt.name.as_str();
    let namespace_id = NamespaceId::new(name);

    // Check if namespace exists
    let namespace = match namespaces_provider.get_namespace(&namespace_id)? {
        Some(ns) => ns,
        None => {
            if stmt.if_exists {
                let message = format!("Namespace '{}' does not exist", name);
                return Ok(ExecutionResult::Success { message });
            } else {
                return Err(KalamDbError::NotFound(format!("Namespace '{}' not found", name)));
            }
        },
    };

    // Check if namespace has tables
    if !namespace.can_delete() {
        return Err(KalamDbError::InvalidOperation(format!(
            "Cannot drop namespace '{}': namespace contains {} table(s). Drop all tables first.",
            name, namespace.table_count
        )));
    }

    // Delete namespace via provider
    namespaces_provider.delete_namespace(&namespace_id)?;

    let message = format!("Namespace '{}' dropped successfully", name);
    Ok(ExecutionResult::Success { message })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use std::sync::Arc;

    fn init_app_context() -> Arc<AppContext> {
        test_app_context_simple()
    }

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::from("test_user"), Role::Dba, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_create_namespace_success() {
        let app_ctx = init_app_context();
        let session = SessionContext::new();
        let ctx = test_context();

        let sql = "CREATE NAMESPACE test_ns_mod";
        let result = execute_create_namespace(&app_ctx, &session, sql, &ctx).await;

        assert!(result.is_ok());
        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("test_ns_mod"));
                assert!(message.contains("created successfully"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_create_namespace_if_not_exists() {
        let app_ctx = init_app_context();
        let session = SessionContext::new();
        let ctx = test_context();

        let sql = "CREATE NAMESPACE IF NOT EXISTS test_ns_ine_mod";

        // First creation
        let result1 = execute_create_namespace(&app_ctx, &session, sql, &ctx).await;
        assert!(result1.is_ok());

        // Second creation with IF NOT EXISTS
        let result2 = execute_create_namespace(&app_ctx, &session, sql, &ctx).await;
        assert!(result2.is_ok());

        match result2.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("already exists"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_drop_namespace_success() {
        let app_ctx = init_app_context();
        let session = SessionContext::new();
        let ctx = test_context();

        // Create namespace first
        let create_sql = "CREATE NAMESPACE test_drop_ns_mod";
        execute_create_namespace(&app_ctx, &session, create_sql, &ctx).await.unwrap();

        // Drop it
        let drop_sql = "DROP NAMESPACE test_drop_ns_mod";
        let result = execute_drop_namespace(&app_ctx, &session, drop_sql, &ctx).await;

        assert!(result.is_ok());
        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("dropped successfully"));
            },
            _ => panic!("Expected Success result"),
        }
    }
}
