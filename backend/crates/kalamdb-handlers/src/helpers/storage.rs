//! Storage DDL Handlers
//!
//! Handlers for CREATE STORAGE statements.

use datafusion::execution::context::SessionContext;
use kalamdb_commons::models::StorageId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult};
use kalamdb_filestore::StorageHealthService;
use kalamdb_sql::CreateStorageStatement;
use kalamdb_system::StorageType;
use std::sync::Arc;

/// Execute CREATE STORAGE statement
///
/// Registers a new storage backend (filesystem, S3, etc.) for storing table data.
/// Storage backends define where and how table data is physically stored.
///
/// # Arguments
/// * `app_context` - Application context with system table providers and storage registry
/// * `_session` - DataFusion session context (reserved for future use)
/// * `sql` - Raw SQL statement
/// * `_exec_ctx` - Execution context with user information
///
/// # Returns
/// Success message indicating storage registration status
///
/// # Example SQL
/// ```sql
/// CREATE STORAGE local_fs
///   TYPE 'filesystem'
///   NAME 'Local Filesystem'
///   DESCRIPTION 'Local development storage'
///   BASE_DIRECTORY '/var/lib/kalamdb/data'
///   SHARED_TABLES_TEMPLATE '{namespace}/{tableName}/'
///   USER_TABLES_TEMPLATE '{namespace}/{tableName}/{userId}/';
///
/// CREATE STORAGE s3_prod
///   TYPE 's3'
///   NAME 'Production S3 Storage'
///   BASE_DIRECTORY 's3://my-bucket/kalamdb/'
///   SHARED_TABLES_TEMPLATE '{namespace}/{tableName}/'
///   USER_TABLES_TEMPLATE '{namespace}/{tableName}/{userId}/';
/// ```
pub async fn execute_create_storage(
    app_context: &Arc<AppContext>,
    _session: &SessionContext,
    sql: &str,
    _exec_ctx: &ExecutionContext,
) -> Result<ExecutionResult, KalamDbError> {
    let storages_provider = app_context.system_tables().storages();
    let storage_registry = app_context.storage_registry();

    // Parse CREATE STORAGE statement
    let stmt = CreateStorageStatement::parse(sql).map_err(|e| {
        KalamDbError::InvalidOperation(format!("CREATE STORAGE parse error: {}", e))
    })?;

    // Check if storage already exists
    let storage_id = StorageId::from(stmt.storage_id.as_str());
    if storages_provider
        .get_storage_by_id(&storage_id)
        .into_kalamdb_error("Failed to check storage")?
        .is_some()
    {
        return Err(KalamDbError::InvalidOperation(format!(
            "Storage '{}' already exists",
            stmt.storage_id
        )));
    }

    // Validate templates using StorageRegistry
    if !stmt.shared_tables_template.is_empty() {
        storage_registry.validate_template(&stmt.shared_tables_template, false)?;
    }
    if !stmt.user_tables_template.is_empty() {
        storage_registry.validate_template(&stmt.user_tables_template, true)?;
    }

    // SECURITY: Validate base directory for path traversal attacks
    validate_storage_path(&stmt.base_directory)?;

    // Ensure filesystem storages eagerly create their base directory
    if stmt.storage_type == StorageType::Filesystem {
        ensure_filesystem_directory(&stmt.base_directory)?;
    }

    // Validate credentials JSON (if provided)
    let normalized_credentials = if let Some(raw) = stmt.credentials.as_ref() {
        let value: serde_json::Value =
            serde_json::from_str(raw).into_invalid_operation("Invalid credentials JSON")?;

        if !value.is_object() {
            return Err(KalamDbError::InvalidOperation(
                "Credentials must be a JSON object".to_string(),
            ));
        }

        Some(
            serde_json::to_string(&value)
                .into_invalid_operation("Failed to normalize credentials JSON")?,
        )
    } else {
        None
    };

    // Create storage record
    let storage = kalamdb_system::Storage {
        storage_id: stmt.storage_id.clone(),
        storage_name: stmt.storage_name,
        description: stmt.description,
        storage_type: stmt.storage_type,
        base_directory: stmt.base_directory,
        credentials: normalized_credentials,
        config_json: stmt.config_json,
        shared_tables_template: stmt.shared_tables_template,
        user_tables_template: stmt.user_tables_template,
        created_at: chrono::Utc::now().timestamp_millis(),
        updated_at: chrono::Utc::now().timestamp_millis(),
    };

    let connectivity = StorageHealthService::test_connectivity(&storage)
        .await
        .into_kalamdb_error("Storage connectivity check failed")?;

    if !connectivity.connected {
        let error = connectivity.error.unwrap_or_else(|| "Unknown connectivity error".to_string());
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

    // Insert into system.storages
    storages_provider
        .insert_storage(storage)
        .into_kalamdb_error("Failed to create storage")?;

    Ok(ExecutionResult::Success {
        message: format!("Storage '{}' created successfully", stmt.storage_id),
    })
}

/// Validate storage path for security issues.
///
/// Rejects paths that could lead to path traversal or access to sensitive directories.
fn validate_storage_path(path: &str) -> Result<(), KalamDbError> {
    // Skip validation for S3/cloud paths
    if path.starts_with("s3://") || path.starts_with("gs://") || path.starts_with("az://") {
        return Ok(());
    }

    // Check for path traversal sequences
    if path.contains("..") {
        return Err(KalamDbError::InvalidOperation(
            "Storage path cannot contain '..' (path traversal not allowed)".to_string(),
        ));
    }

    // Check for null bytes
    if path.contains('\0') {
        return Err(KalamDbError::InvalidOperation(
            "Storage path cannot contain null bytes".to_string(),
        ));
    }

    // Block sensitive directories
    let path_lower = path.to_lowercase();
    let sensitive_prefixes = [
        "/etc/",
        "/root/",
        "/var/log/",
        "/proc/",
        "/sys/",
        "c:\\windows",
        "c:/windows",
    ];
    for prefix in &sensitive_prefixes {
        if path_lower.starts_with(prefix) {
            return Err(KalamDbError::InvalidOperation(format!(
                "Storage path cannot be in sensitive directory: {}",
                prefix
            )));
        }
    }

    Ok(())
}

/// Ensure a filesystem directory exists (used by CREATE STORAGE handlers)
pub fn ensure_filesystem_directory(path: &str) -> Result<(), KalamDbError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(KalamDbError::InvalidOperation(
            "Filesystem storage requires a non-empty base directory".to_string(),
        ));
    }

    let dir = std::path::Path::new(trimmed);
    std::fs::create_dir_all(dir)
        .into_kalamdb_error(&format!("Failed to create storage directory '{}'", dir.display()))?;
    Ok(())
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
    async fn test_create_storage_success() {
        let app_ctx = init_app_context();
        let session = SessionContext::new();
        let ctx = test_context();

        let sql = r#"
            CREATE STORAGE test_storage
              TYPE 'filesystem'
              NAME 'Test Storage'
              DESCRIPTION 'Test storage for unit tests'
              BASE_DIRECTORY '/tmp/kalamdb/test_storage'
              SHARED_TABLES_TEMPLATE '{namespace}/{tableName}/'
              USER_TABLES_TEMPLATE '{namespace}/{tableName}/{userId}/'
        "#;

        let result = execute_create_storage(&app_ctx, &session, sql, &ctx).await;

        assert!(result.is_ok());
        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("test_storage"));
                assert!(message.contains("created successfully"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_create_storage_duplicate() {
        let app_ctx = init_app_context();
        let session = SessionContext::new();
        let ctx = test_context();

        let sql = r#"
            CREATE STORAGE test_storage_dup
              TYPE 'filesystem'
              NAME 'Duplicate Storage'
              BASE_DIRECTORY '/tmp/kalamdb/dup'
              SHARED_TABLES_TEMPLATE '{namespace}/{tableName}/'
              USER_TABLES_TEMPLATE '{namespace}/{tableName}/{userId}/'
        "#;

        // First creation
        let result1 = execute_create_storage(&app_ctx, &session, sql, &ctx).await;
        assert!(result1.is_ok());

        // Second creation should fail
        let result2 = execute_create_storage(&app_ctx, &session, sql, &ctx).await;
        assert!(result2.is_err());
        match result2.unwrap_err() {
            KalamDbError::InvalidOperation(msg) => {
                assert!(msg.contains("already exists"));
            },
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[tokio::test]
    async fn test_create_storage_invalid_type() {
        let app_ctx = init_app_context();
        let session = SessionContext::new();
        let ctx = test_context();

        let sql = r#"
            CREATE STORAGE test_storage_bad
              TYPE 'invalid_type'
              NAME 'Bad Storage'
              BASE_DIRECTORY '/tmp/kalamdb/bad'
              SHARED_TABLES_TEMPLATE '{namespace}/{tableName}/'
              USER_TABLES_TEMPLATE '{namespace}/{tableName}/{userId}/'
        "#;

        let result = execute_create_storage(&app_ctx, &session, sql, &ctx).await;
        assert!(result.is_err());
    }
}
