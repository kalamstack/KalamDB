//! DDL Guard Helpers
//!
//! Common authorization and validation guards for DDL operations.
//! These helpers consolidate repeated validation patterns across handlers.

use kalamdb_commons::models::NamespaceId;
use kalamdb_core::{error::KalamDbError, sql::context::ExecutionContext};

/// Block modifications (ALTER, DROP, CREATE) on system namespaces.
///
/// System namespaces (`system`, `information_schema`, `pg_catalog`, `datafusion`)
/// are managed internally and cannot be modified by users.
///
/// # Arguments
/// * `namespace_id` - The namespace to check
/// * `operation` - The operation being attempted (e.g., "ALTER", "DROP", "CREATE")
/// * `object_type` - The type of object (e.g., "TABLE", "NAMESPACE")
/// * `object_name` - Optional object name for error messages
///
/// # Returns
/// * `Ok(())` if the namespace is NOT a system namespace
/// * `Err(KalamDbError::InvalidOperation)` if modification is blocked
///
/// # Example
/// ```ignore
/// block_system_namespace_modification(&namespace_id, "ALTER", "TABLE", Some(&table_name))?;
/// ```
pub fn block_system_namespace_modification(
    namespace_id: &NamespaceId,
    operation: &str,
    object_type: &str,
    object_name: Option<&str>,
) -> Result<(), KalamDbError> {
    if namespace_id.is_system_namespace() {
        let full_name = match object_name {
            Some(name) => format!("'{}.{}'", namespace_id.as_str(), name),
            None => format!("'{}'", namespace_id.as_str()),
        };

        log::warn!(
            "❌ {} {} blocked: Cannot modify system {} {}",
            operation,
            object_type,
            object_type.to_lowercase(),
            full_name
        );

        return Err(KalamDbError::InvalidOperation(format!(
            "Cannot {} system {} {}. System {}s are managed internally.",
            operation.to_lowercase(),
            object_type.to_lowercase(),
            full_name,
            object_type.to_lowercase()
        )));
    }
    Ok(())
}

/// Require admin privileges (DBA or System role) for an operation.
///
/// # Arguments
/// * `context` - The execution context containing user role information
/// * `action` - Description of the action (e.g., "create storage", "drop namespace")
///
/// # Returns
/// * `Ok(())` if user has admin privileges
/// * `Err(KalamDbError::Unauthorized)` if user lacks privileges
///
/// # Example
/// ```ignore
/// require_admin(context, "create storage")?;
/// ```
pub fn require_admin(context: &ExecutionContext, action: &str) -> Result<(), KalamDbError> {
    use kalamdb_session::is_admin_role;
    if !is_admin_role(context.user_role()) {
        log::error!(
            "❌ {}: Insufficient privileges (user: {}, role: {:?})",
            action,
            context.user_id().as_str(),
            context.user_role()
        );
        return Err(KalamDbError::Unauthorized(format!(
            "Insufficient privileges to {}. DBA or System role required.",
            action
        )));
    }
    Ok(())
}

/// Block write operations (CREATE, ALTER, DROP, INSERT, UPDATE, DELETE) for anonymous users.
///
/// Anonymous users can only SELECT from public tables.
/// Any attempt to modify data will result in an Unauthorized error.
///
/// # Arguments
/// * `context` - The execution context containing user information
/// * `operation` - The operation being attempted (e.g., "CREATE TABLE", "INSERT")
///
/// # Returns
/// * `Ok(())` if user is authenticated (not anonymous)
/// * `Err(KalamDbError::Unauthorized)` if user is anonymous
///
/// # Example
/// ```ignore
/// block_anonymous_write(context, "CREATE TABLE")?;
/// ```
pub fn block_anonymous_write(
    context: &ExecutionContext,
    operation: &str,
) -> Result<(), KalamDbError> {
    if context.is_anonymous() {
        log::warn!("❌ {} blocked: Anonymous users cannot perform write operations", operation);
        return Err(KalamDbError::Unauthorized(
            "Anonymous users can only SELECT from public tables. Please authenticate to perform \
             write operations."
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kalamdb_commons::{models::UserId, Role};
    use kalamdb_session::AuthSession;

    use super::*;

    fn create_context(role: Role) -> ExecutionContext {
        ExecutionContext::new(
            UserId::from("test_user"),
            role,
            Arc::new(datafusion::prelude::SessionContext::new()),
        )
    }

    #[test]
    fn test_block_system_namespace_modification() {
        // System namespaces should be blocked
        let system_ns = NamespaceId::system();
        let result =
            block_system_namespace_modification(&system_ns, "ALTER", "TABLE", Some("users"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot alter"));

        let info_schema = NamespaceId::from("information_schema");
        let result = block_system_namespace_modification(&info_schema, "DROP", "TABLE", None);
        assert!(result.is_err());

        // User namespaces should pass
        let user_ns = NamespaceId::from("my_namespace");
        let result = block_system_namespace_modification(&user_ns, "ALTER", "TABLE", Some("data"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_require_admin() {
        // Admin roles should pass
        let system_ctx = create_context(Role::System);
        assert!(require_admin(&system_ctx, "create storage").is_ok());

        let dba_ctx = create_context(Role::Dba);
        assert!(require_admin(&dba_ctx, "drop namespace").is_ok());

        // Non-admin roles should fail
        let user_ctx = create_context(Role::User);
        let result = require_admin(&user_ctx, "create storage");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Insufficient privileges"));

        let service_ctx = create_context(Role::Service);
        assert!(require_admin(&service_ctx, "drop namespace").is_err());
    }

    fn create_anonymous_context() -> ExecutionContext {
        ExecutionContext::from_session(
            AuthSession::anonymous(),
            Arc::new(datafusion::prelude::SessionContext::new()),
        )
    }

    #[test]
    fn test_block_anonymous_write() {
        // Authenticated users should pass
        let user_ctx = create_context(Role::User);
        assert!(block_anonymous_write(&user_ctx, "CREATE TABLE").is_ok());

        let service_ctx = create_context(Role::Service);
        assert!(block_anonymous_write(&service_ctx, "INSERT").is_ok());

        let dba_ctx = create_context(Role::Dba);
        assert!(block_anonymous_write(&dba_ctx, "UPDATE").is_ok());

        // Anonymous users should be blocked
        let anon_ctx = create_anonymous_context();
        let result = block_anonymous_write(&anon_ctx, "CREATE TABLE");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Anonymous users"));

        let result = block_anonymous_write(&anon_ctx, "INSERT");
        assert!(result.is_err());

        let result = block_anonymous_write(&anon_ctx, "DELETE");
        assert!(result.is_err());
    }
}
