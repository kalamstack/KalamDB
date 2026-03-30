//! Audit Logging Utilities
//!
//! Helper functions for creating and managing audit log entries.
//! **Phase 2 Task T018**: Centralized audit logging for SQL operations.

use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::ExecutionContext;
use chrono::Utc;
use kalamdb_commons::models::AuditLogId;
use kalamdb_commons::{UserId, UserName};
use kalamdb_system::AuditLogEntry;

/// Create an audit log entry for a SQL operation
///
/// # Arguments
/// * `context` - Execution context with user and audit information
/// * `action` - Action performed (e.g., "CREATE_TABLE", "DROP_USER")
/// * `target` - Target of the action (e.g., "default.users", "user:alice")
/// * `details` - Optional JSON details about the operation
/// * `subject_user_id` - Optional subject for impersonation (None for direct operations)
///
/// # Returns
/// * `AuditLogEntry` - Populated audit log entry
///
/// # Example
/// ```ignore
/// use kalamdb_core::sql::executor::handlers::audit::create_audit_entry;
///
/// let entry = create_audit_entry(
///     &exec_ctx,
///     "CREATE_TABLE",
///     "default.users",
///     Some(r#"{"columns": 5, "table_type": "USER"}"#.to_string()),
///     None, // No impersonation
/// );
/// ```
pub fn create_audit_entry(
    context: &ExecutionContext,
    action: &str,
    target: &str,
    details: Option<String>,
    subject_user_id: Option<kalamdb_commons::UserId>,
) -> AuditLogEntry {
    // Generate a unique audit ID (timestamp + random suffix to prevent collisions)
    let timestamp = Utc::now().timestamp_millis();
    let uuid = uuid::Uuid::new_v4().simple();
    let audit_id = AuditLogId::from(format!("audit_{}_{}", timestamp, uuid));

    AuditLogEntry {
        audit_id,
        timestamp,
        actor_user_id: context.user_id().clone(),
        actor_username: kalamdb_commons::UserName::from(context.user_id().as_str()),
        action: action.to_string(),
        target: target.to_string(),
        details,
        ip_address: context.ip_address().map(|s| s.to_string()),
        subject_user_id,
    }
}

/// Create audit entry for DDL operations
///
/// Shorthand for common DDL actions.
///
/// # Arguments
/// * `context` - Execution context
/// * `operation` - DDL operation (CREATE, ALTER, DROP)
/// * `object_type` - Object type (TABLE, NAMESPACE, STORAGE, USER)
/// * `object_name` - Fully qualified object name
/// * `details` - Optional operation details
/// * `subject_user_id` - Optional subject for impersonation
pub fn log_ddl_operation(
    context: &ExecutionContext,
    operation: &str,
    object_type: &str,
    object_name: &str,
    details: Option<String>,
    subject_user_id: Option<kalamdb_commons::UserId>,
) -> AuditLogEntry {
    let action = format!("{}_{}", operation, object_type);
    create_audit_entry(context, &action, object_name, details, subject_user_id)
}

/// Create audit entry for query operations
///
/// Logs SELECT and other read operations.
///
/// # Arguments
/// * `context` - Execution context
/// * `query_type` - Query type (SELECT, DESCRIBE, SHOW)
/// * `target` - Query target
/// * `took` - Execution time in milliseconds
/// * `subject_user_id` - Optional subject for impersonation
pub fn log_query_operation(
    context: &ExecutionContext,
    query_type: &str,
    target: &str,
    took: f64,
    subject_user_id: Option<kalamdb_commons::UserId>,
) -> AuditLogEntry {
    let details = serde_json::json!({
        "took": took,
    })
    .to_string();

    create_audit_entry(context, query_type, target, Some(details), subject_user_id)
}

/// Create audit entry for authentication events
///
/// Logs login, logout, password changes, etc.
///
/// # Arguments
/// * `user_id` - User ID
/// * `action` - Authentication action (LOGIN, LOGOUT, PASSWORD_CHANGE, etc.)
/// * `success` - Whether the action succeeded
/// * `ip_address` - Optional IP address
pub fn log_auth_event(
    user_id: &kalamdb_commons::UserId,
    action: &str,
    success: bool,
    ip_address: Option<String>,
) -> AuditLogEntry {
    let timestamp = Utc::now().timestamp_millis();
    let audit_id = AuditLogId::from(format!("audit_{}", timestamp));

    let details = serde_json::json!({
        "success": success,
    })
    .to_string();

    AuditLogEntry {
        audit_id,
        timestamp,
        actor_user_id: user_id.clone(),
        actor_username: kalamdb_commons::UserName::from(user_id.as_str()),
        action: action.to_string(),
        target: format!("user:{}", user_id.as_str()),
        details: Some(details),
        ip_address,
        subject_user_id: None, // Authentication events don't involve impersonation
    }
}

/// Create audit entry for authentication events when only username is known.
/// Uses anonymous actor_user_id and preserves attempted username for security audits.
pub fn log_auth_event_with_username(
    username: &UserName,
    action: &str,
    success: bool,
    ip_address: Option<String>,
) -> AuditLogEntry {
    let timestamp = Utc::now().timestamp_millis();
    let audit_id = AuditLogId::from(format!("audit_{}", timestamp));

    let details = serde_json::json!({
        "success": success,
    })
    .to_string();

    AuditLogEntry {
        audit_id,
        timestamp,
        actor_user_id: UserId::anonymous(),
        actor_username: username.clone(),
        action: action.to_string(),
        target: format!("username:{}", username.as_str()),
        details: Some(details),
        ip_address,
        subject_user_id: None,
    }
}

use kalamdb_core::app_context::AppContext;
use std::sync::Arc;

/// Persist an audit entry to the system.audit_logs table
///
/// Delegates to provider's async method which handles spawn_blocking internally.
pub async fn persist_audit_entry(
    app_context: &Arc<AppContext>,
    entry: &AuditLogEntry,
) -> Result<(), KalamDbError> {
    app_context
        .system_tables()
        .audit_logs()
        .append_async(entry.clone())
        .await
        .map_err(KalamDbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use kalamdb_commons::{Role, UserId};
    use std::sync::Arc;

    fn test_session() -> Arc<SessionContext> {
        Arc::new(SessionContext::new())
    }

    #[test]
    fn test_create_audit_entry() {
        let ctx = ExecutionContext::with_audit_info(
            UserId::from("alice"),
            Role::User,
            Some("req-123".to_string()),
            Some("192.168.1.1".to_string()),
            test_session(),
        );

        let entry = create_audit_entry(
            &ctx,
            "CREATE_TABLE",
            "default.users",
            Some(r#"{"columns": 5}"#.to_string()),
            None,
        );

        assert_eq!(entry.action, "CREATE_TABLE");
        assert_eq!(entry.target, "default.users");
        assert_eq!(entry.actor_user_id.as_str(), "alice");
        assert_eq!(entry.ip_address, Some("192.168.1.1".to_string()));
        assert!(entry.details.is_some());
        assert_eq!(entry.subject_user_id, None);
    }

    #[test]
    fn test_log_ddl_operation() {
        let ctx = ExecutionContext::new(UserId::from("bob"), Role::Dba, test_session());

        let entry = log_ddl_operation(
            &ctx,
            "CREATE",
            "TABLE",
            "prod.events",
            Some(r#"{"table_type": "STREAM"}"#.to_string()),
            None,
        );

        assert_eq!(entry.action, "CREATE_TABLE");
        assert_eq!(entry.target, "prod.events");
        assert_eq!(entry.actor_user_id.as_str(), "bob");
    }

    #[test]
    fn test_log_query_operation() {
        let ctx = ExecutionContext::new(UserId::from("dave"), Role::User, test_session());

        let entry = log_query_operation(&ctx, "SELECT", "default.users", 150.0, None);

        assert_eq!(entry.action, "SELECT");
        assert_eq!(entry.target, "default.users");
        assert!(entry.details.unwrap().contains("150"));
    }

    #[test]
    fn test_log_auth_event() {
        let user_id = UserId::from("eve");

        let entry = log_auth_event(&user_id, "LOGIN", true, Some("10.0.0.1".to_string()));

        assert_eq!(entry.action, "LOGIN");
        assert_eq!(entry.target, "user:eve");
        assert_eq!(entry.ip_address, Some("10.0.0.1".to_string()));
        assert!(entry.details.unwrap().contains("true"));
    }
}
