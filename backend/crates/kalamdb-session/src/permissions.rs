//! Centralized Permission Checking
//!
//! This module provides all permission checking logic for table access.
//! Used by both the SQL executor (plan-level checks) and SecuredSystemTableProvider (scan-level checks).

use crate::error::SessionError;
use crate::user_context::UserContext as SessionUserContext;
use datafusion::catalog::Session;
use datafusion::execution::context::SessionState;
use kalamdb_commons::models::{NamespaceId, ReadContext, Role, TableId, TableName, UserId};
use kalamdb_commons::schemas::{TableDefinition, TableOptions, TableType};
use kalamdb_commons::TableAccess;

/// Check if a role can access system tables.
///
/// # Access Rules
/// - **System role**: Full access
/// - **Dba role**: Full access
/// - **Service role**: DENIED
/// - **User role**: DENIED
#[inline]
pub fn can_access_system_table(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba)
}

/// Check if a role can access a given table type.
///
/// # Access Rules
/// - **System role**: can access ALL table types
/// - **Dba role**: can access ALL table types
/// - **Service role**: can access USER/SHARED/STREAM tables
/// - **User role**: can access USER/STREAM tables
#[inline]
pub fn can_access_table_type(role: Role, table_type: TableType) -> bool {
    match role {
        Role::System | Role::Dba => true,
        Role::Service => {
            matches!(table_type, TableType::Shared | TableType::Stream | TableType::User)
        },
        Role::User => matches!(table_type, TableType::User | TableType::Stream),
        Role::Anonymous => false,
    }
}

/// Check if a role can create a table of the given type.
#[inline]
pub fn can_create_table(role: Role, table_type: TableType) -> bool {
    match role {
        Role::System => true,
        Role::Dba => matches!(table_type, TableType::User | TableType::Shared | TableType::Stream),
        Role::Service => {
            matches!(table_type, TableType::User | TableType::Shared | TableType::Stream)
        },
        Role::User => matches!(table_type, TableType::User | TableType::Stream),
        Role::Anonymous => false,
    }
}

/// Check if a role can delete a table.
#[inline]
pub fn can_delete_table(role: Role, table_type: TableType, _is_owner: bool) -> bool {
    match role {
        Role::System => true,
        Role::Dba => !matches!(table_type, TableType::System),
        Role::Service | Role::User => false,
        Role::Anonymous => false,
    }
}

/// Check if a role can alter a table.
///
/// - System/Dba can alter any table.
/// - Service can alter Shared/User/Stream tables.
/// - User can alter User/Stream tables.
#[inline]
pub fn can_alter_table(role: Role, table_type: TableType, _is_owner: bool) -> bool {
    match role {
        Role::System | Role::Dba => true,
        Role::Service => {
            matches!(table_type, TableType::Shared | TableType::User | TableType::Stream)
        },
        Role::User => matches!(table_type, TableType::User | TableType::Stream),
        Role::Anonymous => false,
    }
}

/// Check if a role can manage users.
#[inline]
pub fn can_manage_users(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba)
}

/// Check if a role can create views.
#[inline]
pub fn can_create_view(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba)
}

/// Check if a role has admin privileges.
#[inline]
pub fn is_admin_role(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba)
}

/// Check if a role is system.
#[inline]
pub fn is_system_role(role: Role) -> bool {
    matches!(role, Role::System)
}

/// Helper for CREATE TABLE: allow USER/SERVICE to downgrade shared to user table.
#[inline]
pub fn can_downgrade_shared_to_user(role: Role) -> bool {
    matches!(role, Role::User | Role::Service)
}

/// Extract the full SessionUserContext from a DataFusion session.
///
/// # Returns
/// * `Ok(&SessionUserContext)` - The session context
/// * `Err(SessionError::SessionContextNotFound)` - Context not in session
pub fn extract_session_context(session: &dyn Session) -> Result<&SessionUserContext, SessionError> {
    session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or(SessionError::InvalidSessionState("Expected SessionState".to_string()))?
        .config()
        .options()
        .extensions
        .get::<SessionUserContext>()
        .ok_or(SessionError::SessionContextNotFound)
}

/// Extract user role from DataFusion session.
///
/// Falls back to Role::User (least privileged) if context is not found.
/// This is the fail-closed behavior for security.
pub fn extract_user_role(session: &dyn Session) -> Role {
    extract_session_context(session).map(|ctx| ctx.role).unwrap_or(Role::User) // Fail closed: default to least privileged
}

/// Extract user ID from DataFusion session.
///
/// Falls back to "anonymous" if context is not found.
pub fn extract_user_id(session: &dyn Session) -> UserId {
    extract_session_context(session)
        .map(|ctx| ctx.user_id.clone())
        .unwrap_or_else(|_| UserId::anonymous())
}

/// Extract (user_id, role) from DataFusion session.
pub fn extract_user_context(session: &dyn Session) -> Result<(&UserId, Role), SessionError> {
    let ctx = extract_session_context(session)?;
    Ok((&ctx.user_id, ctx.role))
}

/// Extract full session context (user_id, role, read_context) from DataFusion session.
pub fn extract_full_user_context(
    session: &dyn Session,
) -> Result<(&UserId, Role, ReadContext), SessionError> {
    let ctx = extract_session_context(session)?;
    Ok((&ctx.user_id, ctx.role, ctx.read_context))
}

/// Check if the current session can access a system table.
///
/// This is the primary security check for system table providers.
/// Call this at the start of `TableProvider::scan()` to enforce
/// defense-in-depth permission checking.
///
/// # Arguments
/// * `session` - DataFusion session reference
/// * `table_id` - The TableId (namespace + table name)
///
/// # Returns
/// * `Ok(())` if access is allowed
/// * `Err(SessionError::AccessDenied)` if access is denied
pub fn check_system_table_access(
    session: &dyn Session,
    table_id: &TableId,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);

    if can_access_system_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            role,
            reason: "System tables require 'dba' or 'system' role.".to_string(),
        })
    }
}

/// Check if a role can access a user table.
///
/// # Access Rules
/// - **System/Dba**: Full access
/// - **Service/User**: Allowed (row-level security restricts data visibility)
#[inline]
pub fn can_access_user_table(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba | Role::Service | Role::User)
}

/// Check if the current session can access a user table.
///
/// User tables rely on row-level security in the provider to scope data by user_id.
pub fn check_user_table_access(
    session: &dyn Session,
    table_id: &TableId,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);
    if can_access_user_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            role,
            reason: "User tables require user/service or admin role".to_string(),
        })
    }
}

/// Check if a role can read all user rows (RLS bypass).
#[inline]
pub fn can_read_all_users(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba | Role::Service)
}

/// Check if a role can execute DML statements.
#[inline]
pub fn can_execute_dml(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba | Role::Service | Role::User)
}

/// Check if a role can execute maintenance operations (flush/compact).
#[inline]
pub fn can_execute_maintenance(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba | Role::Service)
}

/// Check if a role can write (INSERT/UPDATE/DELETE) a user/stream table.
#[inline]
pub fn can_write_user_table(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba | Role::Service | Role::User)
}

/// Check if the current session can write to a user table.
pub fn check_user_table_write_access(
    session: &dyn Session,
    table_id: &TableId,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);
    if can_write_user_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            role,
            reason: "User table write denied due to insufficient privileges.".to_string(),
        })
    }
}

/// Check user table write access using table identity.
pub fn check_user_table_write_access_level(
    role: Role,
    namespace_id: &NamespaceId,
    table_name: &TableName,
) -> Result<(), SessionError> {
    if can_write_user_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: namespace_id.clone(),
            table_name: table_name.clone(),
            role,
            reason: "User table write denied due to insufficient privileges.".to_string(),
        })
    }
}

/// Stream tables use the same permissions as user tables.
#[inline]
pub fn can_write_stream_table(role: Role) -> bool {
    can_write_user_table(role)
}

/// Check stream table write access using table identity.
pub fn check_stream_table_write_access_level(
    role: Role,
    namespace_id: &NamespaceId,
    table_name: &TableName,
) -> Result<(), SessionError> {
    check_user_table_write_access_level(role, namespace_id, table_name)
}

/// Check if a role can impersonate another user (EXECUTE AS USER).
#[inline]
pub fn can_impersonate_user(role: Role) -> bool {
    matches!(role, Role::Service | Role::Dba | Role::System)
}

/// Check whether `actor_role` is allowed to impersonate `target_role`.
///
/// Policy:
/// - System can impersonate any role.
/// - Dba can impersonate Service/User/Anonymous.
/// - Service can impersonate User/Anonymous.
/// - User/Anonymous cannot impersonate.
#[inline]
pub fn can_impersonate_role(actor_role: Role, target_role: Role) -> bool {
    match actor_role {
        Role::System => true,
        Role::Dba => matches!(target_role, Role::Service | Role::User | Role::Anonymous),
        Role::Service => matches!(target_role, Role::User | Role::Anonymous),
        Role::User | Role::Anonymous => false,
    }
}

/// Determine the access level for a shared table definition.
#[inline]
pub fn shared_table_access_level(def: &TableDefinition) -> TableAccess {
    match &def.table_options {
        TableOptions::Shared(opts) => opts.access_level.unwrap_or(TableAccess::Private),
        _ => TableAccess::Private,
    }
}

/// Check if a role can access (read) a shared table.
#[inline]
pub fn can_access_shared_table(access_level: TableAccess, role: Role) -> bool {
    match access_level {
        TableAccess::Dba => matches!(role, Role::System | Role::Dba),
        TableAccess::Restricted => matches!(role, Role::System | Role::Dba | Role::Service),
        TableAccess::Private => matches!(role, Role::System | Role::Dba | Role::Service),
        TableAccess::Public => true, // All authenticated users can read public tables
    }
}

/// Check if a role can write (INSERT/UPDATE/DELETE) a shared table.
#[inline]
pub fn can_write_shared_table(access_level: TableAccess, role: Role) -> bool {
    match access_level {
        TableAccess::Dba => matches!(role, Role::System | Role::Dba),
        TableAccess::Restricted => matches!(role, Role::System | Role::Dba | Role::Service),
        TableAccess::Private => matches!(role, Role::System | Role::Dba | Role::Service),
        TableAccess::Public => matches!(role, Role::System | Role::Dba | Role::Service),
    }
}

/// Check if the current session can access a shared table.
///
/// Uses TableDefinition to read access level and applies RBAC rules.
pub fn check_shared_table_access(
    session: &dyn Session,
    table_def: &TableDefinition,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);
    //let user_id = extract_user_id(session);
    let access_level = shared_table_access_level(table_def);

    if can_access_shared_table(access_level, role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_def.namespace_id.clone(),
            table_name: table_def.table_name.clone(),
            role,
            reason: format!("Shared table access denied (access_level={:?})", access_level),
        })
    }
}

/// Check if the current session can write to a shared table.
///
/// Uses TableDefinition to read access level and applies RBAC rules.
pub fn check_shared_table_write_access(
    session: &dyn Session,
    table_def: &TableDefinition,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);
    let access_level = shared_table_access_level(table_def);

    if can_write_shared_table(access_level, role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_def.namespace_id.clone(),
            table_name: table_def.table_name.clone(),
            role,
            reason: format!("Shared table write denied (access_level={:?})", access_level),
        })
    }
}

/// Check shared table write access using table identity and access level.
pub fn check_shared_table_write_access_level(
    role: Role,
    access_level: TableAccess,
    namespace_id: &NamespaceId,
    table_name: &TableName,
) -> Result<(), SessionError> {
    if can_write_shared_table(access_level, role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: namespace_id.clone(),
            table_name: table_name.clone(),
            role,
            reason: format!("Shared table write denied (access_level={:?})", access_level),
        })
    }
}

/// Check system table access using namespace and table name directly.
///
/// Convenience function when you don't have a TableId constructed.
pub fn check_system_table_access_by_name(
    session: &dyn Session,
    namespace_id: &NamespaceId,
    table_name: &TableName,
) -> Result<(), SessionError> {
    let table_id = TableId::new(namespace_id.clone(), table_name.clone());
    check_system_table_access(session, &table_id)
}

/// Centralized permission checker for use across the codebase.
///
/// Provides both session-based and role-based permission checks.
pub struct PermissionChecker;

impl PermissionChecker {
    /// Check if the current session can access a system table.
    ///
    /// Returns a DataFusion error on access denial for easy integration.
    pub fn check_system_table(
        session: &dyn Session,
        table_id: &TableId,
    ) -> datafusion::error::Result<()> {
        check_system_table_access(session, table_id).map_err(|e| e.into())
    }

    /// Check if the current session can access a user table.
    pub fn check_user_table(
        session: &dyn Session,
        table_id: &TableId,
    ) -> datafusion::error::Result<()> {
        check_user_table_access(session, table_id).map_err(|e| e.into())
    }

    /// Check if the current session can access a shared table.
    pub fn check_shared_table(
        session: &dyn Session,
        table_def: &TableDefinition,
    ) -> datafusion::error::Result<()> {
        check_shared_table_access(session, table_def).map_err(|e| e.into())
    }

    /// Check if the current session can write to a shared table.
    pub fn check_shared_table_write(
        session: &dyn Session,
        table_def: &TableDefinition,
    ) -> datafusion::error::Result<()> {
        check_shared_table_write_access(session, table_def).map_err(|e| e.into())
    }

    /// Check if the current session can write to a user table.
    pub fn check_user_table_write(
        session: &dyn Session,
        table_id: &TableId,
    ) -> datafusion::error::Result<()> {
        check_user_table_write_access(session, table_id).map_err(|e| e.into())
    }

    /// Check if the current session has admin privileges (System or Dba role).
    #[inline]
    pub fn is_admin(session: &dyn Session) -> bool {
        let role = extract_user_role(session);
        matches!(role, Role::System | Role::Dba)
    }

    /// Get the role from session without performing any check.
    #[inline]
    pub fn get_role(session: &dyn Session) -> Role {
        extract_user_role(session)
    }

    /// Get the user ID from session.
    #[inline]
    pub fn get_user_id(session: &dyn Session) -> UserId {
        extract_user_id(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_access_system_table() {
        // Admins can access
        assert!(can_access_system_table(Role::System));
        assert!(can_access_system_table(Role::Dba));

        // Non-admins cannot access
        assert!(!can_access_system_table(Role::Service));
        assert!(!can_access_system_table(Role::User));
    }

    #[test]
    fn test_can_impersonate_role() {
        assert!(can_impersonate_role(Role::System, Role::System));
        assert!(can_impersonate_role(Role::System, Role::Dba));
        assert!(can_impersonate_role(Role::System, Role::Service));
        assert!(can_impersonate_role(Role::System, Role::User));

        assert!(!can_impersonate_role(Role::Dba, Role::System));
        assert!(!can_impersonate_role(Role::Dba, Role::Dba));
        assert!(can_impersonate_role(Role::Dba, Role::Service));
        assert!(can_impersonate_role(Role::Dba, Role::User));

        assert!(!can_impersonate_role(Role::Service, Role::System));
        assert!(!can_impersonate_role(Role::Service, Role::Dba));
        assert!(!can_impersonate_role(Role::Service, Role::Service));
        assert!(can_impersonate_role(Role::Service, Role::User));

        assert!(!can_impersonate_role(Role::User, Role::User));
        assert!(!can_impersonate_role(Role::Anonymous, Role::User));
    }

    #[test]
    fn test_session_error_display() {
        let err = SessionError::AccessDenied {
            namespace_id: NamespaceId::system(),
            table_name: TableName::new("users"),
            role: Role::User,
            reason: "Test reason".to_string(),
        };
        let display = format!("{}", err);
        assert!(display.contains("Access denied"));
        assert!(display.contains("system.users"));
        assert!(display.contains("User"));
    }

    #[test]
    fn test_dba_shared_access_level_is_dba_only() {
        assert!(can_access_shared_table(TableAccess::Dba, Role::Dba));
        assert!(can_access_shared_table(TableAccess::Dba, Role::System));
        assert!(!can_access_shared_table(TableAccess::Dba, Role::Service));
        assert!(!can_access_shared_table(TableAccess::Dba, Role::User));

        assert!(can_write_shared_table(TableAccess::Dba, Role::Dba));
        assert!(can_write_shared_table(TableAccess::Dba, Role::System));
        assert!(!can_write_shared_table(TableAccess::Dba, Role::Service));
    }

    #[test]
    fn test_public_shared_write_access_allows_admin_and_service_only() {
        assert!(can_write_shared_table(TableAccess::Public, Role::System));
        assert!(can_write_shared_table(TableAccess::Public, Role::Dba));
        assert!(can_write_shared_table(TableAccess::Public, Role::Service));
        assert!(!can_write_shared_table(TableAccess::Public, Role::User));
    }
}
