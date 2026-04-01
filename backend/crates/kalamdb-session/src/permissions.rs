//! Centralized Permission Checking
//!
//! This module provides pure role- and table-definition-based permission helpers.
//! DataFusion session extraction and provider wrappers live in
//! `kalamdb-session-datafusion`.

use crate::error::SessionError;
use kalamdb_commons::models::{NamespaceId, Role, TableName};
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

/// Check if a role can access a user table.
///
/// # Access Rules
/// - **System/Dba**: Full access
/// - **Service/User**: Allowed (row-level security restricts data visibility)
#[inline]
pub fn can_access_user_table(role: Role) -> bool {
    matches!(role, Role::System | Role::Dba | Role::Service | Role::User)
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
