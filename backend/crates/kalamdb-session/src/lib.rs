//! # kalamdb-session
//!
//! Session context, permissions, and secured table provider abstraction for KalamDB.
//!
//! This crate provides:
//! - [`UserContext`]: User identity passed through DataFusion sessions
//! - [`permissions`]: Centralized permission checking for table access
//! - [`SecuredSystemTableProvider`]: Wrapper that enforces permissions on system tables
//!
//! ## Security Philosophy
//!
//! - **Defense in Depth**: Permissions are checked at multiple layers:
//!   1. SQL Classifier (statement-level authorization)
//!   2. TableProvider.scan() (final security gate via SecuredSystemTableProvider)
//!
//! - **Fail Closed**: When in doubt, deny access. Default to least privileged role.
//!
//! ## Architecture
//!
//! ```text
//! HTTP Handler → ExecutionContext → SessionState.extensions → SecuredSystemTableProvider.scan()
//!                                                                      ↓
//!                                                              Permission Check
//!                                                                      ↓
//!                                                              Inner Provider.scan()
//! ```
//!
//! ## Permission Check Locations
//!
//! | Category | Location | Description |
//! |----------|----------|-------------|
//! | Table Type Access | `permissions` | System/User/Shared table access |
//! | Statement Auth | `kalamdb_sql::classifier` | DDL/DML statement-level auth |
//! | User Management | `handlers::user` | Role changes, password updates |
//! | RBAC Functions | `kalamdb_session::permissions` | Role-based access helpers |

pub mod auth_session;
pub mod error;
pub mod permissions;
pub mod secured_provider;
pub mod user_context;

// Re-export main types
pub use auth_session::{AuthMethod, AuthSession};
pub use error::{SessionError, SessionResult};
pub use permissions::{
    can_access_shared_table, can_access_system_table, can_access_table_type, can_access_user_table,
    can_alter_table, can_create_table, can_create_view, can_delete_table,
    can_downgrade_shared_to_user, can_execute_dml, can_execute_maintenance, can_impersonate_role,
    can_impersonate_user, can_manage_users, can_read_all_users, can_write_shared_table,
    can_write_stream_table, can_write_user_table, check_shared_table_access,
    check_shared_table_write_access, check_shared_table_write_access_level,
    check_stream_table_write_access_level, check_system_table_access, check_user_table_access,
    check_user_table_write_access, check_user_table_write_access_level, extract_full_user_context,
    extract_session_context, extract_user_context, extract_user_id, extract_user_role,
    is_admin_role, is_system_role, shared_table_access_level, PermissionChecker,
};
pub use secured_provider::{secure_provider, SecuredSystemTableProvider};
pub use user_context::UserContext;

/// Type alias for `UserContext` used in session extensions.
pub type SessionUserContext = UserContext;
