//! # kalamdb-session
//!
//! Session context and pure authorization helpers for KalamDB.
//!
//! This crate provides:
//! - [`AuthSession`]: Lightweight authenticated request/session identity
//! - [`UserContext`]: Shared user identity and read-routing metadata
//! - [`permissions`]: Pure role and table-policy helpers
//!
//! ## Security Philosophy
//!
//! - **Fail Closed**: When in doubt, deny access. Default to least privileged role.
//! - **Lightweight by Default**: DataFusion-specific session extensions live in
//!   `kalamdb-session-datafusion` so auth and API paths do not pull query-engine deps.
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
pub mod user_context;

// Re-export main types
pub use auth_session::{AuthMethod, AuthSession};
pub use error::{SessionError, SessionResult};
pub use permissions::{
    can_access_shared_table, can_access_system_table, can_access_table_type, can_access_user_table,
    can_alter_table, can_create_table, can_create_view, can_delete_table,
    can_downgrade_shared_to_user, can_execute_dml, can_execute_maintenance, can_impersonate_role,
    can_impersonate_user, can_manage_users, can_read_all_users, can_write_shared_table,
    can_write_stream_table, can_write_user_table, check_shared_table_write_access_level,
    check_stream_table_write_access_level, check_user_table_write_access_level, is_admin_role,
    is_system_role, shared_table_access_level,
};
pub use user_context::UserContext;
