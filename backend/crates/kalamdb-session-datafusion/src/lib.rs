//! # kalamdb-session-datafusion
//!
//! DataFusion-bound session adapters for KalamDB.
//!
//! This crate keeps query-engine-specific session extensions and secured provider
//! wrappers out of `kalamdb-session` so request identity stays lightweight for
//! auth and transport paths.

pub mod context;
pub mod permissions;
pub mod secured_provider;

pub use context::SessionUserContext;
pub use permissions::{
    check_shared_table_access, check_shared_table_write_access, check_system_table_access,
    check_system_table_access_by_name, check_user_table_access, check_user_table_write_access,
    extract_full_user_context, extract_session_context, extract_user_context, extract_user_id,
    extract_user_role, session_error_to_datafusion, PermissionChecker,
};
pub use secured_provider::{secure_provider, SecuredSystemTableProvider};
