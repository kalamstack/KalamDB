//! System Tables and Configuration Tests
//!
//! Tests covering:
//! - Audit logging
//! - Configuration access
//! - Live queries metadata
//! - System users
//! - System user initialization

// Re-export test_support from crate root
pub(super) use crate::test_support;

// Test helpers for ignored tests
mod test_helpers;

// System Tests
mod test_audit_logging;
mod test_dba_init;
mod test_runtime_metrics;
// NOTE: test_config_access requires kalamdb-core's internal test infrastructure
// and is marked #[ignore]. It should be run via kalamdb-core's test suite instead.
// mod test_config_access;
mod test_live_queries_metadata;
mod test_system_user_init;
mod test_system_users;
