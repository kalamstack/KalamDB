//! SQL and Data Tests
//!
//! Tests covering:
//! - DML operations (INSERT, UPDATE, DELETE)
//! - Data type preservation (moved to kalamdb-core/kalamdb-commons tests)
//! - DateTime and timezone storage (moved to kalamdb-commons tests)
//! - Edge cases
//! - Index efficiency
//! - Row count behavior
//! - Shared table access
//! - Stream TTL eviction
//! - Version resolution

// Re-export test_support from crate root
pub(super) use crate::test_support;

// SQL & Data Tests
mod test_combined_data_integrity;
mod test_datafusion_commands;
mod test_dml_complex;
mod test_edge_cases;
mod test_explain_index_usage;
mod test_pk_index_efficiency;
mod test_row_count_behavior;
mod test_shared_access;
mod test_sql_error_redaction;
mod test_system_table_index_usage;
mod test_update_delete_version_resolution;
