//! System table providers
//!
//! This module contains all system table provider implementations.
//! Each provider implements the DataFusion TableProvider trait.
//!
//! **Architecture**:
//! - Model types with `#[table]` derive are the source of truth for `TableDefinition`
//! - `*TableProvider` structs implement DataFusion's `TableProvider` trait
//! - Providers memoize Arrow schemas with local `OnceLock` caches
//! - `base` module contains common traits for unified scan logic

pub mod audit_logs;
pub mod base;
pub mod job_nodes;
pub mod jobs;
pub mod live;
pub mod manifest;
pub mod namespaces;
pub mod storages;
pub mod tables;
pub mod topic_offsets;
pub mod topics;
pub mod users;

// Re-export base traits
pub use base::{
    extract_filter_value, extract_range_filters, SimpleSystemTableScan, SystemTableScan,
};

// Re-export all providers
pub use audit_logs::AuditLogEntry;
pub use audit_logs::AuditLogsTableProvider;
pub use job_nodes::JobNodesTableProvider;
pub use jobs::JobsTableProvider;
pub use manifest::{InMemoryChecker, ManifestTableProvider};
pub use namespaces::NamespacesTableProvider;
pub use storages::StoragesTableProvider;
pub use tables::SchemasTableProvider;
pub use topic_offsets::TopicOffsetsTableProvider;
pub use topics::TopicsTableProvider;
pub use users::UsersTableProvider;

pub use manifest::manifest_table_definition;
pub use tables::schemas_table_definition;
