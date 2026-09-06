//! System table providers
//!
//! This module contains all system table provider implementations.
//! Each provider implements the DataFusion TableProvider trait.
//!
//! **Architecture**:
//! - Model types with `#[table]` derive are the source of truth for `TableDefinition`
//! - `*TableProvider` structs implement DataFusion's `TableProvider` trait
//! - Providers memoize Arrow schemas with local `OnceLock` caches
//! - `base` centralizes deferred scan execution so planning stays lightweight and provider families
//!   share one filter/projection execution model

pub mod audit_logs;
pub mod base;
pub mod catalog;
pub mod job_nodes;
pub mod jobs;
pub mod live;
pub mod manifest;
pub mod migrations;
pub mod namespaces;
pub mod storages;
pub mod table_policies;
pub mod tables;
pub mod topic_offsets;
pub mod topics;
pub mod users;

// Re-export base traits
// Re-export all providers
pub use audit_logs::{AuditLogEntry, AuditLogsTableProvider};
pub use base::{
    extract_filter_value, extract_range_filters, SimpleSystemTableScan, SystemTableScan,
};
pub use catalog::{
    ActivateFunctionOutcome, CatalogFunctionArtifact, CatalogFunctionModule,
    CatalogFunctionRevision, CatalogRoutine, CatalogRoutineGrant, CatalogRoutineParameter,
    CatalogStores, CatalogTrigger, CatalogTriggerAttempt, CatalogType, CatalogTypeField,
    FunctionArtifactsTableProvider, FunctionModulesTableProvider, FunctionRevisionsTableProvider,
    RoutineGrantsTableProvider, RoutineParametersTableProvider, RoutinesTableProvider,
    TriggerAttemptsTableProvider, TriggersTableProvider, TypeFieldsTableProvider,
    TypesTableProvider,
};
pub use job_nodes::JobNodesTableProvider;
pub use jobs::JobsTableProvider;
pub use manifest::{manifest_table_definition, InMemoryChecker, ManifestTableProvider};
pub use migrations::MigrationsTableProvider;
pub use namespaces::NamespacesTableProvider;
pub use storages::StoragesTableProvider;
pub use table_policies::TablePoliciesTableProvider;
pub use tables::{schemas_table_definition, SchemasTableProvider};
pub use topic_offsets::TopicOffsetsTableProvider;
pub use topics::TopicsTableProvider;
pub use users::UsersTableProvider;
