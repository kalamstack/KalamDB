//! Schema Registry module for KalamDB Core
//!
//! Provides unified caching and metadata management for table schemas.
//! All schema-related functionality has been consolidated here from the former kalamdb-registry crate.

pub mod cached_table_data;
pub mod projection;
pub mod registry;

pub use cached_table_data::{CachedProvider, CachedTableData, TableEntry};
pub use kalamdb_commons::helpers::string_interner::SystemColumns;
pub use kalamdb_views::error::RegistryError;

pub use kalamdb_system::SystemColumnsService;
pub use projection::{project_batch, schemas_compatible};
pub use registry::SchemaRegistry;
pub use registry::TablesSchemaRegistryAdapter;

// Re-export common types from kalamdb_commons for convenience
pub use kalamdb_commons::models::{NamespaceId, TableName, UserId};
pub use kalamdb_commons::schemas::TableType;
