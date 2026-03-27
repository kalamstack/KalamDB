//! Schema Models - Single Source of Truth for Table Definitions
//!
//! This module provides the consolidated schema system for KalamDB, implementing
//! **Phase 3 (Schema Consolidation)** from the 008-schema-consolidation feature.
//!
//! # Architecture
//!
//! The schema system uses a layered approach:
//!
//! ```text
//! User Input (SQL)
//!       ↓
//! TableDefinition (kalamdb-commons)
//!       ↓
//! Arrow Schema (Apache Arrow)
//!       ↓
//! DataFusion Execution (kalamdb-sql)
//!       ↓
//! RocksDB Storage (kalamdb-store)
//! ```
//!
//! # Core Types
//!
//! - **`TableDefinition`**: Complete table schema (columns, options, versioning)
//! - **`ColumnDefinition`**: Individual column schema (name, type, constraints)
//! - **`SchemaVersion`**: Schema evolution tracking (version, changes, timestamp)
//! - **`TableOptions`**: Type-specific settings (User, Shared, Stream, System)
//! - **`TableType`**: Table category (User, Shared, Stream, System)
//! - **`ColumnDefault`**: Default value specification (literal, function call)
//!
//! # Key Features
//!
//! - **Type Safety**: Strongly-typed schemas prevent runtime errors
//! - **Bidirectional Conversion**: `TableDefinition ↔ Arrow Schema`
//! - **Schema Versioning**: Track all schema changes with timestamps
//! - **RocksDB Persistence**: Efficient binary serialization
//! - **Caching**: DashMap-based lock-free schema cache in kalamdb-core
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use kalamdb_commons::schemas::{TableDefinition, ColumnDefinition, TableType, TableOptions};
//! use kalamdb_commons::datatypes::KalamDataType;
//!
//! // Define a table schema
//! let table_def = TableDefinition {
//!     namespace_id: "my_namespace".into(),
//!     table_name: "users".into(),
//!     table_type: TableType::User,
//!     columns: vec![
//!         ColumnDefinition {
//!             name: "id".into(),
//!             data_type: KalamDataType::Uuid,
//!             is_nullable: false,
//!             is_primary_key: true,
//!             ..Default::default()
//!         },
//!         ColumnDefinition {
//!             name: "email".into(),
//!             data_type: KalamDataType::Utf8,
//!             is_nullable: false,
//!             ..Default::default()
//!         },
//!     ],
//!     options: TableOptions::User(Default::default()),
//!     ..Default::default()
//! };
//!
//! // Convert to Arrow Schema for DataFusion execution
//! let arrow_schema = table_def.to_arrow_schema();
//! ```
//!
//! # Related Modules
//!
//! - `kalamdb_commons::datatypes` - Unified type system (KalamDataType)
//! - `kalamdb_core::schema_cache` - Lock-free caching layer
//! - `kalamdb_sql::ddl` - SQL DDL statement handling
//! - `kalamdb_store::entities` - RocksDB storage implementation

pub mod column_default;
pub mod column_definition;
pub mod field_flag;
pub mod policy;
#[cfg(feature = "schema-metadata")]
pub mod schema_field;
#[cfg(feature = "table-definition")]
pub mod schema_version;
pub mod table_access;
#[cfg(feature = "table-definition")]
pub mod table_definition;
pub mod table_name;
pub mod table_options;
pub mod table_type;

pub use column_default::ColumnDefault;
pub use column_definition::ColumnDefinition;
pub use field_flag::{FieldFlag, FieldFlags};
#[cfg(feature = "schema-metadata")]
pub use schema_field::SchemaField;
#[cfg(feature = "table-definition")]
pub use schema_version::SchemaVersion;
pub use table_access::TableAccess;
#[cfg(feature = "table-definition")]
pub use table_definition::TableDefinition;
pub use table_name::TableName;
pub use table_options::{
    SharedTableOptions, StreamTableOptions, SystemTableOptions, TableOptions, UserTableOptions,
};
pub use table_type::TableType;
