use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::Schema;
use kalamdb_commons::{
    models::{NamespaceId, StorageId, TableAccess, TableName},
    schemas::{policy::FlushPolicy, ColumnDefault, TableType},
};

/// Unified CREATE TABLE statement that works for USER, SHARED, and STREAM tables
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    /// Table name (without namespace prefix)
    pub table_name: TableName,
    /// Namespace ID
    pub namespace_id: NamespaceId,
    /// Table type (User, Shared, or Stream)
    pub table_type: TableType,
    /// Arrow schema for the table
    pub schema: Arc<Schema>,
    /// Column default values (column_name -> default_spec)
    pub column_defaults: HashMap<String, ColumnDefault>,
    /// PRIMARY KEY column name (if detected)
    pub primary_key_column: Option<String>,
    /// Storage ID - References system.storages (defaults to 'local')
    pub storage_id: Option<StorageId>,
    /// Use user-specific storage - Allow per-user storage override (USER tables only)
    pub use_user_storage: bool,
    /// Flush policy
    pub flush_policy: Option<FlushPolicy>,
    /// Deleted row retention in hours
    pub deleted_retention_hours: Option<u32>,
    /// TTL for stream tables in seconds (STREAM tables only)
    pub ttl_seconds: Option<u64>,
    /// If true, don't error if table already exists
    pub if_not_exists: bool,
    /// Access level for SHARED tables (public, private, restricted)
    /// Defaults to private. Only applicable to SHARED tables.
    pub access_level: Option<TableAccess>,
}
