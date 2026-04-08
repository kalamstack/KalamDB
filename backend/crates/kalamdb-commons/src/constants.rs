//! System-wide constants for KalamDB.
//!
//! This module centralizes constant definitions used across all crates, including:
//! - Column family names (non-system)
//! - Reserved identifiers

/// RocksDB column family names.
///
/// Provides centralized naming for all column families used in KalamDB storage.
pub struct ColumnFamilyNames;

#[allow(non_upper_case_globals)]
impl ColumnFamilyNames {
    // Default column family (RocksDB built-in)
    // pub const DEFAULT: &'static str = "default";
    /// Unified information_schema tables (replaces system_table_schemas + system_columns)
    pub const INFORMATION_SCHEMA_TABLES: &'static str = "information_schema_tables";

    /// Prefix for user table column families (appended with table name)
    pub const USER_TABLE_PREFIX: &'static str = "user_";

    /// Prefix for shared table column families (appended with table name)
    pub const SHARED_TABLE_PREFIX: &'static str = "shared_";

    /// Prefix for stream table column families (appended with table name)
    pub const STREAM_TABLE_PREFIX: &'static str = "stream_";
}

/// System column names added automatically to all tables.
pub struct SystemColumnNames;

#[allow(non_upper_case_globals)]
impl SystemColumnNames {
    // REMOVED: _updated column (timestamp is embedded in _seq Snowflake ID)
    // Use _seq >> 22 to extract timestamp in milliseconds
    // pub const UPDATED: &'static str = "_updated";

    /// Soft delete flag (true = deleted)
    pub const DELETED: &'static str = "_deleted";

    /// Sequence column used for MVCC versioning
    pub const SEQ: &'static str = "_seq";

    /// Commit-order marker used for snapshot visibility on committed rows.
    pub const COMMIT_SEQ: &'static str = "_commit_seq";

    /// Check if a column name is a system column
    pub fn is_system_column(column_name: &str) -> bool {
        matches!(column_name, Self::DELETED | Self::SEQ | Self::COMMIT_SEQ)
    }
}

// /// Global instance of system column names.
// pub const SYSTEM_COLUMNS: SystemColumnNames = SystemColumnNames;

// /// Reserved namespace name for system tables.
pub const SYSTEM_NAMESPACE: &str = "system";

// /// Default namespace name for user tables when not specified.
// pub const DEFAULT_NAMESPACE: &str = "default";

/// Maximum SQL query length in bytes (1MB)
///
/// Prevents DoS attacks via extremely long SQL strings that could
/// cause excessive memory usage or parsing time.
/// Most legitimate queries are under 10KB.
pub const MAX_SQL_QUERY_LENGTH: usize = 1024 * 1024; // 1MB

/// Authentication-related constants.
pub struct AuthConstants;

#[allow(non_upper_case_globals)]
impl AuthConstants {
    /// Default system user username created on first database initialization
    pub const DEFAULT_SYSTEM_USERNAME: &'static str = "root";

    /// Default system user ID created on first database initialization
    pub const DEFAULT_ROOT_USER_ID: &'static str = "root";

    /// Default system user id
    pub const DEFAULT_SYSTEM_USER_ID: &'static str = SYSTEM_NAMESPACE;

    /// Anonymous user ID constant (matches anonymous AuthSession execution)
    pub const ANONYMOUS_USER_ID: &str = "anonymous";
}

/// Global instance of authentication constants.
pub const AUTH: AuthConstants = AuthConstants;

/// Reserved namespace names that cannot be used by users.
///
/// These names are reserved for system use and will be rejected during
/// namespace creation. The check is case-insensitive.
///
/// ## Reserved Names
/// - `system`: System tables namespace
/// - `sys`: Common system alias
/// - `root`: Root/admin namespace
/// - `kalamdb`/`kalam`: KalamDB internal namespaces
/// - `main`/`default`: Default namespace aliases
/// - `sql`/`admin`/`internal`: Reserved for system operations
/// - `information_schema`: SQL standard metadata schema
/// - `pg_catalog`: PostgreSQL compatibility
/// - `datafusion`: DataFusion internal catalog
pub const RESERVED_NAMESPACE_NAMES: &[&str] = &[
    "system",
    "sys",
    "root",
    "kalamdb",
    "kalam",
    "main",
    "default",
    "sql",
    "admin",
    "internal",
    "information_schema",
    "pg_catalog",
    "datafusion",
];
