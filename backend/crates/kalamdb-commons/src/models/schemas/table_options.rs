//! Type-safe table options for different table types

use std::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::{schemas::policy::FlushPolicy, StorageId, TableAccess};

/// Compression algorithms supported by KalamDB's Parquet cold-storage writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableCompression {
    None,
    #[default]
    Snappy,
    Zstd,
}

impl TableCompression {
    pub const SUPPORTED_VALUES: &'static str = "none, snappy, zstd";

    pub const fn as_str(self) -> &'static str {
        match self {
            TableCompression::None => "none",
            TableCompression::Snappy => "snappy",
            TableCompression::Zstd => "zstd",
        }
    }
}

impl fmt::Display for TableCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for TableCompression {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TableCompression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TableCompressionVisitor;

        impl de::Visitor<'_> for TableCompressionVisitor {
            type Value = TableCompression;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a supported table compression: none, snappy, or zstd")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                value.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(TableCompressionVisitor)
    }
}

impl FromStr for TableCompression {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(TableCompression::None),
            "snappy" => Ok(TableCompression::Snappy),
            "zstd" => Ok(TableCompression::Zstd),
            other => Err(format!(
                "Invalid COMPRESSION '{}'. Supported: {}",
                other,
                TableCompression::SUPPORTED_VALUES
            )),
        }
    }
}

/// **Q: How does per-user storage assignment work with use_user_storage option?** → A: Lookup
/// chain: table.use_user_storage=true → check user.storage_mode → if "region" use user.storage_id,
/// if "table" use table.storage_id fallback
/// - *Impact*: User Story 2, User Story 10 (user management), new storage assignment logic
/// - *Rationale*: Enables data sovereignty (users in EU region → EU S3 bucket). Flexible fallback
///   prevents orphaned data. user.storage_mode="table" allows per-table override when needed.
///   Supports multi-tenant SaaS scenarios with region-specific compliance.
///
/// Table options for USER tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserTableOptions {
    /// The storage ID to use for this table (overrides user storage if use_user_storage is false)
    pub storage_id: StorageId,

    /// Whether to use the user's assigned storage ID instead of the table's storage ID
    #[serde(default)]
    pub use_user_storage: bool,

    /// Flush policy (e.g. time-based, size-based or both)
    pub flush_policy: Option<FlushPolicy>,

    /// Compression algorithm for Parquet cold-storage files.
    #[serde(default = "default_compression")]
    pub compression: TableCompression,
}

/// Table options for SHARED tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SharedTableOptions {
    pub storage_id: StorageId,

    /// Deprecated catalog field. Shared-table authorization is FORCE RLS; this
    /// value is ignored and omitted from new writes.
    #[serde(default, skip_serializing)]
    pub access_level: Option<TableAccess>,

    pub flush_policy: Option<FlushPolicy>,

    /// Compression algorithm for Parquet cold-storage files.
    #[serde(default = "default_compression")]
    pub compression: TableCompression,
}

/// Table options for STREAM tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamTableOptions {
    /// Time-to-live for stream events in seconds (required for STREAM tables)
    pub ttl_seconds: u64,

    /// Eviction strategy (time_based, size_based, hybrid)
    #[serde(default = "default_eviction_strategy")]
    pub eviction_strategy: String,

    /// Maximum stream size in bytes (0 = unlimited)
    #[serde(default)]
    pub max_stream_size_bytes: u64,
}

/// Table options for SYSTEM tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemTableOptions {
    /// System table is read-only
    #[serde(default = "default_true")]
    pub read_only: bool,

    /// Enable system table caching
    #[serde(default = "default_true")]
    pub enable_cache: bool,

    /// Cache TTL in seconds
    #[serde(default = "default_system_cache_ttl")]
    pub cache_ttl_seconds: u64,

    /// Allow system table queries only from localhost
    #[serde(default)]
    pub localhost_only: bool,
}

/// Union type for all table options
#[derive(Debug, Clone, PartialEq)]
pub enum TableOptions {
    User(UserTableOptions),
    Shared(SharedTableOptions),
    Stream(StreamTableOptions),
    System(SystemTableOptions),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "table_type", rename_all = "UPPERCASE")]
enum TableOptionsHuman {
    User(UserTableOptions),
    Shared(SharedTableOptions),
    Stream(StreamTableOptions),
    System(SystemTableOptions),
}

#[derive(Serialize, Deserialize)]
enum TableOptionsBinary {
    User(UserTableOptions),
    Shared(SharedTableOptions),
    Stream(StreamTableOptions),
    System(SystemTableOptions),
}

impl From<&TableOptions> for TableOptionsHuman {
    fn from(value: &TableOptions) -> Self {
        match value {
            TableOptions::User(opts) => TableOptionsHuman::User(opts.clone()),
            TableOptions::Shared(opts) => TableOptionsHuman::Shared(opts.clone()),
            TableOptions::Stream(opts) => TableOptionsHuman::Stream(opts.clone()),
            TableOptions::System(opts) => TableOptionsHuman::System(opts.clone()),
        }
    }
}

impl From<TableOptionsHuman> for TableOptions {
    fn from(value: TableOptionsHuman) -> Self {
        match value {
            TableOptionsHuman::User(opts) => TableOptions::User(opts),
            TableOptionsHuman::Shared(opts) => TableOptions::Shared(opts),
            TableOptionsHuman::Stream(opts) => TableOptions::Stream(opts),
            TableOptionsHuman::System(opts) => TableOptions::System(opts),
        }
    }
}

impl From<&TableOptions> for TableOptionsBinary {
    fn from(value: &TableOptions) -> Self {
        match value {
            TableOptions::User(opts) => TableOptionsBinary::User(opts.clone()),
            TableOptions::Shared(opts) => TableOptionsBinary::Shared(opts.clone()),
            TableOptions::Stream(opts) => TableOptionsBinary::Stream(opts.clone()),
            TableOptions::System(opts) => TableOptionsBinary::System(opts.clone()),
        }
    }
}

impl From<TableOptionsBinary> for TableOptions {
    fn from(value: TableOptionsBinary) -> Self {
        match value {
            TableOptionsBinary::User(opts) => TableOptions::User(opts),
            TableOptionsBinary::Shared(opts) => TableOptions::Shared(opts),
            TableOptionsBinary::Stream(opts) => TableOptions::Stream(opts),
            TableOptionsBinary::System(opts) => TableOptions::System(opts),
        }
    }
}

impl Serialize for TableOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            TableOptionsHuman::from(self).serialize(serializer)
        } else {
            TableOptionsBinary::from(self).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TableOptions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let human = TableOptionsHuman::deserialize(deserializer)?;
            Ok(human.into())
        } else {
            let binary = TableOptionsBinary::deserialize(deserializer)?;
            Ok(binary.into())
        }
    }
}

impl TableOptions {
    /// Create default options for a USER table
    pub fn user() -> Self {
        TableOptions::User(UserTableOptions::default())
    }

    /// Create default options for a SHARED table
    pub fn shared() -> Self {
        TableOptions::Shared(SharedTableOptions::default())
    }

    /// Create default options for a STREAM table with required TTL
    pub fn stream(ttl_seconds: u64) -> Self {
        TableOptions::Stream(StreamTableOptions {
            ttl_seconds,
            ..Default::default()
        })
    }

    /// Create default options for a SYSTEM table
    pub fn system() -> Self {
        TableOptions::System(SystemTableOptions::default())
    }

    /// Get the table-level Parquet compression setting when applicable.
    pub fn compression(&self) -> &str {
        match self {
            TableOptions::User(opts) => opts.compression.as_str(),
            TableOptions::Shared(opts) => opts.compression.as_str(),
            TableOptions::Stream(_) | TableOptions::System(_) => "none",
        }
    }

    /// Get the configured Parquet compression codec for table types that write cold segments.
    pub fn parquet_compression(&self) -> TableCompression {
        match self {
            TableOptions::User(opts) => opts.compression,
            TableOptions::Shared(opts) => opts.compression,
            TableOptions::Stream(_) | TableOptions::System(_) => TableCompression::None,
        }
    }

    /// Check if caching is enabled (where applicable)
    pub fn is_cache_enabled(&self) -> bool {
        match self {
            TableOptions::User(_) => false,   // User tables don't cache
            TableOptions::Shared(_) => false, // Shared tables don't cache
            TableOptions::Stream(_) => false, // Stream tables don't cache
            TableOptions::System(opts) => opts.enable_cache,
        }
    }

    /// Get cache TTL in seconds (returns None if caching not supported)
    pub fn cache_ttl_seconds(&self) -> Option<u64> {
        match self {
            TableOptions::User(_) => None,
            TableOptions::Shared(_) => None,
            TableOptions::Stream(_) => None,
            TableOptions::System(_) => None,
        }
    }

    /// Get the flush policy for this table (User and Shared only)
    pub fn flush_policy(&self) -> Option<&FlushPolicy> {
        match self {
            TableOptions::User(opts) => opts.flush_policy.as_ref(),
            TableOptions::Shared(opts) => opts.flush_policy.as_ref(),
            TableOptions::Stream(_) | TableOptions::System(_) => None,
        }
    }
}

// Default value functions for serde
fn default_true() -> bool {
    true
}

fn default_compression() -> TableCompression {
    TableCompression::default()
}

fn default_system_cache_ttl() -> u64 {
    300 // 5 minutes
}

fn default_eviction_strategy() -> String {
    "time_based".to_string()
}

impl Default for UserTableOptions {
    fn default() -> Self {
        Self {
            storage_id:       StorageId::default(),
            use_user_storage: false,
            flush_policy:     None,
            compression:      default_compression(),
        }
    }
}

impl Default for SharedTableOptions {
    fn default() -> Self {
        Self {
            storage_id:   StorageId::default(),
            access_level: None,
            flush_policy: None,
            compression:  default_compression(),
        }
    }
}

impl Default for StreamTableOptions {
    fn default() -> Self {
        Self {
            ttl_seconds:           86400, // 24 hours default
            eviction_strategy:     default_eviction_strategy(),
            max_stream_size_bytes: 0,
        }
    }
}

impl Default for SystemTableOptions {
    fn default() -> Self {
        Self {
            read_only:         true,
            enable_cache:      true,
            cache_ttl_seconds: default_system_cache_ttl(),
            localhost_only:    false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_table_options_default() {
        let opts = UserTableOptions::default();
        assert!(opts.flush_policy.is_none());
        assert_eq!(opts.compression, TableCompression::Snappy);
    }

    #[test]
    fn test_shared_table_options_default() {
        let opts = SharedTableOptions::default();
        assert!(opts.access_level.is_none());
        assert!(opts.flush_policy.is_none());
        assert_eq!(opts.compression, TableCompression::Snappy);
    }

    #[test]
    fn test_stream_table_options_default() {
        let opts = StreamTableOptions::default();
        assert_eq!(opts.ttl_seconds, 86400);
        assert_eq!(opts.eviction_strategy, "time_based");
        assert_eq!(opts.max_stream_size_bytes, 0);
    }

    #[test]
    fn test_system_table_options_default() {
        let opts = SystemTableOptions::default();
        assert!(opts.read_only);
        assert!(opts.enable_cache);
        assert_eq!(opts.cache_ttl_seconds, 300);
        assert!(!opts.localhost_only);
    }

    #[test]
    fn test_table_options_constructors() {
        let user = TableOptions::user();
        assert!(matches!(user, TableOptions::User(_)));

        let shared = TableOptions::shared();
        assert!(matches!(shared, TableOptions::Shared(_)));

        let stream = TableOptions::stream(3600);
        if let TableOptions::Stream(opts) = stream {
            assert_eq!(opts.ttl_seconds, 3600);
        } else {
            panic!("Expected Stream options");
        }

        let system = TableOptions::system();
        assert!(matches!(system, TableOptions::System(_)));
    }

    #[test]
    fn test_compression_getter() {
        assert_eq!(TableOptions::user().compression(), "snappy");
        assert_eq!(TableOptions::shared().compression(), "snappy");
        assert_eq!(TableOptions::stream(3600).compression(), "none");
        assert_eq!(TableOptions::system().compression(), "none");
    }

    #[test]
    fn test_cache_enabled() {
        assert!(!TableOptions::user().is_cache_enabled());
        assert!(!TableOptions::shared().is_cache_enabled()); // Shared tables no longer have caching
        assert!(!TableOptions::stream(3600).is_cache_enabled());
        assert!(TableOptions::system().is_cache_enabled());
    }

    #[test]
    fn test_cache_ttl() {
        assert_eq!(TableOptions::user().cache_ttl_seconds(), None);
        assert_eq!(TableOptions::shared().cache_ttl_seconds(), None); // Shared tables no longer have caching
        assert_eq!(TableOptions::stream(3600).cache_ttl_seconds(), None);
        assert_eq!(TableOptions::system().cache_ttl_seconds(), None); // Updated to return None per
                                                                      // implementation
    }

    #[test]
    fn test_serialization() {
        let opts = TableOptions::stream(7200);
        let json = serde_json::to_string(&opts).unwrap();
        let decoded: TableOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(opts, decoded);
    }

    #[test]
    fn test_custom_options() {
        let custom_stream = TableOptions::Stream(StreamTableOptions {
            ttl_seconds:           1800,
            eviction_strategy:     "size_based".to_string(),
            max_stream_size_bytes: 1_000_000_000,
        });

        assert_eq!(custom_stream.compression(), "none");
        if let TableOptions::Stream(opts) = custom_stream {
            assert_eq!(opts.ttl_seconds, 1800);
            assert_eq!(opts.eviction_strategy, "size_based");
            assert_eq!(opts.max_stream_size_bytes, 1_000_000_000);
        }
    }

    #[test]
    fn test_table_compression_supported_values() {
        assert_eq!("none".parse::<TableCompression>().unwrap(), TableCompression::None);
        assert_eq!("snappy".parse::<TableCompression>().unwrap(), TableCompression::Snappy);
        assert_eq!("ZSTD".parse::<TableCompression>().unwrap(), TableCompression::Zstd);
        assert!("lz4".parse::<TableCompression>().is_err());
    }

    #[test]
    fn test_table_compression_serializes_as_lowercase_string() {
        let json = serde_json::to_string(&TableCompression::Zstd).unwrap();
        assert_eq!(json, "\"zstd\"");
        let decoded: TableCompression = serde_json::from_str("\"snappy\"").unwrap();
        assert_eq!(decoded, TableCompression::Snappy);
    }
}
