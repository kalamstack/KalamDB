//! Table type classification

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Enum representing the type of table in KalamDB.
///
/// Each table type has associated type-safe options:
/// - **User** → `UserTableOptions`: Per-user tables with user-specific partitioning (e.g.,
///   `user_123/conversations`)
/// - **Shared** → `SharedTableOptions`: Shared tables accessible across all users (e.g.,
///   `categories`)
/// - **Stream** → `StreamTableOptions`: Event stream tables with TTL-based eviction (e.g.,
///   `chat_events`)
/// - **System** → `SystemTableOptions`: Internal system metadata tables (e.g.,
///   `information_schema.tables`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableType {
    /// Per-user tables with user-specific partitioning
    /// Options: `UserTableOptions` (partition_by_user, max_rows_per_user, enable_rls, compression)
    User,

    /// Shared tables accessible across all users
    /// Options: `SharedTableOptions` (access_level, enable_cache, cache_ttl_seconds, compression,
    /// enable_replication)
    Shared,

    /// Event stream tables with TTL-based eviction
    /// Options: `StreamTableOptions` (ttl_seconds, eviction_strategy, max_stream_size_bytes,
    /// enable_compaction, watermark_delay_seconds, compression)
    Stream,

    /// Internal system metadata tables
    /// Options: `SystemTableOptions` (read_only, enable_cache, cache_ttl_seconds, localhost_only)
    System,
}

impl TableType {
    /// Returns the table type as a string (lowercase for column family names).
    pub fn as_str(&self) -> &'static str {
        match self {
            TableType::User => "user",
            TableType::Shared => "shared",
            TableType::Stream => "stream",
            TableType::System => "system",
        }
    }

    /// Parse a table type from a string (case-insensitive).
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "USER" => Some(TableType::User),
            "SHARED" => Some(TableType::Shared),
            "STREAM" => Some(TableType::Stream),
            "SYSTEM" => Some(TableType::System),
            _ => None,
        }
    }
}

impl FromStr for TableType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TableType::from_str_opt(s).ok_or_else(|| format!("Invalid TableType: {}", s))
    }
}

impl fmt::Display for TableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_str() {
        assert_eq!(TableType::User.as_str(), "user");
        assert_eq!(TableType::Shared.as_str(), "shared");
        assert_eq!(TableType::Stream.as_str(), "stream");
        assert_eq!(TableType::System.as_str(), "system");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(TableType::from_str_opt("USER"), Some(TableType::User));
        assert_eq!(TableType::from_str_opt("user"), Some(TableType::User));
        assert_eq!(TableType::from_str_opt("SHARED"), Some(TableType::Shared));
        assert_eq!(TableType::from_str_opt("STREAM"), Some(TableType::Stream));
        assert_eq!(TableType::from_str_opt("SYSTEM"), Some(TableType::System));
        assert_eq!(TableType::from_str_opt("INVALID"), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", TableType::User), "user");
        assert_eq!(format!("{}", TableType::Shared), "shared");
    }

    #[test]
    fn test_serialization() {
        let table_type = TableType::User;
        let json = serde_json::to_string(&table_type).unwrap();
        let decoded: TableType = serde_json::from_str(&json).unwrap();
        assert_eq!(table_type, decoded);
    }
}
