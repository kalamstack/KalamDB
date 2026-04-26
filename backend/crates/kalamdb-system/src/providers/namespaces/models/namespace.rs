//! Namespace entity for system.namespaces table.

use kalamdb_commons::{datatypes::KalamDataType, models::ids::NamespaceId};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Namespace entity for system.namespaces table.
///
/// Represents a database namespace for data isolation.
///
/// ## Fields
/// - `namespace_id`: Unique namespace identifier
/// - `name`: Namespace name (e.g., "default", "production")
/// - `created_at`: Unix timestamp in milliseconds when namespace was created
/// - `options`: Optional JSON configuration
/// - `table_count`: Number of tables in this namespace
///
/// ## Serialization
/// - **RocksDB**: FlatBuffers envelope + FlexBuffers payload
/// - **API**: JSON via Serde
///
/// ## Example
///
/// ```rust
/// use kalamdb_commons::NamespaceId;
/// use kalamdb_system::Namespace;
///
/// let namespace = Namespace {
///     namespace_id: NamespaceId::default(),
///     name:         "default".to_string(),
///     created_at:   1730000000000,
///     options:      Some(serde_json::json!({})),
///     table_count:  0,
/// };
/// ```
/// Namespace struct with fields ordered for optimal memory alignment.
/// 8-byte aligned fields first (i64, String types), then smaller types.
#[table(name = "namespaces", comment = "Database namespaces for multi-tenancy")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Namespace {
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Namespace creation timestamp"
    )]
    pub created_at: i64, // Unix timestamp in milliseconds
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Namespace identifier"
    )]
    pub namespace_id: NamespaceId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Namespace name"
    )]
    pub name: String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Namespace configuration options (JSON)"
    )]
    #[serde(default)]
    pub options: Option<Value>,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Number of tables in this namespace"
    )]
    pub table_count: i32, // TODO: Remove this field and calculate on the fly
}

impl Namespace {
    /// Create a new namespace with default values
    ///
    /// # Arguments
    /// * `name` - Namespace identifier
    ///
    /// # Example
    /// ```
    /// use kalamdb_system::Namespace;
    ///
    /// let namespace = Namespace::new("app");
    /// assert_eq!(namespace.name, "app");
    /// assert_eq!(namespace.table_count, 0);
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        let name_str = name.into();
        Self {
            namespace_id: NamespaceId::new(&name_str),
            name: name_str,
            created_at: chrono::Utc::now().timestamp_millis(),
            options: Some(serde_json::json!({})),
            table_count: 0,
        }
    }

    /// Check if this namespace can be deleted (has no tables)
    #[inline]
    pub fn can_delete(&self) -> bool {
        self.table_count == 0
    }

    /// Increment the table count
    #[inline]
    pub fn increment_table_count(&mut self) {
        self.table_count += 1;
    }

    /// Decrement the table count
    #[inline]
    pub fn decrement_table_count(&mut self) {
        if self.table_count > 0 {
            self.table_count -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_serialization() {
        let namespace = Namespace {
            namespace_id: NamespaceId::default(),
            name: "default".to_string(),
            created_at: 1730000000000,
            options: Some(serde_json::json!({})),
            table_count: 0,
        };

        let bytes = serde_json::to_vec(&namespace).unwrap();
        let deserialized: Namespace = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(namespace, deserialized);
    }
}
