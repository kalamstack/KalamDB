//! Column definition for table schemas

use serde::{Deserialize, Serialize};

use crate::models::{datatypes::KalamDataType, schemas::column_default::ColumnDefault};

/// Complete definition of a table column.
/// Fields ordered for optimal memory alignment (8-byte types first).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Stable column identifier - NEVER changes after creation
    ///
    /// This ID is written to Parquet files as the `field_id` in the schema.
    /// When reading, we match Parquet field_id to column_id, not column_name.
    /// This enables:
    /// - Column renames without rewriting data
    /// - Type promotions (e.g., INT32 → INT64) with correct mapping
    /// - Detecting which columns exist in old files vs current schema
    ///
    /// Assigned by TableDefinition.next_column_id when column is created.
    pub column_id: u64,

    /// Column name (case-insensitive, stored as lowercase)
    /// Can change via ALTER TABLE RENAME COLUMN
    pub column_name: String,

    /// Optional column comment/description
    pub column_comment: Option<String>,

    /// Data type - can change via type promotion only
    pub data_type: KalamDataType,

    /// Default value specification for INSERT operations
    pub default_value: ColumnDefault,

    /// Ordinal position in table (1-indexed, sequential)
    /// Determines SELECT * column ordering. Can change via reordering.
    pub ordinal_position: u32,

    /// Whether column can contain NULL values
    pub is_nullable: bool,

    /// Whether this column is part of the primary key
    pub is_primary_key: bool,

    /// Whether this column is part of the partition key (for distributed tables)
    pub is_partition_key: bool,
}

impl ColumnDefinition {
    /// Create a new column definition with explicit column_id
    ///
    /// # Arguments
    /// * `column_id` - Stable unique identifier (from TableDefinition.next_column_id)
    /// * `column_name` - Column name (will be lowercased)
    /// * `ordinal_position` - Display order (1-indexed)
    /// * `data_type` - Column data type
    /// * `is_nullable` - Whether NULL values are allowed
    /// * `is_primary_key` - Whether part of primary key
    /// * `is_partition_key` - Whether part of partition key
    /// * `default_value` - Default value for INSERT
    /// * `column_comment` - Optional description
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        column_id: u64,
        column_name: impl Into<String>,
        ordinal_position: u32,
        data_type: KalamDataType,
        is_nullable: bool,
        is_primary_key: bool,
        is_partition_key: bool,
        default_value: ColumnDefault,
        column_comment: Option<String>,
    ) -> Self {
        Self {
            column_id,
            column_name: column_name.into().to_lowercase(),
            ordinal_position,
            data_type,
            is_nullable,
            is_primary_key,
            is_partition_key,
            default_value,
            column_comment,
        }
    }

    /// Create a simple column with minimal configuration
    ///
    /// # Arguments
    /// * `column_id` - Stable unique identifier
    /// * `column_name` - Column name
    /// * `ordinal_position` - Display order
    /// * `data_type` - Column data type
    pub fn simple(
        column_id: u64,
        column_name: impl Into<String>,
        ordinal_position: u32,
        data_type: KalamDataType,
    ) -> Self {
        Self {
            column_id,
            column_name: column_name.into().to_lowercase(),
            ordinal_position,
            data_type,
            is_nullable: true,
            is_primary_key: false,
            is_partition_key: false,
            default_value: ColumnDefault::None,
            column_comment: None,
        }
    }

    /// Create a primary key column
    pub fn primary_key(
        column_id: u64,
        column_name: impl Into<String>,
        ordinal_position: u32,
        data_type: KalamDataType,
    ) -> Self {
        Self {
            column_id,
            column_name: column_name.into().to_lowercase(),
            ordinal_position,
            data_type,
            is_nullable: false, // Primary keys cannot be NULL
            is_primary_key: true,
            is_partition_key: false,
            default_value: ColumnDefault::None,
            column_comment: None,
        }
    }

    /// Get SQL DDL fragment for this column
    pub fn to_sql(&self) -> String {
        let mut parts = vec![self.column_name.clone(), self.data_type.sql_name()];

        if self.is_primary_key {
            parts.push("PRIMARY KEY".to_string());
        }

        if !self.is_nullable && !self.is_primary_key {
            parts.push("NOT NULL".to_string());
        }

        if !self.default_value.is_none() {
            parts.push(format!("DEFAULT {}", self.default_value.to_sql()));
        }

        if let Some(ref comment) = self.column_comment {
            parts.push(format!("COMMENT '{}'", comment.replace('\'', "''")));
        }

        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_simple_column() {
        let col = ColumnDefinition::simple(1, "name", 1, KalamDataType::Text);
        assert_eq!(col.column_id, 1);
        assert_eq!(col.column_name, "name");
        assert_eq!(col.ordinal_position, 1);
        assert!(col.is_nullable);
        assert!(!col.is_primary_key);
    }

    #[test]
    fn test_primary_key_column() {
        let col = ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt);
        assert_eq!(col.column_id, 1);
        assert_eq!(col.column_name, "id");
        assert!(!col.is_nullable);
        assert!(col.is_primary_key);
    }

    #[test]
    fn test_column_with_default() {
        let col = ColumnDefinition::new(
            3, // column_id
            "created_at",
            3,
            KalamDataType::Timestamp,
            false,
            false,
            false,
            ColumnDefault::function("NOW", vec![]),
            Some("Creation timestamp".to_string()),
        );

        assert_eq!(col.column_id, 3);
        assert_eq!(col.ordinal_position, 3);
        assert!(!col.is_nullable);
        assert_eq!(col.column_comment, Some("Creation timestamp".to_string()));
    }

    #[test]
    fn test_to_sql() {
        let col = ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt);
        let sql = col.to_sql();
        assert!(sql.contains("id"));
        assert!(sql.contains("BIGINT"));
        assert!(sql.contains("PRIMARY KEY"));

        let col = ColumnDefinition::new(
            2, // column_id
            "status",
            2,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::literal(json!("pending")),
            None,
        );
        let sql = col.to_sql();
        assert!(sql.contains("status"));
        assert!(sql.contains("TEXT"));
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("DEFAULT 'pending'"));
    }

    #[test]
    fn test_serialization() {
        let col = ColumnDefinition::simple(1, "test", 1, KalamDataType::Int);
        let json = serde_json::to_string(&col).unwrap();
        let decoded: ColumnDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(col, decoded);
    }

    #[test]
    fn test_partition_key() {
        let col = ColumnDefinition::new(
            1, // column_id
            "partition_key",
            1,
            KalamDataType::Text,
            false,
            false,
            true,
            ColumnDefault::None,
            None,
        );
        assert!(col.is_partition_key);
        assert!(!col.is_primary_key);
    }

    #[test]
    fn test_flexbuffers_roundtrip() {
        let column = ColumnDefinition::new(
            5, // column_id
            "status",
            5,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::literal(json!({"state": "active"})),
            Some("Status column".to_string()),
        );

        let bytes = flexbuffers::to_vec(&column).expect("encode column definition");
        let decoded: ColumnDefinition =
            flexbuffers::from_slice(&bytes).expect("decode column definition");

        assert_eq!(decoded, column);
    }

    #[test]
    fn test_column_name_case_insensitive() {
        // Column names should be normalized to lowercase
        let col1 = ColumnDefinition::simple(1, "FirstName", 1, KalamDataType::Text);
        let col2 = ColumnDefinition::simple(1, "firstname", 1, KalamDataType::Text);
        let col3 = ColumnDefinition::simple(1, "FIRSTNAME", 1, KalamDataType::Text);

        assert_eq!(col1.column_name, "firstname");
        assert_eq!(col2.column_name, "firstname");
        assert_eq!(col3.column_name, "firstname");
        assert_eq!(col1, col2);
        assert_eq!(col2, col3);

        // primary_key constructor also normalizes
        let pk = ColumnDefinition::primary_key(1, "UserId", 1, KalamDataType::BigInt);
        assert_eq!(pk.column_name, "userid");

        // new constructor also normalizes
        let full = ColumnDefinition::new(
            2, // column_id
            "CreatedAt",
            2,
            KalamDataType::Timestamp,
            false,
            false,
            false,
            ColumnDefault::None,
            None,
        );
        assert_eq!(full.column_name, "createdat");
    }

    #[test]
    fn test_column_id_stability() {
        // column_id should remain stable across renames
        let col1 = ColumnDefinition::simple(42, "old_name", 1, KalamDataType::Text);
        let mut col2 = col1.clone();
        col2.column_name = "new_name".to_string();

        // Even with different names, column_id should be the same
        assert_eq!(col1.column_id, col2.column_id);
        assert_eq!(col1.column_id, 42);
    }
}
