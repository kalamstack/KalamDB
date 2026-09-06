//! Table definition - single source of truth for table schemas
//!
//! **Phase 16 Schema Versioning**: Schema history is now stored separately using TableVersionId
//! keys. Each TableDefinition stores only its current `schema_version: u32`.
//! Historical versions are stored as separate entries: `{tableId}<ver>{version:08}` ->
//! TableDefinition The latest pointer `{tableId}<lat>` points to the current version.

use std::sync::Arc;

use arrow_schema::{Field, Schema as ArrowSchema};
use chrono::{DateTime, Utc};
use serde::{
    de::{self, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::{
    conversions::{with_kalam_column_flags_metadata, with_kalam_data_type_metadata},
    models::{
        datatypes::{ArrowConversionError, ToArrowType},
        schemas::{ColumnDefinition, ScalarIndexDefinition, SchemaField, TableOptions, TableType},
    },
    NamespaceId, TableName,
};

/// Complete definition of a table including schema, history, and options
///
/// **Phase 15 Consolidation**: This is now the SINGLE SOURCE OF TRUTH for all table metadata.
/// Previously split between SystemTable (registry metadata) and TableDefinition (schema).
/// Now unified to eliminate duplication and simplify architecture.
#[derive(Debug, Clone, PartialEq)]
pub struct TableDefinition {
    pub namespace_id: NamespaceId,

    /// Table name (case-sensitive)
    pub table_name: TableName,

    /// Table type (USER, SHARED, STREAM, SYSTEM)
    pub table_type: TableType,

    /// Column definitions (ordered by ordinal_position)
    pub columns: Vec<ColumnDefinition>,

    /// Current schema version number (starts at 1, incremented on ALTER TABLE)
    pub schema_version: u32,

    /// Next available column_id for new columns
    ///
    /// Monotonically increasing. When adding a column:
    /// 1. Assign next_column_id to new column
    /// 2. Increment next_column_id
    ///
    /// This ensures column_ids are never reused, even after DROP COLUMN.
    /// Written to Parquet files as `field_id` for schema evolution support.
    pub next_column_id: u64,

    /// Type-safe table options based on table_type
    pub table_options: TableOptions,

    /// Table comment/description
    pub table_comment: Option<String>,

    /// Scalar secondary indexes (non-unique by default). Empty when omitted.
    pub scalar_indexes: Vec<ScalarIndexDefinition>,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last modification timestamp
    pub updated_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct TableDefinitionRepr {
    namespace_id:   NamespaceId,
    table_name:     TableName,
    table_type:     TableType,
    columns:        Vec<ColumnDefinition>,
    schema_version: u32,
    next_column_id: u64,
    table_options:  TableOptions,
    table_comment:  Option<String>,
    #[serde(default)]
    scalar_indexes: Vec<ScalarIndexDefinition>,
    created_at:     DateTime<Utc>,
    updated_at:     DateTime<Utc>,
}

impl From<&TableDefinition> for TableDefinitionRepr {
    fn from(table: &TableDefinition) -> Self {
        Self {
            namespace_id:   table.namespace_id.clone(),
            table_name:     table.table_name.clone(),
            table_type:     table.table_type,
            columns:        table.columns.clone(),
            schema_version: table.schema_version,
            next_column_id: table.next_column_id,
            table_options:  table.table_options.clone(),
            table_comment:  table.table_comment.clone(),
            scalar_indexes: table.scalar_indexes.clone(),
            created_at:     table.created_at,
            updated_at:     table.updated_at,
        }
    }
}

#[derive(PartialEq, Serialize)]
struct SemanticSchema<'a> {
    namespace_id:   &'a NamespaceId,
    table_name:     &'a TableName,
    table_type:     TableType,
    columns:        &'a [ColumnDefinition],
    next_column_id: u64,
    table_options:  &'a TableOptions,
    table_comment:  &'a Option<String>,
    scalar_indexes: &'a [ScalarIndexDefinition],
}

impl From<TableDefinitionRepr> for TableDefinition {
    fn from(value: TableDefinitionRepr) -> Self {
        Self {
            namespace_id:   value.namespace_id,
            table_name:     value.table_name,
            table_type:     value.table_type,
            columns:        value.columns,
            schema_version: value.schema_version,
            next_column_id: value.next_column_id,
            table_options:  value.table_options,
            table_comment:  value.table_comment,
            scalar_indexes: value.scalar_indexes,
            created_at:     value.created_at,
            updated_at:     value.updated_at,
        }
    }
}

struct BinaryTableDefinitionVisitor;

impl<'de> Visitor<'de> for BinaryTableDefinitionVisitor {
    type Value = TableDefinition;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("table definition tuple")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let namespace_id =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let table_name = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let table_type = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
        let columns = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &self))?;
        let schema_version =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(4, &self))?;
        let next_column_id =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(5, &self))?;
        let table_options =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(6, &self))?;
        let table_comment =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(7, &self))?;
        let created_at = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(8, &self))?;
        let updated_at = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(9, &self))?;
        let scalar_indexes = seq.next_element()?.unwrap_or_default();
        Ok(TableDefinition {
            namespace_id,
            table_name,
            table_type,
            columns,
            schema_version,
            next_column_id,
            table_options,
            table_comment,
            scalar_indexes,
            created_at,
            updated_at,
        })
    }
}

impl Serialize for TableDefinition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            TableDefinitionRepr::from(self).serialize(serializer)
        } else {
            (
                &self.namespace_id,
                &self.table_name,
                &self.table_type,
                &self.columns,
                self.schema_version,
                self.next_column_id,
                &self.table_options,
                &self.table_comment,
                &self.created_at,
                &self.updated_at,
                &self.scalar_indexes,
            )
                .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TableDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let repr = TableDefinitionRepr::deserialize(deserializer)?;
            Ok(repr.into())
        } else {
            deserializer.deserialize_seq(BinaryTableDefinitionVisitor)
        }
    }
}

impl TableDefinition {
    /// Create a new table definition with full control over all fields
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use kalamdb_commons::models::schemas::{TableDefinition, ColumnDefinition, TableType, TableOptions};
    /// use kalamdb_commons::models::datatypes::KalamDataType;
    /// use kalamdb_commons::{NamespaceId, TableName, TableId, StorageId};
    ///
    /// let columns = vec![
    ///     ColumnDefinition::primary_key("id", 1, KalamDataType::Uuid),
    ///     ColumnDefinition::simple("name", 2, KalamDataType::Text),
    ///     ColumnDefinition::simple("age", 3, KalamDataType::Int),
    /// ];
    ///
    /// let namespace_id = NamespaceId::new("my_namespace");
    /// let table_name = TableName::new("users");
    /// let table_id = TableId::new(namespace_id.clone(), table_name.clone());
    ///
    /// let table = TableDefinition::new(namespace_id, table_name, TableType::User, columns, TableOptions::user(), Some("User accounts table".into())).unwrap();
    ///
    /// assert_eq!(table.namespace_id, NamespaceId::new("my_namespace"));
    /// assert_eq!(table.table_name, TableName::new("users"));
    /// assert_eq!(table.columns.len(), 3);
    /// assert_eq!(table.schema_version, 1);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace_id: NamespaceId,
        table_name: TableName,
        table_type: TableType,
        columns: Vec<ColumnDefinition>,
        table_options: TableOptions,
        table_comment: Option<String>,
    ) -> Result<Self, String> {
        let columns_sorted = Self::validate_and_sort_columns(columns)?;
        let now = Utc::now();

        // Compute next_column_id as max(column_id) + 1
        let next_column_id = columns_sorted.iter().map(|c| c.column_id).max().unwrap_or(0) + 1;

        Ok(Self {
            namespace_id,
            table_name,
            table_type,
            columns: columns_sorted,
            schema_version: 1,
            next_column_id,
            table_options,
            table_comment,
            scalar_indexes: Vec::new(),
            created_at: now,
            updated_at: now,
        })
    }

    /// Create a new table with sensible defaults based on table type
    ///
    /// This is a convenience constructor that auto-generates table_id and sets defaults:
    /// - storage_id: None (use default storage)
    /// - use_user_storage: false
    /// - deleted_retention_hours: 24 (24 hours)
    pub fn new_with_defaults(
        namespace_id: NamespaceId,
        table_name: TableName,
        table_type: TableType,
        columns: Vec<ColumnDefinition>,
        table_comment: Option<String>,
    ) -> Result<Self, String> {
        let table_options = match table_type {
            TableType::User => TableOptions::user(),
            TableType::Shared => TableOptions::shared(),
            TableType::Stream => TableOptions::stream(86400), // Default 24h TTL
            TableType::System => TableOptions::system(),
        };

        Self::new(namespace_id, table_name, table_type, columns, table_options, table_comment)
    }

    /// Validate and sort columns by ordinal_position
    fn validate_and_sort_columns(
        mut columns: Vec<ColumnDefinition>,
    ) -> Result<Vec<ColumnDefinition>, String> {
        // Check for duplicate ordinal positions
        let mut positions = std::collections::HashSet::new();
        for col in &columns {
            if col.ordinal_position == 0 {
                return Err(format!(
                    "Column '{}' has invalid ordinal_position 0 (must be ≥ 1)",
                    col.column_name
                ));
            }
            if !positions.insert(col.ordinal_position) {
                return Err(format!("Duplicate ordinal_position {}", col.ordinal_position));
            }
        }

        // Sort by ordinal_position
        columns.sort_by_key(|col| col.ordinal_position);

        // Validate sequential positions starting from 1
        for (idx, col) in columns.iter().enumerate() {
            let expected = (idx + 1) as u32;
            if col.ordinal_position != expected {
                return Err(format!(
                    "Non-sequential ordinal_position: expected {}, got {}",
                    expected, col.ordinal_position
                ));
            }
        }

        Ok(columns)
    }

    /// Convert to Arrow schema (current version)
    ///
    /// Each Arrow field includes a `kalam_data_type` metadata entry with the
    /// serialized KalamDataType for lossless round-trip conversion.
    pub fn to_arrow_schema(&self) -> Result<Arc<ArrowSchema>, ArrowConversionError> {
        let fields: Result<Vec<Field>, _> = self
            .columns
            .iter()
            .map(|col| {
                let arrow_type = col.data_type.to_arrow_type()?;
                let mut field = Field::new(&col.column_name, arrow_type, col.is_nullable);

                // Store KalamDataType in metadata for lossless round-trip conversion
                field = with_kalam_data_type_metadata(field, &col.data_type);

                if let Some(flags) =
                    SchemaField::flags_for_column(col.is_primary_key, col.is_nullable)
                {
                    field = with_kalam_column_flags_metadata(field, &flags);
                }

                Ok(field)
            })
            .collect();

        let schema = ArrowSchema::new(fields?);
        Ok(Arc::new(schema))
    }

    /// Increment schema version (for ALTER TABLE operations)
    ///
    /// **Phase 16**: Version history is stored externally using TableVersionId keys.
    /// After calling this method, the caller must persist this TableDefinition
    /// to the versioned storage using `TablesStore::put_version()`.
    pub fn increment_version(&mut self) {
        self.schema_version += 1;
        self.updated_at = Utc::now();
    }

    /// Get a reference to the table options
    pub fn options(&self) -> &TableOptions {
        &self.table_options
    }

    /// Update table options (must match table type)
    pub fn set_options(&mut self, options: TableOptions) {
        self.table_options = options;
        self.updated_at = Utc::now();
    }

    /// Get fully qualified table name (namespace.table)
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace_id.as_str(), self.table_name.as_str())
    }

    /// Add a column (for ALTER TABLE ADD COLUMN)
    /// The column must have column_id == self.next_column_id
    pub fn add_column(&mut self, column: ColumnDefinition) -> Result<(), String> {
        // Validate ordinal_position is next available
        let max_ordinal = self.columns.iter().map(|c| c.ordinal_position).max().unwrap_or(0);
        if column.ordinal_position != max_ordinal + 1 {
            return Err(format!(
                "New column must have ordinal_position {}, got {}",
                max_ordinal + 1,
                column.ordinal_position
            ));
        }

        // Validate column_id matches next_column_id
        if column.column_id != self.next_column_id {
            return Err(format!(
                "New column must have column_id {}, got {}",
                self.next_column_id, column.column_id
            ));
        }

        self.columns.push(column);
        self.next_column_id += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Drop a column (for ALTER TABLE DROP COLUMN)
    /// Note: Does NOT renumber remaining columns' ordinal_position
    pub fn drop_column(&mut self, column_name: &str) -> Result<ColumnDefinition, String> {
        let position = self
            .columns
            .iter()
            .position(|c| c.column_name == column_name)
            .ok_or_else(|| format!("Column '{}' not found", column_name))?;

        let removed = self.columns.remove(position);
        self.updated_at = Utc::now();
        Ok(removed)
    }

    fn semantic_schema(&self) -> SemanticSchema<'_> {
        SemanticSchema {
            namespace_id:   &self.namespace_id,
            table_name:     &self.table_name,
            table_type:     self.table_type,
            columns:        &self.columns,
            next_column_id: self.next_column_id,
            table_options:  &self.table_options,
            table_comment:  &self.table_comment,
            scalar_indexes: &self.scalar_indexes,
        }
    }

    /// Compare schema identity, ignoring `schema_version` and timestamps.
    ///
    /// Restarts rebuild in-memory definitions with `Utc::now()`, so full
    /// [`PartialEq`] is the wrong check for "did this table actually change?".
    pub fn semantically_equal(&self, other: &Self) -> bool {
        self.semantic_schema() == other.semantic_schema()
    }

    /// Stable JSON of semantic schema fields for catalog reconcile markers.
    pub fn semantic_reconcile_bytes(defs: &[&Self]) -> Result<Vec<u8>, String> {
        let rows: Vec<SemanticSchema<'_>> =
            defs.iter().copied().map(Self::semantic_schema).collect();
        serde_json::to_vec(&rows).map_err(|error| error.to_string())
    }

    /// Get the names of primary key columns, sorted by ordinal position
    ///
    /// Returns an empty vector if no primary key columns are defined.
    /// This is used for default ORDER BY clauses to ensure consistent
    /// ordering across hot and cold storage queries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let pk_columns = table_def.get_primary_key_columns();
    /// // Returns: vec!["id"] or vec!["user_id", "order_id"] for composite keys
    /// ```
    pub fn get_primary_key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|col| col.is_primary_key)
            .map(|col| col.column_name.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{models::datatypes::KalamDataType, NamespaceId, TableName};

    fn sample_columns() -> Vec<ColumnDefinition> {
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
            ColumnDefinition::simple(3, "age", 3, KalamDataType::Int),
        ]
    }

    #[test]
    fn test_new_table_definition() {
        let table = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            Some("User table".to_string()),
        )
        .unwrap();

        assert_eq!(table.namespace_id, NamespaceId::default());
        assert_eq!(table.table_name, TableName::new("users"));
        assert_eq!(table.table_type, TableType::User);
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.schema_version, 1);
        assert_eq!(table.qualified_name(), "default.users");
    }

    #[test]
    fn test_new_with_defaults() {
        let table = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("events"),
            TableType::Stream,
            sample_columns(),
            None,
        )
        .unwrap();

        assert_eq!(table.table_type, TableType::Stream);
        // Should have default stream options with 24h TTL
        if let TableOptions::Stream(opts) = &table.table_options {
            assert_eq!(opts.ttl_seconds, 86400);
        } else {
            panic!("Expected Stream options");
        }
    }

    #[test]
    fn test_column_ordering() {
        // Create columns out of order
        let columns = vec![
            ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(3, "age", 3, KalamDataType::Int),
        ];

        let table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            columns,
            TableOptions::user(),
            None,
        )
        .unwrap();

        // Verify columns are sorted by ordinal_position
        assert_eq!(table.columns[0].column_name, "id");
        assert_eq!(table.columns[1].column_name, "name");
        assert_eq!(table.columns[2].column_name, "age");
    }

    #[test]
    fn test_duplicate_ordinal_position() {
        let columns = vec![
            ColumnDefinition::simple(1, "col1", 1, KalamDataType::Int),
            ColumnDefinition::simple(2, "col2", 1, KalamDataType::Int),
        ];

        let result = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("test"),
            TableType::User,
            columns,
            TableOptions::user(),
            None,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Duplicate ordinal_position"));
    }

    #[test]
    fn test_non_sequential_ordinal_position() {
        let columns = vec![
            ColumnDefinition::simple(1, "col1", 1, KalamDataType::Int),
            ColumnDefinition::simple(2, "col2", 3, KalamDataType::Int), // Skips 2
        ];

        let result = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("test"),
            TableType::User,
            columns,
            TableOptions::user(),
            None,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Non-sequential"));
    }

    #[test]
    fn test_to_arrow_schema() {
        let table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            TableOptions::user(),
            None,
        )
        .unwrap();

        let arrow_schema = table.to_arrow_schema().unwrap();
        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(arrow_schema.field(2).name(), "age");
    }

    #[test]
    fn test_increment_version() {
        let mut table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            TableOptions::user(),
            None,
        )
        .unwrap();

        assert_eq!(table.schema_version, 1);
        table.increment_version();
        assert_eq!(table.schema_version, 2);
        table.increment_version();
        assert_eq!(table.schema_version, 3);
    }

    #[test]
    fn test_add_column() {
        let mut table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            TableOptions::user(),
            None,
        )
        .unwrap();

        let new_col = ColumnDefinition::simple(4, "email", 4, KalamDataType::Text);
        table.add_column(new_col).unwrap();

        assert_eq!(table.columns.len(), 4);
        assert_eq!(table.columns[3].column_name, "email");
    }

    #[test]
    fn test_drop_column() {
        let mut table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            TableOptions::user(),
            None,
        )
        .unwrap();

        let dropped = table.drop_column("name").unwrap();
        assert_eq!(dropped.column_name, "name");
        assert_eq!(table.columns.len(), 2);

        // Verify ordinal_position preserved
        assert_eq!(table.columns[0].ordinal_position, 1); // id
        assert_eq!(table.columns[1].ordinal_position, 3); // age (not renumbered)
    }

    #[test]
    fn test_table_options_type_safety() {
        // Test USER table options
        let user_table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            TableOptions::user(),
            None,
        )
        .unwrap();

        assert!(matches!(user_table.options(), TableOptions::User(_)));

        // Test STREAM table options
        let stream_table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("events"),
            TableType::Stream,
            sample_columns(),
            TableOptions::stream(3600),
            None,
        )
        .unwrap();

        if let TableOptions::Stream(opts) = stream_table.options() {
            assert_eq!(opts.ttl_seconds, 3600);
        } else {
            panic!("Expected Stream options");
        }

        // Test SHARED table options
        let _shared_table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("categories"),
            TableType::Shared,
            sample_columns(),
            TableOptions::shared(),
            None,
        )
        .unwrap();
    }

    #[test]
    fn test_set_options() {
        let mut table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("events"),
            TableType::Stream,
            sample_columns(),
            TableOptions::stream(3600),
            None,
        )
        .unwrap();

        // Update to custom stream options
        let custom_opts = TableOptions::Stream(crate::models::schemas::StreamTableOptions {
            ttl_seconds:           1800,
            eviction_strategy:     "size_based".to_string(),
            max_stream_size_bytes: 1_000_000_000,
        });

        table.set_options(custom_opts);

        if let TableOptions::Stream(opts) = table.options() {
            assert_eq!(opts.ttl_seconds, 1800);
            assert_eq!(opts.eviction_strategy, "size_based");
        } else {
            panic!("Expected Stream options");
        }
    }

    #[test]
    fn test_get_primary_key_columns_single() {
        let columns = vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
            ColumnDefinition::simple(3, "age", 3, KalamDataType::Int),
        ];

        let table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            columns,
            TableOptions::user(),
            None,
        )
        .unwrap();

        let pk_columns = table.get_primary_key_columns();
        assert_eq!(pk_columns.len(), 1);
        assert_eq!(pk_columns[0], "id");
    }

    #[test]
    fn test_get_primary_key_columns_empty() {
        // Table without any primary key column
        let columns = vec![
            ColumnDefinition::simple(1, "field1", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "field2", 2, KalamDataType::Int),
        ];

        let table = TableDefinition::new(
            NamespaceId::default(),
            TableName::new("no_pk"),
            TableType::Shared,
            columns,
            TableOptions::shared(),
            None,
        )
        .unwrap();

        let pk_columns = table.get_primary_key_columns();
        assert!(pk_columns.is_empty());
    }

    #[test]
    fn scalar_indexes_roundtrip_json_and_default_to_empty() {
        let mut table = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("messages"),
            TableType::Shared,
            sample_columns(),
            None,
        )
        .unwrap();
        table.scalar_indexes = vec![
            ScalarIndexDefinition::new(
                "messages_conversation_id",
                vec![crate::models::ColumnId::new(2)],
                false,
            ),
            ScalarIndexDefinition::new(
                "messages_created_at",
                vec![
                    crate::models::ColumnId::new(2),
                    crate::models::ColumnId::new(3),
                ],
                false,
            ),
        ];

        let json = serde_json::to_string(&table).unwrap();
        let decoded: TableDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.scalar_indexes, table.scalar_indexes);
        assert!(!decoded.scalar_indexes[1].unique);

        let mut omitted = serde_json::from_str::<serde_json::Value>(&json).unwrap();
        omitted.as_object_mut().unwrap().remove("scalar_indexes");
        let without_field: TableDefinition = serde_json::from_value(omitted).unwrap();
        assert!(without_field.scalar_indexes.is_empty());
    }

    #[test]
    fn scalar_indexes_resolve_renamed_column_by_id() {
        let mut table = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("messages"),
            TableType::Shared,
            sample_columns(),
            None,
        )
        .unwrap();
        table.scalar_indexes = vec![ScalarIndexDefinition::new(
            "idx_name",
            vec![crate::models::ColumnId::new(2)],
            false,
        )];
        table.columns[1].column_name = "title".to_string();
        assert_eq!(
            table.scalar_indexes[0].resolved_column_names(&table.columns),
            Some(vec!["title"])
        );
    }

    #[test]
    fn semantically_equal_ignores_timestamps_and_schema_version() {
        let mut current = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            Some("User table".to_string()),
        )
        .unwrap();
        let expected = current.clone();
        current.schema_version = 9;
        current.created_at -= chrono::Duration::seconds(30);
        current.updated_at -= chrono::Duration::seconds(5);

        assert_ne!(current, expected);
        assert!(current.semantically_equal(&expected));
    }

    #[test]
    fn semantically_equal_detects_column_changes() {
        let current = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            None,
        )
        .unwrap();
        let mut expected = current.clone();
        expected.columns[1].column_name = "title".to_string();

        assert!(!current.semantically_equal(&expected));
    }

    #[test]
    fn semantic_reconcile_bytes_are_stable_across_timestamps() {
        let mut first = TableDefinition::new_with_defaults(
            NamespaceId::default(),
            TableName::new("users"),
            TableType::User,
            sample_columns(),
            None,
        )
        .unwrap();
        let second = first.clone();
        first.created_at -= chrono::Duration::days(1);
        first.updated_at -= chrono::Duration::hours(3);
        first.schema_version = 4;

        let left = TableDefinition::semantic_reconcile_bytes(&[&first]).unwrap();
        let right = TableDefinition::semantic_reconcile_bytes(&[&second]).unwrap();
        assert_eq!(left, right);
    }
}

// KSerializable implementation for EntityStore support
#[cfg(feature = "serialization")]
impl crate::serialization::KSerializable for TableDefinition {}
