//! system.describe virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides DESCRIBE TABLE functionality as a virtual view, computing column
//! metadata dynamically from the schema registry.
//!
//! **DataFusion Pattern**: Implements VirtualView trait for consistent view behavior
//! - Computes column metadata dynamically on each query
//! - No persistent state in RocksDB
//! - Memoizes schema via `OnceLock`
//!
//! **Schema**: TableDefinition provides consistent metadata for views

use std::sync::{Arc, OnceLock};

use datafusion::arrow::{
    array::{ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringBuilder},
    datatypes::SchemaRef,
};
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, TableId, TableName,
};
use kalamdb_system::SystemTable;

use crate::{error::RegistryError, view_base::VirtualView};

/// Get the describe schema (memoized)
fn describe_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            DescribeView::definition()
                .to_arrow_schema()
                .expect("Failed to convert describe TableDefinition to Arrow schema")
        })
        .clone()
}

/// Callback type for fetching table definition
/// Returns the TableDefinition for the specified table
pub type DescribeCallback = Arc<dyn Fn(&TableId) -> Option<TableDefinition> + Send + Sync>;

/// Virtual view that provides DESCRIBE TABLE functionality
///
/// **DataFusion Design**:
/// - Implements VirtualView trait
/// - Returns TableType::View
/// - Computes batch dynamically from schema registry
pub struct DescribeView {
    /// Optional callback to fetch table definitions
    /// Used for testing or when integrated with AppContext
    callback: Option<DescribeCallback>,
}

impl std::fmt::Debug for DescribeView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DescribeView")
            .field("has_callback", &self.callback.is_some())
            .finish()
    }
}

impl DescribeView {
    /// Get the TableDefinition for system.describe view
    ///
    /// This is the single source of truth for:
    /// - Column definitions (names, types, nullability)
    /// - Column comments/descriptions
    ///
    /// Schema:
    /// - column_name TEXT NOT NULL
    /// - ordinal_position INT NOT NULL (as u32)
    /// - column_id BIGINT NOT NULL (Parquet field_id)
    /// - data_type TEXT NOT NULL
    /// - is_nullable BOOLEAN NOT NULL
    /// - is_primary_key BOOLEAN NOT NULL
    /// - column_default TEXT (nullable)
    /// - column_comment TEXT (nullable)
    /// - schema_version INT NOT NULL (version when column was added)
    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "column_name",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Name of the column".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "ordinal_position",
                2,
                KalamDataType::Int,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Position of column in SELECT * (1-indexed)".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "column_id",
                3,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Stable column ID (Parquet field_id)".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "data_type",
                4,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Column data type".to_string()),
            ),
            ColumnDefinition::new(
                5,
                "is_nullable",
                5,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether NULL values are allowed".to_string()),
            ),
            ColumnDefinition::new(
                6,
                "is_primary_key",
                6,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether column is part of primary key".to_string()),
            ),
            ColumnDefinition::new(
                7,
                "column_default",
                7,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Default value expression".to_string()),
            ),
            ColumnDefinition::new(
                8,
                "column_comment",
                8,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Column description/comment".to_string()),
            ),
            ColumnDefinition::new(
                9,
                "schema_version",
                9,
                KalamDataType::Int,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Schema version when column was added (1 = original)".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Describe.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Virtual view for DESCRIBE TABLE functionality".to_string()),
        )
        .expect("Failed to create system.describe view definition")
    }

    /// Create a new describe view without a callback (placeholder mode)
    pub fn new() -> Self {
        Self { callback: None }
    }

    /// Create a new describe view with a callback
    pub fn with_callback(callback: DescribeCallback) -> Self {
        Self {
            callback: Some(callback),
        }
    }

    /// Build a RecordBatch for the given table definition
    ///
    /// **Note**: Currently, we don't track which schema version each column was added in.
    /// For now, we use `schema_version = 1` for all columns (meaning they were all present
    /// in the original schema). To support proper version tracking, we would need to:
    /// 1. Add a `added_in_version` field to ColumnDefinition
    /// 2. Track this during ALTER TABLE ADD COLUMN operations
    /// 3. Store version history in schema_history
    pub fn build_batch(def: &TableDefinition) -> Result<RecordBatch, RegistryError> {
        let schema = describe_schema();
        let col_count = def.columns.len();

        let mut names = StringBuilder::with_capacity(col_count, 1024);
        let mut ordinals = Vec::with_capacity(col_count);
        let mut column_ids = Vec::with_capacity(col_count);
        let mut types = StringBuilder::with_capacity(col_count, 1024);
        let mut nulls = Vec::with_capacity(col_count);
        let mut pks = Vec::with_capacity(col_count);
        let mut defaults = StringBuilder::with_capacity(col_count, 1024);
        let mut comments = StringBuilder::with_capacity(col_count, 1024);
        let mut schema_versions = Vec::with_capacity(col_count);

        for c in &def.columns {
            names.append_value(&c.column_name);
            ordinals.push(c.ordinal_position as i32); // Convert u32 to i32 for Int
            column_ids.push(c.column_id as i64); // Convert u64 to i64 for BigInt
            types.append_value(c.data_type.sql_name());
            nulls.push(c.is_nullable);
            pks.push(c.is_primary_key);

            if c.default_value.is_none() {
                defaults.append_null();
            } else {
                defaults.append_value(c.default_value.to_sql());
            }

            if let Some(comment) = &c.column_comment {
                comments.append_value(comment);
            } else {
                comments.append_null();
            }

            // TODO: Track actual version when column was added
            // For now, assume all columns were in version 1 (original schema)
            schema_versions.push(1i32); // Use i32 for Int type
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(names.finish()) as ArrayRef,
                Arc::new(Int32Array::from(ordinals)) as ArrayRef,
                Arc::new(Int64Array::from(column_ids)) as ArrayRef,
                Arc::new(types.finish()) as ArrayRef,
                Arc::new(BooleanArray::from(nulls)) as ArrayRef,
                Arc::new(BooleanArray::from(pks)) as ArrayRef,
                Arc::new(defaults.finish()) as ArrayRef,
                Arc::new(comments.finish()) as ArrayRef,
                Arc::new(Int32Array::from(schema_versions)) as ArrayRef,
            ],
        )
        .map_err(|e| RegistryError::ArrowError {
            message: e.to_string(),
        })?;

        Ok(batch)
    }
}

impl Default for DescribeView {
    fn default() -> Self {
        Self::new()
    }
}

impl VirtualView for DescribeView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Describe
    }

    fn schema(&self) -> SchemaRef {
        describe_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, RegistryError> {
        // Return empty batch by default - actual implementation should be called
        // via DescribeTableHandler which has access to the specific table
        let schema = self.schema();
        Ok(RecordBatch::new_empty(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_view_definition() {
        let def = DescribeView::definition();
        assert_eq!(def.table_name.as_str(), "describe");
        assert_eq!(def.namespace_id.as_str(), "system");
        assert_eq!(def.columns.len(), 9);

        // Verify schema has all required columns
        assert_eq!(def.columns[0].column_name, "column_name");
        assert_eq!(def.columns[1].column_name, "ordinal_position");
        assert_eq!(def.columns[2].column_name, "column_id");
        assert_eq!(def.columns[3].column_name, "data_type");
        assert_eq!(def.columns[4].column_name, "is_nullable");
        assert_eq!(def.columns[5].column_name, "is_primary_key");
        assert_eq!(def.columns[6].column_name, "column_default");
        assert_eq!(def.columns[7].column_name, "column_comment");
        assert_eq!(def.columns[8].column_name, "schema_version");
    }

    #[test]
    fn test_build_batch_simple_table() {
        // Create a simple table definition
        let columns = vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
            ColumnDefinition::simple(3, "age", 3, KalamDataType::Int),
        ];

        let table_def = TableDefinition::new(
            NamespaceId::new("chat"),
            TableName::new("uploads"),
            TableType::User,
            columns,
            TableOptions::user(),
            Some("Test table".to_string()),
        )
        .unwrap();

        let batch = DescribeView::build_batch(&table_def).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 9);

        // Verify column names
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "id");
        assert_eq!(names.value(1), "name");
        assert_eq!(names.value(2), "age");
    }
}
