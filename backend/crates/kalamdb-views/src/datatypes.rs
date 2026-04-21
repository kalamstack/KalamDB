//! system.datatypes virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides a mapping between Arrow data types and KalamDB SQL types.
//! This enables the UI to display user-friendly type names (TEXT, BIGINT, etc.)
//! instead of Arrow internal types (Utf8, Int64, etc.).
//!
//! **DataFusion Pattern**: Implements `VirtualView` for the shared deferred
//! execution path
//! - Static mapping computed once at startup
//! - No persistent state in RocksDB
//! - Batch construction is deferred until execution time
//!
//! **Schema Caching**: Memoized via `OnceLock`
//! **Schema**: TableDefinition provides consistent metadata for views

use crate::view_base::VirtualView;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::schemas::{
    ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType,
};
use kalamdb_commons::{NamespaceId, TableName};
use kalamdb_system::SystemTable;
use std::sync::{Arc, OnceLock};

/// Get or initialize the datatypes schema (memoized)
fn datatypes_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            DatatypesView::definition()
                .to_arrow_schema()
                .expect("Failed to convert datatypes TableDefinition to Arrow schema")
        })
        .clone()
}

/// Virtual view that provides Arrow → KalamDB type mappings
///
/// **DataFusion Design**:
/// - Implements VirtualView trait
/// - Returns TableType::View
/// - Static data computed once (type mappings don't change at runtime)
#[derive(Debug)]
pub struct DatatypesView;

impl DatatypesView {
    /// Get the TableDefinition for system.datatypes view
    ///
    /// Schema:
    /// - arrow_type TEXT NOT NULL (Arrow internal type name)
    /// - kalam_type TEXT NOT NULL (KalamDB type name)
    /// - sql_name TEXT NOT NULL (SQL type name for display)
    /// - description TEXT NOT NULL (Human-readable description)
    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "arrow_type",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Arrow internal type name (e.g., Utf8, Int64)".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "kalam_type",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("KalamDB type name (e.g., Text, BigInt)".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "sql_name",
                3,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("SQL type name for display (e.g., TEXT, BIGINT)".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "description",
                4,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Human-readable description of the data type".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Datatypes.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Arrow to KalamDB SQL type mappings (read-only view)".to_string()),
        )
        .expect("Failed to create system.datatypes view definition")
    }

    /// Create a new datatypes view
    pub fn new() -> Self {
        Self
    }

    /// Get all Arrow → KalamDB type mappings
    ///
    /// Returns tuples of (arrow_type_pattern, kalam_type, sql_name, description)
    fn get_mappings() -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
        vec![
            // Boolean
            ("Boolean", "Boolean", "BOOLEAN", "True/false boolean value"),
            // Integer types
            ("Int8", "SmallInt", "TINYINT", "8-bit signed integer"),
            ("Int16", "SmallInt", "SMALLINT", "16-bit signed integer"),
            ("Int32", "Int", "INT", "32-bit signed integer"),
            ("Int64", "BigInt", "BIGINT", "64-bit signed integer"),
            ("UInt8", "Int", "TINYINT UNSIGNED", "8-bit unsigned integer"),
            ("UInt16", "Int", "SMALLINT UNSIGNED", "16-bit unsigned integer"),
            ("UInt32", "BigInt", "INT UNSIGNED", "32-bit unsigned integer"),
            ("UInt64", "BigInt", "BIGINT UNSIGNED", "64-bit unsigned integer"),
            // Floating point
            ("Float32", "Float", "FLOAT", "32-bit floating point"),
            ("Float64", "Double", "DOUBLE", "64-bit floating point"),
            // String types
            ("Utf8", "Text", "TEXT", "Variable-length UTF-8 string"),
            ("LargeUtf8", "Text", "TEXT", "Large variable-length UTF-8 string"),
            // Binary types
            ("Binary", "Bytes", "BYTES", "Variable-length binary data"),
            ("LargeBinary", "Bytes", "BYTES", "Large variable-length binary data"),
            // Temporal types - Timestamps
            (
                "Timestamp(Microsecond, None)",
                "Timestamp",
                "TIMESTAMP",
                "Timestamp with microsecond precision",
            ),
            (
                "Timestamp(Millisecond, None)",
                "Timestamp",
                "TIMESTAMP",
                "Timestamp with millisecond precision",
            ),
            (
                "Timestamp(Nanosecond, None)",
                "Timestamp",
                "TIMESTAMP",
                "Timestamp with nanosecond precision",
            ),
            (
                "Timestamp(Second, None)",
                "Timestamp",
                "TIMESTAMP",
                "Timestamp with second precision",
            ),
            // DateTime with timezone
            (
                "Timestamp(Microsecond, Some(\"UTC\"))",
                "DateTime",
                "DATETIME",
                "DateTime with timezone (UTC)",
            ),
            (
                "Timestamp(Millisecond, Some(\"UTC\"))",
                "DateTime",
                "DATETIME",
                "DateTime with timezone (UTC)",
            ),
            // Date types
            ("Date32", "Date", "DATE", "Date (days since epoch)"),
            ("Date64", "Date", "DATE", "Date (milliseconds since epoch)"),
            // Time types
            ("Time64(Microsecond)", "Time", "TIME", "Time of day with microsecond precision"),
            ("Time32(Second)", "Time", "TIME", "Time of day with second precision"),
            ("Time32(Millisecond)", "Time", "TIME", "Time of day with millisecond precision"),
            // Special types
            ("FixedSizeBinary(16)", "Uuid", "UUID", "128-bit universally unique identifier"),
            // Decimal (pattern-based)
            ("Decimal128", "Decimal", "DECIMAL", "Fixed-point decimal number"),
            // FixedSizeList for embeddings (pattern-based)
            (
                "FixedSizeList",
                "Embedding",
                "EMBEDDING",
                "Fixed-size float32 vector for ML embeddings",
            ),
        ]
    }
}

impl Default for DatatypesView {
    fn default() -> Self {
        Self::new()
    }
}

impl VirtualView for DatatypesView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Datatypes
    }

    fn schema(&self) -> SchemaRef {
        datatypes_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, crate::error::RegistryError> {
        let mut arrow_types = StringBuilder::new();
        let mut kalam_types = StringBuilder::new();
        let mut sql_names = StringBuilder::new();
        let mut descriptions = StringBuilder::new();

        for (arrow_type, kalam_type, sql_name, description) in Self::get_mappings() {
            arrow_types.append_value(arrow_type);
            kalam_types.append_value(kalam_type);
            sql_names.append_value(sql_name);
            descriptions.append_value(description);
        }

        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(arrow_types.finish()) as ArrayRef,
                Arc::new(kalam_types.finish()) as ArrayRef,
                Arc::new(sql_names.finish()) as ArrayRef,
                Arc::new(descriptions.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            crate::error::RegistryError::Other(format!("Failed to build datatypes batch: {}", e))
        })
    }
}

// Re-export as DatatypesTableProvider for consistency
pub type DatatypesTableProvider = crate::view_base::ViewTableProvider<DatatypesView>;

/// Helper function to create a datatypes table provider
pub fn create_datatypes_provider() -> DatatypesTableProvider {
    DatatypesTableProvider::new(Arc::new(DatatypesView::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatypes_schema() {
        let schema = datatypes_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "arrow_type");
        assert_eq!(schema.field(1).name(), "kalam_type");
        assert_eq!(schema.field(2).name(), "sql_name");
        assert_eq!(schema.field(3).name(), "description");
    }

    #[test]
    fn test_datatypes_view_compute() {
        let view = DatatypesView::new();
        let batch = view.compute_batch().expect("compute batch");
        assert!(batch.num_rows() > 0, "Should have at least one type mapping");
        assert_eq!(batch.num_columns(), 4);
        println!("✅ DatatypesView returned {} type mappings", batch.num_rows());
    }

    #[test]
    fn test_table_provider() {
        let view = Arc::new(DatatypesView::new());
        let provider = DatatypesTableProvider::new(view);
        use datafusion::datasource::TableProvider;
        use datafusion::datasource::TableType;

        assert_eq!(provider.table_type(), TableType::View);
        assert_eq!(provider.schema().fields().len(), 4);
    }
}
