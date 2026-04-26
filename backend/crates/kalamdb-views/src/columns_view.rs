//! system.columns virtual view
//!
//! **Type**: Virtual View (computed from system.schemas)
//!
//! Provides column metadata for all tables in the system.
//! Similar to information_schema.columns but includes KalamDB-specific metadata.
//!
//! **Schema**: namespace_id, table_name, version, column_id, column_name, comment,
//!             data_type, default_value, ordinal, nullable, primary_key, primary_key_pos
//!
//! **DataFusion Pattern**: Implements VirtualView trait for consistent view behavior

use std::sync::{Arc, OnceLock};

use datafusion::arrow::{
    array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, SystemTable, TableName,
};
use kalamdb_system::SystemTablesRegistry;

use crate::{error::RegistryError, view_base::VirtualView};

/// Get the columns view schema (memoized)
fn columns_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            ColumnsView::definition()
                .to_arrow_schema()
                .expect("Failed to convert columns view TableDefinition to Arrow schema")
        })
        .clone()
}

/// Virtual view that computes column metadata from system.schemas
pub struct ColumnsView {
    /// Reference to system tables registry for accessing system.schemas
    system_registry: Arc<SystemTablesRegistry>,
}

impl std::fmt::Debug for ColumnsView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnsView").finish()
    }
}

impl ColumnsView {
    /// Get the TableDefinition for system.columns view
    ///
    /// Schema:
    /// - namespace_id TEXT NOT NULL
    /// - table_name TEXT NOT NULL
    /// - version INT NOT NULL
    /// - column_id INT NOT NULL
    /// - column_name TEXT NOT NULL
    /// - comment TEXT (nullable)
    /// - data_type TEXT NOT NULL (KalamDataType name)
    /// - default_value TEXT (nullable)
    /// - ordinal INT NOT NULL
    /// - nullable BOOLEAN NOT NULL
    /// - primary_key BOOLEAN NOT NULL
    /// - primary_key_pos INT (nullable, position in PK if is PK)
    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "namespace_id",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Namespace containing this table".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "table_name",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Table name within namespace".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "version",
                3,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Schema version this column belongs to".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "column_id",
                4,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Unique column identifier within table".to_string()),
            ),
            ColumnDefinition::new(
                5,
                "column_name",
                5,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Column name".to_string()),
            ),
            ColumnDefinition::new(
                6,
                "comment",
                6,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Column comment/description".to_string()),
            ),
            ColumnDefinition::new(
                7,
                "data_type",
                7,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("KalamDataType of the column".to_string()),
            ),
            ColumnDefinition::new(
                8,
                "default_value",
                8,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Default value expression (if any)".to_string()),
            ),
            ColumnDefinition::new(
                9,
                "ordinal",
                9,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Column position in table (1-based)".to_string()),
            ),
            ColumnDefinition::new(
                10,
                "nullable",
                10,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether column accepts NULL values".to_string()),
            ),
            ColumnDefinition::new(
                11,
                "primary_key",
                11,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether column is part of primary key".to_string()),
            ),
            ColumnDefinition::new(
                12,
                "primary_key_pos",
                12,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Position in primary key (1-based, NULL if not PK)".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Columns.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Column metadata view (computed from system.schemas)".to_string()),
        )
        .expect("Failed to create system.columns view definition")
    }

    /// Create a new columns view
    pub fn new(system_registry: Arc<SystemTablesRegistry>) -> Self {
        Self { system_registry }
    }
}

impl VirtualView for ColumnsView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Columns
    }

    fn schema(&self) -> SchemaRef {
        columns_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, RegistryError> {
        // Build columns from system table definitions
        let mut namespace_ids = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut versions = Int64Builder::new();
        let mut column_ids = Int64Builder::new();
        let mut column_names = StringBuilder::new();
        let mut comments = StringBuilder::new();
        let mut data_types = StringBuilder::new();
        let mut default_values = StringBuilder::new();
        let mut ordinals = Int64Builder::new();
        let mut nullables = BooleanBuilder::new();
        let mut primary_keys = BooleanBuilder::new();
        let mut primary_key_positions = Int64Builder::new();

        // Add columns from all persisted table definitions in system.schemas.
        let persisted_tables = self.system_registry.tables().list_tables().map_err(|e| {
            RegistryError::Other(format!("Failed to list persisted table definitions: {}", e))
        })?;

        for table_def in persisted_tables {
            for col in &table_def.columns {
                namespace_ids.append_value(table_def.namespace_id.as_str());
                table_names.append_value(table_def.table_name.as_str());
                versions.append_value(table_def.schema_version as i64);
                column_ids.append_value(col.column_id as i64);
                column_names.append_value(&col.column_name);

                if let Some(comment) = &col.column_comment {
                    comments.append_value(comment);
                } else {
                    comments.append_null();
                }

                data_types.append_value(col.data_type.to_string());

                // Format default value
                match &col.default_value {
                    ColumnDefault::None => default_values.append_null(),
                    ColumnDefault::Literal(lit) => {
                        default_values.append_value(format!("{:?}", lit))
                    },
                    ColumnDefault::FunctionCall { name, args } => {
                        if args.is_empty() {
                            default_values.append_value(format!("{}()", name));
                        } else {
                            default_values.append_value(format!("{}({:?})", name, args));
                        }
                    },
                }

                ordinals.append_value(col.ordinal_position as i64);
                nullables.append_value(col.is_nullable);
                primary_keys.append_value(col.is_primary_key);

                if col.is_primary_key {
                    // For multi-column PKs, we'd need to track position
                    // For now, assume single-column PK gets position 1
                    primary_key_positions.append_value(1);
                } else {
                    primary_key_positions.append_null();
                }
            }
        }

        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(namespace_ids.finish()) as ArrayRef,
                Arc::new(table_names.finish()) as ArrayRef,
                Arc::new(versions.finish()) as ArrayRef,
                Arc::new(column_ids.finish()) as ArrayRef,
                Arc::new(column_names.finish()) as ArrayRef,
                Arc::new(comments.finish()) as ArrayRef,
                Arc::new(data_types.finish()) as ArrayRef,
                Arc::new(default_values.finish()) as ArrayRef,
                Arc::new(ordinals.finish()) as ArrayRef,
                Arc::new(nullables.finish()) as ArrayRef,
                Arc::new(primary_keys.finish()) as ArrayRef,
                Arc::new(primary_key_positions.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| RegistryError::Other(format!("Failed to build columns view batch: {}", e)))
    }
}

// Re-export as ColumnsViewTableProvider
pub type ColumnsViewTableProvider = crate::view_base::ViewTableProvider<ColumnsView>;

/// Helper function to create a columns view provider
pub fn create_columns_view_provider(
    system_registry: Arc<SystemTablesRegistry>,
) -> ColumnsViewTableProvider {
    ColumnsViewTableProvider::new(Arc::new(ColumnsView::new(system_registry)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_columns_view_definition() {
        let def = ColumnsView::definition();
        assert_eq!(def.table_name.as_str(), "columns");
        assert_eq!(def.columns.len(), 12);
    }

    #[test]
    fn test_columns_view_schema() {
        let schema = columns_schema();
        assert_eq!(schema.fields().len(), 12);
    }
}
