use std::sync::OnceLock;

use datafusion::arrow::datatypes::SchemaRef;
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, SystemTable, TableName,
};

pub fn manifest_table_definition() -> TableDefinition {
    let columns = vec![
        ColumnDefinition::new(
            1,
            "cache_key",
            1,
            KalamDataType::Text,
            false,
            true,
            false,
            ColumnDefault::None,
            Some("Cache key identifier (format: namespace:table:scope)".to_string()),
        ),
        ColumnDefinition::new(
            2,
            "namespace_id",
            2,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Namespace containing the table".to_string()),
        ),
        ColumnDefinition::new(
            3,
            "table_name",
            3,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Table name".to_string()),
        ),
        ColumnDefinition::new(
            4,
            "scope",
            4,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Scope: user_id for USER tables, 'shared' for SHARED tables".to_string()),
        ),
        ColumnDefinition::new(
            5,
            "etag",
            5,
            KalamDataType::Text,
            true,
            false,
            false,
            ColumnDefault::None,
            Some("Storage ETag or version identifier".to_string()),
        ),
        ColumnDefinition::new(
            6,
            "last_refreshed",
            6,
            KalamDataType::Timestamp,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Last successful cache refresh timestamp".to_string()),
        ),
        ColumnDefinition::new(
            7,
            "last_accessed",
            7,
            KalamDataType::Timestamp,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Last access timestamp (in-memory tracking)".to_string()),
        ),
        ColumnDefinition::new(
            8,
            "in_memory",
            8,
            KalamDataType::Boolean,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("True if manifest is currently in hot cache (RAM)".to_string()),
        ),
        ColumnDefinition::new(
            9,
            "sync_state",
            9,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Synchronization state: in_sync, stale, error".to_string()),
        ),
        ColumnDefinition::new(
            10,
            "manifest_json",
            10,
            KalamDataType::Json,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Serialized Manifest object as JSON".to_string()),
        ),
    ];

    TableDefinition::new(
        NamespaceId::system(),
        TableName::new(SystemTable::Manifest.table_name()),
        TableType::System,
        columns,
        TableOptions::system(),
        Some("Manifest cache entries for query optimization".to_string()),
    )
    .expect("Failed to create system.manifest table definition")
}

pub fn manifest_arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            manifest_table_definition()
                .to_arrow_schema()
                .expect("Failed to convert manifest table definition to Arrow schema")
        })
        .clone()
}
