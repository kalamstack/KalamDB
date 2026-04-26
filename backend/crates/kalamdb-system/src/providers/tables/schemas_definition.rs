use std::sync::OnceLock;

use datafusion::arrow::datatypes::SchemaRef;
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, SystemTable, TableName,
};

pub fn schemas_table_definition() -> TableDefinition {
    let columns = vec![
        ColumnDefinition::new(
            1,
            "table_id",
            1,
            KalamDataType::Text,
            false,
            true,
            false,
            ColumnDefault::None,
            Some("Table identifier: namespace_id:table_name".to_string()),
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
            "namespace_id",
            3,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Namespace containing this table".to_string()),
        ),
        ColumnDefinition::new(
            4,
            "table_type",
            4,
            KalamDataType::Text,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Table type: USER, SHARED, STREAM, SYSTEM".to_string()),
        ),
        ColumnDefinition::new(
            5,
            "created_at",
            5,
            KalamDataType::Timestamp,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Table creation timestamp".to_string()),
        ),
        ColumnDefinition::new(
            6,
            "schema_version",
            6,
            KalamDataType::Int,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Current schema version number".to_string()),
        ),
        ColumnDefinition::new(
            7,
            "columns",
            7,
            KalamDataType::Json,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Column definitions as JSON array".to_string()),
        ),
        ColumnDefinition::new(
            8,
            "table_comment",
            8,
            KalamDataType::Text,
            true,
            false,
            false,
            ColumnDefault::None,
            Some("Optional table description or comment".to_string()),
        ),
        ColumnDefinition::new(
            9,
            "updated_at",
            9,
            KalamDataType::Timestamp,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Last modification timestamp".to_string()),
        ),
        ColumnDefinition::new(
            10,
            "options",
            10,
            KalamDataType::Json,
            true,
            false,
            false,
            ColumnDefault::None,
            Some("Serialized table options (JSON)".to_string()),
        ),
        ColumnDefinition::new(
            11,
            "access_level",
            11,
            KalamDataType::Text,
            true,
            false,
            false,
            ColumnDefault::None,
            Some("Access level for Shared tables: public, private, protected".to_string()),
        ),
        ColumnDefinition::new(
            12,
            "is_latest",
            12,
            KalamDataType::Boolean,
            false,
            false,
            false,
            ColumnDefault::None,
            Some("Whether this is the latest version of the table schema".to_string()),
        ),
        ColumnDefinition::new(
            13,
            "storage_id",
            13,
            KalamDataType::Text,
            true,
            false,
            false,
            ColumnDefault::None,
            Some("Storage backend identifier for this table".to_string()),
        ),
        ColumnDefinition::new(
            14,
            "use_user_storage",
            14,
            KalamDataType::Boolean,
            true,
            false,
            false,
            ColumnDefault::None,
            Some("Whether this table uses user-specific storage assignment".to_string()),
        ),
    ];

    TableDefinition::new(
        NamespaceId::system(),
        TableName::new(SystemTable::Schemas.table_name()),
        TableType::System,
        columns,
        TableOptions::system(),
        Some("Registry of all table schemas and their histories in the database".to_string()),
    )
    .expect("Failed to create system.schemas table definition")
}

pub fn schemas_arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            schemas_table_definition()
                .to_arrow_schema()
                .expect("Failed to convert schemas table definition to Arrow schema")
        })
        .clone()
}
