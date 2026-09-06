//! Schema-guided mapping between typed catalog/system models and [`Row`] values.
//!
//! Primitive columns encode as [`ScalarValue`]s. JSON is used only for `Json` /
//! `File` document columns.

mod decode;
mod encode;
mod timestamp;

use kalamdb_commons::{models::rows::Row, schemas::TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
pub use timestamp::{model_ms_to_storage_micros, storage_micros_to_model_ms};

use crate::error::{Result, SerializationError};

/// Serialize a typed model into a schema-aligned row.
pub fn model_to_row<T: Serialize>(model: &T, table_def: &TableDefinition) -> Result<Row> {
    encode::serialize_model(model, table_def)
}

/// Inverse of [`model_to_row`].
pub fn row_to_model<T: DeserializeOwned>(row: &Row, table_def: &TableDefinition) -> Result<T> {
    decode::deserialize_model(row, table_def)
}

fn map_ser(message: String) -> SerializationError {
    SerializationError::Encode(message)
}

fn map_de(message: String) -> SerializationError {
    SerializationError::Decode(message)
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use kalamdb_commons::{
        models::datatypes::KalamDataType,
        schemas::{ColumnDefinition, TableDefinition, TableOptions, TableType},
        NamespaceId, TableName,
    };
    use serde::{Deserialize, Serialize};

    use super::{model_to_row, row_to_model};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Sample {
        id:         String,
        count:      i32,
        flag:       bool,
        created_at: i64,
        note:       Option<String>,
        tags:       Vec<String>,
    }

    fn sample_table() -> TableDefinition {
        TableDefinition::new(
            NamespaceId::system(),
            TableName::new("samples"),
            TableType::System,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "count", 2, KalamDataType::Int),
                ColumnDefinition::simple(3, "flag", 3, KalamDataType::Boolean),
                ColumnDefinition::simple(4, "created_at", 4, KalamDataType::Timestamp),
                ColumnDefinition::simple(5, "note", 5, KalamDataType::Text),
                ColumnDefinition::simple(6, "tags", 6, KalamDataType::Json),
            ],
            TableOptions::system(),
            None,
        )
        .expect("table definition")
    }

    #[test]
    fn primitive_columns_roundtrip_without_document_json() {
        let model = Sample {
            id:         "a1".to_string(),
            count:      7,
            flag:       true,
            created_at: 12_345,
            note:       None,
            tags:       vec!["x".to_string(), "y".to_string()],
        };
        let table = sample_table();
        let row = model_to_row(&model, &table).expect("encode");

        assert!(matches!(row.values.get("count"), Some(ScalarValue::Int32(Some(7)))));
        assert!(matches!(row.values.get("flag"), Some(ScalarValue::Boolean(Some(true)))));
        assert!(matches!(
            row.values.get("created_at"),
            Some(ScalarValue::TimestampMicrosecond(Some(12_345), None))
        ));
        assert!(matches!(row.values.get("note"), Some(ScalarValue::Null)));
        match row.values.get("tags") {
            Some(ScalarValue::Utf8(Some(json))) => assert_eq!(json, r#"["x","y"]"#),
            other => panic!("expected utf8 json tags, got {other:?}"),
        }

        let decoded: Sample = row_to_model(&row, &table).expect("decode");
        assert_eq!(decoded, model);
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "lowercase")]
    enum SampleCommand {
        Select,
        Insert,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct SampleEnumRow {
        id:      String,
        command: SampleCommand,
    }

    fn enum_table() -> TableDefinition {
        TableDefinition::new(
            NamespaceId::system(),
            TableName::new("sample_enums"),
            TableType::System,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "command", 2, KalamDataType::Json),
            ],
            TableOptions::system(),
            None,
        )
        .expect("table definition")
    }

    #[test]
    fn json_column_unit_enum_roundtrips() {
        let model = SampleEnumRow {
            id:      "p1".to_string(),
            command: SampleCommand::Select,
        };
        let table = enum_table();
        let row = model_to_row(&model, &table).expect("encode");
        let decoded: SampleEnumRow = row_to_model(&row, &table).expect("decode");
        assert_eq!(decoded, model);
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "snake_case")]
    enum SampleProgram {
        RowLocal { column: String },
        Always,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct SampleProgramRow {
        id:      String,
        program: SampleProgram,
    }

    #[test]
    fn json_column_tagged_enum_roundtrips() {
        let table = TableDefinition::new(
            NamespaceId::system(),
            TableName::new("sample_programs"),
            TableType::System,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "program", 2, KalamDataType::Json),
            ],
            TableOptions::system(),
            None,
        )
        .expect("table definition");
        let model = SampleProgramRow {
            id:      "p2".to_string(),
            program: SampleProgram::RowLocal {
                column: "owner_id".to_string(),
            },
        };
        let row = model_to_row(&model, &table).expect("encode");
        let decoded: SampleProgramRow = row_to_model(&row, &table).expect("decode");
        assert_eq!(decoded, model);
    }
}
