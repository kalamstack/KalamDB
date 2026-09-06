//! Build [`StorageSchema`] from catalog table definitions.

use arrow::datatypes::DataType;
use kalamdb_commons::{
    constants::SystemColumnNames,
    models::datatypes::KalamDataType,
    schemas::{TableDefinition, TableType},
};

use super::schema::{StorageDataType, StorageField, StorageSchema};
use crate::error::{Result, SerializationError};

fn is_key_or_envelope_column(table: &TableDefinition, name: &str) -> bool {
    if SystemColumnNames::is_system_column(name) {
        return true;
    }
    // STREAM identity `user_id` lives on the RocksDB key and is injected on scan.
    // USER tables may declare `user_id` as a regular payload column (owner identity
    // is still reconstructed onto `UserTableRow.user_id` from the key).
    // SHARED `user_id` is always payload (membership principal).
    name == "user_id" && matches!(table.table_type, TableType::Stream)
}

/// Map a catalog [`KalamDataType`] to a storage type.
pub fn storage_data_type_from_kalam(data_type: &KalamDataType) -> Result<StorageDataType> {
    match data_type {
        KalamDataType::Boolean => Ok(StorageDataType::Boolean),
        KalamDataType::SmallInt => Ok(StorageDataType::Int16),
        KalamDataType::Int => Ok(StorageDataType::Int32),
        KalamDataType::BigInt => Ok(StorageDataType::Int64),
        KalamDataType::Float => Ok(StorageDataType::Float32),
        KalamDataType::Double => Ok(StorageDataType::Float64),
        KalamDataType::Text | KalamDataType::Json | KalamDataType::File => {
            Ok(StorageDataType::Utf8)
        },
        KalamDataType::Bytes | KalamDataType::Uuid => Ok(StorageDataType::Binary),
        KalamDataType::Date => Ok(StorageDataType::Date32),
        KalamDataType::Time => Ok(StorageDataType::Time64Microsecond),
        KalamDataType::Timestamp | KalamDataType::DateTime => {
            Ok(StorageDataType::TimestampMicrosecond)
        },
        KalamDataType::Decimal { precision, scale } => {
            let scale = i8::try_from(*scale).map_err(|_| {
                SerializationError::Encode(format!("decimal scale {scale} does not fit in i8"))
            })?;
            Ok(StorageDataType::Decimal {
                precision: *precision,
                scale,
            })
        },
        KalamDataType::Embedding(dimension) => Ok(StorageDataType::Embedding {
            dimension: i32::from(*dimension),
        }),
    }
}

/// Map Arrow types, including nested STRUCT/List, to storage types.
pub fn storage_data_type_from_arrow(data_type: &DataType) -> Result<StorageDataType> {
    match data_type {
        DataType::Boolean => Ok(StorageDataType::Boolean),
        DataType::Int8 => Ok(StorageDataType::Int8),
        DataType::Int16 => Ok(StorageDataType::Int16),
        DataType::Int32 => Ok(StorageDataType::Int32),
        DataType::Int64 => Ok(StorageDataType::Int64),
        DataType::UInt8 => Ok(StorageDataType::UInt8),
        DataType::UInt16 => Ok(StorageDataType::UInt16),
        DataType::UInt32 => Ok(StorageDataType::UInt32),
        DataType::UInt64 => Ok(StorageDataType::UInt64),
        DataType::Float32 => Ok(StorageDataType::Float32),
        DataType::Float64 => Ok(StorageDataType::Float64),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(StorageDataType::Utf8),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Ok(StorageDataType::Binary)
        },
        DataType::Date32 => Ok(StorageDataType::Date32),
        DataType::Time64(_) => Ok(StorageDataType::Time64Microsecond),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            Ok(StorageDataType::TimestampMillisecond)
        },
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            Ok(StorageDataType::TimestampMicrosecond)
        },
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            Ok(StorageDataType::TimestampNanosecond)
        },
        DataType::Timestamp(_, _) => Ok(StorageDataType::TimestampMicrosecond),
        DataType::Decimal128(precision, scale) => Ok(StorageDataType::Decimal {
            precision: *precision,
            scale:     *scale,
        }),
        DataType::FixedSizeList(field, dimension) if field.data_type() == &DataType::Float32 => {
            Ok(StorageDataType::Embedding {
                dimension: *dimension,
            })
        },
        DataType::Struct(fields) => {
            let mut nested = Vec::with_capacity(fields.len());
            for field in fields {
                nested.push(StorageField::new(
                    field.name(),
                    storage_data_type_from_arrow(field.data_type())?,
                ));
            }
            Ok(StorageDataType::Struct(nested))
        },
        DataType::List(field) | DataType::LargeList(field) => Ok(StorageDataType::List(Box::new(
            storage_data_type_from_arrow(field.data_type())?,
        ))),
        other => Err(SerializationError::Encode(format!("unsupported storage type {other:?}"))),
    }
}

/// Build a storage schema from a table definition.
///
/// Physical slots are `column_id` 1..=max among non-identity columns. Missing ids are
/// dropped holes so a later column keeps its slot after an earlier DROP.
pub fn storage_schema_from_table(table: &TableDefinition) -> Result<StorageSchema> {
    let version = u16::try_from(table.schema_version).map_err(|_| {
        SerializationError::Encode(format!(
            "schema version {} does not fit in u16",
            table.schema_version
        ))
    })?;

    let mut live = Vec::new();
    for column in &table.columns {
        if is_key_or_envelope_column(table, &column.column_name) {
            continue;
        }
        if column.column_id == 0 {
            return Err(SerializationError::Encode("column_id 0 is reserved".to_string()));
        }
        live.push(column);
    }
    live.sort_by_key(|column| column.column_id);

    let max_id = live.last().map(|column| column.column_id).unwrap_or(0);
    let mut by_id = std::collections::BTreeMap::new();
    for column in live {
        by_id.insert(column.column_id, column);
    }

    let mut fields = Vec::new();
    for id in 1..=max_id {
        match by_id.get(&id) {
            Some(column) => {
                fields.push(StorageField::new(
                    column.column_name.clone(),
                    storage_data_type_from_kalam(&column.data_type)?,
                ));
            },
            None => fields.push(StorageField::dropped_slot()),
        }
    }

    Ok(StorageSchema::new(version, fields))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Fields};
    use kalamdb_commons::{
        models::datatypes::KalamDataType,
        schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
        NamespaceId, TableName,
    };

    use super::*;

    fn column(id: u64, name: &str, ordinal: u32, data_type: KalamDataType) -> ColumnDefinition {
        ColumnDefinition::new(
            id,
            name,
            ordinal,
            data_type,
            true,
            false,
            false,
            ColumnDefault::None,
            None,
        )
    }

    #[test]
    fn slots_follow_column_id_with_drop_holes() {
        let table = TableDefinition::new(
            NamespaceId::new("app"),
            TableName::new("orders"),
            TableType::User,
            vec![
                column(1, "id", 1, KalamDataType::BigInt),
                column(3, "email", 2, KalamDataType::Text),
                column(4, "_seq", 3, KalamDataType::BigInt),
            ],
            TableOptions::user(),
            None,
        )
        .unwrap();
        let schema = storage_schema_from_table(&table).unwrap();
        assert_eq!(schema.version, 1);
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].name, "id");
        assert!(schema.fields[1].dropped);
        assert_eq!(schema.fields[2].name, "email");
    }

    #[test]
    fn rename_keeps_physical_slot() {
        let original = TableDefinition::new(
            NamespaceId::new("app"),
            TableName::new("t"),
            TableType::User,
            vec![column(2, "old_name", 1, KalamDataType::Text)],
            TableOptions::user(),
            None,
        )
        .unwrap();
        let renamed = TableDefinition::new(
            NamespaceId::new("app"),
            TableName::new("t"),
            TableType::User,
            vec![column(2, "new_name", 1, KalamDataType::Text)],
            TableOptions::user(),
            None,
        )
        .unwrap();
        let a = storage_schema_from_table(&original).unwrap();
        let b = storage_schema_from_table(&renamed).unwrap();
        assert_eq!(a.fields.len(), 2);
        assert!(a.fields[0].dropped);
        assert!(!a.fields[1].dropped);
        assert_eq!(a.fields[1].name, "old_name");
        assert_eq!(b.fields[1].name, "new_name");
        assert_eq!(a.fields[1].data_type, b.fields[1].data_type);
    }

    #[test]
    fn shared_table_keeps_user_id_payload_column() {
        let table = TableDefinition::new(
            NamespaceId::new("chat"),
            TableName::new("members"),
            TableType::Shared,
            vec![
                column(1, "id", 1, KalamDataType::Text),
                column(2, "user_id", 2, KalamDataType::Text),
                column(3, "group_id", 3, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .unwrap();
        let schema = storage_schema_from_table(&table).unwrap();
        let names: Vec<&str> = schema.fields.iter().map(|field| field.name.as_str()).collect();
        assert_eq!(names, vec!["id", "user_id", "group_id"]);
    }

    #[test]
    fn user_table_keeps_declared_user_id_payload_column() {
        let table = TableDefinition::new(
            NamespaceId::new("app"),
            TableName::new("profiles"),
            TableType::User,
            vec![
                column(1, "id", 1, KalamDataType::Text),
                column(2, "user_id", 2, KalamDataType::Text),
                column(3, "name", 3, KalamDataType::Text),
            ],
            TableOptions::user(),
            None,
        )
        .unwrap();
        let schema = storage_schema_from_table(&table).unwrap();
        let names: Vec<&str> = schema.fields.iter().map(|field| field.name.as_str()).collect();
        assert_eq!(names, vec!["id", "user_id", "name"]);
    }

    #[test]
    fn nested_arrow_struct_maps_to_storage_struct() {
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let mapped = storage_data_type_from_arrow(&DataType::Struct(fields)).unwrap();
        match mapped {
            StorageDataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name, "id");
                assert_eq!(fields[0].data_type, StorageDataType::Int64);
                assert_eq!(fields[1].name, "name");
                assert_eq!(fields[1].data_type, StorageDataType::Utf8);
            },
            other => panic!("expected struct, got {other:?}"),
        }
    }
}
