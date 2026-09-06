//! Ordinal nested row codec.

mod decode;
mod encode;
mod from_table;
mod metadata;
mod scalar;
mod schema;
mod value;

pub use decode::{decode_row_fields, decode_shared_row, decode_stream_row, decode_user_row};
pub use encode::{encode_row_fields, encode_shared_row, encode_stream_row, encode_user_row};
pub use from_table::{
    storage_data_type_from_arrow, storage_data_type_from_kalam, storage_schema_from_table,
};
pub use metadata::{decode_row_metadata, RowMetadata};
pub use schema::{StorageDataType, StorageField, StorageSchema};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, ListArray, StringArray, StructArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };
    use datafusion_common::ScalarValue;
    use kalamdb_commons::{
        ids::SeqId,
        models::{
            rows::{Row, StreamTableRow, UserTableRow},
            UserId,
        },
    };

    use super::*;
    use crate::error::SerializationError;

    fn customer_type() -> StorageDataType {
        StorageDataType::Struct(vec![
            StorageField::new("id", StorageDataType::Int64),
            StorageField::new("name", StorageDataType::Utf8),
        ])
    }

    fn orders_schema() -> StorageSchema {
        StorageSchema::new(
            1,
            vec![
                StorageField::new("id", StorageDataType::Int64),
                StorageField::new("customer", customer_type()),
                StorageField::new("tags", StorageDataType::List(Box::new(StorageDataType::Utf8))),
            ],
        )
    }

    fn struct_scalar(id: i64, name: &str) -> ScalarValue {
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(id)])) as arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some(name)])) as arrow::array::ArrayRef,
            ),
        ]);
        ScalarValue::Struct(Arc::new(struct_array))
    }

    fn utf8_list(values: &[&str]) -> ScalarValue {
        let items: Vec<ScalarValue> = values
            .iter()
            .map(|value| ScalarValue::Utf8(Some((*value).to_string())))
            .collect();
        if items.is_empty() {
            let field = Arc::new(Field::new("item", DataType::Utf8, true));
            let list = ListArray::try_new(
                field,
                OffsetBuffer::from_lengths([0]),
                Arc::new(StringArray::from(Vec::<String>::new())),
                None,
            )
            .unwrap();
            return ScalarValue::List(Arc::new(list));
        }
        ScalarValue::List(ScalarValue::new_list(&items, &DataType::Utf8, true))
    }

    fn sample_row() -> UserTableRow {
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(9)));
        values.insert("customer".to_string(), struct_scalar(7, "ada"));
        values.insert("tags".to_string(), utf8_list(&["vip", "west"]));
        UserTableRow {
            user_id:     UserId::new("user-1"),
            _seq:        SeqId::from_i64(100),
            _commit_seq: 3,
            _deleted:    false,
            fields:      Row { values },
        }
    }

    #[test]
    fn nested_struct_and_list_roundtrip_without_string_fallback() {
        let schema = orders_schema();
        let row = sample_row();
        let encoded = encode_user_row(&row, &schema).unwrap();
        assert_eq!(&encoded.as_slice()[0..4], b"KOBJ");
        let raw = encoded.as_slice();
        assert!(
            !raw.windows(b"{\"name\"".len()).any(|w| w == b"{\"name\""),
            "nested struct must not be JSON-encoded"
        );
        let decoded =
            decode_user_row(encoded.as_slice(), &schema, row.user_id.clone(), row._seq).unwrap();
        assert_eq!(decoded.user_id, row.user_id);
        assert_eq!(decoded._seq, row._seq);
        assert_eq!(decoded._commit_seq, row._commit_seq);
        assert_eq!(decoded.fields.values.get("id"), row.fields.values.get("id"));
        assert_eq!(decoded.fields.values.get("customer"), row.fields.values.get("customer"));
        assert_eq!(decoded.fields.values.get("tags"), row.fields.values.get("tags"));
    }

    #[test]
    fn raft_field_payload_roundtrips_without_flexbuffers_names() {
        let schema = orders_schema();
        let row = sample_row();
        let encoded = encode_row_fields(&row.fields, &schema).unwrap();
        assert!(
            !encoded.windows(b"customer".len()).any(|w| w == b"customer"),
            "ordinal field payload must not store column names"
        );
        let decoded = decode_row_fields(&encoded, &schema).unwrap();
        assert_eq!(decoded.values.get("id"), row.fields.values.get("id"));
        assert_eq!(decoded.values.get("customer"), row.fields.values.get("customer"));
        assert_eq!(decoded.values.get("tags"), row.fields.values.get("tags"));
    }

    #[test]
    fn additive_nullable_struct_field_decodes_as_null() {
        let old_schema =
            StorageSchema::new(1, vec![StorageField::new("customer", customer_type())]);
        let mut values = std::collections::BTreeMap::new();
        values.insert("customer".to_string(), struct_scalar(1, "lin"));
        let row = UserTableRow {
            user_id:     UserId::new("user-2"),
            _seq:        SeqId::from_i64(1),
            _commit_seq: 1,
            _deleted:    false,
            fields:      Row { values },
        };
        let encoded = encode_user_row(&row, &old_schema).unwrap();

        let mut wider_fields = match customer_type() {
            StorageDataType::Struct(fields) => fields,
            _ => unreachable!(),
        };
        wider_fields.push(StorageField::new("username", StorageDataType::Utf8));
        let new_schema = StorageSchema::new(
            1,
            vec![StorageField::new(
                "customer",
                StorageDataType::Struct(wider_fields),
            )],
        );
        let decoded =
            decode_user_row(encoded.as_slice(), &new_schema, row.user_id.clone(), row._seq)
                .unwrap();
        let ScalarValue::Struct(struct_array) = decoded.fields.values.get("customer").unwrap()
        else {
            panic!("expected struct");
        };
        assert_eq!(struct_array.num_columns(), 3);
        assert!(struct_array.column(2).is_null(0));
    }

    #[test]
    fn metadata_decode_skips_nested_columns() {
        let encoded = encode_user_row(&sample_row(), &orders_schema()).unwrap();
        let raw = encoded.as_slice();
        assert!(
            !raw.windows(b"user-1".len()).any(|w| w == b"user-1"),
            "user_id must not be stored in the row value"
        );
        let meta = decode_row_metadata(encoded.as_slice()).unwrap();
        assert_eq!(meta.commit_seq, 3);
        assert!(!meta.deleted);
    }

    #[test]
    fn unsupported_value_does_not_use_string_fallback() {
        let schema = StorageSchema::new(1, vec![StorageField::new("id", StorageDataType::Int64)]);
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::IntervalYearMonth(Some(1)));
        let row = UserTableRow {
            user_id:     UserId::new("user-3"),
            _seq:        SeqId::from_i64(1),
            _commit_seq: 1,
            _deleted:    false,
            fields:      Row { values },
        };
        let err = encode_user_row(&row, &schema).unwrap_err();
        assert!(matches!(err, SerializationError::Encode(_)));
        assert!(!err.to_string().contains("fallback"));
    }

    #[test]
    fn shared_and_stream_rows_roundtrip_without_identity() {
        let schema = StorageSchema::new(1, vec![StorageField::new("id", StorageDataType::Int64)]);
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(42)));
        let fields = Row { values };

        let encoded = encode_shared_row(9, true, &fields, &schema).unwrap();
        let (seq, commit_seq, deleted, decoded_fields) =
            decode_shared_row(encoded.as_slice(), &schema, SeqId::from_i64(77)).unwrap();
        assert_eq!(seq, SeqId::from_i64(77));
        assert_eq!(commit_seq, 9);
        assert!(deleted);
        assert_eq!(decoded_fields.values.get("id"), fields.values.get("id"));

        let stream = StreamTableRow {
            user_id: UserId::new("owner"),
            _seq:    SeqId::from_i64(5),
            fields:  fields.clone(),
        };
        let encoded = encode_stream_row(&stream, &schema).unwrap();
        let decoded = decode_stream_row(
            encoded.as_slice(),
            &schema,
            UserId::new("owner"),
            SeqId::from_i64(5),
        )
        .unwrap();
        assert_eq!(decoded.user_id.as_str(), "owner");
        assert_eq!(decoded._seq, SeqId::from_i64(5));
        assert_eq!(decoded.fields.values.get("id"), fields.values.get("id"));
    }

    #[test]
    fn extra_stored_ordinals_are_skipped() {
        let wide = StorageSchema::new(
            1,
            vec![
                StorageField::new("id", StorageDataType::Int64),
                StorageField::new("gone", StorageDataType::Utf8),
            ],
        );
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        values.insert("gone".to_string(), ScalarValue::Utf8(Some("drop-me".to_string())));
        let row = UserTableRow {
            user_id:     UserId::new("user-x"),
            _seq:        SeqId::from_i64(1),
            _commit_seq: 1,
            _deleted:    false,
            fields:      Row { values },
        };
        let encoded = encode_user_row(&row, &wide).unwrap();
        let narrow = StorageSchema::new(1, vec![StorageField::new("id", StorageDataType::Int64)]);
        let decoded =
            decode_user_row(encoded.as_slice(), &narrow, UserId::new("user-x"), SeqId::from_i64(1))
                .unwrap();
        assert_eq!(decoded.fields.values.get("id"), Some(&ScalarValue::Int64(Some(1))));
        assert!(!decoded.fields.values.contains_key("gone"));
    }

    #[test]
    fn stored_int32_decodes_after_alter_to_int64() {
        let int32_schema = StorageSchema::new(
            1,
            vec![
                StorageField::new("id", StorageDataType::Int64),
                StorageField::new("quantity", StorageDataType::Int32),
            ],
        );
        let mut values = std::collections::BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(2001)));
        values.insert("quantity".to_string(), ScalarValue::Int32(Some(5)));
        let row = UserTableRow {
            user_id:     UserId::new("user-x"),
            _seq:        SeqId::from_i64(1),
            _commit_seq: 1,
            _deleted:    false,
            fields:      Row { values },
        };
        let encoded = encode_user_row(&row, &int32_schema).unwrap();
        let int64_schema = StorageSchema::new(
            2,
            vec![
                StorageField::new("id", StorageDataType::Int64),
                StorageField::new("quantity", StorageDataType::Int64),
            ],
        );
        let decoded = decode_user_row(
            encoded.as_slice(),
            &int64_schema,
            UserId::new("user-x"),
            SeqId::from_i64(1),
        )
        .unwrap();
        assert_eq!(decoded.fields.values.get("quantity"), Some(&ScalarValue::Int64(Some(5))));
    }
}
