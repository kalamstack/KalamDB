use datafusion_common::ScalarValue;
use kalamdb_commons::{
    conversions::{scalar_to_f64, scalar_to_json_for_column},
    models::{datatypes::KalamDataType, rows::Row},
    schemas::{ColumnDefinition, TableDefinition},
};
use serde::de::{
    DeserializeOwned, DeserializeSeed, Deserializer, IntoDeserializer, MapAccess, Visitor,
};
use serde_json::Value;

use super::map_de;

/// Deserialize a schema-aligned row into a typed model. Timestamps must already
/// be millisecond `i64`s when this is called.
pub(super) fn deserialize_model<T: DeserializeOwned>(
    row: &Row,
    table_def: &TableDefinition,
) -> crate::error::Result<T> {
    T::deserialize(RowDeserializer { row, table_def }).map_err(|error| map_de(error.0))
}

fn stores_document(data_type: &KalamDataType) -> bool {
    matches!(data_type, KalamDataType::Json | KalamDataType::File)
}

fn is_null(scalar: &ScalarValue) -> bool {
    match scalar {
        ScalarValue::Null
        | ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::TimestampSecond(None, _)
        | ScalarValue::Time64Microsecond(None)
        | ScalarValue::Date32(None)
        | ScalarValue::Decimal128(None, _, _) => true,
        _ => false,
    }
}

#[derive(Debug)]
struct DeError(String);

impl std::fmt::Display for DeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DeError {}

impl serde::de::Error for DeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

struct RowDeserializer<'a> {
    row:       &'a Row,
    table_def: &'a TableDefinition,
}

impl<'de> Deserializer<'de> for RowDeserializer<'_> {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(RowMapAccess {
            row:     self.row,
            columns: &self.table_def.columns,
            index:   0,
        })
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf seq tuple tuple_struct enum identifier
    }
}

struct RowMapAccess<'a> {
    row:     &'a Row,
    columns: &'a [ColumnDefinition],
    index:   usize,
}

impl<'de> MapAccess<'de> for RowMapAccess<'_> {
    type Error = DeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.index >= self.columns.len() {
            return Ok(None);
        }
        let name = self.columns[self.index].column_name.as_str();
        seed.deserialize(name.into_deserializer()).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let column = &self.columns[self.index];
        self.index += 1;
        let scalar = self.row.values.get(&column.column_name).unwrap_or(&ScalarValue::Null);
        seed.deserialize(FieldDeserializer {
            scalar,
            data_type: &column.data_type,
        })
    }
}

struct FieldDeserializer<'a> {
    scalar:    &'a ScalarValue,
    data_type: &'a KalamDataType,
}

impl FieldDeserializer<'_> {
    fn document_value(&self) -> Result<Value, DeError> {
        scalar_to_json_for_column(self.scalar, self.data_type)
            .map_err(|error| DeError(format!("scalar->json conversion failed: {error}")))
    }
}

impl<'de> Deserializer<'de> for FieldDeserializer<'_> {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if stores_document(self.data_type) {
            return visit_json(self.document_value()?, visitor);
        }
        if is_null(self.scalar) {
            return visitor.visit_unit();
        }
        match self.data_type {
            KalamDataType::Boolean => {
                visitor.visit_bool(matches!(self.scalar, ScalarValue::Boolean(Some(true))))
            },
            KalamDataType::SmallInt
            | KalamDataType::Int
            | KalamDataType::BigInt
            | KalamDataType::Date
            | KalamDataType::Timestamp
            | KalamDataType::DateTime
            | KalamDataType::Time => visitor.visit_i64(model_i64(self.scalar)),
            KalamDataType::Double | KalamDataType::Float => {
                visitor.visit_f64(scalar_to_f64(self.scalar).unwrap_or(0.0))
            },
            KalamDataType::Bytes => match self.scalar {
                ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
                    visitor.visit_bytes(bytes)
                },
                _ => visitor.visit_unit(),
            },
            KalamDataType::Decimal { .. } => visit_json(self.document_value()?, visitor),
            KalamDataType::Text | KalamDataType::Uuid | KalamDataType::Embedding(_) => {
                match utf8_scalar(self.scalar) {
                    Some(text) => visitor.visit_string(text),
                    None => visitor.visit_unit(),
                }
            },
            KalamDataType::Json | KalamDataType::File => {
                visit_json(self.document_value()?, visitor)
            },
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if is_null(self.scalar) {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if stores_document(self.data_type) {
            return visit_json(self.document_value()?, visitor);
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if stores_document(self.data_type) {
            return visit_json(self.document_value()?, visitor);
        }
        self.deserialize_any(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if stores_document(self.data_type) {
            return self
                .document_value()?
                .deserialize_enum(name, variants, visitor)
                .map_err(map_json_de);
        }
        let variant = utf8_scalar(self.scalar).unwrap_or_default();
        visitor.visit_enum(variant.into_deserializer())
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf tuple tuple_struct identifier
    }
}

fn visit_json<'de, V>(value: Value, visitor: V) -> Result<V::Value, DeError>
where
    V: Visitor<'de>,
{
    value.deserialize_any(visitor).map_err(map_json_de)
}

fn map_json_de(error: serde_json::Error) -> DeError {
    DeError(error.to_string())
}

fn model_i64(scalar: &ScalarValue) -> i64 {
    match scalar {
        ScalarValue::Int64(Some(value))
        | ScalarValue::TimestampSecond(Some(value), _)
        | ScalarValue::TimestampMillisecond(Some(value), _)
        | ScalarValue::TimestampMicrosecond(Some(value), _)
        | ScalarValue::TimestampNanosecond(Some(value), _)
        | ScalarValue::Time64Microsecond(Some(value)) => *value,
        ScalarValue::Int32(Some(value)) | ScalarValue::Date32(Some(value)) => i64::from(*value),
        ScalarValue::Int16(Some(value)) => i64::from(*value),
        ScalarValue::Int8(Some(value)) => i64::from(*value),
        ScalarValue::UInt64(Some(value)) => *value as i64,
        ScalarValue::UInt32(Some(value)) => i64::from(*value),
        ScalarValue::UInt16(Some(value)) => i64::from(*value),
        ScalarValue::UInt8(Some(value)) => i64::from(*value),
        _ => 0,
    }
}

fn utf8_scalar(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}
