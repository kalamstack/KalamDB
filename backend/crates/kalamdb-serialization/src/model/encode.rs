use std::collections::{BTreeMap, HashMap};

use datafusion_common::ScalarValue;
use kalamdb_commons::{
    conversions::json_value_to_scalar_for_column,
    models::{datatypes::KalamDataType, rows::Row},
    schemas::TableDefinition,
};
use serde::ser::{Error as _, Impossible, Serialize, SerializeSeq, SerializeStruct, Serializer};
use serde_json::Value;

use super::map_ser;

/// Serialize a typed model into a schema-aligned [`Row`] without timestamp scaling.
pub(super) fn serialize_model<T: Serialize>(
    model: &T,
    table_def: &TableDefinition,
) -> crate::error::Result<Row> {
    let columns = column_types(table_def);
    model
        .serialize(RowSerializer { table_def, columns })
        .map_err(|error| map_ser(error.0))
}

fn column_types(table_def: &TableDefinition) -> HashMap<&str, &KalamDataType> {
    table_def
        .columns
        .iter()
        .map(|column| (column.column_name.as_str(), &column.data_type))
        .collect()
}

fn stores_document(data_type: &KalamDataType) -> bool {
    matches!(data_type, KalamDataType::Json | KalamDataType::File)
}

#[derive(Debug)]
struct SerError(String);

impl std::fmt::Display for SerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SerError {}

impl serde::ser::Error for SerError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

struct RowSerializer<'a> {
    table_def: &'a TableDefinition,
    columns:   HashMap<&'a str, &'a KalamDataType>,
}

impl<'a> Serializer for RowSerializer<'a> {
    type Ok = Row;
    type Error = SerError;
    type SerializeSeq = Impossible<Row, SerError>;
    type SerializeTuple = Impossible<Row, SerError>;
    type SerializeTupleStruct = Impossible<Row, SerError>;
    type SerializeTupleVariant = Impossible<Row, SerError>;
    type SerializeMap = Impossible<Row, SerError>;
    type SerializeStruct = StructFields<'a>;
    type SerializeStructVariant = Impossible<Row, SerError>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(StructFields {
            table_def: self.table_def,
            columns:   self.columns,
            fields:    BTreeMap::new(),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(SerError::custom("model serialize failed: expected struct"))
    }
}

struct StructFields<'a> {
    table_def: &'a TableDefinition,
    columns:   HashMap<&'a str, &'a KalamDataType>,
    fields:    BTreeMap<String, ScalarValue>,
}

impl SerializeStruct for StructFields<'_> {
    type Ok = Row;
    type Error = SerError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let Some(data_type) = self.columns.get(key).copied() else {
            return Ok(());
        };
        let scalar = if stores_document(data_type) {
            let json = serde_json::to_value(value)
                .map_err(|error| SerError(format!("json field serialize failed: {error}")))?;
            json_value_to_scalar_for_column(&json, data_type)
                .map_err(|error| SerError(format!("json->scalar conversion failed: {error}")))?
        } else {
            value.serialize(ScalarSerializer { data_type })?
        };
        self.fields.insert(key.to_string(), scalar);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut fields = self.fields;
        for column in &self.table_def.columns {
            fields.entry(column.column_name.clone()).or_insert(ScalarValue::Null);
        }
        Ok(Row::new(fields))
    }
}

struct ScalarSerializer<'a> {
    data_type: &'a KalamDataType,
}

impl<'a> Serializer for ScalarSerializer<'a> {
    type Ok = ScalarValue;
    type Error = SerError;
    type SerializeSeq = BytesSeq<'a>;
    type SerializeTuple = Impossible<ScalarValue, SerError>;
    type SerializeTupleStruct = Impossible<ScalarValue, SerError>;
    type SerializeTupleVariant = Impossible<ScalarValue, SerError>;
    type SerializeMap = Impossible<ScalarValue, SerError>;
    type SerializeStruct = Impossible<ScalarValue, SerError>;
    type SerializeStructVariant = Impossible<ScalarValue, SerError>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        Ok(match self.data_type {
            KalamDataType::Boolean => ScalarValue::Boolean(Some(value)),
            KalamDataType::Text => ScalarValue::Utf8(Some(value.to_string())),
            other => {
                return json_fallback(&Value::Bool(value), other);
            },
        })
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(i64::from(value), self.data_type)
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(i64::from(value), self.data_type)
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(i64::from(value), self.data_type)
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(value, self.data_type)
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(i64::from(value), self.data_type)
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(i64::from(value), self.data_type)
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        integer_to_scalar(i64::from(value), self.data_type)
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        let signed = i64::try_from(value)
            .map_err(|_| SerError::custom("u64 value does not fit in signed storage"))?;
        integer_to_scalar(signed, self.data_type)
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        Ok(match self.data_type {
            KalamDataType::Float => ScalarValue::Float32(Some(value)),
            KalamDataType::Double => ScalarValue::Float64(Some(f64::from(value))),
            other => {
                return json_fallback(&Value::from(f64::from(value)), other);
            },
        })
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        Ok(match self.data_type {
            KalamDataType::Double => ScalarValue::Float64(Some(value)),
            KalamDataType::Float => ScalarValue::Float32(Some(value as f32)),
            other => {
                return json_fallback(&Value::from(value), other);
            },
        })
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&value.to_string())
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        match self.data_type {
            KalamDataType::Text | KalamDataType::Uuid | KalamDataType::Embedding(_) => {
                Ok(ScalarValue::Utf8(Some(value.to_string())))
            },
            other => json_fallback(&Value::String(value.to_string()), other),
        }
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(match self.data_type {
            KalamDataType::Bytes => ScalarValue::Binary(Some(value.to_vec())),
            KalamDataType::Text => {
                ScalarValue::Utf8(Some(String::from_utf8_lossy(value).into_owned()))
            },
            other => {
                return Err(SerError(format!("bytes are not valid for column type {other}")));
            },
        })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(ScalarValue::Null)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(ScalarValue::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(ScalarValue::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_str(variant)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if matches!(self.data_type, KalamDataType::Bytes) {
            Ok(BytesSeq {
                data_type: self.data_type,
                bytes:     Vec::new(),
            })
        } else {
            Err(SerError(format!(
                "sequences are only stored on json or bytes columns, found {}",
                self.data_type
            )))
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let _ = len;
        Err(SerError::custom("tuple values require a json column"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(SerError::custom("tuple struct values require a json column"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(SerError::custom("tuple variant values require a json column"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(SerError::custom("map values require a json column"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(SerError::custom("nested structs require a json column"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(SerError::custom("struct variant values require a json column"))
    }
}

struct BytesSeq<'a> {
    data_type: &'a KalamDataType,
    bytes:     Vec<u8>,
}

impl SerializeSeq for BytesSeq<'_> {
    type Ok = ScalarValue;
    type Error = SerError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let n = value.serialize(U8Serializer)?;
        self.bytes.push(n);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(match self.data_type {
            KalamDataType::Bytes => ScalarValue::Binary(Some(self.bytes)),
            other => {
                return Err(SerError(format!("byte sequence is not valid for {other}")));
            },
        })
    }
}

struct U8Serializer;

impl Serializer for U8Serializer {
    type Ok = u8;
    type Error = SerError;
    type SerializeSeq = Impossible<u8, SerError>;
    type SerializeTuple = Impossible<u8, SerError>;
    type SerializeTupleStruct = Impossible<u8, SerError>;
    type SerializeTupleVariant = Impossible<u8, SerError>;
    type SerializeMap = Impossible<u8, SerError>;
    type SerializeStruct = Impossible<u8, SerError>;
    type SerializeStructVariant = Impossible<u8, SerError>;

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        u8::try_from(v).map_err(|_| SerError::custom("byte sequence value out of range"))
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(SerError::custom("byte sequence expected an integer"))
    }
}

fn integer_to_scalar(value: i64, data_type: &KalamDataType) -> Result<ScalarValue, SerError> {
    Ok(match data_type {
        KalamDataType::Boolean => ScalarValue::Boolean(Some(value != 0)),
        KalamDataType::SmallInt => ScalarValue::Int16(Some(value as i16)),
        KalamDataType::Int => ScalarValue::Int32(Some(value as i32)),
        KalamDataType::BigInt => ScalarValue::Int64(Some(value)),
        KalamDataType::Timestamp => ScalarValue::TimestampMicrosecond(Some(value), None),
        KalamDataType::DateTime => {
            ScalarValue::TimestampMicrosecond(Some(value), Some("UTC".into()))
        },
        KalamDataType::Time => ScalarValue::Time64Microsecond(Some(value)),
        KalamDataType::Date => ScalarValue::Date32(Some(value as i32)),
        KalamDataType::Double => ScalarValue::Float64(Some(value as f64)),
        KalamDataType::Float => ScalarValue::Float32(Some(value as f32)),
        KalamDataType::Text | KalamDataType::Uuid => ScalarValue::Utf8(Some(value.to_string())),
        other => return json_fallback(&Value::from(value), other),
    })
}

fn json_fallback(value: &Value, data_type: &KalamDataType) -> Result<ScalarValue, SerError> {
    json_value_to_scalar_for_column(value, data_type)
        .map_err(|error| SerError(format!("json->scalar conversion failed: {error}")))
}
