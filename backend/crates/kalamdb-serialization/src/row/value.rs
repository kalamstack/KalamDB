//! Recursive encode/decode of DataFusion [`ScalarValue`] against a storage type.

use std::sync::Arc;

use arrow::{
    array::{
        new_null_array, Array, ArrayRef, FixedSizeListArray, Float32Array, ListArray, StructArray,
    },
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::{DataType, Field, Fields},
};
use datafusion_common::ScalarValue;

use super::{
    scalar::{
        TAG_BOOL, TAG_BYTES, TAG_DATE32, TAG_DECIMAL128, TAG_EMBEDDING, TAG_F32, TAG_F64, TAG_I16,
        TAG_I32, TAG_I64, TAG_I8, TAG_LIST, TAG_NULL, TAG_STRUCT, TAG_TIME64_US, TAG_TS_MS,
        TAG_TS_NS, TAG_TS_US, TAG_U16, TAG_U32, TAG_U64, TAG_U8, TAG_UTF8,
    },
    schema::StorageDataType,
};
use crate::error::{Result, SerializationError};

pub(crate) fn write_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}

pub(crate) fn write_u16(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i64(buf: &mut Vec<u8>, value: i64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| SerializationError::Encode("bytes length exceeds u32".to_string()))?;
    write_u32(buf, len);
    buf.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn write_str(buf: &mut Vec<u8>, value: &str) -> Result<()> {
    write_bytes(buf, value.as_bytes())
}

pub(crate) struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.rest.len() < n {
            return Err(SerializationError::Truncated);
        }
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn i32(&mut self) -> Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn i64(&mut self) -> Result<i64> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(crate) fn f32(&mut self) -> Result<f32> {
        let bytes = self.take(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn f64(&mut self) -> Result<f64> {
        let bytes = self.take(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    pub(crate) fn string(&mut self) -> Result<String> {
        let bytes = self.bytes()?;
        String::from_utf8(bytes.to_vec())
            .map_err(|err| SerializationError::Decode(format!("invalid utf8: {err}")))
    }
}

pub(crate) fn encode_value(
    buf: &mut Vec<u8>,
    value: &ScalarValue,
    expected: &StorageDataType,
) -> Result<()> {
    if value.is_null() {
        write_u8(buf, TAG_NULL);
        return Ok(());
    }
    match (value, expected) {
        (ScalarValue::Boolean(Some(v)), StorageDataType::Boolean) => {
            write_u8(buf, TAG_BOOL);
            write_u8(buf, u8::from(*v));
        },
        (ScalarValue::Int8(Some(v)), StorageDataType::Int8) => {
            write_u8(buf, TAG_I8);
            write_u8(buf, *v as u8);
        },
        (ScalarValue::Int16(Some(v)), StorageDataType::Int16) => {
            write_u8(buf, TAG_I16);
            write_u16(buf, *v as u16);
        },
        (ScalarValue::Int32(Some(v)), StorageDataType::Int32) => {
            write_u8(buf, TAG_I32);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::Int64(Some(v)), StorageDataType::Int64) => {
            write_u8(buf, TAG_I64);
            write_i64(buf, *v);
        },
        (ScalarValue::UInt8(Some(v)), StorageDataType::UInt8) => {
            write_u8(buf, TAG_U8);
            write_u8(buf, *v);
        },
        (ScalarValue::UInt16(Some(v)), StorageDataType::UInt16) => {
            write_u8(buf, TAG_U16);
            write_u16(buf, *v);
        },
        (ScalarValue::UInt32(Some(v)), StorageDataType::UInt32) => {
            write_u8(buf, TAG_U32);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::UInt64(Some(v)), StorageDataType::UInt64) => {
            write_u8(buf, TAG_U64);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::Float32(Some(v)), StorageDataType::Float32) => {
            write_u8(buf, TAG_F32);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::Float64(Some(v)), StorageDataType::Float64) => {
            write_u8(buf, TAG_F64);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::Utf8(Some(v)), StorageDataType::Utf8)
        | (ScalarValue::LargeUtf8(Some(v)), StorageDataType::Utf8) => {
            write_u8(buf, TAG_UTF8);
            write_str(buf, v)?;
        },
        (ScalarValue::Binary(Some(v)), StorageDataType::Binary)
        | (ScalarValue::LargeBinary(Some(v)), StorageDataType::Binary)
        | (ScalarValue::BinaryView(Some(v)), StorageDataType::Binary)
        | (ScalarValue::FixedSizeBinary(_, Some(v)), StorageDataType::Binary) => {
            write_u8(buf, TAG_BYTES);
            write_bytes(buf, v)?;
        },
        (ScalarValue::Date32(Some(v)), StorageDataType::Date32) => {
            write_u8(buf, TAG_DATE32);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::Time64Microsecond(Some(v)), StorageDataType::Time64Microsecond) => {
            write_u8(buf, TAG_TIME64_US);
            write_i64(buf, *v);
        },
        (ScalarValue::TimestampMillisecond(Some(v), _), StorageDataType::TimestampMillisecond) => {
            write_u8(buf, TAG_TS_MS);
            write_i64(buf, *v);
        },
        (ScalarValue::TimestampMicrosecond(Some(v), _), StorageDataType::TimestampMicrosecond) => {
            write_u8(buf, TAG_TS_US);
            write_i64(buf, *v);
        },
        (ScalarValue::TimestampNanosecond(Some(v), _), StorageDataType::TimestampNanosecond) => {
            write_u8(buf, TAG_TS_NS);
            write_i64(buf, *v);
        },
        (
            ScalarValue::Decimal128(Some(v), precision, scale),
            StorageDataType::Decimal {
                precision: expected_p,
                scale: expected_s,
            },
        ) if precision == expected_p && *scale == *expected_s => {
            write_u8(buf, TAG_DECIMAL128);
            write_u8(buf, *precision);
            write_u8(buf, *scale as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        },
        (ScalarValue::FixedSizeList(array), StorageDataType::Embedding { dimension }) => {
            encode_embedding(buf, array.as_ref(), *dimension)?;
        },
        (ScalarValue::Struct(array), StorageDataType::Struct(fields)) => {
            encode_struct(buf, array.as_ref(), fields)?;
        },
        (ScalarValue::List(array), StorageDataType::List(inner)) => {
            encode_list(buf, array.as_ref(), inner)?;
        },
        _ => {
            return Err(SerializationError::Encode(format!(
                "unsupported or mismatched value {value:?} for storage type {expected:?}"
            )));
        },
    }
    Ok(())
}

fn encode_embedding(buf: &mut Vec<u8>, list: &FixedSizeListArray, dimension: i32) -> Result<()> {
    if list.len() != 1 {
        return Err(SerializationError::Encode(
            "embedding scalar must contain one list".to_string(),
        ));
    }
    if list.is_null(0) {
        write_u8(buf, TAG_NULL);
        return Ok(());
    }
    let values = list.values().as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
        SerializationError::Encode("embedding values must be float32".to_string())
    })?;
    if values.len() != dimension as usize {
        return Err(SerializationError::Encode(format!(
            "embedding dimension mismatch: expected {dimension}, got {}",
            values.len()
        )));
    }
    write_u8(buf, TAG_EMBEDDING);
    buf.extend_from_slice(&dimension.to_le_bytes());
    for i in 0..values.len() {
        buf.extend_from_slice(&values.value(i).to_le_bytes());
    }
    Ok(())
}

fn encode_struct(
    buf: &mut Vec<u8>,
    struct_array: &StructArray,
    fields: &[super::schema::StorageField],
) -> Result<()> {
    if struct_array.len() != 1 {
        return Err(SerializationError::Encode("struct scalar must contain one row".to_string()));
    }
    if struct_array.is_null(0) {
        write_u8(buf, TAG_NULL);
        return Ok(());
    }
    write_u8(buf, TAG_STRUCT);
    let count = u16::try_from(fields.len())
        .map_err(|_| SerializationError::Encode("too many struct fields".to_string()))?;
    write_u16(buf, count);
    for field in fields {
        let value = match struct_array.column_by_name(&field.name) {
            Some(child) => ScalarValue::try_from_array(child, 0).map_err(|err| {
                SerializationError::Encode(format!("struct field '{}': {err}", field.name))
            })?,
            None => datafusion_common::ScalarValue::Null,
        };
        encode_value(buf, &value, &field.data_type)?;
    }
    Ok(())
}

fn encode_list(buf: &mut Vec<u8>, list: &ListArray, inner: &StorageDataType) -> Result<()> {
    if list.len() != 1 {
        return Err(SerializationError::Encode("list scalar must contain one list".to_string()));
    }
    if list.is_null(0) {
        write_u8(buf, TAG_NULL);
        return Ok(());
    }
    let values = list.value(0);
    write_u8(buf, TAG_LIST);
    let len = u32::try_from(values.len())
        .map_err(|_| SerializationError::Encode("list length exceeds u32".to_string()))?;
    write_u32(buf, len);
    for i in 0..values.len() {
        let item = ScalarValue::try_from_array(&values, i)
            .map_err(|err| SerializationError::Encode(format!("list element {i}: {err}")))?;
        encode_value(buf, &item, inner)?;
    }
    Ok(())
}

pub(crate) fn decode_value(
    reader: &mut Reader<'_>,
    expected: &StorageDataType,
) -> Result<ScalarValue> {
    let tag = reader.u8()?;
    if tag == TAG_NULL {
        return Ok(null_for_type(expected));
    }
    match (tag, expected) {
        (TAG_BOOL, StorageDataType::Boolean) => Ok(ScalarValue::Boolean(Some(reader.u8()? != 0))),
        (TAG_I8, StorageDataType::Int8) => Ok(ScalarValue::Int8(Some(reader.u8()? as i8))),
        (TAG_I8, StorageDataType::Int16) => {
            Ok(ScalarValue::Int16(Some(i16::from(reader.u8()? as i8))))
        },
        (TAG_I8, StorageDataType::Int32) => {
            Ok(ScalarValue::Int32(Some(i32::from(reader.u8()? as i8))))
        },
        (TAG_I8, StorageDataType::Int64) => {
            Ok(ScalarValue::Int64(Some(i64::from(reader.u8()? as i8))))
        },
        (TAG_I16, StorageDataType::Int16) => Ok(ScalarValue::Int16(Some(reader.u16()? as i16))),
        (TAG_I16, StorageDataType::Int32) => {
            Ok(ScalarValue::Int32(Some(i32::from(reader.u16()? as i16))))
        },
        (TAG_I16, StorageDataType::Int64) => {
            Ok(ScalarValue::Int64(Some(i64::from(reader.u16()? as i16))))
        },
        (TAG_I32, StorageDataType::Int32) => Ok(ScalarValue::Int32(Some(reader.i32()?))),
        (TAG_I32, StorageDataType::Int64) => Ok(ScalarValue::Int64(Some(i64::from(reader.i32()?)))),
        (TAG_I64, StorageDataType::Int64) => Ok(ScalarValue::Int64(Some(reader.i64()?))),
        (TAG_U8, StorageDataType::UInt8) => Ok(ScalarValue::UInt8(Some(reader.u8()?))),
        (TAG_U8, StorageDataType::UInt16) => Ok(ScalarValue::UInt16(Some(u16::from(reader.u8()?)))),
        (TAG_U8, StorageDataType::UInt32) => Ok(ScalarValue::UInt32(Some(u32::from(reader.u8()?)))),
        (TAG_U8, StorageDataType::UInt64) => Ok(ScalarValue::UInt64(Some(u64::from(reader.u8()?)))),
        (TAG_U16, StorageDataType::UInt16) => Ok(ScalarValue::UInt16(Some(reader.u16()?))),
        (TAG_U16, StorageDataType::UInt32) => {
            Ok(ScalarValue::UInt32(Some(u32::from(reader.u16()?))))
        },
        (TAG_U16, StorageDataType::UInt64) => {
            Ok(ScalarValue::UInt64(Some(u64::from(reader.u16()?))))
        },
        (TAG_U32, StorageDataType::UInt32) => Ok(ScalarValue::UInt32(Some(reader.u32()?))),
        (TAG_U32, StorageDataType::UInt64) => {
            Ok(ScalarValue::UInt64(Some(u64::from(reader.u32()?))))
        },
        (TAG_U64, StorageDataType::UInt64) => {
            let bytes = reader.take(8)?;
            Ok(ScalarValue::UInt64(Some(u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))))
        },
        (TAG_F32, StorageDataType::Float32) => Ok(ScalarValue::Float32(Some(reader.f32()?))),
        (TAG_F32, StorageDataType::Float64) => {
            Ok(ScalarValue::Float64(Some(f64::from(reader.f32()?))))
        },
        (TAG_F64, StorageDataType::Float64) => Ok(ScalarValue::Float64(Some(reader.f64()?))),
        (TAG_UTF8, StorageDataType::Utf8) => Ok(ScalarValue::Utf8(Some(reader.string()?))),
        (TAG_BYTES, StorageDataType::Binary) => {
            Ok(ScalarValue::Binary(Some(reader.bytes()?.to_vec())))
        },
        (TAG_DATE32, StorageDataType::Date32) => {
            let bytes = reader.take(4)?;
            Ok(ScalarValue::Date32(Some(i32::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))))
        },
        (TAG_TIME64_US, StorageDataType::Time64Microsecond) => {
            Ok(ScalarValue::Time64Microsecond(Some(reader.i64()?)))
        },
        (TAG_TS_MS, StorageDataType::TimestampMillisecond) => {
            Ok(ScalarValue::TimestampMillisecond(Some(reader.i64()?), None))
        },
        (TAG_TS_US, StorageDataType::TimestampMicrosecond) => {
            Ok(ScalarValue::TimestampMicrosecond(Some(reader.i64()?), None))
        },
        (TAG_TS_NS, StorageDataType::TimestampNanosecond) => {
            Ok(ScalarValue::TimestampNanosecond(Some(reader.i64()?), None))
        },
        (TAG_DECIMAL128, StorageDataType::Decimal { precision, scale }) => {
            let got_p = reader.u8()?;
            let got_s = reader.u8()? as i8;
            if got_p != *precision || got_s != *scale {
                return Err(SerializationError::Decode(format!(
                    "decimal metadata mismatch: stored ({got_p},{got_s}) schema \
                     ({precision},{scale})"
                )));
            }
            let bytes = reader.take(16)?;
            let mut value_bytes = [0u8; 16];
            value_bytes.copy_from_slice(bytes);
            Ok(ScalarValue::Decimal128(
                Some(i128::from_le_bytes(value_bytes)),
                *precision,
                *scale,
            ))
        },
        (TAG_EMBEDDING, StorageDataType::Embedding { dimension }) => {
            decode_embedding(reader, *dimension)
        },
        (TAG_STRUCT, StorageDataType::Struct(fields)) => decode_struct(reader, fields),
        (TAG_LIST, StorageDataType::List(inner)) => decode_list(reader, inner),
        (tag, expected) => Err(SerializationError::Decode(format!(
            "value tag {tag} does not match storage type {expected:?}"
        ))),
    }
}

fn decode_embedding(reader: &mut Reader<'_>, dimension: i32) -> Result<ScalarValue> {
    let stored_dim_bytes = reader.take(4)?;
    let stored_dim = i32::from_le_bytes([
        stored_dim_bytes[0],
        stored_dim_bytes[1],
        stored_dim_bytes[2],
        stored_dim_bytes[3],
    ]);
    if stored_dim != dimension {
        return Err(SerializationError::Decode(format!(
            "embedding dimension mismatch: stored {stored_dim}, schema {dimension}"
        )));
    }
    let mut values = Vec::with_capacity(dimension as usize);
    for _ in 0..dimension {
        values.push(Some(reader.f32()?));
    }
    let list = arrow::array::FixedSizeListArray::from_iter_primitive::<
        arrow::datatypes::Float32Type,
        _,
        _,
    >(std::iter::once(Some(values)), dimension);
    Ok(ScalarValue::FixedSizeList(Arc::new(list)))
}

fn decode_struct(
    reader: &mut Reader<'_>,
    fields: &[super::schema::StorageField],
) -> Result<ScalarValue> {
    let count = reader.u16()? as usize;
    let live_count = count.min(fields.len());
    let mut columns: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let value = if index < live_count {
            decode_value(reader, &field.data_type)?
        } else {
            // Additive nullable field: materialize NULL without rewriting old rows.
            null_for_type(&field.data_type)
        };
        let array = value.to_array().map_err(|err| {
            SerializationError::Decode(format!("struct field '{}': {err}", field.name))
        })?;
        columns.push((Arc::new(Field::new(&field.name, array.data_type().clone(), true)), array));
    }
    for _ in live_count..count {
        skip_value(reader)?;
    }
    let struct_array = StructArray::from(columns);
    Ok(ScalarValue::Struct(Arc::new(struct_array)))
}

fn decode_list(reader: &mut Reader<'_>, inner: &StorageDataType) -> Result<ScalarValue> {
    let len = reader.u32()? as usize;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        items.push(decode_value(reader, inner)?);
    }
    if items.is_empty() {
        let data_type = arrow_type(inner);
        let values = new_null_array(&data_type, 0);
        let field = Arc::new(Field::new("item", data_type, true));
        let list = ListArray::try_new(field, OffsetBuffer::from_lengths([0]), values, None)
            .map_err(|err| SerializationError::Decode(format!("empty list: {err}")))?;
        return Ok(ScalarValue::List(Arc::new(list)));
    }
    Ok(ScalarValue::List(ScalarValue::new_list(&items, &items[0].data_type(), true)))
}

fn null_for_type(data_type: &StorageDataType) -> ScalarValue {
    match data_type {
        StorageDataType::Boolean => ScalarValue::Boolean(None),
        StorageDataType::Int8 => ScalarValue::Int8(None),
        StorageDataType::Int16 => ScalarValue::Int16(None),
        StorageDataType::Int32 => ScalarValue::Int32(None),
        StorageDataType::Int64 => ScalarValue::Int64(None),
        StorageDataType::UInt8 => ScalarValue::UInt8(None),
        StorageDataType::UInt16 => ScalarValue::UInt16(None),
        StorageDataType::UInt32 => ScalarValue::UInt32(None),
        StorageDataType::UInt64 => ScalarValue::UInt64(None),
        StorageDataType::Float32 => ScalarValue::Float32(None),
        StorageDataType::Float64 => ScalarValue::Float64(None),
        StorageDataType::Utf8 => ScalarValue::Utf8(None),
        StorageDataType::Binary => ScalarValue::Binary(None),
        StorageDataType::Date32 => ScalarValue::Date32(None),
        StorageDataType::Time64Microsecond => ScalarValue::Time64Microsecond(None),
        StorageDataType::TimestampMillisecond => ScalarValue::TimestampMillisecond(None, None),
        StorageDataType::TimestampMicrosecond => ScalarValue::TimestampMicrosecond(None, None),
        StorageDataType::TimestampNanosecond => ScalarValue::TimestampNanosecond(None, None),
        StorageDataType::Decimal { precision, scale } => {
            ScalarValue::Decimal128(None, *precision, *scale)
        },
        StorageDataType::Embedding { dimension } => {
            let data_type = DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                *dimension,
            );
            let array = new_null_array(&data_type, 1);
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("fixed-size list null array")
                .clone();
            ScalarValue::FixedSizeList(Arc::new(list))
        },
        StorageDataType::Struct(fields) => {
            let arrow_fields: Fields = fields
                .iter()
                .map(|field| Field::new(&field.name, arrow_type(&field.data_type), true))
                .collect();
            let columns: Vec<ArrayRef> = fields
                .iter()
                .map(|field| new_null_array(&arrow_type(&field.data_type), 1))
                .collect();
            let array = StructArray::new(arrow_fields, columns, Some(NullBuffer::new_null(1)));
            ScalarValue::Struct(Arc::new(array))
        },
        StorageDataType::List(inner) => {
            let field = Arc::new(Field::new("item", arrow_type(inner), true));
            let values = new_null_array(&arrow_type(inner), 0);
            let list = ListArray::new(
                field,
                OffsetBuffer::from_lengths([0]),
                values,
                Some(NullBuffer::new_null(1)),
            );
            ScalarValue::List(Arc::new(list))
        },
    }
}

/// Skip one tagged value without knowing its schema type (dropped ordinals).
pub(crate) fn skip_value(reader: &mut Reader<'_>) -> Result<()> {
    let tag = reader.u8()?;
    match tag {
        TAG_NULL => Ok(()),
        TAG_BOOL | TAG_I8 | TAG_U8 => {
            reader.u8()?;
            Ok(())
        },
        TAG_I16 | TAG_U16 => {
            reader.u16()?;
            Ok(())
        },
        TAG_I32 | TAG_U32 | TAG_F32 | TAG_DATE32 => {
            reader.take(4)?;
            Ok(())
        },
        TAG_I64 | TAG_U64 | TAG_F64 | TAG_TIME64_US | TAG_TS_MS | TAG_TS_US | TAG_TS_NS => {
            reader.take(8)?;
            Ok(())
        },
        TAG_UTF8 | TAG_BYTES => {
            reader.bytes()?;
            Ok(())
        },
        TAG_DECIMAL128 => {
            reader.take(18)?;
            Ok(())
        },
        TAG_EMBEDDING => {
            let dim_bytes = reader.take(4)?;
            let dimension =
                i32::from_le_bytes([dim_bytes[0], dim_bytes[1], dim_bytes[2], dim_bytes[3]]);
            if dimension < 0 {
                return Err(SerializationError::Decode("negative embedding dimension".to_string()));
            }
            reader.take((dimension as usize).saturating_mul(4))?;
            Ok(())
        },
        TAG_STRUCT => {
            let count = reader.u16()? as usize;
            for _ in 0..count {
                skip_value(reader)?;
            }
            Ok(())
        },
        TAG_LIST => {
            let len = reader.u32()? as usize;
            for _ in 0..len {
                skip_value(reader)?;
            }
            Ok(())
        },
        other => Err(SerializationError::Decode(format!("unknown value tag {other}"))),
    }
}

fn arrow_type(data_type: &StorageDataType) -> DataType {
    match data_type {
        StorageDataType::Boolean => DataType::Boolean,
        StorageDataType::Int8 => DataType::Int8,
        StorageDataType::Int16 => DataType::Int16,
        StorageDataType::Int32 => DataType::Int32,
        StorageDataType::Int64 => DataType::Int64,
        StorageDataType::UInt8 => DataType::UInt8,
        StorageDataType::UInt16 => DataType::UInt16,
        StorageDataType::UInt32 => DataType::UInt32,
        StorageDataType::UInt64 => DataType::UInt64,
        StorageDataType::Float32 => DataType::Float32,
        StorageDataType::Float64 => DataType::Float64,
        StorageDataType::Utf8 => DataType::Utf8,
        StorageDataType::Binary => DataType::Binary,
        StorageDataType::Date32 => DataType::Date32,
        StorageDataType::Time64Microsecond => {
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        },
        StorageDataType::TimestampMillisecond => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        },
        StorageDataType::TimestampMicrosecond => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        },
        StorageDataType::TimestampNanosecond => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        },
        StorageDataType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        StorageDataType::Embedding { dimension } => DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            *dimension,
        ),
        StorageDataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Field::new(&field.name, arrow_type(&field.data_type), true))
                .collect(),
        ),
        StorageDataType::List(inner) => {
            DataType::List(Arc::new(Field::new("item", arrow_type(inner), true)))
        },
    }
}
