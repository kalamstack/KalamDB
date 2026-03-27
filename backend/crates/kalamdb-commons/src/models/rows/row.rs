use arrow::array::{Array, FixedSizeListArray, Float32Array};
use datafusion_common::ScalarValue;
use serde::de;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

/// A unified Row representation that holds DataFusion ScalarValues
/// but serializes to clean, standard JSON for clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    // RowEnvelope values as column name -> ScalarValue map
    pub values: BTreeMap<String, ScalarValue>,
}

/// Compact, ordered representation of a row aligned with a physical schema.
#[derive(Debug, Clone, PartialEq)]
pub struct RowEnvelope {
    /// Column-aligned scalar values (Null when column missing)
    pub columns: Vec<ScalarValue>,
}

impl RowEnvelope {
    pub fn new(columns: Vec<ScalarValue>) -> Self {
        Self { columns }
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

#[derive(Debug, Error)]
pub enum RowConversionError {
    #[error("column schema mismatch: expected {expected} columns, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
}

/// Internal storage representation for ScalarValue
/// Uses derive for binary storage compatibility.
/// Note: Int64 and UInt64 are stored as strings to preserve precision in JSON
/// (JavaScript's Number.MAX_SAFE_INTEGER = 2^53-1 causes precision loss for large i64/u64)
///
/// This type is also used for typed JSON serialization to match WebSocket subscription format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StoredScalarValue {
    Null,
    Boolean(Option<bool>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    /// Int64 stored as string to preserve precision in JSON
    Int64(Option<String>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    /// UInt64 stored as string to preserve precision in JSON
    UInt64(Option<String>),
    Utf8(Option<String>),
    LargeUtf8(Option<String>),
    Binary(Option<Vec<u8>>),
    LargeBinary(Option<Vec<u8>>),
    FixedSizeBinary {
        size: i32,
        value: Option<Vec<u8>>,
    },
    Date32(Option<i32>),
    Time64Microsecond(Option<i64>),
    TimestampMillisecond {
        value: Option<i64>,
        timezone: Option<String>,
    },
    TimestampMicrosecond {
        value: Option<i64>,
        timezone: Option<String>,
    },
    TimestampNanosecond {
        value: Option<i64>,
        timezone: Option<String>,
    },
    Decimal128 {
        #[serde(
            serialize_with = "serialize_decimal128_option",
            deserialize_with = "deserialize_decimal128_option",
            default
        )]
        value: Option<i128>,
        precision: u8,
        scale: i8,
    },
    Embedding {
        size: i32,
        values: Option<Vec<Option<f32>>>,
    },
    Fallback(String),
}

fn serialize_decimal128_option<S>(value: &Option<i128>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(v) => serializer.serialize_some(&v.to_string()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_decimal128_option<'de, D>(deserializer: D) -> Result<Option<i128>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Decimal128Value {
        String(String),
        I64(i64),
        U64(u64),
        I128(i128),
    }

    let maybe_value = Option::<Decimal128Value>::deserialize(deserializer)?;
    match maybe_value {
        None => Ok(None),
        Some(Decimal128Value::String(v)) => v.parse::<i128>().map(Some).map_err(de::Error::custom),
        Some(Decimal128Value::I64(v)) => Ok(Some(v as i128)),
        Some(Decimal128Value::U64(v)) => Ok(Some(v as i128)),
        Some(Decimal128Value::I128(v)) => Ok(Some(v)),
    }
}

impl From<&ScalarValue> for StoredScalarValue {
    fn from(value: &ScalarValue) -> Self {
        match value {
            ScalarValue::Null => StoredScalarValue::Null,
            ScalarValue::Boolean(v) => StoredScalarValue::Boolean(*v),
            ScalarValue::Float32(v) => StoredScalarValue::Float32(*v),
            ScalarValue::Float64(v) => StoredScalarValue::Float64(*v),
            ScalarValue::Int8(v) => StoredScalarValue::Int8(*v),
            ScalarValue::Int16(v) => StoredScalarValue::Int16(*v),
            ScalarValue::Int32(v) => StoredScalarValue::Int32(*v),
            ScalarValue::Int64(v) => StoredScalarValue::Int64(v.map(|i| i.to_string())),
            ScalarValue::UInt8(v) => StoredScalarValue::UInt8(*v),
            ScalarValue::UInt16(v) => StoredScalarValue::UInt16(*v),
            ScalarValue::UInt32(v) => StoredScalarValue::UInt32(*v),
            ScalarValue::UInt64(v) => StoredScalarValue::UInt64(v.map(|i| i.to_string())),
            ScalarValue::Utf8(v) => StoredScalarValue::Utf8(v.clone()),
            ScalarValue::LargeUtf8(v) => StoredScalarValue::LargeUtf8(v.clone()),
            ScalarValue::Binary(v) => StoredScalarValue::Binary(v.clone()),
            ScalarValue::LargeBinary(v) => StoredScalarValue::LargeBinary(v.clone()),
            ScalarValue::FixedSizeBinary(size, v) => StoredScalarValue::FixedSizeBinary {
                size: *size,
                value: v.clone(),
            },
            ScalarValue::Date32(v) => StoredScalarValue::Date32(*v),
            ScalarValue::Time64Microsecond(v) => StoredScalarValue::Time64Microsecond(*v),
            ScalarValue::TimestampMillisecond(v, tz) => StoredScalarValue::TimestampMillisecond {
                value: *v,
                timezone: tz.as_ref().map(|t| t.to_string()),
            },
            ScalarValue::TimestampMicrosecond(v, tz) => StoredScalarValue::TimestampMicrosecond {
                value: *v,
                timezone: tz.as_ref().map(|t| t.to_string()),
            },
            ScalarValue::TimestampNanosecond(v, tz) => StoredScalarValue::TimestampNanosecond {
                value: *v,
                timezone: tz.as_ref().map(|t| t.to_string()),
            },
            ScalarValue::Decimal128(value, precision, scale) => StoredScalarValue::Decimal128 {
                value: *value,
                precision: *precision,
                scale: *scale,
            },
            ScalarValue::FixedSizeList(array) => match encode_embedding_from_list(array.clone()) {
                Some(stored) => stored,
                None => StoredScalarValue::Fallback(value.to_string()),
            },
            _ => StoredScalarValue::Fallback(value.to_string()),
        }
    }
}

impl From<StoredScalarValue> for ScalarValue {
    fn from(value: StoredScalarValue) -> Self {
        match value {
            StoredScalarValue::Null => ScalarValue::Null,
            StoredScalarValue::Boolean(v) => ScalarValue::Boolean(v),
            StoredScalarValue::Float32(v) => ScalarValue::Float32(v),
            StoredScalarValue::Float64(v) => ScalarValue::Float64(v),
            StoredScalarValue::Int8(v) => ScalarValue::Int8(v),
            StoredScalarValue::Int16(v) => ScalarValue::Int16(v),
            StoredScalarValue::Int32(v) => ScalarValue::Int32(v),
            StoredScalarValue::Int64(v) => {
                ScalarValue::Int64(v.and_then(|s| s.parse::<i64>().ok()))
            },
            StoredScalarValue::UInt8(v) => ScalarValue::UInt8(v),
            StoredScalarValue::UInt16(v) => ScalarValue::UInt16(v),
            StoredScalarValue::UInt32(v) => ScalarValue::UInt32(v),
            StoredScalarValue::UInt64(v) => {
                ScalarValue::UInt64(v.and_then(|s| s.parse::<u64>().ok()))
            },
            StoredScalarValue::Utf8(v) => ScalarValue::Utf8(v),
            StoredScalarValue::LargeUtf8(v) => ScalarValue::LargeUtf8(v),
            StoredScalarValue::Binary(v) => ScalarValue::Binary(v),
            StoredScalarValue::LargeBinary(v) => ScalarValue::LargeBinary(v),
            StoredScalarValue::FixedSizeBinary { size, value } => {
                ScalarValue::FixedSizeBinary(size, value)
            },
            StoredScalarValue::Date32(v) => ScalarValue::Date32(v),
            StoredScalarValue::Time64Microsecond(v) => ScalarValue::Time64Microsecond(v),
            StoredScalarValue::TimestampMillisecond { value, timezone } => {
                ScalarValue::TimestampMillisecond(value, timezone.map(Arc::<str>::from))
            },
            StoredScalarValue::TimestampMicrosecond { value, timezone } => {
                ScalarValue::TimestampMicrosecond(value, timezone.map(Arc::<str>::from))
            },
            StoredScalarValue::TimestampNanosecond { value, timezone } => {
                ScalarValue::TimestampNanosecond(value, timezone.map(Arc::<str>::from))
            },
            StoredScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => ScalarValue::Decimal128(value, precision, scale),
            StoredScalarValue::Embedding { size, values } => decode_embedding(size, &values),
            StoredScalarValue::Fallback(s) => ScalarValue::Utf8(Some(s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RowSerdeHelper {
    values: Vec<(String, StoredScalarValue)>,
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.values.len()))?;
        for (k, v) in &self.values {
            let stored = StoredScalarValue::from(v);
            map.serialize_entry(k, &stored)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let helper = RowSerdeHelper::deserialize(deserializer)?;
        let mut values = BTreeMap::new();
        for (k, v) in helper.values {
            values.insert(k, ScalarValue::from(v));
        }
        Ok(Row { values })
    }
}

impl Serialize for RowSerdeHelper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.values.len()))?;
        for (k, v) in &self.values {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RowSerdeHelper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: std::collections::BTreeMap<String, StoredScalarValue> =
            Deserialize::deserialize(deserializer)?;
        let values = map.into_iter().collect();
        Ok(RowSerdeHelper { values })
    }
}

impl Row {
    pub fn new(values: BTreeMap<String, ScalarValue>) -> Self {
        Self { values }
    }

    pub fn from_vec(values: Vec<(String, ScalarValue)>) -> Self {
        let mut map = BTreeMap::new();
        for (k, v) in values {
            map.insert(k, v);
        }
        Self { values: map }
    }

    pub fn get(&self, key: &str) -> Option<&ScalarValue> {
        self.values.get(key)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, ScalarValue> {
        self.values.iter()
    }
}

fn encode_embedding_from_list(array: Arc<FixedSizeListArray>) -> Option<StoredScalarValue> {
    let size = array.value_length();
    let values = array.values();
    let float_array = values.as_any().downcast_ref::<Float32Array>()?;

    let mut vector = Vec::with_capacity(size as usize);
    for i in 0..size {
        let value = float_array.value(i as usize);
        vector.push(Some(value));
    }

    Some(StoredScalarValue::Embedding {
        size,
        values: Some(vector),
    })
}

fn decode_embedding(size: i32, values: &Option<Vec<Option<f32>>>) -> ScalarValue {
    use arrow::datatypes::Float32Type;

    let values = values.clone().unwrap_or(vec![None; size as usize]);
    let mut floats = Vec::with_capacity(size as usize);
    for value in values {
        floats.push(value.unwrap_or(0.0));
    }

    let array = Float32Array::from(floats);
    let list = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        (0..1).map(|_| Some(array.iter())),
        size,
    );

    ScalarValue::FixedSizeList(Arc::new(list))
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by number of values first
        let len_cmp = self.values.len().cmp(&other.values.len());
        if len_cmp != Ordering::Equal {
            return len_cmp;
        }

        // Then compare by key/value pairs
        for ((k1, v1), (k2, v2)) in self.values.iter().zip(other.values.iter()) {
            let key_cmp = k1.cmp(k2);
            if key_cmp != Ordering::Equal {
                return key_cmp;
            }
            let val_cmp = v1.partial_cmp(v2).unwrap_or(Ordering::Equal);
            if val_cmp != Ordering::Equal {
                return val_cmp;
            }
        }

        Ordering::Equal
    }
}

// KSerializable implementation for EntityStore support
#[cfg(feature = "serialization")]
impl crate::serialization::KSerializable for Row {
    fn encode(&self) -> Result<Vec<u8>, crate::storage::StorageError> {
        crate::serialization::row_codec::encode_row(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self, crate::storage::StorageError>
    where
        Self: Sized,
    {
        crate::serialization::row_codec::decode_row(bytes)
    }
}
