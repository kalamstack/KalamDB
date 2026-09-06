//! Typed primary-key bucket keys for winner maps.
//!
//! Integer PKs stay on the stack. Text keys allocate once. Callers that used to
//! stringify every PK (`ScalarValue::to_string()`, `format!("_seq:{}")`) should
//! go through these helpers so hot and cold paths keep the same encoding.

use std::fmt;

use arrow::array::{
    Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use super::scalar::string::parse_string_as_scalar;
use crate::{ids::SeqId, models::rows::Row};

/// Hash-map key for PK-keyed winner selection.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PkBucketKey {
    Int(i64),
    UInt(u64),
    Text(String),
    Seq(i64),
}

impl From<String> for PkBucketKey {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for PkBucketKey {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl fmt::Display for PkBucketKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(value) => write!(f, "{value}"),
            Self::UInt(value) => write!(f, "{value}"),
            Self::Text(value) => write!(f, "{value}"),
            Self::Seq(value) => write!(f, "_seq:{value}"),
        }
    }
}

/// Required-PK conversion used by DML identity (ON CONFLICT, staged mutations).
///
/// Null is an error. Empty text stays an empty text key instead of a `_seq`
/// fallback so mutation identity matches `scalar_to_pk_string`.
pub fn try_pk_bucket_key(value: &ScalarValue) -> Result<PkBucketKey, String> {
    match value {
        ScalarValue::Int8(Some(v)) => Ok(PkBucketKey::Int(i64::from(*v))),
        ScalarValue::Int16(Some(v)) => Ok(PkBucketKey::Int(i64::from(*v))),
        ScalarValue::Int32(Some(v)) => Ok(PkBucketKey::Int(i64::from(*v))),
        ScalarValue::Int64(Some(v)) => Ok(PkBucketKey::Int(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(PkBucketKey::UInt(u64::from(*v))),
        ScalarValue::UInt16(Some(v)) => Ok(PkBucketKey::UInt(u64::from(*v))),
        ScalarValue::UInt32(Some(v)) => Ok(PkBucketKey::UInt(u64::from(*v))),
        ScalarValue::UInt64(Some(v)) => Ok(PkBucketKey::UInt(*v)),
        ScalarValue::Boolean(Some(v)) => Ok(PkBucketKey::Text(v.to_string())),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(PkBucketKey::Text(s.clone())),
        _ => Err(format!("unsupported primary key type: {value:?}")),
    }
}

/// Build a merge bucket key from a cell value without stringifying integers.
///
/// Null or empty text falls back to `_seq` so rows without a PK still bucket.
/// Unsupported non-null types stringify so merge still groups them.
#[inline]
pub fn pk_bucket_key_from_scalar(value: &ScalarValue, seq: SeqId) -> PkBucketKey {
    match try_pk_bucket_key(value) {
        Ok(PkBucketKey::Text(text)) if text.is_empty() => PkBucketKey::Seq(seq.as_i64()),
        Ok(key) => key,
        Err(_) if value.is_null() => PkBucketKey::Seq(seq.as_i64()),
        Err(_) => PkBucketKey::Text(value.to_string()),
    }
}

pub fn pk_bucket_key_from_row(row: &Row, pk_name: &str, seq: SeqId) -> PkBucketKey {
    match row.get(pk_name) {
        Some(value) => pk_bucket_key_from_scalar(value, seq),
        None => PkBucketKey::Seq(seq.as_i64()),
    }
}

/// Convert a previously stringified PK back into a typed merge bucket using the column type.
pub fn pk_bucket_key_from_typed_string(
    value: &str,
    data_type: &DataType,
    seq: SeqId,
) -> Result<PkBucketKey, String> {
    let scalar = parse_string_as_scalar(value, data_type)?;
    Ok(pk_bucket_key_from_scalar(&scalar, seq))
}

/// Convert a previously stringified PK into a required identity key using the column type.
///
/// Empty text stays `Text("")`. Does not fall back to `_seq`.
pub fn try_pk_bucket_key_from_typed_string(
    value: &str,
    data_type: &DataType,
) -> Result<PkBucketKey, String> {
    try_pk_bucket_key(&parse_string_as_scalar(value, data_type)?)
}

/// Required PK from an Arrow array. Nulls return `None`. Empty text stays `Text("")`.
pub fn try_pk_bucket_key_from_array(array: &dyn Array, row_idx: usize) -> Option<PkBucketKey> {
    read_pk_bucket_from_array(array, row_idx)
}

/// Read a PK bucket from an Arrow array using the same encoding as [`pk_bucket_key_from_scalar`].
pub fn pk_bucket_key_from_array(array: &dyn Array, row_idx: usize, seq: SeqId) -> PkBucketKey {
    match read_pk_bucket_from_array(array, row_idx) {
        Some(PkBucketKey::Text(text)) if text.is_empty() => PkBucketKey::Seq(seq.as_i64()),
        Some(key) => key,
        None => PkBucketKey::Seq(seq.as_i64()),
    }
}

fn read_pk_bucket_from_array(array: &dyn Array, row_idx: usize) -> Option<PkBucketKey> {
    if array.is_null(row_idx) {
        return None;
    }

    let any = array.as_any();
    if let Some(values) = any.downcast_ref::<Int64Array>() {
        return Some(PkBucketKey::Int(values.value(row_idx)));
    }
    if let Some(values) = any.downcast_ref::<Int32Array>() {
        return Some(PkBucketKey::Int(i64::from(values.value(row_idx))));
    }
    if let Some(values) = any.downcast_ref::<Int16Array>() {
        return Some(PkBucketKey::Int(i64::from(values.value(row_idx))));
    }
    if let Some(values) = any.downcast_ref::<Int8Array>() {
        return Some(PkBucketKey::Int(i64::from(values.value(row_idx))));
    }
    if let Some(values) = any.downcast_ref::<UInt64Array>() {
        return Some(PkBucketKey::UInt(values.value(row_idx)));
    }
    if let Some(values) = any.downcast_ref::<UInt32Array>() {
        return Some(PkBucketKey::UInt(u64::from(values.value(row_idx))));
    }
    if let Some(values) = any.downcast_ref::<UInt16Array>() {
        return Some(PkBucketKey::UInt(u64::from(values.value(row_idx))));
    }
    if let Some(values) = any.downcast_ref::<UInt8Array>() {
        return Some(PkBucketKey::UInt(u64::from(values.value(row_idx))));
    }
    if let Some(values) = any.downcast_ref::<StringArray>() {
        return Some(PkBucketKey::Text(values.value(row_idx).to_owned()));
    }
    if let Some(values) = any.downcast_ref::<LargeStringArray>() {
        return Some(PkBucketKey::Text(values.value(row_idx).to_owned()));
    }

    ScalarValue::try_from_array(array, row_idx)
        .ok()
        .and_then(|value| try_pk_bucket_key(&value).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_keys_stay_on_the_stack() {
        let seq = SeqId::from_i64(99);
        assert_eq!(
            pk_bucket_key_from_scalar(&ScalarValue::Int64(Some(42)), seq),
            PkBucketKey::Int(42)
        );
        assert_eq!(
            pk_bucket_key_from_scalar(&ScalarValue::UInt64(Some(7)), seq),
            PkBucketKey::UInt(7)
        );
        assert_ne!(PkBucketKey::Int(1), PkBucketKey::Text("1".to_string()));
    }

    #[test]
    fn empty_or_null_falls_back_to_seq_for_merge() {
        let seq = SeqId::from_i64(99);
        assert_eq!(
            pk_bucket_key_from_scalar(&ScalarValue::Utf8(Some(String::new())), seq),
            PkBucketKey::Seq(99)
        );
        assert_eq!(pk_bucket_key_from_scalar(&ScalarValue::Int64(None), seq), PkBucketKey::Seq(99));
    }

    #[test]
    fn required_pk_rejects_null_and_keeps_empty_text() {
        assert!(try_pk_bucket_key(&ScalarValue::Int64(None)).is_err());
        assert_eq!(
            try_pk_bucket_key(&ScalarValue::Utf8(Some(String::new()))).unwrap(),
            PkBucketKey::Text(String::new())
        );
        assert_eq!(
            try_pk_bucket_key(&ScalarValue::Utf8(Some("blog".to_string()))).unwrap(),
            PkBucketKey::Text("blog".to_string())
        );
    }

    #[test]
    fn typed_string_round_trips_int64() {
        let seq = SeqId::from_i64(0);
        assert_eq!(
            pk_bucket_key_from_typed_string("42", &DataType::Int64, seq).unwrap(),
            PkBucketKey::Int(42)
        );
        assert_eq!(
            try_pk_bucket_key_from_typed_string("42", &DataType::Int64).unwrap(),
            PkBucketKey::Int(42)
        );
        assert_eq!(
            try_pk_bucket_key_from_typed_string("", &DataType::Utf8).unwrap(),
            PkBucketKey::Text(String::new())
        );
    }

    #[test]
    fn display_matches_legacy_string_keys() {
        assert_eq!(PkBucketKey::Int(12345).to_string(), "12345");
        assert_eq!(PkBucketKey::Seq(7).to_string(), "_seq:7");
        assert_eq!(PkBucketKey::Text("user".to_string()).to_string(), "user");
    }
}
