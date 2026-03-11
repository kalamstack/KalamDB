//! Shared utilities for converting between Arrow and JSON formats
//!
//! This module provides common functions for converting data between:
//! - Arrow RecordBatch (columnar format for DataFusion queries)
//! - JSON objects (row-oriented format for storage)
//!
//! These utilities are used by user_table_provider, shared_table_provider,
//! and stream_table_provider to avoid code duplication.
//!
//! # Conversion Architecture (Single Source of Truth)
//!
//! All ScalarValue → JSON conversions flow through ONE central function:
//! ```text
//! scalar_value_to_json()  ← SINGLE SOURCE OF TRUTH
//!          ↓
//!     ┌────┴────┐
//!     ↓         ↓
//! row_to_json_map()   record_batch_to_json_arrays()
//!     ↓                ↓
//! WebSocket:           REST API:
//! - notifications      - /v1/api/sql
//! - subscriptions
//! - batch data
//! ```
//!
//! # Serialization Format
//!
//! All values are serialized as plain JSON values:
//! - Int64/UInt64 always serialized as strings to preserve precision
//! - Timestamps serialized as raw microsecond values (numbers)
//! - Boolean, String, Float values as native JSON types

use crate::errors::CommonError;
// Chrono no longer needed - DataFusion handles timestamp serialization natively
use crate::models::rows::Row;
use crate::models::KalamCellValue;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

/// Type alias for Arc<dyn Array> to improve readability
type ArrayRef = Arc<dyn datafusion::arrow::array::Array>;

/// Coerce a list of rows to match the schema types and fill defaults.
///
/// This is useful for INSERT operations where we need to ensure the data matches
/// the schema before broadcasting or storing.
pub fn coerce_rows(rows: Vec<Row>, schema: &SchemaRef) -> Result<Vec<Row>, String> {
    let _span = tracing::info_span!(
        "coerce_rows",
        row_count = rows.len(),
        num_fields = schema.fields().len()
    )
    .entered();
    let defaults = get_column_defaults(schema);
    let typed_nulls = get_typed_nulls(schema);

    rows.into_iter()
        .map(|mut row| {
            // 1. Add defaults for any missing fields (moves/clones only defaults)
            for (i, field) in schema.fields().iter().enumerate() {
                let field_name = field.name().as_str();
                if !row.values.contains_key(field_name) {
                    let default_val = defaults[i].clone().unwrap_or_else(|| typed_nulls[i].clone());
                    row.values.insert(field_name.to_string(), default_val);
                }
            }

            // 2. Coerce existing values in-place using get_mut + mem::replace.
            //    This avoids rebuilding the entire BTreeMap (no remove/re-insert,
            //    no new String key allocations for existing columns).
            for field in schema.fields() {
                if let Some(val) = row.values.get_mut(field.name().as_str()) {
                    let owned = std::mem::replace(val, ScalarValue::Null);
                    *val = coerce_scalar_to_field(owned, field)?;
                }
            }

            Ok(row)
        })
        .collect()
}

/// Convert ScalarValue-backed rows to Arrow RecordBatch
///
/// SELECT operations build row-oriented `Row` values (ScalarValue map) directly from storage.
/// This helper converts them into Arrow's columnar format without JSON round-trips.
pub fn json_rows_to_arrow_batch(schema: &SchemaRef, rows: Vec<Row>) -> Result<RecordBatch, String> {
    if rows.is_empty() {
        // Return empty batch with correct schema
        let empty_arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| create_empty_array(field.data_type()))
            .collect();

        return RecordBatch::try_new(schema.clone(), empty_arrays)
            .map_err(|e| format!("Failed to create empty batch: {}", e));
    }

    // Pre-calculate default values for each column to avoid repeated logic in loop
    let defaults = get_column_defaults(schema);
    let typed_nulls = get_typed_nulls(schema);

    // Transpose rows to columns (Row-oriented -> Column-oriented)
    // We use move semantics (row.values.remove) to avoid cloning strings/blobs
    let mut columns: Vec<Vec<ScalarValue>> =
        (0..schema.fields().len()).map(|_| Vec::with_capacity(rows.len())).collect();

    for mut row in rows {
        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name().as_str();

            // Take value from map (move) instead of cloning
            let raw_value = row
                .values
                .remove(field_name)
                .or_else(|| defaults[i].clone())
                .unwrap_or_else(|| typed_nulls[i].clone());

            // We still need to coerce, but now we own the value
            let coerced = coerce_scalar_to_field(raw_value, field)
                .map_err(|e| format!("Failed to coerce column '{}': {}", field.name(), e))?;

            columns[i].push(coerced);
        }
    }

    // Build arrays from columns
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (i, col_values) in columns.into_iter().enumerate() {
        let field_name = schema.field(i).name();
        let array = ScalarValue::iter_to_array(col_values.into_iter())
            .map_err(|e| format!("Failed to build column '{}': {}", field_name, e))?;
        arrays.push(array);
    }

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("Failed to build record batch: {}", e))
}

fn get_column_defaults(schema: &SchemaRef) -> Vec<Option<ScalarValue>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            if field.is_nullable() {
                return None;
            }
            match field.data_type() {
                DataType::Boolean => Some(ScalarValue::Boolean(Some(false))),
                DataType::Int8 => Some(ScalarValue::Int8(Some(0))),
                DataType::Int16 => Some(ScalarValue::Int16(Some(0))),
                DataType::Int32 => Some(ScalarValue::Int32(Some(0))),
                DataType::Int64 => Some(ScalarValue::Int64(Some(0))),
                DataType::UInt8 => Some(ScalarValue::UInt8(Some(0))),
                DataType::UInt16 => Some(ScalarValue::UInt16(Some(0))),
                DataType::UInt32 => Some(ScalarValue::UInt32(Some(0))),
                DataType::UInt64 => Some(ScalarValue::UInt64(Some(0))),
                DataType::Float32 => Some(ScalarValue::Float32(Some(0.0))),
                DataType::Float64 => Some(ScalarValue::Float64(Some(0.0))),
                DataType::Utf8 => Some(ScalarValue::Utf8(Some("".to_string()))),
                DataType::LargeUtf8 => Some(ScalarValue::LargeUtf8(Some("".to_string()))),
                DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                    Some(ScalarValue::TimestampMicrosecond(Some(0), tz_opt.clone()))
                },
                DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                    Some(ScalarValue::TimestampMillisecond(Some(0), tz_opt.clone()))
                },
                DataType::Date32 => Some(ScalarValue::Date32(Some(0))),
                DataType::Date64 => Some(ScalarValue::Date64(Some(0))),
                _ => None,
            }
        })
        .collect()
}

fn get_typed_nulls(schema: &SchemaRef) -> Vec<ScalarValue> {
    schema
        .fields()
        .iter()
        .map(|field| ScalarValue::try_from(field.data_type()).unwrap_or(ScalarValue::Null))
        .collect()
}

fn create_empty_array(data_type: &DataType) -> ArrayRef {
    new_empty_array(data_type)
}

pub fn coerce_scalar_to_field(value: ScalarValue, field: &Field) -> Result<ScalarValue, String> {
    if matches!(value, ScalarValue::Null) {
        return ScalarValue::try_from(field.data_type())
            .map_err(|e| format!("Failed to build typed NULL for '{}': {}", field.name(), e));
    }

    if &value.data_type() == field.data_type() {
        return Ok(value);
    }

    if matches!(field.data_type(), DataType::FixedSizeBinary(16)) {
        return coerce_uuid_scalar(value, field)?.ok_or_else(|| {
            format!(
                "Unable to coerce value {:?} to UUID for column '{}'",
                field.data_type(),
                field.name()
            )
        });
    }

    if let Some(embedding) = coerce_embedding_scalar(value.clone(), field)? {
        return Ok(embedding);
    }

    value.cast_to(field.data_type()).map_err(|e| {
        format!(
            "Unable to cast value {:?} to {:?} for column '{}': {}",
            value.data_type(),
            field.data_type(),
            field.name(),
            e
        )
    })
}

fn coerce_embedding_scalar(
    value: ScalarValue,
    field: &Field,
) -> Result<Option<ScalarValue>, String> {
    let DataType::FixedSizeList(child, len) = field.data_type() else {
        return Ok(None);
    };

    if !matches!(child.data_type(), DataType::Float32) {
        return Ok(None);
    }

    let raw = match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s,
        _ => return Ok(None),
    };

    let normalized = raw.trim();
    let normalized = normalized
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(normalized);

    let parsed = match serde_json::from_str::<Vec<f32>>(normalized) {
        Ok(v) => v,
        Err(json_err) => parse_debug_embedding_repr(normalized).ok_or_else(|| {
            format!(
                "Invalid embedding JSON for column '{}': {}",
                field.name(),
                json_err
            )
        })?,
    };

    if parsed.len() != (*len as usize) {
        return Err(format!(
            "Embedding dimension mismatch for column '{}': expected {}, got {}",
            field.name(),
            len,
            parsed.len()
        ));
    }

    let values = Float32Array::from(parsed);
    let list = FixedSizeListArray::new(child.clone(), *len, Arc::new(values), None);

    Ok(Some(ScalarValue::FixedSizeList(Arc::new(list))))
}

fn parse_debug_embedding_repr(raw: &str) -> Option<Vec<f32>> {
    let mut values = Vec::new();
    for line in raw.lines() {
        let token = line.trim().trim_end_matches(',');
        if token.is_empty()
            || token.starts_with('[')
            || token.starts_with(']')
            || token.contains("Array")
            || token.contains('<')
            || token.contains('>')
        {
            continue;
        }
        if let Ok(parsed) = token.parse::<f32>() {
            values.push(parsed);
        }
    }
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn coerce_uuid_scalar(value: ScalarValue, field: &Field) -> Result<Option<ScalarValue>, String> {
    match value {
        ScalarValue::Utf8(Some(raw)) | ScalarValue::LargeUtf8(Some(raw)) => {
            let uuid = Uuid::parse_str(&raw).map_err(|e| {
                format!("Invalid UUID literal '{}' for column '{}': {}", raw, field.name(), e)
            })?;
            Ok(Some(ScalarValue::FixedSizeBinary(16, Some(uuid.as_bytes().to_vec()))))
        },
        ScalarValue::Binary(Some(bytes)) => {
            if bytes.len() != 16 {
                return Err(format!(
                    "UUID binary literal must be 16 bytes for column '{}', got {}",
                    field.name(),
                    bytes.len()
                ));
            }
            Ok(Some(ScalarValue::FixedSizeBinary(16, Some(bytes))))
        },
        ScalarValue::FixedSizeBinary(size, Some(bytes)) => {
            if size != 16 || bytes.len() != 16 {
                return Err(format!(
                    "UUID fixed binary literal must be 16 bytes for column '{}', got size {} and len {}",
                    field.name(),
                    size,
                    bytes.len()
                ));
            }
            Ok(Some(ScalarValue::FixedSizeBinary(16, Some(bytes))))
        },
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn make_row(entries: Vec<(&str, ScalarValue)>) -> Row {
        let mut values = BTreeMap::new();
        for (key, value) in entries {
            values.insert(key.to_string(), value);
        }
        Row::new(values)
    }

    #[test]
    fn test_json_rows_to_arrow_batch_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let rows = vec![
            make_row(vec![
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("Alice".into()))),
            ]),
            make_row(vec![
                ("id", ScalarValue::Int64(Some(2))),
                ("name", ScalarValue::Null),
            ]),
            make_row(vec![
                ("id", ScalarValue::Int64(Some(3))),
                ("name", ScalarValue::Utf8(Some("Charlie".into()))),
            ]),
        ];

        let batch = json_rows_to_arrow_batch(&schema, rows).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let id_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert!(name_col.is_null(1));
        assert_eq!(name_col.value(2), "Charlie");
    }

    #[test]
    fn test_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = json_rows_to_arrow_batch(&schema, vec![]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_timestamp_conversion() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));

        // Test i64 timestamp
        let rows = vec![make_row(vec![(
            "ts",
            ScalarValue::Int64(Some(1609459200000)),
        )])];
        let batch = json_rows_to_arrow_batch(&schema, rows).unwrap();
        let ts_col = batch.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(ts_col.value(0), 1609459200000i64);

        // Test RFC3339 string
        let rows = vec![make_row(vec![(
            "ts",
            ScalarValue::Utf8(Some("2021-01-01T00:00:00Z".into())),
        )])];
        let batch = json_rows_to_arrow_batch(&schema, rows).unwrap();
        let ts_col = batch.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(ts_col.value(0), 1609459200000i64);
    }
}

/// Convert Arrow array value at given index to DataFusion ScalarValue
pub fn arrow_value_to_scalar(
    array: &dyn datafusion::arrow::array::Array,
    row_idx: usize,
) -> Result<ScalarValue, datafusion::error::DataFusionError> {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::*;

    if array.is_null(row_idx) {
        return Ok(ScalarValue::try_from(array.data_type()).unwrap_or(ScalarValue::Null));
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(ScalarValue::Boolean(Some(arr.value(row_idx))))
        },
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(ScalarValue::Int8(Some(arr.value(row_idx))))
        },
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(ScalarValue::Int16(Some(arr.value(row_idx))))
        },
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(ScalarValue::Int32(Some(arr.value(row_idx))))
        },
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(ScalarValue::Int64(Some(arr.value(row_idx))))
        },
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(ScalarValue::UInt8(Some(arr.value(row_idx))))
        },
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(ScalarValue::UInt16(Some(arr.value(row_idx))))
        },
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(ScalarValue::UInt32(Some(arr.value(row_idx))))
        },
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(ScalarValue::UInt64(Some(arr.value(row_idx))))
        },
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(ScalarValue::Float32(Some(arr.value(row_idx))))
        },
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(ScalarValue::Float64(Some(arr.value(row_idx))))
        },
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(ScalarValue::Utf8(Some(arr.value(row_idx).to_string())))
        },
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(ScalarValue::LargeUtf8(Some(arr.value(row_idx).to_string())))
        },
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            Ok(ScalarValue::TimestampMillisecond(Some(arr.value(row_idx)), tz.clone()))
        },
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            Ok(ScalarValue::TimestampMicrosecond(Some(arr.value(row_idx)), tz.clone()))
        },
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            Ok(ScalarValue::TimestampNanosecond(Some(arr.value(row_idx)), tz.clone()))
        },
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(ScalarValue::Date32(Some(arr.value(row_idx))))
        },
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Ok(ScalarValue::Date64(Some(arr.value(row_idx))))
        },
        DataType::Time32(TimeUnit::Second) => {
            let arr = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
            Ok(ScalarValue::Time32Second(Some(arr.value(row_idx))))
        },
        DataType::Time32(TimeUnit::Millisecond) => {
            let arr = array.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
            Ok(ScalarValue::Time32Millisecond(Some(arr.value(row_idx))))
        },
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = array.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
            Ok(ScalarValue::Time64Microsecond(Some(arr.value(row_idx))))
        },
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = array.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
            Ok(ScalarValue::Time64Nanosecond(Some(arr.value(row_idx))))
        },
        DataType::FixedSizeBinary(size) => {
            let arr = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            Ok(ScalarValue::FixedSizeBinary(*size, Some(arr.value(row_idx).to_vec())))
        },
        DataType::Decimal128(precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(ScalarValue::Decimal128(Some(arr.value(row_idx)), *precision, *scale))
        },
        _ => {
            // Fallback: convert to string representation
            Ok(ScalarValue::Utf8(Some(format!("{:?}", array.slice(row_idx, 1)))))
        },
    }
}

/// Convert JSON Object to Row
///
/// Used for converting storage JSON rows to Row format for live query notifications.
pub fn json_to_row(json: &JsonValue) -> Option<Row> {
    if let JsonValue::Object(map) = json {
        let mut values = BTreeMap::new();
        for (k, v) in map {
            values.insert(k.clone(), json_value_to_scalar(v));
        }
        Some(Row::new(values))
    } else {
        None
    }
}

/// Convert serde_json::Value to DataFusion ScalarValue
///
/// This variant uses fallbacks for complex types (arrays/objects are converted to JSON strings).
/// For strict validation (e.g., SQL parameter binding), use `json_value_to_scalar_strict`.
pub fn json_value_to_scalar(v: &JsonValue) -> ScalarValue {
    match v {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::Bool(b) => ScalarValue::Boolean(Some(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                ScalarValue::Int64(Some(i))
            } else if let Some(f) = n.as_f64() {
                ScalarValue::Float64(Some(f))
            } else {
                ScalarValue::Float64(None)
            }
        },
        JsonValue::String(s) => ScalarValue::Utf8(Some(s.clone())),
        JsonValue::Array(_) => ScalarValue::Utf8(Some(v.to_string())), // Fallback for arrays
        JsonValue::Object(_) => ScalarValue::Utf8(Some(v.to_string())), // Fallback for objects
    }
}

/// Convert serde_json::Value to DataFusion ScalarValue with strict validation
///
/// Unlike `json_value_to_scalar`, this function returns an error for unsupported types
/// (arrays and objects). Use this for API parameter binding where strict validation is needed.
///
/// # Errors
/// Returns an error string if the JSON value is an array or object.
pub fn json_value_to_scalar_strict(v: &JsonValue) -> Result<ScalarValue, String> {
    match v {
        JsonValue::Null => Ok(ScalarValue::Utf8(None)),
        JsonValue::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                Err(format!("Unsupported number format: {}", n))
            }
        },
        JsonValue::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        JsonValue::Array(_) => Err("Array parameters not yet supported".to_string()),
        JsonValue::Object(_) => Err("Object parameters not yet supported".to_string()),
    }
}

/// Convert DataFusion ScalarValue to [`KalamCellValue`]
///
/// **SINGLE SOURCE OF TRUTH** for converting ScalarValue to a cell value.
/// All ScalarValue → cell-value conversions in the codebase flow through
/// this function.
///
/// Plain JSON values with Int64/UInt64 as strings for precision.
/// - Example: `"123"` for Int64(123), `"Alice"` for Utf8("Alice")
/// - Timestamps serialized as raw microsecond values (numbers)
pub fn scalar_value_to_json(value: &ScalarValue) -> Result<KalamCellValue, CommonError> {
    let json = match value {
        ScalarValue::Null => JsonValue::Null,
        ScalarValue::Boolean(Some(b)) => JsonValue::Bool(*b),
        ScalarValue::Boolean(None) => JsonValue::Null,
        ScalarValue::Int8(Some(i)) => JsonValue::Number((*i).into()),
        ScalarValue::Int8(None) => JsonValue::Null,
        ScalarValue::Int16(Some(i)) => JsonValue::Number((*i).into()),
        ScalarValue::Int16(None) => JsonValue::Null,
        ScalarValue::Int32(Some(i)) => JsonValue::Number((*i).into()),
        ScalarValue::Int32(None) => JsonValue::Null,
        // Int64 always as string for precision (no MAX_SAFE_INTEGER check)
        ScalarValue::Int64(Some(i)) => JsonValue::String(i.to_string()),
        ScalarValue::Int64(None) => JsonValue::Null,
        ScalarValue::UInt8(Some(i)) => JsonValue::Number((*i).into()),
        ScalarValue::UInt8(None) => JsonValue::Null,
        ScalarValue::UInt16(Some(i)) => JsonValue::Number((*i).into()),
        ScalarValue::UInt16(None) => JsonValue::Null,
        ScalarValue::UInt32(Some(i)) => JsonValue::Number((*i).into()),
        ScalarValue::UInt32(None) => JsonValue::Null,
        // UInt64 always as string for precision
        ScalarValue::UInt64(Some(i)) => JsonValue::String(i.to_string()),
        ScalarValue::UInt64(None) => JsonValue::Null,
        ScalarValue::Float32(Some(f)) => {
            return serde_json::Number::from_f64(*f as f64)
                .map(|n| KalamCellValue(JsonValue::Number(n)))
                .ok_or_else(|| CommonError::invalid_input("Invalid float value"));
        },
        ScalarValue::Float32(None) => JsonValue::Null,
        ScalarValue::Float64(Some(f)) => {
            return serde_json::Number::from_f64(*f)
                .map(|n| KalamCellValue(JsonValue::Number(n)))
                .ok_or_else(|| CommonError::invalid_input("Invalid float value"));
        },
        ScalarValue::Float64(None) => JsonValue::Null,
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            JsonValue::String(s.clone())
        },
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => JsonValue::Null,
        ScalarValue::Date32(Some(d)) => JsonValue::Number((*d).into()),
        ScalarValue::Date32(None) => JsonValue::Null,
        ScalarValue::Date64(Some(d)) => JsonValue::Number((*d).into()),
        ScalarValue::Date64(None) => JsonValue::Null,
        ScalarValue::TimestampMillisecond(Some(ts), _) => JsonValue::Number((*ts).into()),
        ScalarValue::TimestampMillisecond(None, _) => JsonValue::Null,
        ScalarValue::TimestampMicrosecond(Some(ts), _) => JsonValue::Number((*ts).into()),
        ScalarValue::TimestampMicrosecond(None, _) => JsonValue::Null,
        ScalarValue::TimestampNanosecond(Some(ts), _) => JsonValue::Number((*ts).into()),
        ScalarValue::TimestampNanosecond(None, _) => JsonValue::Null,
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            // Format decimal with proper scale
            // e.g., 20075 with scale=2 -> "200.75"
            let divisor = 10i128.pow(*scale as u32);
            let integer_part = v / divisor;
            let fractional_part = (v % divisor).abs();
            if *scale == 0 {
                JsonValue::String(integer_part.to_string())
            } else {
                JsonValue::String(format!(
                    "{}.{:0>width$}",
                    integer_part,
                    fractional_part,
                    width = *scale as usize
                ))
            }
        },
        ScalarValue::Decimal128(None, _, _) => JsonValue::Null,
        ScalarValue::FixedSizeBinary(_, Some(bytes)) => {
            // If it looks like a UUID (16 bytes), try to format as UUID string
            if bytes.len() == 16 {
                if let Ok(uuid) = uuid::Uuid::from_slice(bytes) {
                    return Ok(KalamCellValue(JsonValue::String(uuid.to_string())));
                }
            }
            // Otherwise, use array of numbers for generic binary
            JsonValue::Array(bytes.iter().map(|&b| JsonValue::Number(b.into())).collect())
        },
        ScalarValue::FixedSizeBinary(_, None) => JsonValue::Null,
        ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
            JsonValue::Array(bytes.iter().map(|&b| JsonValue::Number(b.into())).collect())
        },
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => JsonValue::Null,
        // Time types - output as raw microseconds/nanoseconds value (integer)
        ScalarValue::Time64Microsecond(Some(t)) => JsonValue::Number((*t).into()),
        ScalarValue::Time64Microsecond(None) => JsonValue::Null,
        ScalarValue::Time64Nanosecond(Some(t)) => JsonValue::Number((*t).into()),
        ScalarValue::Time64Nanosecond(None) => JsonValue::Null,
        ScalarValue::Time32Millisecond(Some(t)) => JsonValue::Number((*t).into()),
        ScalarValue::Time32Millisecond(None) => JsonValue::Null,
        ScalarValue::Time32Second(Some(t)) => JsonValue::Number((*t).into()),
        ScalarValue::Time32Second(None) => JsonValue::Null,
        _ => {
            return Err(CommonError::invalid_input(format!(
                "Unsupported ScalarValue conversion to JSON: {:?}",
                value
            )));
        },
    };
    Ok(KalamCellValue(json))
}

/// Convert Arrow RecordBatch to JSON rows
///
/// **Used by:** REST API `/v1/api/sql` endpoint for query results
///
/// For each cell in the batch, extracts ScalarValue and calls `scalar_value_to_json()`
/// (the single source of truth for all conversions).
///
/// # Arguments
/// * `batch` - Arrow RecordBatch from DataFusion query execution
///
/// # Architecture Note
/// ✅ NO direct conversions - all cells go through scalar_value_to_json()
pub fn record_batch_to_json_rows(
    batch: &RecordBatch,
) -> Result<Vec<std::collections::HashMap<String, KalamCellValue>>, CommonError> {
    let schema = batch.schema();
    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut json_row = std::collections::HashMap::with_capacity(num_cols);

        for (col_idx, col_name) in column_names.iter().enumerate() {
            let column = batch.column(col_idx);

            let scalar = ScalarValue::try_from_array(column.as_ref(), row_idx).map_err(|e| {
                CommonError::invalid_input(format!("Failed to extract scalar: {}", e))
            })?;

            // ✅ SINGLE SOURCE: All conversions flow through this function
            let json_value = scalar_value_to_json(&scalar)?;
            json_row.insert(col_name.clone(), json_value);
        }

        rows.push(json_row);
    }

    Ok(rows)
}

/// Convert Arrow RecordBatch to JSON arrays (one array per row, values ordered by column index)
///
/// **Used by:** REST API `/v1/api/sql` endpoint for query results
///
/// Returns rows as arrays of values where each value's position corresponds to the schema field index.
/// Example output: `[["123", "Alice", 1699000000000000], ["456", "Bob", 1699000001000000]]`
///
/// # Arguments
/// * `batch` - Arrow RecordBatch from DataFusion query execution
///
/// # Architecture Note
/// ✅ NO direct conversions - all cells go through scalar_value_to_json()
pub fn record_batch_to_json_arrays(
    batch: &RecordBatch,
) -> Result<Vec<Vec<KalamCellValue>>, CommonError> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut json_row = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let column = batch.column(col_idx);

            let scalar = ScalarValue::try_from_array(column.as_ref(), row_idx).map_err(|e| {
                CommonError::invalid_input(format!("Failed to extract scalar: {}", e))
            })?;

            // ✅ SINGLE SOURCE: All conversions flow through this function
            let json_value = scalar_value_to_json(&scalar)?;
            json_row.push(json_value);
        }

        rows.push(json_row);
    }

    Ok(rows)
}

/// Convert Row to HashMap<String, JsonValue>
///
/// **Used by:** WebSocket subscriptions, change notifications, and batch data
///
/// For each column in the row, calls `scalar_value_to_json()` (the single source of truth).
///
/// # Arguments
/// * `row` - Row from live query system (ScalarValue map)
///
/// # Returns
/// HashMap mapping column names to JSON values
///
/// # Architecture Note
/// ✅ NO direct conversions - all columns go through scalar_value_to_json()
pub fn row_to_json_map(
    row: &Row,
) -> Result<std::collections::HashMap<String, KalamCellValue>, CommonError> {
    let mut json_row = std::collections::HashMap::with_capacity(row.values.len());

    for (col_name, scalar_value) in &row.values {
        // ✅ SINGLE SOURCE: All conversions flow through this function
        let json_value = scalar_value_to_json(scalar_value)?;
        json_row.insert(col_name.clone(), json_value);
    }

    Ok(json_row)
}

/// Coerce a single row of updates to match the schema types.
///
/// Unlike `coerce_rows`, this does NOT fill in default values for missing columns.
/// It only coerces the columns that are present in the update row.
pub fn coerce_updates(row: Row, schema: &SchemaRef) -> Result<Row, String> {
    let mut new_values = BTreeMap::new();

    for (col_name, value) in row.values {
        if let Ok(field) = schema.field_with_name(&col_name) {
            let coerced = coerce_scalar_to_field(value, field)?;
            new_values.insert(col_name, coerced);
        } else {
            // If column not in schema, keep it as is
            new_values.insert(col_name, value);
        }
    }

    Ok(Row::new(new_values))
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn test_int64_always_string() {
        // Int64 should always be serialized as string for precision
        let value = ScalarValue::Int64(Some(123));
        let json = scalar_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!("123").into());

        // Even small values should be strings
        let value = ScalarValue::Int64(Some(42));
        let json = scalar_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!("42").into());
    }

    #[test]
    fn test_uint64_always_string() {
        let value = ScalarValue::UInt64(Some(123));
        let json = scalar_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!("123").into());
    }

    #[test]
    fn test_utf8_plain_string() {
        let value = ScalarValue::Utf8(Some("hello".to_string()));
        let json = scalar_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!("hello").into());
    }

    #[test]
    fn test_boolean_plain_value() {
        let value = ScalarValue::Boolean(Some(true));
        let json = scalar_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(true).into());
    }

    #[test]
    fn test_timestamp_as_number() {
        let value = ScalarValue::TimestampMicrosecond(Some(1765741510326539), None);
        let json = scalar_value_to_json(&value).unwrap();
        // Timestamp should be raw number (microseconds)
        assert_eq!(json, serde_json::json!(1765741510326539_i64).into());
    }

    #[test]
    fn test_null_values() {
        let null = ScalarValue::Null;
        let json = scalar_value_to_json(&null).unwrap();
        assert_eq!(json, serde_json::json!(null).into());
    }

    #[test]
    fn test_float64_plain_number() {
        let value = ScalarValue::Float64(Some(3.25));
        let json = scalar_value_to_json(&value).unwrap();
        // Check it's a number approximately equal to 3.25
        assert!(json.inner().as_f64().unwrap() > 3.24 && json.inner().as_f64().unwrap() < 3.26);
    }

    #[test]
    fn test_int32_plain_number() {
        let value = ScalarValue::Int32(Some(42));
        let json = scalar_value_to_json(&value).unwrap();
        assert_eq!(json, serde_json::json!(42).into());
    }
}
