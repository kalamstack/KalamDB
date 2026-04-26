//! Arrow RecordBatch builder utilities for reducing boilerplate.
//!
//! This module provides convenient builders for creating Arrow RecordBatches
//! with common column types, reducing code duplication across system table
//! providers and other RecordBatch construction sites.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeStringArray, StringArray, StringBuilder, TimestampMicrosecondArray,
        TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    compute,
    compute::kernels::aggregate::{max_string, min_string},
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::RecordBatch,
};
pub use arrow_schema::DataType as ArrowDataType;
use arrow_schema::{Field, Schema, TimeUnit};

/// Arrow UTF-8 string type.
pub fn arrow_utf8() -> ArrowDataType {
    ArrowDataType::Utf8
}

/// Arrow Large UTF-8 string type.
pub fn arrow_large_utf8() -> ArrowDataType {
    ArrowDataType::LargeUtf8
}

/// Arrow Int64 type.
pub fn arrow_int64() -> ArrowDataType {
    ArrowDataType::Int64
}

/// Arrow Int32 type.
pub fn arrow_int32() -> ArrowDataType {
    ArrowDataType::Int32
}

/// Arrow UInt64 type.
pub fn arrow_uint64() -> ArrowDataType {
    ArrowDataType::UInt64
}

/// Arrow UInt32 type.
pub fn arrow_uint32() -> ArrowDataType {
    ArrowDataType::UInt32
}

/// Arrow Float64 type.
pub fn arrow_float64() -> ArrowDataType {
    ArrowDataType::Float64
}

/// Arrow Float32 type.
pub fn arrow_float32() -> ArrowDataType {
    ArrowDataType::Float32
}

/// Arrow Boolean type.
pub fn arrow_boolean() -> ArrowDataType {
    ArrowDataType::Boolean
}

/// Arrow Timestamp (microsecond precision, no timezone) type.
pub fn arrow_timestamp_micros() -> ArrowDataType {
    ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
}

/// Arrow Date32 type.
pub fn arrow_date32() -> ArrowDataType {
    ArrowDataType::Date32
}

/// Arrow Time64 (microsecond precision) type.
pub fn arrow_time64_micros() -> ArrowDataType {
    ArrowDataType::Time64(TimeUnit::Microsecond)
}

/// Build a UTF-8 field.
pub fn field_utf8(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_utf8(), nullable)
}

/// Build a Large UTF-8 field.
pub fn field_large_utf8(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_large_utf8(), nullable)
}

/// Build an Int64 field.
pub fn field_int64(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_int64(), nullable)
}

/// Build an Int32 field.
pub fn field_int32(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_int32(), nullable)
}

/// Build a UInt64 field.
pub fn field_uint64(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_uint64(), nullable)
}

/// Build a UInt32 field.
pub fn field_uint32(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_uint32(), nullable)
}

/// Build a Float64 field.
pub fn field_float64(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_float64(), nullable)
}

/// Build a Boolean field.
pub fn field_boolean(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_boolean(), nullable)
}

/// Build a Timestamp (microsecond) field.
pub fn field_timestamp_micros(name: &str, nullable: bool) -> Field {
    Field::new(name, arrow_timestamp_micros(), nullable)
}

/// Build a SchemaRef from fields.
pub fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// Builder for constructing Arrow RecordBatches with type-safe column additions.
///
/// This builder simplifies the process of creating RecordBatches by providing
/// convenience methods for common column types used in KalamDB system tables.
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::arrow_utils::RecordBatchBuilder;
///
/// let mut builder = RecordBatchBuilder::new(schema);
/// builder
///     .add_string_column(vec![Some("user1"), Some("user2"), None])
///     .add_int64_column(vec![Some(123), Some(456), Some(789)])
///     .add_timestamp_micros_column(vec![Some(1234567890), None, Some(9876543210)]);
///
/// let batch = builder.build()?;
/// ```
pub struct RecordBatchBuilder {
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}

impl RecordBatchBuilder {
    /// Create a new RecordBatchBuilder with the given schema.
    ///
    /// The schema defines the structure of the RecordBatch to be built.
    pub fn new(schema: SchemaRef) -> Self {
        let capacity = schema.fields().len();
        Self {
            schema,
            columns: Vec::with_capacity(capacity),
        }
    }

    fn push_array(&mut self, array: ArrayRef) -> &mut Self {
        self.columns.push(array);
        self
    }

    fn add_string_column_internal<I, S>(&mut self, data: I) -> &mut Self
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let iter = data.into_iter();
        let (lower, upper) = iter.size_hint();
        let len = upper.unwrap_or(lower);
        let mut builder = StringBuilder::with_capacity(len, len * 32);
        for value in iter {
            match value {
                Some(s) => builder.append_value(s.as_ref()),
                None => builder.append_null(),
            }
        }
        self.push_array(Arc::new(builder.finish()))
    }

    /// Add a string column (UTF-8) to the batch.
    ///
    /// # Arguments
    /// * `data` - Vector of optional string values
    pub fn add_string_column(&mut self, data: Vec<Option<&str>>) -> &mut Self {
        self.add_string_column_internal(data)
    }

    /// Add a string column from owned Strings.
    ///
    /// # Arguments
    /// * `data` - Vector of optional String values
    pub fn add_string_column_owned(&mut self, data: Vec<Option<String>>) -> &mut Self {
        self.add_string_column_internal(data)
    }

    /// Add an Int64 column to the batch.
    ///
    /// # Arguments
    /// * `data` - Vector of optional i64 values
    pub fn add_int64_column(&mut self, data: Vec<Option<i64>>) -> &mut Self {
        self.push_array(Arc::new(Int64Array::from(data)))
    }

    /// Add a UInt64 column to the batch.
    ///
    /// # Arguments
    /// * `data` - Vector of optional u64 values
    pub fn add_uint64_column(&mut self, data: Vec<Option<u64>>) -> &mut Self {
        self.push_array(Arc::new(UInt64Array::from(data)))
    }

    /// Add an Int32 column to the batch.
    ///
    /// # Arguments
    /// * `data` - Vector of optional i32 values
    pub fn add_int32_column(&mut self, data: Vec<Option<i32>>) -> &mut Self {
        self.push_array(Arc::new(Int32Array::from(data)))
    }

    /// Add a Float64 column to the batch.
    ///
    /// # Arguments
    /// * `data` - Vector of optional f64 values
    pub fn add_float64_column(&mut self, data: Vec<Option<f64>>) -> &mut Self {
        self.push_array(Arc::new(Float64Array::from(data)))
    }

    /// Add a Boolean column to the batch.
    ///
    /// # Arguments
    /// * `data` - Vector of optional bool values
    pub fn add_boolean_column(&mut self, data: Vec<Option<bool>>) -> &mut Self {
        self.push_array(Arc::new(BooleanArray::from(data)))
    }

    /// Add a TimestampMicrosecond column to the batch.
    ///
    /// KalamDB stores timestamps as milliseconds but Arrow requires microseconds.
    /// This method automatically converts millisecond timestamps to microseconds.
    ///
    /// # Arguments
    /// * `data` - Vector of optional i64 values in **milliseconds**
    pub fn add_timestamp_micros_column(&mut self, data: Vec<Option<i64>>) -> &mut Self {
        let micros: Vec<Option<i64>> = data.into_iter().map(|ts| ts.map(|ms| ms * 1000)).collect();
        self.push_array(Arc::new(TimestampMicrosecondArray::from(micros)))
    }

    /// Add a TimestampMillisecond column to the batch.
    ///
    /// Use this for timestamps that are already in milliseconds and don't need conversion.
    ///
    /// # Arguments
    /// * `data` - Vector of optional i64 values in **milliseconds**
    pub fn add_timestamp_millis_column(&mut self, data: Vec<Option<i64>>) -> &mut Self {
        self.push_array(Arc::new(TimestampMillisecondArray::from(data)))
    }

    /// Add a pre-built array column directly.
    ///
    /// Use this for custom array types not covered by the convenience methods.
    ///
    /// # Arguments
    /// * `array` - Arc-wrapped array implementing the Array trait
    pub fn add_array_column(&mut self, array: ArrayRef) -> &mut Self {
        self.push_array(array)
    }

    /// Build the RecordBatch from accumulated columns.
    ///
    /// # Returns
    /// * `Ok(RecordBatch)` if the batch was built successfully
    /// * `Err(ArrowError)` if column count mismatch or other Arrow errors occur
    ///
    /// # Errors
    /// - Column count doesn't match schema field count
    /// - Column types don't match schema field types
    /// - Column lengths are inconsistent
    pub fn build(self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(self.schema, self.columns)
    }

    /// Get the current number of columns added.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the expected number of columns from the schema.
    pub fn expected_column_count(&self) -> usize {
        self.schema.fields().len()
    }
}

/// Helper function to create an empty RecordBatch for a given schema.
///
/// Useful for queries that return no results but need a valid batch structure.
pub fn empty_batch(schema: SchemaRef) -> Result<RecordBatch, ArrowError> {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| arrow::array::new_empty_array(field.data_type()))
        .collect();
    RecordBatch::try_new(schema, columns)
}

use crate::models::rows::StoredScalarValue;

/// Compute min/max stats for a column, returning StoredScalarValue.
///
/// Returns values as StoredScalarValue, enabling:
/// - Zero-copy binary serialization for RocksDB manifest cache
/// - Proper JSON output for manifest.json files
/// - Type-safe comparisons in query planning
///
/// Returns `None` for empty arrays or when all values are null.
pub fn compute_min_max(array: &ArrayRef) -> (Option<StoredScalarValue>, Option<StoredScalarValue>) {
    match array.data_type() {
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::Int8(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::Int8(Some(v))),
            )
        },
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::Int16(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::Int16(Some(v))),
            )
        },
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::Int32(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::Int32(Some(v))),
            )
        },
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::Int64(Some(v.to_string()))),
                compute::max(arr).map(|v| StoredScalarValue::Int64(Some(v.to_string()))),
            )
        },
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::UInt8(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::UInt8(Some(v))),
            )
        },
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::UInt16(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::UInt16(Some(v))),
            )
        },
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::UInt32(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::UInt32(Some(v))),
            )
        },
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::UInt64(Some(v.to_string()))),
                compute::max(arr).map(|v| StoredScalarValue::UInt64(Some(v.to_string()))),
            )
        },
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::Float32(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::Float32(Some(v))),
            )
        },
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            (
                compute::min(arr).map(|v| StoredScalarValue::Float64(Some(v))),
                compute::max(arr).map(|v| StoredScalarValue::Float64(Some(v))),
            )
        },
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            (
                min_string(arr).map(|s| StoredScalarValue::Utf8(Some(s.to_string()))),
                max_string(arr).map(|s| StoredScalarValue::Utf8(Some(s.to_string()))),
            )
        },
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            (
                min_string(arr).map(|s| StoredScalarValue::LargeUtf8(Some(s.to_string()))),
                max_string(arr).map(|s| StoredScalarValue::LargeUtf8(Some(s.to_string()))),
            )
        },
        ArrowDataType::Boolean => (None, None),
        _ => (None, None),
    }
}

/// Extract a string representation of the value at `row_idx` from a supported array.
pub fn array_value_to_string(array: &dyn arrow::array::Array, row_idx: usize) -> Option<String> {
    match array.data_type() {
        ArrowDataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| arr.value(row_idx).to_string()),
        ArrowDataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| arr.value(row_idx).to_string()),
        ArrowDataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| arr.value(row_idx).to_string()),
        ArrowDataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|arr| arr.value(row_idx).to_string()),
        _ => None,
    }
}

/// Check if ArrowDataType is Int64.
pub fn is_int64(data_type: &ArrowDataType) -> bool {
    matches!(data_type, ArrowDataType::Int64)
}

/// Check if ArrowDataType is Boolean.
pub fn is_boolean(data_type: &ArrowDataType) -> bool {
    matches!(data_type, ArrowDataType::Boolean)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]))
    }

    #[test]
    fn test_basic_batch_builder() {
        let schema = test_schema();
        let mut builder = RecordBatchBuilder::new(schema);

        builder
            .add_string_column(vec![Some("id1"), Some("id2")])
            .add_int64_column(vec![Some(100), Some(200)])
            .add_timestamp_micros_column(vec![Some(1000), Some(2000)]);

        let batch = builder.build().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_empty_batch() {
        let schema = test_schema();
        let batch = empty_batch(schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_column_count_mismatch() {
        let schema = test_schema();
        let mut builder = RecordBatchBuilder::new(schema);

        // Only add 2 columns when schema expects 3
        builder.add_string_column(vec![Some("id1")]).add_int64_column(vec![Some(100)]);

        assert!(builder.build().is_err());
    }

    #[test]
    fn test_timestamp_conversion() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));

        let mut builder = RecordBatchBuilder::new(schema);
        builder.add_timestamp_micros_column(vec![Some(1000)]); // 1000ms = 1000000μs

        let batch = builder.build().unwrap();
        let ts_array =
            batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

        assert_eq!(ts_array.value(0), 1000000); // Converted to microseconds
    }
}
