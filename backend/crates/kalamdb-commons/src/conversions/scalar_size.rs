//! Memory size estimation for ScalarValue
//!
//! Provides functions to estimate the memory footprint of scalar values.
//! Used for query planning, memory limits, and resource management.

use datafusion_common::ScalarValue;

/// Estimate memory size in bytes for a ScalarValue
///
/// Returns an approximate size in bytes for the given scalar value,
/// including heap-allocated data for strings and binary types.
///
/// # Size Estimates
/// - Fixed-size types (Int64, Float64, Boolean, etc.): 8 bytes
/// - Strings: 24 bytes (pointer + len + capacity) + string length
/// - Binary: 24 bytes + data length
/// - Null values: 1 byte
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::conversions::estimate_scalar_value_size;
/// use datafusion::scalar::ScalarValue;
///
/// let int_value = ScalarValue::Int64(Some(42));
/// assert_eq!(estimate_scalar_value_size(&int_value), 8);
///
/// let str_value = ScalarValue::Utf8(Some("hello".to_string()));
/// assert_eq!(estimate_scalar_value_size(&str_value), 24 + 5);
/// ```
pub fn estimate_scalar_value_size(value: &ScalarValue) -> usize {
    match value {
        // Null values - minimal overhead
        ScalarValue::Null => 1,

        // Fixed-size numeric types - 8 bytes each
        ScalarValue::Boolean(_) => 8,
        ScalarValue::Float64(_) => 8,
        ScalarValue::Float32(_) => 8,
        ScalarValue::Int8(_) => 8,
        ScalarValue::Int16(_) => 8,
        ScalarValue::Int32(_) => 8,
        ScalarValue::Int64(_) => 8,
        ScalarValue::UInt8(_) => 8,
        ScalarValue::UInt16(_) => 8,
        ScalarValue::UInt32(_) => 8,
        ScalarValue::UInt64(_) => 8,

        // String types - 24 bytes (String struct) + string length
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => 24 + s.len(),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => 24,

        // Binary types - 24 bytes (Vec struct) + data length
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => 24 + b.len(),
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => 24,

        // Timestamp types - 8 bytes for the i64 value
        ScalarValue::TimestampSecond(_, _) => 8,
        ScalarValue::TimestampMillisecond(_, _) => 8,
        ScalarValue::TimestampMicrosecond(_, _) => 8,
        ScalarValue::TimestampNanosecond(_, _) => 8,

        // Date types - 4 or 8 bytes
        ScalarValue::Date32(_) => 4,
        ScalarValue::Date64(_) => 8,

        // Time types
        ScalarValue::Time32Second(_) => 4,
        ScalarValue::Time32Millisecond(_) => 4,
        ScalarValue::Time64Microsecond(_) => 8,
        ScalarValue::Time64Nanosecond(_) => 8,

        // Duration types
        ScalarValue::DurationSecond(_) => 8,
        ScalarValue::DurationMillisecond(_) => 8,
        ScalarValue::DurationMicrosecond(_) => 8,
        ScalarValue::DurationNanosecond(_) => 8,

        // Interval types
        ScalarValue::IntervalYearMonth(_) => 4,
        ScalarValue::IntervalDayTime(_) => 8,
        ScalarValue::IntervalMonthDayNano(_) => 16,

        // Decimal types - 16 bytes for i128
        ScalarValue::Decimal128(_, _, _) => 16,
        ScalarValue::Decimal256(_, _, _) => 32,

        // List types - conservative estimate (24 bytes for Vec overhead + average element size)
        ScalarValue::List(_) => 24 + 64, // Assume average 64 bytes per list
        ScalarValue::LargeList(_) => 24 + 64,

        // Struct types - conservative estimate
        ScalarValue::Struct(_) => 64, // Assume 64 bytes average struct size

        // Dictionary types - assume 8 bytes for key + value size estimate
        ScalarValue::Dictionary(_, value) => 8 + estimate_scalar_value_size(value),

        // For any other types, use a conservative estimate
        _ => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_int() {
        assert_eq!(estimate_scalar_value_size(&ScalarValue::Int64(Some(42))), 8);
        assert_eq!(estimate_scalar_value_size(&ScalarValue::Int32(Some(10))), 8);
    }

    #[test]
    fn test_estimate_string() {
        let value = ScalarValue::Utf8(Some("hello".to_string()));
        assert_eq!(estimate_scalar_value_size(&value), 24 + 5);

        let empty = ScalarValue::Utf8(Some(String::new()));
        assert_eq!(estimate_scalar_value_size(&empty), 24);
    }

    #[test]
    fn test_estimate_null() {
        assert_eq!(estimate_scalar_value_size(&ScalarValue::Null), 1);
        assert_eq!(estimate_scalar_value_size(&ScalarValue::Int64(None)), 8);
    }

    #[test]
    fn test_estimate_binary() {
        let value = ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5]));
        assert_eq!(estimate_scalar_value_size(&value), 24 + 5);
    }
}
