//! Numeric conversions for ScalarValue
//!
//! Provides conversion functions between ScalarValue and numeric types (f64, i64, etc.)
//! Used for arithmetic operations, comparisons, and statistical calculations.

use datafusion_common::ScalarValue;

/// Convert ScalarValue to f64 for numeric operations
///
/// Handles all numeric types (integers, unsigned integers, floats) by converting
/// them to f64. Returns None for non-numeric types or null values.
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::conversions::scalar_to_f64;
/// use datafusion::scalar::ScalarValue;
///
/// let value = ScalarValue::Int64(Some(42));
/// assert_eq!(scalar_to_f64(&value), Some(42.0));
///
/// let float_value = ScalarValue::Float64(Some(3.14));
/// assert_eq!(scalar_to_f64(&value), Some(3.14));
/// ```
pub fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(f)) => Some(*f),
        ScalarValue::Float32(Some(f)) => Some(*f as f64),
        ScalarValue::Int64(Some(i)) => Some(*i as f64),
        ScalarValue::Int32(Some(i)) => Some(*i as f64),
        ScalarValue::Int16(Some(i)) => Some(*i as f64),
        ScalarValue::Int8(Some(i)) => Some(*i as f64),
        ScalarValue::UInt64(Some(u)) => Some(*u as f64),
        ScalarValue::UInt32(Some(u)) => Some(*u as f64),
        ScalarValue::UInt16(Some(u)) => Some(*u as f64),
        ScalarValue::UInt8(Some(u)) => Some(*u as f64),
        _ => None,
    }
}

/// Convert ScalarValue to i64 for integer operations
///
/// Handles integer and unsigned integer types by converting them to i64.
/// Returns None for non-integer types, floats, or null values.
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::conversions::scalar_to_i64;
/// use datafusion::scalar::ScalarValue;
///
/// let value = ScalarValue::Int32(Some(42));
/// assert_eq!(scalar_to_i64(&value), Some(42));
///
/// let uint_value = ScalarValue::UInt32(Some(100));
/// assert_eq!(scalar_to_i64(&uint_value), Some(100));
/// ```
pub fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(i)) => Some(*i),
        ScalarValue::Int32(Some(i)) => Some(*i as i64),
        ScalarValue::Int16(Some(i)) => Some(*i as i64),
        ScalarValue::Int8(Some(i)) => Some(*i as i64),
        ScalarValue::UInt64(Some(u)) => Some(*u as i64),
        ScalarValue::UInt32(Some(u)) => Some(*u as i64),
        ScalarValue::UInt16(Some(u)) => Some(*u as i64),
        ScalarValue::UInt8(Some(u)) => Some(*u as i64),
        _ => None,
    }
}

/// Alias for scalar_to_f64 for backwards compatibility
///
/// Some older code may use this name instead.
#[inline]
pub fn as_f64(value: &ScalarValue) -> Option<f64> {
    scalar_to_f64(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_to_f64_int() {
        assert_eq!(scalar_to_f64(&ScalarValue::Int64(Some(42))), Some(42.0));
        assert_eq!(scalar_to_f64(&ScalarValue::Int32(Some(-10))), Some(-10.0));
    }

    #[test]
    fn test_scalar_to_f64_float() {
        assert_eq!(scalar_to_f64(&ScalarValue::Float64(Some(3.14))), Some(3.14));
        assert_eq!(scalar_to_f64(&ScalarValue::Float32(Some(2.5))), Some(2.5));
    }

    #[test]
    fn test_scalar_to_f64_null() {
        assert_eq!(scalar_to_f64(&ScalarValue::Int64(None)), None);
        assert_eq!(scalar_to_f64(&ScalarValue::Utf8(Some("text".to_string()))), None);
    }

    #[test]
    fn test_scalar_to_i64() {
        assert_eq!(scalar_to_i64(&ScalarValue::Int32(Some(42))), Some(42));
        assert_eq!(scalar_to_i64(&ScalarValue::UInt32(Some(100))), Some(100));
        assert_eq!(scalar_to_i64(&ScalarValue::Int64(None)), None);
    }
}
