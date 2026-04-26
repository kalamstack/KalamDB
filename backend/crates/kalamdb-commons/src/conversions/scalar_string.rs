//! String conversions for ScalarValue
//!
//! Provides conversion functions between ScalarValue and string representations.
//! Used for primary key handling, display formatting, and serialization.

use arrow_schema::DataType;
use datafusion_common::ScalarValue;

/// Parse a string value into a ScalarValue with the specified Arrow DataType
///
/// This function is critical for UPDATE/DELETE operations that query Parquet files,
/// as Parquet uses strongly-typed columns and string comparisons against typed
/// columns (e.g., INT64) will fail. This ensures proper type coercion.
///
/// # Arguments
/// * `value_str` - String representation of the value (e.g., "12345" or "user_abc")
/// * `data_type` - Target Arrow DataType from schema
///
/// # Returns
/// ScalarValue with proper type for filtering/comparison
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::conversions::parse_string_as_scalar;
/// use arrow_schema::DataType;
/// use datafusion::scalar::ScalarValue;
///
/// // Parse string as Int64
/// let value = parse_string_as_scalar("12345", &DataType::Int64).unwrap();
/// assert_eq!(value, ScalarValue::Int64(Some(12345)));
///
/// // Parse string as Utf8
/// let value = parse_string_as_scalar("hello", &DataType::Utf8).unwrap();
/// assert_eq!(value, ScalarValue::Utf8(Some("hello".to_string())));
/// ```
pub fn parse_string_as_scalar(
    value_str: &str,
    data_type: &DataType,
) -> Result<ScalarValue, String> {
    match data_type {
        DataType::Int64 => {
            let val = value_str
                .parse::<i64>()
                .map_err(|e| format!("Failed to parse '{}' as Int64: {}", value_str, e))?;
            Ok(ScalarValue::Int64(Some(val)))
        },
        DataType::Int32 => {
            let val = value_str
                .parse::<i32>()
                .map_err(|e| format!("Failed to parse '{}' as Int32: {}", value_str, e))?;
            Ok(ScalarValue::Int32(Some(val)))
        },
        DataType::Int16 => {
            let val = value_str
                .parse::<i16>()
                .map_err(|e| format!("Failed to parse '{}' as Int16: {}", value_str, e))?;
            Ok(ScalarValue::Int16(Some(val)))
        },
        DataType::Int8 => {
            let val = value_str
                .parse::<i8>()
                .map_err(|e| format!("Failed to parse '{}' as Int8: {}", value_str, e))?;
            Ok(ScalarValue::Int8(Some(val)))
        },
        DataType::UInt64 => {
            let val = value_str
                .parse::<u64>()
                .map_err(|e| format!("Failed to parse '{}' as UInt64: {}", value_str, e))?;
            Ok(ScalarValue::UInt64(Some(val)))
        },
        DataType::UInt32 => {
            let val = value_str
                .parse::<u32>()
                .map_err(|e| format!("Failed to parse '{}' as UInt32: {}", value_str, e))?;
            Ok(ScalarValue::UInt32(Some(val)))
        },
        DataType::UInt16 => {
            let val = value_str
                .parse::<u16>()
                .map_err(|e| format!("Failed to parse '{}' as UInt16: {}", value_str, e))?;
            Ok(ScalarValue::UInt16(Some(val)))
        },
        DataType::UInt8 => {
            let val = value_str
                .parse::<u8>()
                .map_err(|e| format!("Failed to parse '{}' as UInt8: {}", value_str, e))?;
            Ok(ScalarValue::UInt8(Some(val)))
        },
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ScalarValue::Utf8(Some(value_str.to_string()))),
        DataType::Boolean => {
            let val = value_str
                .parse::<bool>()
                .map_err(|e| format!("Failed to parse '{}' as Boolean: {}", value_str, e))?;
            Ok(ScalarValue::Boolean(Some(val)))
        },
        DataType::Float64 => {
            let val = value_str
                .parse::<f64>()
                .map_err(|e| format!("Failed to parse '{}' as Float64: {}", value_str, e))?;
            Ok(ScalarValue::Float64(Some(val)))
        },
        DataType::Float32 => {
            let val = value_str
                .parse::<f32>()
                .map_err(|e| format!("Failed to parse '{}' as Float32: {}", value_str, e))?;
            Ok(ScalarValue::Float32(Some(val)))
        },
        other => {
            // For any other type, use string representation and let caller handle coercion
            log::warn!(
                "Unsupported data type {:?} for string parsing, using Utf8 representation for \
                 value '{}'",
                other,
                value_str
            );
            Ok(ScalarValue::Utf8(Some(value_str.to_string())))
        },
    }
}

/// Convert ScalarValue to string representation for primary keys
///
/// Converts various scalar types to their string representation suitable for
/// use as primary key values. Returns error for unsupported types.
///
/// # Supported Types
/// - Integers (Int8, Int16, Int32, Int64)
/// - Unsigned integers (UInt8, UInt16, UInt32, UInt64)
/// - Strings (Utf8, LargeUtf8)
/// - Booleans
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::conversions::scalar_to_pk_string;
/// use datafusion::scalar::ScalarValue;
///
/// let value = ScalarValue::Int64(Some(12345));
/// assert_eq!(scalar_to_pk_string(&value).unwrap(), "12345");
///
/// let str_value = ScalarValue::Utf8(Some("user123".to_string()));
/// assert_eq!(scalar_to_pk_string(&str_value).unwrap(), "user123");
/// ```
pub fn scalar_to_pk_string(value: &ScalarValue) -> Result<String, String> {
    match value {
        ScalarValue::Int64(Some(n)) => Ok(n.to_string()),
        ScalarValue::Int32(Some(n)) => Ok(n.to_string()),
        ScalarValue::Int16(Some(n)) => Ok(n.to_string()),
        ScalarValue::Int8(Some(n)) => Ok(n.to_string()),
        ScalarValue::UInt64(Some(n)) => Ok(n.to_string()),
        ScalarValue::UInt32(Some(n)) => Ok(n.to_string()),
        ScalarValue::UInt16(Some(n)) => Ok(n.to_string()),
        ScalarValue::UInt8(Some(n)) => Ok(n.to_string()),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s.clone()),
        ScalarValue::Boolean(Some(b)) => Ok(b.to_string()),
        _ => Err(format!("Unsupported primary key type: {:?}", value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_to_pk_string_int() {
        let value = ScalarValue::Int64(Some(12345));
        assert_eq!(scalar_to_pk_string(&value).unwrap(), "12345");
    }

    #[test]
    fn test_scalar_to_pk_string_string() {
        let value = ScalarValue::Utf8(Some("user123".to_string()));
        assert_eq!(scalar_to_pk_string(&value).unwrap(), "user123");
    }

    #[test]
    fn test_scalar_to_pk_string_bool() {
        let value = ScalarValue::Boolean(Some(true));
        assert_eq!(scalar_to_pk_string(&value).unwrap(), "true");
    }

    #[test]
    fn test_scalar_to_pk_string_null() {
        let value = ScalarValue::Int64(None);
        assert!(scalar_to_pk_string(&value).is_err());
    }

    #[test]
    fn test_parse_string_as_scalar_int64() {
        let value = parse_string_as_scalar("12345", &DataType::Int64).unwrap();
        assert_eq!(value, ScalarValue::Int64(Some(12345)));
    }

    #[test]
    fn test_parse_string_as_scalar_string() {
        let value = parse_string_as_scalar("hello", &DataType::Utf8).unwrap();
        assert_eq!(value, ScalarValue::Utf8(Some("hello".to_string())));
    }

    #[test]
    fn test_parse_string_as_scalar_bool() {
        let value = parse_string_as_scalar("true", &DataType::Boolean).unwrap();
        assert_eq!(value, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn test_parse_string_as_scalar_invalid() {
        let result = parse_string_as_scalar("not_a_number", &DataType::Int64);
        assert!(result.is_err());
    }
}
