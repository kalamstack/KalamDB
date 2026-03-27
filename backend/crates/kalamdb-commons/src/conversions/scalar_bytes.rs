//! Centralized datatype and value conversion utilities
//!
//! This module provides unified conversion functions for mapping between different data representations
//! (ScalarValue, bytes, Arrow types, etc.) used throughout the KalamDB codebase.
//!
//! The goal is to eliminate duplication of similar conversion logic scattered across multiple files
//! and provide a single source of truth for all datatype conversions.

use datafusion_common::ScalarValue;

/// Encode a scalar value to bytes for use in index keys or storage
///
/// Converts ScalarValue to a byte representation suitable for use as:
/// - Primary key index keys
/// - Secondary index keys
/// - Storage key prefixes
///
/// # Numeric Types
/// Integers and unsigned integers are converted to strings then bytes.
///
/// # String Types
/// UTF-8 strings are converted directly to bytes.
///
/// # Other Types
/// All other types are converted to their string representation then bytes.
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::conversions::scalar_value_to_bytes;
/// use arrow_schema::ScalarValue;
///
/// let value = ScalarValue::Int64(Some(12345));
/// let bytes = scalar_value_to_bytes(&value);
/// assert_eq!(bytes, b"12345".to_vec());
///
/// let str_value = ScalarValue::Utf8(Some("hello".to_string()));
/// let str_bytes = scalar_value_to_bytes(&str_value);
/// assert_eq!(str_bytes, b"hello".to_vec());
/// ```
pub fn scalar_value_to_bytes(value: &ScalarValue) -> Vec<u8> {
    if value.is_null() {
        return Vec::new();
    }
    match value {
        ScalarValue::Int64(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::Int32(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::Int16(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::Int8(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::UInt64(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::UInt32(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::UInt16(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::UInt8(Some(n)) => n.to_string().into_bytes(),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_bytes().to_vec(),
        // For other types, convert to string representation
        _ => value.to_string().into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_int64() {
        let value = ScalarValue::Int64(Some(12345));
        let bytes = scalar_value_to_bytes(&value);
        assert_eq!(bytes, b"12345".to_vec());
    }

    #[test]
    fn test_encode_utf8() {
        let value = ScalarValue::Utf8(Some("hello".to_string()));
        let bytes = scalar_value_to_bytes(&value);
        assert_eq!(bytes, b"hello".to_vec());
    }

    #[test]
    fn test_encode_null() {
        let value = ScalarValue::Utf8(None);
        let bytes = scalar_value_to_bytes(&value);
        assert_eq!(bytes, b"".to_vec());
    }

    #[test]
    fn test_encode_uint64() {
        let value = ScalarValue::UInt64(Some(999));
        let bytes = scalar_value_to_bytes(&value);
        assert_eq!(bytes, b"999".to_vec());
    }

    #[test]
    fn test_encode_int32() {
        let value = ScalarValue::Int32(Some(-42));
        let bytes = scalar_value_to_bytes(&value);
        assert_eq!(bytes, b"-42".to_vec());
    }
}
