//! ULID() function implementation
//!
//! This module provides a user-defined function for DataFusion that generates
//! ULID (Universally Unique Lexicographically Sortable Identifier).
//!
//! ULID format:
//! - 48 bits: Unix timestamp in milliseconds
//! - 80 bits: randomness
//! - Encoded as 26-character Crockford Base32 string
//!
//! ULIDs are URL-safe, case-insensitive, and time-ordered, making them
//! suitable for PRIMARY KEY columns and correlation IDs.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::array::{ArrayRef, StringArray},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};
use kalamdb_commons::arrow_utils::{arrow_utf8, ArrowDataType};
use ulid::Ulid;

/// ULID() scalar function implementation
///
/// Generates a 128-bit globally unique identifier using ULID specification.
/// The ULID includes a 48-bit timestamp for time-based ordering and is
/// encoded as a 26-character Crockford Base32 string.
///
/// # Returns
/// - STRING (Utf8) - A 26-character ULID in Crockford Base32 encoding
///
/// # Properties
/// - VOLATILE: Generates a new ULID on each invocation
/// - Time-ordered: ULIDs increase monotonically with time
/// - URL-safe: Uses Crockford Base32 (no ambiguous characters)
/// - Case-insensitive: Can be stored in uppercase or lowercase
/// - Globally unique: Extremely low collision probability
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UlidFunction;

impl UlidFunction {
    /// Create a new ULID function
    pub fn new() -> Self {
        Self
    }

    /// Generate a single ULID
    fn generate_ulid(&self) -> String {
        Ulid::new().to_string()
    }
}

impl Default for UlidFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for UlidFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ulid"
    }

    fn signature(&self) -> &Signature {
        // Static signature with no arguments
        static SIGNATURE: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::exact(vec![], Volatility::Volatile))
    }

    fn return_type(&self, _args: &[ArrowDataType]) -> DataFusionResult<ArrowDataType> {
        Ok(arrow_utf8())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if !args.args.is_empty() {
            return Err(DataFusionError::Plan("ULID() takes no arguments".to_string()));
        }
        let mut ulids = Vec::with_capacity(args.number_rows);
        for _ in 0..args.number_rows {
            ulids.push(self.generate_ulid());
        }
        let array = StringArray::from(ulids);
        Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use datafusion::logical_expr::ScalarUDF;

    use super::*;

    #[test]
    fn test_ulid_function_creation() {
        let func_impl = UlidFunction::new();
        let func = ScalarUDF::new_from_impl(func_impl);
        assert_eq!(func.name(), "ulid");
    }

    #[test]
    fn test_ulid_generation() {
        let func_impl = UlidFunction::new();
        let ulid1 = func_impl.generate_ulid();
        let ulid2 = func_impl.generate_ulid();

        // Verify length (26 characters)
        assert_eq!(ulid1.len(), 26);
        assert_eq!(ulid2.len(), 26);

        // Verify uniqueness
        assert_ne!(ulid1, ulid2);
    }

    #[test]
    fn test_ulid_format_compliance() {
        let func_impl = UlidFunction::new();
        let ulid_str = func_impl.generate_ulid();

        // ULID should be exactly 26 characters
        assert_eq!(ulid_str.len(), 26, "ULID should be 26 characters");

        // Verify Crockford Base32: 0-9, A-Z excluding I, L, O, U
        for c in ulid_str.chars() {
            assert!(c.is_ascii_alphanumeric(), "ULID should only contain alphanumeric characters");

            let upper_c = c.to_ascii_uppercase();
            assert!(
                !matches!(upper_c, 'I' | 'L' | 'O' | 'U'),
                "ULID should not contain I, L, O, U (Crockford Base32 excludes these)"
            );
        }
    }

    #[test]
    fn test_ulid_uniqueness() {
        let func_impl = UlidFunction::new();
        let mut ulids = HashSet::new();

        // Generate 10000 ULIDs and ensure no duplicates
        for _ in 0..10000 {
            let ulid = func_impl.generate_ulid();
            assert!(ulids.insert(ulid.clone()), "Duplicate ULID detected: {}", ulid);
        }
    }

    #[test]
    fn test_ulid_time_ordering() {
        let func_impl = UlidFunction::new();
        let ulid1 = func_impl.generate_ulid();

        // Small delay to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(2));

        let ulid2 = func_impl.generate_ulid();

        // ULIDs should be lexicographically ordered by time
        // (timestamp is in the first 10 characters)
        assert!(ulid1 < ulid2, "ULID should be time-ordered: {} < {}", ulid1, ulid2);
    }

    #[test]
    fn test_ulid_timestamp_component() {
        let func_impl = UlidFunction::new();
        let ulid_str = func_impl.generate_ulid();

        // First 10 characters are the timestamp component
        let timestamp_part = &ulid_str[0..10];
        assert_eq!(timestamp_part.len(), 10, "Timestamp component should be 10 characters");

        // Timestamp should be valid Crockford Base32
        for c in timestamp_part.chars() {
            assert!(c.is_ascii_alphanumeric());
        }
    }

    #[test]
    fn test_ulid_invoke() {
        let func_impl = UlidFunction::new();
        let ulid_str = func_impl.generate_ulid();
        assert_eq!(ulid_str.len(), 26);
    }

    #[test]
    // Removed: direct invoke with arguments test due to DataFusion API changes
    fn test_ulid_return_type() {
        let func_impl = UlidFunction::new();
        let return_type = func_impl.return_type(&[]);
        assert!(return_type.is_ok());
        assert_eq!(return_type.unwrap(), ArrowDataType::Utf8);
    }

    #[test]
    fn test_ulid_parseable() {
        let func_impl = UlidFunction::new();
        let ulid_str = func_impl.generate_ulid();

        // Verify the string can be parsed back into a ULID
        let parsed = ulid_str.parse::<Ulid>();
        assert!(parsed.is_ok(), "Generated ULID should be parseable: {}", ulid_str);
    }
}
