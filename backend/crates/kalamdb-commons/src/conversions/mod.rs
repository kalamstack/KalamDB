//! Centralized data type and value conversion utilities
//!
//! This module provides a unified location for all conversion functions used throughout
//! the KalamDB codebase. The goal is to eliminate code duplication and provide a single
//! source of truth for all data type conversions.
//!
//! # Module Organization
//!
//! - `scalar_bytes` - ScalarValue ↔ bytes conversions (for index keys, storage keys)
//! - `scalar_numeric` - ScalarValue ↔ numeric types (f64, i64) conversions
//! - `scalar_string` - ScalarValue ↔ string conversions (for PKs, display)
//! - `scalar_size` - Memory size estimation for ScalarValue types
//!
//! # Usage Examples
//!
//! ```rust,ignore
//! use kalamdb_commons::conversions::{
//!     scalar_value_to_bytes,
//!     scalar_to_f64,
//!     scalar_to_pk_string,
//!     estimate_scalar_value_size,
//! };
//! use datafusion::scalar::ScalarValue;
//!
//! // Convert to bytes for indexing
//! let value = ScalarValue::Int64(Some(12345));
//! let bytes = scalar_value_to_bytes(&value);
//!
//! // Convert to f64 for arithmetic
//! let num = scalar_to_f64(&value).unwrap();
//!
//! // Convert to string for primary keys
//! let pk = scalar_to_pk_string(&value).unwrap();
//!
//! // Estimate memory usage
//! let size = estimate_scalar_value_size(&value);
//! ```

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;
#[cfg(feature = "conversions")]
pub mod arrow_json_conversion;
#[cfg(feature = "conversions")]
pub mod scalar_bytes;
#[cfg(feature = "conversions")]
pub mod scalar_json;
#[cfg(feature = "conversions")]
pub mod scalar_numeric;
#[cfg(feature = "conversions")]
pub mod scalar_size;
#[cfg(feature = "conversions")]
pub mod scalar_string;
#[cfg(feature = "schema-metadata")]
pub mod schema_metadata;

// Re-export commonly used functions at the module root for convenience
#[cfg(feature = "conversions")]
pub use arrow_json_conversion::*;
#[cfg(feature = "conversions")]
pub use scalar_bytes::scalar_value_to_bytes;
#[cfg(feature = "conversions")]
pub use scalar_json::{json_value_to_scalar_for_column, scalar_to_json_for_column};
#[cfg(feature = "conversions")]
pub use scalar_numeric::{as_f64, scalar_to_f64, scalar_to_i64};
#[cfg(feature = "conversions")]
pub use scalar_size::estimate_scalar_value_size;
#[cfg(feature = "conversions")]
pub use scalar_string::{parse_string_as_scalar, scalar_to_pk_string};
#[cfg(feature = "schema-metadata")]
pub use schema_metadata::{
    read_kalam_column_flags_metadata, read_kalam_data_type_metadata,
    with_kalam_column_flags_metadata, with_kalam_data_type_metadata,
    KALAM_COLUMN_FLAGS_METADATA_KEY, KALAM_DATA_TYPE_METADATA_KEY,
};
#[cfg(all(feature = "schema-metadata", feature = "arrow-conversion"))]
pub use schema_metadata::{mask_sensitive_rows_for_role, schema_fields_from_arrow_schema};
