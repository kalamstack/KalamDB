//! Centralized data type and value conversion utilities
//!
//! Query-engine conversions (ScalarValue, Arrow, PK buckets) live here.
//! Persistence codecs live in `kalamdb-serialization`.

#[cfg(any(feature = "arrow-conversion", feature = "conversions"))]
pub mod arrow;
#[cfg(feature = "conversions")]
pub mod pk_bucket;
#[cfg(feature = "conversions")]
pub mod scalar;
#[cfg(feature = "schema-metadata")]
pub mod schema_metadata;

#[cfg(feature = "conversions")]
pub use arrow::json as arrow_json_conversion;
#[cfg(feature = "conversions")]
pub use arrow::json::*;
#[cfg(feature = "arrow-conversion")]
pub use arrow::types as arrow_conversion;
#[cfg(feature = "conversions")]
pub use pk_bucket::{
    pk_bucket_key_from_array, pk_bucket_key_from_row, pk_bucket_key_from_scalar,
    pk_bucket_key_from_typed_string, try_pk_bucket_key, try_pk_bucket_key_from_array,
    try_pk_bucket_key_from_typed_string, PkBucketKey,
};
#[cfg(feature = "conversions")]
pub use scalar::bytes as scalar_bytes;
#[cfg(feature = "conversions")]
pub use scalar::bytes::scalar_value_to_bytes;
#[cfg(feature = "conversions")]
pub use scalar::json as scalar_json;
#[cfg(feature = "conversions")]
pub use scalar::json::{json_value_to_scalar_for_column, scalar_to_json_for_column};
#[cfg(feature = "conversions")]
pub use scalar::numeric as scalar_numeric;
#[cfg(feature = "conversions")]
pub use scalar::numeric::{as_f64, scalar_to_f64, scalar_to_i64};
#[cfg(feature = "conversions")]
pub use scalar::size as scalar_size;
#[cfg(feature = "conversions")]
pub use scalar::size::estimate_scalar_value_size;
#[cfg(feature = "conversions")]
pub use scalar::string as scalar_string;
#[cfg(feature = "conversions")]
pub use scalar::string::{parse_string_as_scalar, scalar_to_pk_string};
#[cfg(all(feature = "schema-metadata", feature = "arrow-conversion"))]
pub use schema_metadata::{mask_sensitive_rows_for_role, schema_fields_from_arrow_schema};
#[cfg(feature = "schema-metadata")]
pub use schema_metadata::{
    read_kalam_column_flags_metadata, read_kalam_data_type_metadata,
    with_kalam_column_flags_metadata, with_kalam_data_type_metadata,
    KALAM_COLUMN_FLAGS_METADATA_KEY, KALAM_DATA_TYPE_METADATA_KEY,
};
