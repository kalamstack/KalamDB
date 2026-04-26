//! Arrow type conversion for KalamDataType
//!
//! Provides bidirectional conversion between KalamDataType and Apache Arrow DataType.

use arrow_schema::{DataType as ArrowDataType, Field, TimeUnit};
use thiserror::Error;

use crate::models::datatypes::KalamDataType;

#[derive(Error, Debug)]
pub enum ArrowConversionError {
    #[error("Unsupported Arrow type: {0:?}")]
    UnsupportedArrowType(ArrowDataType),

    #[error("Invalid EMBEDDING dimension: {0}")]
    InvalidEmbeddingDimension(i32),

    #[error("Type conversion failed: {0}")]
    ConversionFailed(String),
}

/// Trait for converting to Arrow DataType
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::models::datatypes::{KalamDataType, ToArrowType};
/// use arrow_schema::DataType as ArrowDataType;
///
/// let kalam_type = KalamDataType::Decimal { precision: 10, scale: 2 };
/// let arrow_type = kalam_type.to_arrow_type().unwrap();
///
/// assert_eq!(arrow_type, ArrowDataType::Decimal128(10, 2));
/// ```
pub trait ToArrowType {
    /// Convert to Arrow DataType
    fn to_arrow_type(&self) -> Result<ArrowDataType, ArrowConversionError>;
}

/// Trait for converting from Arrow DataType
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_commons::models::datatypes::{KalamDataType, FromArrowType};
/// use arrow_schema::DataType as ArrowDataType;
///
/// let arrow_type = ArrowDataType::Float64;
/// let kalam_type = KalamDataType::from_arrow_type(&arrow_type).unwrap();
///
/// assert_eq!(kalam_type, KalamDataType::Double);
/// ```
pub trait FromArrowType {
    /// Convert from Arrow DataType
    fn from_arrow_type(arrow_type: &ArrowDataType) -> Result<Self, ArrowConversionError>
    where
        Self: Sized;
}

impl ToArrowType for KalamDataType {
    fn to_arrow_type(&self) -> Result<ArrowDataType, ArrowConversionError> {
        let arrow_type = match self {
            KalamDataType::Boolean => ArrowDataType::Boolean,
            KalamDataType::Int => ArrowDataType::Int32,
            KalamDataType::BigInt => ArrowDataType::Int64,
            KalamDataType::Double => ArrowDataType::Float64,
            KalamDataType::Float => ArrowDataType::Float32,
            KalamDataType::Text => ArrowDataType::Utf8,
            KalamDataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            KalamDataType::Date => ArrowDataType::Date32,
            KalamDataType::DateTime => {
                // DateTime with timezone stored as Timestamp (microsecond precision) UTC
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            },
            KalamDataType::Time => ArrowDataType::Time64(TimeUnit::Microsecond),
            KalamDataType::Json => ArrowDataType::Utf8, // JSON stored as UTF-8 string
            KalamDataType::Bytes => ArrowDataType::Binary,
            KalamDataType::Embedding(dim) => {
                // EMBEDDING(N) → FixedSizeList<Float32>
                let field = Field::new("item", ArrowDataType::Float32, false);
                ArrowDataType::FixedSizeList(std::sync::Arc::new(field), *dim as i32)
            },
            KalamDataType::Uuid => {
                // UUID stored as 16-byte binary (RFC 4122 format)
                ArrowDataType::FixedSizeBinary(16)
            },
            KalamDataType::Decimal { precision, scale } => {
                // DECIMAL → Decimal128
                ArrowDataType::Decimal128(*precision, *scale as i8)
            },
            KalamDataType::SmallInt => ArrowDataType::Int16,
            KalamDataType::File => {
                // FILE → Utf8 (stored as JSON string containing FileRef)
                ArrowDataType::Utf8
            },
        };

        Ok(arrow_type)
    }
}

impl FromArrowType for KalamDataType {
    fn from_arrow_type(arrow_type: &ArrowDataType) -> Result<Self, ArrowConversionError> {
        let kalam_type = match arrow_type {
            ArrowDataType::Boolean => KalamDataType::Boolean,
            ArrowDataType::Int16 => KalamDataType::SmallInt,
            ArrowDataType::Int32 => KalamDataType::Int,
            ArrowDataType::Int64 => KalamDataType::BigInt,
            ArrowDataType::Float64 => KalamDataType::Double,
            ArrowDataType::Float32 => KalamDataType::Float,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => KalamDataType::Text,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => KalamDataType::Timestamp,
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => KalamDataType::Timestamp,
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => KalamDataType::Timestamp,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => KalamDataType::DateTime,
            ArrowDataType::Timestamp(TimeUnit::Millisecond, Some(_)) => KalamDataType::DateTime,
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => KalamDataType::DateTime,
            ArrowDataType::Date32 => KalamDataType::Date,
            ArrowDataType::Date64 => KalamDataType::Date,
            ArrowDataType::Time32(TimeUnit::Second) => KalamDataType::Time,
            ArrowDataType::Time32(TimeUnit::Millisecond) => KalamDataType::Time,
            ArrowDataType::Time64(TimeUnit::Microsecond) => KalamDataType::Time,
            ArrowDataType::Time64(TimeUnit::Nanosecond) => KalamDataType::Time,
            ArrowDataType::Binary | ArrowDataType::LargeBinary => KalamDataType::Bytes,
            ArrowDataType::FixedSizeBinary(16) => KalamDataType::Uuid,
            ArrowDataType::FixedSizeBinary(_) => KalamDataType::Bytes,
            ArrowDataType::Decimal128(precision, scale) => {
                if *precision < 1 || *precision > 38 {
                    return Err(ArrowConversionError::ConversionFailed(format!(
                        "Decimal precision {} out of range (1-38)",
                        precision
                    )));
                }
                KalamDataType::Decimal {
                    precision: *precision,
                    scale: *scale as u8,
                }
            },
            ArrowDataType::FixedSizeList(field, size) => {
                // FixedSizeList<Float32> → EMBEDDING
                if matches!(field.data_type(), ArrowDataType::Float32) {
                    if *size < 1 || *size > 8192 {
                        return Err(ArrowConversionError::InvalidEmbeddingDimension(*size));
                    }
                    KalamDataType::Embedding(*size as usize)
                } else {
                    return Err(ArrowConversionError::UnsupportedArrowType(arrow_type.clone()));
                }
            },
            _ => return Err(ArrowConversionError::UnsupportedArrowType(arrow_type.clone())),
        };

        Ok(kalam_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_types_round_trip() {
        let types = vec![
            KalamDataType::Boolean,
            KalamDataType::SmallInt,
            KalamDataType::Int,
            KalamDataType::BigInt,
            KalamDataType::Double,
            KalamDataType::Float,
            KalamDataType::Text,
            KalamDataType::Timestamp,
            KalamDataType::Date,
            KalamDataType::DateTime,
            KalamDataType::Time,
            KalamDataType::Bytes,
            KalamDataType::Uuid,
        ];

        for original in types {
            let arrow = original.to_arrow_type().unwrap();
            let round_trip = KalamDataType::from_arrow_type(&arrow).unwrap();
            assert_eq!(original, round_trip, "Failed round-trip for {:?}", original);
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid_type = KalamDataType::Uuid;
        let arrow = uuid_type.to_arrow_type().unwrap();

        // Verify Arrow type structure
        assert!(matches!(arrow, ArrowDataType::FixedSizeBinary(16)));

        // Round-trip
        let round_trip = KalamDataType::from_arrow_type(&arrow).unwrap();
        assert_eq!(uuid_type, round_trip);
    }

    #[test]
    fn test_decimal_conversion() {
        let test_cases = vec![
            (10, 2),  // Money
            (18, 0),  // Large integers
            (38, 10), // High precision
            (5, 5),   // All decimal
        ];

        for (precision, scale) in test_cases {
            let original = KalamDataType::Decimal { precision, scale };
            let arrow = original.to_arrow_type().unwrap();

            // Verify Arrow type structure
            if let ArrowDataType::Decimal128(p, s) = arrow {
                assert_eq!(p, precision);
                assert_eq!(s, scale as i8);
            } else {
                panic!("Expected Decimal128, got {:?}", arrow);
            }

            // Round-trip
            let round_trip = KalamDataType::from_arrow_type(&arrow).unwrap();
            assert_eq!(original, round_trip);
        }
    }

    #[test]
    fn test_smallint_conversion() {
        let original = KalamDataType::SmallInt;
        let arrow = original.to_arrow_type().unwrap();

        assert!(matches!(arrow, ArrowDataType::Int16));

        let round_trip = KalamDataType::from_arrow_type(&arrow).unwrap();
        assert_eq!(original, round_trip);
    }

    #[test]
    fn test_embedding_conversion() {
        let dimensions = vec![1, 384, 768, 1536, 3072, 8192];

        for dim in dimensions {
            let original = KalamDataType::Embedding(dim);
            let arrow = original.to_arrow_type().unwrap();

            // Verify Arrow type structure
            if let ArrowDataType::FixedSizeList(ref field, size) = arrow {
                assert_eq!(size, dim as i32);
                assert!(matches!(field.data_type(), ArrowDataType::Float32));
            } else {
                panic!("Expected FixedSizeList, got {:?}", arrow);
            }

            // Round-trip
            let round_trip = KalamDataType::from_arrow_type(&arrow).unwrap();
            assert_eq!(original, round_trip);
        }
    }

    #[test]
    fn test_invalid_embedding_dimension() {
        let invalid_arrow = ArrowDataType::FixedSizeList(
            std::sync::Arc::new(Field::new("item", ArrowDataType::Float32, false)),
            9999,
        );

        assert!(KalamDataType::from_arrow_type(&invalid_arrow).is_err());
    }

    #[test]
    fn test_unsupported_arrow_type() {
        // Use a truly unsupported type (not Decimal128 which is now supported)
        let unsupported = ArrowDataType::Decimal256(76, 10);
        assert!(KalamDataType::from_arrow_type(&unsupported).is_err());

        // Also test Duration (time interval) - not yet supported
        let unsupported_duration = ArrowDataType::Duration(TimeUnit::Second);
        assert!(KalamDataType::from_arrow_type(&unsupported_duration).is_err());
    }

    #[test]
    fn test_timestamp_variants() {
        // Timestamp without timezone (microsecond precision)
        let ts = KalamDataType::Timestamp;
        let arrow = ts.to_arrow_type().unwrap();
        assert!(matches!(arrow, ArrowDataType::Timestamp(TimeUnit::Microsecond, None)));

        // DateTime with timezone (microsecond precision UTC)
        let dt = KalamDataType::DateTime;
        let arrow_dt = dt.to_arrow_type().unwrap();
        assert!(matches!(arrow_dt, ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_))));
    }
}
