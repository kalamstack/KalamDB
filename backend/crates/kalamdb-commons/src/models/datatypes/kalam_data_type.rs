//! KalamDataType - Unified data type system for KalamDB
//!
//! This enum represents all supported data types in the system with deterministic
//! wire format tags for efficient serialization.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Unified data type enum with wire format tags
///
/// Each variant has an associated tag byte for wire format serialization:
/// - BOOLEAN = 0x01
/// - INT = 0x02 (32-bit signed integer)
/// - BIGINT = 0x03 (64-bit signed integer)
/// - DOUBLE = 0x04 (64-bit floating point)
/// - FLOAT = 0x05 (32-bit floating point)
/// - TEXT = 0x06 (UTF-8 string)
/// - TIMESTAMP = 0x07 (microseconds since epoch)
/// - DATE = 0x08 (days since epoch)
/// - DATETIME = 0x09 (datetime with timezone)
/// - TIME = 0x0A (time of day)
/// - JSON = 0x0B (JSON document)
/// - BYTES = 0x0C (binary data)
/// - EMBEDDING = 0x0D (fixed-size float32 vector with dimension parameter)
/// - UUID = 0x0E (128-bit universally unique identifier)
/// - DECIMAL = 0x0F (fixed-point decimal with precision and scale)
/// - SMALLINT = 0x10 (16-bit signed integer)
/// - FILE = 0x11 (file reference - stored as JSON FileRef object)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KalamDataType {
    /// Boolean type (0x01)
    Boolean,

    /// 32-bit signed integer (0x02)
    Int,

    /// 64-bit signed integer (0x03)
    BigInt,

    /// 64-bit floating point (0x04)
    Double,

    /// 32-bit floating point (0x05)
    Float,

    /// UTF-8 string (0x06)
    Text,

    /// Timestamp with microsecond precision (0x07)
    Timestamp,

    /// Date (days since epoch) (0x08)
    Date,

    /// DateTime with timezone (0x09)
    DateTime,

    /// Time of day (0x0A)
    Time,

    /// JSON document (0x0B)
    Json,

    /// Binary data (0x0C)
    Bytes,

    /// Fixed-size float32 vector for embeddings (0x0D)
    /// Parameter: dimension (1 ≤ dim ≤ 8192)
    Embedding(usize),

    /// UUID (128-bit universally unique identifier) (0x0E)
    /// Stored as 16 bytes in standard RFC 4122 format
    Uuid,

    /// Fixed-point decimal (0x0F)
    /// Parameters: precision (total digits 1-38), scale (decimal places 0-precision)
    /// Example: DECIMAL(10, 2) can store values like 12345678.90
    Decimal { precision: u8, scale: u8 },

    /// 16-bit signed integer (0x10)
    /// Range: -32,768 to 32,767
    SmallInt,

    /// File reference (0x11)
    /// Stored as a JSON FileRef object containing file metadata
    /// (id, subfolder, name, size, mime, sha256)
    File,
}

impl KalamDataType {
    /// Get the wire format tag byte for this type
    pub fn tag(&self) -> u8 {
        match self {
            KalamDataType::Boolean => 0x01,
            KalamDataType::Int => 0x02,
            KalamDataType::BigInt => 0x03,
            KalamDataType::Double => 0x04,
            KalamDataType::Float => 0x05,
            KalamDataType::Text => 0x06,
            KalamDataType::Timestamp => 0x07,
            KalamDataType::Date => 0x08,
            KalamDataType::DateTime => 0x09,
            KalamDataType::Time => 0x0A,
            KalamDataType::Json => 0x0B,
            KalamDataType::Bytes => 0x0C,
            KalamDataType::Embedding(_) => 0x0D,
            KalamDataType::Uuid => 0x0E,
            KalamDataType::Decimal { .. } => 0x0F,
            KalamDataType::SmallInt => 0x10,
            KalamDataType::File => 0x11,
        }
    }

    /// Create a KalamDataType from a wire format tag
    pub fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            0x01 => Ok(KalamDataType::Boolean),
            0x02 => Ok(KalamDataType::Int),
            0x03 => Ok(KalamDataType::BigInt),
            0x04 => Ok(KalamDataType::Double),
            0x05 => Ok(KalamDataType::Float),
            0x06 => Ok(KalamDataType::Text),
            0x07 => Ok(KalamDataType::Timestamp),
            0x08 => Ok(KalamDataType::Date),
            0x09 => Ok(KalamDataType::DateTime),
            0x0A => Ok(KalamDataType::Time),
            0x0B => Ok(KalamDataType::Json),
            0x0C => Ok(KalamDataType::Bytes),
            0x0D => Err("EMBEDDING type requires dimension parameter".to_string()),
            0x0E => Ok(KalamDataType::Uuid),
            0x0F => Err("DECIMAL type requires precision and scale parameters".to_string()),
            0x10 => Ok(KalamDataType::SmallInt),
            0x11 => Ok(KalamDataType::File),
            _ => Err(format!("Unknown type tag: 0x{:02X}", tag)),
        }
    }

    /// Validate EMBEDDING dimension is within allowed range
    pub fn validate_embedding_dimension(dim: usize) -> Result<(), String> {
        if !(1..=8192).contains(&dim) {
            Err(format!("EMBEDDING dimension must be between 1 and 8192, got {}", dim))
        } else {
            Ok(())
        }
    }

    /// Validate DECIMAL precision and scale
    pub fn validate_decimal_params(precision: u8, scale: u8) -> Result<(), String> {
        if !(1..=38).contains(&precision) {
            return Err(format!("DECIMAL precision must be between 1 and 38, got {}", precision));
        }
        if scale > precision {
            return Err(format!(
                "DECIMAL scale ({}) cannot exceed precision ({})",
                scale, precision
            ));
        }
        Ok(())
    }

    /// Get the SQL type name for display
    pub fn sql_name(&self) -> String {
        match self {
            KalamDataType::Boolean => "BOOLEAN".to_string(),
            KalamDataType::Int => "INT".to_string(),
            KalamDataType::BigInt => "BIGINT".to_string(),
            KalamDataType::Double => "DOUBLE".to_string(),
            KalamDataType::Float => "FLOAT".to_string(),
            KalamDataType::Text => "TEXT".to_string(),
            KalamDataType::Timestamp => "TIMESTAMP".to_string(),
            KalamDataType::Date => "DATE".to_string(),
            KalamDataType::DateTime => "DATETIME".to_string(),
            KalamDataType::Time => "TIME".to_string(),
            KalamDataType::Json => "JSON".to_string(),
            KalamDataType::Bytes => "BYTES".to_string(),
            KalamDataType::Embedding(dim) => format!("EMBEDDING({})", dim),
            KalamDataType::Uuid => "UUID".to_string(),
            KalamDataType::Decimal { precision, scale } => {
                format!("DECIMAL({}, {})", precision, scale)
            },
            KalamDataType::SmallInt => "SMALLINT".to_string(),
            KalamDataType::File => "FILE".to_string(),
        }
    }
}

impl fmt::Display for KalamDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.sql_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_values() {
        assert_eq!(KalamDataType::Boolean.tag(), 0x01);
        assert_eq!(KalamDataType::Int.tag(), 0x02);
        assert_eq!(KalamDataType::BigInt.tag(), 0x03);
        assert_eq!(KalamDataType::Double.tag(), 0x04);
        assert_eq!(KalamDataType::Float.tag(), 0x05);
        assert_eq!(KalamDataType::Text.tag(), 0x06);
        assert_eq!(KalamDataType::Timestamp.tag(), 0x07);
        assert_eq!(KalamDataType::Date.tag(), 0x08);
        assert_eq!(KalamDataType::DateTime.tag(), 0x09);
        assert_eq!(KalamDataType::Time.tag(), 0x0A);
        assert_eq!(KalamDataType::Json.tag(), 0x0B);
        assert_eq!(KalamDataType::Bytes.tag(), 0x0C);
        assert_eq!(KalamDataType::Embedding(384).tag(), 0x0D);
        assert_eq!(KalamDataType::Uuid.tag(), 0x0E);
        assert_eq!(
            KalamDataType::Decimal {
                precision: 10,
                scale: 2,
            }
            .tag(),
            0x0F
        );
        assert_eq!(KalamDataType::SmallInt.tag(), 0x10);
        assert_eq!(KalamDataType::File.tag(), 0x11);
    }

    #[test]
    fn test_from_tag() {
        assert_eq!(KalamDataType::from_tag(0x01).unwrap(), KalamDataType::Boolean);
        assert_eq!(KalamDataType::from_tag(0x06).unwrap(), KalamDataType::Text);
        assert_eq!(KalamDataType::from_tag(0x0E).unwrap(), KalamDataType::Uuid);
        assert_eq!(KalamDataType::from_tag(0x10).unwrap(), KalamDataType::SmallInt);
        assert_eq!(KalamDataType::from_tag(0x11).unwrap(), KalamDataType::File);
        assert!(KalamDataType::from_tag(0xFF).is_err());
        // DECIMAL and EMBEDDING require parameters
        assert!(KalamDataType::from_tag(0x0D).is_err());
        assert!(KalamDataType::from_tag(0x0F).is_err());
    }

    #[test]
    fn test_embedding_validation() {
        assert!(KalamDataType::validate_embedding_dimension(384).is_ok());
        assert!(KalamDataType::validate_embedding_dimension(768).is_ok());
        assert!(KalamDataType::validate_embedding_dimension(1536).is_ok());
        assert!(KalamDataType::validate_embedding_dimension(3072).is_ok());
        assert!(KalamDataType::validate_embedding_dimension(0).is_err());
        assert!(KalamDataType::validate_embedding_dimension(8193).is_err());
    }

    #[test]
    fn test_decimal_validation() {
        // Valid cases
        assert!(KalamDataType::validate_decimal_params(10, 2).is_ok());
        assert!(KalamDataType::validate_decimal_params(38, 10).is_ok());
        assert!(KalamDataType::validate_decimal_params(18, 0).is_ok());
        assert!(KalamDataType::validate_decimal_params(5, 5).is_ok());

        // Invalid cases
        assert!(KalamDataType::validate_decimal_params(0, 0).is_err()); // precision too small
        assert!(KalamDataType::validate_decimal_params(39, 2).is_err()); // precision too large
        assert!(KalamDataType::validate_decimal_params(10, 11).is_err()); // scale > precision
    }

    #[test]
    fn test_sql_name() {
        assert_eq!(KalamDataType::Boolean.sql_name(), "BOOLEAN");
        assert_eq!(KalamDataType::Text.sql_name(), "TEXT");
        assert_eq!(KalamDataType::Embedding(768).sql_name(), "EMBEDDING(768)");
        assert_eq!(KalamDataType::Uuid.sql_name(), "UUID");
        assert_eq!(
            KalamDataType::Decimal {
                precision: 10,
                scale: 2,
            }
            .sql_name(),
            "DECIMAL(10, 2)"
        );
        assert_eq!(KalamDataType::SmallInt.sql_name(), "SMALLINT");
    }
}
