//! Canonical catalog types for columns, type fields, routine signatures, and defaults.
//!
//! Adding a builtin is one variant here. Named `CREATE TYPE` identities stay
//! [`crate::models::TypeId`] and are not variants of this enum, so the type
//! remains `Copy` and compact for RocksDB keys and row metadata.

use std::{fmt, mem::size_of};

use serde::{Deserialize, Serialize};

/// Unified data type enum with wire format tags.
///
/// Each variant has an associated tag byte for [`super::WireFormat`]:
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    /// Fixed-size float32 vector for embeddings (0x0D).
    /// Dimension is `1..=8192`.
    Embedding(u16),

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

const _: () = assert!(size_of::<KalamDataType>() <= 4);

impl KalamDataType {
    /// Get the wire format tag byte for this type
    #[inline]
    pub const fn tag(&self) -> u8 {
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

    /// Create a unit [`KalamDataType`] from a wire format tag.
    ///
    /// EMBEDDING and DECIMAL require extra payload bytes; use [`super::WireFormat`].
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
            _ => Err(format!("Unknown type tag: 0x{tag:02X}")),
        }
    }

    /// Validate EMBEDDING dimension is within allowed range
    pub fn validate_embedding_dimension(dim: u16) -> Result<(), String> {
        if !(1..=8192).contains(&dim) {
            Err(format!("EMBEDDING dimension must be between 1 and 8192, got {dim}"))
        } else {
            Ok(())
        }
    }

    /// Validate DECIMAL precision and scale
    pub fn validate_decimal_params(precision: u8, scale: u8) -> Result<(), String> {
        if !(1..=38).contains(&precision) {
            return Err(format!("DECIMAL precision must be between 1 and 38, got {precision}"));
        }
        if scale > precision {
            return Err(format!("DECIMAL scale ({scale}) cannot exceed precision ({precision})"));
        }
        Ok(())
    }

    /// Types that support Parquet Bloom filters and equality min/max prune.
    ///
    /// Embeddings, JSON, bytes, files, and floating-point columns are skipped.
    pub fn supports_equality_bloom(&self) -> bool {
        matches!(
            self,
            Self::Boolean
                | Self::Int
                | Self::BigInt
                | Self::SmallInt
                | Self::Text
                | Self::Uuid
                | Self::Date
                | Self::Timestamp
                | Self::DateTime
                | Self::Time
                | Self::Decimal { .. }
        )
    }

    /// Static SQL name for types that do not carry parameters.
    #[inline]
    pub const fn sql_name_static(&self) -> Option<&'static str> {
        match self {
            KalamDataType::Boolean => Some("BOOLEAN"),
            KalamDataType::Int => Some("INT"),
            KalamDataType::BigInt => Some("BIGINT"),
            KalamDataType::Double => Some("DOUBLE"),
            KalamDataType::Float => Some("FLOAT"),
            KalamDataType::Text => Some("TEXT"),
            KalamDataType::Timestamp => Some("TIMESTAMP"),
            KalamDataType::Date => Some("DATE"),
            KalamDataType::DateTime => Some("DATETIME"),
            KalamDataType::Time => Some("TIME"),
            KalamDataType::Json => Some("JSON"),
            KalamDataType::Bytes => Some("BYTES"),
            KalamDataType::Uuid => Some("UUID"),
            KalamDataType::SmallInt => Some("SMALLINT"),
            KalamDataType::File => Some("FILE"),
            KalamDataType::Embedding(_) | KalamDataType::Decimal { .. } => None,
        }
    }

    /// SQL type name for display and catalog `type_name` columns.
    pub fn sql_name(&self) -> String {
        match self {
            KalamDataType::Embedding(dim) => format!("EMBEDDING({dim})"),
            KalamDataType::Decimal { precision, scale } => {
                format!("DECIMAL({precision}, {scale})")
            },
            other => other
                .sql_name_static()
                .expect("parameterized types are handled above")
                .to_string(),
        }
    }

    /// Parse a SQL type name such as `UUID`, `BOOLEAN`, `DECIMAL(10, 2)`.
    ///
    /// Named catalog types (`chat.message`) return `None`.
    pub fn from_sql_name(name: &str) -> Option<Self> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return None;
        }
        let (base, params) = split_sql_type_params(trimmed);
        if sql_type_key_eq(base, "boolean") || sql_type_key_eq(base, "bool") {
            return Some(KalamDataType::Boolean);
        }
        if sql_type_key_eq(base, "smallint")
            || sql_type_key_eq(base, "int2")
            || sql_type_key_eq(base, "tinyint")
            || sql_type_key_eq(base, "int1")
        {
            return Some(KalamDataType::SmallInt);
        }
        if sql_type_key_eq(base, "int")
            || sql_type_key_eq(base, "integer")
            || sql_type_key_eq(base, "int4")
        {
            return Some(KalamDataType::Int);
        }
        if sql_type_key_eq(base, "bigint")
            || sql_type_key_eq(base, "int8")
            || sql_type_key_eq(base, "int64")
        {
            return Some(KalamDataType::BigInt);
        }
        if sql_type_key_eq(base, "float")
            || sql_type_key_eq(base, "real")
            || sql_type_key_eq(base, "float4")
        {
            return Some(KalamDataType::Float);
        }
        if sql_type_key_eq(base, "double")
            || sql_type_key_eq(base, "float8")
            || sql_type_key_eq(base, "float64")
            || sql_type_key_eq(base, "doubleprecision")
        {
            return Some(KalamDataType::Double);
        }
        if sql_type_key_eq(base, "text")
            || sql_type_key_eq(base, "varchar")
            || sql_type_key_eq(base, "char")
            || sql_type_key_eq(base, "character")
            || sql_type_key_eq(base, "string")
        {
            return Some(KalamDataType::Text);
        }
        if sql_type_key_eq(base, "json") || sql_type_key_eq(base, "jsonb") {
            return Some(KalamDataType::Json);
        }
        if sql_type_key_eq(base, "file") {
            return Some(KalamDataType::File);
        }
        if sql_type_key_eq(base, "bytes")
            || sql_type_key_eq(base, "bytea")
            || sql_type_key_eq(base, "binary")
            || sql_type_key_eq(base, "blob")
            || sql_type_key_eq(base, "varbinary")
        {
            return Some(KalamDataType::Bytes);
        }
        if sql_type_key_eq(base, "date") {
            return Some(KalamDataType::Date);
        }
        if sql_type_key_eq(base, "timestamp") || sql_type_key_eq(base, "timestamptz") {
            return Some(KalamDataType::Timestamp);
        }
        if sql_type_key_eq(base, "datetime") {
            return Some(KalamDataType::DateTime);
        }
        if sql_type_key_eq(base, "time") {
            return Some(KalamDataType::Time);
        }
        if sql_type_key_eq(base, "uuid") {
            return Some(KalamDataType::Uuid);
        }
        if sql_type_key_eq(base, "decimal") || sql_type_key_eq(base, "numeric") {
            let (precision, scale) = parse_decimal_params(params)?;
            return Some(KalamDataType::Decimal { precision, scale });
        }
        if sql_type_key_eq(base, "embedding") {
            let dim = parse_embedding_dim(params)?;
            return Some(KalamDataType::Embedding(dim));
        }
        None
    }
}

fn split_sql_type_params(name: &str) -> (&str, Option<&str>) {
    let Some(open) = name.find('(') else {
        return (name, None);
    };
    let close = name.rfind(')').unwrap_or(name.len());
    if close <= open {
        return (name, None);
    }
    (name[..open].trim(), Some(name[open + 1..close].trim()))
}

fn parse_decimal_params(params: Option<&str>) -> Option<(u8, u8)> {
    let Some(params) = params.filter(|value| !value.is_empty()) else {
        return Some((38, 10));
    };
    let mut parts = params.split(',').map(str::trim);
    let precision = parts.next()?.parse::<u8>().ok()?;
    let scale = parts.next().unwrap_or("0").parse::<u8>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    KalamDataType::validate_decimal_params(precision, scale).ok()?;
    Some((precision, scale))
}

fn parse_embedding_dim(params: Option<&str>) -> Option<u16> {
    let dim = params?.parse::<u16>().ok()?;
    KalamDataType::validate_embedding_dimension(dim).ok()?;
    Some(dim)
}

/// Case-insensitive match that ignores ASCII whitespace in `input`.
///
/// `key` must be lowercase with no spaces (`"doubleprecision"`, `"int"`).
fn sql_type_key_eq(input: &str, key: &str) -> bool {
    let mut input = input
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .map(|byte| byte.to_ascii_lowercase());
    let mut key = key.bytes();
    loop {
        match (input.next(), key.next()) {
            (Some(left), Some(right)) if left == right => {},
            (None, None) => return true,
            _ => return false,
        }
    }
}

impl fmt::Display for KalamDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KalamDataType::Embedding(dim) => write!(f, "EMBEDDING({dim})"),
            KalamDataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({precision}, {scale})")
            },
            other => f.write_str(other.sql_name_static().expect("parameterized types are handled")),
        }
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
                scale:     2,
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
        assert!(KalamDataType::validate_decimal_params(10, 2).is_ok());
        assert!(KalamDataType::validate_decimal_params(38, 10).is_ok());
        assert!(KalamDataType::validate_decimal_params(18, 0).is_ok());
        assert!(KalamDataType::validate_decimal_params(5, 5).is_ok());

        assert!(KalamDataType::validate_decimal_params(0, 0).is_err());
        assert!(KalamDataType::validate_decimal_params(39, 2).is_err());
        assert!(KalamDataType::validate_decimal_params(10, 11).is_err());
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
                scale:     2,
            }
            .sql_name(),
            "DECIMAL(10, 2)"
        );
        assert_eq!(KalamDataType::SmallInt.sql_name(), "SMALLINT");
    }

    #[test]
    fn test_from_sql_name() {
        assert_eq!(KalamDataType::from_sql_name("UUID"), Some(KalamDataType::Uuid));
        assert_eq!(KalamDataType::from_sql_name("bool"), Some(KalamDataType::Boolean));
        assert_eq!(KalamDataType::from_sql_name("DOUBLE PRECISION"), Some(KalamDataType::Double));
        assert_eq!(KalamDataType::from_sql_name("varchar(255)"), Some(KalamDataType::Text));
        assert_eq!(
            KalamDataType::from_sql_name("DECIMAL(10, 2)"),
            Some(KalamDataType::Decimal {
                precision: 10,
                scale:     2,
            })
        );
        assert_eq!(
            KalamDataType::from_sql_name("EMBEDDING(384)"),
            Some(KalamDataType::Embedding(384))
        );
        assert_eq!(KalamDataType::from_sql_name("chat.message"), None);
        assert_eq!(
            KalamDataType::from_sql_name("DECIMAL"),
            Some(KalamDataType::Decimal {
                precision: 38,
                scale:     10,
            })
        );
    }

    #[test]
    fn enum_is_copy_and_compact() {
        let ty = KalamDataType::Embedding(384);
        let copied = ty;
        assert_eq!(ty, copied);
        assert!(size_of::<KalamDataType>() <= 4);
    }
}
