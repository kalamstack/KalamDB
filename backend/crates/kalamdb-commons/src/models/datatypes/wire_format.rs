//! Compact binary encoding for [`KalamDataType`].
//!
//! Layout:
//! - unit types: `[tag]`
//! - EMBEDDING: `[0x0D][dimension u16 LE]`
//! - DECIMAL: `[0x0F][precision u8][scale u8]`

use std::io::{Read, Write};

use thiserror::Error;

use crate::models::datatypes::KalamDataType;

#[derive(Error, Debug)]
pub enum WireFormatError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid type tag: {0:#x}")]
    InvalidTag(u8),

    #[error("Invalid EMBEDDING dimension: {0}")]
    InvalidDimension(u16),

    #[error("Invalid DECIMAL precision {precision} scale {scale}")]
    InvalidDecimal { precision: u8, scale: u8 },

    #[error("Unexpected end of data")]
    UnexpectedEof,
}

/// Trait for types that can be serialized to wire format
pub trait WireFormat: Sized {
    /// Encode this type to wire format
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), WireFormatError>;

    /// Decode this type from wire format
    fn decode<R: Read>(reader: &mut R) -> Result<Self, WireFormatError>;

    /// Estimate encoded size in bytes
    fn encoded_size(&self) -> usize;
}

impl WireFormat for KalamDataType {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), WireFormatError> {
        writer.write_all(&[self.tag()])?;
        match self {
            KalamDataType::Embedding(dim) => {
                KalamDataType::validate_embedding_dimension(*dim)
                    .map_err(|_| WireFormatError::InvalidDimension(*dim))?;
                writer.write_all(&dim.to_le_bytes())?;
            },
            KalamDataType::Decimal { precision, scale } => {
                KalamDataType::validate_decimal_params(*precision, *scale).map_err(|_| {
                    WireFormatError::InvalidDecimal {
                        precision: *precision,
                        scale:     *scale,
                    }
                })?;
                writer.write_all(&[*precision, *scale])?;
            },
            _ => {},
        }
        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, WireFormatError> {
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf)?;
        let tag = tag_buf[0];

        match tag {
            0x0D => {
                let mut dim_buf = [0u8; 2];
                reader.read_exact(&mut dim_buf).map_err(|_| WireFormatError::UnexpectedEof)?;
                let dim = u16::from_le_bytes(dim_buf);
                KalamDataType::validate_embedding_dimension(dim)
                    .map_err(|_| WireFormatError::InvalidDimension(dim))?;
                Ok(KalamDataType::Embedding(dim))
            },
            0x0F => {
                let mut params = [0u8; 2];
                reader.read_exact(&mut params).map_err(|_| WireFormatError::UnexpectedEof)?;
                let precision = params[0];
                let scale = params[1];
                KalamDataType::validate_decimal_params(precision, scale)
                    .map_err(|_| WireFormatError::InvalidDecimal { precision, scale })?;
                Ok(KalamDataType::Decimal { precision, scale })
            },
            _ => KalamDataType::from_tag(tag).map_err(|_| WireFormatError::InvalidTag(tag)),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            KalamDataType::Embedding(_) | KalamDataType::Decimal { .. } => 3,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_simple_type_round_trip() {
        let types = [
            KalamDataType::Boolean,
            KalamDataType::Int,
            KalamDataType::BigInt,
            KalamDataType::Double,
            KalamDataType::Float,
            KalamDataType::Text,
            KalamDataType::Timestamp,
            KalamDataType::Date,
            KalamDataType::DateTime,
            KalamDataType::Time,
            KalamDataType::Json,
            KalamDataType::Bytes,
            KalamDataType::Uuid,
            KalamDataType::SmallInt,
            KalamDataType::File,
        ];

        for original in types {
            let mut buffer = Vec::new();
            original.encode(&mut buffer).unwrap();

            let mut cursor = Cursor::new(buffer);
            let decoded = KalamDataType::decode(&mut cursor).unwrap();

            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_embedding_round_trip() {
        let dimensions = [1u16, 384, 768, 1536, 3072, 8192];

        for dim in dimensions {
            let original = KalamDataType::Embedding(dim);
            let mut buffer = Vec::new();
            original.encode(&mut buffer).unwrap();

            assert_eq!(buffer.len(), 3);

            let mut cursor = Cursor::new(buffer);
            let decoded = KalamDataType::decode(&mut cursor).unwrap();

            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_decimal_round_trip() {
        let original = KalamDataType::Decimal {
            precision: 10,
            scale:     2,
        };
        let mut buffer = Vec::new();
        original.encode(&mut buffer).unwrap();
        assert_eq!(buffer, vec![0x0F, 10, 2]);

        let decoded = KalamDataType::decode(&mut Cursor::new(buffer)).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_invalid_embedding_dimension() {
        let invalid = KalamDataType::Embedding(0);
        let mut buffer = Vec::new();
        assert!(invalid.encode(&mut buffer).is_err());

        let invalid = KalamDataType::Embedding(9999);
        let mut buffer = Vec::new();
        assert!(invalid.encode(&mut buffer).is_err());
    }

    #[test]
    fn test_encoded_size() {
        assert_eq!(KalamDataType::Boolean.encoded_size(), 1);
        assert_eq!(KalamDataType::Text.encoded_size(), 1);
        assert_eq!(KalamDataType::Embedding(384).encoded_size(), 3);
        assert_eq!(
            KalamDataType::Decimal {
                precision: 10,
                scale:     2,
            }
            .encoded_size(),
            3
        );
    }

    #[test]
    fn test_invalid_tag() {
        let buffer = vec![0xFF];
        let mut cursor = Cursor::new(buffer);
        assert!(KalamDataType::decode(&mut cursor).is_err());
    }
}
