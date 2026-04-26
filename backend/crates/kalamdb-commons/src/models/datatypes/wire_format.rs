//! Wire format encoding/decoding for KalamDataType
//!
//! Provides efficient binary serialization with tag bytes for type identification.
//! Format: [tag byte][optional dimension for EMBEDDING]

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
    InvalidDimension(usize),

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
        // Write tag byte
        writer.write_all(&[self.tag()])?;

        // For EMBEDDING, write dimension as 4-byte unsigned integer
        if let KalamDataType::Embedding(dim) = self {
            KalamDataType::validate_embedding_dimension(*dim)
                .map_err(|_| WireFormatError::InvalidDimension(*dim))?;
            writer.write_all(&(*dim as u32).to_le_bytes())?;
        }

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, WireFormatError> {
        // Read tag byte
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf)?;
        let tag = tag_buf[0];

        // Handle EMBEDDING specially (needs dimension)
        if tag == 0x0D {
            let mut dim_buf = [0u8; 4];
            reader.read_exact(&mut dim_buf).map_err(|_| WireFormatError::UnexpectedEof)?;
            let dim = u32::from_le_bytes(dim_buf) as usize;

            KalamDataType::validate_embedding_dimension(dim)
                .map_err(|_| WireFormatError::InvalidDimension(dim))?;

            Ok(KalamDataType::Embedding(dim))
        } else {
            // All other types
            KalamDataType::from_tag(tag).map_err(|_| WireFormatError::InvalidTag(tag))
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            KalamDataType::Embedding(_) => 5, // 1 byte tag + 4 bytes dimension
            _ => 1,                           // Just tag byte
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_simple_type_round_trip() {
        let types = vec![
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
        let dimensions = vec![1, 384, 768, 1536, 3072, 8192];

        for dim in dimensions {
            let original = KalamDataType::Embedding(dim);
            let mut buffer = Vec::new();
            original.encode(&mut buffer).unwrap();

            assert_eq!(buffer.len(), 5); // 1 tag + 4 dimension

            let mut cursor = Cursor::new(buffer);
            let decoded = KalamDataType::decode(&mut cursor).unwrap();

            assert_eq!(original, decoded);
        }
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
        assert_eq!(KalamDataType::Embedding(384).encoded_size(), 5);
    }

    #[test]
    fn test_invalid_tag() {
        let buffer = vec![0xFF]; // Invalid tag
        let mut cursor = Cursor::new(buffer);
        assert!(KalamDataType::decode(&mut cursor).is_err());
    }
}
