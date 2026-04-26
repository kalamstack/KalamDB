//! Serialization traits for KalamDB entity storage.
//!
//! This module provides the `KSerializable` trait which standardizes how
//! entities are serialized/deserialized for storage in RocksDB.

use serde::{Deserialize, Serialize};

use crate::storage::StorageError;

pub mod envelope;
pub mod generated;
pub mod row_codec;
pub mod schema;

type Result<T> = std::result::Result<T, StorageError>;

pub use envelope::{encode_envelope_inline, CodecKind, EntityEnvelope};

/// Trait implemented by values that can be stored in an [`EntityStore`].
///
/// Types can override `encode`/`decode` for custom storage formats (e.g.,
/// row envelopes vs. JSON). The default implementation uses FlexBuffers.
///
/// ## Example
///
/// ```rust
/// use kalamdb_commons::serialization::KSerializable;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyEntity {
///     id:    String,
///     value: i64,
/// }
///
/// impl KSerializable for MyEntity {}
/// ```
pub trait KSerializable: Serialize + for<'de> Deserialize<'de> + Send + Sync {
    fn encode(&self) -> Result<Vec<u8>> {
        flexbuffers::to_vec(self).map_err(|e| {
            StorageError::SerializationError(format!("flexbuffers encode failed: {}", e))
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        flexbuffers::from_slice(bytes).map_err(|e| {
            StorageError::SerializationError(format!("flexbuffers decode failed: {}", e))
        })
    }
}

// Blanket implementation for String (common storage type)
impl KSerializable for String {}

/// Encode a payload into a versioned entity envelope.
///
/// This helper establishes the envelope contract used during the migration away
/// from raw payloads. Callers are expected to increment `schema_version` on
/// wire changes.
pub fn encode_enveloped(
    codec_kind: CodecKind,
    schema_version: u16,
    payload: Vec<u8>,
) -> Result<Vec<u8>> {
    let envelope = EntityEnvelope::new(codec_kind, schema_version, payload);
    envelope.encode()
}

/// Like [`encode_enveloped`] but accepts the payload as `&[u8]` instead of
/// `Vec<u8>`, avoiding an intermediate allocation when the caller already has
/// a byte slice (e.g. from a `FlatBufferBuilder::finished_data()` or FlexBuffers
/// encode).
pub fn encode_enveloped_ref(
    codec_kind: CodecKind,
    schema_version: u16,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let envelope = EntityEnvelope::new(codec_kind, schema_version, payload.to_vec());
    envelope.encode()
}

/// Decode and validate an entity envelope.
///
/// `expected_schema_version` provides strict version validation.
pub fn decode_enveloped(bytes: &[u8], expected_schema_version: u16) -> Result<EntityEnvelope> {
    let envelope = EntityEnvelope::decode(bytes)?;
    envelope.validate(expected_schema_version)?;
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let bytes =
            encode_enveloped(CodecKind::FlatBuffers, 1, vec![1, 2, 3]).expect("encode envelope");

        let decoded = decode_enveloped(&bytes, 1).expect("decode envelope");
        assert_eq!(decoded.codec_kind, CodecKind::FlatBuffers);
        assert_eq!(decoded.payload, vec![1, 2, 3]);
    }

    #[test]
    fn envelope_schema_version_mismatch_rejected() {
        let bytes = encode_enveloped(CodecKind::FlatBuffers, 1, vec![9]).expect("encode envelope");

        let err = decode_enveloped(&bytes, 2).expect_err("schema version mismatch should fail");
        assert!(err.to_string().contains("schema_version mismatch"));
    }
}
