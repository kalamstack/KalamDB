//! Kalam Object Protocol envelope.
//!
//! Callers choose a semantic [`ObjectKind`]. Codec choice stays inside this crate.

use crate::{
    error::{Result, SerializationError},
    version::{MAGIC, PROTOCOL_VERSION},
};

/// Semantic persisted-object class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectKind {
    /// Generic structured catalog/system objects.
    Generic = 1,
    /// Hot table rows.
    Row = 2,
    /// Typed protocol objects such as Raft commands.
    Protocol = 3,
    /// Stream log records.
    Stream = 4,
}

impl ObjectKind {
    pub(crate) fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Generic),
            2 => Ok(Self::Row),
            3 => Ok(Self::Protocol),
            4 => Ok(Self::Stream),
            other => Err(SerializationError::Decode(format!("unknown object kind {other}"))),
        }
    }
}

/// Owned encoded object bytes. Does not re-serialize on access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedObject {
    bytes: Vec<u8>,
}

impl EncodedObject {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Borrow the encoded bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Encoded length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the object is empty (should not happen for a valid envelope).
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Consume and return the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Header parsed from a valid envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeHeader {
    pub protocol_version: u16,
    pub object_kind:      ObjectKind,
    pub schema_version:   u16,
    pub flags:            u16,
}

const HEADER_LEN: usize = 4 + 2 + 1 + 2 + 2 + 4;

/// True when `bytes` starts with the Kalam object magic (`KOBJ`).
pub fn has_object_magic(bytes: &[u8]) -> bool {
    bytes.len() >= MAGIC.len() && bytes[..MAGIC.len()] == MAGIC
}

pub(crate) fn encode_envelope(
    object_kind: ObjectKind,
    schema_version: u16,
    payload: &[u8],
) -> Result<EncodedObject> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| SerializationError::Encode("payload exceeds u32 length".to_string()))?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    bytes.push(object_kind as u8);
    bytes.extend_from_slice(&schema_version.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(EncodedObject::from_bytes(bytes))
}

pub(crate) fn decode_envelope(
    bytes: &[u8],
    expected_kind: ObjectKind,
) -> Result<(EnvelopeHeader, &[u8])> {
    if bytes.len() < HEADER_LEN {
        return Err(SerializationError::Truncated);
    }
    if bytes[0..4] != MAGIC {
        return Err(SerializationError::InvalidMagic);
    }
    let protocol_version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if protocol_version != PROTOCOL_VERSION {
        return Err(SerializationError::UnsupportedProtocolVersion {
            found:     protocol_version,
            supported: PROTOCOL_VERSION,
        });
    }
    let object_kind = ObjectKind::from_u8(bytes[6])?;
    if object_kind != expected_kind {
        return Err(SerializationError::WrongObjectKind {
            expected: expected_kind,
            found:    object_kind,
        });
    }
    let schema_version = u16::from_le_bytes([bytes[7], bytes[8]]);
    let flags = u16::from_le_bytes([bytes[9], bytes[10]]);
    let payload_len = u32::from_le_bytes([bytes[11], bytes[12], bytes[13], bytes[14]]) as usize;
    let payload_start = HEADER_LEN;
    let payload_end =
        payload_start.checked_add(payload_len).ok_or(SerializationError::Truncated)?;
    if bytes.len() < payload_end {
        return Err(SerializationError::Truncated);
    }
    if bytes.len() != payload_end {
        return Err(SerializationError::Decode(
            "trailing bytes after envelope payload".to_string(),
        ));
    }
    Ok((
        EnvelopeHeader {
            protocol_version,
            object_kind,
            schema_version,
            flags,
        },
        &bytes[payload_start..payload_end],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::PROTOCOL_VERSION;

    #[test]
    fn envelope_roundtrip() {
        let encoded = encode_envelope(ObjectKind::Generic, 3, &[1, 2, 3]).unwrap();
        let (header, payload) = decode_envelope(encoded.as_slice(), ObjectKind::Generic).unwrap();
        assert_eq!(header.protocol_version, PROTOCOL_VERSION);
        assert_eq!(header.object_kind, ObjectKind::Generic);
        assert_eq!(header.schema_version, 3);
        assert_eq!(payload, &[1, 2, 3]);
    }

    #[test]
    fn rejects_wrong_magic() {
        let err = decode_envelope(b"NOPE", ObjectKind::Generic).unwrap_err();
        assert!(matches!(err, SerializationError::InvalidMagic | SerializationError::Truncated));
    }

    #[test]
    fn rejects_wrong_object_kind() {
        let encoded = encode_envelope(ObjectKind::Row, 1, &[]).unwrap();
        let err = decode_envelope(encoded.as_slice(), ObjectKind::Generic).unwrap_err();
        assert!(matches!(
            err,
            SerializationError::WrongObjectKind {
                expected: ObjectKind::Generic,
                found:    ObjectKind::Row,
            }
        ));
    }

    #[test]
    fn rejects_future_protocol_version() {
        let mut bytes = encode_envelope(ObjectKind::Generic, 1, &[]).unwrap().into_bytes();
        bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
        let err = decode_envelope(&bytes, ObjectKind::Generic).unwrap_err();
        assert!(matches!(
            err,
            SerializationError::UnsupportedProtocolVersion {
                found:     99,
                supported: PROTOCOL_VERSION,
            }
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let mut bytes = encode_envelope(ObjectKind::Generic, 1, &[9, 9]).unwrap().into_bytes();
        bytes.pop();
        let err = decode_envelope(&bytes, ObjectKind::Generic).unwrap_err();
        assert_eq!(err, SerializationError::Truncated);
    }
}
