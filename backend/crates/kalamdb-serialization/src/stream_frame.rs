//! Stream log record encoding and length-prefixed frames.

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{Result, SerializationError},
    object::{
        decode_envelope, decode_flexbuffers, encode_envelope, encode_flexbuffers, EncodedObject,
        ObjectKind,
    },
};

const STREAM_SCHEMA_VERSION: u16 = 1;

/// Encode a stream log record.
pub fn encode_stream<T: Serialize>(value: &T) -> Result<EncodedObject> {
    let payload = encode_flexbuffers(value)?;
    encode_envelope(ObjectKind::Stream, STREAM_SCHEMA_VERSION, &payload)
}

/// Decode a current-envelope stream record.
pub fn decode_stream<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (_header, payload) = decode_envelope(bytes, ObjectKind::Stream)?;
    decode_flexbuffers(payload)
}

/// Encode a length-prefixed stream frame (`u32` LE length + object bytes).
pub fn encode_stream_frame<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let encoded = encode_stream(value)?;
    let payload = encoded.as_slice();
    let len = u32::try_from(payload.len())
        .map_err(|_| SerializationError::Encode("stream frame exceeds u32 length".to_string()))?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Decode the inner payload of a length-prefixed stream frame (length already stripped).
pub fn decode_stream_frame_payload<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    decode_stream(payload)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::object::encode_flexbuffers;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Rec {
        id: u64,
    }

    #[test]
    fn stream_roundtrip() {
        let value = Rec { id: 3 };
        let encoded = encode_stream(&value).unwrap();
        let decoded: Rec = decode_stream(encoded.as_slice()).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn stream_rejects_unenveloped_flexbuffers() {
        let value = Rec { id: 4 };
        let legacy = encode_flexbuffers(&value).unwrap();
        assert!(decode_stream::<Rec>(&legacy).is_err());
    }

    #[test]
    fn stream_frame_roundtrip() {
        let value = Rec { id: 5 };
        let frame = encode_stream_frame(&value).unwrap();
        let len = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        let payload = &frame[4..4 + len];
        let decoded: Rec = decode_stream_frame_payload(payload).unwrap();
        assert_eq!(decoded, value);
    }
}
