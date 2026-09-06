//! Typed protocol objects (Raft commands, state-machine snapshots, etc.).

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{Result, SerializationError},
    object::{
        decode_envelope, decode_flexbuffers, encode_envelope, encode_flexbuffers, EncodedObject,
        ObjectKind,
    },
};

/// Discriminant for centralized protocol objects. Stored as envelope `schema_version`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ProtocolKind {
    /// Metadata Raft command.
    MetaCommand = 1,
    /// Per-user data Raft command.
    UserDataCommand = 2,
    /// Shared-table Raft command.
    SharedDataCommand = 3,
    /// Combined Raft command envelope.
    RaftCommand = 4,
    /// Metadata Raft response.
    MetaResponse = 5,
    /// Data Raft response.
    DataResponse = 6,
    /// Combined Raft response.
    RaftResponse = 7,
    /// Durable Raft state-machine snapshot/payload.
    StateMachine = 8,
}

/// Encode a typed protocol object. The kind is stored as `schema_version`.
pub fn encode_protocol<T: Serialize>(kind: ProtocolKind, value: &T) -> Result<EncodedObject> {
    let payload = encode_protocol_payload(kind, value)?;
    encode_envelope(ObjectKind::Protocol, kind as u16, &payload)
}

/// Decode a typed protocol object, rejecting a mismatched kind.
pub fn decode_protocol<T: DeserializeOwned>(
    bytes: &[u8],
    expected_kind: ProtocolKind,
) -> Result<T> {
    let (header, payload) = decode_envelope(bytes, ObjectKind::Protocol)?;
    if header.schema_version != expected_kind as u16 {
        return Err(SerializationError::Decode(format!(
            "protocol kind mismatch: expected {}, found {}",
            expected_kind as u16, header.schema_version
        )));
    }
    decode_protocol_payload(expected_kind, payload)
}

fn encode_protocol_payload<T: Serialize>(kind: ProtocolKind, value: &T) -> Result<Vec<u8>> {
    if kind == ProtocolKind::StateMachine {
        encode_msgpack(value)
    } else {
        encode_flexbuffers(value)
    }
}

fn decode_protocol_payload<T: DeserializeOwned>(kind: ProtocolKind, payload: &[u8]) -> Result<T> {
    if kind == ProtocolKind::StateMachine {
        decode_msgpack(payload)
    } else {
        decode_flexbuffers(payload)
    }
}

fn encode_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value)
        .map_err(|err| SerializationError::Encode(format!("msgpack encode failed: {err}")))
}

fn decode_msgpack<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes)
        .map_err(|err| SerializationError::Decode(format!("msgpack decode failed: {err}")))
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        n: i64,
    }

    #[test]
    fn protocol_roundtrip() {
        let value = Sample { n: 7 };
        let encoded = encode_protocol(ProtocolKind::MetaCommand, &value).unwrap();
        let decoded: Sample =
            decode_protocol(encoded.as_slice(), ProtocolKind::MetaCommand).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn protocol_rejects_unenveloped_bytes() {
        let value = Sample { n: 9 };
        let legacy = encode_flexbuffers(&value).unwrap();
        assert!(decode_protocol::<Sample>(&legacy, ProtocolKind::MetaCommand).is_err());

        let msgpack = rmp_serde::to_vec_named(&value).unwrap();
        assert!(decode_protocol::<Sample>(&msgpack, ProtocolKind::StateMachine).is_err());
    }

    #[test]
    fn state_machine_roundtrip() {
        let value = Sample { n: 11 };
        let encoded = encode_protocol(ProtocolKind::StateMachine, &value).unwrap();
        let current: Sample =
            decode_protocol(encoded.as_slice(), ProtocolKind::StateMachine).unwrap();
        assert_eq!(current, value);
    }
}
