use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::RaftError;
use crate::{
    DataResponse, MetaCommand, MetaResponse, RaftCommand, RaftResponse, SharedDataCommand,
    UserDataCommand,
};

const COMMAND_WIRE_VERSION: u16 = 1;

const KIND_META_COMMAND: &str = "meta_command";
const KIND_USER_DATA_COMMAND: &str = "user_data_command";
const KIND_SHARED_DATA_COMMAND: &str = "shared_data_command";
const KIND_RAFT_COMMAND: &str = "raft_command";

const KIND_META_RESPONSE: &str = "meta_response";
const KIND_DATA_RESPONSE: &str = "data_response";
const KIND_RAFT_RESPONSE: &str = "raft_response";

#[derive(Debug, Serialize)]
struct TypedEnvelopeEnc<'a, T> {
    v: u16,
    kind: &'a str,
    payload: T,
}

#[derive(Debug, Deserialize)]
struct TypedEnvelopeDec<T> {
    v: u16,
    kind: String,
    payload: T,
}

fn encode_typed<T: Serialize>(kind: &str, payload: &T) -> Result<Vec<u8>, RaftError> {
    let envelope = TypedEnvelopeEnc {
        v: COMMAND_WIRE_VERSION,
        kind,
        payload,
    };
    flexbuffers::to_vec(&envelope).map_err(|e| RaftError::Serialization(e.to_string()))
}

fn decode_typed<T: DeserializeOwned>(bytes: &[u8], expected_kind: &str) -> Result<T, RaftError> {
    let envelope: TypedEnvelopeDec<T> =
        flexbuffers::from_slice(bytes).map_err(|e| RaftError::Serialization(e.to_string()))?;

    if envelope.v != COMMAND_WIRE_VERSION {
        return Err(RaftError::Serialization(format!(
            "Unsupported command codec version: {} (expected {})",
            envelope.v, COMMAND_WIRE_VERSION
        )));
    }

    if envelope.kind != expected_kind {
        return Err(RaftError::Serialization(format!(
            "Unexpected command kind: '{}' (expected '{}')",
            envelope.kind, expected_kind
        )));
    }

    Ok(envelope.payload)
}

pub fn encode_meta_command(command: &MetaCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_META_COMMAND, command)
}

pub fn decode_meta_command(bytes: &[u8]) -> Result<MetaCommand, RaftError> {
    decode_typed(bytes, KIND_META_COMMAND)
}

pub fn encode_user_data_command(command: &UserDataCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_USER_DATA_COMMAND, command)
}

pub fn decode_user_data_command(bytes: &[u8]) -> Result<UserDataCommand, RaftError> {
    decode_typed(bytes, KIND_USER_DATA_COMMAND)
}

pub fn encode_shared_data_command(command: &SharedDataCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_SHARED_DATA_COMMAND, command)
}

pub fn decode_shared_data_command(bytes: &[u8]) -> Result<SharedDataCommand, RaftError> {
    decode_typed(bytes, KIND_SHARED_DATA_COMMAND)
}

pub fn encode_raft_command(command: &RaftCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_RAFT_COMMAND, command)
}

pub fn decode_raft_command(bytes: &[u8]) -> Result<RaftCommand, RaftError> {
    decode_typed(bytes, KIND_RAFT_COMMAND)
}

pub fn encode_meta_response(response: &MetaResponse) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_META_RESPONSE, response)
}

pub fn decode_meta_response(bytes: &[u8]) -> Result<MetaResponse, RaftError> {
    decode_typed(bytes, KIND_META_RESPONSE)
}

pub fn encode_data_response(response: &DataResponse) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_DATA_RESPONSE, response)
}

pub fn decode_data_response(bytes: &[u8]) -> Result<DataResponse, RaftError> {
    decode_typed(bytes, KIND_DATA_RESPONSE)
}

pub fn encode_raft_response(response: &RaftResponse) -> Result<Vec<u8>, RaftError> {
    encode_typed(KIND_RAFT_RESPONSE, response)
}

pub fn decode_raft_response(bytes: &[u8]) -> Result<RaftResponse, RaftError> {
    decode_typed(bytes, KIND_RAFT_RESPONSE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataResponse, MetaResponse};

    #[test]
    fn meta_response_roundtrip() {
        let value = MetaResponse::Message {
            message: "ok".to_string(),
        };
        let bytes = encode_meta_response(&value).expect("encode");
        let decoded = decode_meta_response(&bytes).expect("decode");
        match decoded {
            MetaResponse::Message { message } => assert_eq!(message, "ok"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn data_response_roundtrip() {
        let value = DataResponse::RowsAffected(3);
        let bytes = encode_data_response(&value).expect("encode");
        let decoded = decode_data_response(&bytes).expect("decode");
        assert_eq!(decoded.rows_affected(), 3);
    }

    #[test]
    fn decode_rejects_wrong_kind() {
        let value = MetaResponse::Ok;
        let bytes = encode_meta_response(&value).expect("encode");
        let err = decode_data_response(&bytes).expect_err("should reject kind mismatch");
        assert!(err.to_string().contains("Unexpected command kind"));
    }

    #[test]
    fn decode_rejects_unsupported_version() {
        #[derive(Serialize)]
        struct LegacyEnvelope {
            v: u16,
            kind: String,
            payload: MetaResponse,
        }

        let bytes = flexbuffers::to_vec(&LegacyEnvelope {
            v: COMMAND_WIRE_VERSION + 1,
            kind: KIND_META_RESPONSE.to_string(),
            payload: MetaResponse::Ok,
        })
        .expect("encode legacy envelope");

        let err = decode_meta_response(&bytes).expect_err("should reject version mismatch");
        assert!(err.to_string().contains("Unsupported command codec version"));
    }
}
