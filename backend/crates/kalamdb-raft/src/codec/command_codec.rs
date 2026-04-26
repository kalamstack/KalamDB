use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    error::RaftError, DataResponse, MetaCommand, MetaResponse, RaftCommand, RaftResponse,
    SharedDataCommand, UserDataCommand,
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
    use std::collections::BTreeMap;

    use kalamdb_commons::{
        models::{rows::Row, NamespaceId, NodeId, TableName, UserId},
        TableId, TableType,
    };
    use kalamdb_transactions::StagedMutation;

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

    #[test]
    fn decode_rejects_retired_live_query_command() {
        #[derive(Serialize)]
        enum RetiredUserDataCommand {
            CleanupNodeSubscriptions {
                required_meta_index: u64,
                user_id: UserId,
                failed_node_id: NodeId,
            },
        }

        let bytes = encode_typed(
            KIND_USER_DATA_COMMAND,
            &RetiredUserDataCommand::CleanupNodeSubscriptions {
                required_meta_index: 17,
                user_id: UserId::from("user_1"),
                failed_node_id: NodeId::from(9),
            },
        )
        .expect("encode retired user data command");

        let err = decode_user_data_command(&bytes).expect_err("retired command must be rejected");
        assert!(
            err.to_string().contains("Unknown variant index")
                || err.to_string().contains("unknown variant")
        );
    }

    #[test]
    fn option_none_serializes_compactly() {
        let none_bytes =
            flexbuffers::to_vec(&Option::<kalamdb_commons::models::TransactionId>::None)
                .expect("encode none option");
        let some_bytes = flexbuffers::to_vec(&Some(kalamdb_commons::models::TransactionId::new(
            "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d",
        )))
        .expect("encode some option");

        assert!(none_bytes.len() <= 3);
        assert!(none_bytes.len() < some_bytes.len());
    }

    #[test]
    fn decode_user_data_command_defaults_transaction_id_to_none() {
        #[derive(Serialize)]
        enum LegacyUserDataCommand {
            Insert {
                required_meta_index: u64,
                table_id: TableId,
                user_id: UserId,
                rows: Vec<kalamdb_commons::models::rows::Row>,
            },
        }

        let bytes = encode_typed(
            KIND_USER_DATA_COMMAND,
            &LegacyUserDataCommand::Insert {
                required_meta_index: 7,
                table_id: TableId::new(NamespaceId::from("ns"), TableName::from("items")),
                user_id: UserId::from("user_1"),
                rows: vec![],
            },
        )
        .expect("encode legacy user command");

        let decoded = decode_user_data_command(&bytes).expect("decode user command with default");
        assert!(decoded.transaction_id().is_none());
    }

    #[test]
    fn decode_shared_data_command_defaults_transaction_id_to_none() {
        #[derive(Serialize)]
        enum LegacySharedDataCommand {
            Insert {
                required_meta_index: u64,
                table_id: TableId,
                rows: Vec<kalamdb_commons::models::rows::Row>,
            },
        }

        let bytes = encode_typed(
            KIND_SHARED_DATA_COMMAND,
            &LegacySharedDataCommand::Insert {
                required_meta_index: 9,
                table_id: TableId::new(NamespaceId::from("ns"), TableName::from("shared_items")),
                rows: vec![],
            },
        )
        .expect("encode legacy shared command");

        let decoded =
            decode_shared_data_command(&bytes).expect("decode shared command with default");
        assert!(decoded.transaction_id().is_none());
    }

    #[test]
    fn raft_transaction_commit_roundtrip() {
        let command = RaftCommand::TransactionCommit {
            transaction_id: kalamdb_commons::models::TransactionId::new(
                "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d",
            ),
            mutations: vec![StagedMutation::new(
                kalamdb_commons::models::TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d"),
                TableId::new(NamespaceId::from("ns"), TableName::from("items")),
                TableType::Shared,
                None,
                kalamdb_commons::models::OperationKind::Insert,
                "1",
                Row::new(BTreeMap::new()),
                false,
            )],
        };

        let bytes = encode_raft_command(&command).expect("encode raft command");
        let decoded = decode_raft_command(&bytes).expect("decode raft command");
        match decoded {
            RaftCommand::TransactionCommit {
                transaction_id,
                mutations,
            } => {
                assert_eq!(transaction_id.as_str(), "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
                assert_eq!(mutations.len(), 1);
            },
            _ => panic!("expected transaction commit variant"),
        }
    }
}
