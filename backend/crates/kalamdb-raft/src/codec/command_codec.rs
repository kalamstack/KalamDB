use kalamdb_serialization::{decode_protocol, encode_protocol, ProtocolKind};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::RaftError, DataResponse, MetaCommand, MetaResponse, RaftCommand, RaftResponse,
    SharedDataCommand, UserDataCommand,
};

fn map_ser(err: kalamdb_serialization::SerializationError) -> RaftError {
    RaftError::Serialization(err.to_string())
}

fn encode_typed<T: Serialize>(kind: ProtocolKind, payload: &T) -> Result<Vec<u8>, RaftError> {
    encode_protocol(kind, payload)
        .map(|encoded| encoded.into_bytes())
        .map_err(map_ser)
}

fn decode_typed<T: DeserializeOwned>(
    bytes: &[u8],
    expected_kind: ProtocolKind,
) -> Result<T, RaftError> {
    decode_protocol(bytes, expected_kind).map_err(map_ser)
}

pub fn encode_meta_command(command: &MetaCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::MetaCommand, command)
}

pub fn decode_meta_command(bytes: &[u8]) -> Result<MetaCommand, RaftError> {
    decode_typed(bytes, ProtocolKind::MetaCommand)
}

pub fn encode_user_data_command(command: &UserDataCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::UserDataCommand, command)
}

pub fn decode_user_data_command(bytes: &[u8]) -> Result<UserDataCommand, RaftError> {
    decode_typed(bytes, ProtocolKind::UserDataCommand)
}

pub fn encode_shared_data_command(command: &SharedDataCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::SharedDataCommand, command)
}

pub fn decode_shared_data_command(bytes: &[u8]) -> Result<SharedDataCommand, RaftError> {
    decode_typed(bytes, ProtocolKind::SharedDataCommand)
}

pub fn encode_raft_command(command: &RaftCommand) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::RaftCommand, command)
}

pub fn decode_raft_command(bytes: &[u8]) -> Result<RaftCommand, RaftError> {
    decode_typed(bytes, ProtocolKind::RaftCommand)
}

pub fn encode_meta_response(response: &MetaResponse) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::MetaResponse, response)
}

pub fn decode_meta_response(bytes: &[u8]) -> Result<MetaResponse, RaftError> {
    decode_typed(bytes, ProtocolKind::MetaResponse)
}

pub fn encode_data_response(response: &DataResponse) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::DataResponse, response)
}

pub fn decode_data_response(bytes: &[u8]) -> Result<DataResponse, RaftError> {
    decode_typed(bytes, ProtocolKind::DataResponse)
}

pub fn encode_raft_response(response: &RaftResponse) -> Result<Vec<u8>, RaftError> {
    encode_typed(ProtocolKind::RaftResponse, response)
}

pub fn decode_raft_response(bytes: &[u8]) -> Result<RaftResponse, RaftError> {
    decode_typed(bytes, ProtocolKind::RaftResponse)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kalamdb_commons::{
        models::{rows::Row, NamespaceId, TableName},
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
        assert_eq!(&bytes[..4], b"KOBJ");
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
        assert!(err.to_string().contains("kind"));
    }

    #[test]
    fn decode_rejects_unenveloped_bytes() {
        let err = decode_meta_response(b"not-a-kobj-envelope").expect_err("legacy bytes rejected");
        assert!(
            err.to_string().contains("magic")
                || err.to_string().contains("envelope")
                || err.to_string().contains("decode")
        );
    }

    #[test]
    fn raft_transaction_commit_roundtrip() {
        let command = RaftCommand::TransactionCommit {
            transaction_id: kalamdb_commons::models::TransactionId::new(
                "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d",
            ),
            mutations:      vec![StagedMutation::new(
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

    #[test]
    fn shared_insert_encoded_fields_roundtrip() {
        let cmd = SharedDataCommand::Insert {
            required_meta_index: 1,
            transaction_id:      None,
            actor_user_id:       None,
            table_id:            TableId::new(NamespaceId::from("ns"), TableName::from("t")),
            rows:                vec![],
            encoded_fields:      vec![vec![1, 2, 3, 4]],
        };
        let bytes = encode_shared_data_command(&cmd).expect("encode");
        match decode_shared_data_command(&bytes).expect("decode") {
            SharedDataCommand::Insert {
                rows,
                encoded_fields,
                ..
            } => {
                assert!(rows.is_empty());
                assert_eq!(encoded_fields, vec![vec![1, 2, 3, 4]]);
            },
            _ => panic!("expected insert"),
        }
    }
}
