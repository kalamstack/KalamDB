//! Serialization helpers for Raft state machine payloads.
//!
//! Encoding goes through `kalamdb-serialization`. Legacy unenveloped MessagePack
//! snapshots are rejected; wipe the data directory before upgrading to 0.7.

use kalamdb_serialization::{decode_protocol, encode_protocol, ProtocolKind};
use serde::{de::DeserializeOwned, Serialize};

use crate::error::RaftError;

/// Encode a value for durable state-machine storage.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RaftError> {
    encode_protocol(ProtocolKind::StateMachine, value)
        .map(|encoded| encoded.into_bytes())
        .map_err(|e| RaftError::Serialization(e.to_string()))
}

/// Decode a value from durable state-machine storage.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RaftError> {
    decode_protocol(bytes, ProtocolKind::StateMachine)
        .map_err(|e| RaftError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        id:   u64,
        name: String,
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data = TestData {
            id:   42,
            name: "test".to_string(),
        };
        let bytes = encode(&data).unwrap();
        assert_eq!(&bytes[..4], b"KOBJ");
        let decoded: TestData = decode(&bytes).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_unenveloped_bytes_are_rejected() {
        assert!(decode::<TestData>(b"not-a-kobj-envelope").is_err());
    }

    #[test]
    fn test_entry_payload_membership_roundtrip() {
        use std::collections::BTreeMap;

        use openraft::{EntryPayload, Membership};

        use crate::storage::{KalamNode, KalamTypeConfig};

        let node = KalamNode::new("127.0.0.1:2911", "http://127.0.0.1:2901");
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, node);
        let membership: Membership<u64, KalamNode> = nodes.into();
        let payload: EntryPayload<KalamTypeConfig> = EntryPayload::Membership(membership);

        let bytes = encode(&payload).expect("Membership should encode");
        let decoded: EntryPayload<KalamTypeConfig> =
            decode(&bytes).expect("Membership should decode");

        match (&payload, &decoded) {
            (EntryPayload::Membership(m1), EntryPayload::Membership(m2)) => {
                assert_eq!(m1.nodes().count(), m2.nodes().count(), "Node count should match");
            },
            _ => panic!("Decoded payload type mismatch"),
        }

        let blank: EntryPayload<KalamTypeConfig> = EntryPayload::Blank;
        let blank_bytes = encode(&blank).expect("Blank should encode");
        let _: EntryPayload<KalamTypeConfig> = decode(&blank_bytes).expect("Blank should decode");
    }

    #[test]
    fn test_entry_payload_membership_with_two_nodes() {
        use std::collections::BTreeMap;

        use openraft::{EntryPayload, Membership};

        use crate::storage::{KalamNode, KalamTypeConfig};

        let node1 = KalamNode::new("127.0.0.1:2911", "http://127.0.0.1:2901");
        let node2 = KalamNode::new("127.0.0.1:2912", "http://127.0.0.1:2902");
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, node1);
        nodes.insert(2u64, node2);

        let membership: Membership<u64, KalamNode> = nodes.into();
        let payload: EntryPayload<KalamTypeConfig> = EntryPayload::Membership(membership);

        let bytes = encode(&payload).expect("2-node Membership should encode");
        let decoded: EntryPayload<KalamTypeConfig> =
            decode(&bytes).expect("2-node Membership should decode");

        match (&payload, &decoded) {
            (EntryPayload::Membership(m1), EntryPayload::Membership(m2)) => {
                assert_eq!(m1.nodes().count(), m2.nodes().count(), "Node count should match");
                assert_eq!(m1.nodes().count(), 2, "Should have 2 nodes");
            },
            _ => panic!("Decoded payload type mismatch"),
        }
    }
}
