//! Serialization helpers for Raft state machine payloads.
//!
//! Uses MessagePack (rmp-serde) with named fields for encoding: compact binary,
//! self-describing, schema-evolution-friendly (add/remove/reorder fields),
//! and supports any serde key type including integer-keyed maps.

use crate::error::RaftError;
use serde::{de::DeserializeOwned, Serialize};

/// Encode a value to bytes using MessagePack with named fields.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RaftError> {
    rmp_serde::to_vec_named(value).map_err(|e| RaftError::Serialization(e.to_string()))
}

/// Decode a value from MessagePack bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RaftError> {
    rmp_serde::from_slice(bytes).map_err(|e| RaftError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        id: u64,
        name: String,
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data = TestData {
            id: 42,
            name: "test".to_string(),
        };
        let bytes = encode(&data).unwrap();
        let decoded: TestData = decode(&bytes).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_msgpack_compact() {
        let data = TestData {
            id: 123456789,
            name: "a_reasonably_long_name_for_testing".to_string(),
        };
        let mp_bytes = encode(&data).unwrap();
        // MessagePack with named fields should be compact
        assert!(mp_bytes.len() < 60, "msgpack should be compact, got {} bytes", mp_bytes.len());
    }

    #[test]
    fn test_entry_payload_membership_roundtrip() {
        use crate::storage::{KalamNode, KalamTypeConfig};
        use openraft::{EntryPayload, Membership};
        use std::collections::BTreeMap;

        let node = KalamNode::new("127.0.0.1:9081", "http://127.0.0.1:8081");
        let mut nodes = BTreeMap::new();
        nodes.insert(1u64, node);
        let membership: Membership<u64, KalamNode> = nodes.into();
        let payload: EntryPayload<KalamTypeConfig> = EntryPayload::Membership(membership);

        let bytes = encode(&payload).expect("Membership should encode");
        let decoded: EntryPayload<KalamTypeConfig> = decode(&bytes)
            .expect("Membership should decode");

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
        use crate::storage::{KalamNode, KalamTypeConfig};
        use openraft::{EntryPayload, Membership};
        use std::collections::BTreeMap;

        let node1 = KalamNode::new("127.0.0.1:9081", "http://127.0.0.1:8081");
        let node2 = KalamNode::new("127.0.0.1:9082", "http://127.0.0.1:8082");
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
