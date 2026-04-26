//! Node identifier type for cluster deployments
//!
//! Each KalamDB server instance has a unique node ID used for:
//! - Raft consensus (matches OpenRaft's u64 NodeId)
//! - Job assignment and tracking
//! - Live query routing
//! - Distributed coordination

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Node identifier for cluster deployments
///
/// Uses u64 to match OpenRaft's NodeId type for seamless integration.
/// Configured via server.toml `[cluster] node_id = 1`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum NodeIdRepr {
            Number(u64),
            String(String),
        }

        match NodeIdRepr::deserialize(deserializer)? {
            NodeIdRepr::Number(value) => Ok(NodeId::new(value)),
            NodeIdRepr::String(value) => {
                value.parse::<u64>().map(NodeId::new).map_err(serde::de::Error::custom)
            },
        }
    }
}

impl NodeId {
    /// Create a new node ID
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the node ID as a u64
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    /// Create default node ID (1)
    #[inline]
    pub const fn default_node() -> Self {
        Self(1)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::default_node()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_creation() {
        let node_id = NodeId::new(42);
        assert_eq!(node_id.as_u64(), 42);
    }

    #[test]
    fn test_node_id_display() {
        let node_id = NodeId::new(123);
        assert_eq!(format!("{}", node_id), "123");
    }

    #[test]
    fn test_node_id_from_u64() {
        let node_id = NodeId::from(456u64);
        assert_eq!(node_id.as_u64(), 456);
    }

    #[test]
    fn test_node_id_default() {
        let node_id = NodeId::default();
        assert_eq!(node_id.as_u64(), 1);
    }

    #[test]
    fn test_node_id_equality() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(1);
        let node3 = NodeId::new(2);

        assert_eq!(node1, node2);
        assert_ne!(node1, node3);
    }

    #[test]
    fn test_node_id_to_u64() {
        let node_id = NodeId::new(789);
        let value: u64 = node_id.into();
        assert_eq!(value, 789);
    }

    #[test]
    fn test_node_id_ordering() {
        let a = NodeId::new(1);
        let b = NodeId::new(2);
        let c = NodeId::new(3);
        assert!(a < b);
        assert!(b < c);
        // Verify BTreeSet ordering
        let set: std::collections::BTreeSet<NodeId> = [c, a, b].into_iter().collect();
        let ordered: Vec<NodeId> = set.into_iter().collect();
        assert_eq!(ordered, vec![a, b, c]);
    }
}
