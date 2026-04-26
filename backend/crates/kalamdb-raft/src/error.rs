//! Error types for the Raft layer

use thiserror::Error;

/// Result type for Raft operations
pub type Result<T> = std::result::Result<T, RaftError>;

/// Errors that can occur in the Raft layer
#[derive(Debug, Error)]
pub enum RaftError {
    /// The node is not the leader for this group
    #[error("Not leader for group {group}: leader is node {leader:?}")]
    NotLeader { group: String, leader: Option<u64> },

    /// Raft group not found
    #[error("Raft group not found: {0}")]
    GroupNotFound(String),

    /// Invalid Raft group (e.g., invalid shard number)
    #[error("Invalid Raft group: {0}")]
    InvalidGroup(String),

    /// Raft group not started
    #[error("Raft group not started: {0}")]
    NotStarted(String),

    /// Failed to apply command to state machine
    #[error("Failed to apply command: {0}")]
    ApplyFailed(String),

    /// Failed to serialize/deserialize
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Proposal rejected (e.g., not leader, timeout)
    #[error("Proposal rejected: {0}")]
    Proposal(String),

    /// Network error during Raft communication
    #[error("Network error: {0}")]
    Network(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Command timeout
    #[error("Command timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Replication timeout - command committed but not all nodes applied
    #[error(
        "Replication timeout for group {group}: committed at {committed_log_id} but not all nodes \
         applied within {timeout_ms}ms"
    )]
    ReplicationTimeout {
        group: String,
        committed_log_id: String,
        timeout_ms: u64,
    },

    /// Raft is shutting down
    #[error("Raft is shutting down")]
    Shutdown,

    /// Invalid state
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Provider error (from underlying data layer)
    #[error("Provider error: {0}")]
    Provider(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl RaftError {
    /// Create a NotLeader error
    pub fn not_leader(group: impl Into<String>, leader: Option<u64>) -> Self {
        RaftError::NotLeader {
            group: group.into(),
            leader,
        }
    }

    /// Create an ApplyFailed error
    pub fn apply_failed(msg: impl Into<String>) -> Self {
        RaftError::ApplyFailed(msg.into())
    }

    /// Create a Storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        RaftError::Storage(msg.into())
    }

    /// Create a Provider error
    pub fn provider(msg: impl Into<String>) -> Self {
        RaftError::Provider(msg.into())
    }

    /// Returns true if retrying might succeed
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RaftError::NotLeader { .. }
                | RaftError::Timeout(_)
                | RaftError::Network(_)
                | RaftError::ReplicationTimeout { .. }
        )
    }

    /// Returns the leader hint if this is a NotLeader error
    pub fn leader_hint(&self) -> Option<u64> {
        if let RaftError::NotLeader { leader, .. } = self {
            *leader
        } else {
            None
        }
    }
}

impl From<std::io::Error> for RaftError {
    fn from(err: std::io::Error) -> Self {
        RaftError::Storage(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_leader_constructor() {
        let err = RaftError::not_leader("group_1", Some(2));
        match err {
            RaftError::NotLeader { group, leader } => {
                assert_eq!(group, "group_1");
                assert_eq!(leader, Some(2));
            },
            _ => panic!("Expected NotLeader error"),
        }
    }

    #[test]
    fn test_apply_failed_constructor() {
        let err = RaftError::apply_failed("test error");
        match err {
            RaftError::ApplyFailed(msg) => assert_eq!(msg, "test error"),
            _ => panic!("Expected ApplyFailed error"),
        }
    }

    #[test]
    fn test_storage_constructor() {
        let err = RaftError::storage("disk full");
        match err {
            RaftError::Storage(msg) => assert_eq!(msg, "disk full"),
            _ => panic!("Expected Storage error"),
        }
    }

    #[test]
    fn test_provider_constructor() {
        let err = RaftError::provider("provider failure");
        match err {
            RaftError::Provider(msg) => assert_eq!(msg, "provider failure"),
            _ => panic!("Expected Provider error"),
        }
    }

    #[test]
    fn test_is_retryable() {
        assert!(RaftError::not_leader("g1", Some(2)).is_retryable());
        assert!(RaftError::Timeout(std::time::Duration::from_secs(1)).is_retryable());
        assert!(RaftError::Network("connection lost".to_string()).is_retryable());
        assert!(RaftError::ReplicationTimeout {
            group: "g1".to_string(),
            committed_log_id: "1-100".to_string(),
            timeout_ms: 5000,
        }
        .is_retryable());

        // Non-retryable errors
        assert!(!RaftError::ApplyFailed("failed".to_string()).is_retryable());
        assert!(!RaftError::InvalidState("bad state".to_string()).is_retryable());
        assert!(!RaftError::Shutdown.is_retryable());
    }

    #[test]
    fn test_leader_hint() {
        let err_with_leader = RaftError::not_leader("g1", Some(3));
        assert_eq!(err_with_leader.leader_hint(), Some(3));

        let err_no_leader = RaftError::not_leader("g1", None);
        assert_eq!(err_no_leader.leader_hint(), None);

        let err_other = RaftError::Shutdown;
        assert_eq!(err_other.leader_hint(), None);
    }

    #[test]
    fn test_error_display() {
        let err = RaftError::not_leader("user-shard-5", Some(2));
        let msg = format!("{}", err);
        assert!(msg.contains("Not leader"));
        assert!(msg.contains("user-shard-5"));
        assert!(msg.contains("2"));
    }

    #[test]
    fn test_timeout_error() {
        let duration = std::time::Duration::from_millis(5000);
        let err = RaftError::Timeout(duration);
        let msg = format!("{}", err);
        assert!(msg.contains("timed out"));
        assert!(msg.contains("5s"));
    }

    #[test]
    fn test_replication_timeout_error() {
        let err = RaftError::ReplicationTimeout {
            group: "meta".to_string(),
            committed_log_id: "1-250".to_string(),
            timeout_ms: 3000,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("meta"));
        assert!(msg.contains("1-250"));
        assert!(msg.contains("3000"));
    }

    #[test]
    fn test_all_error_variants_are_errors() {
        let errors: Vec<RaftError> = vec![
            RaftError::NotLeader {
                group: "g".to_string(),
                leader: None,
            },
            RaftError::GroupNotFound("g".to_string()),
            RaftError::InvalidGroup("g".to_string()),
            RaftError::NotStarted("g".to_string()),
            RaftError::ApplyFailed("e".to_string()),
            RaftError::Serialization("e".to_string()),
            RaftError::Proposal("e".to_string()),
            RaftError::Network("e".to_string()),
            RaftError::Storage("e".to_string()),
            RaftError::Config("e".to_string()),
            RaftError::Timeout(std::time::Duration::from_secs(1)),
            RaftError::ReplicationTimeout {
                group: "g".to_string(),
                committed_log_id: "1".to_string(),
                timeout_ms: 100,
            },
            RaftError::Shutdown,
            RaftError::InvalidState("s".to_string()),
            RaftError::Provider("p".to_string()),
            RaftError::Internal("i".to_string()),
        ];

        for err in errors {
            // Just ensure they all implement Error trait properly
            let _msg = format!("{}", err);
            let _debug = format!("{:?}", err);
        }
    }
}
