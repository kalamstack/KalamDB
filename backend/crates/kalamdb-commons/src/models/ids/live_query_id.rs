// File: backend/crates/kalamdb-commons/src/models/live_query_id.rs
// Type-safe composite identifier for live query subscriptions

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    encode_prefix,
    models::{ConnectionId, UserId},
    StorageKey,
};

/// Unique identifier for live query subscriptions.
///
/// Composite key containing user, connection, and subscription information.
/// Format when serialized: {user_id}-{connection_id}-{subscription_id}
///
/// ## Memory Safety
/// Uses a pre-computed `cached_string` to provide zero-allocation `AsRef<str>` access.
/// This avoids the previous `Box::leak` pattern which caused memory leaks (~48MB/24h).
#[derive(Debug, Clone)]
pub struct LiveQueryId {
    pub user_id: UserId,
    pub connection_id: ConnectionId,
    pub subscription_id: String,
    /// Pre-computed string representation for zero-allocation AsRef<str>
    /// Computed once at construction time or after deserialization.
    cached_string: String,
}

impl Serialize for LiveQueryId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

// Custom serde Deserialize implementation that populates cached_string after deserialization
impl<'de> Deserialize<'de> for LiveQueryId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum LiveQueryIdRepr {
            String(String),
            Struct {
                user_id: UserId,
                connection_id: ConnectionId,
                subscription_id: String,
            },
        }

        match LiveQueryIdRepr::deserialize(deserializer)? {
            LiveQueryIdRepr::String(value) => {
                LiveQueryId::from_string(&value).map_err(serde::de::Error::custom)
            },
            LiveQueryIdRepr::Struct {
                user_id,
                connection_id,
                subscription_id,
            } => Ok(LiveQueryId::new(user_id, connection_id, subscription_id)),
        }
    }
}

// Manual PartialEq implementation that ignores cached_string
impl PartialEq for LiveQueryId {
    fn eq(&self, other: &Self) -> bool {
        self.user_id == other.user_id
            && self.connection_id == other.connection_id
            && self.subscription_id == other.subscription_id
    }
}

impl Eq for LiveQueryId {}

// Manual Hash implementation that ignores cached_string
impl std::hash::Hash for LiveQueryId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.user_id.hash(state);
        self.connection_id.hash(state);
        self.subscription_id.hash(state);
    }
}

impl LiveQueryId {
    /// Creates a new LiveQueryId from composite components
    pub fn new(
        user_id: UserId,
        connection_id: ConnectionId,
        subscription_id: impl Into<String>,
    ) -> Self {
        let subscription_id = subscription_id.into();
        // Pre-compute the string representation once
        let cached_string =
            format!("{}-{}-{}", user_id.as_str(), connection_id.as_str(), subscription_id);
        Self {
            user_id,
            connection_id,
            subscription_id,
            cached_string,
        }
    }

    /// Parse LiveQueryId from string format
    pub fn from_string(s: &str) -> Result<Self, String> {
        let mut parts = s.splitn(3, '-');
        let user = parts.next();
        let connection = parts.next();
        let subscription = parts.next();
        if user.is_none() || connection.is_none() || subscription.is_none() {
            return Err(format!(
                "Invalid live_query_id format: {}. Expected: \
                 {{user_id}}-{{connection_id}}-{{subscription_id}}",
                s
            ));
        }

        let user_id = UserId::new(user.unwrap().to_string());
        let connection_id = ConnectionId::new(connection.unwrap().to_string());
        let subscription_id = subscription.unwrap().to_string();

        Ok(Self::new(user_id, connection_id, subscription_id))
    }

    /// Returns the live query ID as a string reference (zero-allocation)
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.cached_string
    }

    /// Get the live query ID as bytes (zero-allocation reference)
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.cached_string.as_bytes()
    }

    /// Consumes the wrapper and returns the cached String
    pub fn into_string(self) -> String {
        self.cached_string
    }

    // Accessor methods
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    pub fn connection_id(&self) -> &ConnectionId {
        &self.connection_id
    }

    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    /// Creates a prefix key for scanning all live queries for a user+connection.
    ///
    /// Used by `delete_by_connection_id` for efficient range deletion.
    pub fn user_connection_prefix(user_id: &UserId, connection_id: &ConnectionId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(), connection_id.as_str()))
    }

    /// Creates a prefix key for scanning all live queries for a user.
    ///
    /// Used for getting all live queries belonging to a user.
    pub fn user_prefix(user_id: &UserId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(),))
    }
}

impl fmt::Display for LiveQueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use the pre-computed cached string (zero allocation)
        f.write_str(&self.cached_string)
    }
}

impl From<String> for LiveQueryId {
    fn from(s: String) -> Self {
        Self::from_string(&s).expect("Invalid LiveQueryId string format")
    }
}

impl From<&str> for LiveQueryId {
    fn from(s: &str) -> Self {
        Self::from_string(s).expect("Invalid LiveQueryId string format")
    }
}

impl AsRef<str> for LiveQueryId {
    #[inline]
    fn as_ref(&self) -> &str {
        // Use the pre-computed cached string (zero allocation, no memory leak)
        &self.cached_string
    }
}

impl AsRef<[u8]> for LiveQueryId {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        // Use the pre-computed cached string (zero allocation, no memory leak)
        self.cached_string.as_bytes()
    }
}

impl StorageKey for LiveQueryId {
    fn storage_key(&self) -> Vec<u8> {
        self.cached_string.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let s = String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())?;
        Self::from_string(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_live_query_id() -> LiveQueryId {
        let user_id = UserId::new("user1");
        let connection_id = ConnectionId::new("conn123");
        LiveQueryId::new(user_id, connection_id, "sub456")
    }

    #[test]
    fn test_live_query_id_new() {
        let id = create_test_live_query_id();
        assert_eq!(id.subscription_id(), "sub456");
        assert_eq!(id.user_id().as_str(), "user1");
    }

    #[test]
    fn test_live_query_id_from_string() {
        let id_str = "user1-conn123-sub456";
        let id = LiveQueryId::from_string(id_str).unwrap();
        assert_eq!(id.user_id().as_str(), "user1");
        assert_eq!(id.connection_id().as_str(), "conn123");
        assert_eq!(id.subscription_id(), "sub456");
    }

    #[test]
    fn test_live_query_id_to_string() {
        let id = create_test_live_query_id();
        let s = id.to_string();
        assert_eq!(s, "user1-conn123-sub456");
    }

    #[test]
    fn test_live_query_id_as_bytes() {
        let id = create_test_live_query_id();
        let bytes = id.as_bytes();
        assert_eq!(bytes, b"user1-conn123-sub456");
    }

    #[test]
    fn test_live_query_id_display() {
        let id = create_test_live_query_id();
        assert_eq!(format!("{}", id), "user1-conn123-sub456");
    }

    #[test]
    fn test_live_query_id_into_string() {
        let id = create_test_live_query_id();
        let s = id.into_string();
        assert_eq!(s, "user1-conn123-sub456");
    }

    #[test]
    fn test_live_query_id_clone() {
        let id1 = create_test_live_query_id();
        let id2 = id1.clone();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_live_query_id_serialization() {
        let id = create_test_live_query_id();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: LiveQueryId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_storage_key_roundtrip() {
        let id = create_test_live_query_id();
        let key = id.storage_key();
        let restored = LiveQueryId::from_storage_key(&key).unwrap();
        assert_eq!(id, restored);
    }
}
