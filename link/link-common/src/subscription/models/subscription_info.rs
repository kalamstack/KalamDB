//! Subscription metadata exposed to callers.
//!
//! [`SubscriptionInfo`] provides a read-only snapshot of an active
//! subscription's state — useful for debugging, tests, and UI dashboards.

use crate::seq_id::SeqId;
use serde::{Deserialize, Serialize};

/// Read-only snapshot of an active subscription's metadata.
///
/// Returned by [`KalamLinkClient::subscriptions()`] and the WASM
/// `getSubscriptions()` method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    /// Subscription ID assigned when subscribing.
    pub id: String,
    /// The SQL query this subscription is tracking.
    pub query: String,
    /// Last received sequence ID (used for resume-from on reconnect).
    pub last_seq_id: Option<SeqId>,
    /// Timestamp (millis since Unix epoch) of the last received event,
    /// or `None` if no events have been received yet.
    pub last_event_time_ms: Option<u64>,
    /// Timestamp (millis since Unix epoch) when the subscription was created.
    pub created_at_ms: u64,
    /// Whether the subscription has been closed.
    pub closed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_info_serialize_roundtrip() {
        let info = SubscriptionInfo {
            id: "sub-1".to_string(),
            query: "SELECT * FROM t".to_string(),
            last_seq_id: Some(SeqId::new(42)),
            last_event_time_ms: Some(1700000000000),
            created_at_ms: 1700000000000,
            closed: false,
        };
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: SubscriptionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "sub-1");
        assert_eq!(deserialized.query, "SELECT * FROM t");
        assert_eq!(deserialized.last_seq_id.unwrap().as_i64(), 42);
        assert_eq!(deserialized.last_event_time_ms, Some(1700000000000));
        assert!(!deserialized.closed);
    }

    #[test]
    fn test_subscription_info_none_fields() {
        let info = SubscriptionInfo {
            id: "sub-2".to_string(),
            query: "SELECT 1".to_string(),
            last_seq_id: None,
            last_event_time_ms: None,
            created_at_ms: 1700000000000,
            closed: true,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"closed\":true"));
        let deserialized: SubscriptionInfo = serde_json::from_str(&json).unwrap();
        assert!(deserialized.last_seq_id.is_none());
        assert!(deserialized.last_event_time_ms.is_none());
        assert!(deserialized.closed);
    }
}
