//! Composite identity for one trigger delivery attempt.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::TriggerId;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// `{trigger_id}:{partition}:{offset}:{attempt}` identity for `system.trigger_attempts`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TriggerAttemptId(String);

impl TriggerAttemptId {
    pub fn new(
        trigger_id: &TriggerId,
        partition: u32,
        offset: u64,
        attempt: u32,
    ) -> Result<Self, String> {
        if attempt == 0 {
            return Err("trigger attempt number must be greater than zero".to_string());
        }
        Ok(Self(format!("{}:{partition}:{offset}:{attempt}", trigger_id.as_str())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn trigger_id(&self) -> TriggerId {
        TriggerId::new(self.split().0)
    }

    pub fn partition(&self) -> u32 {
        self.split().1.parse().expect("TriggerAttemptId partition is numeric")
    }

    pub fn offset(&self) -> u64 {
        self.split().2.parse().expect("TriggerAttemptId offset is numeric")
    }

    pub fn attempt(&self) -> u32 {
        self.split().3.parse().expect("TriggerAttemptId attempt is numeric")
    }

    fn split(&self) -> (&str, &str, &str, &str) {
        let (rest, attempt) =
            self.0.rsplit_once(':').expect("TriggerAttemptId always contains ':'");
        let (rest, offset) = rest.rsplit_once(':').expect("TriggerAttemptId offset is present");
        let (trigger_id, partition) =
            rest.rsplit_once(':').expect("TriggerAttemptId partition is present");
        (trigger_id, partition, offset, attempt)
    }
}

impl From<String> for TriggerAttemptId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TriggerAttemptId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for TriggerAttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TriggerAttemptId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(feature = "storage")]
impl StorageKey for TriggerAttemptId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let value = String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())?;
        Ok(Self(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_attempt_id_splits_schema_qualified_trigger() {
        let id = TriggerAttemptId::new(
            &TriggerId::from_parts(
                Some(&crate::models::NamespaceId::new("chat")),
                "process_message",
            ),
            2,
            41,
            3,
        )
        .unwrap();
        assert_eq!(id.as_str(), "chat.process_message:2:41:3");
        assert_eq!(id.trigger_id().as_str(), "chat.process_message");
        assert_eq!(id.partition(), 2);
        assert_eq!(id.offset(), 41);
        assert_eq!(id.attempt(), 3);
    }
}
