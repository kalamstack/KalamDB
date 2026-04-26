//! Schema version history tracking

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents a single schema version in the history
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number (1-indexed, monotonically increasing)
    pub version: u32,

    /// Timestamp when this version was created
    pub created_at: DateTime<Utc>,

    /// Description of changes made in this version
    /// Examples: "Initial schema", "Added email column", "Dropped legacy_id"
    pub changes: String,

    /// Serialized Arrow schema as JSON for this version
    /// Allows reconstructing the exact Arrow schema at any point in history
    pub arrow_schema_json: String,
}

impl SchemaVersion {
    /// Create a new schema version
    pub fn new(
        version: u32,
        changes: impl Into<String>,
        arrow_schema_json: impl Into<String>,
    ) -> Self {
        Self {
            version,
            created_at: Utc::now(),
            changes: changes.into(),
            arrow_schema_json: arrow_schema_json.into(),
        }
    }

    /// Create the initial schema version (version 1)
    pub fn initial(arrow_schema_json: impl Into<String>) -> Self {
        Self::new(1, "Initial schema", arrow_schema_json)
    }

    /// Create a schema version with custom timestamp (for testing)
    pub fn with_timestamp(
        version: u32,
        created_at: DateTime<Utc>,
        changes: impl Into<String>,
        arrow_schema_json: impl Into<String>,
    ) -> Self {
        Self {
            version,
            created_at,
            changes: changes.into(),
            arrow_schema_json: arrow_schema_json.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_new_schema_version() {
        let version = SchemaVersion::new(1, "Initial schema", "{}");
        assert_eq!(version.version, 1);
        assert_eq!(version.changes, "Initial schema");
        assert_eq!(version.arrow_schema_json, "{}");
        // created_at should be recent
        assert!(version.created_at <= Utc::now());
    }

    #[test]
    fn test_initial_schema_version() {
        let version = SchemaVersion::initial("{}");
        assert_eq!(version.version, 1);
        assert_eq!(version.changes, "Initial schema");
    }

    #[test]
    fn test_with_timestamp() {
        let timestamp = Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap();
        let version = SchemaVersion::with_timestamp(2, timestamp, "Added column", "{}");
        assert_eq!(version.version, 2);
        assert_eq!(version.created_at, timestamp);
    }

    #[test]
    fn test_serialization() {
        let version = SchemaVersion::new(1, "Test", "{}");
        let json = serde_json::to_string(&version).unwrap();
        let decoded: SchemaVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(version.version, decoded.version);
        assert_eq!(version.changes, decoded.changes);
        assert_eq!(version.arrow_schema_json, decoded.arrow_schema_json);
    }

    #[test]
    fn test_version_history_order() {
        let v1 = SchemaVersion::initial("{}");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let v2 = SchemaVersion::new(2, "Updated", "{}");

        assert!(v1.created_at < v2.created_at);
        assert_eq!(v1.version + 1, v2.version);
    }
}
