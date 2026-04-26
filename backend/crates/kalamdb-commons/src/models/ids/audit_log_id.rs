// File: backend/crates/kalamdb-commons/src/models/audit_log_id.rs
// Type-safe wrapper for audit log identifiers.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::StorageKey;

/// Type-safe wrapper for audit log identifiers stored in system.audit_log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuditLogId(String);

impl AuditLogId {
    /// Creates a new AuditLogId from any string-like input.
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the identifier as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the identifier and returns the owned String.
    #[inline]
    pub fn into_string(self) -> String {
        self.0
    }

    /// Returns the identifier as bytes for storage keys.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for AuditLogId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for AuditLogId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for AuditLogId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for AuditLogId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for AuditLogId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl StorageKey for AuditLogId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        String::from_utf8(bytes.to_vec()).map(AuditLogId).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_accessors() {
        let id = AuditLogId::new("audit-123");
        assert_eq!(id.as_str(), "audit-123");
        assert_eq!(id.as_bytes(), b"audit-123");
    }

    #[test]
    fn test_into_string() {
        let id = AuditLogId::new("audit-into");
        assert_eq!(id.into_string(), "audit-into");
    }

    #[test]
    fn test_from_str() {
        let id: AuditLogId = "audit-str".into();
        assert_eq!(id.as_str(), "audit-str");
    }

    #[test]
    fn test_display() {
        let id = AuditLogId::new("audit-display");
        assert_eq!(format!("{}", id), "audit-display");
    }
}
