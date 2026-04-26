// File: backend/crates/kalamdb-commons/src/models/job_id.rs
// Type-safe wrapper for job identifiers

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::StorageKey;

/// Type-safe wrapper for job identifiers in system.jobs table.
///
/// Ensures job IDs cannot be accidentally used where other identifier types
/// are expected.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    /// Creates a new JobId from a string.
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the job ID as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner String.
    #[inline]
    pub fn into_string(self) -> String {
        self.0
    }

    /// Get the job ID as bytes for storage
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for JobId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for JobId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for JobId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for JobId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl StorageKey for JobId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        String::from_utf8(bytes.to_vec()).map(JobId).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_id_new() {
        let id = JobId::new("job-123");
        assert_eq!(id.as_str(), "job-123");
    }

    #[test]
    fn test_job_id_from_string() {
        let id = JobId::from("job-456".to_string());
        assert_eq!(id.as_str(), "job-456");
    }

    #[test]
    fn test_job_id_from_str() {
        let id = JobId::from("job-789");
        assert_eq!(id.as_str(), "job-789");
    }

    #[test]
    fn test_job_id_as_ref_str() {
        let id = JobId::new("job-abc");
        let s: &str = id.as_ref();
        assert_eq!(s, "job-abc");
    }

    #[test]
    fn test_job_id_as_ref_bytes() {
        let id = JobId::new("job-def");
        let bytes: &[u8] = id.as_ref();
        assert_eq!(bytes, b"job-def");
    }

    #[test]
    fn test_job_id_as_bytes() {
        let id = JobId::new("job-ghi");
        assert_eq!(id.as_bytes(), b"job-ghi");
    }

    #[test]
    fn test_job_id_display() {
        let id = JobId::new("job-xyz");
        assert_eq!(format!("{}", id), "job-xyz");
    }

    #[test]
    fn test_job_id_into_string() {
        let id = JobId::new("job-123");
        let s = id.into_string();
        assert_eq!(s, "job-123");
    }

    #[test]
    fn test_job_id_clone() {
        let id1 = JobId::new("job-clone");
        let id2 = id1.clone();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_job_id_serialization() {
        let id = JobId::new("job-serialize");
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: JobId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }
}
