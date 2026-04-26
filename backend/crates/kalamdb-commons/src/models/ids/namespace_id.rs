//! Type-safe wrapper for namespace identifiers.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{
    constants::{RESERVED_NAMESPACE_NAMES, SYSTEM_NAMESPACE},
    StorageKey,
};

/// Error returned when a namespace ID fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceIdValidationError {
    pub name: String,
    pub reason: String,
}

impl fmt::Display for NamespaceIdValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid namespace ID '{}': {}", self.name, self.reason)
    }
}

impl std::error::Error for NamespaceIdValidationError {}

/// Type-safe wrapper for namespace identifiers.
///
/// Ensures namespace IDs cannot be accidentally used where user IDs or table names
/// are expected.
///
/// # Security
///
/// Namespace IDs are validated to prevent path traversal attacks. The following
/// are rejected:
/// - Empty strings
/// - Names containing `..` (parent directory traversal)
/// - Names containing `/` or `\` (path separators)
/// - Names containing null bytes
/// Statics for cheap singleton construction.
static SYSTEM_NS: OnceLock<Arc<str>> = OnceLock::new();
static DEFAULT_NS: OnceLock<Arc<str>> = OnceLock::new();

/// Type-safe wrapper for namespace identifiers.
///
/// Stored as `Arc<str>` so `clone()` is a cheap atomic refcount increment
/// rather than a heap allocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NamespaceId(Arc<str>);

impl NamespaceId {
    /// Validates a namespace ID for security issues.
    ///
    /// Returns an error if the ID:
    /// - Is empty
    /// - Contains path traversal sequences (`..`)
    /// - Contains path separators (`/` or `\`)
    /// - Contains null bytes
    fn validate(name: &str) -> Result<(), NamespaceIdValidationError> {
        if name.is_empty() {
            return Err(NamespaceIdValidationError {
                name: name.to_string(),
                reason: "Namespace ID cannot be empty".to_string(),
            });
        }

        if name.contains("..") {
            return Err(NamespaceIdValidationError {
                name: name.to_string(),
                reason: "Namespace ID cannot contain '..' (path traversal)".to_string(),
            });
        }

        if name.contains('/') || name.contains('\\') {
            return Err(NamespaceIdValidationError {
                name: name.to_string(),
                reason: "Namespace ID cannot contain path separators".to_string(),
            });
        }

        if name.contains('\0') {
            return Err(NamespaceIdValidationError {
                name: name.to_string(),
                reason: "Namespace ID cannot contain null bytes".to_string(),
            });
        }

        Ok(())
    }

    /// Creates a new NamespaceId from a string, with validation.
    ///
    /// Returns an error if the ID fails security validation.
    ///
    /// Namespace IDs are case-insensitive - they are normalized to lowercase internally.
    pub fn try_new(id: impl Into<String>) -> Result<Self, NamespaceIdValidationError> {
        let id = id.into();
        Self::validate(&id)?;
        Ok(Self(Arc::<str>::from(id.to_lowercase())))
    }

    /// Creates a new NamespaceId from a string.
    ///
    /// Namespace IDs are case-insensitive - they are normalized to lowercase internally.
    /// For example, `NamespaceId::new("MyApp")` and `NamespaceId::new("myapp")` are equal.
    ///
    /// # Panics
    ///
    /// Panics if the ID fails security validation. Use `try_new` for fallible creation.
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        Self::validate(&id).expect("Invalid namespace ID");
        Self(Arc::<str>::from(id.to_lowercase()))
    }

    /// Returns the namespace ID as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner String.
    #[inline]
    pub fn into_string(self) -> String {
        String::from(&*self.0)
    }

    /// Create system namespace ID — cached singleton.
    #[inline]
    pub fn system() -> Self {
        Self(SYSTEM_NS.get_or_init(|| Arc::from(SYSTEM_NAMESPACE)).clone())
    }

    /// Create default namespace ID — cached singleton.
    #[inline]
    pub fn default_ns() -> Self {
        Self(DEFAULT_NS.get_or_init(|| Arc::from("default")).clone())
    }

    /// Check if this is a system namespace (internal DataFusion/KalamDB namespaces).
    ///
    /// Returns true for namespaces used internally by the system:
    /// - `system`: KalamDB system tables
    /// - `information_schema`: SQL standard metadata
    /// - `pg_catalog`: PostgreSQL compatibility
    /// - `datafusion`: DataFusion internal catalog
    #[inline]
    pub fn is_system_namespace(&self) -> bool {
        matches!(
            self.as_str(),
            SYSTEM_NAMESPACE | "information_schema" | "pg_catalog" | "datafusion"
        )
    }

    /// If default namespace
    #[inline]
    pub fn is_default_namespace(&self) -> bool {
        self.as_str() == "default"
    }

    /// Check if this namespace name is reserved and cannot be created by users.
    ///
    /// This performs a case-insensitive check against all reserved namespace names
    /// defined in `constants::RESERVED_NAMESPACE_NAMES`.
    ///
    /// # Example
    /// ```
    /// use kalamdb_commons::NamespaceId;
    ///
    /// assert!(NamespaceId::system().is_reserved());
    /// assert!(NamespaceId::system().is_reserved());
    /// assert!(NamespaceId::new("kalamdb").is_reserved());
    /// assert!(!NamespaceId::new("my_app").is_reserved());
    /// ```
    /// Values are already stored lowercase, so no allocation needed.
    #[inline]
    pub fn is_reserved(&self) -> bool {
        RESERVED_NAMESPACE_NAMES.iter().any(|&reserved| reserved == self.as_str())
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for NamespaceId {
    fn default() -> Self {
        Self::default_ns()
    }
}

impl From<String> for NamespaceId {
    fn from(s: String) -> Self {
        Self::validate(&s).expect("Invalid namespace ID");
        Self(Arc::<str>::from(s.to_lowercase()))
    }
}

impl From<&str> for NamespaceId {
    fn from(s: &str) -> Self {
        Self::validate(s).expect("Invalid namespace ID");
        Self(Arc::<str>::from(s.to_lowercase()))
    }
}

impl AsRef<str> for NamespaceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for NamespaceId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl StorageKey for NamespaceId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        String::from_utf8(bytes.to_vec())
            .map(|s| NamespaceId(Arc::<str>::from(s)))
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_reserved_lowercase() {
        assert!(NamespaceId::system().is_reserved());
        assert!(NamespaceId::new("sys").is_reserved());
        assert!(NamespaceId::new("root").is_reserved());
        assert!(NamespaceId::new("kalamdb").is_reserved());
        assert!(NamespaceId::new("kalam").is_reserved());
        assert!(NamespaceId::new("main").is_reserved());
        assert!(NamespaceId::default().is_reserved());
        assert!(NamespaceId::new("sql").is_reserved());
        assert!(NamespaceId::new("admin").is_reserved());
        assert!(NamespaceId::new("internal").is_reserved());
        assert!(NamespaceId::new("information_schema").is_reserved());
        assert!(NamespaceId::new("pg_catalog").is_reserved());
        assert!(NamespaceId::new("datafusion").is_reserved());
    }

    #[test]
    fn test_is_reserved_case_insensitive() {
        assert!(NamespaceId::system().is_reserved());
        assert!(NamespaceId::system().is_reserved());
        assert!(NamespaceId::new("KalamDB").is_reserved());
        assert!(NamespaceId::new("INFORMATION_SCHEMA").is_reserved());
    }

    #[test]
    fn test_is_not_reserved() {
        assert!(!NamespaceId::new("my_app").is_reserved());
        assert!(!NamespaceId::new("production").is_reserved());
        assert!(!NamespaceId::new("user_data").is_reserved());
        assert!(!NamespaceId::new("analytics").is_reserved());
    }

    #[test]
    fn test_is_system_namespace() {
        assert!(NamespaceId::system().is_system_namespace());
        assert!(NamespaceId::new("information_schema").is_system_namespace());
        assert!(NamespaceId::new("pg_catalog").is_system_namespace());
        assert!(NamespaceId::new("datafusion").is_system_namespace());

        // These are reserved but not system namespaces
        assert!(!NamespaceId::new("kalamdb").is_system_namespace());
        assert!(!NamespaceId::default().is_system_namespace());
    }

    #[test]
    fn test_system_constructor() {
        let ns = NamespaceId::system();
        assert_eq!(ns.as_str(), "system");
        assert!(ns.is_system_namespace());
        assert!(ns.is_reserved());
    }

    #[test]
    fn test_namespace_id_case_insensitive() {
        // All these should be equal because names are normalized to lowercase
        let ns1 = NamespaceId::new("MyApp");
        let ns2 = NamespaceId::new("myapp");
        let ns3 = NamespaceId::new("MYAPP");
        let ns4 = NamespaceId::from("MyApP".to_string());
        let ns5: NamespaceId = "mYaPp".into();

        assert_eq!(ns1, ns2);
        assert_eq!(ns2, ns3);
        assert_eq!(ns3, ns4);
        assert_eq!(ns4, ns5);

        // Stored value should always be lowercase
        assert_eq!(ns1.as_str(), "myapp");
        assert_eq!(ns3.as_str(), "myapp");
    }

    // Security tests for path traversal prevention
    #[test]
    fn test_try_new_valid_names() {
        assert!(NamespaceId::try_new("myapp").is_ok());
        assert!(NamespaceId::try_new("my_namespace").is_ok());
        assert!(NamespaceId::try_new("namespace123").is_ok());
        assert!(NamespaceId::try_new("a").is_ok());
    }

    #[test]
    fn test_try_new_rejects_empty() {
        let result = NamespaceId::try_new("");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("empty"));
    }

    #[test]
    fn test_try_new_rejects_path_traversal() {
        let result = NamespaceId::try_new("../myapp");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("path traversal"));

        let result = NamespaceId::try_new("myapp/../admin");
        assert!(result.is_err());

        let result = NamespaceId::try_new("..%2fmyapp");
        assert!(result.is_err());
    }

    #[test]
    fn test_try_new_rejects_forward_slash() {
        let result = NamespaceId::try_new("path/to/namespace");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("path separators"));
    }

    #[test]
    fn test_try_new_rejects_backslash() {
        let result = NamespaceId::try_new("path\\to\\namespace");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("path separators"));
    }

    #[test]
    fn test_try_new_rejects_null_byte() {
        let result = NamespaceId::try_new("namespace\0name");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("null bytes"));
    }

    #[test]
    #[should_panic(expected = "Invalid namespace ID")]
    fn test_new_panics_on_path_traversal() {
        let _ = NamespaceId::new("../etc/passwd");
    }

    #[test]
    #[should_panic(expected = "Invalid namespace ID")]
    fn test_from_panics_on_path_traversal() {
        let _: NamespaceId = "../etc/passwd".into();
    }
}
