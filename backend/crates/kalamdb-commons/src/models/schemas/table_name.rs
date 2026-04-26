//! Type-safe wrapper for table names.

use std::{fmt, sync::Arc};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Error returned when a table name fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableNameValidationError {
    pub name: String,
    pub reason: String,
}

impl fmt::Display for TableNameValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid table name '{}': {}", self.name, self.reason)
    }
}

impl std::error::Error for TableNameValidationError {}

/// Type-safe wrapper for table names.
///
/// Stored as `Arc<str>` so `clone()` is a cheap atomic refcount increment
/// rather than a heap allocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TableName(Arc<str>);

impl TableName {
    /// Validates a table name for security issues.
    ///
    /// Returns an error if the name:
    /// - Is empty
    /// - Contains path traversal sequences (`..`)
    /// - Contains path separators (`/` or `\`)
    /// - Contains null bytes
    fn validate(name: &str) -> Result<(), TableNameValidationError> {
        if name.is_empty() {
            return Err(TableNameValidationError {
                name: name.to_string(),
                reason: "Table name cannot be empty".to_string(),
            });
        }

        if name.contains("..") {
            return Err(TableNameValidationError {
                name: name.to_string(),
                reason: "Table name cannot contain '..' (path traversal)".to_string(),
            });
        }

        if name.contains('/') || name.contains('\\') {
            return Err(TableNameValidationError {
                name: name.to_string(),
                reason: "Table name cannot contain path separators".to_string(),
            });
        }

        if name.contains('\0') {
            return Err(TableNameValidationError {
                name: name.to_string(),
                reason: "Table name cannot contain null bytes".to_string(),
            });
        }

        Ok(())
    }

    /// Creates a new TableName from a string, with validation.
    ///
    /// Returns an error if the name fails security validation.
    ///
    /// Table names are case-insensitive - they are normalized to lowercase internally.
    pub fn try_new(name: impl Into<String>) -> Result<Self, TableNameValidationError> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(Self(Arc::<str>::from(name.to_lowercase())))
    }

    /// Creates a new TableName from a string.
    ///
    /// Table names are case-insensitive - they are normalized to lowercase internally.
    /// For example, `TableName::new("Users")` and `TableName::new("users")` are equal.
    ///
    /// # Panics
    ///
    /// Panics if the name fails security validation. Use `try_new` for fallible creation.
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self::validate(&name).expect("Invalid table name");
        Self(Arc::<str>::from(name.to_lowercase()))
    }

    /// Returns the table name as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner String.
    #[inline]
    pub fn into_string(self) -> String {
        String::from(&*self.0)
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TableName {
    fn from(s: String) -> Self {
        Self::validate(&s).expect("Invalid table name");
        Self(Arc::<str>::from(s.to_lowercase()))
    }
}

impl From<&str> for TableName {
    fn from(s: &str) -> Self {
        Self::validate(s).expect("Invalid table name");
        Self(Arc::<str>::from(s.to_lowercase()))
    }
}

impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_case_insensitive() {
        // All these should be equal because names are normalized to lowercase
        let name1 = TableName::new("Users");
        let name2 = TableName::new("users");
        let name3 = TableName::new("USERS");
        let name4 = TableName::from("UsErS".to_string());
        let name5: TableName = "uSeRs".into();

        assert_eq!(name1, name2);
        assert_eq!(name2, name3);
        assert_eq!(name3, name4);
        assert_eq!(name4, name5);

        // Stored value should always be lowercase
        assert_eq!(name1.as_str(), "users");
        assert_eq!(name3.as_str(), "users");
    }

    #[test]
    fn test_table_name_display() {
        let name = TableName::new("MyTable");
        assert_eq!(format!("{}", name), "mytable");
    }

    // Security tests for path traversal prevention
    #[test]
    fn test_try_new_valid_names() {
        assert!(TableName::try_new("users").is_ok());
        assert!(TableName::try_new("my_table").is_ok());
        assert!(TableName::try_new("table123").is_ok());
        assert!(TableName::try_new("a").is_ok());
    }

    #[test]
    fn test_try_new_rejects_empty() {
        let result = TableName::try_new("");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("empty"));
    }

    #[test]
    fn test_try_new_rejects_path_traversal() {
        let result = TableName::try_new("../users");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("path traversal"));

        let result = TableName::try_new("users/../admin");
        assert!(result.is_err());

        let result = TableName::try_new("..%2fusers");
        assert!(result.is_err());
    }

    #[test]
    fn test_try_new_rejects_forward_slash() {
        let result = TableName::try_new("path/to/table");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("path separators"));
    }

    #[test]
    fn test_try_new_rejects_backslash() {
        let result = TableName::try_new("path\\to\\table");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("path separators"));
    }

    #[test]
    fn test_try_new_rejects_null_byte() {
        let result = TableName::try_new("table\0name");
        assert!(result.is_err());
        assert!(result.unwrap_err().reason.contains("null bytes"));
    }

    #[test]
    #[should_panic(expected = "Invalid table name")]
    fn test_new_panics_on_path_traversal() {
        let _ = TableName::new("../etc/passwd");
    }

    #[test]
    #[should_panic(expected = "Invalid table name")]
    fn test_from_panics_on_path_traversal() {
        let _: TableName = "../etc/passwd".into();
    }
}
