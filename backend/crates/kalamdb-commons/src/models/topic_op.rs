//! Topic operation type enumeration.

use std::{fmt, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Enum representing topic operation types for change data capture.
///
/// Topics can be configured to capture specific types of table operations,
/// enabling fine-grained control over which events are published.
///
/// # Examples
/// ```
/// use kalamdb_commons::models::TopicOp;
///
/// let op = TopicOp::Insert;
/// assert_eq!(op.as_str(), "insert");
/// assert_eq!(op.to_string(), "insert");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum TopicOp {
    /// Captures INSERT operations on source tables
    #[default]
    Insert,
    /// Captures UPDATE operations on source tables
    Update,
    /// Captures DELETE operations on source tables
    Delete,
}

impl TopicOp {
    /// Returns the string representation of the operation type.
    pub fn as_str(&self) -> &'static str {
        match self {
            TopicOp::Insert => "insert",
            TopicOp::Update => "update",
            TopicOp::Delete => "delete",
        }
    }

    /// Attempts to parse a TopicOp from a string, returning None if invalid.
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "insert" => Some(TopicOp::Insert),
            "update" => Some(TopicOp::Update),
            "delete" => Some(TopicOp::Delete),
            _ => None,
        }
    }
}

impl FromStr for TopicOp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TopicOp::from_str_opt(s)
            .ok_or_else(|| format!("Invalid TopicOp: '{}'. Expected: insert, update, delete", s))
    }
}

impl fmt::Display for TopicOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for TopicOp {
    fn from(s: &str) -> Self {
        s.parse().unwrap_or(TopicOp::Insert)
    }
}

impl From<String> for TopicOp {
    fn from(s: String) -> Self {
        TopicOp::from(s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_op_as_str() {
        assert_eq!(TopicOp::Insert.as_str(), "insert");
        assert_eq!(TopicOp::Update.as_str(), "update");
        assert_eq!(TopicOp::Delete.as_str(), "delete");
    }

    #[test]
    fn test_topic_op_display() {
        assert_eq!(TopicOp::Insert.to_string(), "insert");
        assert_eq!(TopicOp::Update.to_string(), "update");
        assert_eq!(TopicOp::Delete.to_string(), "delete");
    }

    #[test]
    fn test_topic_op_from_str() {
        assert_eq!("insert".parse::<TopicOp>().unwrap(), TopicOp::Insert);
        assert_eq!("INSERT".parse::<TopicOp>().unwrap(), TopicOp::Insert);
        assert_eq!("update".parse::<TopicOp>().unwrap(), TopicOp::Update);
        assert_eq!("delete".parse::<TopicOp>().unwrap(), TopicOp::Delete);
        assert!("invalid".parse::<TopicOp>().is_err());
    }

    #[test]
    fn test_topic_op_from_str_opt() {
        assert_eq!(TopicOp::from_str_opt("insert"), Some(TopicOp::Insert));
        assert_eq!(TopicOp::from_str_opt("UPDATE"), Some(TopicOp::Update));
        assert_eq!(TopicOp::from_str_opt("invalid"), None);
    }
}
