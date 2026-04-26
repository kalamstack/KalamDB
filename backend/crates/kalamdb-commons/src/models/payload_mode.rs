//! Payload mode enumeration for topic messages.

use std::{fmt, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Enum representing topic payload modes for controlling message content.
///
/// Different payload modes provide tradeoffs between message size, network bandwidth,
/// and downstream processing requirements.
///
/// # Examples
/// ```
/// use kalamdb_commons::models::PayloadMode;
///
/// let mode = PayloadMode::Key;
/// assert_eq!(mode.as_str(), "key");
/// assert_eq!(mode.to_string(), "key");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum PayloadMode {
    /// Only primary key values are included in the message.
    ///
    /// Consumers must fetch full records separately. Minimizes message size
    /// and network bandwidth. Ideal for high-volume change streams where
    /// consumers only need to know *which* records changed.
    Key,

    /// Full row snapshot is included in the message.
    ///
    /// Consumers receive all column values directly. Eliminates need for
    /// additional lookups but increases message size. Best for consumers
    /// that need complete record state.
    Full,

    /// Only changed columns are included (future enhancement).
    ///
    /// Captures before/after values for modified columns only. Reduces
    /// message size for UPDATE operations while providing richer context
    /// than Key mode. Currently unimplemented.
    Diff,
}

impl PayloadMode {
    /// Returns the string representation of the payload mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            PayloadMode::Key => "key",
            PayloadMode::Full => "full",
            PayloadMode::Diff => "diff",
        }
    }

    /// Attempts to parse a PayloadMode from a string, returning None if invalid.
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "key" => Some(PayloadMode::Key),
            "full" => Some(PayloadMode::Full),
            "diff" => Some(PayloadMode::Diff),
            _ => None,
        }
    }

    /// Returns whether this mode is currently supported.
    ///
    /// # Note
    /// Diff mode is planned but not yet implemented.
    pub fn is_supported(&self) -> bool {
        match self {
            PayloadMode::Key | PayloadMode::Full => true,
            PayloadMode::Diff => false,
        }
    }
}

impl FromStr for PayloadMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PayloadMode::from_str_opt(s)
            .ok_or_else(|| format!("Invalid PayloadMode: '{}'. Expected: key, full, diff", s))
    }
}

impl fmt::Display for PayloadMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for PayloadMode {
    /// Returns the default payload mode (Key).
    fn default() -> Self {
        PayloadMode::Key
    }
}

impl From<&str> for PayloadMode {
    fn from(s: &str) -> Self {
        s.parse().unwrap_or_default()
    }
}

impl From<String> for PayloadMode {
    fn from(s: String) -> Self {
        PayloadMode::from(s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_mode_as_str() {
        assert_eq!(PayloadMode::Key.as_str(), "key");
        assert_eq!(PayloadMode::Full.as_str(), "full");
        assert_eq!(PayloadMode::Diff.as_str(), "diff");
    }

    #[test]
    fn test_payload_mode_display() {
        assert_eq!(PayloadMode::Key.to_string(), "key");
        assert_eq!(PayloadMode::Full.to_string(), "full");
        assert_eq!(PayloadMode::Diff.to_string(), "diff");
    }

    #[test]
    fn test_payload_mode_from_str() {
        assert_eq!("key".parse::<PayloadMode>().unwrap(), PayloadMode::Key);
        assert_eq!("KEY".parse::<PayloadMode>().unwrap(), PayloadMode::Key);
        assert_eq!("full".parse::<PayloadMode>().unwrap(), PayloadMode::Full);
        assert_eq!("diff".parse::<PayloadMode>().unwrap(), PayloadMode::Diff);
        assert!("invalid".parse::<PayloadMode>().is_err());
    }

    #[test]
    fn test_payload_mode_is_supported() {
        assert!(PayloadMode::Key.is_supported());
        assert!(PayloadMode::Full.is_supported());
        assert!(!PayloadMode::Diff.is_supported());
    }

    #[test]
    fn test_payload_mode_default() {
        assert_eq!(PayloadMode::default(), PayloadMode::Key);
    }
}
