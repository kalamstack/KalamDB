use serde::{Deserialize, Serialize};
use std::fmt;

/// Type-safe wrapper for usernames in the kalam-link SDK.
///
/// Prevents confusion between usernames and other string identifiers
/// (user IDs, topic names, group IDs, etc.) at compile time.
///
/// # Example (Rust)
/// ```rust
/// let username = Username::new("alice");
/// assert_eq!(username.as_str(), "alice");
/// ```
///
/// # TypeScript
/// Generated via tsify as a branded `string` newtype.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct Username(String);

impl Username {
    /// Create a new Username from a string.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the username as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the wrapper and return the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for Username {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Username {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Username {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for Username {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
