//! KalamCellValue — type-safe wrapper for individual cell values
//!
//! Replaces raw `serde_json::Value` in query results and subscription
//! notifications while keeping the exact same JSON wire format via
//! `#[serde(transparent)]`.
//!
//! # Two row shapes, one cell type
//!
//! ```text
//! Query results  → Vec<Vec<KalamCellValue>>          (positional)
//! Subscriptions  → HashMap<String, KalamCellValue>   (named)
//! ```
//!
//! # Wire format
//!
//! Serializes identically to `serde_json::Value` — no breaking changes:
//! ```json
//! "Alice"          // Text
//! 42               // Number
//! true             // Boolean
//! null             // Null
//! {"id":"..."}     // Object (e.g., FILE column)
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use std::ops::Deref;

/// A single cell value in a query result row or subscription notification.
///
/// Thin wrapper around [`serde_json::Value`] with `#[serde(transparent)]`
/// for zero-cost JSON (de)serialization. Implements [`Deref`] to `JsonValue`
/// so existing accessor methods (`.as_str()`, `.is_null()`, `.as_i64()`, …)
/// continue to work unchanged.
///
/// # Constructors
///
/// ```rust
/// use kalamdb_commons::models::KalamCellValue;
///
/// let cell = KalamCellValue::text("hello");
/// assert_eq!(cell.as_str(), Some("hello"));
///
/// let cell = KalamCellValue::null();
/// assert!(cell.is_null());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KalamCellValue(pub JsonValue);

// ── Constructors ────────────────────────────────────────────────────────────

impl KalamCellValue {
    /// Null cell value.
    #[inline]
    pub fn null() -> Self {
        Self(JsonValue::Null)
    }

    /// Text (string) cell value.
    #[inline]
    pub fn text(s: impl Into<String>) -> Self {
        Self(JsonValue::String(s.into()))
    }

    /// Boolean cell value.
    #[inline]
    pub fn boolean(b: bool) -> Self {
        Self(JsonValue::Bool(b))
    }

    /// Integer cell value.
    #[inline]
    pub fn int(i: i64) -> Self {
        Self(JsonValue::Number(i.into()))
    }

    /// Floating-point cell value.
    ///
    /// Returns `None` if the value is NaN or infinity (not representable in JSON).
    #[inline]
    pub fn float(f: f64) -> Option<Self> {
        serde_json::Number::from_f64(f).map(|n| Self(JsonValue::Number(n)))
    }

    /// Cell value from a raw JSON value.
    #[inline]
    pub fn from_json(value: JsonValue) -> Self {
        Self(value)
    }
}

// ── Accessors ───────────────────────────────────────────────────────────────

impl KalamCellValue {
    /// Borrow the inner `serde_json::Value`.
    #[inline]
    pub fn inner(&self) -> &JsonValue {
        &self.0
    }

    /// Consume `self` and return the inner `serde_json::Value`.
    #[inline]
    pub fn into_inner(self) -> JsonValue {
        self.0
    }
}

// ── Trait impls ─────────────────────────────────────────────────────────────

impl Deref for KalamCellValue {
    type Target = JsonValue;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<JsonValue> for KalamCellValue {
    #[inline]
    fn from(value: JsonValue) -> Self {
        Self(value)
    }
}

impl From<KalamCellValue> for JsonValue {
    #[inline]
    fn from(cell: KalamCellValue) -> Self {
        cell.0
    }
}

impl fmt::Display for KalamCellValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let cell = KalamCellValue::null();
        assert!(cell.is_null());
        assert_eq!(serde_json::to_string(&cell).unwrap(), "null");
    }

    #[test]
    fn test_text() {
        let cell = KalamCellValue::text("hello");
        assert_eq!(cell.as_str(), Some("hello"));
        assert_eq!(serde_json::to_string(&cell).unwrap(), "\"hello\"");
    }

    #[test]
    fn test_boolean() {
        let cell = KalamCellValue::boolean(true);
        assert_eq!(cell.as_bool(), Some(true));
        assert_eq!(serde_json::to_string(&cell).unwrap(), "true");
    }

    #[test]
    fn test_int() {
        let cell = KalamCellValue::int(42);
        assert_eq!(cell.as_i64(), Some(42));
        assert_eq!(serde_json::to_string(&cell).unwrap(), "42");
    }

    #[test]
    fn test_float() {
        let cell = KalamCellValue::float(3.14).unwrap();
        assert!((cell.as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_nan_returns_none() {
        assert!(KalamCellValue::float(f64::NAN).is_none());
    }

    #[test]
    fn test_transparent_serde_roundtrip() {
        let original = serde_json::json!({"id": "abc", "size": 1024});
        let cell = KalamCellValue::from(original.clone());
        let serialized = serde_json::to_string(&cell).unwrap();
        let deserialized: KalamCellValue = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, original);
    }

    #[test]
    fn test_deref_methods() {
        let cell = KalamCellValue::text("test");
        // These use Deref<Target = JsonValue>
        assert!(cell.is_string());
        assert!(!cell.is_null());
        assert!(!cell.is_number());
    }

    #[test]
    fn test_partial_eq() {
        let a = KalamCellValue::int(42);
        let b = KalamCellValue::int(42);
        let c = KalamCellValue::int(43);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_from_json_value() {
        let json = serde_json::json!([1, 2, 3]);
        let cell = KalamCellValue::from(json.clone());
        assert_eq!(*cell.inner(), json);
    }

    #[test]
    fn test_into_inner() {
        let json = serde_json::json!("hello");
        let cell = KalamCellValue::from(json.clone());
        assert_eq!(cell.into_inner(), json);
    }
}
