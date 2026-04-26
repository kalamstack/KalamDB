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
//! # Typed accessors — one per KalamDataType
//!
//! Call the method that matches the column's declared `KalamDataType`:
//!
//! | KalamDataType | Method            | Return type        |
//! |---------------|-------------------|--------------------|
//! | Text          | `as_text()`       | `Option<&str>`     |
//! | Boolean       | `as_boolean()`    | `Option<bool>`     |
//! | SmallInt      | `as_small_int()`  | `Option<i16>`      |
//! | Int           | `as_int()`        | `Option<i32>`      |
//! | BigInt        | `as_big_int()`    | `Option<i64>`      |
//! | Float         | `as_float()`      | `Option<f32>`      |
//! | Double        | `as_double()`     | `Option<f64>`      |
//! | Decimal       | `as_decimal()`    | `Option<f64>`      |
//! | Timestamp     | `as_timestamp()`  | `Option<i64>` (µs since epoch) |
//! | Date          | `as_date()`       | `Option<i32>` (days since epoch) |
//! | DateTime      | `as_datetime()`   | `Option<i64>` (µs since epoch) |
//! | Time          | `as_time()`       | `Option<i64>` (µs since midnight) |
//! | Uuid          | `as_uuid()`       | `Option<&str>`     |
//! | Json          | `as_json()`       | `Option<&JsonValue>` |
//! | Bytes         | `as_bytes()`      | `Option<Vec<u8>>` (base64-decoded) |
//! | Embedding     | `as_embedding()`  | `Option<Vec<f32>>` |
//! | File          | `as_file()`       | `Option<FileRef>`  |
//!
//! # Wire format
//!
//! Serializes identically to `serde_json::Value` — no breaking changes:
//! ```json
//! "Alice"          // Text / Uuid
//! 42               // Int / BigInt
//! true             // Boolean
//! null             // Null
//! "1699000000000"  // Timestamp (string-encoded for i64 precision)
//! {"id":"..."}     // File column (JSON object)
//! ```

use std::{fmt, ops::Deref};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// A single cell value in a query result row or subscription notification.
///
/// Thin wrapper around [`serde_json::Value`] with `#[serde(transparent)]`
/// for zero-cost JSON (de)serialization. Implements [`Deref`] to `JsonValue`
/// so all existing `serde_json` accessor methods (`.as_str()`, `.is_null()`,
/// `.as_i64()`, …) continue to work unchanged.
///
/// Use the typed accessor methods (e.g. [`as_big_int`][Self::as_big_int],
/// [`as_file`][Self::as_file]) to get values in the correct Rust type
/// matching the column's [`KalamDataType`][super::kalam_data_type::KalamDataType].
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

    /// Integer cell value (`Int` / `BigInt` / `SmallInt`).
    #[inline]
    pub fn int(i: i64) -> Self {
        Self(JsonValue::Number(i.into()))
    }

    /// Floating-point cell value (`Float` / `Double`).
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

// ── Raw accessors ─────────────────────────────────────────────────────────

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

// ── Typed accessors — one per KalamDataType ──────────────────────────────────

impl KalamCellValue {
    // ── Text ──────────────────────────────────────────────────────────────

    /// Extract the cell as a UTF-8 string slice (`Text` columns).
    #[inline]
    pub fn as_text(&self) -> Option<&str> {
        self.0.as_str()
    }

    // ── Numeric ───────────────────────────────────────────────────────────

    /// Extract the cell as a 16-bit integer (`SmallInt` columns).
    ///
    /// Handles both JSON numbers and string-encoded integers.
    pub fn as_small_int(&self) -> Option<i16> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64().map(|v| v as i16),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as a 32-bit integer (`Int` columns).
    ///
    /// Handles both JSON numbers and string-encoded integers.
    pub fn as_int(&self) -> Option<i32> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64().map(|v| v as i32),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as a 64-bit integer (`BigInt` columns).
    ///
    /// The backend serializes `BigInt` as a JSON string to preserve precision —
    /// this method handles both the string and numeric forms.
    pub fn as_big_int(&self) -> Option<i64> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64(),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as a 32-bit float (`Float` columns).
    ///
    /// Handles both JSON numbers and string-encoded floats.
    pub fn as_float(&self) -> Option<f32> {
        match &self.0 {
            JsonValue::Number(n) => n.as_f64().map(|v| v as f32),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as a 64-bit float (`Double` columns).
    ///
    /// Handles both JSON numbers and string-encoded floats.
    pub fn as_double(&self) -> Option<f64> {
        match &self.0 {
            JsonValue::Number(n) => n.as_f64(),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as a `f64` (`Decimal` columns).
    ///
    /// Handles both JSON numbers and string-encoded decimal values.
    pub fn as_decimal(&self) -> Option<f64> {
        match &self.0 {
            JsonValue::Number(n) => n.as_f64(),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    // ── Boolean ───────────────────────────────────────────────────────────

    /// Extract the cell as a boolean (`Boolean` columns).
    #[inline]
    pub fn as_boolean(&self) -> Option<bool> {
        self.0.as_bool()
    }

    // ── Temporal ──────────────────────────────────────────────────────────

    /// Extract the cell as microseconds since Unix epoch (`Timestamp` columns).
    ///
    /// KalamDB serializes timestamps as strings to avoid JSON integer
    /// precision loss — this method handles both string and numeric forms.
    pub fn as_timestamp(&self) -> Option<i64> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64(),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as days since Unix epoch (`Date` columns).
    pub fn as_date(&self) -> Option<i32> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64().map(|v| v as i32),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as microseconds since Unix epoch (`DateTime` columns).
    pub fn as_datetime(&self) -> Option<i64> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64(),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract the cell as microseconds since midnight (`Time` columns).
    pub fn as_time(&self) -> Option<i64> {
        match &self.0 {
            JsonValue::Number(n) => n.as_i64(),
            JsonValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    // ── Identity / structured ─────────────────────────────────────────────

    /// Extract the cell as a UUID string (`Uuid` columns).
    #[inline]
    pub fn as_uuid(&self) -> Option<&str> {
        self.0.as_str()
    }

    /// Extract the cell as a raw JSON value (`Json` columns).
    ///
    /// Returns `Some` only when the cell contains a JSON object or array.
    pub fn as_json(&self) -> Option<&JsonValue> {
        match &self.0 {
            JsonValue::Object(_) | JsonValue::Array(_) => Some(&self.0),
            _ => None,
        }
    }

    /// Extract the cell as raw bytes, base64-decoded (`Bytes` columns).
    ///
    /// KalamDB stores binary data as base64-encoded strings.
    pub fn as_bytes(&self) -> Option<Vec<u8>> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        self.0.as_str().and_then(|s| STANDARD.decode(s).ok())
    }

    // ── Vector / ML ───────────────────────────────────────────────────────

    /// Extract the cell as a float32 vector (`Embedding` columns).
    ///
    /// Returns `None` if any element cannot be parsed as a number.
    pub fn as_embedding(&self) -> Option<Vec<f32>> {
        self.0.as_array().and_then(|arr| {
            arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect::<Option<Vec<_>>>()
        })
    }

    // ── File ──────────────────────────────────────────────────────────────

    /// Extract the cell as a [`FileRef`] (`File` columns).
    ///
    /// FILE columns are stored as a JSON object (or a JSON-encoded string).
    /// This method handles both representations automatically.
    ///
    /// # Example
    /// ```rust
    /// use kalam_client::KalamCellValue;
    ///
    /// // From a JSON object
    /// let cell = KalamCellValue::from(serde_json::json!({
    ///     "id": "123", "sub": "f0001", "name": "photo.png",
    ///     "size": 1024, "mime": "image/png", "sha256": "abc"
    /// }));
    /// let file_ref = cell.as_file().unwrap();
    /// assert_eq!(file_ref.name, "photo.png");
    /// assert!(file_ref.is_image());
    /// ```
    pub fn as_file(&self) -> Option<super::file_ref::FileRef> {
        super::file_ref::FileRef::from_json_value(&self.0)
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
    fn from(v: JsonValue) -> Self {
        Self(v)
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
        match &self.0 {
            JsonValue::Null => write!(f, "NULL"),
            JsonValue::String(s) => write!(f, "{s}"),
            JsonValue::Number(n) => write!(f, "{n}"),
            JsonValue::Bool(b) => write!(f, "{b}"),
            other => write!(f, "{other}"),
        }
    }
}

impl Default for KalamCellValue {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

/// Type alias for a subscription row — named columns.
pub type RowData = std::collections::HashMap<String, KalamCellValue>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transparent_roundtrip() {
        let cell = KalamCellValue::text("hello");
        let json = serde_json::to_string(&cell).unwrap();
        assert_eq!(json, r#""hello""#);
        let back: KalamCellValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cell);
    }

    #[test]
    fn null_cell() {
        let cell = KalamCellValue::null();
        assert!(cell.is_null());
        assert_eq!(cell.to_string(), "NULL");
    }

    #[test]
    fn int_cell() {
        let cell = KalamCellValue::int(42);
        assert_eq!(cell.as_i64(), Some(42));
    }

    #[test]
    fn deref_access() {
        let cell = KalamCellValue::text("world");
        assert_eq!(cell.as_str(), Some("world"));
    }

    #[test]
    fn from_json_value() {
        let jv = serde_json::json!({"key": "val"});
        let cell = KalamCellValue::from(jv.clone());
        assert_eq!(*cell, jv);
    }

    #[test]
    fn into_json_value() {
        let cell = KalamCellValue::boolean(true);
        let jv: JsonValue = cell.into();
        assert_eq!(jv, JsonValue::Bool(true));
    }
}
