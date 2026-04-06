use wasm_bindgen::prelude::*;

/// WASM wrapper for TimestampFormatter
#[wasm_bindgen]
pub struct WasmTimestampFormatter {
    inner: crate::timestamp::TimestampFormatter,
}

impl Default for WasmTimestampFormatter {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl WasmTimestampFormatter {
    /// Create a new timestamp formatter with ISO 8601 format
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: crate::timestamp::TimestampFormatter::new(
                crate::timestamp::TimestampFormat::Iso8601,
            ),
        }
    }

    /// Create a formatter with a specific format
    ///
    /// # Arguments
    /// * `format` - One of: "iso8601", "iso8601-date", "iso8601-datetime", "unix-ms", "unix-sec", "relative", "rfc2822", "rfc3339"
    #[wasm_bindgen(js_name = withFormat)]
    pub fn with_format(format: &str) -> Result<WasmTimestampFormatter, JsValue> {
        let fmt = match format {
            "iso8601" => crate::timestamp::TimestampFormat::Iso8601,
            "iso8601-date" => crate::timestamp::TimestampFormat::Iso8601Date,
            "iso8601-datetime" => crate::timestamp::TimestampFormat::Iso8601DateTime,
            "unix-ms" => crate::timestamp::TimestampFormat::UnixMs,
            "unix-sec" => crate::timestamp::TimestampFormat::UnixSec,
            "relative" => crate::timestamp::TimestampFormat::Relative,
            "rfc2822" => crate::timestamp::TimestampFormat::Rfc2822,
            "rfc3339" => crate::timestamp::TimestampFormat::Rfc3339,
            _ => return Err(JsValue::from_str(&format!("Unknown format: {}", format))),
        };

        Ok(Self {
            inner: crate::timestamp::TimestampFormatter::new(fmt),
        })
    }

    /// Format a timestamp (milliseconds since epoch) to a string
    ///
    /// # Arguments
    /// * `milliseconds` - Timestamp in milliseconds since Unix epoch (or null)
    ///
    /// # Returns
    /// Formatted string, or "null" if input is null/undefined
    ///
    /// # Example
    /// ```javascript
    /// const formatter = new WasmTimestampFormatter();
    /// console.log(formatter.format(1734191445123)); // "2024-12-14T15:30:45.123Z"
    /// ```
    pub fn format(&self, milliseconds: Option<f64>) -> String {
        let ms = milliseconds.map(|f| f as i64);
        self.inner.format(ms)
    }

    /// Format a timestamp as relative time (e.g., "2 hours ago")
    ///
    /// # Arguments
    /// * `milliseconds` - Timestamp in milliseconds since Unix epoch
    ///
    /// # Returns
    /// Relative time string (e.g., "just now", "5 minutes ago", "2 days ago")
    #[wasm_bindgen(js_name = formatRelative)]
    pub fn format_relative(&self, milliseconds: f64) -> String {
        self.inner.format_relative(milliseconds as i64)
    }
}

/// Parse an ISO 8601 timestamp string to milliseconds since epoch
///
/// # Arguments
/// * `iso_string` - ISO 8601 formatted string (e.g., "2024-12-14T15:30:45.123Z")
///
/// # Returns
/// Milliseconds since Unix epoch
///
/// # Errors
/// Returns JsValue error if parsing fails
///
/// # Example
/// ```javascript
/// const ms = parseIso8601("2024-12-14T15:30:45.123Z");
/// console.log(ms); // 1734191445123
/// ```
#[wasm_bindgen(js_name = parseIso8601)]
pub fn parse_iso8601(iso_string: &str) -> Result<f64, JsValue> {
    crate::timestamp::parse_iso8601(iso_string)
        .map(|ms| ms as f64)
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Get the current timestamp in milliseconds since epoch
///
/// # Returns
/// Current time in milliseconds
///
/// # Example
/// ```javascript
/// const now = timestampNow();
/// console.log(now); // 1734191445123
/// ```
#[wasm_bindgen(js_name = timestampNow)]
pub fn timestamp_now() -> f64 {
    crate::timestamp::now() as f64
}
