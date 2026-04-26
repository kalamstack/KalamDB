use wasm_bindgen::prelude::JsValue;

/// Validate a SQL identifier (table name, column name) to prevent SQL injection.
/// Only allows: letters, numbers, underscores, and dots (for namespace.table format).
/// Must start with a letter or underscore.
#[inline]
pub(crate) fn validate_sql_identifier(name: &str, context: &str) -> Result<(), JsValue> {
    if name.is_empty() {
        return Err(JsValue::from_str(&format!("{} cannot be empty", context)));
    }
    if name.len() > 128 {
        return Err(JsValue::from_str(&format!("{} too long (max 128 chars)", context)));
    }

    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(JsValue::from_str(&format!(
            "{} must start with a letter or underscore",
            context
        )));
    }

    // Only allow alphanumeric, underscore, and dot (for namespace.table)
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '.' {
            return Err(JsValue::from_str(&format!(
                "{} contains invalid character '{}'. Only letters, numbers, underscores, and dots \
                 allowed",
                context, c
            )));
        }
    }

    // Check for path traversal attempts
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err(JsValue::from_str(&format!("{} contains forbidden sequence", context)));
    }

    Ok(())
}

/// Validate a row ID to prevent SQL injection.
/// Accepts: UUIDs, integers, or alphanumeric strings with underscores/hyphens.
///
/// Optimized to avoid heap allocation: uses case-insensitive byte matching
/// instead of `.to_uppercase()`.
pub(crate) fn validate_row_id(row_id: &str) -> Result<(), JsValue> {
    if row_id.is_empty() {
        return Err(JsValue::from_str("Row ID cannot be empty"));
    }
    if row_id.len() > 128 {
        return Err(JsValue::from_str("Row ID too long (max 128 chars)"));
    }

    // Check for SQL injection patterns using case-insensitive matching
    // without allocating a new uppercase string.
    let dangerous_patterns: &[&[u8]] = &[b";", b"--", b"/*", b"*/", b"'", b"\""];
    let dangerous_keywords: &[&[u8]] = &[
        b"DROP", b"DELETE", b"UPDATE", b"INSERT", b"UNION", b"SELECT",
    ];
    let bytes = row_id.as_bytes();

    for pattern in dangerous_patterns {
        if contains_bytes(bytes, pattern) {
            return Err(JsValue::from_str(&format!(
                "Row ID contains forbidden pattern '{}'",
                std::str::from_utf8(pattern).unwrap_or("?")
            )));
        }
    }

    for keyword in dangerous_keywords {
        if contains_bytes_case_insensitive(bytes, keyword) {
            return Err(JsValue::from_str(&format!(
                "Row ID contains forbidden pattern '{}'",
                std::str::from_utf8(keyword).unwrap_or("?")
            )));
        }
    }

    // Only allow safe characters: alphanumeric, underscore, hyphen
    for c in row_id.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
            return Err(JsValue::from_str(&format!("Row ID contains invalid character '{}'", c)));
        }
    }

    Ok(())
}

/// Check if `haystack` contains `needle` (exact byte match).
#[inline]
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Check if `haystack` contains `needle` using ASCII case-insensitive matching.
/// `needle` MUST be all-uppercase ASCII.
#[inline]
fn contains_bytes_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|w| w.iter().zip(needle.iter()).all(|(h, n)| h.to_ascii_uppercase() == *n))
}

/// Validate a column name for INSERT operations
#[inline]
pub(crate) fn validate_column_name(name: &str) -> Result<(), JsValue> {
    validate_sql_identifier(name, "Column name")
}

/// Quote a table name properly, handling namespace.table format.
/// Converts "namespace.table" to "namespace"."table" for correct SQL parsing.
#[inline]
pub(crate) fn quote_table_name(table_name: &str) -> String {
    if let Some(dot_pos) = table_name.find('.') {
        let namespace = &table_name[..dot_pos];
        let table = &table_name[dot_pos + 1..];
        format!("\"{}\".\"{}\"", namespace.replace('"', "\"\""), table.replace('"', "\"\""))
    } else {
        format!("\"{}\"", table_name.replace('"', "\"\""))
    }
}
