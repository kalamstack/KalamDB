//! Payload extraction for topic messages.
//!
//! Converts Row data to serialized byte payloads based on the route's PayloadMode.

use kalamdb_commons::{
    conversions::arrow_json_conversion::row_to_json_map,
    errors::{CommonError, Result},
    models::{rows::Row, KalamCellValue, PayloadMode, TableId},
};
use kalamdb_system::providers::topics::TopicRoute;

/// Pre-computed row data that avoids repeated JSON serialization.
///
/// Calling `row_to_json_map` is expensive. This struct caches the result so
/// `extract_payload`, `extract_key`, and `hash_row` can share the same map
/// instead of each serializing independently.
///
/// Optimization: for Full/Diff mode, the `_table` field is pre-inserted at
/// construction time and payload bytes are pre-serialized, eliminating the
/// per-message `HashMap::clone()` and re-serialization.
pub(crate) struct PreparedRow {
    /// Pre-serialized payload bytes for Key mode.
    key_payload: Vec<u8>,
    /// Cached JSON map for primary-key extraction without reserializing the row.
    json_map: std::collections::HashMap<String, KalamCellValue>,
    /// Pre-serialized payload bytes for Full/Diff mode (with `_table` already injected).
    /// Only `Some` when `from_row_with_table` is used.
    full_payload: Option<Vec<u8>>,
    /// Pre-computed hash for partition selection.
    partition_hash: u64,
}

impl PreparedRow {
    /// Build a PreparedRow from a Row. This calls `row_to_json_map` exactly once.
    pub fn from_row(row: &Row) -> Result<Self> {
        let json_map = row_to_json_map(row)
            .map_err(|e| CommonError::Internal(format!("Failed to convert row to JSON: {}", e)))?;
        let key_payload = serde_json::to_vec(&json_map)
            .map_err(|e| CommonError::Internal(format!("Failed to serialize keys: {}", e)))?;
        // SAFETY: serde_json::to_vec() always produces valid UTF-8 JSON bytes.
        let partition_hash = hash_key(unsafe { std::str::from_utf8_unchecked(&key_payload) });
        Ok(Self {
            key_payload,
            json_map,
            full_payload: None,
            partition_hash,
        })
    }

    /// Build a PreparedRow with pre-injected `_table` metadata for Full/Diff mode.
    ///
    /// This avoids the per-message `HashMap::clone()` + re-serialization that
    /// previously happened inside `extract_payload()`.
    pub fn from_row_with_table(row: &Row, table_id: &TableId) -> Result<Self> {
        let mut json_map = row_to_json_map(row)
            .map_err(|e| CommonError::Internal(format!("Failed to convert row to JSON: {}", e)))?;
        let key_payload = serde_json::to_vec(&json_map)
            .map_err(|e| CommonError::Internal(format!("Failed to serialize keys: {}", e)))?;
        // SAFETY: serde_json::to_vec() always produces valid UTF-8 JSON bytes.
        let partition_hash = hash_key(unsafe { std::str::from_utf8_unchecked(&key_payload) });
        // Pre-insert _table and serialize for Full/Diff payloads.
        json_map.insert("_table".to_string(), KalamCellValue::text(table_id.to_string()));
        let full_payload = serde_json::to_vec(&json_map)
            .map_err(|e| CommonError::Internal(format!("Failed to serialize row: {}", e)))?;
        json_map.remove("_table");
        Ok(Self {
            key_payload,
            json_map,
            full_payload: Some(full_payload),
            partition_hash,
        })
    }

    /// Extract payload bytes using pre-computed data (zero cloning).
    #[inline]
    pub fn extract_payload(&self, route: &TopicRoute, table_id: &TableId) -> Result<Vec<u8>> {
        match route.payload_mode {
            PayloadMode::Key => Ok(self.key_payload.clone()),
            PayloadMode::Full | PayloadMode::Diff => {
                // If we pre-computed full payload, return it directly.
                if let Some(ref full) = self.full_payload {
                    Ok(full.clone())
                } else {
                    // Fallback: build with _table injection (shouldn't happen in batch path).
                    let mut raw: serde_json::Value = serde_json::from_slice(&self.key_payload)
                        .map_err(|e| {
                            CommonError::Internal(format!("Failed to parse cached JSON: {}", e))
                        })?;
                    if let serde_json::Value::Object(ref mut map) = raw {
                        map.insert(
                            "_table".to_string(),
                            serde_json::Value::String(table_id.to_string()),
                        );
                    }
                    serde_json::to_vec(&raw).map_err(|e| {
                        CommonError::Internal(format!("Failed to serialize row: {}", e))
                    })
                }
            },
        }
    }

    /// Extract a message key from the cached primary-key columns.
    #[inline]
    pub fn extract_key(&self, primary_key_columns: &[String]) -> Result<Option<String>> {
        extract_primary_key(&self.json_map, primary_key_columns)
    }

    /// Hash the row for consistent partition selection using the cached hash.
    #[inline]
    pub fn hash_row(&self) -> u64 {
        self.partition_hash
    }
}

/// Extract payload bytes from a Row based on the route's PayloadMode.
///
/// For Full and Diff modes, injects `_table` metadata so consumers can identify
/// the source table of each message.
pub(crate) fn extract_payload(
    route: &TopicRoute,
    row: &Row,
    table_id: &TableId,
) -> Result<Vec<u8>> {
    match route.payload_mode {
        PayloadMode::Key => extract_key_columns(row),
        PayloadMode::Full | PayloadMode::Diff => extract_full_row_with_metadata(row, table_id),
    }
}

/// Extract a message key from a Row using the table primary key columns.
pub(crate) fn extract_key(row: &Row, primary_key_columns: &[String]) -> Result<Option<String>> {
    let json_map = row_to_json_map(row)
        .map_err(|e| CommonError::Internal(format!("Failed to convert row to JSON: {}", e)))?;

    extract_primary_key(&json_map, primary_key_columns)
}

/// Hash a row for consistent partition selection.
pub(crate) fn hash_row(row: &Row) -> u64 {
    // Hash the JSON representation for consistency
    if let Ok(json_map) = row_to_json_map(row) {
        if let Ok(json_str) = serde_json::to_string(&json_map) {
            return hash_key(&json_str);
        }
    }

    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    let mut hasher = DefaultHasher::new();

    // Fallback: hash column names only
    for key in row.values.keys() {
        key.hash(&mut hasher);
    }

    hasher.finish()
}

/// Hash a serialized topic key using the same stable hash as partition selection.
pub(crate) fn hash_key(key: &str) -> u64 {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

// ---- Internal helpers ----

fn extract_primary_key(
    json_map: &std::collections::HashMap<String, KalamCellValue>,
    primary_key_columns: &[String],
) -> Result<Option<String>> {
    if primary_key_columns.is_empty() {
        return Ok(None);
    }

    if primary_key_columns.len() == 1 {
        let column = &primary_key_columns[0];
        let value = json_map.get(column).ok_or_else(|| {
            CommonError::InvalidInput(format!("Row missing primary key column '{}'", column))
        })?;

        return Ok(Some(match value.as_str() {
            Some(text) => text.to_string(),
            None => value.to_string(),
        }));
    }

    let mut key_map = std::collections::BTreeMap::new();
    for column in primary_key_columns {
        let value = json_map.get(column).ok_or_else(|| {
            CommonError::InvalidInput(format!("Row missing primary key column '{}'", column))
        })?;
        key_map.insert(column.as_str(), value);
    }

    serde_json::to_string(&key_map)
        .map(Some)
        .map_err(|e| CommonError::Internal(format!("Failed to serialize key: {}", e)))
}

fn extract_key_columns(row: &Row) -> Result<Vec<u8>> {
    if row.values.is_empty() {
        return Err(CommonError::InvalidInput("Cannot extract key from empty row".to_string()));
    }

    let json_map = row_to_json_map(row)
        .map_err(|e| CommonError::Internal(format!("Failed to convert row to JSON: {}", e)))?;

    serde_json::to_vec(&json_map)
        .map_err(|e| CommonError::Internal(format!("Failed to serialize keys: {}", e)))
}

/// Extract full row payload with `_table` metadata injected.
///
/// This allows consumers to identify which source table produced each message
/// without needing to track route configurations.
fn extract_full_row_with_metadata(row: &Row, table_id: &TableId) -> Result<Vec<u8>> {
    let mut json_map = row_to_json_map(row)
        .map_err(|e| CommonError::Internal(format!("Failed to convert row to JSON: {}", e)))?;

    // Inject source table metadata (uses system column convention with underscore prefix)
    json_map.insert("_table".to_string(), KalamCellValue::text(table_id.to_string()));

    serde_json::to_vec(&json_map)
        .map_err(|e| CommonError::Internal(format!("Failed to serialize row: {}", e)))
}
