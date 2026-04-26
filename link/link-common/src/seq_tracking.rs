//! Sequence ID tracking helpers for subscriptions.
//!
//! Centralises `_seq` extraction from row data so that all SDKs
//! (native/Dart via `SharedConnection`, WASM/TypeScript via `client.rs`)
//! resume subscriptions from the correct position after reconnect.
//!
//! The `_seq` column is a KalamDB system column present in every
//! subscription row.  It is a Snowflake-based monotonically increasing
//! identifier that the server uses for change tracking.

use std::collections::HashMap;

use crate::{models::KalamCellValue, seq_id::SeqId};

/// Name of the system sequence column in every subscription row.
pub const SEQ_COLUMN: &str = "_seq";

/// Extract a row-level `_seq` value, if present and parseable.
pub fn row_seq(row: &HashMap<String, KalamCellValue>) -> Option<SeqId> {
    row.get(SEQ_COLUMN).and_then(KalamCellValue::as_big_int).map(SeqId::from_i64)
}

/// Extract the maximum `_seq` value from a slice of named-column rows.
///
/// Returns `None` when no row contains a parseable `_seq` value.
///
/// ```ignore
/// let max = extract_max_seq(&rows);
/// if let Some(seq) = max {
///     entry.last_seq_id = Some(seq);
/// }
/// ```
pub fn extract_max_seq(rows: &[HashMap<String, KalamCellValue>]) -> Option<SeqId> {
    let mut max: Option<i64> = None;
    for row in rows {
        if let Some(seq) = row_seq(row).map(|value| value.as_i64()) {
            max = Some(max.map_or(seq, |prev| prev.max(seq)));
        }
    }
    max.map(SeqId::from_i64)
}

/// Remove rows whose `_seq` is less than or equal to `after`.
///
/// Rows without a parseable `_seq` are retained because the client cannot
/// prove they are stale.
pub fn retain_rows_after(rows: &mut Vec<HashMap<String, KalamCellValue>>, after: SeqId) -> usize {
    let original_len = rows.len();
    rows.retain(|row| row_seq(row).map_or(true, |seq| seq > after));
    original_len.saturating_sub(rows.len())
}

/// Update `current` to `candidate` if `candidate` is strictly greater
/// (or `current` is `None`).
///
/// Returns `true` when `current` was advanced.
#[inline]
pub fn advance_seq(current: &mut Option<SeqId>, candidate: SeqId) -> bool {
    if current.map_or(true, |prev| candidate > prev) {
        *current = Some(candidate);
        true
    } else {
        false
    }
}

/// Convenience: extract the max `_seq` from `rows` and advance `current`.
///
/// Combines [`extract_max_seq`] + [`advance_seq`] in one call.
/// Returns `true` when `current` was updated.
pub fn track_rows(current: &mut Option<SeqId>, rows: &[HashMap<String, KalamCellValue>]) -> bool {
    if let Some(seq) = extract_max_seq(rows) {
        advance_seq(current, seq)
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_with_seq(seq: i64) -> HashMap<String, KalamCellValue> {
        let mut row = HashMap::new();
        // _seq is serialised as a string on the wire for i64 precision
        row.insert(SEQ_COLUMN.to_string(), KalamCellValue::text(seq.to_string()));
        row
    }

    #[test]
    fn extract_max_seq_empty() {
        assert_eq!(extract_max_seq(&[]), None);
    }

    #[test]
    fn extract_max_seq_single() {
        let rows = vec![row_with_seq(42)];
        assert_eq!(extract_max_seq(&rows), Some(SeqId::from_i64(42)));
    }

    #[test]
    fn extract_max_seq_multiple() {
        let rows = vec![row_with_seq(10), row_with_seq(50), row_with_seq(30)];
        assert_eq!(extract_max_seq(&rows), Some(SeqId::from_i64(50)));
    }

    #[test]
    fn extract_max_seq_with_numeric() {
        // _seq can also arrive as a JSON number for small values
        let mut row = HashMap::new();
        row.insert(SEQ_COLUMN.to_string(), KalamCellValue::int(99));
        assert_eq!(extract_max_seq(&[row]), Some(SeqId::from_i64(99)));
    }

    #[test]
    fn extract_max_seq_no_seq_column() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), KalamCellValue::text("abc"));
        assert_eq!(extract_max_seq(&[row]), None);
    }

    #[test]
    fn row_seq_reads_text_and_numeric_values() {
        let mut text_row = HashMap::new();
        text_row.insert(SEQ_COLUMN.to_string(), KalamCellValue::text("55"));
        assert_eq!(row_seq(&text_row), Some(SeqId::from_i64(55)));

        let mut int_row = HashMap::new();
        int_row.insert(SEQ_COLUMN.to_string(), KalamCellValue::int(77));
        assert_eq!(row_seq(&int_row), Some(SeqId::from_i64(77)));
    }

    #[test]
    fn retain_rows_after_filters_stale_rows() {
        let mut rows = vec![row_with_seq(10), row_with_seq(20), row_with_seq(30)];

        let removed = retain_rows_after(&mut rows, SeqId::from_i64(20));

        assert_eq!(removed, 2);
        assert_eq!(extract_max_seq(&rows), Some(SeqId::from_i64(30)));
    }

    #[test]
    fn retain_rows_after_keeps_rows_without_seq() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), KalamCellValue::text("abc"));
        let mut rows = vec![row];

        let removed = retain_rows_after(&mut rows, SeqId::from_i64(20));

        assert_eq!(removed, 0);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn advance_seq_from_none() {
        let mut current: Option<SeqId> = None;
        assert!(advance_seq(&mut current, SeqId::from_i64(10)));
        assert_eq!(current, Some(SeqId::from_i64(10)));
    }

    #[test]
    fn advance_seq_greater() {
        let mut current = Some(SeqId::from_i64(10));
        assert!(advance_seq(&mut current, SeqId::from_i64(20)));
        assert_eq!(current, Some(SeqId::from_i64(20)));
    }

    #[test]
    fn advance_seq_not_greater() {
        let mut current = Some(SeqId::from_i64(20));
        assert!(!advance_seq(&mut current, SeqId::from_i64(10)));
        assert_eq!(current, Some(SeqId::from_i64(20)));
    }

    #[test]
    fn advance_seq_equal() {
        let mut current = Some(SeqId::from_i64(10));
        assert!(!advance_seq(&mut current, SeqId::from_i64(10)));
        assert_eq!(current, Some(SeqId::from_i64(10)));
    }

    #[test]
    fn track_rows_advances() {
        let mut current: Option<SeqId> = None;
        let rows = vec![row_with_seq(5), row_with_seq(15)];
        assert!(track_rows(&mut current, &rows));
        assert_eq!(current, Some(SeqId::from_i64(15)));
    }

    #[test]
    fn track_rows_no_advance() {
        let mut current = Some(SeqId::from_i64(100));
        let rows = vec![row_with_seq(50)];
        assert!(!track_rows(&mut current, &rows));
        assert_eq!(current, Some(SeqId::from_i64(100)));
    }

}
