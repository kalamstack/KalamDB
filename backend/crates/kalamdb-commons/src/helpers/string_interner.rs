//! String interning for memory optimization
//!
//! This module provides a global string interner that deduplicates strings
//! by storing each unique string only once in memory. Multiple references
//! to the same string share the same Arc<str> allocation.
//!
//! # Memory Impact
//! For 1M rows with 10 system columns each:
//! - Without interning: 1M × 10 × ~15 bytes = ~150 MB
//! - With interning: 10 × ~15 bytes = ~150 bytes + Arc overhead
//! - **10,000× reduction** for repeated column names
//!
//! # Usage
//! ```rust,ignore
//! use kalamdb_commons::string_interner::{intern, SYSTEM_COLUMNS};
//!
//! // Intern arbitrary string
//! let name = intern("user_id");
//!
//! // Use pre-interned system columns (zero allocation)
//! let updated_col = SYSTEM_COLUMNS.updated;
//! ```

use std::sync::Arc;

use dashmap::{mapref::entry::Entry, DashMap};
use once_cell::sync::Lazy;

use crate::constants::SystemColumnNames;

/// Global string interner
static INTERNER: Lazy<DashMap<Arc<str>, ()>> = Lazy::new(DashMap::new);

/// Intern a string, returning a shared reference
///
/// If the string is already interned, returns the existing Arc<str>.
/// Otherwise, inserts it into the global interner and returns a new Arc<str>.
///
/// # Examples
/// ```rust,ignore
/// use kalamdb_commons::string_interner::intern;
///
/// let s1 = intern("user_id");
/// let s2 = intern("user_id");
/// assert!(Arc::ptr_eq(&s1, &s2)); // Same allocation
/// ```
pub fn intern(s: &str) -> Arc<str> {
    // Try to get existing interned string
    if let Some(entry) = INTERNER.get(s) {
        return entry.key().clone();
    }

    // Insert new string or reuse canonical entry (handles concurrent inserts)
    match INTERNER.entry(Arc::<str>::from(s)) {
        Entry::Occupied(existing) => existing.key().clone(),
        Entry::Vacant(vacant) => {
            let key = vacant.key().clone();
            vacant.insert(());
            key
        },
    }
}

/// Pre-interned system column names
///
/// These are commonly used column names that are pre-allocated on first access.
/// Using these constants avoids repeated allocations for system columns.
///
/// # Examples
/// ```rust,ignore
/// use kalamdb_commons::string_interner::SYSTEM_COLUMNS;
/// use std::collections::HashMap;
///
/// let mut row = HashMap::new();
/// row.insert(SYSTEM_COLUMNS.row_id.clone(), "123".to_string());
/// ```
pub struct SystemColumns {
    /// "_seq" column (Snowflake ID with embedded timestamp)
    pub seq: Arc<str>,
    /// "_deleted" column (soft delete flag)
    pub deleted: Arc<str>,
    /// "_commit_seq" column (committed snapshot visibility marker)
    pub commit_seq: Arc<str>,
    /// "user_id" column (user identifier in system tables)
    pub user_id: Arc<str>,
    /// "namespace_id" column (namespace identifier)
    pub namespace_id: Arc<str>,
    /// "table_id" column (table identifier)
    pub table_id: Arc<str>,
    /// "storage_id" column (storage identifier)
    pub storage_id: Arc<str>,
    /// "job_id" column (job identifier)
    pub job_id: Arc<str>,
    /// "live_query_id" column (live query identifier)
    pub live_query_id: Arc<str>,
}

/// Global pre-interned system column names
pub static SYSTEM_COLUMNS: Lazy<SystemColumns> = Lazy::new(|| SystemColumns {
    seq: intern(SystemColumnNames::SEQ),
    deleted: intern(SystemColumnNames::DELETED),
    commit_seq: intern(SystemColumnNames::COMMIT_SEQ),
    user_id: intern("user_id"),
    namespace_id: intern("namespace_id"),
    table_id: intern("table_id"),
    storage_id: intern("storage_id"),
    job_id: intern("job_id"),
    live_query_id: intern("live_query_id"),
});

/// Get interner statistics
#[derive(Debug, Clone)]
pub struct InternerStats {
    /// Total number of unique strings interned
    pub unique_strings: usize,
}

/// Get current interner statistics
pub fn stats() -> InternerStats {
    InternerStats {
        unique_strings: INTERNER.len(),
    }
}

/// Clear the interner (mainly for testing)
///
/// # Warning
/// This will invalidate all existing Arc<str> references' deduplication,
/// but the references themselves remain valid.
#[cfg(test)]
pub fn clear() {
    INTERNER.clear();
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_intern_same_string_returns_same_arc() {
        let s1 = intern("test_column_unique_1");
        let s2 = intern("test_column_unique_1");

        // Should be the exact same allocation
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_intern_different_strings() {
        let s1 = intern("column_a_unique_2");
        let s2 = intern("column_b_unique_2");

        // Should be different allocations
        assert!(!Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_system_columns_are_interned() {
        // Access system columns
        let seq = SYSTEM_COLUMNS.seq.clone();
        let deleted = SYSTEM_COLUMNS.deleted.clone();
        let commit_seq = SYSTEM_COLUMNS.commit_seq.clone();

        // Verify they have correct values
        assert_eq!(seq.as_ref(), SystemColumnNames::SEQ);
        assert_eq!(deleted.as_ref(), SystemColumnNames::DELETED);
        assert_eq!(commit_seq.as_ref(), SystemColumnNames::COMMIT_SEQ);

        // Interning the same string should return the same Arc
        let seq2 = intern(SystemColumnNames::SEQ);
        assert!(Arc::ptr_eq(&seq, &seq2));
        let commit_seq2 = intern(SystemColumnNames::COMMIT_SEQ);
        assert!(Arc::ptr_eq(&commit_seq, &commit_seq2));
    }

    #[test]
    fn test_all_system_columns() {
        let cols = &*SYSTEM_COLUMNS;

        assert_eq!(cols.seq.as_ref(), SystemColumnNames::SEQ);
        assert_eq!(cols.deleted.as_ref(), SystemColumnNames::DELETED);
        assert_eq!(cols.commit_seq.as_ref(), SystemColumnNames::COMMIT_SEQ);
        assert_eq!(cols.user_id.as_ref(), "user_id");
        assert_eq!(cols.namespace_id.as_ref(), "namespace_id");
        assert_eq!(cols.table_id.as_ref(), "table_id");
        assert_eq!(cols.storage_id.as_ref(), "storage_id");
        assert_eq!(cols.job_id.as_ref(), "job_id");
        assert_eq!(cols.live_query_id.as_ref(), "live_query_id");
    }

    #[test]
    fn test_concurrent_interning() {
        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    // Each thread interns the same set of strings
                    let s1 = intern("concurrent_test_unique_3");
                    let s2 = intern(&format!("thread_unique_3_{}", i));
                    (s1, s2)
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Verify all "concurrent_test_unique_3" strings point to same allocation
        for i in 1..results.len() {
            assert!(Arc::ptr_eq(&results[0].0, &results[i].0));
        }

        // Verify thread-specific strings are different
        for i in 0..results.len() {
            for j in i + 1..results.len() {
                assert!(!Arc::ptr_eq(&results[i].1, &results[j].1));
            }
        }
    }

    #[test]
    fn test_memory_deduplication() {
        // Simulate column names repeated across many rows
        let mut columns = Vec::new();
        for _ in 0..1000 {
            columns.push(intern("dedup_user_id_4"));
            columns.push(intern("dedup_namespace_id_4"));
            columns.push(intern("dedup_updated_4"));
        }

        // Verify all references point to same allocations
        for i in (0..columns.len()).step_by(3) {
            if i + 5 < columns.len() {
                assert!(Arc::ptr_eq(&columns[i], &columns[i + 3])); // dedup_user_id_4 matches
                assert!(Arc::ptr_eq(&columns[i + 1], &columns[i + 4])); // dedup_namespace_id_4 matches
                assert!(Arc::ptr_eq(&columns[i + 2], &columns[i + 5])); // dedup_updated_4 matches
            }
        }
    }

    #[test]
    fn test_empty_string() {
        let s1 = intern("");
        let s2 = intern("");

        assert!(Arc::ptr_eq(&s1, &s2));
        assert_eq!(s1.as_ref(), "");
    }

    #[test]
    fn test_unicode_strings() {
        let s1 = intern("用户ID");
        let s2 = intern("用户ID");

        assert!(Arc::ptr_eq(&s1, &s2));
        assert_eq!(s1.as_ref(), "用户ID");
    }
}
