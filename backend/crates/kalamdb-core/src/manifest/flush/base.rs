//! Base flush trait and common utilities for table flush operations
//!
//! This module provides a common interface and implementation for flushing table data
//! from RocksDB to Parquet files. It eliminates code duplication across different table
//! types (shared, user, stream) by providing:
//!
//! - `TableFlush` trait: Common interface for all flush operations
//! - `FlushJobResult`: Standardized result type with metrics
//! - `FlushExecutor`: Template method pattern for job tracking and error handling
//!
//! ## Architecture
//!
//! ```text
//! TableFlush trait (defines interface)
//!      ↓
//! FlushExecutor (template method - common workflow)
//!      ↓
//! Implementations (users.rs, shared.rs, streams.rs)
//! ```

use serde::{Deserialize, Serialize};

use crate::error::KalamDbError;

/// Metadata for user table flush operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct UserTableFlushMetadata {
    /// Number of unique users whose data was flushed
    pub users_count: usize,

    /// Error messages for users that failed to flush (partial failures)
    pub errors: Vec<String>,
}

/// Metadata for shared table flush operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct SharedTableFlushMetadata {
    /// Additional context (reserved for future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

/// Flush metadata for different table types
///
/// Type-safe alternative to JsonValue for flush operation metadata.
/// Each table type has its own strongly-typed metadata structure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "table_type", rename_all = "snake_case")]
pub enum FlushMetadata {
    /// User table flush metadata (partitioned by user_id)
    UserTable(UserTableFlushMetadata),

    /// Shared table flush metadata (single Parquet file)
    SharedTable(SharedTableFlushMetadata),
}

impl FlushMetadata {
    /// Create metadata for user table flush
    pub fn user_table(users_count: usize, errors: Vec<String>) -> Self {
        FlushMetadata::UserTable(UserTableFlushMetadata {
            users_count,
            errors,
        })
    }

    /// Create metadata for shared table flush
    pub fn shared_table() -> Self {
        FlushMetadata::SharedTable(SharedTableFlushMetadata::default())
    }

    /// Get users_count if this is UserTable metadata
    pub fn users_count(&self) -> Option<usize> {
        match self {
            FlushMetadata::UserTable(meta) => Some(meta.users_count),
            _ => None,
        }
    }

    /// Get errors if this is UserTable metadata
    pub fn errors(&self) -> Option<&[String]> {
        match self {
            FlushMetadata::UserTable(meta) => Some(&meta.errors),
            _ => None,
        }
    }
}

/// Result of a flush job execution
///
/// Contains metrics and output files (no Job record - that's JobsManager's responsibility)
#[derive(Debug, Clone)]
pub struct FlushJobResult {
    /// Total rows flushed from RocksDB to Parquet
    pub rows_flushed: usize,

    /// Parquet files written (relative paths)
    pub parquet_files: Vec<String>,

    /// Type-safe metadata specific to table type
    pub metadata: FlushMetadata,
}

/// Statistics from version resolution during flush
#[derive(Debug, Clone, Default)]
pub struct FlushDedupStats {
    /// Total rows scanned from hot storage (before dedup)
    pub rows_before_dedup: usize,
    /// Unique rows after version resolution
    pub rows_after_dedup: usize,
    /// Number of soft-deleted (tombstone) rows encountered
    pub deleted_count: usize,
    /// Number of tombstones filtered out in final output
    pub tombstones_filtered: usize,
}

impl FlushDedupStats {
    /// Calculate deduplication ratio as percentage
    pub fn dedup_ratio(&self) -> f64 {
        if self.rows_before_dedup > 0 {
            (self.rows_before_dedup - self.rows_after_dedup) as f64 / self.rows_before_dedup as f64
                * 100.0
        } else {
            0.0
        }
    }

    /// Log summary statistics
    /// `table_ref` should be in format "namespace:table" (from TableId Display)
    pub fn log_summary(&self, table_ref: &str) {
        log::debug!(
            "📊 [FLUSH DEDUP] Scanned {} total rows from hot storage (table={})",
            self.rows_before_dedup,
            table_ref
        );
        log::debug!(
            "📊 [FLUSH DEDUP] Version resolution complete: {} rows → {} unique (dedup: {:.1}%, \
             deleted: {})",
            self.rows_before_dedup,
            self.rows_after_dedup,
            self.dedup_ratio(),
            self.deleted_count
        );
        log::debug!(
            "📊 [FLUSH DEDUP] Final: {} rows to flush ({} tombstones filtered)",
            self.rows_after_dedup - self.tombstones_filtered,
            self.tombstones_filtered
        );
    }
}

/// Base trait for table flush operations
///
/// Implement this trait for each table type (shared, user, stream).
/// Called by FlushExecutor after JobsManager creates the Job record.
///
/// ## Snapshot Consistency
///
/// All flush operations use RocksDB snapshots via `scan_iter()`:
/// - ✅ ACID guarantees: Flush sees consistent point-in-time view
/// - ✅ No locking: Writes continue during flush
/// - ✅ Isolation: Concurrent flushes don't interfere
pub trait TableFlush: Send + Sync {
    /// Execute the flush job
    ///
    /// Implement table-specific logic:
    /// - Scan rows from RocksDB (use `scan_iter()` for snapshot consistency)
    /// - Convert rows to Arrow RecordBatch
    /// - Write Parquet file(s) (use `ParquetWriter`)
    ///
    /// # Returns
    ///
    /// `FlushJobResult` with metrics (rows flushed, files created, metadata)
    ///
    /// # Errors
    ///
    /// Returns `KalamDbError` if flush fails (I/O error, invalid data, etc.)
    fn execute(&self) -> Result<FlushJobResult, KalamDbError>;

    /// Get table identifier for logging
    ///
    /// Should return `namespace.table_name` format
    fn table_identifier(&self) -> String;
}

/// Common configuration for flush jobs
///
/// Both SharedTableFlushJob and UserTableFlushJob share these constants.
pub mod config {
    /// Number of rows to process per batch during scan
    pub const BATCH_SIZE: usize = 10000;
}

/// Common helper functions for flush operations
pub mod helpers {
    use datafusion::{
        arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
        scalar::ScalarValue,
    };
    use kalamdb_commons::{
        constants::SystemColumnNames, models::rows::Row, next_storage_key_bytes,
    };

    use crate::{
        error::KalamDbError, error_extensions::KalamDbResultExt,
        providers::arrow_json_conversion::json_rows_to_arrow_batch,
    };

    /// Extract primary key field name from Arrow schema
    ///
    /// Returns the first non-system column (doesn't start with '_') or "id" as fallback.
    pub fn extract_pk_field_name(schema: &SchemaRef) -> String {
        schema
            .fields()
            .iter()
            .find(|f| !f.name().starts_with('_'))
            .map(|f| f.name().clone())
            .unwrap_or_else(|| "id".to_string())
    }

    /// Update cursor for next batch (append null byte to skip current key)
    pub fn advance_cursor(last_key: &[u8]) -> Vec<u8> {
        next_storage_key_bytes(last_key)
    }

    /// Calculate deduplication ratio
    pub fn calculate_dedup_ratio(before: usize, after: usize) -> f64 {
        if before > 0 {
            (before - after) as f64 / before as f64 * 100.0
        } else {
            0.0
        }
    }

    /// Convert rows with _seq and _deleted system columns to Arrow RecordBatch
    ///
    /// This is the common pattern used by both user and shared table flush.
    /// Adds _seq and _deleted columns to each row before conversion.
    pub fn rows_to_arrow_batch(
        schema: &SchemaRef,
        rows: &[(Vec<u8>, Row)],
    ) -> Result<RecordBatch, KalamDbError> {
        // Avoid cloning: collect references, then build columnar arrays directly.
        // The JSON conversion function requires owned Rows (it consumes the BTreeMap),
        // so we must clone — but we skip cloning the key bytes.
        let arrow_rows: Vec<Row> = rows.iter().map(|(_, row)| row.clone()).collect();
        json_rows_to_arrow_batch(schema, arrow_rows)
            .into_kalamdb_error("Failed to build RecordBatch")
    }

    /// Convert owned rows (consuming the key bytes) to Arrow RecordBatch.
    ///
    /// More efficient than `rows_to_arrow_batch` when you can pass ownership,
    /// avoiding one Row clone per row.
    pub fn rows_into_arrow_batch(
        schema: &SchemaRef,
        rows: Vec<(Vec<u8>, Row)>,
    ) -> Result<RecordBatch, KalamDbError> {
        let arrow_rows: Vec<Row> = rows.into_iter().map(|(_, row)| row).collect();
        json_rows_to_arrow_batch(schema, arrow_rows)
            .into_kalamdb_error("Failed to build RecordBatch")
    }

    /// Get schema version from cached table data
    pub fn get_schema_version(
        unified_cache: &crate::schema_registry::SchemaRegistry,
        table_id: &kalamdb_commons::models::TableId,
    ) -> u32 {
        unified_cache
            .get(table_id)
            .map(|cached| cached.table.schema_version)
            .unwrap_or(1)
    }

    /// Extract PK value from row fields with _seq fallback
    ///
    /// Returns the primary key value as string, or "_seq:<value>" if PK is null/missing.
    pub fn extract_pk_value(fields: &Row, pk_field: &str, seq: i64) -> String {
        match fields.get(pk_field) {
            Some(v) if !v.is_null() => v.to_string(),
            _ => format!("_seq:{}", seq),
        }
    }

    /// Add system columns (_seq, _deleted) to a Row
    pub fn add_system_columns(mut row: Row, seq: i64, deleted: bool) -> Row {
        row.values
            .insert(SystemColumnNames::SEQ.to_string(), ScalarValue::Int64(Some(seq)));
        row.values
            .insert(SystemColumnNames::DELETED.to_string(), ScalarValue::Boolean(Some(deleted)));
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockFlushJob {
        should_fail: bool,
        rows_count: usize,
    }

    impl TableFlush for MockFlushJob {
        fn execute(&self) -> Result<FlushJobResult, KalamDbError> {
            if self.should_fail {
                return Err(KalamDbError::Other("Mock failure".to_string()));
            }

            Ok(FlushJobResult {
                rows_flushed: self.rows_count,
                parquet_files: vec!["batch-123.parquet".to_string()],
                metadata: FlushMetadata::shared_table(),
            })
        }

        fn table_identifier(&self) -> String {
            "test_namespace.test_table".to_string()
        }
    }

    #[test]
    fn test_flush_execution_success() {
        let job = MockFlushJob {
            should_fail: false,
            rows_count: 100,
        };

        let result = job.execute();
        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.rows_flushed, 100);
        assert_eq!(result.parquet_files.len(), 1);
    }

    #[test]
    fn test_flush_execution_failure() {
        let job = MockFlushJob {
            should_fail: true,
            rows_count: 0,
        };

        let result = job.execute();
        assert!(result.is_err());
    }
}
