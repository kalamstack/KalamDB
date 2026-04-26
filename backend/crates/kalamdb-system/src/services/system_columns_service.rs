//! System Column Management Service
//!
//! **Phase 12, User Story 5 - MVCC Architecture**:
//! Centralizes all system column logic (`_seq`, `_deleted`) for KalamDB tables.
//!
//! ## Responsibilities
//! - Generate unique Snowflake-based SeqIds for `_seq` column (version identifier)
//! - Handle `_deleted` soft delete flags
//! - Inject system columns into table schemas
//! - Apply deletion filters to queries
//!
//! ## MVCC Architecture Changes
//! - **Removed**: `_id` (replaced by user-defined PK), `_updated` (replaced by
//!   _seq.timestamp_millis())
//! - **Added**: `_seq: SeqId` - Snowflake ID for version tracking with embedded timestamp
//! - **Kept**: `_deleted: bool` - Soft delete flag
//!
//! ## Architecture
//! - **SnowflakeGenerator**: Generates time-ordered unique IDs (41-bit timestamp + 10-bit worker +
//!   12-bit sequence)
//! - **SeqId Wrapper**: Wraps Snowflake ID with timestamp extraction methods
//! - **Soft Deletes**: Records marked `_deleted=true` are filtered from queries unless explicitly
//!   requested

use std::sync::Arc;

use kalamdb_commons::{
    constants::SystemColumnNames,
    ids::{snowflake::SnowflakeGenerator, SeqId},
    models::schemas::{ColumnDefault, ColumnDefinition, TableDefinition},
};

use crate::error::SystemError;

/// System Columns Service
///
/// **MVCC Architecture**: Manages system columns `_seq` and `_deleted`.
/// Thread-safe via interior mutability in SnowflakeGenerator.
pub struct SystemColumnsService {
    /// Snowflake ID generator for `_seq` column
    snowflake_gen: Arc<SnowflakeGenerator>,

    /// Worker ID from config (for logging/debugging)
    worker_id: u16,
}

impl SystemColumnsService {
    /// Create a new SystemColumnsService
    ///
    /// # Arguments
    /// * `worker_id` - Node identifier from server.toml [cluster.node_id] (or 1 for standalone)
    ///
    /// # Returns
    /// A new SystemColumnsService instance
    pub fn new(worker_id: u16) -> Self {
        let snowflake_gen = Arc::new(SnowflakeGenerator::new(worker_id));

        Self {
            snowflake_gen,
            worker_id,
        }
    }

    /// Generate a unique SeqId for `_seq` column
    ///
    /// **MVCC Architecture**: SeqId wraps Snowflake ID for version tracking.
    ///
    /// # Returns
    /// A SeqId containing a 64-bit Snowflake ID:
    /// - 41 bits: timestamp in milliseconds since 2024-01-01
    /// - 10 bits: worker/node ID
    /// - 12 bits: sequence number
    ///
    /// # Errors
    /// Returns `SystemError::InvalidOperation` if clock moves backwards
    pub fn generate_seq_id(&self) -> Result<SeqId, SystemError> {
        let id = self.snowflake_gen.next_id().map_err(|e| {
            SystemError::InvalidOperation(format!("SeqId generation failed: {}", e))
        })?;
        Ok(SeqId::new(id))
    }

    /// Generate multiple unique SeqIds in a single call
    ///
    /// **Performance Optimization**: Acquires the internal mutex only once,
    /// making batch inserts significantly faster than calling `generate_seq_id()` N times.
    ///
    /// # Arguments
    /// * `count` - Number of SeqIds to generate
    ///
    /// # Returns
    /// Vector of unique, time-ordered SeqIds
    ///
    /// # Errors
    /// Returns `SystemError::InvalidOperation` if clock moves backwards
    pub fn generate_seq_ids(&self, count: usize) -> Result<Vec<SeqId>, SystemError> {
        let ids = self.snowflake_gen.next_ids(count).map_err(|e| {
            SystemError::InvalidOperation(format!("Batch SeqId generation failed: {}", e))
        })?;
        Ok(ids.into_iter().map(SeqId::new).collect())
    }

    /// Add system columns to a table definition
    ///
    /// **MVCC Architecture**: Injects `_seq BIGINT` and `_deleted BOOLEAN`
    /// columns if they don't already exist.
    ///
    /// Note: _seq contains embedded timestamp, so no separate _updated column is needed.
    ///
    /// # Arguments
    /// * `table_def` - Mutable reference to table definition
    ///
    /// # Errors
    /// Returns error if column names conflict with user-defined columns
    pub fn add_system_columns(&self, table_def: &mut TableDefinition) -> Result<(), SystemError> {
        // Check for conflicts
        for col in &table_def.columns {
            if col.column_name == SystemColumnNames::SEQ
                || col.column_name == SystemColumnNames::DELETED
            {
                return Err(SystemError::InvalidOperation(format!(
                    "Column name '{}' is reserved for system columns",
                    col.column_name
                )));
            }
        }

        let next_ordinal = table_def.columns.len() as u32 + 1;

        // Add _seq column (BIGINT, NOT NULL)
        // Note: _seq is NOT a primary key - user must define their own PK
        // _seq contains embedded timestamp (Snowflake ID format)
        let seq_column_id = table_def.next_column_id;
        table_def.columns.push(ColumnDefinition {
            column_id: seq_column_id,
            column_name: SystemColumnNames::SEQ.to_string(),
            ordinal_position: next_ordinal,
            data_type: kalamdb_commons::models::datatypes::KalamDataType::BigInt,
            is_nullable: false,
            is_primary_key: false, // User-defined PK required separately
            is_partition_key: false,
            default_value: ColumnDefault::None,
            column_comment: Some("Version ID (MVCC) with embedded timestamp".to_string()),
        });
        table_def.next_column_id += 1;

        // Add _deleted column (BOOLEAN, NOT NULL, DEFAULT FALSE)
        let deleted_column_id = table_def.next_column_id;
        table_def.columns.push(ColumnDefinition {
            column_id: deleted_column_id,
            column_name: SystemColumnNames::DELETED.to_string(),
            ordinal_position: next_ordinal + 1,
            data_type: kalamdb_commons::models::datatypes::KalamDataType::Boolean,
            is_nullable: false,
            is_primary_key: false,
            is_partition_key: false,
            default_value: ColumnDefault::Literal(serde_json::json!(false)),
            column_comment: Some("Soft delete flag".to_string()),
        });
        table_def.next_column_id += 1;

        Ok(())
    }

    /// Handle INSERT operation - generate system column values
    ///
    /// **MVCC Architecture**: Returns new SeqId and _deleted=false.
    ///
    /// # Returns
    /// Tuple of (`_seq`, `_deleted` = false)
    ///
    /// # Errors
    /// - `InvalidOperation` if SeqId generation fails
    pub fn handle_insert(&self) -> Result<(SeqId, bool), SystemError> {
        // Generate unique SeqId
        let seq = self.generate_seq_id()?;

        // New records are not deleted
        let deleted = false;

        Ok((seq, deleted))
    }

    /// Handle UPDATE operation - generate new version
    ///
    /// **MVCC Architecture**: Appends new version with new SeqId, _deleted=false.
    ///
    /// # Returns
    /// Tuple of (new `_seq`, `_deleted` = false)
    ///
    /// # Details
    /// UPDATE creates a new version with a new SeqId (append-only).
    pub fn handle_update(&self) -> Result<(SeqId, bool), SystemError> {
        let new_seq = self.generate_seq_id()?;

        // UPDATE preserves _deleted=false (use DELETE to mark deleted)
        let deleted = false;

        Ok((new_seq, deleted))
    }

    /// Handle DELETE operation - set `_deleted = true` with new version
    ///
    /// **MVCC Architecture**: Appends tombstone version with new SeqId, _deleted=true.
    ///
    /// # Returns
    /// Tuple of (new `_seq`, `_deleted` = true)
    ///
    /// # Details
    /// Soft delete: record remains in storage with `_deleted=true`.
    /// Queries filter deleted records unless `include_deleted=true`.
    pub fn handle_delete(&self) -> Result<(SeqId, bool), SystemError> {
        let new_seq = self.generate_seq_id()?;

        // Soft delete
        let deleted = true;

        Ok((new_seq, deleted))
    }

    /// Apply deletion filter to query
    ///
    /// Injects `WHERE _deleted = false` predicate into query AST unless explicitly disabled.
    ///
    /// # Arguments
    /// * `include_deleted` - If true, don't filter deleted records
    ///
    /// # Returns
    /// SQL predicate string to inject, or None if include_deleted=true
    pub fn apply_deletion_filter(&self, include_deleted: bool) -> Option<String> {
        if include_deleted {
            None
        } else {
            Some(format!("{} = false", SystemColumnNames::DELETED))
        }
    }

    /// Get worker ID (for debugging/logging)
    pub fn worker_id(&self) -> u16 {
        self.worker_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_seq_id() {
        let svc = SystemColumnsService::new(1);
        let seq1 = svc.generate_seq_id().unwrap();
        let seq2 = svc.generate_seq_id().unwrap();

        assert!(seq1.as_i64() > 0);
        assert!(seq2 > seq1, "SeqIds should be strictly increasing");
    }

    #[test]
    fn test_add_system_columns() {
        use kalamdb_commons::{
            models::schemas::{TableOptions, TableType},
            NamespaceId, TableName,
        };

        let svc = SystemColumnsService::new(1);
        let mut table_def = TableDefinition::new(
            NamespaceId::new("test"),
            TableName::new("table"),
            TableType::User,
            vec![],
            TableOptions::user(),
            None,
        )
        .unwrap();

        svc.add_system_columns(&mut table_def).unwrap();

        assert_eq!(table_def.columns.len(), 2); // _seq and _deleted
        assert_eq!(table_def.columns[0].column_name, "_seq");
        assert_eq!(table_def.columns[1].column_name, "_deleted");
    }

    #[test]
    fn test_handle_insert_success() {
        let svc = SystemColumnsService::new(1);
        let (seq, deleted) = svc.handle_insert().unwrap();

        assert!(seq.as_i64() > 0);
        assert!(!deleted);
    }

    #[test]
    fn test_handle_update_new_seq() {
        let svc = SystemColumnsService::new(1);

        let (new_seq, deleted) = svc.handle_update().unwrap();

        assert!(new_seq.as_i64() > 0);
        assert!(!deleted);
    }

    #[test]
    fn test_handle_delete() {
        let svc = SystemColumnsService::new(1);

        let (new_seq, deleted) = svc.handle_delete().unwrap();

        assert!(new_seq.as_i64() > 0);
        assert!(deleted);
    }

    #[test]
    fn test_apply_deletion_filter() {
        let svc = SystemColumnsService::new(1);

        let filter = svc.apply_deletion_filter(false);
        assert_eq!(filter, Some("_deleted = false".to_string()));

        let no_filter = svc.apply_deletion_filter(true);
        assert_eq!(no_filter, None);
    }
}
