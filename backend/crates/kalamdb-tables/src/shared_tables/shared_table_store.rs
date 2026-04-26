//! Shared table store implementation using EntityStore pattern
//!
//! This module provides an EntityStore-based implementation for cross-user shared tables.
//! Unlike system tables, shared tables use EntityStore directly (not SystemTableStore)
//! because they are user data, not system metadata.
//!
//! **MVCC Architecture (Phase 12, User Story 5)**:
//! - SharedTableRowId: SeqId directly (from kalamdb_commons)
//! - SharedTableRow: Minimal structure with _seq, _deleted, fields (JSON)
//! - Storage key format: {_seq} (big-endian bytes)
//! - NO access_level field (cached in schema definition, not per-row)
//!
//! **PK Index Support**:
//! - SharedTableIndexedStore: IndexedEntityStore with PK index for efficient lookups
//! - Enables O(1) row lookup by PK value instead of full scan
//! - Used by UPDATE/DELETE to find target rows

use std::sync::Arc;

use kalamdb_commons::{
    ids::{SeqId, SharedTableRowId},
    models::rows::Row,
    storage::Partition,
    KSerializable, TableId,
};
use kalamdb_store::{EntityStore, IndexedEntityStore, StorageBackend};
use serde::{Deserialize, Serialize};

use super::pk_index::create_shared_table_pk_index;
use crate::common::{ensure_partition, new_indexed_store_with_pk, partition_name};

/// Shared table row data
///
/// **MVCC Architecture (Phase 12, User Story 5)**:
/// - Removed: row_id (redundant with _seq), _updated (timestamp embedded in _seq Snowflake ID),
///   access_level (moved to schema definition)
/// - Kept: _seq (version identifier with embedded timestamp), `_commit_seq` (commit-order
///   visibility), _deleted (tombstone), fields (all shared table columns including PK)
///
/// **Note on System Column Naming**:
/// The underscore prefix (`_seq`, `_deleted`) follows SQL convention for system-managed columns.
/// These names match the SQL column names exactly for consistency across the codebase.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedTableRow {
    /// Monotonically increasing sequence ID (Snowflake ID with embedded timestamp)
    /// Maps to SQL column `_seq`
    pub _seq: SeqId,
    /// Commit-order visibility marker assigned by the durable apply path.
    /// Maps to SQL column `_commit_seq`
    #[serde(default)]
    pub _commit_seq: u64,
    /// Soft delete tombstone marker
    /// Maps to SQL column `_deleted`
    pub _deleted: bool,
    /// All user-defined columns including PK (serialized as JSON map)
    pub fields: Row,
}

impl KSerializable for SharedTableRow {
    fn encode(&self) -> Result<Vec<u8>, kalamdb_commons::storage::StorageError> {
        kalamdb_commons::serialization::row_codec::encode_shared_table_row(
            self._seq,
            self._commit_seq,
            self._deleted,
            &self.fields,
        )
    }

    fn decode(bytes: &[u8]) -> Result<Self, kalamdb_commons::storage::StorageError>
    where
        Self: Sized,
    {
        let (seq, commit_seq, deleted, fields) =
            kalamdb_commons::serialization::row_codec::decode_shared_table_row(bytes)?;
        Ok(Self {
            _seq: seq,
            _commit_seq: commit_seq,
            _deleted: deleted,
            fields,
        })
    }
}

/// Store for shared tables (cross-user data, not system metadata).
///
/// Uses SeqId keys for row versioning. Unlike SystemTableStore, this is a
/// direct EntityStore implementation without admin-only access control.
#[derive(Clone)]
pub struct SharedTableStore {
    backend: Arc<dyn StorageBackend>,
    partition: Partition,
}

impl SharedTableStore {
    /// Create a new shared table store
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    /// * `partition` - Partition name (e.g., "shared_default:products")
    pub fn new(backend: Arc<dyn StorageBackend>, partition: impl Into<Partition>) -> Self {
        Self {
            backend,
            partition: partition.into(),
        }
    }
}

/// Implement EntityStore trait for typed CRUD operations
impl EntityStore<SharedTableRowId, SharedTableRow> for SharedTableStore {
    fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    fn partition(&self) -> Partition {
        self.partition.clone()
    }
}

/// Type alias for shared table store with PK index support.
///
/// This enables efficient O(1) lookups by PK value for UPDATE/DELETE operations.
pub type SharedTableIndexedStore = IndexedEntityStore<SharedTableRowId, SharedTableRow>;

/// Helper function to create a new shared table store
///
/// # Arguments
/// * `backend` - Storage backend (RocksDB or mock)
/// * `namespace_id` - Namespace identifier
/// * `table_name` - Table name
///
/// # Returns
/// A new SharedTableStore instance configured for the shared table
pub fn new_shared_table_store(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
) -> SharedTableStore {
    let name = partition_name(
        kalamdb_commons::constants::ColumnFamilyNames::SHARED_TABLE_PREFIX,
        table_id,
    );
    ensure_partition(&backend, name.clone());

    SharedTableStore::new(backend, name)
}

/// Create a new shared table store with PK index for efficient lookups.
///
/// This store automatically maintains a secondary index on the PK field,
/// enabling O(1) row lookups by PK value (instead of full scan).
///
/// # Arguments
/// * `backend` - Storage backend (RocksDB or mock)
/// * `namespace_id` - Namespace identifier
/// * `table_name` - Table name
/// * `pk_field_name` - Name of the primary key column
///
/// # Returns
/// A new SharedTableIndexedStore instance with PK index
pub fn new_indexed_shared_table_store(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
    pk_field_name: &str,
) -> SharedTableIndexedStore {
    let name = partition_name(
        kalamdb_commons::constants::ColumnFamilyNames::SHARED_TABLE_PREFIX,
        table_id,
    );
    let pk_index = create_shared_table_pk_index(table_id, pk_field_name);
    new_indexed_store_with_pk(Arc::clone(&backend), name, vec![pk_index])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{
        models::{NamespaceId, TableId, TableName},
        StorageKey,
    };

    use super::*;
    use crate::utils::test_backend::RecordingBackend;

    fn create_test_store() -> SharedTableStore {
        let backend: Arc<dyn StorageBackend> = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
        new_shared_table_store(backend, &table_id)
    }

    fn create_test_row(seq: i64, name: &str) -> SharedTableRow {
        let mut values = BTreeMap::new();
        values.insert("name".to_string(), ScalarValue::Utf8(Some(name.to_string())));
        values.insert("id".to_string(), ScalarValue::Int64(Some(seq)));
        SharedTableRow {
            _seq: SeqId::new(seq),
            _commit_seq: 0,
            fields: Row::new(values),
            _deleted: false,
        }
    }

    #[test]
    fn test_shared_table_store_create() {
        let store = create_test_store();
        assert!(store.partition().name().contains("shared_"));
    }

    #[test]
    fn test_shared_table_store_put_get() {
        let store = create_test_store();
        let key = SeqId::new(100);
        let row = create_test_row(100, "Public Data");

        // Put and get
        store.put(&key, &row).unwrap();
        let retrieved = store.get(&key).unwrap().unwrap();
        assert_eq!(retrieved, row);
    }

    #[test]
    fn test_shared_table_store_delete() {
        let store = create_test_store();
        let key = SeqId::new(200);
        let row = create_test_row(200, "test");

        // Put, delete, verify
        store.put(&key, &row).unwrap();
        store.delete(&key).unwrap();
        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn test_shared_table_store_scan_all() {
        let store = create_test_store();

        // Insert multiple rows
        for i in 1..=5 {
            let key = SeqId::new(i as i64 * 100);
            let row = create_test_row(i as i64 * 100, &format!("item_{}", i));
            store.put(&key, &row).unwrap();
        }

        // Scan all
        let all_rows = store.scan_all_typed(None, None, None).unwrap();
        assert_eq!(all_rows.len(), 5);
    }

    #[test]
    fn test_scan_with_raw_prefix_uses_backend_prefix() {
        let backend = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
        let store = new_shared_table_store(backend.clone(), &table_id);

        let prefix_key = SeqId::new(100);
        let prefix = prefix_key.storage_key();

        let _ = store.scan_with_raw_prefix(&prefix, None, 10).unwrap();

        let last = backend.last_scan().expect("missing scan");
        assert_eq!(last.prefix, Some(prefix));
        assert_eq!(last.start_key, None);
    }
}
