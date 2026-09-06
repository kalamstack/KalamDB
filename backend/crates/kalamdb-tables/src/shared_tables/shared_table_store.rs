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

pub use kalamdb_commons::models::rows::SharedTableRow;
use kalamdb_commons::{ids::SharedTableRowId, storage::Partition, TableId};
use kalamdb_serialization::StorageSchema;
use kalamdb_store::{EntityStore, IndexedEntityStore, StorageBackend};

use crate::{
    common::{ensure_partition, new_indexed_store_with_pk, partition_name, table_prefix_indexes},
    row_codec::SharedRowCodec,
};

/// Store for shared tables (cross-user data, not system metadata).
///
/// Uses SeqId keys for row versioning. Unlike SystemTableStore, this is a
/// direct EntityStore implementation without admin-only access control.
#[derive(Clone)]
pub struct SharedTableStore {
    backend:   Arc<dyn StorageBackend>,
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
    schema: Arc<StorageSchema>,
    scalar_indexes: &[kalamdb_commons::models::schemas::ScalarIndexDefinition],
    columns: &[kalamdb_commons::models::schemas::ColumnDefinition],
) -> SharedTableIndexedStore {
    let name = partition_name(
        kalamdb_commons::constants::ColumnFamilyNames::SHARED_TABLE_PREFIX,
        table_id,
    );
    let indexes = table_prefix_indexes(table_id, pk_field_name, scalar_indexes, columns, false);
    new_indexed_store_with_pk(
        Arc::clone(&backend),
        name,
        indexes,
        Arc::new(SharedRowCodec::new(schema)),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{
        ids::SeqId,
        models::{rows::Row, NamespaceId, TableId, TableName},
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
            _seq:        SeqId::new(seq),
            _commit_seq: 0,
            fields:      Row::new(values),
            _deleted:    false,
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

    #[test]
    fn indexed_shared_store_writes_ordinal_kobj_and_rebuilds_seq() {
        let backend: Arc<dyn StorageBackend> = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
        let schema = crate::row_codec::storage_schema_from_named_fields([
            ("id", kalamdb_serialization::StorageDataType::Int64),
            ("name", kalamdb_serialization::StorageDataType::Utf8),
        ]);
        let store =
            new_indexed_shared_table_store(Arc::clone(&backend), &table_id, "id", schema, &[], &[]);
        let key = SeqId::new(100);
        let row = create_test_row(100, "Public Data");
        store.insert(&key, &row).unwrap();

        let raw = backend
            .get(&store.partition(), &key.storage_key())
            .unwrap()
            .expect("stored row value");
        assert_eq!(&raw[0..4], b"KOBJ");

        let retrieved = store.get(&key).unwrap().unwrap();
        assert_eq!(retrieved._seq, key);
        assert_eq!(retrieved.fields.values.get("name"), row.fields.values.get("name"));
    }

    #[test]
    fn indexed_shared_store_prefix_scan_returns_both_seqs_for_same_conversation() {
        use kalamdb_commons::models::{
            datatypes::KalamDataType,
            schemas::{ColumnDefinition, ScalarIndexDefinition},
            ColumnId,
        };

        let backend = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("chat"), TableName::new("messages"));
        let schema = crate::row_codec::storage_schema_from_named_fields([
            ("id", kalamdb_serialization::StorageDataType::Int64),
            ("conversation_id", kalamdb_serialization::StorageDataType::Utf8),
            ("name", kalamdb_serialization::StorageDataType::Utf8),
        ]);
        let columns = [
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "conversation_id", 2, KalamDataType::Text),
            ColumnDefinition::simple(3, "name", 3, KalamDataType::Text),
        ];
        let indexes = [ScalarIndexDefinition::new(
            "messages_conversation_id",
            vec![ColumnId::new(2)],
            false,
        )];
        let store = new_indexed_shared_table_store(
            backend.clone(),
            &table_id,
            "id",
            schema,
            &indexes,
            &columns,
        );
        assert_eq!(store.indexes().len(), 2);

        let insert_row = |seq: i64, conversation: &str, id: i64| {
            let mut values = BTreeMap::new();
            values.insert("id".to_string(), ScalarValue::Int64(Some(id)));
            values.insert(
                "conversation_id".to_string(),
                ScalarValue::Utf8(Some(conversation.to_string())),
            );
            values.insert("name".to_string(), ScalarValue::Utf8(Some("msg".to_string())));
            store
                .insert(
                    &SeqId::new(seq),
                    &SharedTableRow {
                        _seq:        SeqId::new(seq),
                        _commit_seq: 0,
                        _deleted:    false,
                        fields:      Row::new(values),
                    },
                )
                .unwrap();
        };
        insert_row(10, "room-a", 1);
        insert_row(20, "room-a", 2);
        insert_row(30, "room-b", 3);

        let filter = datafusion::logical_expr::col("conversation_id")
            .eq(datafusion::logical_expr::lit("room-a"));
        let (idx, prefix) = store
            .find_best_index_for_filter_expr(None, &filter)
            .expect("conversation_id equality uses the scalar index");
        assert_eq!(idx, 1);
        let hits = store.scan_by_index(idx, Some(&prefix), None).unwrap();
        let seqs: Vec<i64> = hits.iter().map(|(k, _)| k.as_i64()).collect();
        assert_eq!(seqs, vec![10, 20]);

        let last = backend.last_scan().expect("index scan");
        assert!(last.partition.contains("idx_messages_conversation_id"));
        assert_eq!(last.prefix.as_deref(), Some(prefix.as_slice()));

        let pk_prefix = store.indexes()[0]
            .filter_to_prefix(
                &datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit(3_i64)),
            )
            .expect("pk index still answers id equality");
        let pk_hits = store.scan_by_index(0, Some(&pk_prefix), None).unwrap();
        assert_eq!(pk_hits.len(), 1);
        assert_eq!(pk_hits[0].0.as_i64(), 30);
    }

    #[test]
    fn indexed_shared_store_prefix_scan_follows_column_rename() {
        use kalamdb_commons::models::{
            datatypes::KalamDataType,
            schemas::{ColumnDefinition, ScalarIndexDefinition},
            ColumnId,
        };

        let backend = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("chat"), TableName::new("messages"));
        let schema = crate::row_codec::storage_schema_from_named_fields([
            ("id", kalamdb_serialization::StorageDataType::Int64),
            ("room_id", kalamdb_serialization::StorageDataType::Utf8),
            ("name", kalamdb_serialization::StorageDataType::Utf8),
        ]);
        let columns = [
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "room_id", 2, KalamDataType::Text),
            ColumnDefinition::simple(3, "name", 3, KalamDataType::Text),
        ];
        let indexes = [ScalarIndexDefinition::new(
            "messages_conversation_id",
            vec![ColumnId::new(2)],
            false,
        )];
        let store = new_indexed_shared_table_store(
            backend.clone(),
            &table_id,
            "id",
            schema,
            &indexes,
            &columns,
        );
        assert_eq!(store.indexes()[1].indexed_columns(), vec!["room_id"]);

        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        values.insert("room_id".to_string(), ScalarValue::Utf8(Some("room-a".to_string())));
        values.insert("name".to_string(), ScalarValue::Utf8(Some("msg".to_string())));
        store
            .insert(
                &SeqId::new(10),
                &SharedTableRow {
                    _seq:        SeqId::new(10),
                    _commit_seq: 0,
                    _deleted:    false,
                    fields:      Row::new(values),
                },
            )
            .unwrap();

        let filter =
            datafusion::logical_expr::col("room_id").eq(datafusion::logical_expr::lit("room-a"));
        let (idx, prefix) = store
            .find_best_index_for_filter_expr(None, &filter)
            .expect("renamed column still matches catalog column_id");
        assert_eq!(idx, 1);
        let hits = store.scan_by_index(idx, Some(&prefix), None).unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn indexed_shared_store_membership_principal_prefix_skips_other_users() {
        use kalamdb_commons::models::{
            datatypes::KalamDataType,
            schemas::{ColumnDefinition, ScalarIndexDefinition},
            ColumnId,
        };

        let backend = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("chat"), TableName::new("members"));
        let schema = crate::row_codec::storage_schema_from_named_fields([
            ("id", kalamdb_serialization::StorageDataType::Utf8),
            ("user_id", kalamdb_serialization::StorageDataType::Utf8),
            ("group_id", kalamdb_serialization::StorageDataType::Utf8),
        ]);
        let columns = [
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "user_id", 2, KalamDataType::Text),
            ColumnDefinition::simple(3, "group_id", 3, KalamDataType::Text),
        ];
        let indexes = [ScalarIndexDefinition::new(
            "members_user_id",
            vec![ColumnId::new(2)],
            false,
        )];
        let store = new_indexed_shared_table_store(
            backend.clone(),
            &table_id,
            "id",
            schema,
            &indexes,
            &columns,
        );

        let insert_row = |seq: i64, id: &str, user_id: &str, group_id: &str| {
            let mut values = BTreeMap::new();
            values.insert("id".to_string(), ScalarValue::Utf8(Some(id.to_string())));
            values.insert("user_id".to_string(), ScalarValue::Utf8(Some(user_id.to_string())));
            values.insert("group_id".to_string(), ScalarValue::Utf8(Some(group_id.to_string())));
            store
                .insert(
                    &SeqId::new(seq),
                    &SharedTableRow {
                        _seq:        SeqId::new(seq),
                        _commit_seq: 0,
                        _deleted:    false,
                        fields:      Row::new(values),
                    },
                )
                .unwrap();
        };

        for i in 0..1000 {
            insert_row(i + 1, &format!("other-{i}"), &format!("user-{i}"), &format!("room-{i}"));
        }
        insert_row(1001, "alice-1", "alice", "room-a");
        insert_row(1002, "alice-2", "alice", "room-b");

        let filter =
            datafusion::logical_expr::col("user_id").eq(datafusion::logical_expr::lit("alice"));
        let (idx, prefix) = store
            .find_best_index_for_filter_expr(None, &filter)
            .expect("user_id equality uses the scalar index");
        assert_eq!(idx, 1);
        let hits = store.scan_by_index(idx, Some(&prefix), None).unwrap();
        assert_eq!(hits.len(), 2);
        let last = backend.last_scan().expect("index scan");
        assert!(last.partition.contains("idx_members_user_id"));
        assert_eq!(last.prefix.as_deref(), Some(prefix.as_slice()));
    }
}
