//! User table store implementation using EntityStore pattern
//!
//! This module provides an EntityStore-based implementation for user-scoped tables.
//! Unlike system tables, user tables use EntityStore directly (not SystemTableStore)
//! because they are user data, not system metadata.
//!
//! **MVCC Architecture (Phase 12, User Story 5)**:
//! - UserTableRowId: Composite struct with user_id and _seq fields (from kalamdb_commons)
//! - UserTableRow: Minimal structure with user_id, _seq, _deleted, fields (JSON)
//! - Storage key format: {user_id}:{_seq} (big-endian bytes)
//!
//! **PK Index (Phase 14)**:
//! - IndexedEntityStore variant maintains a secondary index on the PK field
//! - Enables O(1) lookup of row by PK value instead of O(n) scan

use std::sync::Arc;

use kalamdb_commons::{
    ids::UserTableRowId, models::rows::UserTableRow, storage::Partition, TableId,
};
use kalamdb_serialization::StorageSchema;
use kalamdb_store::{entity_store::EntityStore, IndexedEntityStore, StorageBackend};

use crate::{
    common::{ensure_partition, new_indexed_store_with_pk, partition_name, table_prefix_indexes},
    row_codec::UserRowCodec,
};

// KSerializable for UserTableRow is implemented in kalamdb-store
// impl KSerializable for UserTableRow {}

/// Store for user tables (user data, not system metadata).
///
/// Uses composite UserTableRowId keys (user_id:_seq) for user isolation.
/// Unlike SystemTableStore, this is a direct EntityStore implementation
/// without admin-only access control.
#[derive(Clone)]
pub struct UserTableStore {
    backend:   Arc<dyn StorageBackend>,
    partition: Partition,
}

impl UserTableStore {
    /// Create a new user table store
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    /// * `partition` - Partition name (e.g., "user_default:users")
    pub fn new(backend: Arc<dyn StorageBackend>, partition: impl Into<Partition>) -> Self {
        Self {
            backend,
            partition: partition.into(),
        }
    }
}

/// Implement EntityStore trait for typed CRUD operations
impl EntityStore<UserTableRowId, UserTableRow> for UserTableStore {
    #[doc(hidden)]
    fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    fn partition(&self) -> Partition {
        self.partition.clone()
    }
}

/// Helper function to create a new user table store
///
/// # Arguments
/// * `backend` - Storage backend (RocksDB or mock)
/// * `namespace_id` - Namespace identifier
/// * `table_name` - Table name
///
/// # Returns
/// A new UserTableStore instance configured for the user table
pub fn new_user_table_store(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
) -> UserTableStore {
    let name =
        partition_name(kalamdb_commons::constants::ColumnFamilyNames::USER_TABLE_PREFIX, table_id);
    ensure_partition(&backend, name.clone());
    UserTableStore::new(backend, name)
}

/// Type alias for indexed user table store with PK index.
///
/// This store automatically maintains a secondary index on the primary key field,
/// enabling O(1) lookup of rows by PK value.
pub type UserTableIndexedStore = IndexedEntityStore<UserTableRowId, UserTableRow>;

/// Create a new indexed user table store with PK index.
///
/// # Arguments
/// * `backend` - Storage backend (RocksDB or mock)
/// * `namespace_id` - Namespace identifier
/// * `table_name` - Table name
/// * `pk_field_name` - Name of the primary key column
///
/// # Returns
/// A new IndexedEntityStore configured with PK index for efficient lookups
pub fn new_indexed_user_table_store(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
    pk_field_name: &str,
    schema: Arc<StorageSchema>,
    scalar_indexes: &[kalamdb_commons::models::schemas::ScalarIndexDefinition],
    columns: &[kalamdb_commons::models::schemas::ColumnDefinition],
) -> UserTableIndexedStore {
    let name =
        partition_name(kalamdb_commons::constants::ColumnFamilyNames::USER_TABLE_PREFIX, table_id);
    let indexes = table_prefix_indexes(table_id, pk_field_name, scalar_indexes, columns, true);
    new_indexed_store_with_pk(backend, name, indexes, Arc::new(UserRowCodec::new(schema)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{
        ids::SeqId,
        models::{rows::Row, NamespaceId, TableId, TableName},
        StorageKey, UserId,
    };

    use super::*;
    use crate::utils::test_backend::RecordingBackend;

    fn create_test_store() -> UserTableStore {
        let backend: Arc<dyn StorageBackend> = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
        new_user_table_store(backend, &table_id)
    }

    fn create_test_row(user_id: &UserId, seq: i64) -> UserTableRow {
        let mut values = BTreeMap::new();
        values.insert("name".to_string(), ScalarValue::Utf8(Some("Alice".to_string())));
        values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        UserTableRow {
            user_id:     user_id.clone(),
            _seq:        SeqId::new(seq),
            _commit_seq: 0,
            fields:      Row::new(values),
            _deleted:    false,
        }
    }

    #[test]
    fn test_user_table_store_create() {
        let store = create_test_store();
        assert!(store.partition().name().contains("user_"));
    }

    #[test]
    fn test_user_table_store_put_get() {
        let store = create_test_store();
        let key = UserTableRowId::new(UserId::new("user1"), SeqId::new(100));
        let row = create_test_row(&UserId::new("user1"), 100);

        // Put and get
        store.put(&key, &row).unwrap();
        let retrieved = store.get(&key).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), row);
    }

    #[test]
    fn test_user_table_store_delete() {
        let store = create_test_store();
        let key = UserTableRowId::new(UserId::new("user1"), SeqId::new(200));
        let row = create_test_row(&UserId::new("user1"), 200);

        // Put, delete, verify
        store.put(&key, &row).unwrap();
        store.delete(&key).unwrap();
        let retrieved = store.get(&key).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_user_table_store_scan_all() {
        let store = create_test_store();

        // Insert multiple rows for different users
        for user_i in 1..=2 {
            for row_i in 1..=3 {
                let key = UserTableRowId::new(
                    UserId::new(format!("user{}", user_i)),
                    SeqId::new((user_i * 1000 + row_i) as i64),
                );
                let row = create_test_row(
                    &UserId::new(&format!("user{}", user_i)),
                    (user_i * 1000 + row_i) as i64,
                );
                store.put(&key, &row).unwrap();
            }
        }

        // Scan all
        let all_rows = store.scan_all_typed(None, None, None).unwrap();
        assert_eq!(all_rows.len(), 6); // 2 users * 3 rows
    }

    #[test]
    fn test_scan_with_raw_prefix_uses_backend_prefix() {
        let backend = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
        let store = new_user_table_store(backend.clone(), &table_id);

        let user_id = UserId::new("user1");
        let start_key = UserTableRowId::new(user_id.clone(), SeqId::new(10));
        let prefix = UserTableRowId::user_prefix(&user_id);

        let _ = store.scan_with_raw_prefix(&prefix, Some(&start_key.storage_key()), 10).unwrap();

        let last = backend.last_scan().expect("missing scan");
        assert_eq!(last.prefix, Some(prefix));
        assert_eq!(last.start_key, Some(start_key.storage_key()));
    }

    #[test]
    fn test_user_isolation() {
        let store = create_test_store();

        // User1's row
        let key1 = UserTableRowId::new(UserId::new("user1"), SeqId::new(100));
        let row1 = create_test_row(&UserId::new("user1"), 100);
        store.put(&key1, &row1).unwrap();

        // User2's row with same seq
        let key2 = UserTableRowId::new(UserId::new("user2"), SeqId::new(100));
        let row2 = create_test_row(&UserId::new("user2"), 100);
        store.put(&key2, &row2).unwrap();

        // Both exist independently
        assert_eq!(store.get(&key1).unwrap().unwrap().user_id, UserId::new("user1"));
        assert_eq!(store.get(&key2).unwrap().unwrap().user_id, UserId::new("user2"));
    }

    #[test]
    fn indexed_user_store_writes_ordinal_kobj_without_identity() {
        let backend: Arc<dyn StorageBackend> = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
        let schema = crate::row_codec::storage_schema_from_named_fields([
            ("id", kalamdb_serialization::StorageDataType::Int64),
            ("name", kalamdb_serialization::StorageDataType::Utf8),
        ]);
        let store =
            new_indexed_user_table_store(Arc::clone(&backend), &table_id, "id", schema, &[], &[]);
        let user_id = UserId::new("user-alice");
        let key = UserTableRowId::new(user_id.clone(), SeqId::new(100));
        let row = create_test_row(&user_id, 100);
        store.insert(&key, &row).unwrap();

        let raw = backend
            .get(&store.partition(), &key.storage_key())
            .unwrap()
            .expect("stored row value");
        assert_eq!(&raw[0..4], b"KOBJ");
        assert!(
            !raw.windows(b"user-alice".len()).any(|window| window == b"user-alice"),
            "user_id must not be stored in the row value"
        );

        let retrieved = store.get(&key).unwrap().unwrap();
        assert_eq!(retrieved.user_id, user_id);
        assert_eq!(retrieved._seq, SeqId::new(100));
        assert_eq!(retrieved.fields.values.get("name"), row.fields.values.get("name"));
    }

    #[test]
    fn indexed_user_store_prefix_scan_is_user_scoped() {
        use kalamdb_commons::models::{
            datatypes::KalamDataType,
            schemas::{ColumnDefinition, ScalarIndexDefinition},
            ColumnId,
        };

        let backend = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("chat"), TableName::new("messages_ai"));
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
        let store = new_indexed_user_table_store(
            backend.clone(),
            &table_id,
            "id",
            schema,
            &indexes,
            &columns,
        );

        let insert_row = |user: &str, seq: i64, conversation: &str, id: i64| {
            let user_id = UserId::new(user);
            let mut values = BTreeMap::new();
            values.insert("id".to_string(), ScalarValue::Int64(Some(id)));
            values.insert(
                "conversation_id".to_string(),
                ScalarValue::Utf8(Some(conversation.to_string())),
            );
            values.insert("name".to_string(), ScalarValue::Utf8(Some("msg".to_string())));
            store
                .insert(
                    &UserTableRowId::new(user_id.clone(), SeqId::new(seq)),
                    &UserTableRow {
                        user_id,
                        _seq: SeqId::new(seq),
                        _commit_seq: 0,
                        _deleted: false,
                        fields: Row::new(values),
                    },
                )
                .unwrap();
        };
        insert_row("alice", 10, "room-a", 1);
        insert_row("alice", 20, "room-a", 2);
        insert_row("bob", 30, "room-a", 3);

        let filter = datafusion::logical_expr::col("conversation_id")
            .eq(datafusion::logical_expr::lit("room-a"));
        let (idx, prefix) = store
            .find_best_index_for_filter_expr(Some(&UserId::new("alice")), &filter)
            .expect("user-scoped conversation_id prefix");
        assert_eq!(idx, 1);
        let hits = store.scan_by_index(idx, Some(&prefix), None).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|(k, _)| k.user_id.as_str() == "alice"));
    }

    #[test]
    fn create_index_backfill_indexes_existing_rows() {
        use kalamdb_commons::models::{
            datatypes::KalamDataType,
            schemas::{ColumnDefinition, ScalarIndexDefinition},
            ColumnId,
        };

        let backend: Arc<dyn StorageBackend> = Arc::new(RecordingBackend::new());
        let table_id = TableId::new(NamespaceId::new("chat"), TableName::new("messages_bf"));
        let schema = crate::row_codec::storage_schema_from_named_fields([
            ("id", kalamdb_serialization::StorageDataType::Int64),
            ("conversation_id", kalamdb_serialization::StorageDataType::Utf8),
        ]);
        let columns = [
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "conversation_id", 2, KalamDataType::Text),
        ];
        let empty = new_indexed_user_table_store(
            Arc::clone(&backend),
            &table_id,
            "id",
            Arc::clone(&schema),
            &[],
            &columns,
        );
        let user_id = UserId::new("alice");
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        values.insert("conversation_id".to_string(), ScalarValue::Utf8(Some("room-a".to_string())));
        empty
            .insert(
                &UserTableRowId::new(user_id.clone(), SeqId::new(10)),
                &UserTableRow {
                    user_id:     user_id.clone(),
                    _seq:        SeqId::new(10),
                    _commit_seq: 0,
                    _deleted:    false,
                    fields:      Row::new(values),
                },
            )
            .unwrap();

        let indexes = [ScalarIndexDefinition::new(
            "messages_conversation_id",
            vec![ColumnId::new(2)],
            false,
        )];
        let indexed = new_indexed_user_table_store(
            Arc::clone(&backend),
            &table_id,
            "id",
            schema,
            &indexes,
            &columns,
        );
        assert_eq!(indexed.backfill_index(1).unwrap(), 1);
        let hits = indexed.scan_by_index(1, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0.user_id, user_id);
    }
}
