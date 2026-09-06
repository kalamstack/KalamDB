//! Primary Key Index for User Tables
//!
//! Thin wrapper over [`PrefixIndex`]. Key format: `(user_id, pk_value_encoded, seq)`.

use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    conversions::scalar_value_to_bytes, ids::UserTableRowId, models::rows::UserTableRow,
    storage::Partition, TableId, UserId,
};
use kalamdb_store::{IndexDefinition, PrefixIndex};

/// Index for querying user table rows by primary key value.
#[derive(Clone)]
pub struct UserTablePkIndex {
    inner: PrefixIndex<UserTableRowId, UserTableRow>,
}

impl UserTablePkIndex {
    /// Create a new PK index for a user table.
    pub fn new(table_id: &TableId, pk_field_name: &str) -> Self {
        let partition_name = format!("user_{}_pk_idx", table_id);
        Self {
            inner: PrefixIndex::new(partition_name, vec![pk_field_name.to_string()], true),
        }
    }

    /// Build a prefix for scanning all versions of a PK for a specific user.
    pub fn build_prefix_for_pk(&self, user_id: &UserId, pk_value: &ScalarValue) -> Vec<u8> {
        let pk_bytes = scalar_value_to_bytes(pk_value);
        self.inner.encode_column_prefix(Some(user_id), &[pk_bytes])
    }

    /// Build a prefix for scanning all PKs for a specific user.
    pub fn build_user_prefix(&self, user_id: &UserId) -> Vec<u8> {
        self.inner.encode_user_prefix(user_id)
    }
}

impl IndexDefinition<UserTableRowId, UserTableRow> for UserTablePkIndex {
    fn partition(&self) -> Partition {
        self.inner.partition()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        self.inner.indexed_columns()
    }

    fn extract_key(&self, primary_key: &UserTableRowId, entity: &UserTableRow) -> Option<Vec<u8>> {
        self.inner.extract_key(primary_key, entity)
    }

    fn filter_to_prefix(&self, filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        self.inner.filter_to_prefix(filter)
    }

    fn filter_to_prefix_with_scope(
        &self,
        user_id: Option<&UserId>,
        filter: &datafusion::logical_expr::Expr,
    ) -> Option<Vec<u8>> {
        self.inner.filter_to_prefix_with_scope(user_id, filter)
    }
}

/// Create a PK index for a user table.
pub fn create_user_table_pk_index(
    table_id: &TableId,
    pk_field_name: &str,
) -> std::sync::Arc<dyn IndexDefinition<UserTableRowId, UserTableRow>> {
    std::sync::Arc::new(UserTablePkIndex::new(table_id, pk_field_name))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{
        ids::SeqId,
        models::{rows::Row, UserId},
    };

    use super::*;

    fn create_test_row(
        user_id: &UserId,
        seq: i64,
        id_value: i64,
    ) -> (UserTableRowId, UserTableRow) {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(id_value)));
        values.insert("name".to_string(), ScalarValue::Utf8(Some("Test".to_string())));

        let key = UserTableRowId::new(user_id.clone(), SeqId::new(seq));
        let row = UserTableRow {
            user_id:     user_id.clone(),
            _seq:        SeqId::new(seq),
            _commit_seq: 0,
            _deleted:    false,
            fields:      Row::new(values),
        };
        (key, row)
    }

    #[test]
    fn test_pk_index_extract_key() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "users");
        let index = UserTablePkIndex::new(&table_id, "id");
        let (key, row) = create_test_row(&UserId::new("user1"), 100, 42);

        let index_key = index.extract_key(&key, &row);
        assert!(index_key.is_some());

        let index_key = index_key.unwrap();
        let prefix =
            index.build_prefix_for_pk(&UserId::new("user1"), &ScalarValue::Int64(Some(42)));
        assert!(index_key.starts_with(&prefix));
    }

    #[test]
    fn test_pk_index_same_pk_different_versions() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "users");
        let index = UserTablePkIndex::new(&table_id, "id");

        let (key1, row1) = create_test_row(&UserId::new("user1"), 100, 42);
        let (key2, row2) = create_test_row(&UserId::new("user1"), 200, 42);

        let index_key1 = index.extract_key(&key1, &row1).unwrap();
        let index_key2 = index.extract_key(&key2, &row2).unwrap();

        let prefix =
            index.build_prefix_for_pk(&UserId::new("user1"), &ScalarValue::Int64(Some(42)));
        assert!(index_key1.starts_with(&prefix));
        assert!(index_key2.starts_with(&prefix));
        assert_ne!(index_key1, index_key2);
    }

    #[test]
    fn test_pk_index_same_pk_different_users() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "users");
        let index = UserTablePkIndex::new(&table_id, "id");

        let (key1, row1) = create_test_row(&UserId::new("alice"), 100, 42);
        let (key2, row2) = create_test_row(&UserId::new("bob"), 100, 42);

        let index_key1 = index.extract_key(&key1, &row1).unwrap();
        let index_key2 = index.extract_key(&key2, &row2).unwrap();

        assert_ne!(index_key1, index_key2);
        let user_prefix_1 = index.build_user_prefix(&UserId::new("alice"));
        let user_prefix_2 = index.build_user_prefix(&UserId::new("bob"));
        assert!(index_key1.starts_with(&user_prefix_1));
        assert!(index_key2.starts_with(&user_prefix_2));
    }

    #[test]
    fn test_pk_index_different_pk_values() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "users");
        let index = UserTablePkIndex::new(&table_id, "id");

        let (key1, row1) = create_test_row(&UserId::new("user1"), 100, 42);
        let (key2, row2) = create_test_row(&UserId::new("user1"), 100, 99);

        let index_key1 = index.extract_key(&key1, &row1).unwrap();
        let index_key2 = index.extract_key(&key2, &row2).unwrap();

        let prefix1 =
            index.build_prefix_for_pk(&UserId::new("user1"), &ScalarValue::Int64(Some(42)));
        let prefix2 =
            index.build_prefix_for_pk(&UserId::new("user1"), &ScalarValue::Int64(Some(99)));
        assert!(index_key1.starts_with(&prefix1));
        assert!(index_key2.starts_with(&prefix2));
        assert_ne!(prefix1, prefix2);
    }

    #[test]
    fn test_build_prefix_for_pk() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "users");
        let index = UserTablePkIndex::new(&table_id, "id");
        let pk_value = ScalarValue::Int64(Some(42));

        let prefix = index.build_prefix_for_pk(&UserId::new("user1"), &pk_value);
        let (key, row) = create_test_row(&UserId::new("user1"), 100, 42);
        let index_key = index.extract_key(&key, &row).unwrap();
        assert!(index_key.starts_with(&prefix));
    }

    #[test]
    fn test_partition_name() {
        let table_id = kalamdb_commons::TableId::from_strings("my_namespace", "my_table");
        let index = UserTablePkIndex::new(&table_id, "id");
        assert_eq!(index.partition().name(), "user_my_namespace:my_table_pk_idx");
    }
}
