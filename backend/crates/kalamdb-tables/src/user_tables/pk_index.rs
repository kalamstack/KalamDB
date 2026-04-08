//! Primary Key Index for User Tables
//!
//! This module provides a secondary index on the primary key field of user tables,
//! enabling efficient lookup of rows by their PK value without scanning all rows.
//!
//! ## Index Key Format
//!
//! Storekey tuple encoding: `(user_id, pk_value_encoded, seq)`
//!
//! - `user_id`: user ID string
//! - `pk_value_encoded`: PK value encoded as bytes
//! - `seq`: sequence ID (i64) for MVCC ordering
//!
//! ## Prefix Scanning
//!
//! To find all versions of a row with a given PK:
//! 1. Build prefix: `(user_id, pk_value_encoded)`
//! 2. Scan all keys with that prefix
//! 3. Results are ordered by seq (storekey preserves numeric ordering)

use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::scalar_value_to_bytes;
use kalamdb_commons::ids::UserTableRowId;
use kalamdb_commons::models::rows::UserTableRow;
use kalamdb_commons::storage::Partition;
use kalamdb_commons::storage_key::{encode_key, encode_prefix};
use kalamdb_store::IndexDefinition;

/// Index for querying user table rows by primary key value.
///
/// Key format (storekey tuple): `(user_id, pk_value_encoded, seq)`
///
/// This index allows efficient lookups by PK value within a user's scope,
/// returning all MVCC versions of rows with matching PK.
/// The user_id prefix ensures the same PK value can exist for different users.
pub struct UserTablePkIndex {
    /// Partition for the index
    partition: Partition,
    /// Name of the primary key field (e.g., "id", "user_id", etc.)
    pk_field_name: String,
}

impl UserTablePkIndex {
    /// Create a new PK index for a user table.
    ///
    /// # Arguments
    /// * `table_id` - Table identifier (namespace + table name)
    /// * `pk_field_name` - Name of the primary key column
    pub fn new(table_id: &kalamdb_commons::TableId, pk_field_name: &str) -> Self {
        let partition_name = format!("user_{}_pk_idx", table_id); // TableId Display: "namespace:table"
        Self {
            partition: Partition::new(partition_name),
            pk_field_name: pk_field_name.to_string(),
        }
    }

    /// Build a prefix for scanning all versions of a PK for a specific user.
    pub fn build_prefix_for_pk(
        &self,
        user_id: &kalamdb_commons::UserId,
        pk_value: &ScalarValue,
    ) -> Vec<u8> {
        let pk_bytes = scalar_value_to_bytes(pk_value);
        encode_prefix(&(user_id.as_str(), pk_bytes))
    }

    /// Build a prefix for scanning all PKs for a specific user.
    ///
    /// This is useful for batch PK validation where we want to scan all
    /// PK index entries for a user in a single pass.
    pub fn build_user_prefix(&self, user_id: &kalamdb_commons::UserId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(),))
    }
}

impl IndexDefinition<UserTableRowId, UserTableRow> for UserTablePkIndex {
    fn partition(&self) -> Partition {
        self.partition.clone()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec![&self.pk_field_name]
    }

    fn extract_key(&self, primary_key: &UserTableRowId, entity: &UserTableRow) -> Option<Vec<u8>> {
        // Get the PK field value from the row
        let pk_value = entity.fields.get(&self.pk_field_name)?;

        let pk_bytes = scalar_value_to_bytes(pk_value);
        Some(encode_key(&(primary_key.user_id.as_str(), pk_bytes, primary_key.seq.as_i64())))
    }

    fn filter_to_prefix(&self, filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        use kalamdb_store::extract_i64_equality;
        use kalamdb_store::extract_string_equality;

        // Try to extract equality filter on PK column
        // Note: User-scoped indexes require user_id to build a valid storekey prefix.
        // Since user_id is not available here, we intentionally disable prefix
        // generation for planner-driven scans.
        if let Some((col, val)) = extract_string_equality(filter) {
            if col == self.pk_field_name {
                let _pk_value = ScalarValue::Utf8(Some(val.to_string()));
                return None;
            }
        }

        if let Some((col, val)) = extract_i64_equality(filter) {
            if col == self.pk_field_name {
                let _pk_value = ScalarValue::Int64(Some(val));
                return None;
            }
        }

        None
    }
}

/// Create a PK index for a user table.
///
/// # Arguments
/// * `table_id` - Table identifier (namespace + table name)
/// * `pk_field_name` - Name of the primary key column
pub fn create_user_table_pk_index(
    table_id: &kalamdb_commons::TableId,
    pk_field_name: &str,
) -> std::sync::Arc<dyn IndexDefinition<UserTableRowId, UserTableRow>> {
    std::sync::Arc::new(UserTablePkIndex::new(table_id, pk_field_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::ids::SeqId;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::UserId;
    use std::collections::BTreeMap;

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
            user_id: user_id.clone(),
            _seq: SeqId::new(seq),
            _commit_seq: 0,
            _deleted: false,
            fields: Row::new(values),
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

        // Two versions of the same row (same PK, different seq)
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

        // Same PK value for different users
        let (key1, row1) = create_test_row(&UserId::new("alice"), 100, 42);
        let (key2, row2) = create_test_row(&UserId::new("bob"), 100, 42);

        let index_key1 = index.extract_key(&key1, &row1).unwrap();
        let index_key2 = index.extract_key(&key2, &row2).unwrap();

        // Different user_id prefix - keys should be completely different
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
