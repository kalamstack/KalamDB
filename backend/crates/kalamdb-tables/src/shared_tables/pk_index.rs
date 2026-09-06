//! Primary Key Index for Shared Tables
//!
//! Thin wrapper over [`PrefixIndex`]. Key format: `(pk_value_encoded, seq)`.

use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    conversions::scalar_value_to_bytes, ids::SharedTableRowId, models::UserId, storage::Partition,
    TableId,
};
use kalamdb_store::{IndexDefinition, PrefixIndex};

use super::SharedTableRow;

/// Index for querying shared table rows by primary key value.
#[derive(Clone)]
pub struct SharedTablePkIndex {
    inner: PrefixIndex<SharedTableRowId, SharedTableRow>,
}

impl SharedTablePkIndex {
    /// Create a new PK index for a shared table.
    pub fn new(table_id: &TableId, pk_field_name: &str) -> Self {
        let partition = format!("shared_{}_pk_idx", table_id);
        Self {
            inner: PrefixIndex::new(partition, vec![pk_field_name.to_string()], false),
        }
    }

    /// Build a prefix for scanning all versions of a PK.
    pub fn build_prefix_for_pk(&self, pk_value: &ScalarValue) -> Vec<u8> {
        let pk_bytes = scalar_value_to_bytes(pk_value);
        self.inner.encode_column_prefix(None, &[pk_bytes])
    }

    /// Build a prefix for a PK string value (for batch existence checks).
    #[inline]
    pub fn build_pk_prefix(&self, pk_value: &str) -> Vec<u8> {
        self.inner.encode_column_prefix(None, &[pk_value.as_bytes().to_vec()])
    }
}

impl IndexDefinition<SharedTableRowId, SharedTableRow> for SharedTablePkIndex {
    fn partition(&self) -> Partition {
        self.inner.partition()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        self.inner.indexed_columns()
    }

    fn extract_key(
        &self,
        primary_key: &SharedTableRowId,
        entity: &SharedTableRow,
    ) -> Option<Vec<u8>> {
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

/// Create a PK index for a shared table.
pub fn create_shared_table_pk_index(
    table_id: &TableId,
    pk_field_name: &str,
) -> std::sync::Arc<dyn IndexDefinition<SharedTableRowId, SharedTableRow>> {
    std::sync::Arc::new(SharedTablePkIndex::new(table_id, pk_field_name))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{ids::SeqId, models::rows::Row};

    use super::*;

    fn create_test_row(seq: i64, id_value: i64) -> (SharedTableRowId, SharedTableRow) {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(id_value)));
        values.insert("name".to_string(), ScalarValue::Utf8(Some("Test".to_string())));

        let key = SeqId::new(seq);
        let row = SharedTableRow {
            _seq:        SeqId::new(seq),
            _commit_seq: 0,
            _deleted:    false,
            fields:      Row::new(values),
        };
        (key, row)
    }

    #[test]
    fn test_pk_index_extract_key() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "products");
        let index = SharedTablePkIndex::new(&table_id, "id");
        let (key, row) = create_test_row(100, 42);

        let index_key = index.extract_key(&key, &row);
        assert!(index_key.is_some());

        let index_key = index_key.unwrap();
        let prefix = index.build_prefix_for_pk(&ScalarValue::Int64(Some(42)));
        assert!(index_key.starts_with(&prefix));
    }

    #[test]
    fn test_pk_index_same_pk_different_versions() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "products");
        let index = SharedTablePkIndex::new(&table_id, "id");

        let (key1, row1) = create_test_row(100, 42);
        let (key2, row2) = create_test_row(200, 42);

        let index_key1 = index.extract_key(&key1, &row1).unwrap();
        let index_key2 = index.extract_key(&key2, &row2).unwrap();

        let prefix = index.build_prefix_for_pk(&ScalarValue::Int64(Some(42)));
        assert!(index_key1.starts_with(&prefix));
        assert!(index_key2.starts_with(&prefix));
        assert_ne!(index_key1, index_key2);
    }

    #[test]
    fn test_pk_index_different_pk_values() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "products");
        let index = SharedTablePkIndex::new(&table_id, "id");

        let (key1, row1) = create_test_row(100, 42);
        let (key2, row2) = create_test_row(100, 99);

        let index_key1 = index.extract_key(&key1, &row1).unwrap();
        let index_key2 = index.extract_key(&key2, &row2).unwrap();

        assert_ne!(index_key1, index_key2);
    }

    #[test]
    fn test_build_prefix_for_pk() {
        let table_id = kalamdb_commons::TableId::from_strings("default", "products");
        let index = SharedTablePkIndex::new(&table_id, "id");
        let pk_value = ScalarValue::Int64(Some(42));

        let prefix = index.build_prefix_for_pk(&pk_value);
        let (key, row) = create_test_row(100, 42);
        let index_key = index.extract_key(&key, &row).unwrap();
        assert!(index_key.starts_with(&prefix));
    }

    #[test]
    fn test_partition_name() {
        let table_id = kalamdb_commons::TableId::from_strings("my_namespace", "my_table");
        let index = SharedTablePkIndex::new(&table_id, "id");
        assert_eq!(index.partition().name(), "shared_my_namespace:my_table_pk_idx");
    }
}
