//! Primary Key Index for Shared Tables
//!
//! This module provides a secondary index on the primary key field of shared tables,
//! enabling efficient lookup of rows by their PK value without scanning all rows.
//!
//! ## Index Key Format
//!
//! Storekey tuple encoding: `(pk_value_encoded, seq)`
//!
//! - `pk_value_encoded`: PK value encoded as bytes
//! - `seq`: sequence ID (i64) for MVCC ordering
//!
//! ## Prefix Scanning
//!
//! To find all versions of a row with a given PK:
//! 1. Build prefix: `(pk_value_encoded)`
//! 2. Scan all keys with that prefix
//! 3. Results are ordered by seq (storekey preserves numeric ordering)

use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::scalar_value_to_bytes;
use kalamdb_commons::ids::SharedTableRowId;
use kalamdb_commons::storage::Partition;
use kalamdb_commons::storage_key::{encode_key, encode_prefix};
use kalamdb_store::IndexDefinition;

use super::SharedTableRow;

/// Index for querying shared table rows by primary key value.
///
/// Key format (storekey tuple): `(pk_value_encoded, seq)`
///
/// This index allows efficient lookups by PK value,
/// returning all MVCC versions of rows with matching PK.
#[derive(Clone)]
pub struct SharedTablePkIndex {
    /// Partition for the index
    partition: Partition,
    /// Name of the primary key field (e.g., "id", "product_id", etc.)
    pk_field_name: String,
}

impl SharedTablePkIndex {
    /// Create a new PK index for a shared table.
    ///
    /// # Arguments
    /// * `table_id` - Table identifier (namespace + table name)
    /// * `pk_field_name` - Name of the primary key column
    pub fn new(table_id: &kalamdb_commons::TableId, pk_field_name: &str) -> Self {
        let partition = format!("shared_{}_pk_idx", table_id); // TableId Display: "namespace:table"
        Self {
            partition: Partition::new(partition),
            pk_field_name: pk_field_name.to_string(),
        }
    }

    /// Build a prefix for scanning all versions of a PK.
    ///
    pub fn build_prefix_for_pk(&self, pk_value: &ScalarValue) -> Vec<u8> {
        let pk_bytes = scalar_value_to_bytes(pk_value);
        encode_prefix(&(pk_bytes,))
    }

    /// Build a prefix for a PK string value (for batch existence checks).
    ///
    /// Returns a prefix for the PK value
    ///
    /// This is a simpler version of `build_prefix_for_pk` that takes a string
    /// directly, avoiding ScalarValue parsing overhead for batch operations.
    #[inline]
    pub fn build_pk_prefix(&self, pk_value: &str) -> Vec<u8> {
        encode_prefix(&(pk_value.as_bytes().to_vec(),))
    }
}

impl IndexDefinition<SharedTableRowId, SharedTableRow> for SharedTablePkIndex {
    fn partition(&self) -> Partition {
        self.partition.clone()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec![&self.pk_field_name]
    }

    fn extract_key(
        &self,
        primary_key: &SharedTableRowId,
        entity: &SharedTableRow,
    ) -> Option<Vec<u8>> {
        // Get the PK field value from the row
        let pk_value = entity.fields.get(&self.pk_field_name)?;

        let pk_bytes = scalar_value_to_bytes(pk_value);
        Some(encode_key(&(pk_bytes, primary_key.as_i64())))
    }

    fn filter_to_prefix(&self, filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        use kalamdb_store::extract_i64_equality;
        use kalamdb_store::extract_string_equality;

        // Try to extract equality filter on PK column
        if let Some((col, val)) = extract_string_equality(filter) {
            if col == self.pk_field_name {
                let pk_value = ScalarValue::Utf8(Some(val.to_string()));
                let pk_bytes = scalar_value_to_bytes(&pk_value);
                return Some(encode_prefix(&(pk_bytes,)));
            }
        }

        if let Some((col, val)) = extract_i64_equality(filter) {
            if col == self.pk_field_name {
                let pk_value = ScalarValue::Int64(Some(val));
                let pk_bytes = scalar_value_to_bytes(&pk_value);
                return Some(encode_prefix(&(pk_bytes,)));
            }
        }

        None
    }
}

/// Create a PK index for a shared table.
///
/// # Arguments
/// * `table_id` - Table identifier (namespace + table name)
/// * `pk_field_name` - Name of the primary key column
pub fn create_shared_table_pk_index(
    table_id: &kalamdb_commons::TableId,
    pk_field_name: &str,
) -> std::sync::Arc<dyn IndexDefinition<SharedTableRowId, SharedTableRow>> {
    std::sync::Arc::new(SharedTablePkIndex::new(table_id, pk_field_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::ids::SeqId;
    use kalamdb_commons::models::rows::Row;
    use std::collections::BTreeMap;

    fn create_test_row(seq: i64, id_value: i64) -> (SharedTableRowId, SharedTableRow) {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(id_value)));
        values.insert("name".to_string(), ScalarValue::Utf8(Some("Test".to_string())));

        let key = SeqId::new(seq);
        let row = SharedTableRow {
            _seq: SeqId::new(seq),
            _commit_seq: 0,
            _deleted: false,
            fields: Row::new(values),
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

        // Two versions of the same row (same PK, different seq)
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

        // Different pk values - keys should be completely different
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
