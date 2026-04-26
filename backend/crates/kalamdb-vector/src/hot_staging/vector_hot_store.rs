use std::sync::Arc;

use kalamdb_commons::models::TableId;
use kalamdb_store::{IndexedEntityStore, StorageBackend};

use super::{
    models::{SharedVectorHotOpId, UserVectorHotOpId, VectorHotOp},
    pk_index::{SharedVectorPkIndex, UserVectorPkIndex},
};

/// Indexed store alias for user-scoped vector hot ops.
pub type UserVectorHotStore = IndexedEntityStore<UserVectorHotOpId, VectorHotOp>;
/// Indexed store alias for shared-scope vector hot ops.
pub type SharedVectorHotStore = IndexedEntityStore<SharedVectorHotOpId, VectorHotOp>;

/// Normalize vector column names for partition IDs.
pub fn normalize_vector_column_name(column_name: &str) -> String {
    let mut out = String::with_capacity(column_name.len());
    for ch in column_name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out
}

pub fn user_vector_ops_partition_name(table_id: &TableId, column_name: &str) -> String {
    format!("vix_{}_{}_user_ops", table_id, normalize_vector_column_name(column_name))
}

pub fn user_vector_pk_index_partition_name(table_id: &TableId, column_name: &str) -> String {
    format!("vix_{}_{}_user_pk_idx", table_id, normalize_vector_column_name(column_name))
}

pub fn shared_vector_ops_partition_name(table_id: &TableId, column_name: &str) -> String {
    format!("vix_{}_{}_shared_ops", table_id, normalize_vector_column_name(column_name))
}

pub fn shared_vector_pk_index_partition_name(table_id: &TableId, column_name: &str) -> String {
    format!("vix_{}_{}_shared_pk_idx", table_id, normalize_vector_column_name(column_name))
}

/// Create an indexed user vector hot-staging store for one table column.
pub fn new_indexed_user_vector_hot_store(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
    column_name: &str,
) -> UserVectorHotStore {
    let partition = user_vector_ops_partition_name(table_id, column_name);
    let pk_index = UserVectorPkIndex::new(table_id, column_name);
    IndexedEntityStore::new(backend, partition, vec![Arc::new(pk_index)])
}

/// Create an indexed shared vector hot-staging store for one table column.
pub fn new_indexed_shared_vector_hot_store(
    backend: Arc<dyn StorageBackend>,
    table_id: &TableId,
    column_name: &str,
) -> SharedVectorHotStore {
    let partition = shared_vector_ops_partition_name(table_id, column_name);
    let pk_index = SharedVectorPkIndex::new(table_id, column_name);
    IndexedEntityStore::new(backend, partition, vec![Arc::new(pk_index)])
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;

    #[test]
    fn test_normalize_vector_column_name() {
        assert_eq!(normalize_vector_column_name("embedding"), "embedding");
        assert_eq!(normalize_vector_column_name("doc.embedding"), "doc_embedding");
        assert_eq!(normalize_vector_column_name("EMBEDDING-V1"), "embedding_v1");
    }

    #[test]
    fn test_partition_names() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("tbl1"));

        assert_eq!(
            user_vector_ops_partition_name(&table_id, "embedding"),
            "vix_ns1:tbl1_embedding_user_ops"
        );
        assert_eq!(
            user_vector_pk_index_partition_name(&table_id, "embedding"),
            "vix_ns1:tbl1_embedding_user_pk_idx"
        );
        assert_eq!(
            shared_vector_ops_partition_name(&table_id, "embedding"),
            "vix_ns1:tbl1_embedding_shared_ops"
        );
        assert_eq!(
            shared_vector_pk_index_partition_name(&table_id, "embedding"),
            "vix_ns1:tbl1_embedding_shared_pk_idx"
        );
    }
}
