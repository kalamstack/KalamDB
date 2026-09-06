use kalamdb_commons::{
    models::{TableId, UserId},
    storage::Partition,
};
use kalamdb_store::{IndexDefinition, PrefixIndex, PrefixIndexedKey, PrefixIndexedValue};

use super::{
    models::{SharedVectorHotOpId, UserVectorHotOpId, VectorHotOp},
    vector_hot_store::{
        shared_vector_pk_index_partition_name, user_vector_pk_index_partition_name,
    },
};

impl PrefixIndexedKey for UserVectorHotOpId {
    fn prefix_index_user_id(&self) -> Option<&UserId> {
        Some(&self.user_id)
    }

    fn prefix_index_seq(&self) -> i64 {
        self.seq.as_i64()
    }
}

impl PrefixIndexedKey for SharedVectorHotOpId {
    fn prefix_index_user_id(&self) -> Option<&UserId> {
        None
    }

    fn prefix_index_seq(&self) -> i64 {
        self.seq.as_i64()
    }
}

impl PrefixIndexedValue for VectorHotOp {
    fn prefix_index_field_bytes(&self, column: &str) -> Option<Vec<u8>> {
        (column == "pk").then(|| self.pk.as_bytes().to_vec())
    }
}

/// Secondary index for user-scoped vector ops by (user_id, pk, seq).
pub struct UserVectorPkIndex {
    inner: PrefixIndex<UserVectorHotOpId, VectorHotOp>,
}

impl UserVectorPkIndex {
    pub fn new(table_id: &TableId, column_name: &str) -> Self {
        Self {
            inner: PrefixIndex::new(
                user_vector_pk_index_partition_name(table_id, column_name),
                vec!["pk".to_string()],
                true,
            ),
        }
    }

    pub fn build_prefix(&self, user_id: &UserId, pk: &str) -> Vec<u8> {
        self.inner.encode_column_prefix(Some(user_id), &[pk.as_bytes().to_vec()])
    }
}

impl IndexDefinition<UserVectorHotOpId, VectorHotOp> for UserVectorPkIndex {
    fn partition(&self) -> Partition {
        self.inner.partition()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        self.inner.indexed_columns()
    }

    fn extract_key(
        &self,
        primary_key: &UserVectorHotOpId,
        entity: &VectorHotOp,
    ) -> Option<Vec<u8>> {
        self.inner.extract_key(primary_key, entity)
    }
}

/// Secondary index for shared vector ops by (pk, seq).
pub struct SharedVectorPkIndex {
    inner: PrefixIndex<SharedVectorHotOpId, VectorHotOp>,
}

impl SharedVectorPkIndex {
    pub fn new(table_id: &TableId, column_name: &str) -> Self {
        Self {
            inner: PrefixIndex::new(
                shared_vector_pk_index_partition_name(table_id, column_name),
                vec!["pk".to_string()],
                false,
            ),
        }
    }

    pub fn build_prefix(&self, pk: &str) -> Vec<u8> {
        self.inner.encode_column_prefix(None, &[pk.as_bytes().to_vec()])
    }
}

impl IndexDefinition<SharedVectorHotOpId, VectorHotOp> for SharedVectorPkIndex {
    fn partition(&self) -> Partition {
        self.inner.partition()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        self.inner.indexed_columns()
    }

    fn extract_key(
        &self,
        primary_key: &SharedVectorHotOpId,
        entity: &VectorHotOp,
    ) -> Option<Vec<u8>> {
        self.inner.extract_key(primary_key, entity)
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{
        ids::SeqId,
        models::{NamespaceId, TableName},
    };

    use super::*;

    #[test]
    fn test_user_vector_pk_index_partition_name() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("tbl1"));
        let idx = UserVectorPkIndex::new(&table_id, "embedding");
        assert_eq!(idx.partition().name(), "vix_ns1:tbl1_embedding_user_pk_idx");
    }

    #[test]
    fn test_shared_vector_pk_index_partition_name() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("tbl1"));
        let idx = SharedVectorPkIndex::new(&table_id, "embedding");
        assert_eq!(idx.partition().name(), "vix_ns1:tbl1_embedding_shared_pk_idx");
    }

    #[test]
    fn test_user_index_key_prefix() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("tbl1"));
        let idx = UserVectorPkIndex::new(&table_id, "embedding");
        let key = UserVectorHotOpId::new(UserId::new("u1"), SeqId::new(10), "pk-1");
        let encoded = idx
            .extract_key(
                &key,
                &VectorHotOp {
                    table_id,
                    column_name: "embedding".to_string(),
                    pk: "pk-1".to_string(),
                    op_type: super::super::models::VectorHotOpType::Upsert,
                    vector: None,
                    vector_ref: None,
                    dimensions: 384,
                    metric: kalamdb_system::VectorMetric::Cosine,
                    updated_at: 0,
                },
            )
            .unwrap();

        let prefix = idx.build_prefix(&UserId::new("u1"), "pk-1");
        assert!(encoded.starts_with(&prefix));
    }
}
