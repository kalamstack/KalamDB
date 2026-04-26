use kalamdb_commons::{
    models::{TableId, UserId},
    storage::Partition,
    storage_key::{encode_key, encode_prefix},
};
use kalamdb_store::IndexDefinition;

use super::{
    models::{SharedVectorHotOpId, UserVectorHotOpId, VectorHotOp},
    vector_hot_store::{
        shared_vector_pk_index_partition_name, user_vector_pk_index_partition_name,
    },
};

/// Secondary index for user-scoped vector ops by (user_id, pk, seq).
pub struct UserVectorPkIndex {
    partition: Partition,
}

impl UserVectorPkIndex {
    pub fn new(table_id: &TableId, column_name: &str) -> Self {
        Self {
            partition: Partition::new(user_vector_pk_index_partition_name(table_id, column_name)),
        }
    }

    pub fn build_prefix(&self, user_id: &UserId, pk: &str) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(), pk))
    }
}

impl IndexDefinition<UserVectorHotOpId, VectorHotOp> for UserVectorPkIndex {
    fn partition(&self) -> Partition {
        self.partition.clone()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec!["pk"]
    }

    fn extract_key(
        &self,
        primary_key: &UserVectorHotOpId,
        _entity: &VectorHotOp,
    ) -> Option<Vec<u8>> {
        Some(encode_key(&(
            primary_key.user_id.as_str(),
            primary_key.pk.as_str(),
            primary_key.seq.as_i64(),
        )))
    }
}

/// Secondary index for shared vector ops by (pk, seq).
pub struct SharedVectorPkIndex {
    partition: Partition,
}

impl SharedVectorPkIndex {
    pub fn new(table_id: &TableId, column_name: &str) -> Self {
        Self {
            partition: Partition::new(shared_vector_pk_index_partition_name(table_id, column_name)),
        }
    }

    pub fn build_prefix(&self, pk: &str) -> Vec<u8> {
        encode_prefix(&(pk,))
    }
}

impl IndexDefinition<SharedVectorHotOpId, VectorHotOp> for SharedVectorPkIndex {
    fn partition(&self) -> Partition {
        self.partition.clone()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec!["pk"]
    }

    fn extract_key(
        &self,
        primary_key: &SharedVectorHotOpId,
        _entity: &VectorHotOp,
    ) -> Option<Vec<u8>> {
        Some(encode_key(&(primary_key.pk.as_str(), primary_key.seq.as_i64())))
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
