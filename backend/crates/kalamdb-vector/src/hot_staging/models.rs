use kalamdb_commons::{
    ids::SeqId,
    models::{TableId, UserId},
    storage_key::{decode_key, encode_key, encode_prefix},
    KSerializable, StorageKey,
};
use kalamdb_system::VectorMetric;
use serde::{Deserialize, Serialize};

/// Operation type stored in vector hot staging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorHotOpType {
    Upsert,
    Delete,
}

/// Hot-staged vector operation payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorHotOp {
    /// Table identifier (namespace + table).
    pub table_id: TableId,
    /// Indexed column name.
    pub column_name: String,
    /// Primary key value for the row.
    pub pk: String,
    /// Operation kind.
    pub op_type: VectorHotOpType,
    /// Embedding payload when available.
    pub vector: Option<Vec<f32>>,
    /// External reference when vector is resolved asynchronously.
    pub vector_ref: Option<String>,
    /// Declared embedding dimensions.
    pub dimensions: u32,
    /// Similarity metric for this vector column.
    pub metric: VectorMetric,
    /// Last update timestamp (unix millis).
    pub updated_at: i64,
}

impl VectorHotOp {
    pub fn new(
        table_id: TableId,
        column_name: impl Into<String>,
        pk: impl Into<String>,
        op_type: VectorHotOpType,
        vector: Option<Vec<f32>>,
        vector_ref: Option<String>,
        dimensions: u32,
        metric: VectorMetric,
    ) -> Self {
        Self {
            table_id,
            column_name: column_name.into(),
            pk: pk.into(),
            op_type,
            vector,
            vector_ref,
            dimensions,
            metric,
            updated_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

impl KSerializable for VectorHotOp {}

/// Composite key for user-table vector staging entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserVectorHotOpId {
    pub user_id: UserId,
    pub seq: SeqId,
    pub pk: String,
}

impl UserVectorHotOpId {
    pub fn new(user_id: UserId, seq: SeqId, pk: impl Into<String>) -> Self {
        Self {
            user_id,
            seq,
            pk: pk.into(),
        }
    }

    /// Prefix for scanning all vector ops for a user scope.
    pub fn user_prefix(user_id: &UserId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(),))
    }
}

impl StorageKey for UserVectorHotOpId {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(&(self.user_id.as_str(), self.seq.as_i64(), self.pk.as_str()))
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        let (user_id, seq, pk): (String, i64, String) = decode_key(bytes)?;
        Ok(Self {
            user_id: UserId::new(user_id),
            seq: SeqId::new(seq),
            pk,
        })
    }
}

/// Composite key for shared-table vector staging entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedVectorHotOpId {
    pub seq: SeqId,
    pub pk: String,
}

impl SharedVectorHotOpId {
    pub fn new(seq: SeqId, pk: impl Into<String>) -> Self {
        Self { seq, pk: pk.into() }
    }
}

impl StorageKey for SharedVectorHotOpId {
    fn storage_key(&self) -> Vec<u8> {
        encode_key(&(self.seq.as_i64(), self.pk.as_str()))
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        let (seq, pk): (i64, String) = decode_key(bytes)?;
        Ok(Self {
            seq: SeqId::new(seq),
            pk,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_vector_hot_op_id_roundtrip() {
        let key = UserVectorHotOpId::new(UserId::new("u1"), SeqId::new(42), "pk-1");
        let encoded = key.storage_key();
        let decoded = UserVectorHotOpId::from_storage_key(&encoded).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_shared_vector_hot_op_id_roundtrip() {
        let key = SharedVectorHotOpId::new(SeqId::new(42), "pk-2");
        let encoded = key.storage_key();
        let decoded = SharedVectorHotOpId::from_storage_key(&encoded).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_user_prefix_isolation() {
        let prefix_u1 = UserVectorHotOpId::user_prefix(&UserId::new("u1"));
        let key_u1 = UserVectorHotOpId::new(UserId::new("u1"), SeqId::new(1), "a").storage_key();
        let key_u2 = UserVectorHotOpId::new(UserId::new("u2"), SeqId::new(1), "a").storage_key();
        assert!(key_u1.starts_with(&prefix_u1));
        assert!(!key_u2.starts_with(&prefix_u1));
    }
}
