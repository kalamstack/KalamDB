//! Generic prefix `IndexDefinition` for PK and scalar secondary indexes.
//!
//! Index keys are storekey tuples of optional `user_id`, column bytes (from
//! `scalar_value_to_bytes`), and `seq`. PK indexes are this adapter with a
//! single column. User-scoped keys include `user_id` once.

use std::marker::PhantomData;

use kalamdb_commons::{
    conversions::scalar_value_to_bytes,
    models::{SharedTableRow, UserId, UserTableRow},
    storage::Partition,
    storage_key::{encode_key, encode_prefix},
    KSerializable, StorageKey,
};

use crate::indexed_store::IndexDefinition;

/// Maximum indexed user columns before `seq` (covers chat composite keys).
const MAX_INDEX_COLUMNS: usize = 4;

/// Primary key / identity parts used when encoding a prefix index key.
pub trait PrefixIndexedKey: StorageKey {
    /// User scope for USER tables and user-scoped vector ops. `None` for SHARED.
    fn prefix_index_user_id(&self) -> Option<&UserId>;

    /// MVCC sequence appended to every index key.
    fn prefix_index_seq(&self) -> i64;
}

/// Entity fields encoded into a prefix index.
pub trait PrefixIndexedValue {
    /// Order-preserving bytes for `column`, or `None` to skip indexing this row.
    fn prefix_index_field_bytes(&self, column: &str) -> Option<Vec<u8>>;
}

/// Column-list prefix index. PK is this type with a single column name.
#[derive(Clone, Debug)]
pub struct PrefixIndex<K, V> {
    partition:   Partition,
    columns:     Vec<String>,
    user_scoped: bool,
    _marker:     PhantomData<(K, V)>,
}

impl<K, V> PrefixIndex<K, V> {
    /// Create a prefix index.
    ///
    /// `user_scoped` must match whether `K` carries a `user_id` (USER tables).
    pub fn new(partition: impl Into<Partition>, columns: Vec<String>, user_scoped: bool) -> Self {
        Self {
            partition: partition.into(),
            columns,
            user_scoped,
            _marker: PhantomData,
        }
    }

    /// Encode a prefix over `user_id` (optional) and column bytes.
    pub fn encode_column_prefix(
        &self,
        user_id: Option<&UserId>,
        column_bytes: &[Vec<u8>],
    ) -> Vec<u8> {
        encode_prefix_parts(user_id.map(UserId::as_str), column_bytes)
    }

    /// Encode a full index key including `seq`.
    pub fn encode_index_key(
        &self,
        user_id: Option<&UserId>,
        column_bytes: &[Vec<u8>],
        seq: i64,
    ) -> Vec<u8> {
        encode_key_parts(user_id.map(UserId::as_str), column_bytes, seq)
    }

    /// Prefix for all index entries for one user (USER tables).
    pub fn encode_user_prefix(&self, user_id: &UserId) -> Vec<u8> {
        encode_prefix(&(user_id.as_str(),))
    }

    /// Whether keys include a leading `user_id`.
    pub fn is_user_scoped(&self) -> bool {
        self.user_scoped
    }

    #[cfg(feature = "datafusion")]
    fn column_bytes_from_filter(
        &self,
        filter: &datafusion::logical_expr::Expr,
    ) -> Option<Vec<Vec<u8>>> {
        let (col, value) = equality_scalar(filter)?;
        if self.columns.first().map(String::as_str) != Some(col) {
            return None;
        }
        Some(vec![scalar_value_to_bytes(&value)])
    }

    /// Prefix for a filter, optionally scoping USER indexes by `user_id`.
    #[cfg(feature = "datafusion")]
    pub fn filter_prefix_with_scope(
        &self,
        user_id: Option<&UserId>,
        filter: &datafusion::logical_expr::Expr,
    ) -> Option<Vec<u8>> {
        let column_bytes = self.column_bytes_from_filter(filter)?;
        if self.user_scoped {
            Some(encode_prefix_parts(Some(user_id?.as_str()), &column_bytes))
        } else {
            Some(encode_prefix_parts(None, &column_bytes))
        }
    }
}

impl<K, V> IndexDefinition<K, V> for PrefixIndex<K, V>
where
    K: PrefixIndexedKey,
    V: PrefixIndexedValue + KSerializable,
{
    fn partition(&self) -> Partition {
        self.partition.clone()
    }

    fn indexed_columns(&self) -> Vec<&str> {
        self.columns.iter().map(String::as_str).collect()
    }

    fn extract_key(&self, primary_key: &K, entity: &V) -> Option<Vec<u8>> {
        if self.columns.is_empty() || self.columns.len() > MAX_INDEX_COLUMNS {
            return None;
        }
        let mut column_bytes = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            column_bytes.push(entity.prefix_index_field_bytes(column)?);
        }
        Some(encode_key_parts(
            primary_key.prefix_index_user_id().map(UserId::as_str),
            &column_bytes,
            primary_key.prefix_index_seq(),
        ))
    }

    #[cfg(feature = "datafusion")]
    fn filter_to_prefix(&self, filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        self.filter_to_prefix_with_scope(None, filter)
    }

    #[cfg(feature = "datafusion")]
    fn filter_to_prefix_with_scope(
        &self,
        user_id: Option<&UserId>,
        filter: &datafusion::logical_expr::Expr,
    ) -> Option<Vec<u8>> {
        self.filter_prefix_with_scope(user_id, filter)
    }
}

pub(crate) fn encode_prefix_parts(user_id: Option<&str>, columns: &[Vec<u8>]) -> Vec<u8> {
    match (user_id, columns) {
        (None, []) => Vec::new(),
        (None, [a]) => encode_prefix(&(a.clone(),)),
        (None, [a, b]) => encode_prefix(&(a.clone(), b.clone())),
        (None, [a, b, c]) => encode_prefix(&(a.clone(), b.clone(), c.clone())),
        (None, [a, b, c, d]) => encode_prefix(&(a.clone(), b.clone(), c.clone(), d.clone())),
        (Some(user_id), []) => encode_prefix(&(user_id,)),
        (Some(user_id), [a]) => encode_prefix(&(user_id, a.clone())),
        (Some(user_id), [a, b]) => encode_prefix(&(user_id, a.clone(), b.clone())),
        (Some(user_id), [a, b, c]) => encode_prefix(&(user_id, a.clone(), b.clone(), c.clone())),
        (Some(user_id), [a, b, c, d]) => {
            encode_prefix(&(user_id, a.clone(), b.clone(), c.clone(), d.clone()))
        },
        _ => encode_prefix_parts(user_id, &columns[..MAX_INDEX_COLUMNS.min(columns.len())]),
    }
}

pub(crate) fn encode_key_parts(user_id: Option<&str>, columns: &[Vec<u8>], seq: i64) -> Vec<u8> {
    match (user_id, columns) {
        (None, [a]) => encode_key(&(a.clone(), seq)),
        (None, [a, b]) => encode_key(&(a.clone(), b.clone(), seq)),
        (None, [a, b, c]) => encode_key(&(a.clone(), b.clone(), c.clone(), seq)),
        (None, [a, b, c, d]) => encode_key(&(a.clone(), b.clone(), c.clone(), d.clone(), seq)),
        (Some(user_id), [a]) => encode_key(&(user_id, a.clone(), seq)),
        (Some(user_id), [a, b]) => encode_key(&(user_id, a.clone(), b.clone(), seq)),
        (Some(user_id), [a, b, c]) => encode_key(&(user_id, a.clone(), b.clone(), c.clone(), seq)),
        (Some(user_id), [a, b, c, d]) => {
            encode_key(&(user_id, a.clone(), b.clone(), c.clone(), d.clone(), seq))
        },
        (None, []) => encode_key(&seq),
        (Some(user_id), []) => encode_key(&(user_id, seq)),
        _ => encode_key_parts(user_id, &columns[..MAX_INDEX_COLUMNS.min(columns.len())], seq),
    }
}

#[cfg(feature = "datafusion")]
fn equality_scalar(
    filter: &datafusion::logical_expr::Expr,
) -> Option<(&str, datafusion::scalar::ScalarValue)> {
    use datafusion::logical_expr::{Expr, Operator};

    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(value, _)) if !value.is_null() => {
                    Some((col.name.as_str(), value.clone()))
                },
                (Expr::Literal(value, _), Expr::Column(col)) if !value.is_null() => {
                    Some((col.name.as_str(), value.clone()))
                },
                _ => None,
            }
        },
        _ => None,
    }
}

impl PrefixIndexedKey for kalamdb_commons::ids::UserTableRowId {
    fn prefix_index_user_id(&self) -> Option<&UserId> {
        Some(&self.user_id)
    }

    fn prefix_index_seq(&self) -> i64 {
        self.seq.as_i64()
    }
}

impl PrefixIndexedKey for kalamdb_commons::ids::SeqId {
    fn prefix_index_user_id(&self) -> Option<&UserId> {
        None
    }

    fn prefix_index_seq(&self) -> i64 {
        self.as_i64()
    }
}

impl PrefixIndexedValue for SharedTableRow {
    fn prefix_index_field_bytes(&self, column: &str) -> Option<Vec<u8>> {
        self.fields.get(column).map(scalar_value_to_bytes)
    }
}

impl PrefixIndexedValue for UserTableRow {
    fn prefix_index_field_bytes(&self, column: &str) -> Option<Vec<u8>> {
        self.fields.get(column).map(scalar_value_to_bytes)
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{
        ids::{SeqId, UserTableRowId},
        models::UserId,
        KSerializable,
    };

    use super::*;

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct TestRow {
        fields: std::collections::BTreeMap<String, Vec<u8>>,
    }

    impl KSerializable for TestRow {}

    impl PrefixIndexedValue for TestRow {
        fn prefix_index_field_bytes(&self, column: &str) -> Option<Vec<u8>> {
            self.fields.get(column).cloned()
        }
    }

    fn conversation_row(conversation_id: &str) -> TestRow {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("conversation_id".to_string(), conversation_id.as_bytes().to_vec());
        TestRow { fields }
    }

    #[test]
    fn extract_key_composite_conversation_id_shares_prefix() {
        let index = PrefixIndex::<SeqId, TestRow>::new(
            "shared_chat:messages_idx_conversation",
            vec!["conversation_id".to_string()],
            false,
        );
        let row = conversation_row("42");
        let key_a = index.extract_key(&SeqId::new(100), &row).unwrap();
        let key_b = index.extract_key(&SeqId::new(200), &row).unwrap();
        let prefix = index.encode_column_prefix(None, &[b"42".to_vec()]);
        assert!(key_a.starts_with(&prefix));
        assert!(key_b.starts_with(&prefix));
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn extract_key_unrelated_column_is_skipped() {
        let index = PrefixIndex::<SeqId, TestRow>::new(
            "shared_chat:messages_idx_conversation",
            vec!["conversation_id".to_string()],
            false,
        );
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("room_id".to_string(), b"99".to_vec());
        assert!(index.extract_key(&SeqId::new(1), &TestRow { fields }).is_none());
    }

    #[test]
    fn user_scoped_keys_include_user_id_once() {
        let index = PrefixIndex::<UserTableRowId, TestRow>::new(
            "user_chat:messages_idx_conversation",
            vec!["conversation_id".to_string()],
            true,
        );
        let row = conversation_row("42");
        let alice = UserTableRowId::new(UserId::new("alice"), SeqId::new(10));
        let bob = UserTableRowId::new(UserId::new("bob"), SeqId::new(10));
        let alice_key = index.extract_key(&alice, &row).unwrap();
        let bob_key = index.extract_key(&bob, &row).unwrap();
        assert!(alice_key.starts_with(&index.encode_user_prefix(&UserId::new("alice"))));
        assert!(bob_key.starts_with(&index.encode_user_prefix(&UserId::new("bob"))));
        assert_ne!(alice_key, bob_key);
        let alice_prefix =
            index.encode_column_prefix(Some(&UserId::new("alice")), &[b"42".to_vec()]);
        assert!(alice_key.starts_with(&alice_prefix));
        assert!(!bob_key.starts_with(&alice_prefix));
    }
}
