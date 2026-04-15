use std::fmt;

use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, TableId, TransactionId, UserId};
use kalamdb_commons::TableType;
use serde::{Deserialize, Serialize};

use crate::overlay::TransactionOverlayEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagedInsertBuildError {
    MissingPrimaryKey { column_name: String },
}

impl fmt::Display for StagedInsertBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrimaryKey { column_name } => {
                write!(f, "transactional INSERT requires primary key column '{}'", column_name)
            },
        }
    }
}

impl std::error::Error for StagedInsertBuildError {}

/// Shared logical DML mutation buffered inside an explicit transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedMutation {
    pub transaction_id: TransactionId,
    pub mutation_order: u64,
    pub table_id: TableId,
    pub table_type: TableType,
    pub user_id: Option<UserId>,
    pub operation_kind: OperationKind,
    pub primary_key: String,
    pub payload: Row,
    pub tombstone: bool,
}

impl StagedMutation {
    pub fn new(
        transaction_id: TransactionId,
        table_id: TableId,
        table_type: TableType,
        user_id: Option<UserId>,
        operation_kind: OperationKind,
        primary_key: impl Into<String>,
        payload: Row,
        tombstone: bool,
    ) -> Self {
        Self {
            transaction_id,
            mutation_order: 0,
            table_id,
            table_type,
            user_id,
            operation_kind,
            primary_key: primary_key.into(),
            payload,
            tombstone,
        }
    }

    #[inline]
    pub fn approximate_size_bytes(&self) -> usize {
        let payload_bytes = serde_json::to_vec(&self.payload).map(|bytes| bytes.len()).unwrap_or(0);
        self.primary_key.len()
            + self.table_id.full_name().len()
            + self.user_id.as_ref().map(|user_id| user_id.as_str().len()).unwrap_or(0)
            + payload_bytes
            + std::mem::size_of::<u64>()
    }

    #[inline]
    pub fn overlay_entry(&self) -> TransactionOverlayEntry {
        TransactionOverlayEntry {
            transaction_id: self.transaction_id.clone(),
            mutation_order: self.mutation_order,
            table_id: self.table_id.clone(),
            table_type: self.table_type,
            user_id: self.user_id.clone(),
            operation_kind: self.operation_kind,
            primary_key: self.primary_key.clone(),
            payload: self.payload.clone(),
            tombstone: self.tombstone,
        }
    }
}

pub fn build_insert_staged_mutations(
    transaction_id: &TransactionId,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    pk_column: &str,
    rows: Vec<Row>,
) -> Result<Vec<StagedMutation>, StagedInsertBuildError> {
    rows.into_iter()
        .map(|row| {
            let primary_key = row.values.get(pk_column).ok_or_else(|| {
                StagedInsertBuildError::MissingPrimaryKey {
                    column_name: pk_column.to_string(),
                }
            })?;

            Ok(StagedMutation::new(
                transaction_id.clone(),
                table_id.clone(),
                table_type,
                user_id.clone(),
                OperationKind::Insert,
                primary_key.to_string(),
                row,
                false,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{build_insert_staged_mutations, StagedInsertBuildError};
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::{TableName, TransactionId, UserId};
    use kalamdb_commons::{NamespaceId, TableId, TableType};

    fn test_table_id() -> TableId {
        TableId::new(NamespaceId::new("app"), TableName::new("items"))
    }

    fn test_transaction_id() -> TransactionId {
        TransactionId::try_new("0195c7a9-4e0f-7b9a-8c9d-1234567890ab".to_string()).unwrap()
    }

    #[test]
    fn build_insert_staged_mutations_preserves_user_scope() {
        let user_id = UserId::new("user-1");
        let rows = vec![Row::from_vec(vec![
            ("id".to_string(), ScalarValue::Utf8(Some("row-1".to_string()))),
            ("value".to_string(), ScalarValue::Int64(Some(10))),
        ])];

        let mutations = build_insert_staged_mutations(
            &test_transaction_id(),
            &test_table_id(),
            TableType::User,
            Some(user_id.clone()),
            "id",
            rows,
        )
        .unwrap();

        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].user_id.as_ref(), Some(&user_id));
        assert_eq!(mutations[0].primary_key, "row-1");
        assert_eq!(mutations[0].operation_kind, kalamdb_commons::models::OperationKind::Insert);
    }

    #[test]
    fn build_insert_staged_mutations_requires_primary_key_column() {
        let rows = vec![Row::from_vec(vec![(
            "value".to_string(),
            ScalarValue::Int64(Some(10)),
        )])];

        let error = build_insert_staged_mutations(
            &test_transaction_id(),
            &test_table_id(),
            TableType::Shared,
            None,
            "id",
            rows,
        )
        .unwrap_err();

        assert_eq!(
            error,
            StagedInsertBuildError::MissingPrimaryKey {
                column_name: "id".to_string(),
            }
        );
    }
}
