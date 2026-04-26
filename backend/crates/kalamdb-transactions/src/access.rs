use std::fmt;

use kalamdb_commons::{
    models::{TableId, TransactionId, UserId},
    TableType,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionAccessError {
    NotLeader { leader_addr: Option<String> },
    InvalidOperation(String),
}

impl TransactionAccessError {
    #[inline]
    pub fn invalid_operation(message: impl Into<String>) -> Self {
        Self::InvalidOperation(message.into())
    }
}

impl fmt::Display for TransactionAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_addr } => match leader_addr {
                Some(addr) => write!(f, "Not leader for shard. Leader: {}", addr),
                None => write!(f, "Not leader for shard. Leader unknown"),
            },
            Self::InvalidOperation(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for TransactionAccessError {}

pub trait TransactionAccessValidator: std::fmt::Debug + Send + Sync {
    fn validate_table_access(
        &self,
        transaction_id: &TransactionId,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<&UserId>,
    ) -> Result<(), TransactionAccessError>;
}
