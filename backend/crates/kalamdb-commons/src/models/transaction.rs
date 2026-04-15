use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Lifecycle state for an explicit transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionState {
    OpenRead,
    OpenWrite,
    Committing,
    Committed,
    RollingBack,
    RolledBack,
    TimedOut,
    Aborted,
}

impl TransactionState {
    /// Backward-compatible observability label used by pg session state.
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionState::OpenRead | TransactionState::OpenWrite => "active",
            TransactionState::Committing => "committing",
            TransactionState::Committed => "committed",
            TransactionState::RollingBack => "rolling_back",
            TransactionState::RolledBack => "rolled_back",
            TransactionState::TimedOut => "timed_out",
            TransactionState::Aborted => "aborted",
        }
    }

    /// Distinct lifecycle label for transaction-specific observability.
    pub fn lifecycle_str(&self) -> &'static str {
        match self {
            TransactionState::OpenRead => "open_read",
            TransactionState::OpenWrite => "open_write",
            TransactionState::Committing => "committing",
            TransactionState::Committed => "committed",
            TransactionState::RollingBack => "rolling_back",
            TransactionState::RolledBack => "rolled_back",
            TransactionState::TimedOut => "timed_out",
            TransactionState::Aborted => "aborted",
        }
    }

    #[inline]
    pub fn is_open(&self) -> bool {
        matches!(self, TransactionState::OpenRead | TransactionState::OpenWrite)
    }

    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed
                | TransactionState::RolledBack
                | TransactionState::TimedOut
                | TransactionState::Aborted
        )
    }
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.lifecycle_str())
    }
}

/// Identifies the surface that opened a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionOrigin {
    PgRpc,
    SqlBatch,
    Internal,
}

impl TransactionOrigin {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionOrigin::PgRpc => "PgRpc",
            TransactionOrigin::SqlBatch => "SqlBatch",
            TransactionOrigin::Internal => "Internal",
        }
    }
}

impl fmt::Display for TransactionOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Kind of staged DML operation captured inside a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum OperationKind {
    Insert,
    Update,
    Delete,
}

impl OperationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationKind::Insert => "Insert",
            OperationKind::Update => "Update",
            OperationKind::Delete => "Delete",
        }
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_observability_keeps_open_transactions_active() {
        assert_eq!(TransactionState::OpenRead.as_str(), "active");
        assert_eq!(TransactionState::OpenWrite.as_str(), "active");
        assert_eq!(TransactionState::OpenRead.lifecycle_str(), "open_read");
        assert_eq!(TransactionState::OpenWrite.lifecycle_str(), "open_write");
    }

    #[test]
    fn transaction_origin_strings_match_spec_labels() {
        assert_eq!(TransactionOrigin::PgRpc.as_str(), "PgRpc");
        assert_eq!(TransactionOrigin::SqlBatch.as_str(), "SqlBatch");
        assert_eq!(TransactionOrigin::Internal.as_str(), "Internal");
    }
}
