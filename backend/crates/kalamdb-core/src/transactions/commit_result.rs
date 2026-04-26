use chrono::{DateTime, Utc};
use kalamdb_commons::{models::TransactionId, TableType};
// Re-export fanout types from kalamdb-live (canonical source)
pub use kalamdb_live::fanout::{
    CommitSideEffectPlan, FanoutDispatchPlan, FanoutOwnerScope, TransactionSideEffects,
};

use super::TransactionWriteSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionCommitOutcome {
    Committed,
    RolledBack,
    Aborted,
}

/// Extension: build a CommitSideEffectPlan from a TransactionWriteSet.
///
/// This lives in kalamdb-core because TransactionWriteSet is a core type,
/// while the fanout plan types are owned by kalamdb-live.
pub fn commit_side_effect_plan_from_write_set(
    write_set: &TransactionWriteSet,
) -> CommitSideEffectPlan {
    let mut plan = CommitSideEffectPlan::new(write_set.transaction_id.clone());
    for mutation in write_set.ordered_mutations() {
        if matches!(mutation.table_type, TableType::User | TableType::Shared) {
            let owner_scope = mutation
                .user_id
                .clone()
                .map(FanoutOwnerScope::User)
                .unwrap_or(FanoutOwnerScope::Shared);
            plan.record_notification_group(mutation.table_id.clone(), owner_scope);
            plan.publisher_events += 1;
            plan.manifest_updates += 1;
        }
    }

    plan.sort_plans();
    plan
}

/// Result of sealing or aborting an explicit transaction.
#[derive(Debug, Clone)]
pub struct TransactionCommitResult {
    pub transaction_id: TransactionId,
    pub outcome: TransactionCommitOutcome,
    pub affected_rows: usize,
    pub committed_at: Option<DateTime<Utc>>,
    pub committed_commit_seq: Option<u64>,
    pub failure_reason: Option<String>,
    pub emitted_side_effects: TransactionSideEffects,
}

impl TransactionCommitResult {
    pub fn committed(
        transaction_id: TransactionId,
        affected_rows: usize,
        committed_commit_seq: Option<u64>,
        emitted_side_effects: TransactionSideEffects,
    ) -> Self {
        Self {
            transaction_id,
            outcome: TransactionCommitOutcome::Committed,
            affected_rows,
            committed_at: Some(Utc::now()),
            committed_commit_seq,
            failure_reason: None,
            emitted_side_effects,
        }
    }

    pub fn rolled_back(transaction_id: TransactionId, affected_rows: usize) -> Self {
        Self {
            transaction_id,
            outcome: TransactionCommitOutcome::RolledBack,
            affected_rows,
            committed_at: None,
            committed_commit_seq: None,
            failure_reason: None,
            emitted_side_effects: TransactionSideEffects::default(),
        }
    }

    pub fn aborted(
        transaction_id: TransactionId,
        affected_rows: usize,
        failure_reason: impl Into<String>,
    ) -> Self {
        Self {
            transaction_id,
            outcome: TransactionCommitOutcome::Aborted,
            affected_rows,
            committed_at: None,
            committed_commit_seq: None,
            failure_reason: Some(failure_reason.into()),
            emitted_side_effects: TransactionSideEffects::default(),
        }
    }
}
