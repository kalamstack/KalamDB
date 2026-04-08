use chrono::{DateTime, Utc};
use datafusion::scalar::ScalarValue;

use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::{TableId, TransactionId, UserId};
use kalamdb_commons::websocket::ChangeNotification;
use kalamdb_commons::TableType;

use super::TransactionWriteSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionCommitOutcome {
    Committed,
    RolledBack,
    Aborted,
}

/// Informational summary of post-commit side effects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionSideEffects {
    pub notifications_sent: usize,
    pub manifest_updates: usize,
    pub publisher_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FanoutOwnerScope {
    Shared,
    User(UserId),
}

impl FanoutOwnerScope {
    #[inline]
    pub fn user_id(&self) -> Option<&UserId> {
        match self {
            Self::Shared => None,
            Self::User(user_id) => Some(user_id),
        }
    }

    #[inline]
    fn sort_key(&self) -> String {
        match self {
            Self::Shared => "shared".to_string(),
            Self::User(user_id) => format!("user:{}", user_id.as_str()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FanoutDispatchPlan {
    pub table_id: TableId,
    pub owner_scope: FanoutOwnerScope,
    pub change_count: usize,
    pub projection_groups: usize,
    pub serialization_groups: usize,
    pub seq_upper_bound: Option<SeqId>,
    pub notifications: Vec<ChangeNotification>,
}

impl FanoutDispatchPlan {
    pub fn new(table_id: TableId, owner_scope: FanoutOwnerScope) -> Self {
        Self {
            table_id,
            owner_scope,
            change_count: 0,
            projection_groups: 0,
            serialization_groups: 0,
            seq_upper_bound: None,
            notifications: Vec::new(),
        }
    }

    #[inline]
    pub fn record_change(&mut self) {
        self.change_count += 1;
    }

    pub fn push_notification(&mut self, notification: ChangeNotification) {
        self.change_count += 1;
        self.observe_notification_seq(&notification);
        self.notifications.push(notification);
    }

    fn observe_notification_seq(&mut self, notification: &ChangeNotification) {
        let seq_value = notification
            .row_data
            .values
            .get(SystemColumnNames::SEQ)
            .and_then(|value| match value {
                ScalarValue::Int64(Some(seq)) => Some(SeqId::from(*seq)),
                ScalarValue::UInt64(Some(seq)) => Some(SeqId::from(*seq as i64)),
                _ => None,
            });

        if let Some(seq_value) = seq_value {
            match self.seq_upper_bound {
                Some(existing) if existing.as_i64() >= seq_value.as_i64() => {},
                _ => self.seq_upper_bound = Some(seq_value),
            }
        }
    }
}

/// Deferred work released after durable commit succeeds.
#[derive(Debug, Clone)]
pub struct CommitSideEffectPlan {
    pub transaction_id: TransactionId,
    pub notifications: Vec<FanoutDispatchPlan>,
    pub publisher_events: usize,
    pub manifest_updates: usize,
}

impl CommitSideEffectPlan {
    pub fn new(transaction_id: TransactionId) -> Self {
        Self {
            transaction_id,
            notifications: Vec::new(),
            publisher_events: 0,
            manifest_updates: 0,
        }
    }

    pub fn from_write_set(write_set: &TransactionWriteSet) -> Self {
        let mut plan = Self::new(write_set.transaction_id.clone());
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

        plan.notifications.sort_by(|left, right| {
            left.table_id
                .full_name()
                .cmp(&right.table_id.full_name())
                .then_with(|| left.owner_scope.sort_key().cmp(&right.owner_scope.sort_key()))
        });

        plan
    }

    pub fn push_notification(
        &mut self,
        owner_scope: FanoutOwnerScope,
        notification: ChangeNotification,
    ) {
        if let Some(dispatch) = self
            .notifications
            .iter_mut()
            .find(|dispatch| dispatch.table_id == notification.table_id && dispatch.owner_scope == owner_scope)
        {
            dispatch.push_notification(notification);
            return;
        }

        let mut dispatch = FanoutDispatchPlan::new(notification.table_id.clone(), owner_scope);
        dispatch.push_notification(notification);
        self.notifications.push(dispatch);
    }

    #[inline]
    pub fn record_publisher_event(&mut self) {
        self.publisher_events += 1;
    }

    #[inline]
    pub fn record_manifest_update(&mut self) {
        self.manifest_updates += 1;
    }

    fn record_notification_group(&mut self, table_id: TableId, owner_scope: FanoutOwnerScope) {
        if let Some(dispatch) = self
            .notifications
            .iter_mut()
            .find(|dispatch| dispatch.table_id == table_id && dispatch.owner_scope == owner_scope)
        {
            dispatch.record_change();
            return;
        }

        let mut dispatch = FanoutDispatchPlan::new(table_id, owner_scope);
        dispatch.record_change();
        self.notifications.push(dispatch);
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.notifications.is_empty()
            && self.publisher_events == 0
            && self.manifest_updates == 0
    }

    #[inline]
    pub fn side_effects(&self) -> TransactionSideEffects {
        TransactionSideEffects {
            notifications_sent: self.notifications.iter().map(|plan| plan.change_count).sum(),
            manifest_updates: self.manifest_updates,
            publisher_events: self.publisher_events,
        }
    }
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