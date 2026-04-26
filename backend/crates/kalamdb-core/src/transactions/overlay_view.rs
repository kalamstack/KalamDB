use std::sync::Arc;

use kalamdb_commons::{
    models::{rows::Row, OperationKind, TableId, TransactionId, UserId},
    TableType,
};
use kalamdb_transactions::{
    TransactionAccessError, TransactionAccessValidator, TransactionMutationSink,
    TransactionOverlay, TransactionOverlayView,
};

use super::{StagedMutation, TransactionCoordinator};

/// Lightweight query-time overlay handle backed by the live transaction coordinator.
#[derive(Debug)]
pub struct CoordinatorOverlayView {
    coordinator: Arc<TransactionCoordinator>,
    transaction_id: TransactionId,
}

impl CoordinatorOverlayView {
    #[inline]
    pub fn new(coordinator: Arc<TransactionCoordinator>, transaction_id: TransactionId) -> Self {
        Self {
            coordinator,
            transaction_id,
        }
    }
}

impl TransactionOverlayView for CoordinatorOverlayView {
    fn overlay(&self) -> TransactionOverlay {
        self.coordinator
            .get_overlay(&self.transaction_id)
            .unwrap_or_else(|| TransactionOverlay::new(self.transaction_id.clone()))
    }

    fn overlay_for_table(&self, table_id: &TableId) -> Option<TransactionOverlay> {
        self.overlay().overlay_for_table(table_id)
    }
}

#[derive(Debug)]
pub struct CoordinatorMutationSink {
    coordinator: Arc<TransactionCoordinator>,
}

impl CoordinatorMutationSink {
    #[inline]
    pub fn new(coordinator: Arc<TransactionCoordinator>) -> Self {
        Self { coordinator }
    }
}

impl TransactionMutationSink for CoordinatorMutationSink {
    fn stage_mutation(
        &self,
        transaction_id: &TransactionId,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<UserId>,
        operation_kind: OperationKind,
        primary_key: String,
        row: Row,
        is_deleted: bool,
    ) -> Result<(), TransactionAccessError> {
        self.coordinator
            .stage(
                transaction_id,
                StagedMutation::new(
                    transaction_id.clone(),
                    table_id.clone(),
                    table_type,
                    user_id,
                    operation_kind,
                    primary_key,
                    row,
                    is_deleted,
                ),
            )
            .map_err(map_transaction_access_error)
    }

    fn stage_batch(
        &self,
        transaction_id: &TransactionId,
        mutations: Vec<StagedMutation>,
    ) -> Result<(), TransactionAccessError> {
        self.coordinator
            .stage_batch(transaction_id, mutations)
            .map_err(map_transaction_access_error)
    }
}

#[derive(Debug)]
pub struct CoordinatorAccessValidator {
    coordinator: Arc<TransactionCoordinator>,
}

impl CoordinatorAccessValidator {
    #[inline]
    pub fn new(coordinator: Arc<TransactionCoordinator>) -> Self {
        Self { coordinator }
    }
}

impl TransactionAccessValidator for CoordinatorAccessValidator {
    fn validate_table_access(
        &self,
        transaction_id: &TransactionId,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<&UserId>,
    ) -> Result<(), TransactionAccessError> {
        self.coordinator
            .validate_table_access(transaction_id, table_id, table_type, user_id)
            .map_err(map_transaction_access_error)
    }
}

fn map_transaction_access_error(error: crate::error::KalamDbError) -> TransactionAccessError {
    match error {
        crate::error::KalamDbError::NotLeader { leader_addr } => {
            TransactionAccessError::NotLeader { leader_addr }
        },
        other => TransactionAccessError::invalid_operation(other.to_string()),
    }
}
