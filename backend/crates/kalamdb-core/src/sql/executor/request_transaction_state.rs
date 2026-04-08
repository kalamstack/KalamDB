use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use kalamdb_commons::models::{TransactionId, TransactionOrigin};

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::sql::context::ExecutionContext;
use crate::transactions::ExecutionOwnerKey;

#[derive(Debug, Clone)]
pub struct RequestTransactionState {
    owner_key: ExecutionOwnerKey,
    owner_id: Arc<str>,
    active_transaction_id: Option<TransactionId>,
}

impl RequestTransactionState {
    pub fn from_execution_context(
        exec_ctx: &ExecutionContext,
    ) -> Result<Option<Self>, KalamDbError> {
        let Some(request_id) = exec_ctx.request_id() else {
            return Ok(None);
        };

        Ok(Some(Self {
            owner_key: Self::owner_key_for_request_id(request_id),
            owner_id: Arc::<str>::from(format!("sql-req-{}", request_id)),
            active_transaction_id: None,
        }))
    }

    pub fn owner_key_for_request_id(request_id: &str) -> ExecutionOwnerKey {
        let mut hasher = DefaultHasher::new();
        request_id.hash(&mut hasher);
        ExecutionOwnerKey::sql_request(hasher.finish())
    }

    #[inline]
    pub fn owner_key(&self) -> ExecutionOwnerKey {
        self.owner_key
    }

    #[inline]
    pub fn active_transaction_id(&self) -> Option<&TransactionId> {
        self.active_transaction_id.as_ref()
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active_transaction_id.is_some()
    }

    pub fn sync_from_coordinator(&mut self, app_context: &AppContext) {
        self.active_transaction_id = app_context
            .transaction_coordinator()
            .active_for_owner(&self.owner_key);
    }

    pub fn begin(&mut self, app_context: &AppContext) -> Result<TransactionId, KalamDbError> {
        if let Some(transaction_id) = self.active_transaction_id.clone() {
            return Err(KalamDbError::Conflict(format!(
                "request owner '{}' already has an active transaction '{}'",
                self.owner_id, transaction_id
            )));
        }

        let transaction_id = app_context.transaction_coordinator().begin(
            self.owner_key,
            Arc::clone(&self.owner_id),
            TransactionOrigin::SqlBatch,
        )?;
        self.active_transaction_id = Some(transaction_id.clone());
        Ok(transaction_id)
    }

    pub async fn commit(&mut self, app_context: &AppContext) -> Result<TransactionId, KalamDbError> {
        let transaction_id = self.active_transaction_id.clone().ok_or_else(|| {
            KalamDbError::InvalidOperation(
                "COMMIT requires an active explicit SQL transaction".to_string(),
            )
        })?;

        let committed = app_context
            .transaction_coordinator()
            .commit(&transaction_id)
            .await?;
        self.active_transaction_id = None;
        Ok(committed.transaction_id)
    }

    pub fn rollback(&mut self, app_context: &AppContext) -> Result<TransactionId, KalamDbError> {
        let transaction_id = self.active_transaction_id.clone().ok_or_else(|| {
            KalamDbError::InvalidOperation(
                "ROLLBACK requires an active explicit SQL transaction".to_string(),
            )
        })?;

        app_context
            .transaction_coordinator()
            .rollback(&transaction_id)?;
        self.active_transaction_id = None;
        Ok(transaction_id)
    }

    pub fn rollback_if_active(
        &mut self,
        app_context: &AppContext,
    ) -> Result<Option<TransactionId>, KalamDbError> {
        if !self.is_active() {
            return Ok(None);
        }

        self.rollback(app_context).map(Some)
    }
}