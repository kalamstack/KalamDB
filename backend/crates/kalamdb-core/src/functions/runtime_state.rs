//! Staged explicit topic publishes for function transactions.

use std::sync::{Arc, OnceLock};

use kalamdb_commons::models::{TopicId, TransactionId, UserId};
use kalamdb_functions::{EngineConfig, FunctionEngine, FunctionsError};

#[derive(Debug, Clone)]
pub struct StagedTopicPublish {
    pub topic_id: TopicId,
    pub payload:  Vec<u8>,
    pub user_id:  Option<UserId>,
}

#[derive(Default)]
pub struct FunctionRuntimeState {
    engine: OnceLock<Result<Arc<FunctionEngine>, String>>,
    staged: dashmap::DashMap<TransactionId, Vec<StagedTopicPublish>>,
}

impl FunctionRuntimeState {
    pub fn engine(&self) -> Result<Arc<FunctionEngine>, FunctionsError> {
        self.engine
            .get_or_init(|| {
                FunctionEngine::new(EngineConfig::default())
                    .map(Arc::new)
                    .map_err(|e| e.to_string())
            })
            .clone()
            .map_err(FunctionsError::Invalid)
    }

    pub fn stage(&self, transaction_id: TransactionId, publish: StagedTopicPublish) {
        self.staged.entry(transaction_id).or_default().push(publish);
    }

    pub fn take(&self, transaction_id: &TransactionId) -> Vec<StagedTopicPublish> {
        self.staged.remove(transaction_id).map(|(_, rows)| rows).unwrap_or_default()
    }
}
