use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{SystemTable, TriggerAttemptId};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogTriggerAttempt,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct TriggerAttemptsTableProvider {
    stores: CatalogStores,
}

impl TriggerAttemptsTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    fn scan_all_attempts(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(
            &self.stores.trigger_attempts,
            &Self::schema(),
            &CatalogTriggerAttempt::definition(),
        )
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.trigger_attempts,
            &Self::schema(),
            &CatalogTriggerAttempt::definition(),
            "attempt_id",
            |value| Some(TriggerAttemptId::from(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = TriggerAttemptsTableProvider,
    table_name = SystemTable::TriggerAttempts.table_name(),
    schema = CatalogTriggerAttempt::definition()
        .to_arrow_schema()
        .expect("failed to build trigger_attempts schema")
);

crate::impl_simple_system_table_provider!(
    provider = TriggerAttemptsTableProvider,
    key = TriggerAttemptId,
    value = CatalogTriggerAttempt,
    definition = provider_definition,
    scan_all = scan_all_attempts,
    scan_filtered = scan_to_batch_filtered
);
