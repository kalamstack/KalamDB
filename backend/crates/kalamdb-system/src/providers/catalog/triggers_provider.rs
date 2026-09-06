use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{SystemTable, TriggerId};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogTrigger,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct TriggersTableProvider {
    stores: CatalogStores,
}

impl TriggersTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    fn scan_all_triggers(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(&self.stores.triggers, &Self::schema(), &CatalogTrigger::definition())
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.triggers,
            &Self::schema(),
            &CatalogTrigger::definition(),
            "trigger_id",
            |value| Some(TriggerId::new(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = TriggersTableProvider,
    table_name = SystemTable::Triggers.table_name(),
    schema = CatalogTrigger::definition()
        .to_arrow_schema()
        .expect("failed to build triggers schema")
);

crate::impl_simple_system_table_provider!(
    provider = TriggersTableProvider,
    key = TriggerId,
    value = CatalogTrigger,
    definition = provider_definition,
    scan_all = scan_all_triggers,
    scan_filtered = scan_to_batch_filtered
);
