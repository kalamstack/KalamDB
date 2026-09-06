use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{FunctionRevisionId, SystemTable};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogFunctionRevision,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct FunctionRevisionsTableProvider {
    stores: CatalogStores,
}

impl FunctionRevisionsTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    fn scan_all_revisions(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(
            &self.stores.function_revisions,
            &Self::schema(),
            &CatalogFunctionRevision::definition(),
        )
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.function_revisions,
            &Self::schema(),
            &CatalogFunctionRevision::definition(),
            "revision_id",
            |value| Some(FunctionRevisionId::new(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = FunctionRevisionsTableProvider,
    table_name = SystemTable::FunctionRevisions.table_name(),
    schema = CatalogFunctionRevision::definition()
        .to_arrow_schema()
        .expect("failed to build function_revisions schema")
);

crate::impl_simple_system_table_provider!(
    provider = FunctionRevisionsTableProvider,
    key = FunctionRevisionId,
    value = CatalogFunctionRevision,
    definition = provider_definition,
    scan_all = scan_all_revisions,
    scan_filtered = scan_to_batch_filtered
);
