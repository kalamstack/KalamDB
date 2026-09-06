use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{ArtifactId, SystemTable};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogFunctionArtifact,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct FunctionArtifactsTableProvider {
    stores: CatalogStores,
}

impl FunctionArtifactsTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    fn scan_all_artifacts(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(
            &self.stores.function_artifacts,
            &Self::schema(),
            &CatalogFunctionArtifact::definition(),
        )
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.function_artifacts,
            &Self::schema(),
            &CatalogFunctionArtifact::definition(),
            "artifact_id",
            |value| Some(ArtifactId::new(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = FunctionArtifactsTableProvider,
    table_name = SystemTable::FunctionArtifacts.table_name(),
    schema = CatalogFunctionArtifact::definition()
        .to_arrow_schema()
        .expect("failed to build function_artifacts schema")
);

crate::impl_simple_system_table_provider!(
    provider = FunctionArtifactsTableProvider,
    key = ArtifactId,
    value = CatalogFunctionArtifact,
    definition = provider_definition,
    scan_all = scan_all_artifacts,
    scan_filtered = scan_to_batch_filtered
);
