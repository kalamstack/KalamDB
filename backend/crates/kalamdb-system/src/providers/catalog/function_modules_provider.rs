use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{FunctionModuleId, SystemTable};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogFunctionModule,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct FunctionModulesTableProvider {
    stores: CatalogStores,
}

impl FunctionModulesTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    fn scan_all_modules(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(
            &self.stores.function_modules,
            &Self::schema(),
            &CatalogFunctionModule::definition(),
        )
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.function_modules,
            &Self::schema(),
            &CatalogFunctionModule::definition(),
            "module_id",
            |value| Some(FunctionModuleId::new(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = FunctionModulesTableProvider,
    table_name = SystemTable::FunctionModules.table_name(),
    schema = CatalogFunctionModule::definition()
        .to_arrow_schema()
        .expect("failed to build function_modules schema")
);

crate::impl_simple_system_table_provider!(
    provider = FunctionModulesTableProvider,
    key = FunctionModuleId,
    value = CatalogFunctionModule,
    definition = provider_definition,
    scan_all = scan_all_modules,
    scan_filtered = scan_to_batch_filtered
);
