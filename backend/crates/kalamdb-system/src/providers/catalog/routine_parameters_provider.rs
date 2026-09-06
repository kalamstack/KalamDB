use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{RoutineId, RoutineParameterId, StorageKey, SystemTable};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogRoutineParameter,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct RoutineParametersTableProvider {
    stores: CatalogStores,
}

impl RoutineParametersTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    pub fn upsert_parameter(&self, parameter: CatalogRoutineParameter) -> Result<(), SystemError> {
        self.stores.upsert_parameter(parameter)
    }

    pub fn list_parameters(
        &self,
        routine_id: &RoutineId,
    ) -> Result<Vec<CatalogRoutineParameter>, SystemError> {
        self.stores.list_parameters(routine_id)
    }

    fn scan_all_parameters(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(
            &self.stores.routine_parameters,
            &Self::schema(),
            &CatalogRoutineParameter::definition(),
        )
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.routine_parameters,
            &Self::schema(),
            &CatalogRoutineParameter::definition(),
            "parameter_id",
            |value| RoutineParameterId::from_storage_key(value.as_bytes()).ok(),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = RoutineParametersTableProvider,
    table_name = SystemTable::RoutineParameters.table_name(),
    schema = CatalogRoutineParameter::definition()
        .to_arrow_schema()
        .expect("failed to build routine_parameters schema")
);

crate::impl_simple_system_table_provider!(
    provider = RoutineParametersTableProvider,
    key = RoutineParameterId,
    value = CatalogRoutineParameter,
    definition = provider_definition,
    scan_all = scan_all_parameters,
    scan_filtered = scan_to_batch_filtered
);
