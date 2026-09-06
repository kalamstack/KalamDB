use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{RoutineId, SystemTable};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogRoutine,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct RoutinesTableProvider {
    stores: CatalogStores,
}

impl RoutinesTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    pub fn stores(&self) -> &CatalogStores {
        &self.stores
    }

    pub fn upsert_routine(&self, routine: CatalogRoutine) -> Result<(), SystemError> {
        self.stores.upsert_routine(routine)
    }

    pub fn get_routine(
        &self,
        routine_id: &RoutineId,
    ) -> Result<Option<CatalogRoutine>, SystemError> {
        self.stores.get_routine(routine_id)
    }

    pub fn list_routines(&self) -> Result<Vec<CatalogRoutine>, SystemError> {
        self.stores.list_routines()
    }

    pub fn drop_routine(&self, routine_id: &RoutineId) -> Result<(), SystemError> {
        self.stores.drop_routine(routine_id)
    }

    fn scan_all_routines(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(&self.stores.routines, &Self::schema(), &CatalogRoutine::definition())
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.routines,
            &Self::schema(),
            &CatalogRoutine::definition(),
            "routine_id",
            |value| Some(RoutineId::new(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = RoutinesTableProvider,
    table_name = SystemTable::Routines.table_name(),
    schema = CatalogRoutine::definition()
        .to_arrow_schema()
        .expect("failed to build routines schema")
);

crate::impl_simple_system_table_provider!(
    provider = RoutinesTableProvider,
    key = RoutineId,
    value = CatalogRoutine,
    definition = provider_definition,
    scan_all = scan_all_routines,
    scan_filtered = scan_to_batch_filtered
);
