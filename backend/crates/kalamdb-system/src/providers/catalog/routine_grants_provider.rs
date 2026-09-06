use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{RoutineGrantId, RoutineId, StorageKey, SystemTable};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogRoutineGrant,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct RoutineGrantsTableProvider {
    stores: CatalogStores,
}

impl RoutineGrantsTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    pub fn upsert_grant(&self, grant: CatalogRoutineGrant) -> Result<(), SystemError> {
        self.stores.upsert_grant(grant)
    }

    pub fn list_grants(
        &self,
        routine_id: &RoutineId,
    ) -> Result<Vec<CatalogRoutineGrant>, SystemError> {
        self.stores.list_grants(routine_id)
    }

    fn scan_all_grants(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(
            &self.stores.routine_grants,
            &Self::schema(),
            &CatalogRoutineGrant::definition(),
        )
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.routine_grants,
            &Self::schema(),
            &CatalogRoutineGrant::definition(),
            "grant_id",
            |value| RoutineGrantId::from_storage_key(value.as_bytes()).ok(),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = RoutineGrantsTableProvider,
    table_name = SystemTable::RoutineGrants.table_name(),
    schema = CatalogRoutineGrant::definition()
        .to_arrow_schema()
        .expect("failed to build routine_grants schema")
);

crate::impl_simple_system_table_provider!(
    provider = RoutineGrantsTableProvider,
    key = RoutineGrantId,
    value = CatalogRoutineGrant,
    definition = provider_definition,
    scan_all = scan_all_grants,
    scan_filtered = scan_to_batch_filtered
);
