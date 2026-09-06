use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{SystemTable, TypeId};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogType,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct TypesTableProvider {
    stores: CatalogStores,
}

impl TypesTableProvider {
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

    pub fn upsert_type(&self, catalog_type: CatalogType) -> Result<(), SystemError> {
        self.stores.upsert_type(catalog_type)
    }

    pub fn get_type(&self, type_id: &TypeId) -> Result<Option<CatalogType>, SystemError> {
        self.stores.get_type(type_id)
    }

    pub fn list_types(&self) -> Result<Vec<CatalogType>, SystemError> {
        self.stores.list_types()
    }

    pub fn drop_type(&self, type_id: &TypeId) -> Result<(), SystemError> {
        self.stores.drop_type(type_id)
    }

    fn scan_all_types(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(&self.stores.types, &Self::schema(), &CatalogType::definition())
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.types,
            &Self::schema(),
            &CatalogType::definition(),
            "type_id",
            |value| Some(TypeId::new(value)),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = TypesTableProvider,
    table_name = SystemTable::Types.table_name(),
    schema = CatalogType::definition()
        .to_arrow_schema()
        .expect("failed to build types schema")
);

crate::impl_simple_system_table_provider!(
    provider = TypesTableProvider,
    key = TypeId,
    value = CatalogType,
    definition = provider_definition,
    scan_all = scan_all_types,
    scan_filtered = scan_to_batch_filtered
);
