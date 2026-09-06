use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{StorageKey, SystemTable, TypeFieldId, TypeId};
use kalamdb_store::StorageBackend;

use super::{
    models::CatalogTypeField,
    scan::{scan_all_rows, scan_filtered_rows},
    CatalogStores,
};
use crate::{error::SystemError, providers::base::SimpleProviderDefinition};

#[derive(Clone)]
pub struct TypeFieldsTableProvider {
    stores: CatalogStores,
}

impl TypeFieldsTableProvider {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            stores: CatalogStores::new(backend),
        }
    }

    pub fn from_stores(stores: CatalogStores) -> Self {
        Self { stores }
    }

    pub fn upsert_type_field(&self, field: CatalogTypeField) -> Result<(), SystemError> {
        self.stores.upsert_type_field(field)
    }

    pub fn list_type_fields(&self, type_id: &TypeId) -> Result<Vec<CatalogTypeField>, SystemError> {
        self.stores.list_type_fields(type_id)
    }

    fn scan_all_type_fields(&self) -> Result<RecordBatch, SystemError> {
        scan_all_rows(&self.stores.type_fields, &Self::schema(), &CatalogTypeField::definition())
    }

    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        scan_filtered_rows(
            &self.stores.type_fields,
            &Self::schema(),
            &CatalogTypeField::definition(),
            "type_field_id",
            |value| TypeFieldId::from_storage_key(value.as_bytes()).ok(),
            filters,
            limit,
        )
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = TypeFieldsTableProvider,
    table_name = SystemTable::TypeFields.table_name(),
    schema = CatalogTypeField::definition()
        .to_arrow_schema()
        .expect("failed to build type_fields schema")
);

crate::impl_simple_system_table_provider!(
    provider = TypeFieldsTableProvider,
    key = TypeFieldId,
    value = CatalogTypeField,
    definition = provider_definition,
    scan_all = scan_all_type_fields,
    scan_filtered = scan_to_batch_filtered
);
