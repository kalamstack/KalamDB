use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use kalamdb_commons::{
    models::schemas::TableDefinition as CommonsTableDefinition, StorageId, TableId,
};
use kalamdb_system::SchemaRegistry as SchemaRegistryTrait;
use kalamdb_tables::TableError;

use crate::{error::KalamDbError, schema_registry::SchemaRegistry};

/// Adapter to expose SchemaRegistry with TableError for kalamdb-tables providers.
pub struct TablesSchemaRegistryAdapter {
    inner: Arc<SchemaRegistry>,
}

impl TablesSchemaRegistryAdapter {
    pub fn new(inner: Arc<SchemaRegistry>) -> Self {
        Self { inner }
    }

    fn map_error(err: KalamDbError) -> TableError {
        match err {
            KalamDbError::AlreadyExists(msg) => TableError::AlreadyExists(msg),
            KalamDbError::NotFound(msg) => TableError::NotFound(msg),
            KalamDbError::TableNotFound(msg) => TableError::TableNotFound(msg),
            KalamDbError::InvalidOperation(msg) => TableError::InvalidOperation(msg),
            KalamDbError::SchemaError(msg) => TableError::SchemaError(msg),
            KalamDbError::SerializationError(msg) => TableError::Serialization(msg),
            other => TableError::Other(other.to_string()),
        }
    }
}

impl SchemaRegistryTrait for TablesSchemaRegistryAdapter {
    type Error = TableError;

    fn get_arrow_schema(&self, table_id: &TableId) -> Result<SchemaRef, Self::Error> {
        self.inner.get_arrow_schema(table_id).map_err(Self::map_error)
    }

    fn get_table_if_exists(
        &self,
        table_id: &TableId,
    ) -> Result<Option<Arc<CommonsTableDefinition>>, Self::Error> {
        self.inner.get_table_if_exists(table_id).map_err(Self::map_error)
    }

    fn get_arrow_schema_for_version(
        &self,
        table_id: &TableId,
        schema_version: u32,
    ) -> Result<SchemaRef, Self::Error> {
        self.inner
            .get_arrow_schema_for_version(table_id, schema_version)
            .map_err(Self::map_error)
    }

    fn get_storage_id(&self, table_id: &TableId) -> Result<StorageId, Self::Error> {
        self.inner.get_storage_id(table_id).map_err(Self::map_error)
    }
}
