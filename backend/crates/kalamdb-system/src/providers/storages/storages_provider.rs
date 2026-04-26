//! System.storages table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.storages table.
//! Uses the new EntityStore architecture with StorageId keys.

use std::sync::{Arc, OnceLock};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{models::rows::SystemTableRow, StorageId, SystemTable};
use kalamdb_store::{entity_store::EntityStore, IndexedEntityStore, StorageBackend};

use crate::{
    error::SystemError,
    providers::{
        base::{extract_filter_value, system_rows_to_batch, SimpleProviderDefinition},
        storages::models::Storage,
    },
    system_row_mapper::{model_to_system_row, system_row_to_model},
};

/// System.storages table provider using EntityStore architecture
#[derive(Clone)]
pub struct StoragesTableProvider {
    store: IndexedEntityStore<StorageId, SystemTableRow>,
}

impl StoragesTableProvider {
    /// Create a new storages table provider
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new StoragesTableProvider instance
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            crate::SystemTable::Storages.column_family_name().expect("Storages is a table"),
            Vec::new(),
        );
        Self { store }
    }

    /// Create a new storage entry
    pub fn create_storage(&self, storage: Storage) -> Result<(), SystemError> {
        let row = Self::encode_storage_row(&storage)?;
        self.store.put(&storage.storage_id, &row)?;
        Ok(())
    }

    /// Alias for create_storage (for backward compatibility)
    pub fn insert_storage(&self, storage: Storage) -> Result<(), SystemError> {
        self.create_storage(storage)
    }

    /// Get a storage by ID
    pub fn get_storage_by_id(
        &self,
        storage_id: &StorageId,
    ) -> Result<Option<Storage>, SystemError> {
        let row = self.store.get(storage_id)?;
        row.map(|value| Self::decode_storage_row(&value)).transpose()
    }

    /// Alias for get_storage_by_id (for backward compatibility)
    pub fn get_storage(&self, storage_id: &StorageId) -> Result<Option<Storage>, SystemError> {
        self.get_storage_by_id(storage_id)
    }

    /// Update an existing storage entry
    pub fn update_storage(&self, storage: Storage) -> Result<(), SystemError> {
        // Check if storage exists
        if self.store.get(&storage.storage_id)?.is_none() {
            return Err(SystemError::NotFound(format!(
                "Storage not found: {}",
                storage.storage_id
            )));
        }

        let row = Self::encode_storage_row(&storage)?;
        self.store.put(&storage.storage_id, &row)?;
        Ok(())
    }

    /// Delete a storage entry
    pub fn delete_storage(&self, storage_id: &StorageId) -> Result<(), SystemError> {
        self.store.delete(storage_id)?;
        Ok(())
    }

    /// List all storages
    pub fn list_storages(&self) -> Result<Vec<Storage>, SystemError> {
        let iter = self.store.scan_iterator(None, None)?;
        let mut storages = Vec::new();
        for item in iter {
            let (_, row) = item?;
            storages.push(Self::decode_storage_row(&row)?);
        }
        Ok(storages)
    }

    /// Scan all storages and return as RecordBatch
    pub fn scan_all_storages(&self) -> Result<RecordBatch, SystemError> {
        let rows = self
            .store
            .scan_all_typed(None, None, None)?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        system_rows_to_batch(&Self::schema(), rows)
    }
    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        // Check for primary key equality filter → O(1) point lookup
        if let Some(storage_id_str) = extract_filter_value(filters, "storage_id") {
            let storage_id = StorageId::new(&storage_id_str);
            if let Some(row) = self.store.get(&storage_id)? {
                return system_rows_to_batch(&Self::schema(), vec![row]);
            }
            return system_rows_to_batch(&Self::schema(), vec![]);
        }

        // With limit: use iterator with early termination (skip sort)
        if let Some(lim) = limit {
            let iter = self.store.scan_iterator(None, None)?;
            let mut rows = Vec::with_capacity(lim.min(1000));
            for item in iter {
                let (_, row) = item?;
                rows.push(row);
                if rows.len() >= lim {
                    break;
                }
            }
            return system_rows_to_batch(&Self::schema(), rows);
        }

        // No limit: full scan with sort (default behavior)
        self.scan_all_storages()
    }

    fn encode_storage_row(storage: &Storage) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(storage, &Storage::definition())
    }

    fn decode_storage_row(row: &SystemTableRow) -> Result<Storage, SystemError> {
        system_row_to_model(row, &Storage::definition())
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = StoragesTableProvider,
    table_name = SystemTable::Storages.table_name(),
    schema = Storage::definition()
        .to_arrow_schema()
        .expect("failed to build storages schema")
);

crate::impl_simple_system_table_provider!(
    provider = StoragesTableProvider,
    key = StorageId,
    value = SystemTableRow,
    definition = provider_definition,
    scan_all = scan_all_storages,
    scan_filtered = scan_to_batch_filtered
);

#[cfg(test)]
mod tests {
    use datafusion::datasource::TableProvider;
    use kalamdb_store::test_utils::InMemoryBackend;

    use super::*;
    use crate::StorageType;

    fn create_test_provider() -> StoragesTableProvider {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        StoragesTableProvider::new(backend)
    }

    fn create_test_storage(storage_id: &str, name: &str) -> Storage {
        Storage {
            storage_id: StorageId::new(storage_id),
            storage_name: name.to_string(),
            description: Some("Test storage".to_string()),
            storage_type: StorageType::Filesystem,
            base_directory: "/data".to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: "{base}/shared/{namespace}/{table}".to_string(),
            user_tables_template: "{base}/user/{namespace}/{table}/{user_id}".to_string(),
            created_at: 1000,
            updated_at: 1000,
        }
    }

    #[test]
    fn test_create_and_get_storage() {
        let provider = create_test_provider();
        let storage = create_test_storage("local", "Local Storage");

        provider.create_storage(storage.clone()).unwrap();

        let storage_id = StorageId::local();
        let retrieved = provider.get_storage(&storage_id).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.storage_id, storage_id);
        assert_eq!(retrieved.storage_name, "Local Storage");
    }

    #[test]
    fn test_update_storage() {
        let provider = create_test_provider();
        let mut storage = create_test_storage("local", "Local Storage");
        provider.create_storage(storage.clone()).unwrap();

        // Update
        storage.description = Some("Updated description".to_string());
        provider.update_storage(storage.clone()).unwrap();

        // Verify
        let storage_id = StorageId::local();
        let retrieved = provider.get_storage(&storage_id).unwrap().unwrap();
        assert_eq!(retrieved.description, Some("Updated description".to_string()));
    }

    #[test]
    fn test_delete_storage() {
        let provider = create_test_provider();
        let storage = create_test_storage("local", "Local Storage");

        provider.create_storage(storage).unwrap();

        let storage_id = StorageId::local();
        provider.delete_storage(&storage_id).unwrap();

        let retrieved = provider.get_storage(&storage_id).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_list_storages() {
        let provider = create_test_provider();

        // Insert multiple storages
        for i in 1..=3 {
            let storage = create_test_storage(&format!("storage{}", i), &format!("Storage {}", i));
            provider.create_storage(storage).unwrap();
        }

        // List
        let storages = provider.list_storages().unwrap();
        assert_eq!(storages.len(), 3);
    }

    #[test]
    fn test_scan_all_storages() {
        let provider = create_test_provider();

        // Insert test data
        let storage = create_test_storage("local", "Local Storage");
        provider.create_storage(storage).unwrap();

        // Scan
        let batch = provider.scan_all_storages().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11); // storage_id, storage_name, description, storage_type,
                                             // base_directory, credentials, config_json,
                                             // shared_tables_template, user_tables_template,
                                             // created_at, updated_at
    }

    #[tokio::test]
    async fn test_datafusion_scan() {
        let provider = create_test_provider();

        // Insert test data
        let storage = create_test_storage("local", "Local Storage");
        provider.create_storage(storage).unwrap();

        // Create DataFusion session
        let ctx = datafusion::execution::context::SessionContext::new();
        let state = ctx.state();

        // Scan via DataFusion
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        assert!(!plan.schema().fields().is_empty());
    }
}
