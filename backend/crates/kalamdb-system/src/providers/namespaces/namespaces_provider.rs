//! System.namespaces table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.namespaces table.
//! Uses the new EntityStore architecture with NamespaceId keys.

use crate::error::{SystemError, SystemResultExt};
use crate::providers::base::{
    extract_filter_value, system_rows_to_batch, SimpleProviderDefinition,
};
use crate::providers::namespaces::models::Namespace;
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::NamespaceId;
use kalamdb_commons::SystemTable;
use kalamdb_store::entity_store::{EntityStore, EntityStoreAsync};
use kalamdb_store::{IndexedEntityStore, StorageBackend};
use std::sync::{Arc, OnceLock};

/// System.namespaces table provider using EntityStore architecture
pub struct NamespacesTableProvider {
    store: IndexedEntityStore<NamespaceId, SystemTableRow>,
}

impl std::fmt::Debug for NamespacesTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespacesTableProvider").finish()
    }
}

impl NamespacesTableProvider {
    /// Create a new namespaces table provider
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new NamespacesTableProvider instance
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            crate::SystemTable::Namespaces
                .column_family_name()
                .expect("Namespaces is a table"),
            Vec::new(),
        );
        Self { store }
    }

    /// Create a new namespace entry
    pub fn create_namespace(&self, namespace: Namespace) -> Result<(), SystemError> {
        let row = Self::encode_namespace_row(&namespace)?;
        self.store.put(&namespace.namespace_id, &row)?;
        Ok(())
    }

    /// Alias for create_namespace (for backward compatibility)
    pub fn insert_namespace(&self, namespace: Namespace) -> Result<(), SystemError> {
        self.create_namespace(namespace)
    }

    /// Async version of `create_namespace()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn create_namespace_async(&self, namespace: Namespace) -> Result<(), SystemError> {
        let row = Self::encode_namespace_row(&namespace)?;
        self.store
            .put_async(&namespace.namespace_id, &row)
            .await
            .into_system_error("put_async error")?;
        Ok(())
    }

    /// Get a namespace by ID
    pub fn get_namespace_by_id(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<Namespace>, SystemError> {
        let row = self.store.get(namespace_id)?;
        row.map(|value| Self::decode_namespace_row(&value)).transpose()
    }

    /// Alias for get_namespace_by_id (for backward compatibility)
    pub fn get_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<Namespace>, SystemError> {
        self.get_namespace_by_id(namespace_id)
    }

    /// Async version of `get_namespace()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn get_namespace_async(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<Namespace>, SystemError> {
        let row = self
            .store
            .get_async(namespace_id.clone())
            .await
            .into_system_error("get_async error")?;
        row.map(|value| Self::decode_namespace_row(&value)).transpose()
    }

    /// Update an existing namespace entry
    pub fn update_namespace(&self, namespace: Namespace) -> Result<(), SystemError> {
        // Check if namespace exists
        if self.store.get(&namespace.namespace_id)?.is_none() {
            return Err(SystemError::NotFound(format!(
                "Namespace not found: {}",
                namespace.namespace_id
            )));
        }

        let row = Self::encode_namespace_row(&namespace)?;
        self.store.put(&namespace.namespace_id, &row)?;
        Ok(())
    }

    /// Async version of `update_namespace()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn update_namespace_async(&self, namespace: Namespace) -> Result<(), SystemError> {
        // Check if namespace exists
        if self.store.get_async(namespace.namespace_id.clone()).await?.is_none() {
            return Err(SystemError::NotFound(format!(
                "Namespace not found: {}",
                namespace.namespace_id
            )));
        }

        let row = Self::encode_namespace_row(&namespace)?;
        self.store
            .put_async(&namespace.namespace_id, &row)
            .await
            .into_system_error("put_async error")?;
        Ok(())
    }

    /// Delete a namespace entry
    pub fn delete_namespace(&self, namespace_id: &NamespaceId) -> Result<(), SystemError> {
        self.store.delete(namespace_id)?;
        Ok(())
    }

    /// Async version of `delete_namespace()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn delete_namespace_async(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(), SystemError> {
        self.store
            .delete_async(namespace_id.clone())
            .await
            .into_system_error("delete_async error")?;
        Ok(())
    }

    /// List all namespaces
    pub fn list_namespaces(&self) -> Result<Vec<Namespace>, SystemError> {
        let iter = self.store.scan_iterator(None, None)?;
        let mut namespaces = Vec::new();
        for item in iter {
            let (_, row) = item?;
            namespaces.push(Self::decode_namespace_row(&row)?);
        }
        Ok(namespaces)
    }

    /// Async version of `list_namespaces()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn list_namespaces_async(&self) -> Result<Vec<Namespace>, SystemError> {
        let results: Vec<(Vec<u8>, SystemTableRow)> = self
            .store
            .scan_all_async(None, None, None)
            .await
            .into_system_error("scan_all_async error")?;
        results.into_iter().map(|(_, row)| Self::decode_namespace_row(&row)).collect()
    }

    /// Alias for list_namespaces (backward compatibility)
    pub fn scan_all(&self) -> Result<Vec<Namespace>, SystemError> {
        self.list_namespaces()
    }

    /// Async version of `scan_all()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn scan_all_async(&self) -> Result<Vec<Namespace>, SystemError> {
        let results: Vec<(Vec<u8>, SystemTableRow)> = self
            .store
            .scan_all_async(None, None, None)
            .await
            .into_system_error("scan_all_async error")?;
        results.into_iter().map(|(_, row)| Self::decode_namespace_row(&row)).collect()
    }

    /// Scan all namespaces and return as RecordBatch
    pub fn scan_all_namespaces(&self) -> Result<RecordBatch, SystemError> {
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
        if let Some(ns_id_str) = extract_filter_value(filters, "namespace_id") {
            let namespace_id = NamespaceId::new(&ns_id_str);
            if let Some(row) = self.store.get(&namespace_id)? {
                return system_rows_to_batch(&Self::schema(), vec![row]);
            }
            return system_rows_to_batch(&Self::schema(), vec![]);
        }

        // Use iterator with early termination on limit
        let iter = self.store.scan_iterator(None, None)?;
        let effective_limit = limit.unwrap_or(100_000);
        let mut entries = Vec::with_capacity(effective_limit.min(1000));
        for item in iter {
            let (_, row) = item?;
            entries.push(row);
            if entries.len() >= effective_limit {
                break;
            }
        }
        system_rows_to_batch(&Self::schema(), entries)
    }

    fn provider_definition() -> SimpleProviderDefinition {
        SimpleProviderDefinition {
            table_name: SystemTable::Namespaces.table_name(),
            schema: Self::schema,
        }
    }

    fn encode_namespace_row(namespace: &Namespace) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(namespace, &Namespace::definition())
    }

    fn decode_namespace_row(row: &SystemTableRow) -> Result<Namespace, SystemError> {
        system_row_to_model(row, &Namespace::definition())
    }

    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Namespace::definition()
                    .to_arrow_schema()
                    .expect("failed to build namespaces schema")
            })
            .clone()
    }
}

crate::impl_simple_system_table_provider!(
    provider = NamespacesTableProvider,
    key = NamespaceId,
    value = SystemTableRow,
    definition = provider_definition,
    scan_all = scan_all_namespaces,
    scan_filtered = scan_to_batch_filtered
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::TableProvider;
    use kalamdb_store::test_utils::InMemoryBackend;

    fn create_test_provider() -> NamespacesTableProvider {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        NamespacesTableProvider::new(backend)
    }

    fn create_test_namespace(namespace_id: &NamespaceId, name: &str) -> Namespace {
        Namespace {
            namespace_id: namespace_id.clone(),
            name: name.to_string(),
            created_at: 1000,
            options: Some(serde_json::json!({})),
            table_count: 0,
        }
    }

    #[test]
    fn test_create_and_get_namespace() {
        let provider = create_test_provider();
        let namespace = create_test_namespace(&NamespaceId::new("app"), "app");

        provider.create_namespace(namespace.clone()).unwrap();

        let namespace_id = NamespaceId::new("app");
        let retrieved = provider.get_namespace(&namespace_id).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.namespace_id, namespace_id);
        assert_eq!(retrieved.name, "app");
    }

    #[test]
    fn test_update_namespace() {
        let provider = create_test_provider();
        let mut namespace = create_test_namespace(&NamespaceId::new("app"), "app");
        provider.create_namespace(namespace.clone()).unwrap();

        // Update
        namespace.table_count = 5;
        provider.update_namespace(namespace.clone()).unwrap();

        // Verify
        let namespace_id = NamespaceId::new("app");
        let retrieved = provider.get_namespace(&namespace_id).unwrap().unwrap();
        assert_eq!(retrieved.table_count, 5);
    }

    #[test]
    fn test_delete_namespace() {
        let provider = create_test_provider();
        let namespace = create_test_namespace(&NamespaceId::new("app"), "app");

        provider.create_namespace(namespace).unwrap();

        let namespace_id = NamespaceId::new("app");
        provider.delete_namespace(&namespace_id).unwrap();

        let retrieved = provider.get_namespace(&namespace_id).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_list_namespaces() {
        let provider = create_test_provider();

        // Insert multiple namespaces
        for i in 1..=3 {
            let namespace = create_test_namespace(
                &NamespaceId::new(&format!("ns{}", i)),
                &format!("namespace{}", i),
            );
            provider.create_namespace(namespace).unwrap();
        }

        // List
        let namespaces = provider.list_namespaces().unwrap();
        assert_eq!(namespaces.len(), 3);
    }

    #[test]
    fn test_scan_all_namespaces() {
        let provider = create_test_provider();

        // Insert test data
        let namespace = create_test_namespace(&NamespaceId::new("app"), "app");
        provider.create_namespace(namespace).unwrap();

        // Scan
        let batch = provider.scan_all_namespaces().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
    }

    #[tokio::test]
    async fn test_datafusion_scan() {
        let provider = create_test_provider();

        // Insert test data
        let namespace = create_test_namespace(&NamespaceId::new("app"), "app");
        provider.create_namespace(namespace).unwrap();

        // Create DataFusion session
        let ctx = datafusion::execution::context::SessionContext::new();
        let state = ctx.state();

        // Scan via DataFusion
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        assert!(!plan.schema().fields().is_empty());
    }
}
