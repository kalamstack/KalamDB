//! System.schemas table provider
//!
//! Phase 16: Consolidated provider using single store with TableVersionId keys.
//! Exposes all table versions with is_latest flag for schema history queries.

use super::{new_schemas_store, schemas_arrow_schema, SchemasStore};
use crate::error::{SystemError, SystemResultExt};
use crate::providers::base::{extract_filter_value, SimpleProviderDefinition};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::Expr;
use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::{TableDefinition, TableOptions};
use kalamdb_store::StorageBackend;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// System.tables table provider using consolidated store with versioning
#[derive(Clone)]
pub struct SchemasTableProvider {
    store: SchemasStore,
}

impl SchemasTableProvider {
    /// Create a new tables table provider
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new SchemasTableProvider instance
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            store: new_schemas_store(backend),
        }
    }

    /// Create a new table entry (stores version 1)
    pub fn create_table(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<(), SystemError> {
        Ok(self.store.put_version(table_id, table_def)?)
    }

    /// Update a table (stores new version and updates latest pointer)
    pub fn update_table(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<(), SystemError> {
        // Check if table exists
        if self.store.get_latest(table_id)?.is_none() {
            return Err(SystemError::NotFound(format!("Table not found: {}", table_id)));
        }

        Ok(self.store.put_version(table_id, table_def)?)
    }

    /// Delete a table entry (removes all versions)
    pub fn delete_table(&self, table_id: &TableId) -> Result<(), SystemError> {
        self.store.delete_all_versions(table_id)?;
        Ok(())
    }

    /// Store a versioned schema entry (alias for put_version)
    pub fn put_versioned_schema(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<(), SystemError> {
        Ok(self.store.put_version(table_id, table_def)?)
    }

    /// Idempotent upsert of the latest schema definition.
    ///
    /// Returns `Ok(true)` when a new version is persisted and `Ok(false)` when
    /// the incoming definition is byte-for-byte identical to the latest version.
    pub fn upsert_table_version(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<bool, SystemError> {
        match self.store.get_latest(table_id)? {
            Some(existing) if existing == *table_def => Ok(false),
            _ => {
                self.store.put_version(table_id, table_def)?;
                Ok(true)
            },
        }
    }

    /// Get the latest version of a table by ID
    pub fn get_table_by_id(
        &self,
        table_id: &TableId,
    ) -> Result<Option<TableDefinition>, SystemError> {
        Ok(self.store.get_latest(table_id)?)
    }

    /// Get a specific version of a table definition
    ///
    /// Used for schema evolution when reading flushed Parquet files.
    pub fn get_version(
        &self,
        table_id: &TableId,
        version: u32,
    ) -> Result<Option<TableDefinition>, SystemError> {
        Ok(self.store.get_version(table_id, version)?)
    }

    /// Async version of `get_table_by_id()`
    pub async fn get_table_by_id_async(
        &self,
        table_id: &TableId,
    ) -> Result<Option<TableDefinition>, SystemError> {
        let table_id = table_id.clone();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.get_latest(&table_id))
            .await
            .into_system_error("spawn_blocking error")?
            .map_err(SystemError::from)
    }

    /// List all latest table definitions
    pub fn list_tables(&self) -> Result<Vec<TableDefinition>, SystemError> {
        let tables = self.store.scan_all_latest()?;
        Ok(tables.into_iter().map(|(_, def)| def).collect())
    }

    /// List all tables in a specific namespace (latest versions only)
    pub fn list_tables_in_namespace(
        &self,
        namespace_id: &kalamdb_commons::models::NamespaceId,
    ) -> Result<Vec<TableDefinition>, SystemError> {
        let tables = self.store.scan_namespace(namespace_id)?;
        Ok(tables.into_iter().map(|(_, def)| def).collect())
    }

    /// Alias for list_tables (backward compatibility)
    pub fn scan_all(&self) -> Result<Vec<TableDefinition>, SystemError> {
        self.list_tables()
    }

    /// Scan all tables and return as RecordBatch (all schema versions)
    ///
    /// Returns all schema versions for schema history queries.
    /// Each ALTER TABLE creates a new version entry with an incremented schema_version.
    /// The `is_latest` column indicates which version is the current active schema.
    pub fn scan_all_tables(&self) -> Result<RecordBatch, SystemError> {
        // Return ALL versions including historical ones for schema evolution support.
        // The store contains both `<lat>` pointers and `<ver>N` rows; we expose only versioned rows.
        let entries = self.store.scan_all_with_versions()?;

        // First pass: find max version for each table.
        let mut max_versions: HashMap<TableId, u32> = HashMap::new();
        for (version_key, table_def, _) in &entries {
            if version_key.is_latest() {
                continue;
            }
            let table_id = version_key.table_id().clone();
            let version = table_def.schema_version;
            max_versions
                .entry(table_id)
                .and_modify(|max| {
                    if version > *max {
                        *max = version;
                    }
                })
                .or_insert(version);
        }

        let table_rows: Vec<(TableDefinition, bool)> = entries
            .into_iter()
            .filter_map(|(version_key, table_def, _)| {
                if version_key.is_latest() {
                    return None;
                }
                let max_version = max_versions
                    .get(version_key.table_id())
                    .copied()
                    .unwrap_or(table_def.schema_version);
                let is_latest = table_def.schema_version == max_version;
                Some((table_def, is_latest))
            })
            .collect();

        self.build_table_def_batch(table_rows)
    }

    /// Build a RecordBatch with all versions for a specific table.
    ///
    /// Much cheaper than `scan_all_tables()` when querying a single table.
    fn build_versions_batch_for_table(
        &self,
        table_id: &TableId,
    ) -> Result<RecordBatch, SystemError> {
        let versions = self.store.list_versions(table_id)?;
        if versions.is_empty() {
            return Ok(RecordBatch::new_empty(schemas_arrow_schema()));
        }

        let max_version = versions.iter().map(|(v, _)| *v).max().unwrap_or(0);
        self.build_table_def_batch(
            versions.into_iter().map(|(v, def)| (def, v == max_version)).collect(),
        )
    }

    /// Build a RecordBatch with latest versions for a specific namespace.
    fn build_versions_batch_for_namespace(
        &self,
        namespace_id: &kalamdb_commons::NamespaceId,
    ) -> Result<RecordBatch, SystemError> {
        let versions = self.store.scan_namespace_with_versions(namespace_id)?;
        if versions.is_empty() {
            return Ok(RecordBatch::new_empty(schemas_arrow_schema()));
        }

        let mut max_versions: HashMap<TableId, u32> = HashMap::new();
        for (table_id, table_def) in &versions {
            max_versions
                .entry(table_id.clone())
                .and_modify(|max| {
                    if table_def.schema_version > *max {
                        *max = table_def.schema_version;
                    }
                })
                .or_insert(table_def.schema_version);
        }

        self.build_table_def_batch(
            versions
                .into_iter()
                .map(|(table_id, def)| {
                    let is_latest = max_versions
                        .get(&table_id)
                        .copied()
                        .unwrap_or(def.schema_version)
                        == def.schema_version;
                    (def, is_latest)
                })
                .collect(),
        )
    }

    /// Build a RecordBatch from a vec of (TableDefinition, is_latest) pairs.
    fn build_table_def_batch(
        &self,
        entries: Vec<(TableDefinition, bool)>,
    ) -> Result<RecordBatch, SystemError> {
        crate::build_record_batch!(
            schema: schemas_arrow_schema(),
            entries: entries,
            columns: [
                table_ids => OptionalString(|entry| Some(format!(
                    "{}:{}",
                    entry.0.namespace_id.as_str(),
                    entry.0.table_name.as_str()
                ))),
                table_names => OptionalString(|entry| Some(entry.0.table_name.as_str())),
                namespaces => OptionalString(|entry| Some(entry.0.namespace_id.as_str())),
                table_types => OptionalString(|entry| Some(entry.0.table_type.as_str())),
                created_ats => Timestamp(|entry| Some(entry.0.created_at.timestamp_millis())),
                schema_versions => OptionalInt32(|entry| Some(entry.0.schema_version as i32)),
                columns_json => OptionalString(|entry| Some(match serde_json::to_string(&entry.0.columns) {
                    Ok(json) => json,
                    Err(e) => format!("{{\"error\":\"failed to serialize columns: {}\"}}", e),
                })),
                table_comments => OptionalString(|entry| entry.0.table_comment.as_deref()),
                updated_ats => Timestamp(|entry| Some(entry.0.updated_at.timestamp_millis())),
                options_json => OptionalString(|entry| Some(match serde_json::to_string(&entry.0.table_options) {
                    Ok(json) => json,
                    Err(e) => format!("{{\"error\":\"failed to serialize options: {}\"}}", e),
                })),
                access_levels => OptionalString(|entry| match &entry.0.table_options {
                    TableOptions::Shared(opts) => opts.access_level.as_ref().map(|a| a.as_str()),
                    _ => None,
                }),
                is_latest_flags => OptionalBoolean(|entry| Some(entry.1)),
                storage_ids => OptionalString(|entry| match &entry.0.table_options {
                    TableOptions::User(opts) => Some(opts.storage_id.as_str()),
                    TableOptions::Shared(opts) => Some(opts.storage_id.as_str()),
                    TableOptions::Stream(_) => Some("local"),
                    TableOptions::System(_) => Some("local"),
                }),
                use_user_storage_flags => OptionalBoolean(|entry| match &entry.0.table_options {
                    TableOptions::User(opts) => Some(opts.use_user_storage),
                    _ => None,
                })
            ]
        )
        .into_arrow_error("Failed to create RecordBatch")
    }
    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        // Check for table_id equality filter → use get_latest for O(1) lookup
        if let Some(table_id_str) = extract_filter_value(filters, "table_id") {
            // table_id format is "namespace:table_name"
            if let Some((ns, tbl)) = table_id_str.split_once(':') {
                let table_id = TableId::new(
                    kalamdb_commons::NamespaceId::new(ns),
                    kalamdb_commons::TableName::new(tbl),
                );
                // Return all versions for this specific table
                return self.build_versions_batch_for_table(&table_id);
            }
        }

        if let (Some(ns_str), Some(table_name)) = (
            extract_filter_value(filters, "namespace_id"),
            extract_filter_value(filters, "table_name"),
        ) {
            let table_id = TableId::new(
                kalamdb_commons::NamespaceId::new(&ns_str),
                kalamdb_commons::TableName::new(&table_name),
            );
            return self.build_versions_batch_for_table(&table_id);
        }

        // Check for namespace equality filter → use namespace-scoped history scan
        if let Some(ns_str) = extract_filter_value(filters, "namespace_id") {
            let namespace_id = kalamdb_commons::NamespaceId::new(&ns_str);
            return self.build_versions_batch_for_namespace(&namespace_id);
        }

        // Fall back to full scan (for LIMIT-only queries, DataFusion will truncate)
        self.scan_all_tables()
    }
}

crate::impl_system_table_provider_metadata!(
    simple,
    provider = SchemasTableProvider,
    table_name = kalamdb_commons::SystemTable::Schemas.table_name(),
    schema = schemas_arrow_schema()
);

crate::impl_simple_system_table_provider!(
    provider = SchemasTableProvider,
    key = TableId,
    value = TableDefinition,
    definition = provider_definition,
    scan_all = scan_all_tables,
    scan_filtered = scan_to_batch_filtered
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{col, lit};
    use kalamdb_commons::datatypes::KalamDataType;
    use kalamdb_commons::schemas::{
        ColumnDefinition, TableDefinition, TableOptions, TableType as KalamTableType,
    };
    use kalamdb_commons::{NamespaceId, TableId, TableName};
    use kalamdb_store::test_utils::InMemoryBackend;

    fn create_test_provider() -> SchemasTableProvider {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        SchemasTableProvider::new(backend)
    }

    fn create_test_table(namespace: &str, table_name: &str) -> (TableId, TableDefinition) {
        let namespace_id = NamespaceId::new(namespace);
        let table_name_id = TableName::new(table_name);
        let table_id = TableId::new(namespace_id.clone(), table_name_id.clone());

        let columns = vec![
            ColumnDefinition::new(
                1,
                "id",
                1,
                KalamDataType::Uuid,
                false,
                true,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                2,
                "name",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
        ];

        let table_def = TableDefinition::new(
            namespace_id,
            table_name_id,
            KalamTableType::User,
            columns,
            TableOptions::user(),
            None,
        )
        .expect("Failed to create table definition");

        (table_id, table_def)
    }

    #[test]
    fn test_create_and_get_table() {
        let provider = create_test_provider();
        let (table_id, table_def) = create_test_table("default", "conversations");

        provider.create_table(&table_id, &table_def).unwrap();

        let retrieved = provider.get_table_by_id(&table_id).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.namespace_id.as_str(), "default");
        assert_eq!(retrieved.table_name.as_str(), "conversations");
    }

    #[test]
    fn test_update_table() {
        let provider = create_test_provider();
        let (table_id, mut table_def) = create_test_table("default", "conversations");
        provider.create_table(&table_id, &table_def).unwrap();

        // Update
        table_def.schema_version = 2;
        provider.update_table(&table_id, &table_def).unwrap();

        // Verify latest version
        let retrieved = provider.get_table_by_id(&table_id).unwrap().unwrap();
        assert_eq!(retrieved.schema_version, 2);
    }

    #[test]
    fn test_delete_table() {
        let provider = create_test_provider();
        let (table_id, table_def) = create_test_table("default", "conversations");

        provider.create_table(&table_id, &table_def).unwrap();
        provider.delete_table(&table_id).unwrap();

        let retrieved = provider.get_table_by_id(&table_id).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_scan_all_tables() {
        let provider = create_test_provider();

        // Insert multiple tables
        for i in 1..=3 {
            let (table_id, table_def) = create_test_table("default", &format!("table{}", i));
            provider.create_table(&table_id, &table_def).unwrap();
        }

        // Scan - should have 3 rows (3 versioned entries, lat pointers are skipped)
        let batch = provider.scan_all_tables().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 14); // All 14 columns from system.tables definition
    }

    #[test]
    fn test_scan_all_tables_with_versions() {
        let provider = create_test_provider();

        // Create table with multiple versions
        let (table_id, mut table_def) = create_test_table("default", "users");
        provider.create_table(&table_id, &table_def).unwrap();

        table_def.schema_version = 2;
        provider.update_table(&table_id, &table_def).unwrap();

        // Scan - should have 2 rows (2 versioned entries, lat pointers are skipped)
        let batch = provider.scan_all_tables().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_scan_to_batch_filtered_preserves_history_for_namespace_and_table_filters() {
        let provider = create_test_provider();

        let (table_id, mut table_def) = create_test_table("default", "users");
        provider.create_table(&table_id, &table_def).unwrap();

        table_def.schema_version = 2;
        provider.update_table(&table_id, &table_def).unwrap();

        let batch = provider
            .scan_to_batch_filtered(
                &[
                    col("namespace_id").eq(lit("default")),
                    col("table_name").eq(lit("users")),
                ],
                None,
            )
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_build_versions_batch_for_namespace_marks_only_latest_version() {
        let provider = create_test_provider();

        let (table_id, mut table_def) = create_test_table("default", "users");
        provider.create_table(&table_id, &table_def).unwrap();

        table_def.schema_version = 2;
        provider.update_table(&table_id, &table_def).unwrap();

        let batch = provider
            .build_versions_batch_for_namespace(table_id.namespace_id())
            .unwrap();

        assert_eq!(batch.num_rows(), 2);

        let is_latest = batch
            .column_by_name("is_latest")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(!is_latest.value(0));
        assert!(is_latest.value(1));
    }

    #[tokio::test]
    async fn test_datafusion_scan() {
        let provider = create_test_provider();

        // Insert test data
        let (table_id, table_def) = create_test_table("default", "conversations");
        provider.create_table(&table_id, &table_def).unwrap();

        // Create DataFusion session
        let ctx = datafusion::execution::context::SessionContext::new();
        let state = ctx.state();

        // Scan via DataFusion
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        assert!(!plan.schema().fields().is_empty());
    }
}
