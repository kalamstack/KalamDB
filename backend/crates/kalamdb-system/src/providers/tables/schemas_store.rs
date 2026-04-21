//! Schemas table store implementation
//!
//! Phase 16: Consolidated store using TableVersionId keys.
//! Stores all table versions in a single partition with dual-key pattern:
//! - Latest pointer: "{namespace}:{table}<lat>" -> TableDefinition
//! - Versioned:      "{namespace}:{table}<ver>{version:08}" -> TableDefinition
//!
//! This allows:
//! - O(1) lookup of latest version
//! - Efficient range scans for version history
//! - Single storage partition for simplicity

use crate::SystemTable;
use kalamdb_commons::models::{NamespaceId, TableId, TableVersionId};
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_commons::storage::Partition;
use kalamdb_store::entity_store::{CrossUserTableStore, EntityStore};
use kalamdb_store::StorageBackend;
use std::sync::Arc;

/// Store for `system.schemas` definitions.
///
/// Uses `TableVersionId` keys to support both latest-pointer and historical
/// version entries in the same partition.
#[derive(Clone)]
pub struct SchemasStore {
    backend: Arc<dyn StorageBackend>,
    partition: Partition,
}

impl SchemasStore {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            partition: Partition::new(
                SystemTable::Schemas.column_family_name().expect("Schemas is a table"),
            ),
        }
    }
}

impl EntityStore<TableVersionId, TableDefinition> for SchemasStore {
    #[doc(hidden)]
    fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    fn partition(&self) -> Partition {
        self.partition.clone()
    }
}

impl CrossUserTableStore<TableVersionId, TableDefinition> for SchemasStore {
    fn table_access(&self) -> Option<kalamdb_commons::models::TableAccess> {
        None
    }
}

/// Helper function to create a new schemas table store
///
/// # Arguments
/// * `backend` - Storage backend (RocksDB or mock)
///
/// # Returns
/// A new SystemTableStore instance configured for the schemas table
pub fn new_schemas_store(backend: Arc<dyn StorageBackend>) -> SchemasStore {
    SchemasStore::new(backend)
}

/// Helper methods for SchemasStore specific operations
impl SchemasStore {
    /// Get the latest version of a table definition
    pub fn get_latest(
        &self,
        table_id: &TableId,
    ) -> Result<Option<TableDefinition>, kalamdb_store::StorageError> {
        let latest_key = TableVersionId::latest(table_id.clone());
        self.get(&latest_key)
    }

    /// Get a specific version of a table definition
    pub fn get_version(
        &self,
        table_id: &TableId,
        version: u32,
    ) -> Result<Option<TableDefinition>, kalamdb_store::StorageError> {
        let version_key = TableVersionId::versioned(table_id.clone(), version);
        self.get(&version_key)
    }

    /// Store a new table definition version
    ///
    /// This stores both:
    /// 1. The versioned entry: `{tableId}<ver>{version:08}` -> TableDefinition
    /// 2. Updates the latest pointer: `{tableId}<lat>` -> TableDefinition
    ///
    /// # Arguments
    /// * `table_id` - The table identifier
    /// * `table_def` - The table definition (must have correct schema_version)
    pub fn put_version(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<(), kalamdb_store::StorageError> {
        let version = table_def.schema_version;

        // Store the versioned entry
        let version_key = TableVersionId::versioned(table_id.clone(), version);
        log::debug!(
            "[SchemasStore::put_version] table_id={}, version={}, version_key={}",
            table_id,
            version,
            version_key
        );
        self.put(&version_key, table_def)?;

        // Update the latest pointer
        let latest_key = TableVersionId::latest(table_id.clone());
        log::debug!("[SchemasStore::put_version] table_id={}, latest_key={}", table_id, latest_key);
        self.put(&latest_key, table_def)?;

        Ok(())
    }

    /// Debug: dump all keys in the store matching a table_id prefix
    #[allow(dead_code)]
    pub fn debug_dump_keys_for_table(&self, table_id: &TableId) {
        let prefix_key = TableVersionId::latest(table_id.clone());
        log::debug!("[SchemasStore::debug_dump] Partition: {}", self.partition());
        match self.scan_keys_typed(Some(&prefix_key), None, 1000) {
            Ok(keys) => {
                log::debug!("[SchemasStore::debug_dump] Keys for table_id={}:", table_id);
                for key in &keys {
                    log::debug!("  Key: {:?}", key);
                }
                log::debug!("[SchemasStore::debug_dump] Total keys found: {}", keys.len());
            },
            Err(e) => {
                log::debug!("[SchemasStore::debug_dump] Error scanning: {:?}", e);
            },
        }
    }

    /// Delete all versions of a table (for DROP TABLE)
    ///
    /// Uses a prefix scan on the table's composite key to find and delete
    /// only entries for this table, instead of scanning the entire store.
    pub fn delete_all_versions(
        &self,
        table_id: &TableId,
    ) -> Result<usize, kalamdb_store::StorageError> {
        let prefix = TableVersionId::table_scan_prefix(table_id);
        let entries = self.scan_with_raw_prefix(&prefix, None, 10_000)?;
        let mut deleted_count = 0;
        for (version_key, _) in entries {
            self.delete(&version_key)?;
            deleted_count += 1;
        }
        Ok(deleted_count)
    }

    /// List all versions of a table (for schema history queries)
    ///
    /// Uses a prefix scan on the table's versioned key prefix for O(versions) lookup.
    /// Returns versions in ascending order by version number.
    pub fn list_versions(
        &self,
        table_id: &TableId,
    ) -> Result<Vec<(u32, TableDefinition)>, kalamdb_store::StorageError> {
        let prefix = TableVersionId::version_scan_prefix(table_id);
        let entries = self.scan_with_raw_prefix(&prefix, None, 10_000)?;

        let mut versions: Vec<(u32, TableDefinition)> = entries
            .into_iter()
            .filter_map(|(version_key, table_def)| version_key.version().map(|v| (v, table_def)))
            .collect();

        // Sort by version ascending
        versions.sort_by_key(|(v, _)| *v);

        Ok(versions)
    }

    /// Get the current version number for a table
    pub fn get_current_version(
        &self,
        table_id: &TableId,
    ) -> Result<Option<u32>, kalamdb_store::StorageError> {
        self.get_latest(table_id).map(|opt| opt.map(|def| def.schema_version))
    }

    /// Scan all tables (latest versions only) in a specific namespace.
    ///
    /// Uses a prefix scan on the namespace portion of the key for O(tables-in-ns) lookup.
    pub fn scan_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<(TableId, TableDefinition)>, kalamdb_store::StorageError> {
        let prefix = TableVersionId::namespace_scan_prefix(namespace_id);
        let entries: Vec<(TableVersionId, TableDefinition)> =
            self.scan_with_raw_prefix(&prefix, None, 10_000)?;

        let result: Vec<(TableId, TableDefinition)> = entries
            .into_iter()
            .filter_map(|(version_key, table_def)| {
                // Only include latest entries
                if version_key.is_latest() {
                    Some((version_key.table_id().clone(), table_def))
                } else {
                    None
                }
            })
            .collect();

        Ok(result)
    }

    /// Scan all versioned table definitions in a specific namespace.
    pub fn scan_namespace_with_versions(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Vec<(TableId, TableDefinition)>, kalamdb_store::StorageError> {
        let prefix = TableVersionId::namespace_scan_prefix(namespace_id);
        let entries: Vec<(TableVersionId, TableDefinition)> =
            self.scan_with_raw_prefix(&prefix, None, 10_000)?;

        let mut result: Vec<(TableId, TableDefinition)> = entries
            .into_iter()
            .filter_map(|(version_key, table_def)| {
                if version_key.is_versioned() {
                    Some((version_key.table_id().clone(), table_def))
                } else {
                    None
                }
            })
            .collect();

        result.sort_by(|(left_id, left_def), (right_id, right_def)| {
            left_id
                .namespace_id()
                .as_str()
                .cmp(right_id.namespace_id().as_str())
                .then_with(|| {
                    left_id
                        .table_name()
                        .as_str()
                        .cmp(right_id.table_name().as_str())
                })
                .then_with(|| left_def.schema_version.cmp(&right_def.schema_version))
        });

        Ok(result)
    }

    /// Scan all table entries (both latest and versioned)
    ///
    /// Returns tuples of (TableVersionId, TableDefinition, is_latest)
    pub fn scan_all_with_versions(
        &self,
    ) -> Result<Vec<(TableVersionId, TableDefinition, bool)>, kalamdb_store::StorageError> {
        let entries = self.scan_all_typed(None, None, None)?;

        let result: Vec<(TableVersionId, TableDefinition, bool)> = entries
            .into_iter()
            .map(|(version_key, table_def)| {
                let is_latest = version_key.is_latest();
                (version_key, table_def, is_latest)
            })
            .collect();

        Ok(result)
    }

    /// Scan all latest table definitions
    pub fn scan_all_latest(
        &self,
    ) -> Result<Vec<(TableId, TableDefinition)>, kalamdb_store::StorageError> {
        let entries = self.scan_all_with_versions()?;
        Ok(entries
            .into_iter()
            .filter_map(|(key, def, is_latest)| {
                if is_latest {
                    Some((key.table_id().clone(), def))
                } else {
                    None
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::datatypes::KalamDataType;
    use kalamdb_commons::schemas::{ColumnDefinition, TableDefinition, TableOptions, TableType};
    use kalamdb_commons::{NamespaceId, Role, TableId, TableName};
    use kalamdb_store::test_utils::InMemoryBackend;
    use kalamdb_store::CrossUserTableStore;

    fn create_test_store() -> SchemasStore {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        new_schemas_store(backend)
    }

    fn create_test_table(
        namespace: &str,
        table_name: &str,
        version: u32,
    ) -> (TableId, TableDefinition) {
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

        let mut table_def = TableDefinition::new(
            namespace_id,
            table_name_id,
            TableType::User,
            columns,
            TableOptions::user(),
            None,
        )
        .expect("Failed to create table definition");
        table_def.schema_version = version;

        (table_id, table_def)
    }

    #[test]
    fn test_create_store() {
        let store = create_test_store();
        assert_eq!(
            store.partition(),
            SystemTable::Schemas
                .column_family_name()
                .expect("Schemas is a table, not a view")
                .into()
        );
    }

    #[test]
    fn test_put_and_get_version() {
        let store = create_test_store();
        let (table_id, table_def) = create_test_table("default", "conversations", 1);

        // Put version
        store.put_version(&table_id, &table_def).unwrap();

        // Get latest
        let retrieved = store.get_latest(&table_id).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.namespace_id.as_str(), "default");
        assert_eq!(retrieved.table_name.as_str(), "conversations");
        assert_eq!(retrieved.schema_version, 1);

        // Get specific version
        let specific = store.get_version(&table_id, 1).unwrap();
        assert!(specific.is_some());
    }

    #[test]
    fn test_multiple_versions() {
        let store = create_test_store();
        let (table_id, mut table_def) = create_test_table("default", "users", 1);

        // Store version 1
        store.put_version(&table_id, &table_def).unwrap();

        // Store version 2
        table_def.schema_version = 2;
        store.put_version(&table_id, &table_def).unwrap();

        // Store version 3
        table_def.schema_version = 3;
        store.put_version(&table_id, &table_def).unwrap();

        // Get latest should be version 3
        let latest = store.get_latest(&table_id).unwrap().unwrap();
        assert_eq!(latest.schema_version, 3);

        // List all versions
        let versions = store.list_versions(&table_id).unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].0, 1);
        assert_eq!(versions[1].0, 2);
        assert_eq!(versions[2].0, 3);
    }

    #[test]
    fn test_delete_all_versions() {
        let store = create_test_store();
        let (table_id, mut table_def) = create_test_table("default", "users", 1);

        // Store multiple versions
        store.put_version(&table_id, &table_def).unwrap();
        table_def.schema_version = 2;
        store.put_version(&table_id, &table_def).unwrap();

        // Delete all
        let deleted = store.delete_all_versions(&table_id).unwrap();
        assert_eq!(deleted, 3); // 2 versioned + 1 latest

        // Verify deleted
        assert!(store.get_latest(&table_id).unwrap().is_none());
        assert!(store.list_versions(&table_id).unwrap().is_empty());
    }

    #[test]
    fn test_scan_all_with_versions() {
        let store = create_test_store();

        // Insert multiple tables with versions
        let (table1_id, mut table1_def) = create_test_table("default", "users", 1);
        store.put_version(&table1_id, &table1_def).unwrap();
        table1_def.schema_version = 2;
        store.put_version(&table1_id, &table1_def).unwrap();

        let (table2_id, table2_def) = create_test_table("default", "posts", 1);
        store.put_version(&table2_id, &table2_def).unwrap();

        // Scan all with versions
        let all = store.scan_all_with_versions().unwrap();
        // 5 entries: users<lat>, users<ver>1, users<ver>2, posts<lat>, posts<ver>1
        assert_eq!(all.len(), 5);

        // Count latest entries
        let latest_count = all.iter().filter(|(_, _, is_latest)| *is_latest).count();
        assert_eq!(latest_count, 2);
    }

    #[test]
    fn test_scan_all_latest() {
        let store = create_test_store();

        // Insert multiple tables with versions
        let (table1_id, mut table1_def) = create_test_table("default", "users", 1);
        store.put_version(&table1_id, &table1_def).unwrap();
        table1_def.schema_version = 2;
        store.put_version(&table1_id, &table1_def).unwrap();

        let (table2_id, table2_def) = create_test_table("default", "posts", 1);
        store.put_version(&table2_id, &table2_def).unwrap();

        // Scan latest only
        let latest = store.scan_all_latest().unwrap();
        assert_eq!(latest.len(), 2);
    }

    #[test]
    fn test_admin_only_access() {
        let store = create_test_store();

        // System tables return None for table_access (admin-only)
        assert!(store.table_access().is_none());

        // Only Service, Dba, System roles can read
        assert!(!store.can_read(&Role::User));
        assert!(store.can_read(&Role::Service));
        assert!(store.can_read(&Role::Dba));
        assert!(store.can_read(&Role::System));
    }

    #[test]
    fn test_scan_namespace() {
        let store = create_test_store();

        // Insert tables in different namespaces
        let (table1_id, table1_def) = create_test_table("default", "users", 1);
        let (table2_id, table2_def) = create_test_table("default", "posts", 1);
        let (table3_id, table3_def) = create_test_table("test", "logs", 1);

        store.put_version(&table1_id, &table1_def).unwrap();
        store.put_version(&table2_id, &table2_def).unwrap();
        store.put_version(&table3_id, &table3_def).unwrap();

        // Scan default namespace (latest only)
        let default_tables = store.scan_namespace(&NamespaceId::default()).unwrap();
        assert_eq!(default_tables.len(), 2);

        // Scan test namespace
        let test_tables = store.scan_namespace(&NamespaceId::new("test")).unwrap();
        assert_eq!(test_tables.len(), 1);
    }

    #[test]
    fn test_scan_namespace_with_versions() {
        let store = create_test_store();
        let (table_id, mut table_def) = create_test_table("default", "users", 1);

        store.put_version(&table_id, &table_def).unwrap();
        table_def.schema_version = 2;
        store.put_version(&table_id, &table_def).unwrap();

        let versions = store
            .scan_namespace_with_versions(table_id.namespace_id())
            .unwrap();

        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].1.schema_version, 1);
        assert_eq!(versions[1].1.schema_version, 2);
    }

    #[test]
    fn test_get_current_version() {
        let store = create_test_store();
        let (table_id, table_def) = create_test_table("default", "users", 1);

        // No version initially
        assert!(store.get_current_version(&table_id).unwrap().is_none());

        // After storing
        store.put_version(&table_id, &table_def).unwrap();
        assert_eq!(store.get_current_version(&table_id).unwrap(), Some(1));
    }
}
