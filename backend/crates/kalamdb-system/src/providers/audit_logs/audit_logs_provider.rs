//! System.audit_log table provider
//!
//! This module provides a DataFusion TableProvider implementation for the system.audit_log table.
//! Uses the EntityStore architecture with type-safe keys (AuditLogId).

use crate::error::{SystemError, SystemResultExt};
use crate::providers::audit_logs::models::AuditLogEntry;
use crate::providers::base::{
    extract_filter_value, system_rows_to_batch, SimpleProviderDefinition,
};
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use kalamdb_commons::models::rows::SystemTableRow;
use kalamdb_commons::models::AuditLogId;
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_commons::SystemTable;
use kalamdb_store::entity_store::{EntityStore, EntityStoreAsync};
use kalamdb_store::{IndexedEntityStore, StorageBackend};
use std::sync::{Arc, OnceLock};

/// System.audit_log table provider using EntityStore architecture
pub struct AuditLogsTableProvider {
    store: IndexedEntityStore<AuditLogId, SystemTableRow>,
}

impl std::fmt::Debug for AuditLogsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogsTableProvider").finish()
    }
}

impl AuditLogsTableProvider {
    /// Create a new audit logs table provider
    ///
    /// # Arguments
    /// * `backend` - Storage backend (RocksDB or mock)
    ///
    /// # Returns
    /// A new AuditLogsTableProvider instance
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        let store = IndexedEntityStore::new(
            backend,
            crate::SystemTable::AuditLog.column_family_name().expect("AuditLog is a table"),
            Vec::new(),
        );
        Self { store }
    }

    /// Append a new audit log entry
    ///
    /// # Arguments
    /// * `entry` - The audit log entry to append
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn append(&self, entry: AuditLogEntry) -> Result<(), SystemError> {
        let row = Self::encode_audit_row(&entry)?;
        self.store.put(&entry.audit_id, &row)?;
        Ok(())
    }

    /// Async version of `append()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn append_async(&self, entry: AuditLogEntry) -> Result<(), SystemError> {
        let row = Self::encode_audit_row(&entry)?;
        self.store
            .put_async(&entry.audit_id, &row)
            .await
            .into_system_error("put_async error")?;
        Ok(())
    }

    /// Get an audit log entry by ID
    ///
    /// # Arguments
    /// * `audit_id` - The audit log ID to lookup
    ///
    /// # Returns
    /// Option<AuditLogEntry> if found, None otherwise
    pub fn get_entry(&self, audit_id: &AuditLogId) -> Result<Option<AuditLogEntry>, SystemError> {
        let row = self.store.get(audit_id)?;
        row.map(|value| Self::decode_audit_row(&value)).transpose()
    }

    /// Async version of `get_entry()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn get_entry_async(
        &self,
        audit_id: &AuditLogId,
    ) -> Result<Option<AuditLogEntry>, SystemError> {
        let row = self
            .store
            .get_async(audit_id.clone())
            .await
            .into_system_error("get_async error")?;
        row.map(|value| Self::decode_audit_row(&value)).transpose()
    }

    /// Scan all audit log entries and return as RecordBatch
    pub fn scan_all_entries(&self) -> Result<RecordBatch, SystemError> {
        let rows = self
            .store
            .scan_all_typed(None, None, None)?
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    /// Scan up to `limit` audit log entries and return as RecordBatch
    pub fn scan_entries_limited(&self, limit: usize) -> Result<RecordBatch, SystemError> {
        use kalamdb_store::entity_store::{EntityStore, ScanDirection};
        let iter = self.store.scan_directional(None, ScanDirection::Newer, limit)?;
        let row_entries: Vec<(AuditLogId, SystemTableRow)> = iter.collect::<Result<Vec<_>, _>>()?;
        let rows = row_entries.into_iter().map(|(_, row)| row).collect();
        system_rows_to_batch(&Self::schema(), rows)
    }

    /// Scan all audit log entries and return as Vec<AuditLogEntry>
    /// Useful for testing and internal usage where RecordBatch is not needed
    pub fn scan_all(&self) -> Result<Vec<AuditLogEntry>, SystemError> {
        use kalamdb_store::entity_store::EntityStore;
        let iter = self.store.scan_iterator(None, None)?;
        let mut entries = Vec::new();
        for item in iter {
            let (_, row) = item?;
            entries.push(Self::decode_audit_row(&row)?);
        }
        Ok(entries)
    }

    /// Async version of `scan_all()` - offloads to blocking thread pool.
    ///
    /// Use this in async contexts to avoid blocking the Tokio runtime.
    pub async fn scan_all_async(&self) -> Result<Vec<AuditLogEntry>, SystemError> {
        let results: Vec<(Vec<u8>, SystemTableRow)> = self
            .store
            .scan_all_async(None, None, None)
            .await
            .into_system_error("scan_all_async error")?;
        results.into_iter().map(|(_, row)| Self::decode_audit_row(&row)).collect()
    }
    fn scan_to_batch_filtered(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        // Check for primary key equality filter → O(1) point lookup
        if let Some(audit_id_str) = extract_filter_value(filters, "audit_id") {
            let audit_id = AuditLogId::new(&audit_id_str);
            if let Some(row) = self.store.get(&audit_id)? {
                return system_rows_to_batch(&Self::schema(), vec![row]);
            }
            return system_rows_to_batch(&Self::schema(), vec![]);
        }

        // Use iterator with early termination on limit
        if let Some(lim) = limit {
            let iter = self.store.scan_iterator(None, None)?;
            let mut entries = Vec::with_capacity(lim.min(1000));
            for item in iter {
                let (audit_id, row) = item?;
                entries.push((audit_id, row));
                if entries.len() >= lim {
                    break;
                }
            }
            let rows = entries.into_iter().map(|(_, row)| row).collect();
            return system_rows_to_batch(&Self::schema(), rows);
        }

        // No filters/limit: full scan
        self.scan_all_entries()
    }

    fn provider_definition() -> SimpleProviderDefinition {
        SimpleProviderDefinition {
            table_name: Self::table_name(),
            schema: Self::schema,
        }
    }

    fn encode_audit_row(entry: &AuditLogEntry) -> Result<SystemTableRow, SystemError> {
        model_to_system_row(entry, &Self::definition())
    }

    fn decode_audit_row(row: &SystemTableRow) -> Result<AuditLogEntry, SystemError> {
        system_row_to_model(row, &Self::definition())
    }

    fn definition() -> TableDefinition {
        AuditLogEntry::definition()
    }

    fn schema() -> SchemaRef {
        static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                Self::definition()
                    .to_arrow_schema()
                    .expect("Failed to convert audit_log TableDefinition to Arrow schema")
            })
            .clone()
    }

    fn table_name() -> &'static str {
        SystemTable::AuditLog.table_name()
    }
}

crate::impl_simple_system_table_provider!(
    provider = AuditLogsTableProvider,
    key = AuditLogId,
    value = SystemTableRow,
    definition = provider_definition,
    scan_all = scan_all_entries,
    scan_filtered = scan_to_batch_filtered
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::datasource::TableProvider;
    use kalamdb_commons::UserId;
    use kalamdb_store::test_utils::InMemoryBackend;
    use serde_json::json;

    fn create_test_provider() -> AuditLogsTableProvider {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        AuditLogsTableProvider::new(backend)
    }

    fn create_test_entry(audit_id: &str, action: &str, timestamp: i64) -> AuditLogEntry {
        AuditLogEntry {
            audit_id: AuditLogId::new(audit_id),
            timestamp,
            subject_user_id: Some(UserId::new("user_123")),
            actor_user_id: UserId::new("admin"),
            action: action.to_string(),
            target: "system.users".to_string(),
            details: Some(json!({"test": true}).to_string()),
            ip_address: Some("127.0.0.1".to_string()),
        }
    }

    #[test]
    fn test_append_and_get_entry() {
        let provider = create_test_provider();
        let entry = create_test_entry("audit_001", "user.create", 1730000000000);

        // Append entry
        provider.append(entry.clone()).unwrap();

        // Get by ID
        let retrieved = provider.get_entry(&AuditLogId::new("audit_001")).unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.action, "user.create");
        assert_eq!(retrieved.timestamp, 1730000000000);
    }

    #[test]
    fn test_scan_all_entries() {
        let provider = create_test_provider();

        // Append multiple entries
        for i in 1..=5 {
            let entry = create_test_entry(
                &format!("audit_{:03}", i),
                "user.update",
                1730000000000 + (i * 1000),
            );
            provider.append(entry).unwrap();
        }

        // Scan all
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 8);
    }

    #[test]
    fn test_filter_by_action() {
        let provider = create_test_provider();

        // Append entries with different actions
        provider
            .append(create_test_entry("audit_001", "user.create", 1730000000000))
            .unwrap();
        provider
            .append(create_test_entry("audit_002", "user.update", 1730000001000))
            .unwrap();
        provider
            .append(create_test_entry("audit_003", "user.create", 1730000002000))
            .unwrap();

        // Scan all and verify
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_rows(), 3);

        // Verify actions column
        let actions = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(actions.len(), 3);
    }

    #[test]
    fn test_projection() {
        let provider = create_test_provider();

        // Append test entry
        provider
            .append(create_test_entry("audit_001", "user.create", 1730000000000))
            .unwrap();

        // Scan with all columns
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_columns(), 8);

        // Verify schema matches
        assert_eq!(batch.schema(), provider.schema());
    }

    #[test]
    fn test_batch_operations() {
        let provider = create_test_provider();

        // Batch append 100 entries
        for i in 0..100 {
            let entry = create_test_entry(
                &format!("audit_{:05}", i),
                "batch.operation",
                1730000000000 + (i * 100),
            );
            provider.append(entry).unwrap();
        }

        // Verify all entries
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_nullable_fields() {
        let provider = create_test_provider();

        // Create entry without optional fields
        let entry = AuditLogEntry {
            audit_id: AuditLogId::new("audit_001"),
            timestamp: 1730000000000,
            subject_user_id: None,
            actor_user_id: UserId::new("admin"),
            action: "test.action".to_string(),
            target: "test.target".to_string(),
            details: None,    // Optional
            ip_address: None, // Optional
        };

        provider.append(entry).unwrap();

        // Verify nullable fields handled correctly
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_timestamp_ordering() {
        let provider = create_test_provider();

        // Insert entries with different timestamps
        let timestamps = [1730000003000, 1730000001000, 1730000002000];
        for (i, &ts) in timestamps.iter().enumerate() {
            let entry = create_test_entry(&format!("audit_{}", i), "test.action", ts);
            provider.append(entry).unwrap();
        }

        // Scan all
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_rows(), 3);

        // Verify timestamps column exists
        let timestamps_col =
            batch.column(1).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(timestamps_col.len(), 3);
    }

    #[test]
    fn test_scan_all_via_provider_api() {
        let provider = create_test_provider();

        assert_eq!(provider.schema().fields().len(), 8);

        provider
            .append(create_test_entry("audit_001", "test.action", 1730000000000))
            .unwrap();
        let batch = provider.scan_all_entries().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
