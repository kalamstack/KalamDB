//! Manifest table index definitions
//!
//! This module defines secondary indexes for the system.manifest table.

use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use kalamdb_commons::{models::rows::SystemTableRow, storage::Partition, ManifestId, StorageKey};
use kalamdb_store::IndexDefinition;

use crate::StoragePartition;

/// Index for querying manifests by PendingWrite state.
///
/// Key format: `{manifest_id_bytes}` (just the manifest ID)
/// Value: Empty (the index key IS the reference to the manifest)
///
/// This index allows efficient discovery of manifests that need flushing:
/// - O(1) lookup instead of O(N) scan of all manifests
/// - Flush jobs can iterate over this index to find pending tables
///
/// The index only contains manifests with sync_state == PendingWrite.
pub struct ManifestPendingWriteIndex;

impl IndexDefinition<ManifestId, SystemTableRow> for ManifestPendingWriteIndex {
    fn partition(&self) -> Partition {
        Partition::new(StoragePartition::ManifestPendingWriteIdx.name())
    }

    fn indexed_columns(&self) -> Vec<&str> {
        vec!["sync_state"]
    }

    fn extract_key(&self, primary_key: &ManifestId, row: &SystemTableRow) -> Option<Vec<u8>> {
        let sync_state = row.fields.values.get("sync_state")?;
        let is_pending_write = matches!(sync_state,
            ScalarValue::Utf8(Some(value)) if value == "pending_write"
        ) || matches!(sync_state,
            ScalarValue::LargeUtf8(Some(value)) if value == "pending_write"
        );

        if is_pending_write {
            // The key IS the manifest ID (the reference itself)
            Some(primary_key.storage_key())
        } else {
            None
        }
    }

    fn filter_to_prefix(&self, _filter: &datafusion::logical_expr::Expr) -> Option<Vec<u8>> {
        // No prefix filtering needed - we want all pending writes
        None
    }
}

/// Create the default set of indexes for the manifest table.
pub fn create_manifest_indexes() -> Vec<Arc<dyn IndexDefinition<ManifestId, SystemTableRow>>> {
    vec![Arc::new(ManifestPendingWriteIndex)]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{
        models::rows::{Row, SystemTableRow},
        NamespaceId, TableId, TableName, UserId,
    };

    use super::*;

    fn create_test_row(sync_state: &str) -> SystemTableRow {
        let mut fields = BTreeMap::new();
        fields.insert("sync_state".to_string(), ScalarValue::Utf8(Some(sync_state.to_string())));
        SystemTableRow {
            fields: Row::new(fields),
        }
    }

    #[test]
    fn test_pending_write_index_only_indexes_pending() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("tbl1"));
        let manifest_id = ManifestId::new(table_id.clone(), None);

        let index = ManifestPendingWriteIndex;

        // PendingWrite entry should be indexed
        let row = create_test_row("pending_write");
        let key = index.extract_key(&manifest_id, &row);
        assert!(key.is_some());
        assert_eq!(key.unwrap(), manifest_id.storage_key());

        // InSync entry should NOT be indexed
        let row = create_test_row("in_sync");
        let key = index.extract_key(&manifest_id, &row);
        assert!(key.is_none());

        // Syncing entry should NOT be indexed
        let row = create_test_row("syncing");
        let key = index.extract_key(&manifest_id, &row);
        assert!(key.is_none());

        // Stale entry should NOT be indexed
        let row = create_test_row("stale");
        let key = index.extract_key(&manifest_id, &row);
        assert!(key.is_none());

        // Error entry should NOT be indexed
        let row = create_test_row("error");
        let key = index.extract_key(&manifest_id, &row);
        assert!(key.is_none());
    }

    #[test]
    fn test_pending_write_index_user_scoped() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("user_tbl"));
        let user_id = UserId::new("user1");
        let manifest_id = ManifestId::new(table_id.clone(), Some(user_id.clone()));

        let index = ManifestPendingWriteIndex;
        let _ = table_id;
        let _ = user_id;
        let row = create_test_row("pending_write");
        let key = index.extract_key(&manifest_id, &row);
        assert!(key.is_some());
        assert_eq!(key.unwrap(), manifest_id.storage_key());
    }
}
