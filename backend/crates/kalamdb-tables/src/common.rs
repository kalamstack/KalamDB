use std::sync::Arc;

use kalamdb_commons::{KSerializable, StorageKey, TableId};
use kalamdb_store::{IndexDefinition, IndexedEntityStore, Partition, StorageBackend};

/// Build the canonical RocksDB partition name for a table scope.
///
/// Format: `{prefix}{namespace}:{table}` (e.g., "user_default:messages")
pub fn partition_name(prefix: &str, table_id: &TableId) -> String {
    format!("{}{}", prefix, table_id) // TableId Display impl gives "namespace:table"
}

/// Create the partition if it does not already exist. Best-effort: errors are ignored.
pub fn ensure_partition(backend: &Arc<dyn StorageBackend>, partition: impl Into<Partition>) {
    let partition = partition.into();
    let _ = backend.create_partition(&partition);
}

/// Create an IndexedEntityStore after ensuring the primary partition exists.
pub fn new_indexed_store_with_pk<K, V>(
    backend: Arc<dyn StorageBackend>,
    partition: impl Into<Partition>,
    indexes: Vec<Arc<dyn IndexDefinition<K, V>>>,
) -> IndexedEntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    let partition_obj = partition.into();
    ensure_partition(&backend, partition_obj.clone());
    IndexedEntityStore::new(backend, partition_obj.name().to_string(), indexes)
}
