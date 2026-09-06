use std::sync::Arc;

use kalamdb_commons::{
    models::schemas::{ColumnDefinition, ScalarIndexDefinition},
    KSerializable, StorageKey, TableId,
};
use kalamdb_store::{
    EntityCodec, IndexDefinition, IndexedEntityStore, Partition, PrefixIndex, PrefixIndexedKey,
    PrefixIndexedValue, StorageBackend,
};

/// Build the canonical RocksDB partition name for a table scope.
///
/// Format: `{prefix}{namespace}:{table}` (e.g., "user_default:messages")
pub fn partition_name(prefix: &str, table_id: &TableId) -> String {
    format!("{}{}", prefix, table_id) // TableId Display impl gives "namespace:table"
}

/// PK first (index 0), then catalog scalar indexes from [`TableDefinition`].
pub fn table_prefix_indexes<K, V>(
    table_id: &TableId,
    pk_field_name: &str,
    scalar_indexes: &[ScalarIndexDefinition],
    columns: &[ColumnDefinition],
    user_scoped: bool,
) -> Vec<Arc<dyn IndexDefinition<K, V>>>
where
    K: PrefixIndexedKey + 'static,
    V: PrefixIndexedValue + KSerializable + 'static,
{
    let kind = if user_scoped { "user" } else { "shared" };
    let mut indexes: Vec<Arc<dyn IndexDefinition<K, V>>> =
        Vec::with_capacity(1 + scalar_indexes.len());
    indexes.push(Arc::new(PrefixIndex::new(
        format!("{}_{}_pk_idx", kind, table_id),
        vec![pk_field_name.to_string()],
        user_scoped,
    )));
    for definition in scalar_indexes {
        let Some(names) = definition.resolved_column_names(columns) else {
            continue;
        };
        if names.is_empty() {
            continue;
        }
        indexes.push(Arc::new(PrefixIndex::new(
            scalar_index_partition_name(table_id, &definition.name, user_scoped),
            names.into_iter().map(str::to_string).collect(),
            user_scoped,
        )));
    }
    indexes
}

/// RocksDB partition for a catalog scalar index (not the PK index).
pub fn scalar_index_partition_name(
    table_id: &TableId,
    index_name: &str,
    user_scoped: bool,
) -> String {
    let kind = if user_scoped { "user" } else { "shared" };
    format!("{}_{}_idx_{}", kind, table_id, index_name)
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
    codec: Arc<dyn EntityCodec<K, V>>,
) -> IndexedEntityStore<K, V>
where
    K: StorageKey,
    V: KSerializable + 'static,
{
    let partition_obj = partition.into();
    ensure_partition(&backend, partition_obj.clone());
    IndexedEntityStore::with_codec(backend, partition_obj.name().to_string(), indexes, codec)
}
