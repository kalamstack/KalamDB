use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, OnceLock,
    },
};

use datafusion::datasource::TableProvider;
use kalamdb_commons::{
    constants::SystemColumnNames,
    models::{schemas::TableDefinition, StorageId, TableId},
    schemas::TableType,
};
use kalamdb_filestore::StorageCached;
use parking_lot::RwLock;

use crate::{app_context::AppContext, error::KalamDbError, error_extensions::KalamDbResultExt};

const PROVIDER_EMPTY: u8 = 0;
const PROVIDER_INITIALIZED: u8 = 1;
const PROVIDER_OVERRIDDEN: u8 = 2;

struct ProviderSlot {
    state:             AtomicU8,
    initial:           OnceLock<Arc<dyn TableProvider + Send + Sync>>,
    override_provider: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
}

impl ProviderSlot {
    fn new() -> Self {
        Self {
            state:             AtomicU8::new(PROVIDER_EMPTY),
            initial:           OnceLock::new(),
            override_provider: RwLock::new(None),
        }
    }

    fn get(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        match self.state.load(Ordering::Acquire) {
            PROVIDER_INITIALIZED => self.initial.get().map(Arc::clone),
            PROVIDER_OVERRIDDEN => self.override_provider.read().as_ref().map(Arc::clone),
            PROVIDER_EMPTY => None,
            _ => None,
        }
    }

    fn set(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        let mut override_provider = self.override_provider.write();

        if self.initial.set(Arc::clone(&provider)).is_ok() {
            *override_provider = None;
            self.state.store(PROVIDER_INITIALIZED, Ordering::Release);
            return;
        }

        *override_provider = Some(provider);
        self.state.store(PROVIDER_OVERRIDDEN, Ordering::Release);
    }
}

/// Lightweight table info for file operations
#[derive(Debug, Clone)]
pub struct TableEntry {
    /// Storage ID for the table
    pub storage_id: StorageId,
    /// Table type (User or Shared)
    pub table_type: TableType,
}

/// Cached table data containing all metadata and schema information
///
/// This struct consolidates data previously split between separate caches
/// to eliminate duplication.
///
/// **Performance Note**: Moka cache handles LRU eviction automatically based on
/// access patterns, so we only track timestamps for metrics and debugging.
pub struct CachedTableData {
    /// Full schema definition with all columns
    pub table: Arc<TableDefinition>,

    /// Reference to storage configuration in system.storages
    pub storage_id: StorageId,

    /// Current schema version number
    pub schema_version: u32,

    /// Bloom filter columns (PRIMARY KEY + equality-friendly scalar indexes)
    /// Static for each table schema version, changes only on ALTER TABLE
    bloom_filter_columns: Vec<String>,

    /// Indexed columns with column_id for stats extraction (column_id, column_name)
    /// Used for Parquet row-group statistics keyed by stable column_id
    indexed_columns: Vec<(u64, String)>,

    /// Cached DataFusion table provider for this table.
    ///
    /// Lazily initialized when first needed and reused for both system and
    /// non-system tables through the common `TableProvider` surface.
    ///
    /// **Thread Safety**: first provider read is lock-free after initialization; rare
    /// override/clear paths use a write lock.
    provider: Arc<ProviderSlot>,
}

impl std::fmt::Debug for CachedTableData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedTableData")
            .field("table", &self.table)
            .field("storage_id", &self.storage_id)
            .field("schema_version", &self.schema_version)
            .field("bloom_filter_columns", &self.bloom_filter_columns)
            .field("indexed_columns", &self.indexed_columns)
            .finish_non_exhaustive()
    }
}

impl Clone for CachedTableData {
    fn clone(&self) -> Self {
        Self {
            table:                Arc::clone(&self.table),
            storage_id:           self.storage_id.clone(),
            schema_version:       self.schema_version,
            bloom_filter_columns: self.bloom_filter_columns.clone(),
            indexed_columns:      self.indexed_columns.clone(),
            provider:             Arc::clone(&self.provider),
        }
    }
}

impl CachedTableData {
    /// Create new cached table data with required storage_id
    pub fn new(schema: Arc<TableDefinition>) -> Self {
        let schema_version = schema.schema_version;
        let storage_id = Self::extract_storage_id(&schema).unwrap_or_else(|| StorageId::local());
        let (bloom_filter_columns, indexed_columns) = Self::compute_indexed_columns(&schema);
        Self {
            table: schema,
            storage_id,
            schema_version,
            bloom_filter_columns,
            indexed_columns,
            provider: Arc::new(ProviderSlot::new()),
        }
    }

    /// Create cached table data from a table definition with full initialization
    ///
    /// This method resolves storage_id and computes all cached fields.
    /// Used when loading table definitions from persistence or creating new tables.
    pub fn from_table_definition(
        _app_ctx: &AppContext,
        _table_id: &TableId,
        table_def: Arc<TableDefinition>,
    ) -> Result<Self, KalamDbError> {
        Ok(Self::new(table_def))
    }

    /// Compute bloom filter columns and indexed columns from table definition
    ///
    /// This is computed once when CachedTableData is created and reused for all
    /// flush operations. Returns (bloom_filter_columns, indexed_columns).
    ///
    /// - bloom_filter_columns: PRIMARY KEY + equality-friendly scalar index columns
    /// - indexed_columns: those columns plus `_seq` for segment stats extraction
    fn compute_indexed_columns(table_def: &TableDefinition) -> (Vec<String>, Vec<(u64, String)>) {
        let mut bloom_filter_columns = Vec::new();
        let mut indexed_columns = Vec::new();
        let mut seen_column_ids = HashSet::new();

        for col in table_def.columns.iter().filter(|c| c.is_primary_key) {
            Self::push_indexed_column(
                col.column_id,
                &col.column_name,
                col.data_type.supports_equality_bloom(),
                &mut bloom_filter_columns,
                &mut indexed_columns,
                &mut seen_column_ids,
            );
        }

        for index in &table_def.scalar_indexes {
            let Some(names) = index.resolved_column_names(&table_def.columns) else {
                continue;
            };
            for (column_id, name) in index.columns.iter().zip(names.iter()) {
                let bloom = table_def
                    .columns
                    .iter()
                    .find(|column| column.column_id == column_id.as_u64())
                    .is_some_and(|column| column.data_type.supports_equality_bloom());
                if !bloom {
                    continue;
                }
                Self::push_indexed_column(
                    column_id.as_u64(),
                    name,
                    true,
                    &mut bloom_filter_columns,
                    &mut indexed_columns,
                    &mut seen_column_ids,
                );
            }
        }

        indexed_columns.push((0, SystemColumnNames::SEQ.to_string()));

        (bloom_filter_columns, indexed_columns)
    }

    fn push_indexed_column(
        column_id: u64,
        column_name: &str,
        bloom: bool,
        bloom_filter_columns: &mut Vec<String>,
        indexed_columns: &mut Vec<(u64, String)>,
        seen_column_ids: &mut HashSet<u64>,
    ) {
        if !seen_column_ids.insert(column_id) {
            return;
        }
        if bloom {
            bloom_filter_columns.push(column_name.to_string());
        }
        indexed_columns.push((column_id, column_name.to_string()));
    }

    /// Extract storage ID from table definition options
    ///
    /// Returns the storage_id from the table's options, or None for system tables.
    pub fn extract_storage_id(table_def: &TableDefinition) -> Option<StorageId> {
        use kalamdb_commons::schemas::TableOptions;
        match &table_def.table_options {
            TableOptions::User(opts) => Some(opts.storage_id.clone()),
            TableOptions::Shared(opts) => Some(opts.storage_id.clone()),
            TableOptions::Stream(_) => Some(StorageId::local()), // Default for streams
            TableOptions::System(_) => None,
        }
    }

    /// Get Arrow schema from the cached provider or compute from TableDefinition
    ///
    /// If a provider is cached, returns its schema directly (zero-cost).
    /// Otherwise computes from the TableDefinition.
    ///
    /// # Returns
    /// Arc-wrapped Arrow Schema for zero-copy sharing across TableProvider instances
    pub fn arrow_schema(&self) -> Result<Arc<datafusion::arrow::datatypes::Schema>, KalamDbError> {
        // Fast path: get schema from cached provider (already computed and stored there)
        if let Some(provider) = self.get_provider() {
            return Ok(provider.schema());
        }

        // Slow path: compute from TableDefinition (provider not yet created)
        self.table
            .to_arrow_schema()
            .into_schema_error("Failed to convert to Arrow schema")
    }

    /// Build a `TableEntry` from this cached data
    pub fn table_entry(&self) -> TableEntry {
        TableEntry {
            storage_id: self.storage_id.clone(),
            table_type: self.table.table_type.into(),
        }
    }

    /// Get StorageCached instance from StorageRegistry (centralized caching)
    ///
    /// StorageCached provides unified operations (list, get, put, delete)
    /// with built-in path template resolution. ObjectStore instances are
    /// cached per-storage in StorageRegistry, not per-table.
    ///
    /// **Performance**: First call builds store (~50-200μs for cloud), subsequent calls return
    /// cached Arc (~1μs)
    ///
    /// # Returns
    /// Arc-wrapped StorageCached for zero-copy sharing across operations
    ///
    /// # Errors
    /// Returns error if storage not found
    pub fn storage_cached(
        &self,
        storage_registry: &Arc<kalamdb_filestore::StorageRegistry>,
    ) -> Result<Arc<StorageCached>, KalamDbError> {
        storage_registry
            .get_cached(&self.storage_id)
            .map_err(|e| KalamDbError::Other(format!("Filestore error: {}", e)))?
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Storage '{}' not found in registry",
                    self.storage_id.as_str()
                ))
            })
    }

    /// Get cached Bloom filter columns (PRIMARY KEY columns)
    ///
    /// These columns are computed once when the cache entry is created and
    /// remain constant for the lifetime of this schema version. Used for
    /// Parquet Bloom filter generation during flush operations.
    ///
    /// **Performance**: O(1) access, no computation required
    #[inline]
    pub fn bloom_filter_columns(&self) -> &[String] {
        &self.bloom_filter_columns
    }

    /// Get cached indexed columns with column_id (PK + scalar indexes + `_seq`)
    ///
    /// Returns (column_id, column_name) pairs for columns that need
    /// row-group statistics in Parquet files. Column IDs are stable
    /// across schema changes (ALTER TABLE).
    ///
    /// **Performance**: O(1) access, no computation required
    #[inline]
    pub fn indexed_columns(&self) -> &[(u64, String)] {
        &self.indexed_columns
    }

    /// Get the cached DataFusion TableProvider for this table
    ///
    /// **Performance**: O(1) access with a lock-free fast path after initialization
    pub fn get_provider(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.provider.get()
    }

    /// Set the cached `TableProvider` for this table.
    pub fn set_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        self.provider.set(provider);
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::datasource::MemTable;

    use super::*;

    fn provider_with_field(name: &str) -> Arc<dyn TableProvider + Send + Sync> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["value"]))],
        )
        .expect("test batch");

        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("test provider"))
    }

    #[test]
    fn provider_slot_supports_rebind() {
        let slot = ProviderSlot::new();
        let first = provider_with_field("first");
        let second = provider_with_field("second");

        assert!(slot.get().is_none());

        slot.set(first);
        assert_eq!(slot.get().unwrap().schema().field(0).name(), "first");

        slot.set(second);
        assert_eq!(slot.get().unwrap().schema().field(0).name(), "second");
    }

    #[test]
    fn cached_column_sets_keep_bloom_filters_to_primary_keys() {
        use kalamdb_commons::{
            models::{KalamDataType, NamespaceId, TableName},
            schemas::{ColumnDefinition, TableOptions, TableType},
        };

        let table_def = TableDefinition::new(
            NamespaceId::from("test"),
            TableName::from("events"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .expect("table definition");

        let cached = CachedTableData::new(Arc::new(table_def));

        assert_eq!(cached.bloom_filter_columns(), &["id".to_string()]);
        assert_eq!(
            cached.indexed_columns(),
            &[
                (1, "id".to_string()),
                (0, SystemColumnNames::SEQ.to_string())
            ]
        );
    }

    #[test]
    fn cached_column_sets_include_scalar_index_columns_and_skip_embeddings() {
        use kalamdb_commons::{
            models::{ColumnId, KalamDataType, NamespaceId, TableName},
            schemas::{ColumnDefinition, ScalarIndexDefinition, TableOptions, TableType},
        };

        let mut table_def = TableDefinition::new(
            NamespaceId::from("chat"),
            TableName::from("messages"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
                ColumnDefinition::simple(2, "conversation_id", 2, KalamDataType::Text),
                ColumnDefinition::simple(3, "embedding", 3, KalamDataType::Embedding(3)),
            ],
            TableOptions::shared(),
            None,
        )
        .expect("table definition");
        table_def.scalar_indexes = vec![
            ScalarIndexDefinition::new("idx_conversation_id", vec![ColumnId::new(2)], false),
            ScalarIndexDefinition::new("idx_embedding", vec![ColumnId::new(3)], false),
        ];

        let cached = CachedTableData::new(Arc::new(table_def));

        assert_eq!(
            cached.bloom_filter_columns(),
            &["id".to_string(), "conversation_id".to_string()]
        );
        assert_eq!(
            cached.indexed_columns(),
            &[
                (1, "id".to_string()),
                (2, "conversation_id".to_string()),
                (0, SystemColumnNames::SEQ.to_string())
            ]
        );
    }
}
