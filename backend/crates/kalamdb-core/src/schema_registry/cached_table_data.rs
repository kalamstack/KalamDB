use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use datafusion::datasource::TableProvider;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::models::schemas::TableDefinition;
use kalamdb_commons::models::{StorageId, TableId};
use kalamdb_commons::schemas::{TableOptions, TableType};
use kalamdb_commons::TableAccess;
use kalamdb_filestore::StorageCached;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lightweight table info for file operations
#[derive(Debug, Clone)]
pub struct TableEntry {
    /// Storage ID for the table
    pub storage_id: StorageId,
    /// Table type (User or Shared)
    pub table_type: TableType,
    /// Access level (for shared tables)
    pub access_level: Option<TableAccess>,
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

    /// Last access timestamp in milliseconds since Unix epoch.
    ///
    /// Used for metrics and debugging. Moka cache handles LRU eviction automatically.
    last_accessed_ms: AtomicU64,

    /// Bloom filter columns (PRIMARY KEY + _seq) - computed once on cache entry creation
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
    /// **Thread Safety**: RwLock allows concurrent reads, exclusive writes for initialization
    provider: Arc<RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>>,
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
            table: Arc::clone(&self.table),
            storage_id: self.storage_id.clone(),
            schema_version: self.schema_version,
            last_accessed_ms: AtomicU64::new(self.last_accessed_ms.load(Ordering::Relaxed)),
            bloom_filter_columns: self.bloom_filter_columns.clone(),
            indexed_columns: self.indexed_columns.clone(),
            provider: Arc::clone(&self.provider),
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
            last_accessed_ms: AtomicU64::new(Self::now_millis()),
            bloom_filter_columns,
            indexed_columns,
            provider: Arc::new(RwLock::new(None)),
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

    fn now_millis() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    /// Compute bloom filter columns and indexed columns from table definition
    ///
    /// This is computed once when CachedTableData is created and reused for all
    /// flush operations. Returns (bloom_filter_columns, indexed_columns).
    ///
    /// - bloom_filter_columns: PRIMARY KEY columns + _seq (for Parquet Bloom filters)
    /// - indexed_columns: (column_id, column_name) pairs for row-group stats extraction
    fn compute_indexed_columns(table_def: &TableDefinition) -> (Vec<String>, Vec<(u64, String)>) {
        let mut bloom_filter_columns = Vec::new();
        let mut indexed_columns = Vec::new();

        // Add PRIMARY KEY columns
        for col in table_def.columns.iter().filter(|c| c.is_primary_key) {
            bloom_filter_columns.push(col.column_name.clone());
            indexed_columns.push((col.column_id, col.column_name.clone()));
        }

        // Add _seq system column (always present for Bloom filters and stats)
        bloom_filter_columns.push(SystemColumnNames::SEQ.to_string());
        indexed_columns.push((0, SystemColumnNames::SEQ.to_string())); // _seq uses column_id 0

        (bloom_filter_columns, indexed_columns)
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

    #[inline]
    pub fn touch_at(&self, timestamp_ms: u64) {
        self.last_accessed_ms.store(timestamp_ms, Ordering::Relaxed);
    }

    #[inline]
    pub fn last_accessed_ms(&self) -> u64 {
        self.last_accessed_ms.load(Ordering::Relaxed)
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
            access_level: match &self.table.table_options {
                TableOptions::Shared(opts) => {
                    Some(opts.access_level.clone().unwrap_or(TableAccess::Private))
                },
                TableOptions::User(_) | TableOptions::System(_) | TableOptions::Stream(_) => None,
            },
        }
    }

    /// Get StorageCached instance from StorageRegistry (centralized caching)
    ///
    /// StorageCached provides unified operations (list, get, put, delete)
    /// with built-in path template resolution. ObjectStore instances are
    /// cached per-storage in StorageRegistry, not per-table.
    ///
    /// **Performance**: First call builds store (~50-200μs for cloud), subsequent calls return cached Arc (~1μs)
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

    /// Get cached Bloom filter columns (PRIMARY KEY + _seq)
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

    /// Get cached indexed columns with column_id (PRIMARY KEY + _seq)
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
    /// **Performance**: O(1) access with read lock
    pub fn get_provider(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.provider.read().as_ref().map(Arc::clone)
    }

    /// Set the cached `TableProvider` for this table.
    pub fn set_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        let mut guard = self.provider.write();
        *guard = Some(provider);
    }

    /// Clear the cached provider (used during table invalidation)
    pub fn clear_provider(&self) {
        let mut guard = self.provider.write();
        *guard = None;
    }
}
