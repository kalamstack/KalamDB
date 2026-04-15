//! Core implementation of SchemaRegistry

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::schema_registry::cached_table_data::CachedTableData;
use chrono::Utc;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::expr::ScalarFunction as ScalarFunctionExpr;
use datafusion::logical_expr::Expr;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::json_value_to_scalar;
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::schemas::TableDefinition;
use kalamdb_commons::models::{StorageId, TableId, TableVersionId};
use kalamdb_commons::schemas::{ColumnDefault, ColumnDefinition, TableType};
use kalamdb_commons::SystemTable;
use kalamdb_live::models::ChangeNotification;
use kalamdb_system::{NotificationService, SchemaRegistry as SchemaRegistryTrait};
// use kalamdb_system::NotificationService as NotificationServiceTrait;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Debug, Default)]
struct SystemSchemaReconcileStats {
    created: usize,
    unchanged: usize,
    upgraded: usize,
}

/// Unified schema cache for table metadata, schemas, and providers
///
/// Uses DashMap for all table data including:
/// - Table definitions and metadata
/// - Memoized Arrow schemas
/// - DataFusion TableProvider instances
///
/// Maximum number of entries in the version_cache before LRU eviction kicks in.
/// Each entry contains a CachedTableData with Arrow schema and column metadata.
const VERSION_CACHE_MAX_ENTRIES: usize = 256;

pub struct SchemaRegistry {
    /// App context for accessing system components (set via set_app_context)
    app_context: OnceLock<Arc<AppContext>>,

    /// Cache for table data (latest versions)
    table_cache: DashMap<TableId, Arc<CachedTableData>>,

    /// Cache for specific table versions (for reading old Parquet files)
    version_cache: DashMap<TableVersionId, Arc<CachedTableData>>,

    /// LRU access timestamps for version_cache entries (separate from hot path)
    version_cache_access: DashMap<TableVersionId, u64>,

    /// Monotonic counter for LRU ordering of version_cache
    version_cache_counter: AtomicU64,

    /// DataFusion base session context for table registration (set once during init)
    base_session_context: OnceLock<Arc<datafusion::prelude::SessionContext>>,
}

impl std::fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistry")
            .field("table_cache_size", &self.table_cache.len())
            .field("version_cache_size", &self.version_cache.len())
            .field("base_session_context_set", &self.base_session_context.get().is_some())
            .field("app_context_set", &self.app_context.get().is_some())
            .finish()
    }
}

impl SchemaRegistry {
    /// Create a new schema cache with specified maximum size
    pub fn new(max_size: usize) -> Self {
        // Use capacity hints to avoid over-allocation.
        // DashMap pre-allocates segments based on capacity hint.
        // A small hint (16-64) is enough for startup; DashMap grows dynamically.
        let initial_capacity = std::cmp::min(max_size, 64);
        Self {
            app_context: OnceLock::new(),
            table_cache: DashMap::with_capacity(initial_capacity),
            version_cache: DashMap::with_capacity(std::cmp::min(VERSION_CACHE_MAX_ENTRIES, 64)),
            version_cache_access: DashMap::with_capacity(std::cmp::min(
                VERSION_CACHE_MAX_ENTRIES,
                64,
            )),
            version_cache_counter: AtomicU64::new(0),
            base_session_context: OnceLock::new(),
        }
    }

    /// Set the DataFusion base session context for table registration
    pub fn set_base_session_context(&self, session: Arc<datafusion::prelude::SessionContext>) {
        let _ = self.base_session_context.set(session);
    }

    /// Set the AppContext (break circular dependency)
    pub fn set_app_context(&self, app_context: Arc<AppContext>) {
        let _ = self.app_context.set(app_context);
    }

    /// Get the AppContext (panics if not set)
    fn app_context(&self) -> &Arc<AppContext> {
        self.app_context.get().expect("AppContext not set on SchemaRegistry")
    }

    /// Initialize registry by loading all existing tables from storage
    ///
    /// This should be called once at system startup.
    pub fn initialize_tables(&self) -> Result<(), KalamDbError> {
        self.reconcile_system_table_definitions()?;

        // Scan all table definitions
        let all_defs = self.scan_all_table_definitions()?;

        if all_defs.is_empty() {
            log::debug!("SchemaRegistry initialized: no existing tables found");
            return Ok(());
        }

        log::debug!("SchemaRegistry initialized: loading {} existing tables...", all_defs.len());

        let mut loaded_count = 0;
        let mut skipped_count = 0;
        let mut failed_count = 0;

        for def in all_defs {
            let table_id =
                TableId::from_strings(def.namespace_id.as_str(), def.table_name.as_str());

            // Skip tables that already have a cached provider — re-creating them would
            // destroy in-memory state (e.g., MemoryStreamLogStore data for stream tables).
            if self.table_cache.contains_key(&table_id) {
                skipped_count += 1;
                continue;
            }

            let table_name = def.table_name.clone();
            match self.put(def) {
                Ok(_) => loaded_count += 1,
                Err(e) => {
                    log::error!("Failed to load table {}: {}", table_name, e);
                    failed_count += 1;
                },
            }
        }

        log::info!(
            "SchemaRegistry initialized. Loaded: {}, Skipped (cached): {}, Failed: {}",
            loaded_count,
            skipped_count,
            failed_count
        );
        Ok(())
    }

    pub fn reconcile_system_table_definitions(&self) -> Result<(), KalamDbError> {
        let system_tables = self.app_context().system_tables();
        let tables_provider = system_tables.tables();
        let expected_defs = system_tables.expected_system_table_definitions();
        let mut stats = SystemSchemaReconcileStats::default();

        for expected in expected_defs {
            let expected = expected.as_ref();
            let table_id =
                TableId::from_strings(expected.namespace_id.as_str(), expected.table_name.as_str());

            let persisted = tables_provider
                .get_table_by_id(&table_id)
                .into_kalamdb_error("Failed to load persisted schema definition")?;

            match persisted {
                None => {
                    let initial = expected.clone();
                    tables_provider
                        .create_table(&table_id, &initial)
                        .into_kalamdb_error("Failed to persist initial system schema definition")?;
                    stats.created += 1;
                    log::debug!(
                        "[SchemaRegistry] persisted missing system table schema {} at version {}",
                        table_id,
                        initial.schema_version
                    );
                },
                Some(current) => {
                    if Self::schema_semantically_equal(&current, expected) {
                        stats.unchanged += 1;
                        continue;
                    }

                    Self::validate_schema_evolution(&current, expected)?;
                    let upgraded = Self::build_reconciled_definition(&current, expected);

                    tables_provider.put_versioned_schema(&table_id, &upgraded).into_kalamdb_error(
                        "Failed to persist upgraded system schema definition",
                    )?;
                    stats.upgraded += 1;
                    log::info!(
                        "[SchemaRegistry] upgraded system schema {} from version {} to {}",
                        table_id,
                        current.schema_version,
                        upgraded.schema_version
                    );
                },
            }
        }

        if stats.created > 0 || stats.upgraded > 0 {
            log::info!(
                "[SchemaRegistry] System schema reconciliation: created={}, upgraded={}, unchanged={}",
                stats.created,
                stats.upgraded,
                stats.unchanged
            );
        } else {
            log::debug!(
                "[SchemaRegistry] System schema reconciliation: no changes (unchanged={})",
                stats.unchanged
            );
        }

        Ok(())
    }

    fn schema_semantically_equal(current: &TableDefinition, expected: &TableDefinition) -> bool {
        current.namespace_id == expected.namespace_id
            && current.table_name == expected.table_name
            && current.table_type == expected.table_type
            && current.columns == expected.columns
            && current.next_column_id == expected.next_column_id
            && current.table_options == expected.table_options
            && current.table_comment == expected.table_comment
    }

    fn build_reconciled_definition(
        current: &TableDefinition,
        expected: &TableDefinition,
    ) -> TableDefinition {
        let mut upgraded = expected.clone();
        upgraded.schema_version = current.schema_version.saturating_add(1);
        upgraded.created_at = current.created_at;
        upgraded.updated_at = Utc::now();
        upgraded
    }

    fn validate_schema_evolution(
        current: &TableDefinition,
        expected: &TableDefinition,
    ) -> Result<(), KalamDbError> {
        if current.namespace_id != expected.namespace_id
            || current.table_name != expected.table_name
        {
            return Err(KalamDbError::invalid_schema_evolution(format!(
                "table identity changed from {}.{} to {}.{}",
                current.namespace_id.as_str(),
                current.table_name.as_str(),
                expected.namespace_id.as_str(),
                expected.table_name.as_str()
            )));
        }

        if current.table_type != expected.table_type {
            return Err(KalamDbError::invalid_schema_evolution(format!(
                "table type changed from {} to {} for {}.{}",
                current.table_type.as_str(),
                expected.table_type.as_str(),
                current.namespace_id.as_str(),
                current.table_name.as_str()
            )));
        }

        let current_pk_ids: HashSet<u64> = current
            .columns
            .iter()
            .filter(|col| col.is_primary_key)
            .map(|col| col.column_id)
            .collect();
        let expected_pk_ids: HashSet<u64> = expected
            .columns
            .iter()
            .filter(|col| col.is_primary_key)
            .map(|col| col.column_id)
            .collect();
        if current_pk_ids != expected_pk_ids {
            return Err(KalamDbError::invalid_schema_evolution(format!(
                "primary key columns changed for {}.{}",
                current.namespace_id.as_str(),
                current.table_name.as_str()
            )));
        }

        let current_partition_ids: HashSet<u64> = current
            .columns
            .iter()
            .filter(|col| col.is_partition_key)
            .map(|col| col.column_id)
            .collect();
        let expected_partition_ids: HashSet<u64> = expected
            .columns
            .iter()
            .filter(|col| col.is_partition_key)
            .map(|col| col.column_id)
            .collect();
        if current_partition_ids != expected_partition_ids {
            return Err(KalamDbError::invalid_schema_evolution(format!(
                "required partition/index key columns changed for {}.{}",
                current.namespace_id.as_str(),
                current.table_name.as_str()
            )));
        }

        let expected_by_id: HashMap<u64, &ColumnDefinition> =
            expected.columns.iter().map(|col| (col.column_id, col)).collect();
        for old_col in &current.columns {
            let Some(new_col) = expected_by_id.get(&old_col.column_id).copied() else {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "column '{}' (id={}) was removed from {}.{}; only additive changes are allowed",
                    old_col.column_name,
                    old_col.column_id,
                    current.namespace_id.as_str(),
                    current.table_name.as_str()
                )));
            };

            if old_col.column_name != new_col.column_name {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "column rename is not allowed in startup reconciliation (id={}): '{}' -> '{}'",
                    old_col.column_id, old_col.column_name, new_col.column_name
                )));
            }

            if old_col.ordinal_position != new_col.ordinal_position {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "column ordinal changed for '{}' (id={}): {} -> {}",
                    old_col.column_name,
                    old_col.column_id,
                    old_col.ordinal_position,
                    new_col.ordinal_position
                )));
            }

            if old_col.is_primary_key != new_col.is_primary_key {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "primary key flag changed for column '{}' (id={})",
                    old_col.column_name, old_col.column_id
                )));
            }

            if old_col.is_partition_key != new_col.is_partition_key {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "partition/index flag changed for column '{}' (id={})",
                    old_col.column_name, old_col.column_id
                )));
            }

            if old_col.is_nullable && !new_col.is_nullable {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "column '{}' changed from nullable to NOT NULL",
                    old_col.column_name
                )));
            }

            if !Self::is_safe_type_widening(&old_col.data_type, &new_col.data_type) {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "incompatible type change for column '{}' (id={}): {} -> {}",
                    old_col.column_name, old_col.column_id, old_col.data_type, new_col.data_type
                )));
            }
        }

        let current_ids: HashSet<u64> = current.columns.iter().map(|col| col.column_id).collect();
        for new_col in &expected.columns {
            if current_ids.contains(&new_col.column_id) {
                continue;
            }
            if !new_col.is_nullable && new_col.default_value.is_none() {
                return Err(KalamDbError::invalid_schema_evolution(format!(
                    "new non-null column '{}' must declare a default value",
                    new_col.column_name
                )));
            }
        }

        Ok(())
    }

    fn is_safe_type_widening(old_type: &KalamDataType, new_type: &KalamDataType) -> bool {
        if old_type == new_type {
            return true;
        }

        match (old_type, new_type) {
            (KalamDataType::SmallInt, KalamDataType::Int | KalamDataType::BigInt) => true,
            (KalamDataType::Int, KalamDataType::BigInt) => true,
            (KalamDataType::Float, KalamDataType::Double) => true,
            (
                KalamDataType::Decimal {
                    precision: old_precision,
                    scale: old_scale,
                },
                KalamDataType::Decimal {
                    precision: new_precision,
                    scale: new_scale,
                },
            ) => new_precision >= old_precision && new_scale == old_scale,
            _ => false,
        }
    }

    // ===== Basic Cache Methods =====

    /// Get cached table data for a table (latest version)
    pub fn get(&self, table_id: &TableId) -> Option<Arc<CachedTableData>> {
        self.table_cache.get(table_id).map(|entry| entry.value().clone())
    }

    /// Register a new or updated table definition (CREATE/ALTER)
    ///
    /// This handles the full lifecycle:
    /// 1. Persisting the latest schema version idempotently
    /// 2. Updating the cache (and DataFusion registry)
    pub fn register_table(&self, table_def: TableDefinition) -> Result<(), KalamDbError> {
        let app_ctx = self.app_context();
        let table_id =
            TableId::from_strings(table_def.namespace_id.as_str(), table_def.table_name.as_str());

        // 1. Persist latest schema definition without duplicating identical writes.
        let tables_provider = app_ctx.system_tables().tables();
        let persisted_new_version = tables_provider
            .upsert_table_version(&table_id, &table_def)
            .into_kalamdb_error("Failed to persist table definition")?;

        if persisted_new_version {
            log::info!("Registering table {} via SchemaRegistry", table_id);
        } else {
            log::debug!(
                "Table {} already has identical persisted schema; refreshing cache/provider only",
                table_id
            );
        }

        // 2. Update cache
        self.put(table_def)?;

        Ok(())
    }

    /// Insert table definition into cache and create provider
    ///
    /// - Creates CachedTableData
    /// - Creates matching TableProvider (User/Shared/Stream)
    /// - Registers with DataFusion
    pub fn put(&self, table_def: TableDefinition) -> Result<(), KalamDbError> {
        let table_id =
            TableId::from_strings(table_def.namespace_id.as_str(), table_def.table_name.as_str());

        // 1. Create CachedTableData
        let cached_data = Arc::new(CachedTableData::new(Arc::new(table_def.clone())));
        let previous_entry = self.table_cache.insert(table_id.clone(), Arc::clone(&cached_data));

        // 2. Bind provider into CachedTableData
        match table_def.table_type {
            TableType::System => {
                if table_def.namespace_id.is_system_namespace() {
                    if let Ok(system_table) = SystemTable::from_name(table_def.table_name.as_str())
                    {
                        if let Some(provider) = self
                            .app_context()
                            .system_tables()
                            .persisted_table_provider(system_table)
                        {
                            cached_data.set_provider(provider);
                        }
                    }
                }
            },
            _ => match self.create_table_provider(&table_def) {
                Ok(kalam_provider) => {
                    let table_provider =
                        Arc::clone(&kalam_provider) as Arc<dyn TableProvider + Send + Sync>;
                    cached_data.set_provider(Arc::clone(&table_provider));

                    // 3. Register with DataFusion immediately
                    self.register_with_datafusion(&table_id, table_provider)?;
                },
                Err(e) => {
                    log::error!("Failed to create provider for table {}: {}", table_id, e);
                    if let Some(previous) = previous_entry {
                        self.table_cache.insert(table_id, previous);
                    } else {
                        self.table_cache.remove(&table_id);
                    }
                    return Err(e);
                },
            },
        }

        Ok(())
    }

    fn build_column_defaults(&self, table_def: &TableDefinition) -> HashMap<String, Expr> {
        let base_state = self.app_context().base_session_context().state();
        let scalar_functions = base_state.scalar_functions();

        table_def
            .columns
            .iter()
            .filter_map(|column| {
                self.build_default_expr(&column.default_value, scalar_functions)
                    .map(|expr| (column.column_name.clone(), expr))
            })
            .collect()
    }

    fn build_default_expr(
        &self,
        default_value: &ColumnDefault,
        scalar_functions: &HashMap<String, Arc<datafusion::logical_expr::ScalarUDF>>,
    ) -> Option<Expr> {
        match default_value {
            ColumnDefault::None => None,
            ColumnDefault::Literal(json) => Some(Expr::Literal(json_value_to_scalar(json), None)),
            ColumnDefault::FunctionCall { name, args } => {
                let udf = Self::lookup_scalar_function(scalar_functions, name)?;
                let arg_exprs =
                    args.iter().map(|arg| Expr::Literal(json_value_to_scalar(arg), None)).collect();
                Some(Expr::ScalarFunction(ScalarFunctionExpr::new_udf(udf, arg_exprs)))
            },
        }
    }

    fn lookup_scalar_function(
        scalar_functions: &HashMap<String, Arc<datafusion::logical_expr::ScalarUDF>>,
        name: &str,
    ) -> Option<Arc<datafusion::logical_expr::ScalarUDF>> {
        if let Some(udf) = scalar_functions.get(name) {
            return Some(Arc::clone(udf));
        }

        let lower = name.to_lowercase();
        if let Some(udf) = scalar_functions.get(&lower) {
            return Some(Arc::clone(udf));
        }

        let upper = name.to_uppercase();
        scalar_functions.get(&upper).map(Arc::clone)
    }

    /// Internal helper to create a KalamTableProvider based on table definition
    fn create_table_provider(
        &self,
        table_def: &TableDefinition,
    ) -> Result<Arc<dyn kalamdb_tables::KalamTableProvider>, KalamDbError> {
        use crate::providers::{
            SharedTableProvider, StreamTableProvider, TableProviderCore, UserTableProvider,
        };
        use crate::schema_registry::TablesSchemaRegistryAdapter;
        use kalamdb_commons::schemas::TableOptions;
        use kalamdb_sharding::ShardRouter;
        use kalamdb_tables::{
            new_indexed_shared_table_store, new_indexed_user_table_store, new_stream_table_store,
            StreamTableStoreConfig,
        };

        let app_ctx = self.app_context();
        let table_id =
            TableId::from_strings(table_def.namespace_id.as_str(), table_def.table_name.as_str());
        let column_defaults = self.build_column_defaults(table_def);

        // Resolve PK field (required for User/Shared; Stream falls back to _seq)
        let pk_field = match table_def.table_type {
            TableType::Stream => table_def
                .columns
                .iter()
                .find(|c| c.is_primary_key)
                .map(|c| c.column_name.clone())
                .unwrap_or_else(|| SystemColumnNames::SEQ.to_string()),
            _ => table_def
                .columns
                .iter()
                .find(|c| c.is_primary_key)
                .map(|c| c.column_name.clone())
                .ok_or_else(|| {
                    KalamDbError::InvalidOperation(format!(
                        "Table {} has no primary key defined",
                        table_id
                    ))
                })?,
        };

        let tables_schema_registry =
            Arc::new(TablesSchemaRegistryAdapter::new(app_ctx.schema_registry()));

        // Build shared TableServices (one Arc for all provider types)
        let services = Arc::new(kalamdb_tables::utils::TableServices::new(
            tables_schema_registry.clone(),
            app_ctx.system_columns_service(),
            Some(app_ctx.storage_registry()),
            app_ctx.manifest_service(),
            Arc::clone(app_ctx.notification_service())
                as Arc<dyn NotificationService<Notification = ChangeNotification>>,
            app_ctx.clone(),
            app_ctx.commit_sequence_tracker(),
            Some(app_ctx.topic_publisher() as Arc<dyn kalamdb_system::TopicPublisher>),
        ));

        // Get Arrow schema from registry (cached at core level)
        let arrow_schema = tables_schema_registry.get_arrow_schema(&table_id).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to get Arrow schema for {}: {}",
                table_id, e
            ))
        })?;

        // Wrap table_def in Arc for sharing across core (avoids cloning TableDefinition)
        let table_def_arc = Arc::new(table_def.clone());

        match table_def.table_type {
            TableType::User => {
                let user_table_store = Arc::new(new_indexed_user_table_store(
                    app_ctx.storage_backend(),
                    &table_id,
                    &pk_field,
                ));

                let core = Arc::new(TableProviderCore::new(
                    table_def_arc,
                    services,
                    pk_field,
                    arrow_schema,
                    column_defaults,
                ));

                let provider = Arc::new(UserTableProvider::new(core, user_table_store));
                Ok(provider as Arc<dyn kalamdb_tables::KalamTableProvider>)
            },
            TableType::Shared => {
                let shared_store = Arc::new(new_indexed_shared_table_store(
                    app_ctx.storage_backend(),
                    &table_id,
                    &pk_field,
                ));

                let core = Arc::new(TableProviderCore::new(
                    table_def_arc,
                    services,
                    pk_field,
                    arrow_schema,
                    column_defaults,
                ));

                let provider = Arc::new(SharedTableProvider::new(core, shared_store));
                Ok(provider as Arc<dyn kalamdb_tables::KalamTableProvider>)
            },
            TableType::Stream => {
                let ttl_seconds = if let TableOptions::Stream(opts) = &table_def.table_options {
                    opts.ttl_seconds
                } else {
                    3600 // Default fallback
                };

                let streams_root = app_ctx.config().storage.streams_dir();
                let base_dir = streams_root
                    .join(table_id.namespace_id().as_str())
                    .join(table_id.table_name().as_str());

                let stream_store = Arc::new(new_stream_table_store(
                    &table_id,
                    StreamTableStoreConfig {
                        base_dir,
                        max_rows_per_user: 256, // Default per-user retention limit
                        shard_router: ShardRouter::default_config(),
                        ttl_seconds: Some(ttl_seconds),
                        storage_mode: kalamdb_tables::StreamTableStorageMode::File,
                    },
                ));

                let core = Arc::new(TableProviderCore::new(
                    table_def_arc,
                    services,
                    pk_field,
                    arrow_schema,
                    column_defaults,
                ));

                let provider =
                    Arc::new(StreamTableProvider::new(core, stream_store, Some(ttl_seconds)));
                Ok(provider as Arc<dyn kalamdb_tables::KalamTableProvider>)
            },
            TableType::System => Err(KalamDbError::InvalidOperation(format!(
                "Cannot create provider for system table {} via SchemaRegistry",
                table_id
            ))),
        }
    }

    /// Insert fully initialized cached table data into the cache
    pub fn insert_cached(&self, table_id: TableId, data: Arc<CachedTableData>) {
        self.table_cache.insert(table_id, data);
    }

    /// Invalidate (remove) cached table data
    pub fn invalidate(&self, table_id: &TableId) {
        self.table_cache.remove(table_id);
        let _ = self.deregister_from_datafusion(table_id);
    }

    /// Invalidate all versions of a table (for DROP TABLE)
    pub fn invalidate_all_versions(&self, table_id: &TableId) {
        // Remove from latest cache
        self.table_cache.remove(table_id);

        // Remove all versioned entries for this table
        let keys_to_remove: Vec<TableVersionId> = self
            .version_cache
            .iter()
            .filter(|entry| entry.key().table_id() == table_id)
            .map(|entry| entry.key().clone())
            .collect();

        for key in &keys_to_remove {
            self.version_cache.remove(key);
            self.version_cache_access.remove(key);
        }

        let _ = self.deregister_from_datafusion(table_id);
    }

    // ===== Versioned Cache Methods (Phase 16) =====

    /// Get cached table data for a specific version
    ///
    /// Used when reading Parquet files written with older schemas.
    pub fn get_version(&self, version_id: &TableVersionId) -> Option<Arc<CachedTableData>> {
        let result = self.version_cache.get(version_id).map(|entry| entry.value().clone());
        if result.is_some() {
            // Update LRU access time
            let ts = self.version_cache_counter.fetch_add(1, Ordering::Relaxed);
            self.version_cache_access.insert(version_id.clone(), ts);
        }
        result
    }

    /// Insert a specific version into the cache, evicting LRU entries if over limit.
    pub fn insert_version(&self, version_id: TableVersionId, data: Arc<CachedTableData>) {
        let ts = self.version_cache_counter.fetch_add(1, Ordering::Relaxed);
        self.version_cache.insert(version_id.clone(), data);
        self.version_cache_access.insert(version_id, ts);

        // Evict oldest entries if over limit
        if self.version_cache.len() > VERSION_CACHE_MAX_ENTRIES {
            self.evict_version_cache_lru();
        }
    }

    /// Evict the oldest ~25% of version_cache entries by LRU order.
    fn evict_version_cache_lru(&self) {
        let target = VERSION_CACHE_MAX_ENTRIES * 3 / 4; // keep 75%
        let excess = self.version_cache.len().saturating_sub(target);
        if excess == 0 {
            return;
        }

        // Collect (key, access_ts) pairs
        let mut entries: Vec<(TableVersionId, u64)> = self
            .version_cache_access
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();

        // Sort ascending by access time (oldest first)
        entries.sort_by_key(|(_k, ts)| *ts);

        // Evict the oldest `excess` entries
        let evict_count = std::cmp::min(excess, entries.len());
        for (key, _) in entries.into_iter().take(evict_count) {
            self.version_cache.remove(&key);
            self.version_cache_access.remove(&key);
        }

        log::debug!(
            "[SchemaRegistry] Evicted {} version_cache entries, remaining={}",
            evict_count,
            self.version_cache.len()
        );
    }

    /// Get cache statistics
    pub fn stats(&self) -> usize {
        self.table_cache.len()
    }

    /// Clear all cached data
    pub fn clear(&self) {
        self.table_cache.clear();
        self.version_cache.clear();
        self.version_cache_access.clear();
    }

    /// Get number of cached entries (latest versions only)
    pub fn len(&self) -> usize {
        self.table_cache.len()
    }

    /// Get total number of cached entries (latest + versioned)
    pub fn total_len(&self) -> usize {
        (self.table_cache.len() + self.version_cache.len()) as usize
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.table_cache.len() == 0 && self.version_cache.len() == 0
    }

    // ===== Provider Methods (consolidated from ProviderRegistry) =====

    /// Insert a DataFusion provider into the cache for a table
    ///
    /// Stores the provider in CachedTableData and registers with DataFusion's
    /// catalog.
    pub fn insert_provider(
        &self,
        table_id: TableId,
        provider: Arc<dyn TableProvider + Send + Sync>,
    ) -> Result<(), KalamDbError> {
        log::debug!("[SchemaRegistry] Inserting provider for table {}", table_id);

        // Store in CachedTableData (as system provider — no DML support)
        if let Some(cached) = self.get(&table_id) {
            cached.set_provider(provider.clone());
        } else {
            // Table not in cache - try to load from persistence first
            if let Some(_table_def) = self.get_table_if_exists(&table_id)? {
                if let Some(cached) = self.get(&table_id) {
                    cached.set_provider(provider.clone());
                } else {
                    return Err(KalamDbError::TableNotFound(format!(
                        "Cannot insert provider: table {} not in cache",
                        table_id
                    )));
                }
            } else {
                return Err(KalamDbError::TableNotFound(format!(
                    "Cannot insert provider: table {} not found",
                    table_id
                )));
            }
        }

        // Register with DataFusion's catalog if available
        self.register_with_datafusion(&table_id, provider)?;

        Ok(())
    }

    /// Remove a cached DataFusion provider for a table and unregister from DataFusion
    pub fn remove_provider(&self, table_id: &TableId) -> Result<(), KalamDbError> {
        // Clear from CachedTableData
        if let Some(cached) = self.get(table_id) {
            cached.clear_provider();
        }

        // Deregister from DataFusion
        self.deregister_from_datafusion(table_id)
    }

    /// Get a cached DataFusion provider for a table
    pub fn get_provider(&self, table_id: &TableId) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        let result = self.get(table_id).and_then(|cached| cached.get_provider());
        if result.is_some() {
            log::trace!("[SchemaRegistry] Retrieved provider for table {}", table_id);
        } else {
            log::warn!("[SchemaRegistry] Provider NOT FOUND for table {}", table_id);
        }
        result
    }

    /// Register a provider with DataFusion's catalog
    ///
    /// If the table already exists (e.g., during ALTER TABLE), it will be
    /// deregistered first, then re-registered with the new provider.
    fn register_with_datafusion(
        &self,
        table_id: &TableId,
        provider: Arc<dyn TableProvider + Send + Sync>,
    ) -> Result<(), KalamDbError> {
        if let Some(base_session) = self.base_session_context.get() {
            // Use constant catalog name "kalam"
            let catalog = base_session.catalog("kalam").ok_or_else(|| {
                KalamDbError::InvalidOperation("Catalog 'kalam' not found".to_string())
            })?;

            // Get or create namespace schema
            let schema = catalog.schema(table_id.namespace_id().as_str()).unwrap_or_else(|| {
                // Create namespace schema if it doesn't exist
                let new_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
                catalog
                    .register_schema(table_id.namespace_id().as_str(), new_schema.clone())
                    .expect("Failed to register namespace schema");
                new_schema
            });

            // For ALTER TABLE: always deregister first (if exists), then register with new provider
            // This ensures the new schema is always visible
            let table_name = table_id.table_name().as_str();

            // Check if table already exists - if so, deregister it first
            if schema.table_exist(table_name) {
                log::debug!(
                    "[SchemaRegistry] Table {} already registered in DataFusion; deregistering before re-registration",
                    table_id
                );
                match schema.deregister_table(table_name) {
                    Ok(Some(_old_provider)) => {
                        log::debug!(
                            "[SchemaRegistry] Successfully deregistered old provider for {}",
                            table_id
                        );
                    },
                    Ok(None) => {
                        // Table existed but deregister returned None - shouldn't happen but handle it
                        log::warn!(
                            "[SchemaRegistry] table_exist returned true but deregister_table returned None for {}",
                            table_id
                        );
                    },
                    Err(e) => {
                        return Err(KalamDbError::InvalidOperation(format!(
                            "Failed to deregister existing table {} from DataFusion: {}",
                            table_id, e
                        )));
                    },
                }
            }

            // Now register the new provider - table should not exist at this point
            log::debug!(
                "[SchemaRegistry] Registering table {} (schema cols: {})",
                table_id,
                provider.schema().fields().len()
            );

            schema.register_table(table_name.to_string(), provider).map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to register table {} with DataFusion: {}",
                    table_id, e
                ))
            })?;

            log::debug!("[SchemaRegistry] Registered table {} with DataFusion catalog", table_id);
        }

        Ok(())
    }

    /// Deregister a table from DataFusion's catalog
    fn deregister_from_datafusion(&self, table_id: &TableId) -> Result<(), KalamDbError> {
        if let Some(base_session) = self.base_session_context.get() {
            let catalog_name =
                base_session.state().config().options().catalog.default_catalog.clone();

            let catalog = base_session.catalog(&catalog_name).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!("Catalog '{}' not found", catalog_name))
            })?;

            // Get namespace schema
            if let Some(schema) = catalog.schema(table_id.namespace_id().as_str()) {
                // Deregister table from DataFusion
                schema.deregister_table(table_id.table_name().as_str()).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to deregister table from DataFusion: {}",
                        e
                    ))
                })?;

                log::debug!("Unregistered table {} from DataFusion catalog", table_id);
            }
        }

        Ok(())
    }

    /// Store table definition in cache (persistence handled by caller)
    pub fn put_table_definition(
        &self,
        table_id: &TableId,
        table_def: &TableDefinition,
    ) -> Result<(), KalamDbError> {
        let app_ctx = self.app_context();
        let table_arc = Arc::new(table_def.clone());
        let data = CachedTableData::from_table_definition(app_ctx, table_id, table_arc)?;
        self.insert_cached(table_id.clone(), Arc::new(data));

        Ok(())
    }

    /// Delete table definition from persistence layer (delete-through pattern)
    pub fn delete_table_definition(&self, table_id: &TableId) -> Result<(), KalamDbError> {
        let tables_provider = self.app_context().system_tables().tables();

        // Delete from storage
        tables_provider.delete_table(table_id)?;

        // Invalidate cache
        self.invalidate_all_versions(table_id);

        Ok(())
    }

    /// Scan all table definitions from persistence layer
    pub fn scan_all_table_definitions(&self) -> Result<Vec<TableDefinition>, KalamDbError> {
        let tables_provider = self.app_context().system_tables().tables();

        // Scan all tables from storage
        let all_entries = tables_provider.scan_all().into_kalamdb_error("Failed to scan tables")?;
        Ok(all_entries)
    }

    /// Return a snapshot of the currently cached table definitions.
    ///
    /// This is useful for extension or runtime glue that needs visibility into
    /// newly registered tables before a persistence-backed rescan occurs.
    pub fn cached_table_definitions(&self) -> Vec<TableDefinition> {
        self.table_cache
            .iter()
            .map(|entry| entry.value().table.as_ref().clone())
            .collect()
    }

    /// Get table definition if it exists (optimized single-call pattern)
    ///
    /// Combines table existence check + definition fetch in one operation.
    ///
    /// # Performance
    /// - Cache hit: Returns immediately (no duplicate lookups)
    /// - Cache miss: Single persistence query + cache population
    /// - Prevents double fetch: table_exists() then get_table_if_exists()
    ///
    /// # Example
    /// ```no_run
    /// // ❌ OLD: Two lookups (inefficient)
    /// if schema_registry.table_exists(&table_id)? {
    ///     let def = schema_registry.get_table_if_exists(&table_id)?;
    /// }
    ///
    /// // ✅ NEW: Single lookup (efficient)
    /// if let Some(def) = schema_registry.get_table_if_exists(&table_id)? {
    ///     // use def
    /// }
    /// ```
    pub fn get_table_if_exists(
        &self,
        table_id: &TableId,
    ) -> Result<Option<Arc<TableDefinition>>, KalamDbError> {
        let app_ctx = self.app_context();
        // Fast path: check cache
        if let Some(cached) = self.get(table_id) {
            return Ok(Some(Arc::clone(&cached.table)));
        }

        let tables_provider = app_ctx.system_tables().tables();

        match tables_provider.get_table_by_id(table_id)? {
            Some(table_def) => {
                let table_arc = Arc::new(table_def);
                let data = CachedTableData::from_table_definition(
                    app_ctx.as_ref(),
                    table_id,
                    table_arc.clone(),
                )?;
                self.insert_cached(table_id.clone(), Arc::new(data));
                Ok(Some(table_arc))
            },
            None => Ok(None),
        }
    }

    /// Async version of get_table_if_exists (avoids blocking RocksDB reads in async context)
    ///
    /// Under high load, synchronous RocksDB operations can starve the tokio runtime.
    /// This async version uses spawn_blocking to prevent runtime starvation.
    pub async fn get_table_if_exists_async(
        &self,
        table_id: &TableId,
    ) -> Result<Option<Arc<TableDefinition>>, KalamDbError> {
        let app_ctx = self.app_context();
        // Fast path: check cache
        if let Some(cached) = self.get(table_id) {
            return Ok(Some(Arc::clone(&cached.table)));
        }

        let tables_provider = app_ctx.system_tables().tables();

        // Use async version to avoid blocking the runtime
        match tables_provider.get_table_by_id_async(table_id).await? {
            Some(table_def) => {
                let table_arc = Arc::new(table_def);
                let data = CachedTableData::from_table_definition(
                    app_ctx.as_ref(),
                    table_id,
                    table_arc.clone(),
                )?;
                self.insert_cached(table_id.clone(), Arc::new(data));
                Ok(Some(table_arc))
            },
            None => Ok(None),
        }
    }

    /// Get Arrow schema for a table
    ///
    /// Directly accesses the memoized Arrow schema from CachedTableData.
    /// The schema is computed once on first access and cached thereafter.
    ///
    /// **Performance**: First call ~75μs (computation), subsequent calls ~1.5μs (cached)
    pub fn get_arrow_schema(
        &self,
        table_id: &TableId,
    ) -> Result<Arc<arrow::datatypes::Schema>, KalamDbError> {
        // Fast path: check cache
        if let Some(cached) = self.get(table_id) {
            return cached.arrow_schema();
        }

        // Slow path: try to load from persistence (lazy loading)
        if self.get_table_if_exists(table_id)?.is_some() {
            // Cache is now populated - retrieve it
            if let Some(cached) = self.get(table_id) {
                return cached.arrow_schema();
            }
        }

        Err(KalamDbError::TableNotFound(format!("Table not found: {}", table_id)))
    }

    /// Get storage ID for a table
    pub fn get_storage_id(&self, table_id: &TableId) -> Result<StorageId, KalamDbError> {
        if let Some(cached) = self.get(table_id) {
            return Ok(cached.storage_id.clone());
        }

        if let Some(table_def) = self.get_table_if_exists(table_id)? {
            if let Some(storage_id) = CachedTableData::extract_storage_id(&table_def) {
                return Ok(storage_id);
            }
        }

        Err(KalamDbError::TableNotFound(format!("Table not found: {}", table_id)))
    }

    /// Get Arrow schema for a specific table version (for reading old Parquet files)
    ///
    /// Uses version-specific cache to avoid repeated schema conversions when reading
    /// multiple Parquet files written with the same historical schema version.
    pub fn get_arrow_schema_for_version(
        &self,
        table_id: &TableId,
        schema_version: u32,
    ) -> Result<Arc<arrow::datatypes::Schema>, KalamDbError> {
        let app_ctx = self.app_context();
        let version_id = TableVersionId::versioned(table_id.clone(), schema_version);

        // Fast path: check version cache
        if let Some(cached) = self.get_version(&version_id) {
            return cached.arrow_schema();
        }

        // Slow path: load from versioned tables store and cache
        let table_def = app_ctx
            .system_tables()
            .tables()
            .get_version(table_id, schema_version)
            .map_err(|e| {
                KalamDbError::Other(format!(
                    "Failed to retrieve schema version {}: {}",
                    schema_version, e
                ))
            })?
            .ok_or_else(|| {
                KalamDbError::Other(format!(
                    "Schema version {} not found for table {}",
                    schema_version, table_id
                ))
            })?;

        // Create cached data and compute arrow schema
        let cached_data = CachedTableData::new(Arc::new(table_def));
        let arrow_schema = cached_data.arrow_schema()?;

        // Cache for future lookups
        self.insert_version(version_id, Arc::new(cached_data));

        Ok(arrow_schema)
    }
}

impl SchemaRegistryTrait for SchemaRegistry {
    type Error = KalamDbError;

    fn get_arrow_schema(&self, table_id: &TableId) -> Result<SchemaRef, Self::Error> {
        self.get_arrow_schema(table_id)
    }

    fn get_table_if_exists(
        &self,
        table_id: &TableId,
    ) -> Result<Option<Arc<TableDefinition>>, Self::Error> {
        self.get_table_if_exists(table_id)
    }

    fn get_arrow_schema_for_version(
        &self,
        table_id: &TableId,
        schema_version: u32,
    ) -> Result<SchemaRef, Self::Error> {
        self.get_arrow_schema_for_version(table_id, schema_version)
    }

    fn get_storage_id(&self, table_id: &TableId) -> Result<StorageId, Self::Error> {
        self.get_storage_id(table_id)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new(10000) // Default max size: 10,000 tables
    }
}

#[cfg(test)]
mod tests {
    use super::SchemaRegistry;
    use chrono::{Duration, Utc};
    use kalamdb_commons::datatypes::KalamDataType;
    use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition};
    use kalamdb_commons::schemas::{ColumnDefault, TableOptions, TableType};
    use kalamdb_commons::{NamespaceId, TableName};

    fn base_table_definition() -> TableDefinition {
        TableDefinition::new(
            NamespaceId::system(),
            TableName::new("reconcile_test"),
            TableType::System,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Uuid),
                ColumnDefinition::new(
                    2,
                    "name",
                    2,
                    KalamDataType::Text,
                    true,
                    false,
                    false,
                    ColumnDefault::None,
                    None,
                ),
            ],
            TableOptions::system(),
            None,
        )
        .expect("failed to build test table definition")
    }

    #[test]
    fn test_schema_semantically_equal_ignores_timestamps_and_version() {
        let mut current = base_table_definition();
        current.schema_version = 9;
        current.created_at = Utc::now() - Duration::days(30);
        current.updated_at = Utc::now() - Duration::days(2);
        let expected = base_table_definition();

        assert!(SchemaRegistry::schema_semantically_equal(&current, &expected));
    }

    #[test]
    fn test_validate_schema_evolution_accepts_additive_nullable_column() {
        let current = base_table_definition();
        let mut expected = current.clone();
        expected
            .add_column(ColumnDefinition::new(
                3,
                "description",
                3,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                None,
            ))
            .expect("failed to add test column");

        let validation = SchemaRegistry::validate_schema_evolution(&current, &expected);
        assert!(validation.is_ok(), "{validation:?}");
    }

    #[test]
    fn test_validate_schema_evolution_rejects_non_nullable_added_column_without_default() {
        let current = base_table_definition();
        let mut expected = current.clone();
        expected
            .add_column(ColumnDefinition::new(
                3,
                "status",
                3,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                None,
            ))
            .expect("failed to add test column");

        let validation = SchemaRegistry::validate_schema_evolution(&current, &expected);
        assert!(validation.is_err());
    }

    #[test]
    fn test_validate_schema_evolution_rejects_primary_key_change() {
        let current = base_table_definition();
        let mut expected = current.clone();
        expected.columns[0].is_primary_key = false;

        let validation = SchemaRegistry::validate_schema_evolution(&current, &expected);
        assert!(validation.is_err());
    }

    #[test]
    fn test_validate_schema_evolution_rejects_incompatible_type_change() {
        let current = base_table_definition();
        let mut expected = current.clone();
        expected.columns[1].data_type = KalamDataType::Int;

        let validation = SchemaRegistry::validate_schema_evolution(&current, &expected);
        assert!(validation.is_err());
    }

    #[test]
    fn test_validate_schema_evolution_accepts_safe_numeric_widening() {
        let mut current = base_table_definition();
        current.columns[1].data_type = KalamDataType::Int;
        let mut expected = current.clone();
        expected.columns[1].data_type = KalamDataType::BigInt;

        let validation = SchemaRegistry::validate_schema_evolution(&current, &expected);
        assert!(validation.is_ok(), "{validation:?}");
    }

    #[test]
    fn test_build_reconciled_definition_bumps_version_and_preserves_created_at() {
        let mut current = base_table_definition();
        current.schema_version = 3;
        let created_at = current.created_at;

        let mut expected = current.clone();
        expected
            .add_column(ColumnDefinition::new(
                3,
                "description",
                3,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                None,
            ))
            .expect("failed to add column");

        let upgraded = SchemaRegistry::build_reconciled_definition(&current, &expected);

        assert_eq!(upgraded.schema_version, 4);
        assert_eq!(upgraded.created_at, created_at);
        assert!(SchemaRegistry::schema_semantically_equal(&upgraded, &expected));
    }
}
