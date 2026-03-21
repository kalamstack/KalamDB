//! AppContext for KalamDB (Phase 5, T204)
//!
//! Provides access to all core resources with simplified 3-parameter initialization.
//! Uses constants from kalamdb_commons for table prefixes.

use crate::applier::UnifiedApplier;
use crate::error_extensions::KalamDbResultExt;
use crate::jobs::executors::{
    BackupExecutor, CleanupExecutor, CompactExecutor, FlushExecutor, JobCleanupExecutor,
    JobRegistry, RestoreExecutor, RetentionExecutor, StreamEvictionExecutor, TopicCleanupExecutor,
    TopicRetentionExecutor, UserCleanupExecutor, UserExportExecutor, VectorIndexExecutor,
};
use crate::live::notification::NotificationService;
use crate::live::ConnectionsManager;
use crate::live::TopicPublisherService;
use crate::live_query::LiveQueryManager;
use crate::schema_registry::SchemaRegistry;
use crate::sql::datafusion_session::DataFusionSessionFactory;
use crate::sql::executor::SqlExecutor;
use crate::sql::table_functions::{CoreVectorSearchRuntime, VectorSearchTableFunction};
use crate::views::system_schema_provider::SystemSchemaProvider;
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::prelude::SessionContext;
use kalamdb_commons::constants::SYSTEM_NAMESPACE;
use kalamdb_commons::models::{NamespaceId, UserId};
use kalamdb_commons::{constants::ColumnFamilyNames, NodeId};
use kalamdb_configs::ServerConfig;
use kalamdb_filestore::StorageRegistry;
use kalamdb_pg::KalamPgService;
use kalamdb_raft::CommandExecutor;
use kalamdb_sharding::{GroupId, ShardRouter};
use kalamdb_store::StorageBackend;
use kalamdb_system::{ClusterCoordinator, Job, Namespace, SystemTablesRegistry};
use kalamdb_tables::{SharedTableStore, UserTableStore};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metrics::runtime::collect_runtime_metrics;
use crate::schema_registry::TablesSchemaRegistryAdapter;

// Use RwLock instead of OnceLock to allow resetting in tests
// The RwLock is wrapped in a OnceLock for lazy initialization
/// AppContext
///
/// Central registry of all shared resources. Services and executors fetch
/// dependencies via explicit passing instead of global access, enabling:
/// - Zero duplication across layers
/// - Stateless services (zero-sized structs)
/// - Memory-efficient per-request operations
/// - Single source of truth for all shared state
pub struct AppContext {
    /// Node identifier loaded once from server.toml (Phase 10, US0, FR-000)
    /// Wrapped in Arc for zero-copy sharing across all components
    node_id: Arc<NodeId>,

    /// Server configuration loaded once at startup (Phase 11, T062-T064)
    /// Provides access to all config settings (execution, limits, storage, etc.)
    config: Arc<ServerConfig>,

    // ===== Caches =====
    schema_registry: Arc<SchemaRegistry>,

    // ===== Stores =====
    user_table_store: Arc<UserTableStore>,
    shared_table_store: Arc<SharedTableStore>,

    // ===== Core Infrastructure =====
    storage_backend: Arc<dyn StorageBackend>,

    // ===== Managers =====
    job_manager: OnceCell<Arc<crate::jobs::JobsManager>>,
    live_query_manager: OnceCell<Arc<LiveQueryManager>>,

    // ===== Notification Service =====
    notification_service: Arc<NotificationService>,

    // ===== Connections Manager (WebSocket connections, subscriptions, heartbeat) =====
    connection_registry: Arc<ConnectionsManager>,

    // ===== Registries =====
    storage_registry: Arc<StorageRegistry>,

    // ===== DataFusion Session Management =====
    session_factory: Arc<DataFusionSessionFactory>,
    base_session_context: Arc<SessionContext>,

    // ===== System Tables Registry =====
    system_tables: Arc<SystemTablesRegistry>,

    // ===== Command Executor (Standalone or Raft-based) =====
    /// Command executor abstraction for standalone/cluster mode
    /// In standalone mode: StandaloneExecutor (direct provider calls)
    /// In cluster mode: RaftExecutor (commands go through Raft consensus)
    executor: Arc<dyn CommandExecutor>,

    // ===== System Columns Service =====
    system_columns_service: Arc<crate::system_columns::SystemColumnsService>,

    // ===== Slow Query Logger =====
    slow_query_logger: Arc<crate::slow_query_logger::SlowQueryLogger>,

    // ===== Manifest Service (unified: hot cache + RocksDB + cold storage) =====
    manifest_service: Arc<crate::manifest::ManifestService>,

    // ===== File Storage Service (for FILE datatype) =====
    file_storage_service: Arc<kalamdb_filestore::FileStorageService>,

    // ===== Topic Pub/Sub Infrastructure =====
    /// Unified topic publisher service - owns message/offset stores and topic registry
    topic_publisher: Arc<TopicPublisherService>,

    // ===== Unified Applier (single execution path for all commands) =====
    applier: OnceCell<Arc<dyn UnifiedApplier>>,

    // ===== Shared SqlExecutor =====
    sql_executor: OnceCell<Arc<SqlExecutor>>,

    // ===== Server Start Time (for uptime calculation) =====
    server_start_time: Instant,
}

impl std::fmt::Debug for AppContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppContext")
            .field("schema_registry", &"Arc<SchemaRegistry>")
            .field("user_table_store", &"Arc<UserTableStore>")
            .field("shared_table_store", &"Arc<SharedTableStore>")
            .field("storage_backend", &"Arc<dyn StorageBackend>")
            .field("job_manager", &"OnceCell<Arc<JobsManager>>")
            .field("live_query_manager", &"OnceCell<Arc<LiveQueryManager>>")
            .field("connection_registry", &"Arc<ConnectionsManager>")
            .field("storage_registry", &"Arc<StorageRegistry>")
            .field("session_factory", &"Arc<DataFusionSessionFactory>")
            .field("base_session_context", &"Arc<SessionContext>")
            .field("system_tables", &"Arc<SystemTablesRegistry>")
            .field("executor", &"Arc<dyn CommandExecutor>")
            .field("applier", &"OnceCell<Arc<dyn UnifiedApplier>>")
            .field("system_columns_service", &"Arc<SystemColumnsService>")
            .field("slow_query_logger", &"Arc<SlowQueryLogger>")
            .field("manifest_service", &"Arc<ManifestService>")
            .field("topic_publisher", &"Arc<TopicPublisherService>")
            .field("sql_executor", &"OnceCell<Arc<SqlExecutor>>")
            .finish()
    }
}

impl AppContext {
    /// Initialize AppContext with config-driven NodeId
    ///
    /// Creates all dependencies internally using constants from kalamdb_commons.
    /// Table prefixes are read from constants::USER_TABLE_PREFIX, etc.
    ///
    /// **Phase 10, US0 (FR-000)**: NodeId is now loaded from config and wrapped in Arc
    /// for zero-copy sharing across all components, eliminating duplicate instantiations.
    ///
    /// # Parameters
    /// - `storage_backend`: Storage abstraction (RocksDB implementation)
    /// - `node_id`: Node identifier loaded from server.toml (wrapped in Arc internally)
    /// - `storage_base_path`: Base directory for storage files
    ///
    /// # Example
    /// ```no_run
    /// use kalamdb_core::app_context::AppContext;
    /// use kalamdb_commons::NodeId;
    /// # use kalamdb_store::StorageBackend;
    /// # use std::sync::Arc;
    ///
    /// let backend: Arc<dyn StorageBackend> = todo!();
    /// let node_id = NodeId::new("prod-node-1".to_string()); // From server.toml
    /// let config = ServerConfig::from_file("server.toml").unwrap();
    /// AppContext::init(
    ///     backend,
    ///     node_id,
    ///     "data/storage".to_string(),
    ///     config,
    /// );
    /// ```
    pub fn init(
        storage_backend: Arc<dyn StorageBackend>,
        node_id: NodeId,
        storage_base_path: String,
        config: ServerConfig,
    ) -> Arc<AppContext> {
        Self::create_impl(storage_backend, node_id, storage_base_path, config)
    }

    /// Create an isolated AppContext instance for tests
    ///
    /// This is essential for test isolation where each test needs its own
    /// independent state (separate RocksDB, Raft groups, etc.).
    ///
    /// **Warning**: Only use this in tests! Production code should use `init()`.
    pub fn create_isolated(
        storage_backend: Arc<dyn StorageBackend>,
        node_id: NodeId,
        storage_base_path: String,
        config: ServerConfig,
    ) -> Arc<AppContext> {
        Self::create_impl(storage_backend, node_id, storage_base_path, config)
    }

    /// Internal implementation that creates an AppContext
    ///
    /// Extracted to allow both singleton and isolated initialization.
    fn create_impl(
        storage_backend: Arc<dyn StorageBackend>,
        node_id: NodeId,
        storage_base_path: String,
        config: ServerConfig,
    ) -> Arc<AppContext> {
        let node_id = Arc::new(node_id); // Wrap NodeId in Arc for zero-copy sharing (FR-000)
        let config = Arc::new(config); // Wrap config in Arc for zero-copy sharing
        {
            // Create stores using constants from kalamdb_commons
            let user_table_store = Arc::new(UserTableStore::new(
                storage_backend.clone(),
                ColumnFamilyNames::USER_TABLE_PREFIX.to_string(),
            ));
            let shared_table_store = Arc::new(SharedTableStore::new(
                storage_backend.clone(),
                ColumnFamilyNames::SHARED_TABLE_PREFIX.to_string(),
            ));

            // Create system table providers registry FIRST (needed by StorageRegistry and information_schema)
            let system_tables = Arc::new(SystemTablesRegistry::new(storage_backend.clone()));

            // Create storage registry (uses StoragesTableProvider from system_tables)
            let storage_registry = Arc::new(StorageRegistry::new(
                system_tables.storages(),
                storage_base_path,
                config.storage.remote_timeouts.clone(),
            ));

            // Create schema cache (Phase 10 unified cache)
            let schema_registry = Arc::new(SchemaRegistry::new(10000));

            // Create system schema provider (views created lazily on first access,
            // stored in SchemaRegistry's CachedTableData)
            let system_schema = Arc::new(SystemSchemaProvider::new(
                Arc::clone(&schema_registry),
                Arc::clone(&system_tables),
                Arc::clone(&config),
                std::path::PathBuf::from(&config.logging.logs_path),
            ));

            // Get stats_view reference for callback wiring later
            let stats_view = system_schema.stats_view();

            // Register all system tables in DataFusion
            // Use config-driven DataFusion settings for parallelism
            let session_factory = Arc::new(
                DataFusionSessionFactory::with_config(&config.datafusion)
                    .expect("Failed to create DataFusion session factory"),
            );
            let base_session_context = Arc::new(session_factory.create_session());

            // Wire up SchemaRegistry with base_session_context for automatic table registration
            schema_registry.set_base_session_context(Arc::clone(&base_session_context));

            // Register system schema with lazy loading
            // Use constant catalog name "kalam" - configured in DataFusionSessionFactory
            let catalog = base_session_context
                    .catalog("kalam")
                    .expect("Catalog 'kalam' not found - ensure DataFusionSessionFactory is properly configured");

            // Register the system schema provider with the catalog
            // Views are created on first access, not eagerly at startup
            catalog
                .register_schema(
                    SYSTEM_NAMESPACE,
                    Arc::clone(&system_schema) as Arc<dyn SchemaProvider>,
                )
                .expect("Failed to register system schema");

            // Register existing namespaces as DataFusion schemas
            // This ensures all namespaces persisted in RocksDB are available for SQL queries
            let namespaces_provider = system_tables.namespaces();

            let default_namespace_id = NamespaceId::default();
            if namespaces_provider
                .get_namespace(&default_namespace_id)
                .expect("Failed to read namespaces")
                .is_none()
            {
                let default_namespace = Namespace::new(default_namespace_id.as_str());
                namespaces_provider
                    .create_namespace(default_namespace)
                    .expect("Failed to create default namespace");
            }

            if let Ok(namespaces) = namespaces_provider.list_namespaces() {
                let namespace_names: Vec<String> =
                    namespaces.iter().map(|ns| ns.namespace_id.as_str().to_string()).collect();
                session_factory.register_namespaces(&base_session_context, &namespace_names);
            }

            // Note: information_schema.tables and information_schema.columns are provided
            // by DataFusion's built-in support (enabled via .with_information_schema(true))

            // Create job registry and register all 8 executors (Phase 9, T154)
            let job_registry = Arc::new(JobRegistry::new());
            job_registry.register(Arc::new(FlushExecutor::new()));
            job_registry.register(Arc::new(CleanupExecutor::new()));
            job_registry.register(Arc::new(JobCleanupExecutor::new()));
            job_registry.register(Arc::new(RetentionExecutor::new()));
            job_registry.register(Arc::new(StreamEvictionExecutor::new()));
            job_registry.register(Arc::new(UserCleanupExecutor::new()));
            job_registry.register(Arc::new(CompactExecutor::new()));
            job_registry.register(Arc::new(BackupExecutor::new()));
            job_registry.register(Arc::new(RestoreExecutor::new()));
            job_registry.register(Arc::new(TopicRetentionExecutor::new()));
            job_registry.register(Arc::new(TopicCleanupExecutor::new()));
            job_registry.register(Arc::new(UserExportExecutor::new()));
            job_registry.register(Arc::new(VectorIndexExecutor::new()));

            // Create unified job manager (Phase 9, T154)
            let jobs_provider = system_tables.jobs();
            let job_nodes_provider = system_tables.job_nodes();

            // Create connections manager (unified WebSocket connection management)
            // Timeouts from config or defaults
            let client_timeout =
                Duration::from_secs(config.websocket.client_timeout_secs.unwrap_or(30));
            let auth_timeout =
                Duration::from_secs(config.websocket.auth_timeout_secs.unwrap_or(10));
            let heartbeat_interval =
                Duration::from_secs(config.websocket.heartbeat_interval_secs.unwrap_or(5));
            let ws_max_connections = config.performance.max_connections;
            let connection_registry = ConnectionsManager::with_max_connections(
                *node_id,
                client_timeout,
                auth_timeout,
                heartbeat_interval,
                ws_max_connections,
            );

            // Create slow query logger (Phase 11)
            let slow_log_path = format!("{}/slow.log", config.logging.logs_path);
            let slow_query_logger = crate::slow_query_logger::SlowQueryLogger::new(
                slow_log_path,
                config.logging.slow_query_threshold_ms,
            );

            // Create system columns service (Phase 12, US5, T027)
            // Extract worker_id from node_id for Snowflake ID generation
            let worker_id = Self::extract_worker_id(&node_id);
            let system_columns_service =
                Arc::new(crate::system_columns::SystemColumnsService::new(worker_id));

            // Create unified manifest service (hot cache + RocksDB + cold storage)
            let mut manifest_service_obj = crate::manifest::ManifestService::new(
                system_tables.manifest(),
                config.manifest_cache.clone(),
            );
            manifest_service_obj.set_schema_registry(Arc::new(TablesSchemaRegistryAdapter::new(
                schema_registry.clone(),
            )));
            manifest_service_obj.set_storage_registry(storage_registry.clone());
            let manifest_service = Arc::new(manifest_service_obj);

            // Create file storage service (for FILE datatype uploads)
            let file_storage_service = Arc::new(kalamdb_filestore::FileStorageService::new(
                Arc::clone(&storage_registry),
                &config.files.staging_path,
                config.files.max_files_per_folder,
                config.files.max_size_bytes,
                config.files.max_files_per_request,
                config.files.allowed_mime_types.clone(),
            ));

            // Create command executor (Phase 20 - Unified Raft Executor)
            // ALWAYS use RaftExecutor - same code path for standalone and cluster
            // In standalone mode: single-node Raft cluster (no peers, instant leader election)
            // In cluster mode: multi-node Raft cluster with peers
            let raft_config = if let Some(cluster_config) = &config.cluster {
                // Multi-node cluster mode
                kalamdb_raft::manager::RaftManagerConfig::from(cluster_config.clone())
            } else {
                // Single-node mode: create a Raft cluster of 1
                // Use machine hostname as cluster ID
                let cluster_id = hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "kalamdb".to_string());
                let api_addr = format!("{}:{}", config.server.host, config.server.port);
                kalamdb_raft::manager::RaftManagerConfig::for_single_node(cluster_id, api_addr)
            };

            log::debug!("Creating RaftManager...");
            let snapshots_dir = config.storage.resolved_snapshots_dir();
            let manager = Arc::new(
                kalamdb_raft::manager::RaftManager::new_persistent(
                    raft_config,
                    storage_backend.clone(),
                    snapshots_dir,
                )
                .expect("Failed to create persistent RaftManager"),
            );

            log::debug!("Creating RaftExecutor...");
            let executor: Arc<dyn CommandExecutor> =
                Arc::new(kalamdb_raft::RaftExecutor::new(manager));

            // Note: ClusterLiveNotifier removed - Raft replication now handles
            // data consistency across nodes, and each node notifies its own
            // live query subscribers locally when data is applied.

            // Wire the executor into the system schema provider
            // This enables cluster and cluster_groups views (created on first access)
            system_schema.set_executor(Arc::clone(&executor));

            // Create notification service (before AppContext)
            let notification_service = NotificationService::new(Arc::clone(&connection_registry));

            // Create unified topic publisher service for pub/sub infrastructure
            let topic_publisher = Arc::new(TopicPublisherService::new(storage_backend.clone()));

            let app_ctx = Arc::new(AppContext {
                node_id,
                config,
                schema_registry: schema_registry.clone(),
                user_table_store,
                shared_table_store,
                storage_backend,
                job_manager: OnceCell::new(),
                live_query_manager: OnceCell::new(),
                notification_service: Arc::clone(&notification_service),
                connection_registry,
                storage_registry,
                system_tables,
                executor,
                applier: OnceCell::new(),
                session_factory,
                base_session_context,
                system_columns_service,
                slow_query_logger,
                manifest_service,
                file_storage_service,
                topic_publisher: Arc::clone(&topic_publisher),
                sql_executor: OnceCell::new(),
                server_start_time: Instant::now(),
            });

            // Register vector_search with AppContext-backed runtime.
            // This overrides the bootstrap placeholder and allows scope-aware lookup.
            app_ctx.base_session_context.register_udtf(
                "vector_search",
                Arc::new(VectorSearchTableFunction::new(Arc::new(CoreVectorSearchRuntime::new(
                    Arc::downgrade(&app_ctx),
                )))),
            );

            // Set AppContext in SchemaRegistry to break circular dependency
            schema_registry.set_app_context(app_ctx.clone());

            // Wire topic publisher into notification service for CDC → topic routing
            // (Topic publishing is now synchronous in table providers, but we keep the
            //  notification service wired for backward compatibility with any code that
            //  checks has_subscribers)

            // ── Restore topic cache from persisted topics ───────────────────────
            // On restart the TopicPublisherService starts with empty caches.
            // Load all persisted topics so CDC routes and offset counters are
            // available immediately for the notification worker.
            match app_ctx.system_tables().topics().list_topics() {
                Ok(topics) => {
                    let count = topics.len();
                    topic_publisher.refresh_topics_cache(topics);
                    topic_publisher.restore_offset_counters();
                    log::info!(
                        "Restored {} topics into TopicPublisherService cache (routes={}, offsets ready)",
                        count,
                        topic_publisher.cache_stats().total_routes,
                    );
                },
                Err(e) => {
                    log::warn!("Failed to restore topic cache on startup: {}", e);
                },
            }

            // Wire AppContext into notification service for Raft leadership checks
            // This ensures only the leader fires notifications, preventing duplicates
            notification_service.set_app_context(Arc::downgrade(&app_ctx));

            // Wire gRPC cluster client + handler for cross-node notification broadcasting.
            // Only effective in cluster mode (RaftExecutor); single-node mode skips this.
            if let Some(raft_executor) =
                app_ctx.executor().as_any().downcast_ref::<kalamdb_raft::RaftExecutor>()
            {
                let cluster_client =
                    Arc::new(kalamdb_raft::ClusterClient::new(raft_executor.manager().clone()));
                notification_service.set_cluster_client(cluster_client);

                let cluster_handler = Arc::new(crate::live::CoreClusterHandler::new(
                    Arc::clone(&app_ctx),
                    Arc::clone(&notification_service),
                ));
                raft_executor.set_cluster_handler(cluster_handler);
                let pg_executor = Arc::new(crate::pg_executor::CorePgQueryExecutor::new(
                    Arc::clone(&app_ctx),
                ));
                let pg_service = Arc::new(
                    KalamPgService::new(Some(format!(
                        "Bearer {}",
                        app_ctx.config().auth.jwt_secret
                    )))
                    .with_query_executor(pg_executor),
                );
                raft_executor.set_pg_service(pg_service);
                log::info!("Wired gRPC ClusterClient and CoreClusterHandler for cluster RPC");
            }

            let job_manager = Arc::new(crate::jobs::JobsManager::new(
                jobs_provider,
                job_nodes_provider,
                job_registry,
                Arc::clone(&app_ctx),
            ));
            if app_ctx.job_manager.set(job_manager).is_err() {
                panic!("JobsManager already initialized");
            }

            let applier = crate::applier::create_applier(Arc::clone(&app_ctx));
            if app_ctx.applier.set(applier).is_err() {
                panic!("UnifiedApplier already initialized");
            }

            let live_query_manager = Arc::new(LiveQueryManager::new(
                app_ctx.system_tables().live_queries(),
                app_ctx.schema_registry(),
                app_ctx.connection_registry(),
                Arc::clone(&app_ctx.base_session_context),
                Arc::clone(&app_ctx),
            ));
            if app_ctx.live_query_manager.set(live_query_manager).is_err() {
                panic!("LiveQueryManager already initialized");
            }

            // Wire up StatsTableProvider metrics callback now that AppContext exists
            let app_ctx_for_stats = Arc::clone(&app_ctx);
            stats_view.set_metrics_callback(Arc::new(move || app_ctx_for_stats.compute_metrics()));

            // Wire up ManifestTableProvider in_memory_checker callback
            // This allows system.manifest to show if a cache entry is in hot memory
            let manifest_for_checker = Arc::clone(&app_ctx.manifest_service);
            app_ctx.system_tables().manifest().set_in_memory_checker(Arc::new(
                move |cache_key: &str| manifest_for_checker.is_in_hot_cache_by_string(cache_key),
            ));

            // Cleanup orphan live queries from previous server run
            // Live queries don't persist across restarts (WebSocket connections are lost)
            match app_ctx.system_tables().live_queries().clear_all() {
                Ok(count) if count > 0 => {
                    log::info!("Cleared {} orphan live queries from previous server run", count);
                },
                Ok(_) => {}, // No orphans to clear
                Err(e) => {
                    log::warn!("Failed to clear orphan live queries: {}", e);
                },
            }

            app_ctx
        }
    }

    /// Wire Raft appliers that apply replicated commands into local providers.
    ///
    /// Safe to call multiple times.
    pub fn wire_raft_appliers(self: &Arc<Self>) {
        log::debug!("Wiring Raft appliers...");

        let executor = self.executor();

        let Some(raft_executor) = executor.as_any().downcast_ref::<kalamdb_raft::RaftExecutor>()
        else {
            log::error!("Failed to downcast executor to RaftExecutor - appliers NOT wired!");
            return;
        };

        let manager = raft_executor.manager();

        log::debug!("Wiring MetaApplier with AppContext for table provider registration...");
        let meta_applier = Arc::new(crate::applier::ProviderMetaApplier::new(Arc::clone(self)));
        manager.set_meta_applier(meta_applier);

        log::debug!("Wiring UserDataApplier for user table data replication...");
        let user_data_applier =
            Arc::new(crate::applier::ProviderUserDataApplier::new(Arc::clone(self)));
        manager.set_user_data_applier(user_data_applier);

        log::debug!("Wiring SharedDataApplier for shared table data replication...");
        let shared_data_applier =
            Arc::new(crate::applier::ProviderSharedDataApplier::new(Arc::clone(self)));
        manager.set_shared_data_applier(shared_data_applier);

        log::debug!("✓ Raft appliers wired successfully");
    }

    /// Restore state machines from persisted snapshots
    ///
    /// This should be called AFTER `wire_appliers()` to restore state machines'
    /// internal state from persisted snapshots. This prevents duplicate log entry
    /// application on server restart.
    ///
    /// The state machines need their appliers to be set first so they can properly
    /// restore persisted state to the underlying providers.
    pub async fn restore_raft_state_machines(&self) {
        log::debug!("Restoring Raft state machines from snapshots...");

        let executor = self.executor();

        let Some(raft_executor) = executor.as_any().downcast_ref::<kalamdb_raft::RaftExecutor>()
        else {
            log::error!(
                "Failed to downcast executor to RaftExecutor - state machines NOT restored!"
            );
            return;
        };

        let manager = raft_executor.manager();

        if let Err(e) = manager.restore_state_machines_from_snapshots().await {
            log::error!("Failed to restore state machines from snapshots: {:?}", e);
        } else {
            log::debug!("✓ Raft state machines restored successfully");
        }
    }

    /// Extract worker_id from node_id for Snowflake ID generation
    ///
    /// Maps node_id string to 10-bit integer (0-1023) using CRC32 hash.
    /// This ensures consistent worker_id across server restarts.
    fn extract_worker_id(node_id: &NodeId) -> u16 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        node_id.as_u64().hash(&mut hasher);
        let hash = hasher.finish();

        // Map to 10-bit range (0-1023)
        (hash % 1024) as u16
    }

    /// Create a minimal AppContext for unit testing
    ///
    /// This factory method creates a lightweight AppContext with only essential
    /// dependencies initialized. Use this in unit tests to avoid relying on any
    /// global AppContext state.
    ///
    /// **Phase 12, US5**: Test-specific factory with SystemColumnsService (worker_id=0)
    ///
    /// # Example
    /// ```no_run
    /// use kalamdb_core::app_context::AppContext;
    /// use std::sync::Arc;
    ///
    /// let app_context = AppContext::new_test();
    /// let sys_cols = app_context.system_columns_service();
    /// let (snowflake_id, updated_ns, deleted) = sys_cols.handle_insert(None).unwrap();
    /// ```
    #[cfg(test)]
    pub fn new_test() -> Arc<AppContext> {
        use kalamdb_store::test_utils::InMemoryBackend;

        // Create minimal in-memory backend for tests
        let storage_backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());

        // Create stores
        let user_table_store = Arc::new(UserTableStore::new(
            storage_backend.clone(),
            ColumnFamilyNames::USER_TABLE_PREFIX.to_string(),
        ));
        let shared_table_store = Arc::new(SharedTableStore::new(
            storage_backend.clone(),
            ColumnFamilyNames::SHARED_TABLE_PREFIX.to_string(),
        ));

        // Create system tables registry
        let system_tables = Arc::new(SystemTablesRegistry::new(storage_backend.clone()));

        // Create storage registry with temp path
        let storage_registry = Arc::new(StorageRegistry::new(
            system_tables.storages(),
            "/tmp/kalamdb-test".to_string(),
            kalamdb_configs::config::types::RemoteStorageTimeouts::default(),
        ));

        // Create minimal schema registry
        let schema_registry = Arc::new(SchemaRegistry::new(100));

        // Create DataFusion session
        let session_factory = Arc::new(
            DataFusionSessionFactory::new().expect("Failed to create test session factory"),
        );
        let base_session_context = Arc::new(session_factory.create_session());

        // Create minimal job manager registry
        let job_registry = Arc::new(JobRegistry::new());
        let jobs_provider = system_tables.jobs();
        let job_nodes_provider = system_tables.job_nodes();

        // Create test NodeId
        let node_id = Arc::new(NodeId::new(22));

        // Create connections manager for tests
        let connection_registry = ConnectionsManager::new(
            (*node_id).clone(),
            Duration::from_secs(30), // client_timeout
            Duration::from_secs(10), // auth_timeout
            Duration::from_secs(5),  // heartbeat_interval
        );

        // Create test config
        let config = Arc::new(ServerConfig::default());

        // Create minimal slow query logger for tests (no async task)
        let slow_query_logger = Arc::new(crate::slow_query_logger::SlowQueryLogger::new_test());

        // Create system columns service with worker_id=0 for tests
        let system_columns_service = Arc::new(crate::system_columns::SystemColumnsService::new(0));

        // Create unified manifest service for tests
        let manifest_service = Arc::new(crate::manifest::ManifestService::new_with_registries(
            storage_backend.clone(),
            "./data/storage".to_string(),
            config.manifest_cache.clone(),
            Arc::new(TablesSchemaRegistryAdapter::new(schema_registry.clone())),
            storage_registry.clone(),
        ));

        // Create file storage service for tests
        let file_storage_service = Arc::new(kalamdb_filestore::FileStorageService::new(
            Arc::clone(&storage_registry),
            "/tmp/kalamdb-test/staging",
            1000,
            1024 * 1024 * 100,
            10,
            vec!["*/*".to_string()],
        ));

        // Create RaftExecutor with single-node config for tests
        // This uses the same code path as production (unified Raft mode)
        let raft_config = kalamdb_raft::manager::RaftManagerConfig::for_single_node(
            "kalamdb-test".to_string(),
            "127.0.0.1:8080".to_string(),
        );
        let manager = Arc::new(kalamdb_raft::manager::RaftManager::new(raft_config));
        let executor: Arc<dyn CommandExecutor> = Arc::new(kalamdb_raft::RaftExecutor::new(manager));

        // Create notification service for tests (before AppContext)
        let notification_service = NotificationService::new(Arc::clone(&connection_registry));

        // Create unified topic publisher service for tests
        let topic_publisher = Arc::new(TopicPublisherService::new(storage_backend.clone()));

        let app_ctx = Arc::new(AppContext {
            node_id,
            config,
            schema_registry,
            user_table_store,
            shared_table_store,
            storage_backend,
            job_manager: OnceCell::new(),
            live_query_manager: OnceCell::new(),
            notification_service: Arc::clone(&notification_service),
            connection_registry,
            storage_registry,
            system_tables,
            executor,
            applier: OnceCell::new(),
            session_factory,
            base_session_context,
            system_columns_service,
            slow_query_logger,
            manifest_service,
            file_storage_service,
            topic_publisher: Arc::clone(&topic_publisher),
            sql_executor: OnceCell::new(),
            server_start_time: Instant::now(),
        });

        // Register vector_search with AppContext-backed runtime for tests.
        app_ctx.base_session_context.register_udtf(
            "vector_search",
            Arc::new(VectorSearchTableFunction::new(Arc::new(CoreVectorSearchRuntime::new(
                Arc::downgrade(&app_ctx),
            )))),
        );

        // Topic publishing is now synchronous in table providers — no need to wire
        // into notification service.

        // Wire AppContext into notification service for leadership checks (tests)
        notification_service.set_app_context(Arc::downgrade(&app_ctx));

        let job_manager = Arc::new(crate::jobs::JobsManager::new(
            jobs_provider,
            job_nodes_provider,
            job_registry,
            Arc::clone(&app_ctx),
        ));
        if app_ctx.job_manager.set(job_manager).is_err() {
            panic!("JobsManager already initialized");
        }

        let applier = crate::applier::create_applier(Arc::clone(&app_ctx));
        if app_ctx.applier.set(applier).is_err() {
            panic!("UnifiedApplier already initialized");
        }

        let live_query_manager = Arc::new(LiveQueryManager::new(
            app_ctx.system_tables().live_queries(),
            app_ctx.schema_registry(),
            app_ctx.connection_registry(),
            Arc::clone(&app_ctx.base_session_context),
            Arc::clone(&app_ctx),
        ));
        if app_ctx.live_query_manager.set(live_query_manager).is_err() {
            panic!("LiveQueryManager already initialized");
        }

        app_ctx
    }

    // ===== Getters =====

    /// Get the server configuration
    ///
    /// Returns an Arc reference for zero-copy sharing. This config is loaded
    /// once during AppContext::init() and shared across all components.
    pub fn config(&self) -> &Arc<ServerConfig> {
        &self.config
    }

    pub fn schema_registry(&self) -> Arc<SchemaRegistry> {
        self.schema_registry.clone()
    }

    pub fn user_table_store(&self) -> Arc<UserTableStore> {
        self.user_table_store.clone()
    }

    pub fn shared_table_store(&self) -> Arc<SharedTableStore> {
        self.shared_table_store.clone()
    }

    pub fn storage_backend(&self) -> Arc<dyn StorageBackend> {
        self.storage_backend.clone()
    }

    pub fn job_manager(&self) -> Arc<crate::jobs::JobsManager> {
        self.job_manager.get().expect("JobsManager not initialized").clone()
    }

    pub fn live_query_manager(&self) -> Arc<LiveQueryManager> {
        self.live_query_manager.get().expect("LiveQueryManager not initialized").clone()
    }

    /// Get the notification service
    pub fn notification_service(&self) -> &Arc<NotificationService> {
        &self.notification_service
    }

    /// Get the connections manager (WebSocket connection state)
    pub fn connection_registry(&self) -> Arc<ConnectionsManager> {
        self.connection_registry.clone()
    }

    /// Get the NodeId loaded from server.toml (Phase 10, US0, FR-000)
    ///
    /// Returns an Arc reference for zero-copy sharing. This NodeId is allocated
    /// exactly once per server instance during AppContext::init().
    pub fn node_id(&self) -> &Arc<NodeId> {
        &self.node_id
    }

    pub fn storage_registry(&self) -> Arc<StorageRegistry> {
        self.storage_registry.clone()
    }

    pub fn session_factory(&self) -> Arc<DataFusionSessionFactory> {
        self.session_factory.clone()
    }

    pub fn base_session_context(&self) -> Arc<SessionContext> {
        self.base_session_context.clone()
    }

    pub fn system_tables(&self) -> Arc<SystemTablesRegistry> {
        self.system_tables.clone()
    }

    /// Get the command executor (Phase 16 - Raft replication foundation)
    ///
    /// Returns the appropriate executor based on server mode:
    /// - Standalone: StandaloneExecutor (direct provider calls, zero overhead)
    /// - Cluster: RaftExecutor (commands go through Raft consensus)
    ///
    /// Handlers should use this instead of directly calling providers to enable
    /// transparent clustering support.
    pub fn executor(&self) -> Arc<dyn CommandExecutor> {
        self.executor.clone()
    }

    /// Check if this node is running in cluster mode
    pub fn is_cluster_mode(&self) -> bool {
        self.config.cluster.is_some()
    }

    // ===== Leadership Check Methods (Spec 021 - Raft Replication Semantics) =====

    /// Check if this node is the leader for a user's data shard
    ///
    /// In Raft cluster mode, user data is partitioned across shards based on
    /// `hash(user_id) % num_shards`. This method checks if the current node
    /// is the leader for the shard that contains the given user's data.
    ///
    /// In standalone mode (single-node), this always returns `true`.
    ///
    /// # Arguments
    /// * `user_id` - The user ID to check leadership for
    ///
    /// # Returns
    /// * `true` if this node is the leader for the user's shard
    /// * `false` if this node is not the leader (follower)
    ///
    /// # Example
    /// ```ignore
    /// if !ctx.is_leader_for_user(&user_id) {
    ///     return Err(KalamDbError::NotLeader { leader_addr: ctx.leader_addr_for_user(&user_id) });
    /// }
    /// ```
    pub async fn is_leader_for_user(&self, user_id: &UserId) -> bool {
        if !self.is_cluster_mode() {
            return true;
        }
        let group_id = self.user_shard_group_id(user_id);
        self.executor.is_leader(group_id).await
    }

    /// Get the leader node ID for a user's data shard
    ///
    /// Returns the NodeId of the current leader for the shard containing
    /// the given user's data. Returns `None` if the leader is unknown
    /// (e.g., during an election).
    ///
    /// # Arguments
    /// * `user_id` - The user ID to get the leader for
    ///
    /// # Returns
    /// * `Some(NodeId)` - The leader's node ID
    /// * `None` - If leader is unknown (election in progress)
    pub async fn leader_for_user(&self, user_id: &UserId) -> Option<NodeId> {
        let group_id = self.user_shard_group_id(user_id);
        self.executor.get_leader(group_id).await
    }

    /// Get the API address of the leader for a user's data shard
    ///
    /// This is used to construct NOT_LEADER error responses with a redirect hint.
    ///
    /// # Arguments
    /// * `user_id` - The user ID to get the leader address for
    ///
    /// # Returns
    /// * `Some(String)` - The leader's API address (e.g., "192.168.1.100:8080")
    /// * `None` - If leader is unknown or address not available
    pub async fn leader_addr_for_user(&self, user_id: &UserId) -> Option<String> {
        let cluster_info = self.executor.get_cluster_info();
        let leader_node_id = self.leader_for_user(user_id).await?;

        // Find the leader's API address from cluster info
        cluster_info
            .nodes
            .iter()
            .find(|node| node.node_id == leader_node_id)
            .map(|node| node.api_addr.clone())
    }

    /// Check if this node is the leader for shared data
    ///
    /// Shared tables are stored in a dedicated shard (currently shard 0).
    /// This method checks if the current node is the leader for that shard.
    ///
    /// In standalone mode (single-node), this always returns `true`.
    pub async fn is_leader_for_shared(&self) -> bool {
        if !self.is_cluster_mode() {
            return true;
        }
        let router = ShardRouter::from_optional_cluster_config(self.config.cluster.as_ref());
        let group_id = router.shared_group_id();
        self.executor.is_leader(group_id).await
    }

    /// Get the API address of the current leader for shared data
    ///
    /// Returns `None` if the leader is unknown or address not available.
    pub async fn leader_addr_for_shared(&self) -> Option<String> {
        let router = ShardRouter::from_optional_cluster_config(self.config.cluster.as_ref());
        let group_id = router.shared_group_id();
        let leader_node_id = self.executor.get_leader(group_id).await?;
        let cluster_info = self.executor.get_cluster_info();
        cluster_info
            .nodes
            .iter()
            .find(|node| node.node_id == leader_node_id)
            .map(|node| node.api_addr.clone())
    }

    /// Compute the Raft group ID for a user's data shard
    ///
    /// Maps user_id → shard number → GroupId using consistent hashing.
    fn user_shard_group_id(&self, user_id: &UserId) -> GroupId {
        let router = ShardRouter::from_optional_cluster_config(self.config.cluster.as_ref());
        router.user_group_id(user_id)
    }

    /// Get the unified applier (Phase 19 - Unified Command Applier)
    ///
    /// Returns the applier that handles all database commands.
    /// All mutations flow through this applier, regardless of mode.
    pub fn applier(&self) -> Arc<dyn UnifiedApplier> {
        self.applier.get().expect("UnifiedApplier not initialized").clone()
    }

    /// Get the system columns service (Phase 12, US5, T027)
    ///
    /// Returns an Arc reference to the SystemColumnsService that manages
    /// all system column operations (_seq, _deleted).
    pub fn system_columns_service(&self) -> Arc<crate::system_columns::SystemColumnsService> {
        self.system_columns_service.clone()
    }

    /// Get the slow query logger
    ///
    /// Returns an Arc reference to the lightweight slow query logger that writes
    /// to a separate slow.log file for queries exceeding the configured threshold.
    pub fn slow_query_logger(&self) -> Arc<crate::slow_query_logger::SlowQueryLogger> {
        self.slow_query_logger.clone()
    }

    /// Get the manifest service (unified: hot cache + RocksDB + cold storage)
    ///
    /// Returns an Arc reference to the ManifestService that provides:
    /// - Hot cache (moka) for sub-millisecond lookups
    /// - RocksDB persistence for crash recovery
    /// - Cold storage access for manifest.json files
    pub fn manifest_service(&self) -> Arc<crate::manifest::ManifestService> {
        self.manifest_service.clone()
    }

    /// Access the FileStorageService for FILE datatype operations.
    ///
    /// Returns an Arc reference to the FileStorageService that provides:
    /// - File staging and finalization
    /// - File download and deletion
    /// - Storage routing per table via StorageRegistry
    pub fn file_storage_service(&self) -> Arc<kalamdb_filestore::FileStorageService> {
        self.file_storage_service.clone()
    }

    // ===== Topic Pub/Sub Service =====

    /// Get the unified topic publisher service for all topic operations
    ///
    /// TopicPublisherService provides:
    /// - Topic registry (in-memory cache + RocksDB persistence)
    /// - Message publishing and consumption
    /// - Consumer group offset tracking
    /// - Table-to-topic routing for CDC
    pub fn topic_publisher(&self) -> Arc<TopicPublisherService> {
        self.topic_publisher.clone()
    }

    /// Register the shared SqlExecutor (called once during bootstrap)
    pub fn set_sql_executor(&self, executor: Arc<SqlExecutor>) {
        if self.sql_executor.set(executor).is_err() {
            log::warn!(
                "SqlExecutor already initialized in AppContext; ignoring duplicate registration"
            );
        }
    }

    /// Try to access the shared SqlExecutor if initialized
    pub fn try_sql_executor(&self) -> Option<Arc<SqlExecutor>> {
        self.sql_executor.get().map(Arc::clone)
    }

    /// Get the shared SqlExecutor (panics if not yet initialized)
    pub fn sql_executor(&self) -> Arc<SqlExecutor> {
        self.try_sql_executor().expect("SqlExecutor not initialized in AppContext")
    }

    // ===== Convenience methods for backward compatibility =====

    /// Insert a job into the jobs table
    ///
    /// Convenience wrapper for system_tables().jobs().create_job()
    pub fn insert_job(&self, job: &Job) -> Result<(), crate::error::KalamDbError> {
        self.system_tables()
            .jobs()
            .create_job(job.clone())
            .map(|_| ())  // Discard the message, just return ()
            .into_kalamdb_error("Failed to insert job")
    }

    /// Scan all jobs from the jobs table
    ///
    /// Convenience wrapper for system_tables().jobs().list_jobs()
    pub fn scan_all_jobs(&self) -> Result<Vec<Job>, crate::error::KalamDbError> {
        self.system_tables()
            .jobs()
            .list_jobs()
            .into_kalamdb_error("Failed to scan jobs")
    }

    /// Get server uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.server_start_time.elapsed().as_secs()
    }

    /// Get the server start time instant (for computing uptime)
    pub fn server_start_time(&self) -> Instant {
        self.server_start_time
    }

    /// Compute current system metrics snapshot
    ///
    /// Returns a vector of (metric_name, metric_value) tuples for display
    /// in system.stats virtual view.
    pub fn compute_metrics(&self) -> Vec<(String, String)> {
        crate::metrics::runtime::compute_metrics(self)
    }

    /// Log a concise snapshot of runtime metrics to the console.
    pub fn log_runtime_metrics(&self) {
        let runtime = collect_runtime_metrics(self.server_start_time);
        log::info!("Runtime metrics: {}", runtime.to_log_string());
    }
}

#[async_trait]
impl ClusterCoordinator for AppContext {
    async fn is_cluster_mode(&self) -> bool {
        self.is_cluster_mode()
    }

    async fn is_meta_leader(&self) -> bool {
        if !self.is_cluster_mode() {
            return true; // Single-node mode: always the leader
        }
        self.executor.is_leader(kalamdb_raft::GroupId::Meta).await
    }

    async fn meta_leader_addr(&self) -> Option<String> {
        let leader_id = self.executor.get_leader(kalamdb_raft::GroupId::Meta).await?;
        let cluster_info = self.executor.get_cluster_info();
        cluster_info
            .nodes
            .iter()
            .find(|node| node.node_id == leader_id)
            .map(|node| node.api_addr.clone())
    }

    async fn is_leader_for_user(&self, user_id: &UserId) -> bool {
        self.is_leader_for_user(user_id).await
    }

    async fn is_leader_for_shared(&self) -> bool {
        self.is_leader_for_shared().await
    }

    async fn leader_addr_for_user(&self, user_id: &UserId) -> Option<String> {
        self.leader_addr_for_user(user_id).await
    }

    async fn leader_addr_for_shared(&self) -> Option<String> {
        self.leader_addr_for_shared().await
    }
}
