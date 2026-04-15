//! System Schema Provider
//!
//! A thin DataFusion SchemaProvider wrapper over SchemaRegistry for the `system` schema.
//! All system table/view providers are stored in SchemaRegistry's CachedTableData.
//! Virtual views (stats, settings, etc.) are created lazily on first access and
//! stored back into SchemaRegistry.

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use kalamdb_commons::SystemTable;
use kalamdb_configs::ServerConfig;
use kalamdb_raft::CommandExecutor;
use kalamdb_session_datafusion::secure_provider;
use kalamdb_system::SystemTablesRegistry;
use parking_lot::RwLock;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use crate::schema_registry::SchemaRegistry;
use kalamdb_views::cluster::create_cluster_provider;
use kalamdb_views::cluster_groups::create_cluster_groups_provider;
use kalamdb_views::columns_view::create_columns_view_provider;
use kalamdb_views::datatypes::{DatatypesTableProvider, DatatypesView};
use kalamdb_views::describe::DescribeView;
use kalamdb_views::live::{LiveTableProvider, LiveView};
use kalamdb_views::server_logs::create_server_logs_provider;
use kalamdb_views::sessions::{SessionsTableProvider, SessionsView};
use kalamdb_views::settings::{SettingsTableProvider, SettingsView};
use kalamdb_views::stats::{StatsTableProvider, StatsView};
use kalamdb_views::tables_view::create_tables_view_provider;
use kalamdb_views::transactions::{TransactionsTableProvider, TransactionsView};
use kalamdb_views::view_base::ViewTableProvider;

/// Configuration for view initialization
pub struct ViewConfig {
    /// Server configuration (for settings view)
    pub config: Arc<ServerConfig>,
    /// Logs path (for server_logs view)
    pub logs_path: PathBuf,
    /// Command executor (for cluster views, set after Raft init)
    pub executor: RwLock<Option<Arc<dyn CommandExecutor>>>,
    /// Pre-created StatsView for callback wiring
    pub stats_view: Arc<StatsView>,
    /// Pre-created system.live view for callback wiring.
    pub live_view: Arc<LiveView>,
    /// Pre-created system.sessions view for callback wiring.
    pub sessions_view: Arc<SessionsView>,
    /// Pre-created system.transactions view for callback wiring.
    pub transactions_view: Arc<TransactionsView>,
}

impl std::fmt::Debug for ViewConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewConfig")
            .field("logs_path", &self.logs_path)
            .field("has_executor", &self.executor.read().is_some())
            .finish()
    }
}

/// DataFusion SchemaProvider for the `system` schema
///
/// Delegates all table/view lookups to SchemaRegistry. Virtual views
/// are created lazily on first access and stored in SchemaRegistry's
/// CachedTableData entries. Persisted system tables (users, jobs, etc.)
/// already have their providers set during SchemaRegistry initialization.
#[derive(Debug)]
pub struct SystemSchemaProvider {
    /// Canonical schema/provider cache for all table types
    schema_registry: Arc<SchemaRegistry>,
    /// Registry of persisted system tables (fallback for providers not yet in cache)
    system_tables: Arc<SystemTablesRegistry>,
    /// Configuration for lazy view initialization
    view_config: Arc<ViewConfig>,
}

impl SystemSchemaProvider {
    /// Create a new system schema provider
    pub fn new(
        schema_registry: Arc<SchemaRegistry>,
        system_tables: Arc<SystemTablesRegistry>,
        config: Arc<ServerConfig>,
        logs_path: PathBuf,
    ) -> Self {
        Self {
            schema_registry,
            system_tables,
            view_config: Arc::new(ViewConfig {
                config,
                logs_path,
                executor: RwLock::new(None),
                stats_view: Arc::new(StatsView::new()),
                live_view: Arc::new(LiveView::new(SystemTable::Live)),
                sessions_view: Arc::new(SessionsView::new()),
                transactions_view: Arc::new(TransactionsView::new()),
            }),
        }
    }

    /// Set the command executor (called after Raft initialization)
    pub fn set_executor(&self, executor: Arc<dyn CommandExecutor>) {
        *self.view_config.executor.write() = Some(executor);
    }

    /// Get the pre-created StatsView for callback wiring
    ///
    /// The StatsView is created during initialization and wrapped in a
    /// provider on first access. The callback can be wired before the
    /// first query.
    pub fn stats_view(&self) -> Arc<StatsView> {
        Arc::clone(&self.view_config.stats_view)
    }

    /// Get the pre-created system.live view for callback wiring.
    pub fn live_view(&self) -> Arc<LiveView> {
        Arc::clone(&self.view_config.live_view)
    }

    /// Get the pre-created system.sessions view for callback wiring.
    pub fn sessions_view(&self) -> Arc<SessionsView> {
        Arc::clone(&self.view_config.sessions_view)
    }

    /// Get the pre-created system.transactions view for callback wiring.
    pub fn transactions_view(&self) -> Arc<TransactionsView> {
        Arc::clone(&self.view_config.transactions_view)
    }

    /// Get or create a view provider, storing it in SchemaRegistry's CachedTableData
    fn get_or_create_view_provider(
        &self,
        system_table: SystemTable,
    ) -> Option<Arc<dyn TableProvider>> {
        let table_id = system_table.table_id();

        // Check if provider already exists in SchemaRegistry
        if let Some(cached) = self.schema_registry.get(&table_id) {
            if let Some(provider) = cached.get_provider() {
                return Some(provider);
            }
        }

        // Create view provider
        let provider: Arc<dyn TableProvider> = match system_table {
            SystemTable::Stats => {
                let provider =
                    Arc::new(StatsTableProvider::new(Arc::clone(&self.view_config.stats_view)));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Live => {
                let provider =
                    Arc::new(LiveTableProvider::new(Arc::clone(&self.view_config.live_view)));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Sessions => {
                let provider = Arc::new(SessionsTableProvider::new(Arc::clone(
                    &self.view_config.sessions_view,
                )));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Transactions => {
                let provider = Arc::new(TransactionsTableProvider::new(Arc::clone(
                    &self.view_config.transactions_view,
                )));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Settings => {
                let settings_view =
                    Arc::new(SettingsView::with_config((*self.view_config.config).clone()));
                let provider = Arc::new(SettingsTableProvider::new(settings_view));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::ServerLogs => {
                let provider = Arc::new(create_server_logs_provider(&self.view_config.logs_path));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Datatypes => {
                let datatypes_view = Arc::new(DatatypesView::new());
                let provider = Arc::new(DatatypesTableProvider::new(datatypes_view));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Tables => {
                let provider =
                    Arc::new(create_tables_view_provider(Arc::clone(&self.system_tables)));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Columns => {
                let provider =
                    Arc::new(create_columns_view_provider(Arc::clone(&self.system_tables)));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Describe => {
                let view = Arc::new(DescribeView::new());
                let provider = Arc::new(ViewTableProvider::new(view));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::Cluster => {
                let executor = self.view_config.executor.read();
                let executor = executor.as_ref()?;
                let provider = Arc::new(create_cluster_provider(Arc::clone(executor)));
                provider as Arc<dyn TableProvider>
            },
            SystemTable::ClusterGroups => {
                let executor = self.view_config.executor.read();
                let executor = executor.as_ref()?;
                let provider = Arc::new(create_cluster_groups_provider(Arc::clone(executor)));
                provider as Arc<dyn TableProvider>
            },
            _ => return None,
        };

        // Wrap with security and store in SchemaRegistry
        let secured: Arc<dyn TableProvider + Send + Sync> =
            secure_provider(provider, system_table.table_id());
        if let Some(cached) = self.schema_registry.get(&table_id) {
            cached.set_provider(Arc::clone(&secured));
        }

        Some(secured)
    }
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .system_tables
            .all_system_providers()
            .into_iter()
            .map(|(table, _)| table.table_name().to_string())
            .collect();

        for view in SystemTable::all_views() {
            names.push(view.table_name().to_string());
        }

        names.sort();
        names.dedup();
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        SystemTable::from_name(name).is_ok()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let system_table = match SystemTable::from_name(name) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };

        let table_id = system_table.table_id();

        // Fast path: check SchemaRegistry for cached provider
        if let Some(cached) = self.schema_registry.get(&table_id) {
            if let Some(provider) = cached.get_provider() {
                return Ok(Some(provider));
            }
        }

        // For views, create lazily and store in SchemaRegistry
        if system_table.is_view() {
            return Ok(self.get_or_create_view_provider(system_table));
        }

        // For persisted tables without a cached provider, try SystemTablesRegistry fallback
        if let Some(provider) = self.system_tables.persisted_table_provider(system_table) {
            return Ok(Some(provider as Arc<dyn TableProvider>));
        }

        Ok(None)
    }

    /// Override table_type for efficiency - returns without loading the full table
    async fn table_type(&self, name: &str) -> DataFusionResult<Option<TableType>> {
        match SystemTable::from_name(name) {
            Ok(table) if table.is_view() => Ok(Some(TableType::View)),
            Ok(_) => Ok(Some(TableType::Base)),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_store::test_utils::InMemoryBackend;

    fn create_test_provider() -> SystemSchemaProvider {
        let backend: Arc<dyn kalamdb_store::StorageBackend> = Arc::new(InMemoryBackend::new());
        let system_tables = Arc::new(SystemTablesRegistry::new(backend));
        let schema_registry = Arc::new(crate::schema_registry::SchemaRegistry::new(100));
        let config = Arc::new(ServerConfig::default());
        let logs_path = std::path::PathBuf::from("/tmp/kalamdb-test/logs");
        std::fs::create_dir_all(&logs_path).ok();

        SystemSchemaProvider::new(schema_registry, system_tables, config, logs_path)
    }

    #[test]
    fn test_table_names_returns_all_known_tables() {
        let provider = create_test_provider();
        let names = provider.table_names();

        // Check all persisted tables
        assert!(names.contains(&"users".to_string()));
        assert!(names.contains(&"jobs".to_string()));
        assert!(names.contains(&"namespaces".to_string()));
        assert!(names.contains(&"storages".to_string()));

        // Check all virtual views
        assert!(names.contains(&"stats".to_string()));
        assert!(names.contains(&"settings".to_string()));
        assert!(names.contains(&"datatypes".to_string()));
        assert!(names.contains(&"tables".to_string()));
        assert!(names.contains(&"columns".to_string()));
    }

    #[test]
    fn test_table_exist_for_known_tables() {
        let provider = create_test_provider();

        assert!(provider.table_exist("users"));
        assert!(provider.table_exist("stats"));
        assert!(provider.table_exist("settings"));
        assert!(provider.table_exist("datatypes"));
        assert!(!provider.table_exist("nonexistent"));
    }

    #[tokio::test]
    async fn test_persisted_table_access() {
        let provider = create_test_provider();

        // Persisted tables should be available immediately
        let users = provider.table("users").await.unwrap();
        assert!(users.is_some());

        let jobs = provider.table("jobs").await.unwrap();
        assert!(jobs.is_some());
    }

    #[tokio::test]
    async fn test_lazy_view_access() {
        let provider = create_test_provider();

        let settings = provider.table("settings").await.unwrap();
        assert!(settings.is_some());

        // Second access should return the same provider (cached in SchemaRegistry or fallback)
        let settings2 = provider.table("settings").await.unwrap();
        assert!(settings2.is_some());
    }

    #[tokio::test]
    async fn test_datatypes_view_lazy_load() {
        let provider = create_test_provider();

        let datatypes = provider.table("datatypes").await.unwrap();
        assert!(datatypes.is_some());
    }

    #[tokio::test]
    async fn test_tables_view_lazy_load() {
        let provider = create_test_provider();

        let tables = provider.table("tables").await.unwrap();
        assert!(tables.is_some());
    }

    #[tokio::test]
    async fn test_columns_view_lazy_load() {
        let provider = create_test_provider();

        let columns = provider.table("columns").await.unwrap();
        assert!(columns.is_some());
    }

    #[tokio::test]
    async fn test_cluster_view_requires_executor() {
        let provider = create_test_provider();

        // Without executor, cluster view should return None
        let cluster = provider.table("cluster").await.unwrap();
        assert!(cluster.is_none());

        let cluster_groups = provider.table("cluster_groups").await.unwrap();
        assert!(cluster_groups.is_none());
    }

    #[tokio::test]
    async fn test_table_type_without_loading() {
        let provider = create_test_provider();

        // table_type should work without loading the table
        let users_type = provider.table_type("users").await.unwrap();
        assert_eq!(users_type, Some(TableType::Base));

        let stats_type = provider.table_type("stats").await.unwrap();
        assert_eq!(stats_type, Some(TableType::View));

        let unknown_type = provider.table_type("nonexistent").await.unwrap();
        assert_eq!(unknown_type, None);
    }

    #[tokio::test]
    async fn test_nonexistent_table_returns_none() {
        let provider = create_test_provider();

        let result = provider.table("nonexistent_table").await.unwrap();
        assert!(result.is_none());
    }
}
