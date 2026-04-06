//! System Tables Registry
//!
//! Centralized registry for all system table providers. Replaces individual
//! provider fields in AppContext with a single registry pattern.
//!
//! **Phase 5 Completion**: Consolidates all 10 system table providers into
//! a single struct for cleaner AppContext API.

use super::providers::job_nodes::models::JobNode;
use super::providers::jobs::models::Job;
use super::providers::manifest::manifest_table_definition;
use super::providers::namespaces::models::Namespace;
use super::providers::storages::models::Storage;
use super::providers::tables::schemas_table_definition;
use super::providers::topic_offsets::models::TopicOffset;
use super::providers::topics::models::Topic;
use super::providers::users::models::User;
use super::providers::{
    AuditLogEntry, AuditLogsTableProvider, JobNodesTableProvider, JobsTableProvider,
    ManifestTableProvider, NamespacesTableProvider, SchemasTableProvider, StoragesTableProvider,
    TopicOffsetsTableProvider, TopicsTableProvider, UsersTableProvider,
};
// SchemaRegistry will be passed as Arc parameter from kalamdb-core
use datafusion::datasource::TableProvider;
use kalamdb_commons::schemas::{TableDefinition, TableType};
use kalamdb_commons::SystemTable;
use kalamdb_session_datafusion::secure_provider;
use kalamdb_store::StorageBackend;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;

/// Registry of all system table providers
///
/// Provides centralized access to all system.* tables.
/// Used by AppContext to eliminate 10 individual provider fields.
///
/// Note: information_schema.tables and information_schema.columns are provided
/// by DataFusion's built-in information_schema support (enabled via .with_information_schema(true)).
#[derive(Debug)]
pub struct SystemTablesRegistry {
    // ===== system.* tables (EntityStore-based) =====
    users: Arc<UsersTableProvider>,
    jobs: Arc<JobsTableProvider>,
    job_nodes: Arc<JobNodesTableProvider>,
    namespaces: Arc<NamespacesTableProvider>,
    storages: Arc<StoragesTableProvider>,
    schemas: Arc<SchemasTableProvider>,
    audit_logs: Arc<AuditLogsTableProvider>,
    topics: Arc<TopicsTableProvider>,
    topic_offsets: Arc<TopicOffsetsTableProvider>,
    // ===== Manifest cache table =====
    manifest: Arc<ManifestTableProvider>,

    // ===== Virtual tables =====
    stats: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    settings: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    server_logs: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    cluster: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    cluster_groups: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    tables: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    columns: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,

    // Expected in-code system table definitions used only for startup reconciliation.
    expected_system_definitions: OnceCell<Vec<Arc<TableDefinition>>>,
}

impl SystemTablesRegistry {
    /// Create a new system tables registry
    ///
    /// Initializes all system table providers from the storage backend.
    ///
    /// # Arguments
    /// * `storage_backend` - Storage backend for EntityStore-based providers
    /// * `kalam_sql` - KalamSQL adapter for information_schema providers
    ///
    /// # Example
    /// ```no_run
    /// use kalamdb_core::tables::system::SystemTablesRegistry;
    /// use std::sync::Arc;
    /// # use kalamdb_store::StorageBackend;
    ///
    /// # let backend: Arc<dyn StorageBackend> = unimplemented!();
    /// # let kalam_sql: Arc<KalamSql> = unimplemented!();
    /// let registry = SystemTablesRegistry::new(backend, kalam_sql);
    /// ```
    pub fn new(storage_backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            // EntityStore-based providers
            users: Arc::new(UsersTableProvider::new(storage_backend.clone())),
            jobs: Arc::new(JobsTableProvider::new(storage_backend.clone())),
            job_nodes: Arc::new(JobNodesTableProvider::new(storage_backend.clone())),
            namespaces: Arc::new(NamespacesTableProvider::new(storage_backend.clone())),
            storages: Arc::new(StoragesTableProvider::new(storage_backend.clone())),
            schemas: Arc::new(SchemasTableProvider::new(storage_backend.clone())),
            audit_logs: Arc::new(AuditLogsTableProvider::new(storage_backend.clone())),
            topics: Arc::new(TopicsTableProvider::new(storage_backend.clone())),
            topic_offsets: Arc::new(TopicOffsetsTableProvider::new(storage_backend.clone())),

            // Manifest cache provider
            manifest: Arc::new(ManifestTableProvider::new(storage_backend)),

            // Virtual tables
            stats: RwLock::new(None),       // Will be wired by kalamdb-core
            settings: RwLock::new(None),    // Will be wired by kalamdb-core
            server_logs: RwLock::new(None), // Will be wired by kalamdb-core (dev only)
            cluster: RwLock::new(None),     // Initialized via set_cluster_provider()
            cluster_groups: RwLock::new(None), // Initialized via set_cluster_groups_provider()
            tables: RwLock::new(None),      // Initialized via set_tables_view_provider()
            columns: RwLock::new(None),     // Initialized via set_columns_view_provider()

            expected_system_definitions: OnceCell::new(),
        }
    }

    pub fn expected_system_table_definitions(&self) -> Vec<Arc<TableDefinition>> {
        self.expected_system_definitions
            .get_or_init(|| {
                let defs: Vec<(SystemTable, TableDefinition)> = vec![
                    (SystemTable::Users, User::definition()),
                    (SystemTable::Namespaces, Namespace::definition()),
                    (SystemTable::Schemas, schemas_table_definition()),
                    (SystemTable::Storages, Storage::definition()),
                    (SystemTable::Jobs, Job::definition()),
                    (SystemTable::JobNodes, JobNode::definition()),
                    (SystemTable::AuditLog, AuditLogEntry::definition()),
                    (SystemTable::Manifest, manifest_table_definition()),
                    (SystemTable::Topics, Topic::definition()),
                    (SystemTable::TopicOffsets, TopicOffset::definition()),
                ];

                defs.into_iter().map(|(_, definition)| Arc::new(definition)).collect()
            })
            .clone()
    }

    // ===== Getter Methods =====

    /// Get the system.users provider
    pub fn users(&self) -> Arc<UsersTableProvider> {
        self.users.clone()
    }

    /// Get the system.jobs provider
    pub fn jobs(&self) -> Arc<JobsTableProvider> {
        self.jobs.clone()
    }

    /// Get the system.job_nodes provider
    pub fn job_nodes(&self) -> Arc<JobNodesTableProvider> {
        self.job_nodes.clone()
    }

    /// Get the system.namespaces provider
    pub fn namespaces(&self) -> Arc<NamespacesTableProvider> {
        self.namespaces.clone()
    }

    /// Get the system.storages provider
    pub fn storages(&self) -> Arc<StoragesTableProvider> {
        self.storages.clone()
    }

    /// Get the system.schemas provider
    pub fn tables(&self) -> Arc<SchemasTableProvider> {
        self.schemas.clone()
    }

    /// Get the system.audit_logs provider
    pub fn audit_logs(&self) -> Arc<AuditLogsTableProvider> {
        self.audit_logs.clone()
    }

    /// Get the system.topics provider
    pub fn topics(&self) -> Arc<TopicsTableProvider> {
        self.topics.clone()
    }

    /// Get the system.topic_offsets provider
    pub fn topic_offsets(&self) -> Arc<TopicOffsetsTableProvider> {
        self.topic_offsets.clone()
    }

    /// Get the system.stats provider (virtual table)
    pub fn stats(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.stats.read().clone()
    }

    /// Set the system.stats provider (called from kalamdb-core)
    pub fn set_stats_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting stats provider");
        *self.stats.write() = Some(provider);
    }

    /// Get the system.settings provider (virtual table)
    pub fn settings(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.settings.read().clone()
    }

    /// Set the system.settings provider (called from kalamdb-core)
    pub fn set_settings_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting settings provider");
        *self.settings.write() = Some(provider);
    }

    /// Get the system.server_logs provider (virtual table reading JSON logs)
    pub fn server_logs(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.server_logs.read().clone()
    }

    /// Set the system.server_logs provider (called from kalamdb-core with logs path)
    pub fn set_server_logs_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting server_logs provider");
        *self.server_logs.write() = Some(provider);
    }

    /// Get the system.manifest provider
    pub fn manifest(&self) -> Arc<ManifestTableProvider> {
        self.manifest.clone()
    }

    /// Get the system.cluster provider (virtual table showing cluster status)
    pub fn cluster(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.cluster.read().clone()
    }

    /// Set the system.cluster provider (called from kalamdb-core with executor)
    pub fn set_cluster_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting cluster provider");
        *self.cluster.write() = Some(provider);
    }

    /// Get the system.cluster_groups provider (virtual table showing per-group status)
    pub fn cluster_groups(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.cluster_groups.read().clone()
    }

    /// Set the system.cluster_groups provider (called from kalamdb-core with executor)
    pub fn set_cluster_groups_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting cluster_groups provider");
        *self.cluster_groups.write() = Some(provider);
    }

    /// Get the system.tables view provider (virtual table showing table metadata)
    pub fn tables_view(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.tables.read().clone()
    }

    /// Set the system.tables view provider (called from kalamdb-core)
    pub fn set_tables_view_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting tables view provider");
        *self.tables.write() = Some(provider);
    }

    /// Get the system.columns view provider (virtual table showing column metadata)
    pub fn columns_view(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.columns.read().clone()
    }

    /// Set the system.columns view provider (called from kalamdb-core)
    pub fn set_columns_view_provider(&self, provider: Arc<dyn TableProvider + Send + Sync>) {
        log::debug!("SystemTablesRegistry: Setting columns view provider");
        *self.columns.write() = Some(provider);
    }

    // ===== Convenience Methods =====

    /// Get all system.* providers as a vector for bulk registration
    ///
    /// Returns tuples of (table_name, provider) for DataFusion schema registration.
    /// All providers are wrapped with `SecuredSystemTableProvider` for defense-in-depth
    /// permission checking at the scan() level.
    pub fn all_system_providers(&self) -> Vec<(SystemTable, Arc<dyn TableProvider>)> {
        let persisted_tables = self.persisted_system_tables();

        // Helper to wrap providers with security
        let wrap =
            |table: SystemTable, provider: Arc<dyn TableProvider>| -> Arc<dyn TableProvider> {
                secure_provider(provider, table.table_id()) as Arc<dyn TableProvider>
            };

        let mut providers = Vec::new();
        for table in [
            SystemTable::Users,
            SystemTable::Jobs,
            SystemTable::JobNodes,
            SystemTable::Namespaces,
            SystemTable::Storages,
            SystemTable::Schemas,
            SystemTable::AuditLog,
            SystemTable::Manifest,
            SystemTable::Topics,
            SystemTable::TopicOffsets,
        ] {
            if !persisted_tables.contains(&table) {
                continue;
            }

            if let Some(provider) = self.provider_for_system_table(table) {
                providers.push((table, wrap(table, provider)));
            }
        }

        // Add stats if initialized (virtual view from kalamdb-core)
        if let Some(stats) = self.stats.read().clone() {
            providers.push((
                SystemTable::Stats,
                wrap(SystemTable::Stats, stats as Arc<dyn TableProvider>),
            ));
        }

        // Add settings if initialized (virtual view from kalamdb-core)
        if let Some(settings) = self.settings.read().clone() {
            providers.push((
                SystemTable::Settings,
                wrap(SystemTable::Settings, settings as Arc<dyn TableProvider>),
            ));
        }

        // Add server_logs if initialized
        if let Some(server_logs) = self.server_logs.read().clone() {
            providers.push((
                SystemTable::ServerLogs,
                wrap(SystemTable::ServerLogs, server_logs as Arc<dyn TableProvider>),
            ));
        }

        // Add cluster if initialized (virtual table showing OpenRaft metrics)
        if let Some(cluster) = self.cluster.read().clone() {
            providers.push((
                SystemTable::Cluster,
                wrap(SystemTable::Cluster, cluster as Arc<dyn TableProvider>),
            ));
        }

        // Add cluster_groups if initialized (virtual table showing per-group OpenRaft metrics)
        if let Some(cluster_groups) = self.cluster_groups.read().clone() {
            providers.push((
                SystemTable::ClusterGroups,
                wrap(SystemTable::ClusterGroups, cluster_groups as Arc<dyn TableProvider>),
            ));
        }

        // Add tables view if initialized (virtual view from kalamdb-core)
        if let Some(tables) = self.tables.read().clone() {
            providers.push((
                SystemTable::Tables,
                wrap(SystemTable::Tables, tables as Arc<dyn TableProvider>),
            ));
        }

        // Add columns view if initialized (virtual view from kalamdb-core)
        if let Some(columns) = self.columns.read().clone() {
            providers.push((
                SystemTable::Columns,
                wrap(SystemTable::Columns, columns as Arc<dyn TableProvider>),
            ));
        }

        providers
    }

    /// Returns a secured provider for a persisted system table.
    ///
    /// This is the canonical provider lookup used by SchemaRegistry cache binding.
    pub fn persisted_table_provider(
        &self,
        table: SystemTable,
    ) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        if table.is_view() {
            return None;
        }

        let persisted_tables = self.persisted_system_tables();
        if !persisted_tables.contains(&table) {
            return None;
        }

        let provider = self.provider_for_system_table(table)?;
        Some(secure_provider(provider, table.table_id()) as Arc<dyn TableProvider + Send + Sync>)
    }

    fn provider_for_system_table(&self, table: SystemTable) -> Option<Arc<dyn TableProvider>> {
        match table {
            SystemTable::Users => Some(self.users.clone() as Arc<dyn TableProvider>),
            SystemTable::Jobs => Some(self.jobs.clone() as Arc<dyn TableProvider>),
            SystemTable::JobNodes => Some(self.job_nodes.clone() as Arc<dyn TableProvider>),
            SystemTable::Namespaces => Some(self.namespaces.clone() as Arc<dyn TableProvider>),
            SystemTable::Storages => Some(self.storages.clone() as Arc<dyn TableProvider>),
            SystemTable::Schemas => Some(self.schemas.clone() as Arc<dyn TableProvider>),
            SystemTable::AuditLog => Some(self.audit_logs.clone() as Arc<dyn TableProvider>),
            SystemTable::Manifest => Some(self.manifest.clone() as Arc<dyn TableProvider>),
            SystemTable::Topics => Some(self.topics.clone() as Arc<dyn TableProvider>),
            SystemTable::TopicOffsets => Some(self.topic_offsets.clone() as Arc<dyn TableProvider>),
            _ => None,
        }
    }

    fn persisted_system_tables(&self) -> HashSet<SystemTable> {
        let Ok(definitions) = self.schemas.list_tables() else {
            log::error!(
                "SystemTablesRegistry: failed reading persisted schemas from system.schemas"
            );
            return Self::default_persisted_system_tables();
        };

        let tables: HashSet<SystemTable> = definitions
            .into_iter()
            .filter(|def| def.table_type == TableType::System)
            .filter(|def| def.namespace_id.is_system_namespace())
            .filter_map(|def| SystemTable::from_name(def.table_name.as_str()).ok())
            .filter(|table| !table.is_view())
            .collect();

        if tables.is_empty() {
            Self::default_persisted_system_tables()
        } else {
            tables
        }
    }

    fn default_persisted_system_tables() -> HashSet<SystemTable> {
        [
            SystemTable::Users,
            SystemTable::Jobs,
            SystemTable::JobNodes,
            SystemTable::Namespaces,
            SystemTable::Storages,
            SystemTable::Schemas,
            SystemTable::AuditLog,
            SystemTable::Manifest,
            SystemTable::Topics,
            SystemTable::TopicOffsets,
        ]
        .into_iter()
        .collect()
    }
}
