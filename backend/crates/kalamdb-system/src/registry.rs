//! System Tables Registry
//!
//! Centralized registry for all system table providers. Replaces individual
//! provider fields in AppContext with a single registry pattern.
//!
//! **Phase 5 Completion**: Consolidates all 10 system table providers into
//! a single struct for cleaner AppContext API.

use std::{collections::HashSet, sync::Arc};

// SchemaRegistry will be passed as Arc parameter from kalamdb-core
use datafusion::datasource::TableProvider;
use kalamdb_commons::{
    schemas::{TableDefinition, TableType},
    SystemTable,
};
use kalamdb_session_datafusion::secure_provider;
use kalamdb_store::StorageBackend;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;

use super::providers::{
    catalog::{
        CatalogFunctionArtifact, CatalogFunctionModule, CatalogFunctionRevision, CatalogRoutine,
        CatalogRoutineGrant, CatalogRoutineParameter, CatalogStores, CatalogTrigger,
        CatalogTriggerAttempt, CatalogType, CatalogTypeField, FunctionArtifactsTableProvider,
        FunctionModulesTableProvider, FunctionRevisionsTableProvider, RoutineGrantsTableProvider,
        RoutineParametersTableProvider, RoutinesTableProvider, TriggerAttemptsTableProvider,
        TriggersTableProvider, TypeFieldsTableProvider, TypesTableProvider,
    },
    job_nodes::models::JobNode,
    jobs::models::Job,
    manifest::manifest_table_definition,
    migrations::{models::Migration, MigrationsTableProvider},
    namespaces::models::Namespace,
    storages::models::Storage,
    table_policies::{TablePoliciesTableProvider, TablePolicyRecord},
    tables::schemas_table_definition,
    topic_offsets::models::TopicOffset,
    topics::models::Topic,
    users::models::User,
    AuditLogEntry, AuditLogsTableProvider, JobNodesTableProvider, JobsTableProvider,
    ManifestTableProvider, NamespacesTableProvider, SchemasTableProvider, StoragesTableProvider,
    TopicOffsetsTableProvider, TopicsTableProvider, UsersTableProvider,
};

/// Registry of all system table providers
///
/// Provides centralized access to all system.* tables.
/// Used by AppContext to eliminate 10 individual provider fields.
///
/// Note: information_schema.tables and information_schema.columns are provided
/// by DataFusion's built-in information_schema support (enabled via
/// .with_information_schema(true)).
#[derive(Debug)]
pub struct SystemTablesRegistry {
    // ===== system.* tables (EntityStore-based) =====
    users:              Arc<UsersTableProvider>,
    jobs:               Arc<JobsTableProvider>,
    job_nodes:          Arc<JobNodesTableProvider>,
    namespaces:         Arc<NamespacesTableProvider>,
    storages:           Arc<StoragesTableProvider>,
    schemas:            Arc<SchemasTableProvider>,
    audit_logs:         Arc<AuditLogsTableProvider>,
    topics:             Arc<TopicsTableProvider>,
    topic_offsets:      Arc<TopicOffsetsTableProvider>,
    migrations:         Arc<MigrationsTableProvider>,
    table_policies:     Arc<TablePoliciesTableProvider>,
    types:              Arc<TypesTableProvider>,
    type_fields:        Arc<TypeFieldsTableProvider>,
    routines:           Arc<RoutinesTableProvider>,
    routine_parameters: Arc<RoutineParametersTableProvider>,
    routine_grants:     Arc<RoutineGrantsTableProvider>,
    function_modules:   Arc<FunctionModulesTableProvider>,
    function_revisions: Arc<FunctionRevisionsTableProvider>,
    function_artifacts: Arc<FunctionArtifactsTableProvider>,
    triggers:           Arc<TriggersTableProvider>,
    trigger_attempts:   Arc<TriggerAttemptsTableProvider>,
    // ===== Manifest cache table =====
    manifest:           Arc<ManifestTableProvider>,

    // ===== Virtual tables =====
    stats:          RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    settings:       RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    server_logs:    RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    cluster:        RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    cluster_groups: RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    tables:         RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,
    columns:        RwLock<Option<Arc<dyn TableProvider + Send + Sync>>>,

    // Expected in-code system table definitions used only for startup reconciliation.
    expected_system_definitions: OnceCell<Vec<Arc<TableDefinition>>>,
}

impl SystemTablesRegistry {
    fn provider_backed_system_tables() -> &'static [SystemTable] {
        &[
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
            SystemTable::Migrations,
            SystemTable::TablePolicies,
            SystemTable::Types,
            SystemTable::TypeFields,
            SystemTable::Routines,
            SystemTable::RoutineParameters,
            SystemTable::RoutineGrants,
            SystemTable::FunctionModules,
            SystemTable::FunctionRevisions,
            SystemTable::FunctionArtifacts,
            SystemTable::Triggers,
            SystemTable::TriggerAttempts,
        ]
    }

    /// Create a new system tables registry
    ///
    /// Initializes all system table providers from the storage backend.
    ///
    /// # Arguments
    /// * `storage_backend` - Storage backend for EntityStore-based providers
    ///
    /// # Example
    /// ```no_run
    /// use std::sync::Arc;
    ///
    /// use kalamdb_core::tables::system::SystemTablesRegistry;
    /// # use kalamdb_store::StorageBackend;
    ///
    /// # let backend: Arc<dyn StorageBackend> = unimplemented!();
    /// let registry = SystemTablesRegistry::new(backend);
    /// ```
    pub fn new(storage_backend: Arc<dyn StorageBackend>) -> Self {
        let catalog_stores = CatalogStores::new(storage_backend.clone());
        Self {
            // EntityStore-based providers
            users:              Arc::new(UsersTableProvider::new(storage_backend.clone())),
            jobs:               Arc::new(JobsTableProvider::new(storage_backend.clone())),
            job_nodes:          Arc::new(JobNodesTableProvider::new(storage_backend.clone())),
            namespaces:         Arc::new(NamespacesTableProvider::new(storage_backend.clone())),
            storages:           Arc::new(StoragesTableProvider::new(storage_backend.clone())),
            schemas:            Arc::new(SchemasTableProvider::new(storage_backend.clone())),
            audit_logs:         Arc::new(AuditLogsTableProvider::new(storage_backend.clone())),
            topics:             Arc::new(TopicsTableProvider::new(storage_backend.clone())),
            topic_offsets:      Arc::new(TopicOffsetsTableProvider::new(storage_backend.clone())),
            migrations:         Arc::new(MigrationsTableProvider::new(storage_backend.clone())),
            table_policies:     Arc::new(TablePoliciesTableProvider::new(storage_backend.clone())),
            types:              Arc::new(TypesTableProvider::from_stores(catalog_stores.clone())),
            type_fields:        Arc::new(TypeFieldsTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            routines:           Arc::new(RoutinesTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            routine_parameters: Arc::new(RoutineParametersTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            routine_grants:     Arc::new(RoutineGrantsTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            function_modules:   Arc::new(FunctionModulesTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            function_revisions: Arc::new(FunctionRevisionsTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            function_artifacts: Arc::new(FunctionArtifactsTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            triggers:           Arc::new(TriggersTableProvider::from_stores(
                catalog_stores.clone(),
            )),
            trigger_attempts:   Arc::new(TriggerAttemptsTableProvider::from_stores(catalog_stores)),

            // Manifest cache provider
            manifest: Arc::new(ManifestTableProvider::new(storage_backend)),

            // Virtual tables
            stats:          RwLock::new(None), // Will be wired by kalamdb-core
            settings:       RwLock::new(None), // Will be wired by kalamdb-core
            server_logs:    RwLock::new(None), // Will be wired by kalamdb-core (dev only)
            cluster:        RwLock::new(None), // Initialized via set_cluster_provider()
            cluster_groups: RwLock::new(None), // Initialized via set_cluster_groups_provider()
            tables:         RwLock::new(None), // Initialized via set_tables_view_provider()
            columns:        RwLock::new(None), // Initialized via set_columns_view_provider()

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
                    (SystemTable::Migrations, Migration::definition()),
                    (SystemTable::TablePolicies, TablePolicyRecord::definition()),
                    (SystemTable::Types, CatalogType::definition()),
                    (SystemTable::TypeFields, CatalogTypeField::definition()),
                    (SystemTable::Routines, CatalogRoutine::definition()),
                    (SystemTable::RoutineParameters, CatalogRoutineParameter::definition()),
                    (SystemTable::RoutineGrants, CatalogRoutineGrant::definition()),
                    (SystemTable::FunctionModules, CatalogFunctionModule::definition()),
                    (SystemTable::FunctionRevisions, CatalogFunctionRevision::definition()),
                    (SystemTable::FunctionArtifacts, CatalogFunctionArtifact::definition()),
                    (SystemTable::Triggers, CatalogTrigger::definition()),
                    (SystemTable::TriggerAttempts, CatalogTriggerAttempt::definition()),
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

    /// Get the system.migrations provider
    pub fn migrations(&self) -> Arc<MigrationsTableProvider> {
        self.migrations.clone()
    }

    /// Get the system.table_policies provider.
    pub fn table_policies(&self) -> Arc<TablePoliciesTableProvider> {
        self.table_policies.clone()
    }

    /// Get the system.types provider.
    pub fn types(&self) -> Arc<TypesTableProvider> {
        self.types.clone()
    }

    /// Get the system.type_fields provider.
    pub fn type_fields(&self) -> Arc<TypeFieldsTableProvider> {
        self.type_fields.clone()
    }

    /// Get the system.routines provider.
    pub fn routines(&self) -> Arc<RoutinesTableProvider> {
        self.routines.clone()
    }

    /// Get the system.routine_parameters provider.
    pub fn routine_parameters(&self) -> Arc<RoutineParametersTableProvider> {
        self.routine_parameters.clone()
    }

    /// Get the system.routine_grants provider.
    pub fn routine_grants(&self) -> Arc<RoutineGrantsTableProvider> {
        self.routine_grants.clone()
    }

    /// Shared catalog stores (types, routines, function revisions).
    pub fn catalog_stores(&self) -> CatalogStores {
        self.types.stores().clone()
    }

    /// Get the system.stats provider (virtual table)
    pub fn stats(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.stats.read().clone()
    }

    /// Get the system.settings provider (virtual table)
    pub fn settings(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.settings.read().clone()
    }

    /// Get the system.server_logs provider (virtual table reading JSON logs)
    pub fn server_logs(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.server_logs.read().clone()
    }

    /// Get the system.manifest provider
    pub fn manifest(&self) -> Arc<ManifestTableProvider> {
        self.manifest.clone()
    }

    /// Get the system.cluster provider (virtual table showing cluster status)
    pub fn cluster(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.cluster.read().clone()
    }

    /// Get the system.cluster_groups provider (virtual table showing per-group status)
    pub fn cluster_groups(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.cluster_groups.read().clone()
    }

    /// Deprecated. Use `information_schema.tables` (`kdb_*` columns).
    pub fn tables_view(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.tables.read().clone()
    }

    /// Deprecated. Use `information_schema.columns` (`kdb_*` columns).
    pub fn columns_view(&self) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        self.columns.read().clone()
    }

    /// Return persisted system tables that have concrete providers, without
    /// constructing or wrapping those providers.
    pub fn persisted_provider_tables(&self) -> Vec<SystemTable> {
        let persisted_tables = self.persisted_system_tables();
        Self::provider_backed_system_tables()
            .iter()
            .copied()
            .filter(|table| persisted_tables.contains(table))
            .collect()
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
            SystemTable::Migrations => Some(self.migrations.clone() as Arc<dyn TableProvider>),
            SystemTable::TablePolicies => {
                Some(self.table_policies.clone() as Arc<dyn TableProvider>)
            },
            SystemTable::Types => Some(self.types.clone() as Arc<dyn TableProvider>),
            SystemTable::TypeFields => Some(self.type_fields.clone() as Arc<dyn TableProvider>),
            SystemTable::Routines => Some(self.routines.clone() as Arc<dyn TableProvider>),
            SystemTable::RoutineParameters => {
                Some(self.routine_parameters.clone() as Arc<dyn TableProvider>)
            },
            SystemTable::RoutineGrants => {
                Some(self.routine_grants.clone() as Arc<dyn TableProvider>)
            },
            SystemTable::FunctionModules => {
                Some(self.function_modules.clone() as Arc<dyn TableProvider>)
            },
            SystemTable::FunctionRevisions => {
                Some(self.function_revisions.clone() as Arc<dyn TableProvider>)
            },
            SystemTable::FunctionArtifacts => {
                Some(self.function_artifacts.clone() as Arc<dyn TableProvider>)
            },
            SystemTable::Triggers => Some(self.triggers.clone() as Arc<dyn TableProvider>),
            SystemTable::TriggerAttempts => {
                Some(self.trigger_attempts.clone() as Arc<dyn TableProvider>)
            },
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
            SystemTable::Migrations,
            SystemTable::TablePolicies,
            SystemTable::Types,
            SystemTable::TypeFields,
            SystemTable::Routines,
            SystemTable::RoutineParameters,
            SystemTable::RoutineGrants,
            SystemTable::FunctionModules,
            SystemTable::FunctionRevisions,
            SystemTable::FunctionArtifacts,
            SystemTable::Triggers,
            SystemTable::TriggerAttempts,
        ]
        .into_iter()
        .collect()
    }
}
