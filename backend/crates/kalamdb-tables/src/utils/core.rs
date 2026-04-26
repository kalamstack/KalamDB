use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{arrow::datatypes::SchemaRef, logical_expr::Expr};
use kalamdb_commons::{
    schemas::{TableDefinition, TableType},
    websocket::ChangeNotification,
    TableId,
};
use kalamdb_filestore::StorageRegistry;
use kalamdb_system::{
    ClusterCoordinator as ClusterCoordinatorTrait, ManifestService as ManifestServiceTrait,
    NotificationService as NotificationServiceTrait, SchemaRegistry as SchemaRegistryTrait,
    TopicPublisher as TopicPublisherTrait,
};
use kalamdb_transactions::CommitSequenceSource;

use crate::error::KalamDbError;

/// Combined services struct shared across all table providers.
///
/// All table providers need the same set of services. Wrapping them in a single
/// `Arc<TableServices>` avoids 6 individual `Arc` clones per provider, reducing
/// per-table memory overhead significantly.
pub struct TableServices {
    /// Schema registry for table metadata and Arrow schema caching
    pub schema_registry: Arc<dyn SchemaRegistryTrait<Error = KalamDbError>>,

    /// System columns service for _seq and _deleted management
    pub system_columns: Arc<kalamdb_system::SystemColumnsService>,

    /// Storage registry for resolving full storage paths (optional)
    pub storage_registry: Option<Arc<StorageRegistry>>,

    /// Manifest service interface (for manifest cache + rebuilds)
    pub manifest_service: Arc<dyn ManifestServiceTrait>,

    /// Live query manager interface (for change notifications)
    pub notification_service: Arc<dyn NotificationServiceTrait<Notification = ChangeNotification>>,

    /// Cluster coordinator for leader checks
    pub cluster_coordinator: Arc<dyn ClusterCoordinatorTrait>,

    /// Commit-sequence source for stamping direct DML writes that bypass the applier fast path.
    pub commit_sequence_source: Arc<dyn CommitSequenceSource>,

    /// Topic publisher for synchronous CDC publishing (optional, None when topics are not
    /// configured)
    pub topic_publisher: Option<Arc<dyn TopicPublisherTrait>>,
}

impl TableServices {
    /// Create a new `TableServices` bundle.
    pub fn new(
        schema_registry: Arc<dyn SchemaRegistryTrait<Error = KalamDbError>>,
        system_columns: Arc<kalamdb_system::SystemColumnsService>,
        storage_registry: Option<Arc<StorageRegistry>>,
        manifest_service: Arc<dyn ManifestServiceTrait>,
        notification_service: Arc<dyn NotificationServiceTrait<Notification = ChangeNotification>>,
        cluster_coordinator: Arc<dyn ClusterCoordinatorTrait>,
        commit_sequence_source: Arc<dyn CommitSequenceSource>,
        topic_publisher: Option<Arc<dyn TopicPublisherTrait>>,
    ) -> Self {
        Self {
            schema_registry,
            system_columns,
            storage_registry,
            manifest_service,
            notification_service,
            cluster_coordinator,
            commit_sequence_source,
            topic_publisher,
        }
    }
}

/// Shared core state for all table providers
///
/// **Memory Optimization**: All provider types share this core structure,
/// reducing per-table memory footprint from 3× allocation to 1× allocation.
/// Services are bundled into a single `Arc<TableServices>` to further
/// reduce Arc overhead.
///
/// Also hosts per-table cached fields: schema, primary key name/id, column defaults,
/// and a precomputed set of non-nullable column names for fast constraint checks.
pub struct TableProviderCore {
    /// Complete table definition (contains namespace_id, table_name, table_type, columns, etc.)
    table_def: Arc<TableDefinition>,

    /// Cached TableId (constructed from namespace_id + table_name for O(1) access)
    table_id: TableId,

    /// Bundled services (schema_registry, system_columns, manifest, notifications, etc.)
    pub services: Arc<TableServices>,

    /// Cached primary key field name (O(1) access)
    primary_key_field_name: String,

    /// Cached primary key column_id (O(1) access, avoids O(n) column scan)
    primary_key_column_id: u64,

    /// Cached flag: is PK auto-increment? (avoids repeated column iteration)
    is_auto_increment_pk: bool,

    /// Cached Arrow schema (prevents panics if table is dropped while provider is in use)
    schema: SchemaRef,

    /// DataFusion column defaults used by INSERT planning.
    column_defaults: HashMap<String, Expr>,

    /// Precomputed set of non-nullable column names for fast NOT NULL constraint checks.
    non_null_columns: HashSet<String>,
}

impl TableProviderCore {
    /// Create new core with required services and table definition
    pub fn new(
        table_def: Arc<TableDefinition>,
        services: Arc<TableServices>,
        primary_key_field_name: String,
        schema: SchemaRef,
        column_defaults: HashMap<String, Expr>,
    ) -> Self {
        use kalamdb_commons::{constants::SystemColumnNames, schemas::ColumnDefault};

        // Precompute non-nullable columns from the schema.
        // Exclude system columns (_seq, _deleted) because they are auto-generated
        // during INSERT and should not be validated against user-provided data.
        let non_null_columns: HashSet<String> = schema
            .fields()
            .iter()
            .filter(|f| !f.is_nullable() && !SystemColumnNames::is_system_column(f.name()))
            .map(|f| f.name().clone())
            .collect();

        // Find PK column (O(n) once at construction, then O(1) access for all derived values)
        let pk_column = table_def.columns.iter().find(|c| c.is_primary_key);

        // Extract primary key column_id
        let primary_key_column_id = pk_column.map(|c| c.column_id).unwrap_or(0);

        // Check if PK is auto-increment (AUTO_INCREMENT or SNOWFLAKE_ID function)
        let is_auto_increment_pk = if let Some(pk_col) = pk_column {
            matches!(
                &pk_col.default_value,
                ColumnDefault::FunctionCall { name, .. }
                    if name.eq_ignore_ascii_case("auto_increment")
                    || name.eq_ignore_ascii_case("snowflake_id")
            )
        } else {
            // No PK column means no uniqueness constraint to check
            true
        };

        // Cache TableId (constructed once, then O(1) access)
        let table_id =
            TableId::from_strings(table_def.namespace_id.as_str(), table_def.table_name.as_str());

        Self {
            table_def,
            table_id,
            services,
            primary_key_field_name,
            primary_key_column_id,
            is_auto_increment_pk,
            schema,
            column_defaults,
            non_null_columns,
        }
    }

    /// Add StorageRegistry to services
    pub fn with_storage_registry(self, registry: Arc<StorageRegistry>) -> Self {
        // Since services is behind Arc, we need to create a new TableServices
        // This is only used during construction so the extra allocation is fine
        let new_services = Arc::new(TableServices {
            schema_registry: self.services.schema_registry.clone(),
            system_columns: self.services.system_columns.clone(),
            storage_registry: Some(registry),
            manifest_service: self.services.manifest_service.clone(),
            notification_service: self.services.notification_service.clone(),
            cluster_coordinator: self.services.cluster_coordinator.clone(),
            commit_sequence_source: self.services.commit_sequence_source.clone(),
            topic_publisher: self.services.topic_publisher.clone(),
        });
        Self {
            services: new_services,
            ..self
        }
    }

    // ===========================
    // Delegating accessors for backward compatibility
    // ===========================

    /// Schema registry accessor
    pub fn schema_registry(&self) -> &Arc<dyn SchemaRegistryTrait<Error = KalamDbError>> {
        &self.services.schema_registry
    }

    /// System columns service accessor
    pub fn system_columns(&self) -> &Arc<kalamdb_system::SystemColumnsService> {
        &self.services.system_columns
    }

    /// Storage registry accessor
    pub fn storage_registry(&self) -> &Option<Arc<StorageRegistry>> {
        &self.services.storage_registry
    }

    /// ManifestService accessor
    pub fn manifest_service(&self) -> &Arc<dyn ManifestServiceTrait> {
        &self.services.manifest_service
    }

    /// NotificationService accessor
    pub fn notification_service(
        &self,
    ) -> &Arc<dyn NotificationServiceTrait<Notification = ChangeNotification>> {
        &self.services.notification_service
    }

    /// Cluster coordinator accessor
    pub fn cluster_coordinator(&self) -> &Arc<dyn ClusterCoordinatorTrait> {
        &self.services.cluster_coordinator
    }

    /// Commit sequence source accessor.
    pub fn commit_sequence_source(&self) -> &Arc<dyn CommitSequenceSource> {
        &self.services.commit_sequence_source
    }

    /// Table definition accessor
    pub fn table_def(&self) -> &Arc<TableDefinition> {
        &self.table_def
    }

    /// TableId accessor (constructed from namespace_id and table_name)
    pub fn table_id(&self) -> &TableId {
        &self.table_id
    }

    /// Cached Arrow schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Cached Arrow schema (cloned Arc)
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Is PK auto-increment? (O(1), precomputed at construction)
    pub fn is_auto_increment_pk(&self) -> bool {
        self.is_auto_increment_pk
    }

    /// Primary key field name accessor (O(1))
    pub fn primary_key_field_name(&self) -> &str {
        &self.primary_key_field_name
    }

    /// Primary key column_id accessor (O(1), precomputed at construction)
    pub fn primary_key_column_id(&self) -> u64 {
        self.primary_key_column_id
    }

    /// Column defaults accessor
    pub fn column_defaults(&self) -> &HashMap<String, Expr> {
        &self.column_defaults
    }

    /// Get column default for a specific column
    pub fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }

    /// Precomputed set of non-nullable column names.
    ///
    /// Use this instead of recomputing from schema on every insert/update.
    pub fn non_null_columns(&self) -> &HashSet<String> {
        &self.non_null_columns
    }

    /// Cloneable TableId handle (for backward compatibility)
    pub fn table_id_arc(&self) -> Arc<TableId> {
        Arc::new(self.table_id.clone())
    }

    /// TableType accessor
    pub fn table_type(&self) -> TableType {
        self.table_def.table_type
    }

    /// Check if any topic routes are configured for this table's ID.
    #[inline]
    pub fn has_topic_routes(&self, table_id: &TableId) -> bool {
        self.services
            .topic_publisher
            .as_ref()
            .map_or(false, |tp| tp.has_topics_for_table(table_id))
    }

    /// Publish a row change to matching topics synchronously.
    ///
    /// Only publishes if the current node is the leader (in cluster mode).
    /// This ensures topic messages are persisted before the write is acknowledged,
    /// replacing the previous async notification-queue approach that dropped events.
    pub async fn publish_to_topics(
        &self,
        table_id: &TableId,
        op: kalamdb_commons::models::TopicOp,
        row: &kalamdb_commons::models::rows::Row,
        user_id: Option<&kalamdb_commons::models::UserId>,
    ) {
        let topic_pub = match self.services.topic_publisher.as_ref() {
            Some(tp) if tp.has_topics_for_table(table_id) => tp,
            _ => return,
        };

        // Leadership check: only leader publishes to avoid duplicates in cluster mode.
        // In standalone mode, is_leader_for_* always returns true.
        let is_leader = match user_id {
            Some(uid) => self.services.cluster_coordinator.is_leader_for_user(uid).await,
            None => self.services.cluster_coordinator.is_leader_for_shared().await,
        };
        if !is_leader {
            return;
        }

        if let Err(e) = topic_pub.publish_for_table(table_id, op, row, user_id) {
            log::warn!("Topic publish failed for table {}: {}", table_id, e);
        }
    }

    /// Publish a batch of row changes to matching topics.
    ///
    /// Uses `publish_batch_for_table` which batches RocksDB writes, reuses JSON
    /// serialization, and acquires partition locks once per partition instead of
    /// once per row — significantly faster than calling `publish_to_topics()` in a loop.
    pub async fn publish_batch_to_topics(
        &self,
        table_id: &TableId,
        op: kalamdb_commons::models::TopicOp,
        rows: &[kalamdb_commons::models::rows::Row],
        user_id: Option<&kalamdb_commons::models::UserId>,
    ) {
        if rows.is_empty() {
            return;
        }

        let topic_pub = match self.services.topic_publisher.as_ref() {
            Some(tp) if tp.has_topics_for_table(table_id) => tp,
            _ => return,
        };

        // Leadership check: only leader publishes to avoid duplicates in cluster mode.
        let is_leader = match user_id {
            Some(uid) => self.services.cluster_coordinator.is_leader_for_user(uid).await,
            None => self.services.cluster_coordinator.is_leader_for_shared().await,
        };
        if !is_leader {
            return;
        }

        if let Err(e) = topic_pub.publish_batch_for_table(table_id, op, rows, user_id) {
            log::warn!("Topic batch publish failed for table {}: {}", table_id, e);
        }
    }
}
