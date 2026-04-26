use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider, execution::context::SessionContext,
    physical_plan::collect, scalar::ScalarValue,
};
use kalamdb_commons::{
    models::{
        datatypes::KalamDataType,
        rows::Row,
        schemas::{ColumnDefinition, TableDefinition, TableOptions},
        NamespaceId, ReadContext, Role, StorageId, TableId, TableName,
    },
    schemas::ColumnDefault,
    websocket::ChangeNotification,
};
use kalamdb_datafusion_sources::exec::DeferredBatchExec;
use kalamdb_filestore::StorageRegistry;
use kalamdb_sharding::ShardRouter;
use kalamdb_store::{test_utils::InMemoryBackend, StorageBackend, StorageError};
use kalamdb_system::{
    ClusterCoordinator, Manifest, ManifestCacheEntry, ManifestService, NotificationService,
    SchemaRegistry, SessionUserContext, Storage, StorageType, StoragesTableProvider,
    SystemColumnsService,
};
use kalamdb_tables::{
    new_stream_table_store, utils::TableServices, BaseTableProvider, StreamTableProvider,
    StreamTableStorageMode, StreamTableStoreConfig, TableProviderCore,
};
use kalamdb_transactions::CommitSequenceSource;
use tempfile::TempDir;

fn row(values: Vec<(&str, ScalarValue)>) -> Row {
    Row::from_vec(values.into_iter().map(|(name, value)| (name.to_string(), value)).collect())
}

#[derive(Debug, Clone)]
struct TestSchemaRegistry {
    table_def: Arc<TableDefinition>,
    schema: SchemaRef,
    storage_id: StorageId,
}

impl TestSchemaRegistry {
    fn new(table_def: Arc<TableDefinition>, schema: SchemaRef, storage_id: StorageId) -> Self {
        Self {
            table_def,
            schema,
            storage_id,
        }
    }
}

impl SchemaRegistry for TestSchemaRegistry {
    type Error = kalamdb_tables::KalamDbError;

    fn get_arrow_schema(&self, table_id: &TableId) -> Result<SchemaRef, Self::Error> {
        if &TableId::from_strings(
            self.table_def.namespace_id.as_str(),
            self.table_def.table_name.as_str(),
        ) == table_id
        {
            Ok(Arc::clone(&self.schema))
        } else {
            Err(kalamdb_tables::KalamDbError::TableNotFound(table_id.to_string()))
        }
    }

    fn get_table_if_exists(
        &self,
        table_id: &TableId,
    ) -> Result<Option<Arc<TableDefinition>>, Self::Error> {
        if &TableId::from_strings(
            self.table_def.namespace_id.as_str(),
            self.table_def.table_name.as_str(),
        ) == table_id
        {
            Ok(Some(Arc::clone(&self.table_def)))
        } else {
            Ok(None)
        }
    }

    fn get_arrow_schema_for_version(
        &self,
        table_id: &TableId,
        _schema_version: u32,
    ) -> Result<SchemaRef, Self::Error> {
        self.get_arrow_schema(table_id)
    }

    fn get_storage_id(&self, _table_id: &TableId) -> Result<StorageId, Self::Error> {
        Ok(self.storage_id.clone())
    }
}

#[derive(Debug, Default)]
struct NoopManifestService;

#[async_trait]
impl ManifestService for NoopManifestService {
    fn get_or_load(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        Ok(None)
    }

    async fn get_or_load_async(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        Ok(None)
    }

    fn validate_manifest(&self, _manifest: &Manifest) -> Result<(), StorageError> {
        Ok(())
    }

    fn mark_as_stale(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn rebuild_manifest(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
    ) -> Result<Manifest, StorageError> {
        panic!("rebuild_manifest is unused in stream planning tests")
    }

    fn mark_pending_write(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn ensure_manifest_initialized(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
    ) -> Result<Manifest, StorageError> {
        panic!("ensure_manifest_initialized is unused in stream planning tests")
    }

    fn stage_before_flush(
        &self,
        _table_id: &TableId,
        _user_id: Option<&kalamdb_commons::UserId>,
        _manifest: &Manifest,
    ) -> Result<(), StorageError> {
        panic!("stage_before_flush is unused in stream planning tests")
    }

    fn get_manifest_user_ids(
        &self,
        _table_id: &TableId,
    ) -> Result<Vec<kalamdb_commons::UserId>, StorageError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Default)]
struct NoopNotificationService;

impl NotificationService for NoopNotificationService {
    type Notification = ChangeNotification;

    fn has_subscribers(
        &self,
        _user_id: Option<&kalamdb_commons::UserId>,
        _table_id: &TableId,
    ) -> bool {
        false
    }

    fn notify_table_change(
        &self,
        _user_id: Option<kalamdb_commons::UserId>,
        _table_id: TableId,
        _notification: Self::Notification,
    ) {
    }
}

#[derive(Debug, Default)]
struct NoopClusterCoordinator;

#[async_trait]
impl ClusterCoordinator for NoopClusterCoordinator {
    async fn is_cluster_mode(&self) -> bool {
        false
    }

    async fn is_meta_leader(&self) -> bool {
        true
    }

    async fn meta_leader_addr(&self) -> Option<String> {
        None
    }

    async fn is_leader_for_user(&self, _user_id: &kalamdb_commons::UserId) -> bool {
        true
    }

    async fn is_leader_for_shared(&self) -> bool {
        true
    }

    async fn leader_addr_for_user(&self, _user_id: &kalamdb_commons::UserId) -> Option<String> {
        None
    }

    async fn leader_addr_for_shared(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Default)]
struct TestCommitSequence;

impl CommitSequenceSource for TestCommitSequence {
    fn current_committed(&self) -> u64 {
        0
    }

    fn allocate_next(&self) -> u64 {
        1
    }
}

struct OwnedServices {
    services: Arc<TableServices>,
    schema: SchemaRef,
    temp_dir: TempDir,
}

fn session_with_user(user_id: &kalamdb_commons::UserId) -> SessionContext {
    let mut state = SessionContext::new().state().clone();
    state.config_mut().options_mut().extensions.insert(SessionUserContext::new(
        user_id.clone(),
        Role::Dba,
        ReadContext::Internal,
    ));
    SessionContext::new_with_state(state)
}

fn build_storage_registry(
    backend: Arc<dyn StorageBackend>,
    temp_dir: &TempDir,
) -> Arc<StorageRegistry> {
    let storages_provider = Arc::new(StoragesTableProvider::new(backend));
    let base_directory = temp_dir.path().to_string_lossy().into_owned();
    storages_provider
        .create_storage(Storage {
            storage_id: StorageId::local(),
            storage_name: "Local Storage".to_string(),
            description: Some("Stream planning test storage".to_string()),
            storage_type: StorageType::Filesystem,
            base_directory: base_directory.clone(),
            credentials: None,
            config_json: None,
            shared_tables_template: "shared/{namespace}/{table}".to_string(),
            user_tables_template: "user/{namespace}/{table}/{userId}".to_string(),
            created_at: 1_000,
            updated_at: 1_000,
        })
        .expect("seed local storage");

    Arc::new(StorageRegistry::new(storages_provider, base_directory, Default::default()))
}

fn build_services(
    table_def: Arc<TableDefinition>,
    backend: Arc<dyn StorageBackend>,
) -> OwnedServices {
    let schema = table_def.to_arrow_schema().expect("build arrow schema");
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage_registry = build_storage_registry(backend, &temp_dir);
    let schema_registry = Arc::new(TestSchemaRegistry::new(
        Arc::clone(&table_def),
        Arc::clone(&schema),
        StorageId::local(),
    ));

    let services = Arc::new(TableServices::new(
        schema_registry,
        Arc::new(SystemColumnsService::new(1)),
        Some(storage_registry),
        Arc::new(NoopManifestService),
        Arc::new(NoopNotificationService),
        Arc::new(NoopClusterCoordinator),
        Arc::new(TestCommitSequence),
        None,
    ));

    OwnedServices {
        services,
        schema,
        temp_dir,
    }
}

fn build_stream_table_definition(table_id: &TableId) -> Arc<TableDefinition> {
    Arc::new(
        TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            kalamdb_commons::TableType::Stream,
            vec![
                ColumnDefinition::new(
                    1,
                    "event_id".to_string(),
                    1,
                    KalamDataType::Text,
                    false,
                    true,
                    false,
                    ColumnDefault::None,
                    None,
                ),
                ColumnDefinition::simple(2, "payload", 2, KalamDataType::Text),
            ],
            TableOptions::stream(3_600),
            None,
        )
        .expect("build stream table definition"),
    )
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn stream_provider_planning_stays_lightweight_until_execution() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let table_id = TableId::new(NamespaceId::new("app"), TableName::new("events_planning"));
    let table_def = build_stream_table_definition(&table_id);
    let services = build_services(Arc::clone(&table_def), Arc::clone(&backend));
    let store = Arc::new(new_stream_table_store(
        &table_id,
        StreamTableStoreConfig {
            base_dir: services.temp_dir.path().join("streams").join("events_planning"),
            max_rows_per_user: 64,
            shard_router: ShardRouter::default_config(),
            ttl_seconds: Some(3_600),
            storage_mode: StreamTableStorageMode::Memory,
        },
    ));
    let provider = StreamTableProvider::new(
        Arc::new(TableProviderCore::new(
            table_def,
            Arc::clone(&services.services),
            "event_id".to_string(),
            Arc::clone(&services.schema),
            HashMap::new(),
        )),
        Arc::clone(&store),
        Some(3_600),
    );

    let user_id = kalamdb_commons::UserId::new("stream-owner");
    let ctx = session_with_user(&user_id);
    let state = ctx.state();
    let plan = provider.scan(&state, None, &[], None).await.expect("build stream plan");

    assert!(plan.as_any().is::<DeferredBatchExec>());

    provider
        .insert(
            &user_id,
            row(vec![
                ("event_id", ScalarValue::Utf8(Some("evt-1".to_string()))),
                ("payload", ScalarValue::Utf8(Some("hello".to_string()))),
            ]),
        )
        .await
        .expect("insert row after planning");

    let batches = collect(plan, state.task_ctx()).await.expect("collect stream plan after insert");

    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 1, "execution should see rows inserted after planning");
}
