use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::collect;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::models::datatypes::KalamDataType;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition, TableOptions};
use kalamdb_commons::models::{NamespaceId, ReadContext, Role, StorageId, TableId, TableName};
use kalamdb_commons::schemas::ColumnDefault;
use kalamdb_commons::websocket::ChangeNotification;
use kalamdb_commons::{OperationKind, TableAccess, TableType, TransactionId, UserId};
use kalamdb_datafusion_sources::exec::DeferredBatchExec;
use kalamdb_filestore::StorageRegistry;
use kalamdb_sharding::ShardRouter;
use kalamdb_store::test_utils::InMemoryBackend;
use kalamdb_store::{StorageBackend, StorageError};
use kalamdb_system::{
    ClusterCoordinator, Manifest, ManifestCacheEntry, ManifestService, NotificationService,
    SchemaRegistry, SessionUserContext, Storage, StorageType, StoragesTableProvider,
    SystemColumnsService,
};
use kalamdb_tables::utils::TableServices;
use kalamdb_tables::{
    new_indexed_shared_table_store, new_indexed_user_table_store, new_stream_table_store,
    BaseTableProvider, SharedTableProvider, SharedTableRow, StreamTableProvider,
    StreamTableStorageMode, StreamTableStoreConfig, TableProviderCore, UserTableProvider,
    UserTableRow,
};
use kalamdb_transactions::{
    CommitSequenceSource, TransactionAccessError, TransactionAccessValidator, TransactionMutationSink,
    TransactionOverlay, TransactionOverlayEntry, TransactionOverlayExec, TransactionQueryContext,
    TransactionQueryExtension,
};
use tempfile::TempDir;

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn row(values: Vec<(&str, ScalarValue)>) -> Row {
    Row::from_vec(
        values
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    )
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
        )
            == table_id
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
        )
            == table_id
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
        _user_id: Option<&UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        Ok(None)
    }

    async fn get_or_load_async(
        &self,
        _table_id: &TableId,
        _user_id: Option<&UserId>,
    ) -> Result<Option<Arc<ManifestCacheEntry>>, StorageError> {
        Ok(None)
    }

    fn validate_manifest(&self, _manifest: &Manifest) -> Result<(), StorageError> {
        Ok(())
    }

    fn mark_as_stale(
        &self,
        _table_id: &TableId,
        _user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn rebuild_manifest(
        &self,
        _table_id: &TableId,
        _user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        panic!("rebuild_manifest is unused in provider source-model tests")
    }

    fn mark_pending_write(
        &self,
        _table_id: &TableId,
        _user_id: Option<&UserId>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn ensure_manifest_initialized(
        &self,
        _table_id: &TableId,
        _user_id: Option<&UserId>,
    ) -> Result<Manifest, StorageError> {
        panic!("ensure_manifest_initialized is unused in provider source-model tests")
    }

    fn stage_before_flush(
        &self,
        _table_id: &TableId,
        _user_id: Option<&UserId>,
        _manifest: &Manifest,
    ) -> Result<(), StorageError> {
        panic!("stage_before_flush is unused in provider source-model tests")
    }

    fn get_manifest_user_ids(&self, _table_id: &TableId) -> Result<Vec<UserId>, StorageError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Default)]
struct NoopNotificationService;

impl NotificationService for NoopNotificationService {
    type Notification = ChangeNotification;

    fn has_subscribers(&self, _user_id: Option<&UserId>, _table_id: &TableId) -> bool {
        false
    }

    fn notify_table_change(
        &self,
        _user_id: Option<UserId>,
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

    async fn is_leader_for_user(&self, _user_id: &UserId) -> bool {
        true
    }

    async fn is_leader_for_shared(&self) -> bool {
        true
    }

    async fn leader_addr_for_user(&self, _user_id: &UserId) -> Option<String> {
        None
    }

    async fn leader_addr_for_shared(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Default)]
struct TestCommitSequence {
    current: AtomicU64,
}

impl CommitSequenceSource for TestCommitSequence {
    fn current_committed(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    fn allocate_next(&self) -> u64 {
        self.current.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[derive(Debug, Default)]
struct NoopMutationSink;

impl TransactionMutationSink for NoopMutationSink {
    fn stage_mutation(
        &self,
        _transaction_id: &TransactionId,
        _table_id: &TableId,
        _table_type: TableType,
        _user_id: Option<UserId>,
        _operation_kind: OperationKind,
        _primary_key: String,
        _row: Row,
        _is_deleted: bool,
    ) -> Result<(), TransactionAccessError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct AllowAllAccessValidator;

impl TransactionAccessValidator for AllowAllAccessValidator {
    fn validate_table_access(
        &self,
        _transaction_id: &TransactionId,
        _table_id: &TableId,
        _table_type: TableType,
        _user_id: Option<&UserId>,
    ) -> Result<(), TransactionAccessError> {
        Ok(())
    }
}

struct OwnedServices {
    services: Arc<TableServices>,
    schema: SchemaRef,
    _temp_dir: TempDir,
}

fn session_with_user(user_id: &UserId) -> SessionContext {
    let mut state = SessionContext::new().state().clone();
    state.config_mut().options_mut().extensions.insert(SessionUserContext::new(
        user_id.clone(),
        Role::Dba,
        ReadContext::Internal,
    ));
    SessionContext::new_with_state(state)
}

fn session_with_transaction(
    user_id: &UserId,
    tx_context: TransactionQueryContext,
) -> SessionContext {
    let mut state = session_with_user(user_id).state().clone();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(TransactionQueryExtension::new(tx_context));
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
            description: Some("Provider source-model test storage".to_string()),
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

    Arc::new(StorageRegistry::new(
        storages_provider,
        base_directory,
        Default::default(),
    ))
}

fn build_services(table_def: Arc<TableDefinition>, backend: Arc<dyn StorageBackend>) -> OwnedServices {
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
        Arc::new(TestCommitSequence::default()),
        None,
    ));

    OwnedServices {
        services,
        schema,
        _temp_dir: temp_dir,
    }
}

fn build_user_table_definition(table_id: &TableId) -> Arc<TableDefinition> {
    let mut table_def = TableDefinition::new(
        table_id.namespace_id().clone(),
        table_id.table_name().clone(),
        TableType::User,
        vec![
            ColumnDefinition::new(
                1,
                "id".to_string(),
                1,
                KalamDataType::BigInt,
                false,
                true,
                false,
                ColumnDefault::None,
                None,
            ),
            ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
        ],
        TableOptions::user(),
        None,
    )
    .expect("build user table definition");
    SystemColumnsService::new(1)
        .add_system_columns(&mut table_def)
        .expect("add user system columns");
    Arc::new(table_def)
}

fn build_shared_table_definition(table_id: &TableId) -> Arc<TableDefinition> {
    let mut table_options = TableOptions::shared();
    if let TableOptions::Shared(options) = &mut table_options {
        options.access_level = Some(TableAccess::Public);
    }

    let mut table_def = TableDefinition::new(
        table_id.namespace_id().clone(),
        table_id.table_name().clone(),
        TableType::Shared,
        vec![
            ColumnDefinition::new(
                1,
                "id".to_string(),
                1,
                KalamDataType::BigInt,
                false,
                true,
                false,
                ColumnDefault::None,
                None,
            ),
            ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
        ],
        table_options,
        None,
    )
    .expect("build shared table definition");
    SystemColumnsService::new(1)
        .add_system_columns(&mut table_def)
        .expect("add shared system columns");
    Arc::new(table_def)
}

fn build_stream_table_definition(table_id: &TableId) -> Arc<TableDefinition> {
    Arc::new(
        TableDefinition::new(
            table_id.namespace_id().clone(),
            table_id.table_name().clone(),
            TableType::Stream,
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

fn overlay_context(
    transaction_id: TransactionId,
    table_id: TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    primary_key: &str,
    payload: Row,
) -> TransactionQueryContext {
    let mut overlay = TransactionOverlay::new(transaction_id.clone());
    overlay.apply_entry(TransactionOverlayEntry {
        transaction_id: transaction_id.clone(),
        mutation_order: 0,
        table_id,
        table_type,
        user_id,
        operation_kind: OperationKind::Insert,
        primary_key: primary_key.to_string(),
        payload,
        tombstone: false,
    });

    TransactionQueryContext::new(
        transaction_id,
        1,
        Arc::new(overlay),
        Arc::new(NoopMutationSink),
        Arc::new(AllowAllAccessValidator),
    )
}

#[tokio::test]
async fn stream_provider_scan_uses_deferred_batch_exec_and_returns_rows() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let table_id = TableId::new(NamespaceId::new("app"), TableName::new("events"));
    let table_def = build_stream_table_definition(&table_id);
    let services = build_services(Arc::clone(&table_def), Arc::clone(&backend));
    let store = Arc::new(new_stream_table_store(
        &table_id,
        StreamTableStoreConfig {
            base_dir: services._temp_dir.path().join("streams").join("events"),
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

    let user_id = UserId::new("stream-owner");
    provider
        .insert(
            &user_id,
            row(vec![
                ("event_id", ScalarValue::Utf8(Some("evt-1".to_string()))),
                ("payload", ScalarValue::Utf8(Some("hello".to_string()))),
            ]),
        )
        .await
        .expect("seed stream row");

    let ctx = session_with_user(&user_id);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build stream plan");

    assert!(plan.as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect stream plan");
    assert_eq!(total_rows(&batches), 1);
}

#[tokio::test]
async fn user_provider_scan_uses_deferred_batch_exec_and_returns_rows() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let table_id = TableId::new(NamespaceId::new("app"), TableName::new("users_exec_plain"));
    let table_def = build_user_table_definition(&table_id);
    let services = build_services(Arc::clone(&table_def), Arc::clone(&backend));
    let store = Arc::new(new_indexed_user_table_store(Arc::clone(&backend), &table_id, "id"));
    let provider = UserTableProvider::new(
        Arc::new(TableProviderCore::new(
            table_def,
            Arc::clone(&services.services),
            "id".to_string(),
            Arc::clone(&services.schema),
            HashMap::new(),
        )),
        Arc::clone(&store),
    );

    let user_id = UserId::new("user-owner");
    let seq = kalamdb_commons::ids::SeqId::from_i64(1);
    store
        .insert(
            &kalamdb_commons::ids::UserTableRowId::new(user_id.clone(), seq),
            &UserTableRow {
                user_id: user_id.clone(),
                _seq: seq,
                _commit_seq: 1,
                _deleted: false,
                fields: row(vec![
                    ("id", ScalarValue::Int64(Some(1))),
                    ("name", ScalarValue::Utf8(Some("committed".to_string()))),
                ]),
            },
        )
        .expect("seed user row");

    let ctx = session_with_user(&user_id);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build user plan");

    assert!(plan.as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect user plan");
    assert_eq!(total_rows(&batches), 1);
}

#[tokio::test]
async fn user_provider_scan_with_overlay_uses_transaction_overlay_exec() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let table_id = TableId::new(NamespaceId::new("app"), TableName::new("users_exec"));
    let table_def = build_user_table_definition(&table_id);
    let services = build_services(Arc::clone(&table_def), Arc::clone(&backend));
    let store = Arc::new(new_indexed_user_table_store(Arc::clone(&backend), &table_id, "id"));
    let provider = UserTableProvider::new(
        Arc::new(TableProviderCore::new(
            table_def,
            Arc::clone(&services.services),
            "id".to_string(),
            Arc::clone(&services.schema),
            HashMap::new(),
        )),
        Arc::clone(&store),
    );

    let user_id = UserId::new("user-owner");
    let seq = kalamdb_commons::ids::SeqId::from_i64(1);
    store
        .insert(
            &kalamdb_commons::ids::UserTableRowId::new(user_id.clone(), seq),
            &UserTableRow {
                user_id: user_id.clone(),
                _seq: seq,
                _commit_seq: 1,
                _deleted: false,
                fields: row(vec![
                    ("id", ScalarValue::Int64(Some(1))),
                    ("name", ScalarValue::Utf8(Some("committed".to_string()))),
                ]),
            },
        )
        .expect("seed user row");

    let tx_context = overlay_context(
        TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d"),
        table_id.clone(),
        TableType::User,
        Some(user_id.clone()),
        "2",
        row(vec![
            ("id", ScalarValue::Int64(Some(2))),
            ("name", ScalarValue::Utf8(Some("overlay".to_string()))),
        ]),
    );

    let ctx = session_with_transaction(&user_id, tx_context);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build user plan");

    assert!(plan.as_any().is::<TransactionOverlayExec>());
    let child = plan.children().into_iter().next().expect("overlay child plan");
    assert!(child.as_ref().as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect user plan");
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn shared_provider_scan_uses_deferred_batch_exec_and_returns_rows() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let table_id = TableId::new(NamespaceId::new("app"), TableName::new("shared_exec_plain"));
    let table_def = build_shared_table_definition(&table_id);
    let services = build_services(Arc::clone(&table_def), Arc::clone(&backend));
    let store = Arc::new(new_indexed_shared_table_store(Arc::clone(&backend), &table_id, "id"));
    let provider = SharedTableProvider::new(
        Arc::new(TableProviderCore::new(
            table_def,
            Arc::clone(&services.services),
            "id".to_string(),
            Arc::clone(&services.schema),
            HashMap::new(),
        )),
        Arc::clone(&store),
    );

    let seq = kalamdb_commons::ids::SeqId::from_i64(1);
    store
        .insert(
            &seq,
            &SharedTableRow {
                _seq: seq,
                _commit_seq: 1,
                _deleted: false,
                fields: row(vec![
                    ("id", ScalarValue::Int64(Some(1))),
                    ("name", ScalarValue::Utf8(Some("committed".to_string()))),
                ]),
            },
        )
        .expect("seed shared row");

    let user_id = UserId::new("shared-reader");
    let ctx = session_with_user(&user_id);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build shared plan");

    assert!(plan.as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect shared plan");
    assert_eq!(total_rows(&batches), 1);
}

#[tokio::test]
async fn shared_provider_scan_with_overlay_uses_transaction_overlay_exec() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let table_id = TableId::new(NamespaceId::new("app"), TableName::new("shared_exec"));
    let table_def = build_shared_table_definition(&table_id);
    let services = build_services(Arc::clone(&table_def), Arc::clone(&backend));
    let store = Arc::new(new_indexed_shared_table_store(Arc::clone(&backend), &table_id, "id"));
    let provider = SharedTableProvider::new(
        Arc::new(TableProviderCore::new(
            table_def,
            Arc::clone(&services.services),
            "id".to_string(),
            Arc::clone(&services.schema),
            HashMap::new(),
        )),
        Arc::clone(&store),
    );

    let seq = kalamdb_commons::ids::SeqId::from_i64(1);
    store
        .insert(
            &seq,
            &SharedTableRow {
                _seq: seq,
                _commit_seq: 1,
                _deleted: false,
                fields: row(vec![
                    ("id", ScalarValue::Int64(Some(1))),
                    ("name", ScalarValue::Utf8(Some("committed".to_string()))),
                ]),
            },
        )
        .expect("seed shared row");

    let user_id = UserId::new("shared-reader");
    let tx_context = overlay_context(
        TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8e"),
        table_id.clone(),
        TableType::Shared,
        None,
        "2",
        row(vec![
            ("id", ScalarValue::Int64(Some(2))),
            ("name", ScalarValue::Utf8(Some("overlay".to_string()))),
        ]),
    );

    let ctx = session_with_transaction(&user_id, tx_context);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build shared plan");

    assert!(plan.as_any().is::<TransactionOverlayExec>());
    let child = plan.children().into_iter().next().expect("overlay child plan");
    assert!(child.as_ref().as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect shared plan");
    assert_eq!(total_rows(&batches), 2);
}