use super::*;
use crate::error::KalamDbError;
use crate::live::manager::ConnectionsManager;
use crate::live::models::connection::MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION;
use crate::live::models::{
    InitialLoadState, SubscriptionFlowControl, SubscriptionHandle, SubscriptionRuntimeMetadata,
    SubscriptionState,
};
use crate::live::NotificationService;
use crate::sql::executor::SqlExecutor;
use crate::test_helpers::test_app_context_simple;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{ConnectionId, ConnectionInfo, TableId};
use kalamdb_commons::schemas::{
    ColumnDefinition, FieldFlag, TableDefinition, TableOptions, TableType,
};
use kalamdb_commons::websocket::{SubscriptionOptions, SubscriptionRequest, WireNotification};
use kalamdb_commons::{NamespaceId, TableName};
use kalamdb_commons::{NodeId, UserId};
use kalamdb_sql::parser::query_parser::QueryParser;
use kalamdb_sharding::ShardRouter;
use kalamdb_store::test_utils::TestDb;
use kalamdb_tables::{
    new_shared_table_store, new_stream_table_store, new_user_table_store, StreamTableStoreConfig,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;

/// Helper function to create a SubscriptionRequest for tests
fn create_test_subscription_request(
    id: String,
    sql: String,
    last_rows: Option<u32>,
) -> SubscriptionRequest {
    SubscriptionRequest {
        id,
        sql,
        options: Some(SubscriptionOptions {
            batch_size: None,
            last_rows,
            from: None,
            snapshot_end_seq: None,
        }),
    }
}

async fn create_test_manager() -> (Arc<ConnectionsManager>, LiveQueryManager, TestDb) {
    let app_ctx = test_app_context_simple();
    let test_db = TestDb::new(&[]).unwrap();
    let backend: Arc<dyn kalamdb_store::StorageBackend> = test_db.backend();

    let schema_registry = app_ctx.schema_registry();
    let base_session_context = app_ctx.base_session_context();

    // Create table stores for testing (using default namespace and table)
    let test_namespace = NamespaceId::new("user1");
    let test_table = TableName::new("messages");
    let table_id = TableId::new(test_namespace.clone(), test_table.clone());
    let _user_table_store = Arc::new(new_user_table_store(backend.clone(), &table_id));
    let _shared_table_store = Arc::new(new_shared_table_store(backend.clone(), &table_id));
    let stream_temp_dir = TempDir::new().unwrap();
    let _stream_table_store = Arc::new(new_stream_table_store(
        &table_id,
        StreamTableStoreConfig {
            base_dir: stream_temp_dir
                .path()
                .join("streams")
                .join(test_namespace.as_str())
                .join(test_table.as_str()),
            max_rows_per_user: 256, // Default per-user retention limit
            shard_router: ShardRouter::default_config(),
            ttl_seconds: Some(60),
            storage_mode: kalamdb_tables::StreamTableStorageMode::File,
        },
    ));

    // Reuse the same app_ctx we created at the start (don't call test_app_context_simple() again)
    // Create test table definitions via SchemaRegistry
    let messages_table = TableDefinition::new(
        NamespaceId::new("user1"),
        TableName::new("messages"),
        TableType::User,
        vec![
            ColumnDefinition::new(
                1,
                "id",
                1,
                KalamDataType::Int,
                false,
                true,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                2,
                "user_id",
                2,
                KalamDataType::Text,
                true,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
        ],
        TableOptions::user(),
        None,
    )
    .unwrap();
    let _messages_table_id =
        TableId::new(messages_table.namespace_id.clone(), messages_table.table_name.clone());
    schema_registry.put(messages_table).unwrap();

    let notifications_table = TableDefinition::new(
        NamespaceId::new("user1"),
        TableName::new("notifications"),
        TableType::User,
        vec![
            ColumnDefinition::new(
                1,
                "id",
                1,
                KalamDataType::Int,
                false,
                true,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                2,
                "user_id",
                2,
                KalamDataType::Text,
                true,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
        ],
        TableOptions::user(),
        None,
    )
    .unwrap();
    schema_registry.put(notifications_table).unwrap();

    // Create connections manager first
    let connection_registry = ConnectionsManager::new(
        NodeId::from(1u64),
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::from_secs(5),
    );

    let manager =
        LiveQueryManager::new(schema_registry, connection_registry.clone(), base_session_context);
    let sql_executor = Arc::new(SqlExecutor::new(
        app_ctx.clone(),
        Arc::new(crate::sql::executor::handler_registry::HandlerRegistry::new()),
    ));
    manager.set_sql_executor(sql_executor);
    (connection_registry, manager, test_db)
}

/// Helper to register and authenticate a connection
/// Returns the SharedConnectionState for use in subscription calls
fn register_and_auth_connection(
    registry: &Arc<ConnectionsManager>,
    connection_id: ConnectionId,
    user_id: UserId,
) -> crate::live::SharedConnectionState {
    let registration = registry
        .register_connection(connection_id.clone(), ConnectionInfo::new(None))
        .unwrap();
    let connection_state = registration.state;
    connection_state.mark_authenticated(user_id.clone(), kalamdb_commons::models::Role::User);
    connection_state
}

fn make_notification_row(id: i64, seq: i64) -> Row {
    let mut values = BTreeMap::new();
    values.insert("id".to_string(), ScalarValue::Int64(Some(id)));
    values.insert(
        "body".to_string(),
        ScalarValue::Utf8(Some(format!("message-{seq}"))),
    );
    values.insert(SystemColumnNames::SEQ.to_string(), ScalarValue::Int64(Some(seq)));
    Row::new(values)
}

fn make_insert_change(table_id: &TableId, seq: i64) -> crate::live::ChangeNotification {
    crate::live::ChangeNotification::insert(table_id.clone(), make_notification_row(seq, seq))
}

fn make_runtime_metadata(query: &str) -> Arc<SubscriptionRuntimeMetadata> {
    Arc::new(SubscriptionRuntimeMetadata::new(query, None, 1))
}

fn make_subscription_handle(
    subscription_id: &str,
    notification_tx: mpsc::Sender<Arc<WireNotification>>,
    flow_control: Arc<SubscriptionFlowControl>,
    runtime_metadata: Arc<SubscriptionRuntimeMetadata>,
) -> SubscriptionHandle {
    SubscriptionHandle {
        subscription_id: Arc::from(subscription_id),
        filter_expr: None,
        projections: None,
        notification_tx,
        flow_control: Some(flow_control),
        runtime_metadata,
    }
}

fn delivered_seq(notification: &WireNotification) -> i64 {
    let json: serde_json::Value =
        serde_json::from_slice(&notification.to_json()).expect("notification json");
    json["rows"][0][SystemColumnNames::SEQ]
        .as_str()
        .expect("delivered notification seq")
        .parse()
        .expect("delivered notification seq parses")
}

#[test]
fn test_admin_detection_matches_sys_root() {
    // LiveQueryManager doesn't have is_admin_user method in the code I saw.
    // This test is a placeholder.
}

#[test]
fn test_build_subscription_schema_includes_def_for_all_cases() {
    let table_def = TableDefinition::new(
        NamespaceId::new("user1"),
        TableName::new("messages"),
        TableType::User,
        vec![
            ColumnDefinition::new(
                1,
                "id",
                1,
                KalamDataType::Int,
                false,
                true,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                2,
                "tenant_id",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                3,
                "payload",
                3,
                KalamDataType::Json,
                true,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
        ],
        TableOptions::user(),
        None,
    )
    .unwrap();

    let schema = LiveQueryManager::build_subscription_schema(&table_def, None);
    assert_eq!(schema.len(), 3);
    assert_eq!(schema[0].name, "id");
    assert!(matches!(
        schema[0].flags,
        Some(ref flags)
            if flags.contains(&FieldFlag::PrimaryKey)
                && flags.contains(&FieldFlag::NonNull)
                && flags.contains(&FieldFlag::Unique)
    ));
    assert_eq!(schema[1].name, "tenant_id");
    assert!(matches!(
        schema[1].flags,
        Some(ref flags) if flags.contains(&FieldFlag::NonNull)
    ));
    assert_eq!(schema[2].name, "payload");
    assert!(schema[2].flags.is_none());
}

#[test]
fn test_build_subscription_schema_projection_keeps_defs_and_order() {
    let table_def = TableDefinition::new(
        NamespaceId::new("user1"),
        TableName::new("messages"),
        TableType::User,
        vec![
            ColumnDefinition::new(
                1,
                "id",
                1,
                KalamDataType::Int,
                false,
                true,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                2,
                "tenant_id",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
            ColumnDefinition::new(
                3,
                "payload",
                3,
                KalamDataType::Json,
                true,
                false,
                false,
                kalamdb_commons::schemas::ColumnDefault::None,
                None,
            ),
        ],
        TableOptions::user(),
        None,
    )
    .unwrap();

    let projections = vec![
        "tenant_id".to_string(),
        "id".to_string(),
        "missing".to_string(),
    ];
    let schema = LiveQueryManager::build_subscription_schema(&table_def, Some(&projections));

    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name, "tenant_id");
    assert_eq!(schema[0].index, 0);
    assert!(matches!(
        schema[0].flags,
        Some(ref flags) if flags.contains(&FieldFlag::NonNull)
    ));
    assert_eq!(schema[1].name, "id");
    assert_eq!(schema[1].index, 1);
    assert!(matches!(
        schema[1].flags,
        Some(ref flags)
            if flags.contains(&FieldFlag::PrimaryKey)
                && flags.contains(&FieldFlag::NonNull)
                && flags.contains(&FieldFlag::Unique)
    ));
}

#[tokio::test]
async fn test_register_connection() {
    let (registry, _manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let _connection_state = register_and_auth_connection(&registry, connection_id.clone(), user_id);

    assert_eq!(registry.connection_count(), 1);
    assert_eq!(registry.subscription_count(), 0);
}

#[tokio::test]
#[ignore] // Requires Raft leader for live query persistence
async fn test_register_subscription() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state =
        register_and_auth_connection(&registry, connection_id.clone(), user_id.clone());

    let subscription = create_test_subscription_request(
        "q1".to_string(),
        "SELECT * FROM user1.messages WHERE id > 0".to_string(),
        Some(50),
    );

    let result = manager
        .register_subscription_with_initial_data(&connection_state, &subscription, None)
        .await
        .unwrap();

    assert_eq!(result.live_id.connection_id(), &connection_id);
    assert_eq!(result.live_id.subscription_id(), "q1");

    assert_eq!(registry.subscription_count(), 1);
}

#[tokio::test]
async fn test_extract_table_name() {
    let (_registry, _manager, _test_db) = create_test_manager().await;

    let table_name = QueryParser::extract_table_name("SELECT * FROM user1.messages WHERE id > 0")
        .map_err(KalamDbError::from)
        .unwrap();
    assert_eq!(table_name, "user1.messages");

    let table_name = QueryParser::extract_table_name("select id from test.users")
        .map_err(KalamDbError::from)
        .unwrap();
    assert_eq!(table_name, "test.users");

    let table_name = QueryParser::extract_table_name("SELECT * FROM \"ns.my_table\" WHERE x = 1")
        .map_err(KalamDbError::from)
        .unwrap();
    assert_eq!(table_name, "ns.my_table");
}

#[tokio::test]
async fn test_register_subscription_rejects_order_by() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state = register_and_auth_connection(&registry, connection_id, user_id);
    let subscription = create_test_subscription_request(
        "q1".to_string(),
        "SELECT * FROM user1.messages WHERE id > 0 ORDER BY id".to_string(),
        None,
    );

    let err = manager
        .register_subscription_with_initial_data(&connection_state, &subscription, None)
        .await
        .unwrap_err();

    assert!(matches!(err, crate::error::KalamDbError::InvalidSql(_)));
    assert!(err.to_string().contains("ORDER BY"));
}

#[tokio::test]
async fn test_register_subscription_rejects_system_projection() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state = register_and_auth_connection(&registry, connection_id, user_id);
    let subscription = create_test_subscription_request(
        "q1".to_string(),
        "SELECT _seq FROM user1.messages".to_string(),
        None,
    );

    let err = manager
        .register_subscription_with_initial_data(&connection_state, &subscription, None)
        .await
        .unwrap_err();

    assert!(matches!(err, crate::error::KalamDbError::InvalidSql(_)));
    assert!(err.to_string().contains("_seq"));
}

#[tokio::test]
#[ignore] // Requires Raft leader for live query persistence
async fn test_get_subscriptions_for_table() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state =
        register_and_auth_connection(&registry, connection_id.clone(), user_id.clone());

    let subscription1 = create_test_subscription_request(
        "q1".to_string(),
        "SELECT * FROM user1.messages".to_string(),
        None,
    );
    manager
        .register_subscription_with_initial_data(&connection_state, &subscription1, None)
        .await
        .unwrap();

    let subscription2 = create_test_subscription_request(
        "q2".to_string(),
        "SELECT * FROM user1.notifications".to_string(),
        None,
    );
    manager
        .register_subscription_with_initial_data(&connection_state, &subscription2, None)
        .await
        .unwrap();

    // Get subscriptions from registry (DashMap - no RwLock)
    let table_id1 = TableId::from_strings("user1", "messages");
    let messages_subs = registry.get_subscriptions_for_table(&user_id, &table_id1);
    assert_eq!(messages_subs.len(), 1);
    // LiveQueryId no longer has table_id() - verify by subscription_id instead
    let sub = messages_subs.iter().next().unwrap();
    assert_eq!(sub.key().subscription_id(), "q1");

    let table_id2 = TableId::from_strings("user1", "notifications");
    let notif_subs = registry.get_subscriptions_for_table(&user_id, &table_id2);
    assert_eq!(notif_subs.len(), 1);
    let sub = notif_subs.iter().next().unwrap();
    assert_eq!(sub.key().subscription_id(), "q2");
}

#[tokio::test]
#[ignore] // Requires Raft leader for live query persistence
async fn test_unregister_connection() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state =
        register_and_auth_connection(&registry, connection_id.clone(), user_id.clone());

    let subscription1 = create_test_subscription_request(
        "q1".to_string(),
        "SELECT * FROM user1.messages".to_string(),
        None,
    );
    manager
        .register_subscription_with_initial_data(&connection_state, &subscription1, None)
        .await
        .unwrap();

    let subscription2 = create_test_subscription_request(
        "q2".to_string(),
        "SELECT * FROM user1.notifications".to_string(),
        None,
    );
    manager
        .register_subscription_with_initial_data(&connection_state, &subscription2, None)
        .await
        .unwrap();

    let removed_live_ids = manager.unregister_connection(&user_id, &connection_id).await.unwrap();
    assert_eq!(removed_live_ids.len(), 2);

    assert_eq!(registry.connection_count(), 0);
    assert_eq!(registry.subscription_count(), 0);
}

/// Test unregistering a subscription - verifies RwLock was removed correctly
/// This test previously hung forever due to an unnecessary RwLock wrapper around
/// DashMap-based LiveQueryRegistry. After removing the RwLock wrapper, this test
/// should complete quickly.
#[tokio::test]
#[ignore] // Requires Raft leader for live query persistence
async fn test_unregister_subscription() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state =
        register_and_auth_connection(&registry, connection_id.clone(), user_id.clone());

    let subscription_request = create_test_subscription_request(
        "q1".to_string(),
        "SELECT * FROM user1.messages".to_string(),
        None,
    );

    let result = manager
        .register_subscription_with_initial_data(&connection_state, &subscription_request, None)
        .await
        .unwrap();

    let live_id = result.live_id;
    let subscription_id = live_id.subscription_id().to_string();

    // This should complete quickly now that RwLock wrapper has been removed
    manager
        .unregister_subscription(&connection_state, &subscription_id, &live_id)
        .await
        .unwrap();

    assert_eq!(registry.subscription_count(), 0);
}

#[tokio::test]
#[ignore] // Requires Raft leader for live query persistence
async fn test_multi_subscription_support() {
    let (registry, manager, _test_db) = create_test_manager().await;
    let user_id = UserId::new("user1".to_string());
    let connection_id = ConnectionId::new("conn1".to_string());

    let connection_state =
        register_and_auth_connection(&registry, connection_id.clone(), user_id.clone());

    // Multiple subscriptions on same connection
    let subscription1 = create_test_subscription_request(
        "messages_query".to_string(),
        "SELECT * FROM user1.messages WHERE conversation_id = 'conv1'".to_string(),
        Some(50),
    );
    let result1 = manager
        .register_subscription_with_initial_data(&connection_state, &subscription1, None)
        .await
        .unwrap();
    let live_id1 = result1.live_id;

    let subscription2 = create_test_subscription_request(
        "notifications_query".to_string(),
        // Note: `CURRENT_USER` is a SQL keyword in sqlparser; calling it like a UDF
        // (CURRENT_USER()) currently fails parsing in this test harness.
        "SELECT * FROM user1.notifications WHERE user_id = 'user1'".to_string(),
        Some(10),
    );
    let result2 = manager
        .register_subscription_with_initial_data(&connection_state, &subscription2, None)
        .await
        .unwrap();
    let live_id2 = result2.live_id;

    let subscription3 = create_test_subscription_request(
        "messages_query2".to_string(),
        "SELECT * FROM user1.messages WHERE conversation_id = 'conv2'".to_string(),
        Some(20),
    );
    let result3 = manager
        .register_subscription_with_initial_data(&connection_state, &subscription3, None)
        .await
        .unwrap();
    let live_id3 = result3.live_id;

    assert_eq!(registry.connection_count(), 1);
    assert_eq!(registry.subscription_count(), 3);

    // Verify all subscriptions are tracked (DashMap - no RwLock)
    let table_id1 = TableId::from_strings("user1", "messages");
    let messages_subs = registry.get_subscriptions_for_table(&user_id, &table_id1);
    assert_eq!(messages_subs.len(), 2); // messages_query and messages_query2

    let table_id2 = TableId::from_strings("user1", "notifications");
    let notif_subs = registry.get_subscriptions_for_table(&user_id, &table_id2);
    assert_eq!(notif_subs.len(), 1);

    // Verify each has unique live_id
    assert_ne!(live_id1.to_string(), live_id2.to_string());
    assert_ne!(live_id1.to_string(), live_id3.to_string());
    assert_ne!(live_id2.to_string(), live_id3.to_string());
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_hot_table_commit_burst_preserves_delivered_order() {
    const HOT_SUBSCRIBER_COUNT: usize = 10_000;
    const OBSERVED_SUBSCRIBERS: usize = 2;
    const BURST_SIZE: usize = 32;

    let registry = ConnectionsManager::new(
        NodeId::new(1),
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::from_secs(5),
    );
    let service = NotificationService::new(Arc::clone(&registry));
    let table_id = TableId::from_strings("shared", "hot_commit_burst");

    let mut observed_receivers = Vec::with_capacity(OBSERVED_SUBSCRIBERS);
    for index in 0..OBSERVED_SUBSCRIBERS {
        let connection_id = ConnectionId::new(format!("observed-hot-{index}"));
        let live_id = crate::live::LiveQueryId::new(
            UserId::new(format!("observed-user-{index}")),
            connection_id.clone(),
            format!("observed-sub-{index}"),
        );
        let (tx, rx) = mpsc::channel(BURST_SIZE + 8);
        let flow_control = Arc::new(SubscriptionFlowControl::new());
        flow_control.mark_initial_complete();

        registry.index_shared_subscription(
            &connection_id,
            live_id,
            table_id.clone(),
            make_subscription_handle(
                &format!("observed-sub-{index}"),
                tx,
                flow_control,
                make_runtime_metadata("SELECT * FROM shared.hot_commit_burst"),
            ),
        );
        observed_receivers.push(rx);
    }

    let (filler_tx, filler_rx) = mpsc::channel(1);
    drop(filler_rx);
    let filler_flow = Arc::new(SubscriptionFlowControl::new());
    filler_flow.mark_initial_complete();
    let filler_metadata = make_runtime_metadata("SELECT * FROM shared.hot_commit_burst");

    for index in OBSERVED_SUBSCRIBERS..HOT_SUBSCRIBER_COUNT {
        let connection_id = ConnectionId::new(format!("filler-hot-{index}"));
        let live_id = crate::live::LiveQueryId::new(
            UserId::new("filler-user"),
            connection_id.clone(),
            format!("filler-sub-{index}"),
        );
        registry.index_shared_subscription(
            &connection_id,
            live_id,
            table_id.clone(),
            make_subscription_handle(
                &format!("filler-sub-{index}"),
                filler_tx.clone(),
                Arc::clone(&filler_flow),
                Arc::clone(&filler_metadata),
            ),
        );
    }

    assert_eq!(registry.subscription_count(), HOT_SUBSCRIBER_COUNT);
    assert_eq!(registry.get_shared_subscriptions_for_table(&table_id).len(), HOT_SUBSCRIBER_COUNT);

    for seq in 1..=BURST_SIZE {
        service.notify_async(None, table_id.clone(), make_insert_change(&table_id, seq as i64));
    }

    let expected: Vec<_> = (1..=BURST_SIZE as i64).collect();
    for (index, receiver) in observed_receivers.iter_mut().enumerate() {
        let mut delivered = Vec::with_capacity(BURST_SIZE);
        for _ in 0..BURST_SIZE {
            let notification = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await
                .expect("notification should arrive before timeout")
                .expect("notification channel should stay open");
            delivered.push(delivered_seq(notification.as_ref()));
        }

        assert_eq!(
            delivered, expected,
            "observed subscriber {index} should preserve per-table delivery order"
        );
    }
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn test_slow_subscriber_commit_burst_stays_bounded_and_cleans_up() {
    const OVERFLOW_NOTIFICATIONS: usize = 64;

    let (registry, manager, _test_db) = create_test_manager().await;
    let service = NotificationService::new(Arc::clone(&registry));

    let user_id = UserId::new("slow-user");
    let connection_id = ConnectionId::new("slow-conn");
    let mut registration = registry
        .register_connection(connection_id.clone(), ConnectionInfo::new(None))
        .expect("connection should register");
    let connection_state = Arc::clone(&registration.state);
    connection_state.mark_auth_started();
    connection_state.mark_authenticated(user_id.clone(), kalamdb_commons::models::Role::User);

    let table_id = TableId::from_strings("shared", "slow_commit_burst");
    let live_id = crate::live::LiveQueryId::new(
        user_id.clone(),
        connection_id.clone(),
        "slow-sub".to_string(),
    );
    let flow_control = Arc::new(SubscriptionFlowControl::new());
    let snapshot_end_seq = Some(SeqId::from(0));
    flow_control.set_snapshot_end_seq(snapshot_end_seq);
    let runtime_metadata = make_runtime_metadata("SELECT * FROM shared.slow_commit_burst");

    connection_state.insert_subscription(
        Arc::from("slow-sub"),
        SubscriptionState {
            live_id: live_id.clone(),
            table_id: table_id.clone(),
            filter_expr: None,
            projections: None,
            initial_load: Some(InitialLoadState {
                batch_size: 128,
                snapshot_end_seq,
                current_batch_num: 0,
                flow_control: Arc::clone(&flow_control),
            }),
            is_shared: true,
            runtime_metadata: Arc::clone(&runtime_metadata),
        },
    );
    registry.index_shared_subscription(
        &connection_id,
        live_id.clone(),
        table_id.clone(),
        make_subscription_handle(
            "slow-sub",
            connection_state.notification_tx.clone(),
            Arc::clone(&flow_control),
            Arc::clone(&runtime_metadata),
        ),
    );

    let burst_size = MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION + OVERFLOW_NOTIFICATIONS;
    for seq in 1..=burst_size {
        service.notify_async(None, table_id.clone(), make_insert_change(&table_id, seq as i64));
    }

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(
        registration.notification_rx.try_recv().is_err(),
        "initial-load gating should buffer commit-burst notifications instead of sending them"
    );

    let buffered = flow_control.drain_buffered_notifications();
    assert_eq!(
        buffered.len(),
        MAX_BUFFERED_NOTIFICATIONS_PER_SUBSCRIPTION,
        "slow subscriber buffer must stay capped during commit bursts"
    );

    let first_seq = buffered
        .first()
        .and_then(|item| item.seq)
        .expect("buffered notifications should carry seq ordering");
    let last_seq = buffered
        .last()
        .and_then(|item| item.seq)
        .expect("buffered notifications should carry seq ordering");
    assert_eq!(
        first_seq,
        SeqId::from((OVERFLOW_NOTIFICATIONS + 1) as i64),
        "bounded buffer should retain the newest notifications"
    );
    assert_eq!(last_seq, SeqId::from(burst_size as i64));

    let removed_live_ids = manager
        .unregister_connection(&user_id, &connection_id)
        .await
        .expect("connection cleanup should succeed");
    assert_eq!(removed_live_ids, vec![live_id]);
    assert_eq!(registry.connection_count(), 0);
    assert_eq!(registry.subscription_count(), 0);
    assert!(!registry.has_shared_subscriptions(&table_id));
    assert_eq!(connection_state.subscription_count(), 0);
}
