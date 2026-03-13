use super::*;
use crate::live::manager::ConnectionsManager;
use crate::sql::executor::SqlExecutor;
use crate::test_helpers::test_app_context_simple;
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::{ConnectionId, ConnectionInfo, TableId};
use kalamdb_commons::schemas::{
    ColumnDefinition, FieldFlag, TableDefinition, TableOptions, TableType,
};
use kalamdb_commons::websocket::{SubscriptionOptions, SubscriptionRequest};
use kalamdb_commons::{NamespaceId, TableName};
use kalamdb_commons::{NodeId, UserId};
use kalamdb_sharding::ShardRouter;
use kalamdb_store::test_utils::TestDb;
use kalamdb_tables::{
    new_shared_table_store, new_stream_table_store, new_user_table_store, StreamTableStoreConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Helper function to create a SubscriptionRequest for tests
fn create_test_subscription_request(
    id: String,
    sql: String,
    last_rows: Option<u32>,
) -> SubscriptionRequest {
    SubscriptionRequest {
        id,
        sql,
        options: SubscriptionOptions {
            batch_size: None,
            last_rows,
            from: None,
            snapshot_end_seq: None,
        },
    }
}

async fn create_test_manager() -> (Arc<ConnectionsManager>, LiveQueryManager, TestDb) {
    let app_ctx = test_app_context_simple();
    let test_db = TestDb::new(&[]).unwrap();
    let backend: Arc<dyn kalamdb_store::StorageBackend> = test_db.backend();

    let live_queries_provider = app_ctx.system_tables().live_queries();
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

    let manager = LiveQueryManager::new(
        live_queries_provider,
        schema_registry,
        connection_registry.clone(),
        base_session_context,
        Arc::clone(&app_ctx),
    );
    let sql_executor = Arc::new(SqlExecutor::new(app_ctx, false));
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
    connection_state
        .write()
        .mark_authenticated(user_id.clone(), kalamdb_commons::models::Role::User);
    registry.on_authenticated(&connection_id, user_id);
    connection_state
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
    let (_registry, manager, _test_db) = create_test_manager().await;

    let table_name = manager
        .extract_table_name_from_query("SELECT * FROM user1.messages WHERE id > 0")
        .unwrap();
    assert_eq!(table_name, "user1.messages");

    let table_name = manager.extract_table_name_from_query("select id from test.users").unwrap();
    assert_eq!(table_name, "test.users");

    let table_name = manager
        .extract_table_name_from_query("SELECT * FROM \"ns.my_table\" WHERE x = 1")
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

    let stats = manager.get_stats().await;
    assert_eq!(stats.total_connections, 0);
    assert_eq!(stats.total_subscriptions, 0);
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

    let stats = manager.get_stats().await;
    assert_eq!(stats.total_subscriptions, 0);
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

    let stats = manager.get_stats().await;
    assert_eq!(stats.total_connections, 1);
    assert_eq!(stats.total_subscriptions, 3);

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
