use super::test_support::{consolidated_helpers, TestServer};
use kalam_link::models::ResponseStatus;
use kalamdb_api::handlers::ws::events::cleanup::cleanup_connection;
use kalamdb_api::limiter::RateLimiter;
use kalamdb_commons::models::{ConnectionId, ConnectionInfo, Role, UserId};
use kalamdb_commons::websocket::{SubscriptionOptions, SubscriptionRequest};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn test_live_queries_metadata() {
    let server = TestServer::new_shared().await;
    let manager = server.app_context.live_query_manager();
    let registry = manager.registry();
    let ns = consolidated_helpers::unique_namespace("live_queries_meta");

    // 1. Register Connection
    let user_id = UserId::new("root");
    let conn_id = ConnectionId::new("conn_meta_test".to_string());

    let registration = registry
        .register_connection(conn_id.clone(), ConnectionInfo::new(None))
        .expect("Failed to register connection");
    let connection_state = registration.state;

    // Authenticate the connection
    connection_state.mark_authenticated(user_id.clone(), Role::User);
    registry.on_authenticated(&conn_id, user_id.clone());

    let create_ns = format!("CREATE NAMESPACE IF NOT EXISTS {}", ns);
    let ns_resp = server.execute_sql_as_user(&create_ns, "root").await;
    assert_eq!(
        ns_resp.status,
        ResponseStatus::Success,
        "namespace create failed: {:?}",
        ns_resp.error
    );

    let create_table = format!(
        "CREATE TABLE {}.events (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'USER')",
        ns
    );
    let table_resp = server.execute_sql_as_user(&create_table, "root").await;
    assert_eq!(
        table_resp.status,
        ResponseStatus::Success,
        "table create failed: {:?}",
        table_resp.error
    );

    // 2. Subscribe using SubscriptionRequest
    let subscription1 = SubscriptionRequest {
        id: "sub_meta_test".to_string(),
        sql: format!("SELECT * FROM {}.events", ns),
        options: SubscriptionOptions::default(),
    };

    println!("Registering subscription...");
    let result = manager
        .register_subscription_with_initial_data(&connection_state, &subscription1, None)
        .await
        .expect("Failed to register subscription");
    let live_id = result.live_id;
    println!("Subscription registered: {}", live_id);

    // 3. Query system.live_queries via SQL
    let sql = "SELECT * FROM system.live_queries WHERE subscription_id = 'sub_meta_test'";
    println!("Executing SQL...");
    let response = server.execute_sql(sql).await;
    println!("SQL executed");

    assert_eq!(response.status, ResponseStatus::Success, "SQL failed: {:?}", response.error);
    let rows = response.results[0].rows.as_ref().expect("Expected rows");
    assert_eq!(rows.len(), 1, "Expected 1 row in system.live_queries");

    // 4. Unsubscribe
    let subscription_id = live_id.subscription_id().to_string();
    manager
        .unregister_subscription(&connection_state, &subscription_id, &live_id)
        .await
        .expect("Failed to unregister");

    // 5. Query again - should be empty
    let response_after = server.execute_sql(sql).await;
    assert_eq!(
        response_after.status,
        ResponseStatus::Success,
        "SQL failed: {:?}",
        response_after.error
    );
    let rows_after = response_after.results[0].rows.as_ref().expect("Expected rows");
    assert_eq!(rows_after.len(), 0, "Expected 0 rows in system.live_queries after unsubscribe");

    // 6. Test connection close cleanup
    // Re-subscribe
    let subscription2 = SubscriptionRequest {
        id: "sub_meta_test2".to_string(),
        sql: format!("SELECT * FROM {}.events", ns),
        options: SubscriptionOptions::default(),
    };
    let result2 = manager
        .register_subscription_with_initial_data(&connection_state, &subscription2, None)
        .await
        .expect("Failed to register subscription 2");
    let live_id2 = result2.live_id;
    println!("Second subscription registered: {}", live_id2);

    // Verify it exists
    let sql2 = "SELECT * FROM system.live_queries WHERE subscription_id = 'sub_meta_test2'";
    let response_before_close = server.execute_sql(sql2).await;
    assert_eq!(response_before_close.results[0].rows.as_ref().unwrap().len(), 1);

    // Close connection
    manager
        .unregister_connection(&user_id, &conn_id)
        .await
        .expect("Failed to unregister connection");

    // Verify all subscriptions removed
    let response_after_close = server.execute_sql(sql2).await;
    assert_eq!(
        response_after_close.results[0].rows.as_ref().unwrap().len(),
        0,
        "Expected 0 rows after connection close"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleanup_connection_removes_live_queries_and_registry_state() {
    let server = TestServer::new_shared().await;
    let manager = server.app_context.live_query_manager();
    let registry = manager.registry();
    let rate_limiter = Arc::new(RateLimiter::new());
    let ns = consolidated_helpers::unique_namespace("live_queries_cleanup");

    let user_id = UserId::new("root");
    let conn_id = ConnectionId::new("conn_cleanup_test".to_string());

    let registration = registry
        .register_connection(conn_id.clone(), ConnectionInfo::new(None))
        .expect("Failed to register connection");
    let connection_state = registration.state;

    connection_state.mark_authenticated(user_id.clone(), Role::User);
    registry.on_authenticated(&conn_id, user_id.clone());

    let create_ns = format!("CREATE NAMESPACE IF NOT EXISTS {}", ns);
    let ns_resp = server.execute_sql_as_user(&create_ns, "root").await;
    assert_eq!(
        ns_resp.status,
        ResponseStatus::Success,
        "namespace create failed: {:?}",
        ns_resp.error
    );

    let create_table = format!(
        "CREATE TABLE {}.events (id INT PRIMARY KEY, value TEXT) WITH (TYPE = 'USER')",
        ns
    );
    let table_resp = server.execute_sql_as_user(&create_table, "root").await;
    assert_eq!(
        table_resp.status,
        ResponseStatus::Success,
        "table create failed: {:?}",
        table_resp.error
    );

    let subscription = SubscriptionRequest {
        id: "sub_cleanup_test".to_string(),
        sql: format!("SELECT * FROM {}.events", ns),
        options: SubscriptionOptions::default(),
    };

    manager
        .register_subscription_with_initial_data(&connection_state, &subscription, None)
        .await
        .expect("Failed to register subscription");

    let sql = "SELECT * FROM system.live_queries WHERE subscription_id = 'sub_cleanup_test'";
    let response_before = server.execute_sql(sql).await;
    assert_eq!(
        response_before.status,
        ResponseStatus::Success,
        "SQL failed: {:?}",
        response_before.error
    );
    assert_eq!(response_before.results[0].rows.as_ref().unwrap().len(), 1);

    cleanup_connection(&connection_state, &registry, &rate_limiter, &manager).await;

    assert!(
        registry.get_connection(&conn_id).is_none(),
        "connection registry entry should be removed"
    );

    let response_after = server.execute_sql(sql).await;
    assert_eq!(
        response_after.status,
        ResponseStatus::Success,
        "SQL failed: {:?}",
        response_after.error
    );
    assert_eq!(
        response_after.results[0].rows.as_ref().unwrap().len(),
        0,
        "Expected 0 rows in system.live_queries after cleanup_connection"
    );
}
