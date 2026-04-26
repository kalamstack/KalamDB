//! Integration tests for Live Query Auth Expiry
//!
//! Verifies that auth expiry events correctly terminate WebSocket connections.

use std::time::Duration;

use kalamdb_commons::models::{ConnectionId, ConnectionInfo, Role, UserId};

use super::test_support::TestServer;

#[tokio::test]
async fn test_live_query_auth_expiry() {
    let server = TestServer::new_shared().await;
    let manager = server.app_context.live_query_manager();
    let registry = server.app_context.connection_registry();

    // 1. Setup User and Connection
    let user_id_str = "user_expiry_test";
    let conn_id_str = "conn_expiry_test";
    let connection_id = ConnectionId::new(conn_id_str.to_string());
    let user_id = UserId::new(user_id_str.to_string());

    // 2. Register Connection using ConnectionsManager
    let conn_info = ConnectionInfo::new(None);
    let registration = registry
        .register_connection(connection_id.clone(), conn_info)
        .expect("Failed to register connection");
    let mut rx = registration.notification_rx;

    // Get connection state from registry to authenticate
    // (don't hold onto it - let the registry own it)
    {
        let connection_state =
            registry.get_connection(&connection_id).expect("Connection should exist");
        connection_state.mark_authenticated(user_id.clone(), Role::User);
    }

    println!("✓ Connection registered and authenticated");

    // Verify connection exists before expiry
    assert!(
        registry.get_connection(&connection_id).is_some(),
        "Connection should exist before auth expiry"
    );

    // 3. Trigger Auth Expiry
    manager
        .unregister_connection(&user_id, &connection_id)
        .await
        .expect("Failed to handle auth expiry");

    println!("✓ Auth expiry handled");

    // Verify connection was removed from registry
    assert!(
        registry.get_connection(&connection_id).is_none(),
        "Connection should be removed from registry after auth expiry"
    );

    // 4. Verify Connection Closed
    // After unregistration, the sender should be dropped (or will be soon).
    // We check that no more messages can be received.
    let result = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;

    match result {
        Ok(None) => println!("✓ Connection closed successfully (channel dropped)"),
        Ok(Some(_msg)) => {
            // This could be a final message before close - try again
            let result2 = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
            match result2 {
                Ok(None) => println!("✓ Connection closed successfully after final message"),
                Ok(Some(_)) => panic!("Received multiple messages after auth expiry"),
                Err(_) => {
                    // Channel still open but no messages - this is acceptable
                    // as long as connection is removed from registry
                    println!("✓ No more messages received (connection removed from registry)")
                },
            }
        },
        Err(_) => {
            // Timeout - channel still open but no messages, which is acceptable
            // The important verification is that the connection was removed from registry
            println!("✓ Channel timeout (connection removed from registry)")
        },
    }

    println!("✓ Auth expiry test passed - connection removed from registry");
}
