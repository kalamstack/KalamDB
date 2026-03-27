#![allow(unused_imports)]

use kalamdb_commons::models::{ConnectionId, ConnectionInfo, UserId};
use kalamdb_commons::websocket::{SubscriptionOptions, SubscriptionRequest};

// #[tokio::test(flavor = "multi_thread")]
// #[ignore = "Requires system schemas registered in live query session context"]
// async fn test_multi_subscription_lifecycle() {
//     // 1. Setup
//     let app_ctx = test_app_context();
//     let manager = app_ctx.live_query_manager();
//     let registry = manager.registry();

//     // 2. Register Connection
//     let user_id = UserId::new("root");
//     let conn_id = ConnectionId::new("conn_multi");

//     println!("Registering connection...");
//     let registration = registry
//         .register_connection(conn_id.clone(), ConnectionInfo::new(None))
//         .expect("Failed to register connection");
//     let connection_state = registration.state;
//     connection_state.write().mark_authenticated(user_id.clone());
//     registry.on_authenticated(&conn_id, user_id.clone());
//     println!("Registered connection");

//     // 3. Register Subscription 1
//     let sub_id1 = "sub1";
//     println!("Registering sub1...");
//     let subscription1 = SubscriptionRequest {
//         id: sub_id1.to_string(),
//         sql: "SELECT table_name FROM information_schema.tables LIMIT 1".to_string(),
//         options: SubscriptionOptions::default(),
//     };
//     let result1 = manager
//         .register_subscription_with_initial_data(&connection_state, &subscription1, None)
//         .await
//         .expect("Failed to register sub1");
//     let live_id1 = result1.live_id;
//     println!("Registered sub1: {}", live_id1);

//     // 4. Register Subscription 2
//     let sub_id2 = "sub2";
//     println!("Registering sub2...");
//     let subscription2 = SubscriptionRequest {
//         id: sub_id2.to_string(),
//         sql: "SELECT column_name FROM information_schema.columns LIMIT 1".to_string(),
//         options: SubscriptionOptions::default(),
//     };
//     let result2 = manager
//         .register_subscription_with_initial_data(&connection_state, &subscription2, None)
//         .await
//         .expect("Failed to register sub2");
//     let live_id2 = result2.live_id;
//     println!("Registered sub2: {}", live_id2);

//     // 5. Verify both exist
//     let subs = manager.get_user_subscriptions(&user_id).await.expect("Failed to get subs");
//     assert_eq!(subs.len(), 2, "Expected 2 subscriptions, got {}", subs.len());
//     assert!(subs.iter().any(|s| s.subscription_id == sub_id1), "sub1 not found");
//     assert!(subs.iter().any(|s| s.subscription_id == sub_id2), "sub2 not found");

//     // 6. Unsubscribe Subscription 1
//     println!("Unregistering sub1...");
//     let subscription_id1 = live_id1.subscription_id().to_string();
//     manager
//         .unregister_subscription(&connection_state, &subscription_id1, &live_id1)
//         .await
//         .expect("Failed to unregister sub1");
//     println!("Unregistered sub1");

//     // 7. Verify sub1 removed, sub2 remains
//     let subs_after = manager.get_user_subscriptions(&user_id).await.expect("Failed to get subs");
//     assert_eq!(
//         subs_after.len(),
//         1,
//         "Expected 1 subscription after unregister, got {}",
//         subs_after.len()
//     );
//     assert!(subs_after.iter().any(|s| s.subscription_id == sub_id2), "sub2 should remain");
//     assert!(
//         !subs_after.iter().any(|s| s.subscription_id == sub_id1),
//         "sub1 should be removed"
//     );

//     // 8. Unregister Connection
//     println!("Unregistering connection...");
//     manager
//         .unregister_connection(&user_id, &conn_id)
//         .await
//         .expect("Failed to unregister connection");
//     println!("Unregistered connection");

//     // 9. Verify all removed
//     let subs_final = manager.get_user_subscriptions(&user_id).await.expect("Failed to get subs");
//     assert_eq!(
//         subs_final.len(),
//         0,
//         "Expected 0 subscriptions after connection unregister, got {}",
//         subs_final.len()
//     );

//     println!("Test completed successfully!");
// }
