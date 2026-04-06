//! Cluster WebSocket follower tests
//!
//! Verifies followers can deliver live query updates from leader writes.

use crate::cluster_common::*;
use crate::common::*;
use kalam_client::{ChangeEvent, KalamLinkTimeouts, SubscriptionManager};
use serde_json::Value;
use std::time::Duration;

fn parse_cluster_nodes() -> (String, String) {
    let urls = cluster_urls();
    let response = execute_on_node_response(
        &urls[0],
        "SELECT node_id, api_addr, is_leader FROM system.cluster",
    )
    .expect("Failed to query system.cluster");

    let result = response.results.first().expect("Missing cluster result");
    let rows = result.rows.as_ref().expect("Missing cluster rows");

    let mut leader_url: Option<String> = None;
    let mut follower_url: Option<String> = None;

    for row in rows {
        if row.len() < 3 {
            continue;
        }

        let api_addr = extract_typed_value(&row[1]);
        let is_leader = extract_typed_value(&row[2]);

        let api_addr = match api_addr {
            Value::String(s) => s,
            other => other.to_string(),
        };

        let leader = matches!(is_leader, Value::Bool(true))
            || matches!(is_leader, Value::String(ref s) if s == "true");

        if leader {
            leader_url = Some(api_addr);
        } else if follower_url.is_none() {
            follower_url = Some(api_addr);
        }
    }

    let leader_url = leader_url.expect("Leader URL not found");
    let follower_url = follower_url.expect("Follower URL not found");

    (leader_url, follower_url)
}

fn create_ws_client(base_url: &str) -> KalamLinkClient {
    client_for_user_on_url_with_timeouts(
        base_url,
        default_username(),
        default_password(),
        KalamLinkTimeouts::builder()
            .connection_timeout_secs(5)
            .receive_timeout_secs(30)
            .send_timeout_secs(10)
            .subscribe_timeout_secs(20)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(30))
            .build(),
    )
    .expect("Failed to build cluster client")
}

async fn subscribe_with_retry(
    client: &KalamLinkClient,
    query: &str,
    max_attempts: usize,
) -> SubscriptionManager {
    let mut last_error: Option<String> = None;
    for attempt in 0..max_attempts {
        let mut subscription = client.subscribe(query).await.expect("Failed to subscribe");

        if let Ok(Some(Ok(event))) =
            tokio::time::timeout(Duration::from_secs(5), subscription.next()).await
        {
            if matches!(event, ChangeEvent::Error { .. }) {
                last_error = Some("subscription registration failed".to_string());
                tokio::time::sleep(Duration::from_millis(200 + (attempt as u64 * 150))).await;
                continue;
            }
        }

        return subscription;
    }

    panic!(
        "Subscription failed to register after {} attempts: {:?}",
        max_attempts, last_error
    );
}

fn response_error_message(response: &kalam_client::QueryResponse) -> String {
    if let Some(error) = &response.error {
        if let Some(details) = &error.details {
            return format!("{} ({})", error.message, details);
        }
        return error.message.clone();
    }
    format!("Query failed: {:?}", response)
}

async fn execute_query_with_retry(
    client: &KalamLinkClient,
    sql: &str,
    max_attempts: usize,
) -> Result<(), String> {
    let mut last_err: Option<String> = None;
    for attempt in 0..max_attempts {
        match client.execute_query(sql, None, None, None).await {
            Ok(response) => {
                if response.success() {
                    return Ok(());
                }
                let err_msg = response_error_message(&response);
                if is_retryable_cluster_error_for_sql(sql, &err_msg) {
                    last_err = Some(err_msg);
                } else {
                    return Err(err_msg);
                }
            },
            Err(e) => {
                let err_msg = e.to_string();
                if is_retryable_cluster_error_for_sql(sql, &err_msg) {
                    last_err = Some(err_msg);
                } else {
                    return Err(err_msg);
                }
            },
        }

        tokio::time::sleep(Duration::from_millis(300 + (attempt as u64 * 200))).await;
    }

    Err(last_err.unwrap_or_else(|| "all retries failed".to_string()))
}

#[test]
fn cluster_test_ws_follower_receives_leader_changes() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: WebSocket Follower Receives Leader Changes ===\n");

    let (leader_url, follower_url) = parse_cluster_nodes();
    println!("Leader: {}", leader_url);
    println!("Follower: {}", follower_url);

    let namespace = generate_unique_namespace("cluster_ws");
    let table = "ws_follow";
    let full = format!("{}.{}", namespace, table);

    let _ =
        execute_on_node(&leader_url, &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&leader_url, &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");
    // Use USER TABLE - subscriptions only work on USER/STREAM tables
    execute_on_node(
        &leader_url,
        &format!("CREATE USER TABLE {} (id BIGINT PRIMARY KEY, value TEXT)", full),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes including follower
    if !wait_for_table_on_all_nodes(&namespace, table, 10000) {
        panic!("Table {} did not replicate to all nodes within timeout", full);
    }
    println!("  ✓ Table replicated to all nodes");

    let query = format!("SELECT * FROM {}", full);
    let insert_value = "follower_event";
    let insert_sql = format!("INSERT INTO {} (id, value) VALUES (1, '{}')", full, insert_value);

    cluster_runtime().block_on(async {
        // Subscribe on FOLLOWER to test follower subscriptions work
        let follower_client = create_ws_client(&follower_url);
        let leader_client = create_ws_client(&leader_url);

        let mut subscription = subscribe_with_retry(&follower_client, &query, 3).await;

        execute_query_with_retry(&leader_client, &insert_sql, 5)
            .await
            .expect("Failed to insert on leader");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut received = false;

        while tokio::time::Instant::now() < deadline {
            let remaining =
                deadline.checked_duration_since(tokio::time::Instant::now()).unwrap_or_default();
            let wait = std::cmp::min(remaining, Duration::from_secs(2));

            match tokio::time::timeout(wait, subscription.next()).await {
                Ok(Some(Ok(event))) => match event {
                    ChangeEvent::Insert { rows, .. } => {
                        for row in rows {
                            if let Some(val) = row.get("value").and_then(|v| v.inner().as_str()) {
                                if val == insert_value {
                                    received = true;
                                    break;
                                }
                            }
                        }
                    },
                    ChangeEvent::Error { code, message, .. } => {
                        panic!("Subscription error: {} - {}", code, message);
                    },
                    _ => {},
                },
                Ok(Some(Err(err))) => panic!("Subscription error: {}", err),
                Ok(None) => break,
                Err(_) => {},
            }

            if received {
                break;
            }
        }

        assert!(received, "Follower did not receive leader insert event via WebSocket");
    });

    let _ =
        execute_on_node(&leader_url, &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    println!("\n  ✅ WebSocket follower delivery test passed\n");
}
