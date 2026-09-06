// Smoke test for opening many live subscriptions concurrently and verifying insert fanout.

use std::time::{Duration, Instant};

use futures_util::future::join_all;
use kalam_client::{
    ChangeEvent, KalamLinkClient, KalamLinkTimeouts, SubscriptionConfig, SubscriptionManager,
};

use crate::common::*;

const DEFAULT_CONCURRENT_SUBSCRIPTIONS: usize = 50;
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(20);
const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(15);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const LIVE_QUERY_COUNT_TIMEOUT: Duration = Duration::from_secs(20);
const LIVE_QUERY_COUNT_QUERY_LIMIT: usize = 1000;

#[ntest::timeout(120000)]
#[test]
fn smoke_test_concurrent_subscription_fanout() {
    if !require_server_running() {
        return;
    }

    let subscription_count = std::env::var("KALAMDB_SMOKE_CONCURRENT_SUBSCRIPTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONCURRENT_SUBSCRIPTIONS);

    let namespace = generate_unique_namespace("smoke_concurrent_subs");
    let table = generate_unique_table("fanout");
    let full_table_name = format!("{}.{}", namespace, table);
    let subscription_prefix = format!("smoke_concurrent_{}_", namespace);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT NOT NULL) WITH (TYPE='USER')",
        full_table_name
    ))
    .expect("create table");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");

    let namespace_for_cleanup = namespace.clone();
    let full_table_name_for_cleanup = full_table_name.clone();

    runtime.block_on(async move {
        let client = client_for_user_on_url_with_timeouts(
            &leader_or_server_url(),
            default_username(),
            default_password(),
            KalamLinkTimeouts::builder()
                .connection_timeout_secs(10)
                .receive_timeout_secs(45)
                .send_timeout_secs(10)
                .subscribe_timeout_secs(12)
                .auth_timeout_secs(10)
                .initial_data_timeout(Duration::from_secs(20))
                .build(),
        )
        .expect("build client");
        client.connect().await.expect("connect");

        let query = format!("SELECT * FROM {}", full_table_name);
        let insert_value = format!("fanout_value_{}", namespace);
        let mut subscriptions = Vec::with_capacity(subscription_count);

        let result: Result<(Duration, Duration), String> = async {
            let open_started = Instant::now();
            let open_results = join_all((0..subscription_count).map(|index| {
                let client = client.clone();
                let query = query.clone();
                let subscription_id = format!("{}{}", subscription_prefix, index);
                async move { open_subscription(client, subscription_id, query).await }
            }))
            .await;

            let mut open_errors = Vec::new();
            for result in open_results {
                match result {
                    Ok(subscription) => subscriptions.push(subscription),
                    Err(error) => open_errors.push(error),
                }
            }

            if !open_errors.is_empty() {
                return Err(format!(
                    "failed to open {} concurrent subscriptions: {}",
                    subscription_count,
                    sample_errors(&open_errors)
                ));
            }

            let open_elapsed = open_started.elapsed();
            let registered = wait_for_live_query_count(
                &subscription_prefix,
                subscription_count,
                LIVE_QUERY_COUNT_TIMEOUT,
            )
            .await;
            if registered != subscription_count {
                let client_registered = client
                    .subscriptions()
                    .await
                    .into_iter()
                    .filter(|subscription| {
                        !subscription.closed && subscription.id.starts_with(&subscription_prefix)
                    })
                    .count();
                return Err(format!(
                    "expected {} live subscriptions in system.live, found {}; client registry has \
                     {}",
                    subscription_count, registered, client_registered
                ));
            }

            execute_sql_as_root_via_client(&format!(
                "INSERT INTO {} (id, value) VALUES (1, '{}')",
                full_table_name, insert_value
            ))
            .map_err(|error| format!("trigger insert failed: {}", error))?;

            let delivery_started = Instant::now();
            let delivery_results =
                join_all(subscriptions.iter_mut().enumerate().map(|(index, subscription)| {
                    let expected_value = insert_value.clone();
                    async move {
                        wait_for_insert_value(index, subscription, expected_value.as_str()).await
                    }
                }))
                .await;

            let delivery_errors: Vec<String> =
                delivery_results.into_iter().filter_map(Result::err).collect();
            if !delivery_errors.is_empty() {
                return Err(format!(
                    "failed to fan out insert to {} subscriptions: {}",
                    subscription_count,
                    sample_errors(&delivery_errors)
                ));
            }

            Ok((open_elapsed, delivery_started.elapsed()))
        }
        .await;

        let close_errors = close_all_subscriptions(&mut subscriptions).await;
        drop(subscriptions);
        client.disconnect().await;
        let remaining_live_queries =
            wait_for_live_query_count(&subscription_prefix, 0, LIVE_QUERY_COUNT_TIMEOUT).await;

        let (open_elapsed, delivery_elapsed) = match result {
            Ok(metrics) => metrics,
            Err(error) => panic!("{}", error),
        };

        assert!(
            close_errors.is_empty(),
            "failed to close subscriptions cleanly: {}",
            sample_errors(&close_errors)
        );
        assert_eq!(
            remaining_live_queries, 0,
            "expected all concurrent subscriptions to be removed from system.live"
        );

        println!(
            "\n=== Concurrent Subscription Smoke Test ===\nSubscriptions: {}\nOpen time: \
             {:?}\nDelivery time: {:?}\n",
            subscription_count, open_elapsed, delivery_elapsed
        );
    });

    let _ = execute_sql_as_root_via_client(&format!(
        "DROP TABLE IF EXISTS {}",
        full_table_name_for_cleanup
    ));
    let _ = execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_for_cleanup
    ));
}

async fn open_subscription(
    client: KalamLinkClient,
    subscription_id: String,
    query: String,
) -> Result<SubscriptionManager, String> {
    let mut subscription = tokio::time::timeout(
        SUBSCRIBE_TIMEOUT,
        client.live_events_with_config(SubscriptionConfig::without_initial_data(
            subscription_id,
            &query,
        )),
    )
    .await
    .map_err(|_| format!("subscribe timed out after {:?}", SUBSCRIBE_TIMEOUT))?
    .map_err(|error| format!("subscribe failed: {}", error))?;

    wait_for_ack(&mut subscription).await?;
    Ok(subscription)
}

async fn wait_for_ack(subscription: &mut SubscriptionManager) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + ACK_TIMEOUT;

    while tokio::time::Instant::now() < deadline {
        let remaining =
            deadline.checked_duration_since(tokio::time::Instant::now()).unwrap_or_default();

        match tokio::time::timeout(remaining, subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Ack { .. }))) => return Ok(()),
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(error))) => return Err(format!("ack failed: {}", error)),
            Ok(None) => return Err("subscription closed before ack".to_string()),
            Err(_) => break,
        }
    }

    Err(format!("timed out waiting for ack after {:?}", ACK_TIMEOUT))
}

async fn wait_for_insert_value(
    index: usize,
    subscription: &mut SubscriptionManager,
    expected_value: &str,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + DELIVERY_TIMEOUT;

    while tokio::time::Instant::now() < deadline {
        let remaining =
            deadline.checked_duration_since(tokio::time::Instant::now()).unwrap_or_default();

        match tokio::time::timeout(remaining, subscription.next()).await {
            Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                for row in rows {
                    if let Some(value) = row.get("value").and_then(|value| value.inner().as_str()) {
                        if value == expected_value {
                            return Ok(());
                        }
                    }
                }
            },
            Ok(Some(Ok(ChangeEvent::Error { code, message, .. }))) => {
                return Err(format!("subscription {} returned error {}: {}", index, code, message));
            },
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(error))) => {
                return Err(format!("subscription {} failed: {}", index, error));
            },
            Ok(None) => {
                return Err(format!("subscription {} closed before insert arrived", index));
            },
            Err(_) => break,
        }
    }

    Err(format!(
        "subscription {} did not receive insert value '{}' within {:?}",
        index, expected_value, DELIVERY_TIMEOUT
    ))
}

async fn close_all_subscriptions(subscriptions: &mut [SubscriptionManager]) -> Vec<String> {
    join_all(subscriptions.iter_mut().enumerate().map(|(index, subscription)| async move {
        tokio::time::timeout(CLOSE_TIMEOUT, subscription.close())
            .await
            .map_err(|_| format!("subscription {} close timed out", index))?
            .map_err(|error| format!("subscription {} close failed: {}", index, error))
    }))
    .await
    .into_iter()
    .filter_map(Result::err)
    .collect()
}

async fn wait_for_live_query_count(prefix: &str, expected: usize, timeout: Duration) -> usize {
    let start = tokio::time::Instant::now();

    loop {
        let current = count_live_query_rows(prefix).await;
        if current == expected || start.elapsed() >= timeout {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

async fn count_live_query_rows(prefix: &str) -> usize {
    let prefix = prefix.to_string();
    let sql =
        format!("SELECT subscription_id FROM system.live LIMIT {}", LIVE_QUERY_COUNT_QUERY_LIMIT);

    tokio::task::spawn_blocking(move || {
        execute_sql_as_root_via_client_json(&sql).map_err(|error| format!("{}", error))
    })
    .await
    .expect("spawn_blocking join failure")
    .map(|json_str| {
        let value: serde_json::Value = serde_json::from_str(&json_str)
            .unwrap_or_else(|error| panic!("Failed to parse system.live JSON: {}", error));
        let rows = get_rows_as_hashmaps(&value).unwrap_or_default();
        rows.iter()
            .filter(|row| {
                let id_value = row
                    .get("subscription_id")
                    .map(extract_typed_value)
                    .unwrap_or(serde_json::Value::Null);
                id_value.as_str().map(|id| id.starts_with(&prefix)).unwrap_or(false)
            })
            .count()
    })
    .expect("system.live JSON query should succeed")
}

fn sample_errors(errors: &[String]) -> String {
    let sample_count = errors.len().min(5);
    let sample = errors.iter().take(sample_count).cloned().collect::<Vec<_>>().join(" | ");

    if errors.len() > sample_count {
        format!("{} total errors; first {}: {}", errors.len(), sample_count, sample)
    } else {
        sample
    }
}
