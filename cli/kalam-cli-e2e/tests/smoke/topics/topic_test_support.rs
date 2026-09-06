use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use kalam_client::{
    consumer::{AutoOffsetReset, TopicConsumer},
    KalamLinkClient, KalamLinkTimeouts,
};

use crate::common;

pub fn default_topic_timeouts() -> KalamLinkTimeouts {
    KalamLinkTimeouts::builder()
        .connection_timeout_secs(10)
        .receive_timeout_secs(15)
        .send_timeout_secs(30)
        .subscribe_timeout_secs(15)
        .auth_timeout_secs(10)
        .initial_data_timeout(Duration::from_secs(60))
        .build()
}

pub fn long_topic_timeouts() -> KalamLinkTimeouts {
    KalamLinkTimeouts::builder()
        .connection_timeout_secs(5)
        .receive_timeout_secs(120)
        .send_timeout_secs(30)
        .subscribe_timeout_secs(10)
        .auth_timeout_secs(10)
        .initial_data_timeout(Duration::from_secs(120))
        .build()
}

pub async fn create_test_client_with_timeouts(timeouts: KalamLinkTimeouts) -> KalamLinkClient {
    let base_url = common::leader_or_server_url();
    common::client_for_user_on_url_with_timeouts(
        &base_url,
        common::default_username(),
        common::default_password(),
        timeouts,
    )
    .expect("failed to build topic test client")
}

pub async fn create_test_client() -> KalamLinkClient {
    create_test_client_with_timeouts(default_topic_timeouts()).await
}

pub async fn execute_sql(sql: &str) -> Result<(), String> {
    let max_attempts = 8;
    let mut attempt = 0usize;

    loop {
        match execute_sql_via_pooled_client(sql).await {
            Ok(response) => {
                let status = response.get("status").and_then(|s| s.as_str()).unwrap_or("");
                if status.eq_ignore_ascii_case("success") {
                    return Ok(());
                }

                let err_msg = response
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error");

                if is_retryable_sql_helper_error(err_msg) && attempt + 1 < max_attempts {
                    attempt += 1;
                    tokio::time::sleep(retry_delay_for_attempt(attempt)).await;
                    continue;
                }

                return Err(format!("SQL failed: {}", err_msg));
            },
            Err(message) => {
                if is_retryable_sql_helper_error(&message) && attempt + 1 < max_attempts {
                    attempt += 1;
                    tokio::time::sleep(retry_delay_for_attempt(attempt)).await;
                    continue;
                }
                return Err(message);
            },
        }
    }
}

async fn execute_sql_via_pooled_client(sql: &str) -> Result<serde_json::Value, String> {
    let sql = sql.to_string();
    let response = tokio::task::spawn_blocking(move || {
        common::execute_sql_as_root_via_client_json(&sql).map_err(|err| err.to_string())
    })
    .await
    .map_err(|err| format!("pooled SQL helper task failed: {}", err))??;

    serde_json::from_str(&response)
        .map_err(|err| format!("failed to parse pooled SQL response: {}", err))
}

fn retry_delay_for_attempt(attempt: usize) -> Duration {
    let millis = (attempt as u64 * 100).min(750);
    Duration::from_millis(millis)
}

fn is_retryable_sql_helper_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    is_retryable_consumer_poll_error(&normalized)
        || normalized.contains("error sending request")
        || normalized.contains("connection reset")
        || normalized.contains("connection refused")
        || normalized.contains("connection aborted")
        || normalized.contains("broken pipe")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
        || normalized.contains("too many")
        || normalized.contains("429")
        || normalized.contains("503")
        || normalized.contains("504")
        || normalized.contains("service unavailable")
        || normalized.contains("gateway timeout")
}

pub async fn wait_for_topic_ready(topic: &str, expected_routes: usize) {
    let sql = format!("SELECT routes FROM system.topics WHERE topic_id = '{}'", topic);
    let deadline = Instant::now() + Duration::from_secs(30);

    while Instant::now() < deadline {
        if let Ok(response) = common::execute_sql_via_http_as_root(&sql).await {
            if let Some(rows) = common::get_rows_as_hashmaps(&response) {
                if let Some(row) = rows.first() {
                    if let Some(routes_value) = row.get("routes") {
                        let routes_untyped = common::extract_typed_value(routes_value);
                        if let Some(routes_json) = routes_untyped
                            .as_str()
                            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                        {
                            let route_count =
                                routes_json.as_array().map(|routes| routes.len()).unwrap_or(0);
                            if route_count >= expected_routes {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                return;
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "Timed out waiting for topic '{}' to have at least {} route(s)",
        topic, expected_routes
    );
}

pub fn is_retryable_consumer_poll_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("error decoding")
        || normalized.contains("network")
        || normalized.contains("invalid_credentials")
        || normalized.contains("invalid credentials")
        || normalized.contains("invalid token")
        || normalized.contains("token expired")
        || normalized.contains("unauthorized")
        || normalized.contains("401")
}

async fn commit_processed_batch(
    consumer: &mut TopicConsumer,
    idle_sleep: Duration,
    deadline: Instant,
) {
    loop {
        match consumer.commit_sync().await {
            Ok(_) => return,
            Err(err) => {
                let message = err.to_string();
                if is_retryable_consumer_poll_error(&message) && Instant::now() < deadline {
                    tokio::time::sleep(idle_sleep).await;
                    continue;
                }
                panic!("topic consumer commit error: {}", message);
            },
        }
    }
}

pub async fn build_test_consumer(
    topic: &str,
    group_id: &str,
    max_poll_records: u32,
    enable_auto_commit: bool,
) -> TopicConsumer {
    let client = create_test_client().await;
    client
        .consumer()
        .topic(topic)
        .group_id(group_id)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(enable_auto_commit)
        .max_poll_records(max_poll_records)
        .build()
        .expect("failed to build topic test consumer")
}

pub struct UniqueOffsetPollConfig {
    pub expected_messages: Option<usize>,
    pub publishers_done:   Option<Arc<AtomicBool>>,
    pub deadline:          Duration,
    pub idle_break_after:  u32,
    pub idle_sleep:        Duration,
    pub per_record_delay:  Duration,
    pub commit_each_batch: bool,
}

pub async fn poll_unique_offsets_until(
    consumer: &mut TopicConsumer,
    config: UniqueOffsetPollConfig,
) -> HashSet<(u32, u64)> {
    let mut seen = HashSet::<(u32, u64)>::new();
    let deadline = Instant::now() + config.deadline;
    let mut idle_loops = 0u32;

    while Instant::now() < deadline
        && config.expected_messages.map(|expected| seen.len() < expected).unwrap_or(true)
    {
        match consumer.poll().await {
            Ok(batch) if batch.is_empty() => {
                idle_loops += 1;
                if config
                    .publishers_done
                    .as_ref()
                    .map(|done| done.load(Ordering::Relaxed))
                    .unwrap_or(false)
                    && idle_loops >= config.idle_break_after
                {
                    break;
                }
                tokio::time::sleep(config.idle_sleep).await;
            },
            Ok(batch) => {
                idle_loops = 0;
                for record in &batch {
                    if !config.per_record_delay.is_zero() {
                        tokio::time::sleep(config.per_record_delay).await;
                    }
                    seen.insert((record.partition_id, record.offset));
                    consumer.mark_processed(record);
                }

                if config.commit_each_batch {
                    commit_processed_batch(consumer, config.idle_sleep, deadline).await;
                }
            },
            Err(err) => {
                let message = err.to_string();
                if is_retryable_consumer_poll_error(&message) {
                    tokio::time::sleep(config.idle_sleep).await;
                    continue;
                }
                panic!("topic consumer poll error: {}", message);
            },
        }
    }

    seen
}

pub async fn publish_numbered_rows(
    table: &str,
    value_column: &str,
    value_prefix: &str,
    expected_messages: usize,
    publisher_parallelism: usize,
) {
    let mut publish_handles = Vec::with_capacity(publisher_parallelism);

    for publisher in 0..publisher_parallelism {
        let base_count = expected_messages / publisher_parallelism;
        let extra = usize::from(publisher < expected_messages % publisher_parallelism);
        let count = base_count + extra;
        let start_id =
            publisher * base_count + publisher.min(expected_messages % publisher_parallelism);
        let table = table.to_string();
        let value_column = value_column.to_string();
        let value_prefix = value_prefix.to_string();
        publish_handles.push(tokio::spawn(async move {
            for idx in 0..count {
                let id = (start_id + idx) as i64;
                execute_sql(&format!(
                    "INSERT INTO {} (id, {}) VALUES ({}, '{}_{}')",
                    table, value_column, id, value_prefix, id
                ))
                .await
                .expect("topic test insert failed");
            }
        }));
    }

    for handle in publish_handles {
        handle.await.expect("publisher task failed");
    }
}
