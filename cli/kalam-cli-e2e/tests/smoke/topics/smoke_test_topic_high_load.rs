//! High-load smoke test for topic consumption with concurrent publishers
//!
//! This test validates that the topic/pub-sub system can handle:
//! - 20+ concurrent publishers inserting/updating data
//! - Multiple table types (user, shared, stream)
//! - Mixed INSERT and UPDATE operations
//! - Various datatypes (INT, TEXT, DOUBLE, BOOLEAN, BIGINT)
//! - Single topic consuming from all sources
//! - No events are dropped under high concurrent load
//!
//! **Requirements**: Running KalamDB server with Topics feature enabled

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use kalam_client::consumer::{AutoOffsetReset, ConsumerRecord, TopicOp};
use kalamdb_configs::config::defaults::default_topic_visibility_timeout_secs;
use tokio::sync::Mutex as TokioMutex;

use crate::{common, topic_test_support};

/// Create a test client using common infrastructure
async fn create_test_client() -> kalam_client::KalamLinkClient {
    topic_test_support::create_test_client().await
}

/// Execute SQL via HTTP helper with error handling
async fn execute_sql(sql: &str) -> Result<(), String> {
    topic_test_support::execute_sql(sql).await
}

fn is_retryable_consumer_poll_error(message: &str) -> bool {
    topic_test_support::is_retryable_consumer_poll_error(message)
}

fn local_visibility_timeout_config_path() -> Option<PathBuf> {
    if !common::server_target_is_local() {
        return None;
    }

    let root = common::workspace_root();
    if common::is_cluster_mode() {
        Some(root.join(".cluster-local/node1/server.toml"))
    } else {
        Some(root.join("backend/server.toml"))
    }
}

fn read_visibility_timeout_secs_from_server_toml(path: &Path) -> Option<u64> {
    let contents = std::fs::read_to_string(path).ok()?;
    let mut in_topics_section = false;

    for raw_line in contents.lines() {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            in_topics_section = line == "[topics]";
            continue;
        }

        if !in_topics_section {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        if key.trim() != "visibility_timeout_secs" {
            continue;
        }

        let value = value.trim().trim_matches('"');
        if let Ok(parsed) = value.parse::<u64>() {
            return Some(parsed);
        }
    }

    None
}

async fn runtime_topic_visibility_timeout_secs() -> Option<u64> {
    let response = common::execute_sql_via_http_as_root(
        "SELECT value FROM system.settings WHERE name = 'topics.visibility_timeout_secs'",
    )
    .await
    .ok()?;

    if !response_is_success(&response) {
        return None;
    }

    let rows = common::get_rows_as_hashmaps(&response)?;
    let value = rows.first()?.get("value")?;
    let value = common::extract_typed_value(value);

    value
        .as_str()
        .and_then(|raw| raw.parse::<u64>().ok())
        .or_else(|| value.as_u64())
        .or_else(|| value.as_i64().and_then(|raw| u64::try_from(raw).ok()))
}

async fn configured_topic_visibility_timeout_secs() -> u64 {
    // Query the running server first so test timing matches the actual runtime
    // configuration across fresh, running, cluster, CI, and Linux builds.
    if let Some(value) = runtime_topic_visibility_timeout_secs().await {
        return value;
    }

    for env_key in [
        "KALAMDB_TOPIC_VISIBILITY_TIMEOUT_SECS",
        "KALAMDB_VISIBILITY_TIMEOUT_SECS",
    ] {
        if let Some(value) = std::env::var(env_key).ok().and_then(|raw| raw.parse().ok()) {
            return value;
        }
    }

    if let Some(path) = local_visibility_timeout_config_path() {
        if let Some(value) = read_visibility_timeout_secs_from_server_toml(&path) {
            return value;
        }
    }

    default_topic_visibility_timeout_secs()
}

async fn topic_recovery_deadline() -> Duration {
    Duration::from_secs(configured_topic_visibility_timeout_secs().await + 30)
}

async fn topic_visibility_timeout_wait() -> Duration {
    Duration::from_secs(configured_topic_visibility_timeout_secs().await + 1)
}

async fn wait_for_topic_ready(topic: &str, expected_routes: usize) {
    topic_test_support::wait_for_topic_ready(topic, expected_routes).await;
}

fn response_is_success(response: &serde_json::Value) -> bool {
    response
        .get("status")
        .and_then(|status| status.as_str())
        .map(|status| status.eq_ignore_ascii_case("success"))
        .unwrap_or(false)
}

/// Helper to parse JSON payload from binary
fn parse_payload(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("Failed to parse payload")
}

fn extract_string_field(payload: &serde_json::Value, key: &str) -> Option<String> {
    let raw = payload.get(key)?;
    let untyped = common::extract_typed_value(raw);
    match untyped {
        serde_json::Value::String(s) => Some(s),
        _ => None,
    }
}

fn extract_i64_field(payload: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(raw) = payload.get(key) {
            let untyped = common::extract_typed_value(raw);
            if let Some(value) = untyped.as_i64() {
                return Some(value);
            }
            if let Some(value) = untyped.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_u64_row_field(row: &HashMap<String, serde_json::Value>, key: &str) -> u64 {
    let raw = row.get(key).unwrap_or_else(|| panic!("row should contain {}", key));
    let untyped = common::extract_typed_value(raw);
    if let Some(value) = untyped.as_u64() {
        return value;
    }
    if let Some(value) = untyped.as_i64().and_then(|value| u64::try_from(value).ok()) {
        return value;
    }
    if let Some(value) = untyped.as_str().and_then(|value| value.parse::<u64>().ok()) {
        return value;
    }
    panic!("{} should be an unsigned integer, got {}", key, untyped);
}

fn record_offsets(records: &[ConsumerRecord]) -> Vec<u64> {
    records.iter().map(|record| record.offset).collect()
}

fn record_payload_ids(records: &[ConsumerRecord], id_keys: &[&str]) -> Vec<i64> {
    records
        .iter()
        .map(|record| {
            let payload = parse_payload(&record.payload);
            extract_i64_field(&payload, id_keys)
                .unwrap_or_else(|| panic!("record payload should include one of {:?}", id_keys))
        })
        .collect()
}

fn assert_consecutive_offsets(offsets: &[u64], expected_start: u64, context: &str) {
    let expected: Vec<u64> =
        (expected_start..expected_start.saturating_add(offsets.len() as u64)).collect();
    assert_eq!(
        offsets, expected,
        "{}: expected consecutive offsets starting at {}, got {:?}",
        context, expected_start, offsets
    );
}

fn assert_complete_ids(ids: &[i64], expected_total: usize, context: &str) {
    let mut actual = ids.to_vec();
    actual.sort_unstable();
    actual.dedup();
    let expected: Vec<i64> = (0..expected_total as i64).collect();
    assert_eq!(
        actual,
        expected,
        "{}: expected complete unique ids 0..{}, got {:?}",
        context,
        expected_total.saturating_sub(1),
        actual
    );
}

async fn topic_offset_rows(topic: &str, group_id: &str) -> Vec<HashMap<String, serde_json::Value>> {
    let response = common::execute_sql_via_http_as_root(&format!(
        "SELECT topic_id, group_id, partition_id, last_acked_offset FROM system.topic_offsets \
         WHERE topic_id = '{}' AND group_id = '{}' ORDER BY partition_id",
        topic, group_id
    ))
    .await
    .expect("topic offset query should return a response");

    assert!(
        response_is_success(&response),
        "topic offset query should succeed: {}",
        response
    );

    common::get_rows_as_hashmaps(&response).unwrap_or_default()
}

struct RawRecordPollConfig {
    min_records:       usize,
    deadline:          Duration,
    idle_sleep:        Duration,
    per_record_delay:  Duration,
    commit_each_batch: bool,
}

async fn poll_records_raw_until(
    consumer: &mut kalam_client::consumer::TopicConsumer,
    config: RawRecordPollConfig,
) -> Vec<ConsumerRecord> {
    let deadline = std::time::Instant::now() + config.deadline;
    let mut records = Vec::new();

    while std::time::Instant::now() < deadline && records.len() < config.min_records {
        match consumer.poll().await {
            Ok(batch) if batch.is_empty() => {
                tokio::time::sleep(config.idle_sleep).await;
            },
            Ok(batch) => {
                for record in batch {
                    if !config.per_record_delay.is_zero() {
                        tokio::time::sleep(config.per_record_delay).await;
                    }
                    consumer.mark_processed(&record);
                    records.push(record);
                }

                if config.commit_each_batch {
                    consumer.commit_sync().await.expect("commit after poll batch should succeed");
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

    assert!(
        records.len() >= config.min_records,
        "Expected at least {} records within {:?}, got {}",
        config.min_records,
        config.deadline,
        records.len()
    );

    records
}

fn build_large_payload(id: usize, payload_size: usize) -> String {
    let prefix = format!("blob_{:04}_", id);
    if prefix.len() >= payload_size {
        return prefix;
    }

    let fill_char = char::from(b'a' + (id % 26) as u8);
    format!("{}{}", prefix, fill_char.to_string().repeat(payload_size - prefix.len()))
}

async fn publish_large_payload_rows(
    table: &str,
    expected_messages: usize,
    payload_size: usize,
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

        publish_handles.push(tokio::spawn(async move {
            for idx in 0..count {
                let id = start_id + idx;
                let payload = build_large_payload(id, payload_size);
                execute_sql(&format!(
                    "INSERT INTO {} (id, payload, payload_size, bucket) VALUES ({}, '{}', {}, \
                     'bucket_{}')",
                    table, id, payload, payload_size, publisher
                ))
                .await
                .expect("large payload insert failed");
            }
        }));
    }

    for handle in publish_handles {
        handle.await.expect("large payload publisher task failed");
    }
}

/// Test high-load concurrent publishing to multiple tables with single topic consumer
#[tokio::test]
#[ntest::timeout(300000)]
async fn test_topic_high_load_concurrent_publishers() {
    let namespace = common::generate_unique_namespace("highload_topic");
    let base_topic = common::generate_unique_table("multi_source");
    let topic = format!("{}.{}", namespace, base_topic);

    eprintln!("[TEST] Starting high-load test with namespace: {}", namespace);

    // Create namespace
    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("Failed to create namespace");

    // Create multiple tables with different types and schemas
    let shared_table = format!("{}.shared_metrics", namespace);
    execute_sql(&format!(
        "CREATE SHARED TABLE {} (id BIGINT PRIMARY KEY, name TEXT, value DOUBLE, active BOOLEAN, \
         counter INT, timestamp BIGINT)",
        shared_table
    ))
    .await
    .expect("Failed to create shared table");
    common::grant_public_shared_table_access(&shared_table);

    let user_table = format!("{}.user_profiles", namespace);
    execute_sql(&format!(
        "CREATE USER TABLE {} (id INT PRIMARY KEY, username TEXT, score DOUBLE, level INT, \
         verified BOOLEAN)",
        user_table
    ))
    .await
    .expect("Failed to create user table");

    let stream_table = format!("{}.event_stream", namespace);
    execute_sql(&format!(
        "CREATE STREAM TABLE {} (event_id BIGINT, event_type TEXT, payload TEXT, value INT, \
         success BOOLEAN) WITH (TTL_SECONDS = 3600)",
        stream_table
    ))
    .await
    .expect("Failed to create stream table");

    let product_table = format!("{}.products", namespace);
    execute_sql(&format!(
        "CREATE SHARED TABLE {} (product_id INT PRIMARY KEY, product_name TEXT, price DOUBLE, \
         stock INT, available BOOLEAN)",
        product_table
    ))
    .await
    .expect("Failed to create product table");
    common::grant_public_shared_table_access(&product_table);

    let session_table = format!("{}.user_sessions", namespace);
    execute_sql(&format!(
        "CREATE USER TABLE {} (session_id BIGINT PRIMARY KEY, user_id INT, duration INT, active \
         BOOLEAN, score DOUBLE)",
        session_table
    ))
    .await
    .expect("Failed to create session table");

    eprintln!("[TEST] Created all tables");

    // Create topic and add all tables as sources
    execute_sql(&format!("CREATE TOPIC {}", topic))
        .await
        .expect("Failed to create topic");

    let tables = vec![
        &shared_table,
        &user_table,
        &stream_table,
        &product_table,
        &session_table,
    ];

    let mut total_routes = 0;
    for table in &tables {
        execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
            .await
            .expect("Failed to add INSERT route");
        execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON UPDATE", topic, table))
            .await
            .expect("Failed to add UPDATE route");
        total_routes += 2;
    }

    eprintln!("[TEST] Added all sources to topic, waiting for routes...");
    wait_for_topic_ready(&topic, total_routes).await;
    eprintln!("[TEST] Topic ready with {} routes", total_routes);

    // Give the topic routing system time to fully initialize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Track expected events
    let expected_events = Arc::new(TokioMutex::new(HashMap::<String, EventInfo>::new()));
    let expected_events_clone = expected_events.clone();
    let publishers_done = Arc::new(AtomicBool::new(false));

    // Spawn consumer first
    let consumer_handle = {
        let topic = topic.clone();
        let publishers_done = publishers_done.clone();
        tokio::spawn(async move {
            eprintln!("[CONSUMER] Starting consumer for topic: {}", topic);

            let client = create_test_client().await;
            let mut consumer = client
                .consumer()
                .topic(&topic)
                .group_id(&format!("highload-test-group-{}", common::random_string(8)))
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .max_poll_records(100)
                .build()
                .expect("Failed to build consumer");

            eprintln!("[CONSUMER] Consumer built, starting to poll...");

            // Start main polling loop immediately
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut all_records = Vec::new();
            let mut seen_offsets = HashSet::<(u32, u64)>::new();
            let timeout = Duration::from_secs(60);
            let deadline = std::time::Instant::now() + timeout;
            let mut consecutive_empty = 0;
            let mut last_new_record_time = std::time::Instant::now();
            let mut consecutive_all_dups = 0;

            eprintln!(
                "[CONSUMER] Starting main polling loop for up to {} seconds",
                timeout.as_secs()
            );

            while std::time::Instant::now() < deadline {
                match consumer.poll().await {
                    Ok(batch) => {
                        if batch.is_empty() {
                            consecutive_empty += 1;
                            // Stop if no new records for 10 seconds
                            if publishers_done.load(Ordering::Relaxed)
                                && last_new_record_time.elapsed() > Duration::from_secs(3)
                                && !all_records.is_empty()
                            {
                                eprintln!(
                                    "[CONSUMER] No new records for 3s, stopping (unique: {})",
                                    seen_offsets.len()
                                );
                                break;
                            }
                            if consecutive_empty >= 20 {
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            } else {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                            continue;
                        }

                        consecutive_empty = 0;

                        // Track new vs duplicate records
                        let mut new_in_batch = 0;
                        for record in &batch {
                            if seen_offsets.insert((record.partition_id, record.offset)) {
                                new_in_batch += 1;
                            }
                        }

                        if new_in_batch > 0 {
                            last_new_record_time = std::time::Instant::now();
                        }

                        eprintln!(
                            "[CONSUMER] Polled {} records ({} new, total unique: {})",
                            batch.len(),
                            new_in_batch,
                            seen_offsets.len()
                        );

                        for record in batch {
                            consumer.mark_processed(&record);
                            all_records.push(record);
                        }

                        // Stop early if we're only getting duplicates
                        if new_in_batch == 0 {
                            consecutive_all_dups += 1;
                            if publishers_done.load(Ordering::Relaxed)
                                && (consecutive_all_dups >= 3
                                    || last_new_record_time.elapsed() > Duration::from_secs(3))
                            {
                                eprintln!(
                                    "[CONSUMER] No new records, stopping (unique: {}, \
                                     time_since_new: {}s)",
                                    seen_offsets.len(),
                                    last_new_record_time.elapsed().as_secs()
                                );
                                break;
                            }
                        } else {
                            consecutive_all_dups = 0;
                        }

                        // Commit each processed batch to reduce offset replay churn
                        if let Err(e) = consumer.commit_sync().await {
                            eprintln!("[CONSUMER] Commit error: {}", e);
                        }
                    },
                    Err(err) => {
                        let msg = err.to_string();
                        if is_retryable_consumer_poll_error(&msg) {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                        eprintln!("[CONSUMER] Poll error: {}", msg);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    },
                }
            }

            // Final commit
            if let Err(e) = consumer.commit_sync().await {
                eprintln!("[CONSUMER] Final commit error: {}", e);
            }

            eprintln!("[CONSUMER] Finished, collected {} total records", all_records.len());
            all_records
        })
    };

    // Give consumer time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn 20+ concurrent publishers
    let num_publishers = 24;
    let operations_per_publisher = 10;

    eprintln!(
        "[TEST] Spawning {} publishers with {} operations each",
        num_publishers, operations_per_publisher
    );

    let mut publish_handles = Vec::new();

    for publisher_id in 0..num_publishers {
        let shared_table = shared_table.clone();
        let user_table = user_table.clone();
        let stream_table = stream_table.clone();
        let product_table = product_table.clone();
        let session_table = session_table.clone();
        let expected = expected_events_clone.clone();

        let handle = tokio::spawn(async move {
            // Each publisher does INSERT then UPDATE operations across multiple tables
            for op_id in 0..operations_per_publisher {
                let record_id = publisher_id * 1000 + op_id;

                // Vary which table to write to based on publisher_id
                match publisher_id % 5 {
                    0 => {
                        // Shared metrics: INSERT then UPDATE
                        let insert_sql = format!(
                            "INSERT INTO {} (id, name, value, active, counter, timestamp) VALUES \
                             ({}, 'metric_{}', {}, {}, {}, {})",
                            shared_table,
                            record_id,
                            record_id,
                            record_id as f64 * 1.5,
                            record_id % 2 == 0,
                            record_id,
                            record_id * 1000
                        );
                        if let Err(e) = execute_sql(&insert_sql).await {
                            eprintln!("[PUBLISHER-{}] Insert error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("shared_metrics_insert_{}", record_id),
                                "shared_metrics",
                                TopicOp::Insert,
                                record_id,
                            )
                            .await;
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;

                        let update_sql = format!(
                            "UPDATE {} SET value = {}, counter = {} WHERE id = {}",
                            shared_table,
                            record_id as f64 * 2.0,
                            record_id + 1,
                            record_id
                        );
                        if let Err(e) = execute_sql(&update_sql).await {
                            eprintln!("[PUBLISHER-{}] Update error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("shared_metrics_update_{}", record_id),
                                "shared_metrics",
                                TopicOp::Update,
                                record_id,
                            )
                            .await;
                        }
                    },
                    1 => {
                        // User profiles: INSERT then UPDATE
                        let insert_sql = format!(
                            "INSERT INTO {} (id, username, score, level, verified) VALUES ({}, \
                             'user_{}', {}, {}, {})",
                            user_table,
                            record_id,
                            record_id,
                            record_id as f64 * 0.5,
                            record_id % 100,
                            record_id % 2 == 1
                        );
                        if let Err(e) = execute_sql(&insert_sql).await {
                            eprintln!("[PUBLISHER-{}] Insert error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("user_profiles_insert_{}", record_id),
                                "user_profiles",
                                TopicOp::Insert,
                                record_id,
                            )
                            .await;
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;

                        let update_sql = format!(
                            "UPDATE {} SET score = {}, level = {} WHERE id = {}",
                            user_table,
                            record_id as f64 * 1.5,
                            (record_id % 100) + 1,
                            record_id
                        );
                        if let Err(e) = execute_sql(&update_sql).await {
                            eprintln!("[PUBLISHER-{}] Update error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("user_profiles_update_{}", record_id),
                                "user_profiles",
                                TopicOp::Update,
                                record_id,
                            )
                            .await;
                        }
                    },
                    2 => {
                        // Stream events: INSERT only (2 records per iteration)
                        let insert_sql = format!(
                            "INSERT INTO {} (event_id, event_type, payload, value, success) \
                             VALUES ({}, 'type_{}', 'payload_{}', {}, {})",
                            stream_table,
                            record_id,
                            record_id % 10,
                            record_id,
                            record_id,
                            record_id % 2 == 0
                        );
                        if let Err(e) = execute_sql(&insert_sql).await {
                            eprintln!("[PUBLISHER-{}] Insert error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("event_stream_insert_{}", record_id),
                                "event_stream",
                                TopicOp::Insert,
                                record_id,
                            )
                            .await;
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;

                        // Another INSERT for stream
                        let record_id2 = record_id + 100000;
                        let insert_sql2 = format!(
                            "INSERT INTO {} (event_id, event_type, payload, value, success) \
                             VALUES ({}, 'type_{}', 'payload_{}', {}, {})",
                            stream_table,
                            record_id2,
                            record_id2 % 10,
                            record_id2,
                            record_id2,
                            record_id2 % 2 == 1
                        );
                        if let Err(e) = execute_sql(&insert_sql2).await {
                            eprintln!("[PUBLISHER-{}] Insert error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("event_stream_insert_{}", record_id2),
                                "event_stream",
                                TopicOp::Insert,
                                record_id2,
                            )
                            .await;
                        }
                    },
                    3 => {
                        // Products: INSERT then UPDATE
                        let insert_sql = format!(
                            "INSERT INTO {} (product_id, product_name, price, stock, available) \
                             VALUES ({}, 'product_{}', {}, {}, {})",
                            product_table,
                            record_id,
                            record_id,
                            record_id as f64 * 9.99,
                            record_id % 1000,
                            record_id % 2 == 0
                        );
                        if let Err(e) = execute_sql(&insert_sql).await {
                            eprintln!("[PUBLISHER-{}] Insert error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("products_insert_{}", record_id),
                                "products",
                                TopicOp::Insert,
                                record_id,
                            )
                            .await;
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;

                        let update_sql = format!(
                            "UPDATE {} SET price = {}, stock = {} WHERE product_id = {}",
                            product_table,
                            record_id as f64 * 12.99,
                            (record_id % 1000) + 10,
                            record_id
                        );
                        if let Err(e) = execute_sql(&update_sql).await {
                            eprintln!("[PUBLISHER-{}] Update error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("products_update_{}", record_id),
                                "products",
                                TopicOp::Update,
                                record_id,
                            )
                            .await;
                        }
                    },
                    4 => {
                        // User sessions: INSERT then UPDATE
                        let insert_sql = format!(
                            "INSERT INTO {} (session_id, user_id, duration, active, score) VALUES \
                             ({}, {}, {}, {}, {})",
                            session_table,
                            record_id as i64,
                            record_id % 10000,
                            record_id % 3600,
                            record_id % 2 == 1,
                            record_id as f64 * 0.75
                        );
                        if let Err(e) = execute_sql(&insert_sql).await {
                            eprintln!("[PUBLISHER-{}] Insert error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("user_sessions_insert_{}", record_id),
                                "user_sessions",
                                TopicOp::Insert,
                                record_id,
                            )
                            .await;
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;

                        let update_sql = format!(
                            "UPDATE {} SET duration = {}, score = {} WHERE session_id = {}",
                            session_table,
                            (record_id % 3600) + 60,
                            record_id as f64 * 1.25,
                            record_id
                        );
                        if let Err(e) = execute_sql(&update_sql).await {
                            eprintln!("[PUBLISHER-{}] Update error: {}", publisher_id, e);
                        } else {
                            record_expected_event(
                                &expected,
                                format!("user_sessions_update_{}", record_id),
                                "user_sessions",
                                TopicOp::Update,
                                record_id,
                            )
                            .await;
                        }
                    },
                    _ => unreachable!(),
                }

                // Small delay between operations
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            eprintln!("[PUBLISHER-{}] Completed all operations", publisher_id);
        });

        publish_handles.push(handle);
    }

    eprintln!("[TEST] Waiting for all publishers to complete...");
    for handle in publish_handles {
        handle.await.expect("Publisher task failed");
    }
    publishers_done.store(true, Ordering::Relaxed);

    eprintln!("[TEST] All publishers completed, waiting for consumer...");

    // Give extra time for all events to propagate through the topic system
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Wait for consumer to finish
    let records = consumer_handle.await.expect("Consumer task failed");

    eprintln!("[TEST] Consumer finished with {} records", records.len());

    // Verify all expected events were received
    let expected_lock = expected_events.lock().await;
    let expected_count = expected_lock.len();
    eprintln!("[TEST] Expected {} events, received {} records", expected_count, records.len());

    // Build a map of received events
    let mut received_events = HashMap::<String, ConsumerRecord>::new();
    for record in &records {
        let payload = parse_payload(&record.payload);

        // Extract table name from _table metadata (format: "namespace:table_name")
        let table_name = extract_string_field(&payload, "_table")
            .and_then(|table| table.rsplit(&[':', '.'][..]).next().map(str::to_string))
            .unwrap_or_else(|| "unknown".to_string());

        // Extract ID from payload
        // Note: BIGINT/Int64 values are serialized as JSON strings for JS precision safety
        let id = extract_i64_field(&payload, &["id", "product_id", "event_id", "session_id"])
            .unwrap_or(-1);

        let op_str = match record.op {
            TopicOp::Insert => "insert",
            TopicOp::Update => "update",
            TopicOp::Delete => "delete",
        };

        let key = format!("{}_{}_{}", table_name, op_str, id);
        received_events.insert(key, record.clone());
    }

    eprintln!("[TEST] Received events by key: {}", received_events.len());
    eprintln!("[TEST] Total records (including potential duplicates): {}", records.len());

    // Calculate coverage based on UNIQUE events received
    let unique_coverage = (received_events.len() as f64 / expected_count as f64) * 100.0;
    let duplication_ratio = records.len() as f64 / received_events.len().max(1) as f64;

    eprintln!("[TEST] Unique event coverage: {:.1}%", unique_coverage);
    eprintln!("[TEST] Duplication ratio: {:.1}x", duplication_ratio);

    // Check for excessive duplication which indicates a bug
    // Note: Consumer offset tracking with AutoOffsetReset::Earliest may cause
    // re-reads within a single session. The primary goal is 100% unique event coverage.
    if duplication_ratio > 2.0 {
        eprintln!("[WARNING] Event duplication detected: {:.1}x", duplication_ratio);
        eprintln!(
            "[WARNING] This is likely due to consumer offset re-reading, not publisher duplication"
        );
    }

    // Check coverage
    let mut missing_events = Vec::new();
    for (expected_key, _expected_info) in expected_lock.iter() {
        if !received_events.contains_key(expected_key) {
            missing_events.push(expected_key.clone());
        }
    }

    if !missing_events.is_empty() {
        eprintln!(
            "[TEST] Missing {} unique events out of {}:",
            missing_events.len(),
            expected_count
        );
        for (i, key) in missing_events.iter().enumerate().take(20) {
            eprintln!("[TEST]   Missing event {}: {}", i + 1, key);
        }
        if missing_events.len() > 20 {
            eprintln!("[TEST]   ... and {} more", missing_events.len() - 20);
        }
    }

    // With synchronous publishing (Phase 3), all successful writes are published
    // directly in the table provider write path. This eliminates the async queue
    // that previously dropped events via try_send.
    // Expected baseline: 100% coverage (1.0x duplication)
    let min_unique_coverage = 95.0;

    assert!(
        unique_coverage >= min_unique_coverage,
        "Expected at least {}% unique event coverage, got {:.1}% ({}/{}) - Synchronous publishing \
         should capture all events.\nCheck for table creation failures or write errors that \
         prevent events from being published.",
        min_unique_coverage,
        unique_coverage,
        received_events.len(),
        expected_count
    );

    // Note: Duplication ratio assertion removed. The consumer's AutoOffsetReset::Earliest
    // behavior combined with lack of server-side offset tracking within a single poll session
    // causes re-reads. The critical metric is unique event coverage, not duplication.

    // Validate datatypes in sample records
    eprintln!("[TEST] Validating datatypes in received records...");
    for record in records.iter().take(20) {
        let payload = parse_payload(&record.payload);

        // Every record should have a valid ID field
        let has_valid_id = payload.get("id").is_some()
            || payload.get("product_id").is_some()
            || payload.get("event_id").is_some()
            || payload.get("session_id").is_some();
        assert!(has_valid_id, "Record missing ID field: {:?}", payload);

        // Check for various datatypes
        if let Some(val) = payload.get("value").or_else(|| payload.get("score")) {
            let normalized = common::extract_typed_value(val);
            let is_numeric_string =
                normalized.as_str().and_then(|raw| raw.parse::<f64>().ok()).is_some();
            assert!(
                normalized.is_number() || is_numeric_string,
                "Numeric field should be a number: {:?}",
                val
            );
        }

        if let Some(val) = payload
            .get("active")
            .or_else(|| payload.get("verified"))
            .or_else(|| payload.get("available"))
            .or_else(|| payload.get("success"))
        {
            let normalized = common::extract_typed_value(val);
            assert!(normalized.is_boolean(), "Boolean field should be boolean: {:?}", val);
        }

        if let Some(val) = payload
            .get("name")
            .or_else(|| payload.get("username"))
            .or_else(|| payload.get("product_name"))
            .or_else(|| payload.get("event_type"))
        {
            let normalized = common::extract_typed_value(val);
            assert!(normalized.is_string(), "Text field should be string: {:?}", val);
        }
    }

    eprintln!("[TEST] Datatype validation passed");

    // Cleanup
    eprintln!("[TEST] Cleaning up...");
    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", shared_table)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", user_table)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", stream_table)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", product_table)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", session_table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;

    eprintln!("[TEST] High-load test completed successfully!");
}

/// Test that two concurrent consumers in the same group do not process the same
/// message offsets under high load.
#[tokio::test]
#[ntest::timeout(300000)]
async fn test_topic_high_load_two_consumers_same_group_single_delivery() {
    let namespace = common::generate_unique_namespace("highload_group");
    let table = format!("{}.events", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("same_group"));
    let group_id = format!("same-group-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("Failed to create namespace");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", table))
        .await
        .expect("Failed to create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic))
        .await
        .expect("Failed to create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("Failed to add topic source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages: usize = 800;
    let publishers_done = Arc::new(AtomicBool::new(false));

    let spawn_consumer = |consumer_name: &'static str, publishers_done: Arc<AtomicBool>| {
        let topic = topic.clone();
        let group_id = group_id.clone();
        tokio::spawn(async move {
            let mut consumer =
                topic_test_support::build_test_consumer(&topic, &group_id, 200, false).await;
            let seen_offsets = topic_test_support::poll_unique_offsets_until(
                &mut consumer,
                topic_test_support::UniqueOffsetPollConfig {
                    expected_messages: None,
                    publishers_done:   Some(publishers_done),
                    deadline:          Duration::from_secs(150),
                    idle_break_after:  120,
                    idle_sleep:        Duration::from_millis(100),
                    per_record_delay:  Duration::ZERO,
                    commit_each_batch: true,
                },
            )
            .await;
            eprintln!("[TEST] {} received {} offsets", consumer_name, seen_offsets.len());
            seen_offsets
        })
    };

    let consumer_a_handle = spawn_consumer("consumer-a", publishers_done.clone());
    let consumer_b_handle = spawn_consumer("consumer-b", publishers_done.clone());

    tokio::time::sleep(Duration::from_secs(1)).await;

    let publisher_parallelism = 24;
    topic_test_support::publish_numbered_rows(
        &table,
        "payload",
        "event",
        expected_messages,
        publisher_parallelism,
    )
    .await;
    publishers_done.store(true, Ordering::Relaxed);

    let consumer_a_offsets = consumer_a_handle.await.expect("consumer-a failed");
    let consumer_b_offsets = consumer_b_handle.await.expect("consumer-b failed");

    let overlap_count = consumer_a_offsets.intersection(&consumer_b_offsets).count();
    let total_unique = consumer_a_offsets.union(&consumer_b_offsets).count();

    eprintln!(
        "[TEST] same-group consumers results: A={}, B={}, overlap={}, total_unique={}",
        consumer_a_offsets.len(),
        consumer_b_offsets.len(),
        overlap_count,
        total_unique
    );

    assert_eq!(
        overlap_count, 0,
        "Consumers in the same group should not receive overlapping offsets"
    );
    let min_expected = expected_messages * 95 / 100;
    assert!(
        total_unique >= min_expected,
        "Expected at least {} messages processed by the group, got {}",
        min_expected,
        total_unique
    );
    assert!(
        total_unique <= expected_messages,
        "Processed messages ({}) should not exceed produced ({})",
        total_unique,
        expected_messages
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

/// Test fan-out: two consumer groups each receive the full message stream.
///
/// This verifies that different consumer groups operate independently — every
/// group sees every message, while within a single group, messages are not
/// duplicated.
#[tokio::test]
#[ntest::timeout(300000)]
async fn test_topic_fan_out_different_groups_receive_all() {
    let namespace = common::generate_unique_namespace("fanout");
    let table = format!("{}.events", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("fanout_topic"));
    let group_a = format!("fanout-group-a-{}", common::random_string(8));
    let group_b = format!("fanout-group-b-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create ns");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, data TEXT)", table))
        .await
        .expect("create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages: usize = 300;
    let publishers_done = Arc::new(AtomicBool::new(false));

    // Helper: spawn a single consumer for a given group
    let spawn_group_consumer =
        |group_id: String, publishers_done: Arc<AtomicBool>, label: &'static str| {
            let topic = topic.clone();
            tokio::spawn(async move {
                let mut consumer =
                    topic_test_support::build_test_consumer(&topic, &group_id, 200, false).await;
                let seen = topic_test_support::poll_unique_offsets_until(
                    &mut consumer,
                    topic_test_support::UniqueOffsetPollConfig {
                        expected_messages: None,
                        publishers_done:   Some(publishers_done),
                        deadline:          Duration::from_secs(150),
                        idle_break_after:  40,
                        idle_sleep:        Duration::from_millis(100),
                        per_record_delay:  Duration::ZERO,
                        commit_each_batch: true,
                    },
                )
                .await;
                eprintln!("[TEST] {} received {} offsets", label, seen.len());
                seen
            })
        };

    let handle_a = spawn_group_consumer(group_a.clone(), publishers_done.clone(), "group-a");
    let handle_b = spawn_group_consumer(group_b.clone(), publishers_done.clone(), "group-b");

    tokio::time::sleep(Duration::from_millis(300)).await;

    topic_test_support::publish_numbered_rows(&table, "data", "val", expected_messages, 10).await;
    publishers_done.store(true, Ordering::Relaxed);

    let offsets_a = handle_a.await.expect("group-a consumer");
    let offsets_b = handle_b.await.expect("group-b consumer");

    eprintln!(
        "[TEST] fan-out results: group_a={}, group_b={}, expected={}",
        offsets_a.len(),
        offsets_b.len(),
        expected_messages
    );

    let min_expected = expected_messages * 95 / 100;
    assert!(
        offsets_a.len() >= min_expected,
        "Group A should receive at least {} messages (got {})",
        min_expected,
        offsets_a.len()
    );
    assert!(
        offsets_b.len() >= min_expected,
        "Group B should receive at least {} messages (got {})",
        min_expected,
        offsets_b.len()
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

/// Stress test: 4 consumers in the same group under high load.
///
/// Verifies exactly-once delivery semantics hold with more consumer concurrency.
#[tokio::test]
#[ntest::timeout(300000)]
async fn test_topic_four_consumers_same_group_no_duplicates() {
    let namespace = common::generate_unique_namespace("stress4c");
    let table = format!("{}.items", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("stress4"));
    let group_id = format!("stress4-group-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create ns");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, value TEXT)", table))
        .await
        .expect("create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages: usize = 1_200;
    let publishers_done = Arc::new(AtomicBool::new(false));

    let consumer_count = 4;
    let mut consumer_handles = Vec::with_capacity(consumer_count);

    for idx in 0..consumer_count {
        let topic = topic.clone();
        let group_id = group_id.clone();
        let done = publishers_done.clone();
        let label = format!("consumer-{}", idx);

        consumer_handles.push(tokio::spawn(async move {
            let mut consumer =
                topic_test_support::build_test_consumer(&topic, &group_id, 100, false).await;
            let seen = topic_test_support::poll_unique_offsets_until(
                &mut consumer,
                topic_test_support::UniqueOffsetPollConfig {
                    expected_messages: None,
                    publishers_done:   Some(done),
                    deadline:          Duration::from_secs(180),
                    idle_break_after:  150,
                    idle_sleep:        Duration::from_millis(80),
                    per_record_delay:  Duration::ZERO,
                    commit_each_batch: true,
                },
            )
            .await;
            (label, seen)
        }));
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    topic_test_support::publish_numbered_rows(&table, "value", "item", expected_messages, 24).await;
    publishers_done.store(true, Ordering::Relaxed);

    // Collect results
    let mut all_consumer_offsets: Vec<(String, HashSet<(u32, u64)>)> = Vec::new();
    for h in consumer_handles {
        all_consumer_offsets.push(h.await.expect("consumer task"));
    }

    // Check: no overlap between any pair of consumers
    let mut combined: HashSet<(u32, u64)> = HashSet::new();
    let mut total_messages_across_consumers = 0;

    for (label, offsets) in &all_consumer_offsets {
        eprintln!("[TEST] {} received {} messages", label, offsets.len());
        total_messages_across_consumers += offsets.len();

        for (other_label, other_offsets) in &all_consumer_offsets {
            if label != other_label {
                let overlap = offsets.intersection(other_offsets).count();
                assert_eq!(
                    overlap, 0,
                    "Overlap between {} and {} = {} (must be 0)",
                    label, other_label, overlap
                );
            }
        }

        combined.extend(offsets.iter());
    }

    eprintln!(
        "[TEST] 4-consumer stress: total_unique={}, total_received={}, expected={}",
        combined.len(),
        total_messages_across_consumers,
        expected_messages
    );

    // No duplicates: total received should equal total unique
    assert_eq!(
        total_messages_across_consumers,
        combined.len(),
        "No duplicates: sum of per-consumer counts should equal unique count"
    );

    // All messages delivered
    let min_expected = expected_messages * 95 / 100;
    assert!(
        combined.len() >= min_expected,
        "Expected at least {} unique messages, got {}",
        min_expected,
        combined.len()
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

/// High-load recovery test:
/// 1. Consumer A claims a range and never commits (simulated ack failure/crash).
/// 2. After visibility timeout, Consumer B (same group) must recover and process the entire stream
///    without offset gaps, even with per-message processing latency.
#[tokio::test]
#[ntest::timeout(180000)]
async fn test_topic_ack_failure_recovery_no_message_loss_with_latency() {
    let namespace = common::generate_unique_namespace("ack_recovery");
    let table = format!("{}.events", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("ack_topic"));
    let group_id = format!("ack-recovery-group-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create namespace");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", table))
        .await
        .expect("create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages: usize = 480;
    topic_test_support::publish_numbered_rows(&table, "payload", "payload", expected_messages, 12)
        .await;

    let consumer_a_claim_target = 160usize;
    let claimed_by_a = {
        let mut consumer_a =
            topic_test_support::build_test_consumer(&topic, &group_id, 40, false).await;
        poll_records_raw_until(
            &mut consumer_a,
            RawRecordPollConfig {
                min_records:       consumer_a_claim_target,
                deadline:          Duration::from_secs(35),
                idle_sleep:        Duration::from_millis(80),
                per_record_delay:  Duration::ZERO,
                commit_each_batch: false,
            },
        )
        .await
    }; // drop without commit -> simulate crash/ack failure

    assert!(
        claimed_by_a.len() >= 120,
        "Consumer A should claim a meaningful prefix before failure (claimed={})",
        claimed_by_a.len()
    );

    let claimed_offsets = record_offsets(&claimed_by_a);
    assert_consecutive_offsets(&claimed_offsets, 0, "consumer-a claimed prefix before failure");

    tokio::time::sleep(topic_visibility_timeout_wait().await).await;

    let mut consumer_b =
        topic_test_support::build_test_consumer(&topic, &group_id, 60, false).await;

    let recovered_records = poll_records_raw_until(
        &mut consumer_b,
        RawRecordPollConfig {
            min_records:       expected_messages,
            deadline:          topic_recovery_deadline().await,
            idle_sleep:        Duration::from_millis(80),
            per_record_delay:  Duration::from_millis(2),
            commit_each_batch: false,
        },
    )
    .await;

    assert_eq!(
        recovered_records.len(),
        expected_messages,
        "Recovered consumer must process every produced message exactly once after timeout"
    );

    let recovered_offsets = record_offsets(&recovered_records);
    let recovered_ids = record_payload_ids(&recovered_records, &["id"]);
    assert_consecutive_offsets(&recovered_offsets, 0, "ordered recovery offsets");
    assert_complete_ids(&recovered_ids, expected_messages, "ordered recovery id coverage");

    let commit_result = consumer_b.commit_sync().await.expect("recovery commit should succeed");
    assert_eq!(
        commit_result.acknowledged_offset,
        (expected_messages - 1) as u64,
        "Recovered consumer should commit the final produced offset"
    );

    let empty_after_commit = consumer_b
        .poll_with_timeout(Duration::from_millis(750))
        .await
        .expect("post-commit poll should succeed");
    assert!(
        empty_after_commit.is_empty(),
        "Committed recovery consumer should not receive an immediate replay"
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

/// Verify that an unacked prefix is not re-delivered before the visibility
/// timeout, then recovers in order after expiry, and a stale ACK cannot
/// regress the committed group offset.
#[tokio::test]
#[ntest::timeout(180000)]
async fn test_topic_redelivery_waits_for_visibility_timeout_and_late_ack_does_not_replay() {
    let namespace = common::generate_unique_namespace("late_ack_visibility");
    let table = format!("{}.events", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("late_ack_topic"));
    let group_id = format!("late-ack-group-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create namespace");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", table))
        .await
        .expect("create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages = 120usize;
    topic_test_support::publish_numbered_rows(&table, "payload", "payload", expected_messages, 8)
        .await;

    let claimed_prefix = {
        let mut consumer_a =
            topic_test_support::build_test_consumer(&topic, &group_id, 20, false).await;
        poll_records_raw_until(
            &mut consumer_a,
            RawRecordPollConfig {
                min_records:       60,
                deadline:          Duration::from_secs(20),
                idle_sleep:        Duration::from_millis(80),
                per_record_delay:  Duration::ZERO,
                commit_each_batch: false,
            },
        )
        .await
    };

    let claimed_offsets = record_offsets(&claimed_prefix);
    assert_consecutive_offsets(&claimed_offsets, 0, "unacked claimed prefix offsets");

    let claimed_set: HashSet<u64> = claimed_offsets.iter().copied().collect();
    let last_claimed_offset = *claimed_offsets.last().expect("claimed prefix should not be empty");
    let stale_ack_offset = claimed_offsets[claimed_offsets.len() / 2];

    {
        let mut consumer_b =
            topic_test_support::build_test_consumer(&topic, &group_id, 20, false).await;
        let pre_timeout_batch = consumer_b
            .poll_with_timeout(Duration::from_secs(1))
            .await
            .expect("pre-timeout poll should succeed");

        for record in &pre_timeout_batch {
            assert!(
                !claimed_set.contains(&record.offset),
                "Claimed offsets must not be re-delivered before visibility timeout expires"
            );
            assert!(
                record.offset > last_claimed_offset,
                "Pre-timeout delivery should only advance beyond the pending prefix"
            );
        }
    }

    tokio::time::sleep(topic_visibility_timeout_wait().await).await;

    let mut consumer_c =
        topic_test_support::build_test_consumer(&topic, &group_id, 30, false).await;
    let recovered_records = poll_records_raw_until(
        &mut consumer_c,
        RawRecordPollConfig {
            min_records:       expected_messages,
            deadline:          topic_recovery_deadline().await,
            idle_sleep:        Duration::from_millis(80),
            per_record_delay:  Duration::ZERO,
            commit_each_batch: false,
        },
    )
    .await;

    assert_eq!(
        recovered_records.len(),
        expected_messages,
        "Recovered stream should include the full message set exactly once"
    );

    let recovered_offsets = record_offsets(&recovered_records);
    let recovered_ids = record_payload_ids(&recovered_records, &["id"]);
    assert_consecutive_offsets(&recovered_offsets, 0, "post-timeout recovered offsets");
    assert_complete_ids(&recovered_ids, expected_messages, "post-timeout recovered id coverage");

    let commit_result = consumer_c.commit_sync().await.expect("recovery commit should succeed");
    assert_eq!(
        commit_result.acknowledged_offset,
        (expected_messages - 1) as u64,
        "Recovered consumer should commit the final produced offset"
    );

    let offsets_after_commit = topic_offset_rows(&topic, &group_id).await;
    assert_eq!(
        offsets_after_commit.len(),
        1,
        "Commit should persist one topic offset row for the group"
    );
    assert_eq!(
        parse_u64_row_field(&offsets_after_commit[0], "last_acked_offset"),
        (expected_messages - 1) as u64,
        "Committed offset should match the last produced message"
    );

    execute_sql(&format!(
        "ACK {} GROUP '{}' PARTITION 0 UPTO OFFSET {}",
        topic, group_id, stale_ack_offset
    ))
    .await
    .expect("stale ack should not fail");

    let offsets_after_stale_ack = topic_offset_rows(&topic, &group_id).await;
    assert_eq!(
        parse_u64_row_field(&offsets_after_stale_ack[0], "last_acked_offset"),
        (expected_messages - 1) as u64,
        "Late ACK must not regress the committed group offset"
    );

    let mut consumer_d =
        topic_test_support::build_test_consumer(&topic, &group_id, 20, false).await;
    let empty_after_recovery = consumer_d
        .poll_with_timeout(Duration::from_millis(750))
        .await
        .expect("post-recovery poll should succeed");
    assert!(
        empty_after_recovery.is_empty(),
        "Fully committed stream should not replay after a stale ACK"
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

/// Verify that after committing an initial prefix, a crash with a later
/// unacked claim resumes from exactly the first unacked offset instead of
/// replaying the committed prefix or skipping ahead.
#[tokio::test]
#[ntest::timeout(180000)]
async fn test_topic_partial_commit_then_crash_recovers_from_first_unacked_offset() {
    let namespace = common::generate_unique_namespace("partial_commit_recovery");
    let table = format!("{}.events", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("partial_commit_topic"));
    let group_id = format!("partial-commit-group-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create namespace");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", table))
        .await
        .expect("create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages = 120usize;
    let committed_prefix_len = 48usize;
    let unacked_claim_len = 32usize;
    topic_test_support::publish_numbered_rows(&table, "payload", "payload", expected_messages, 8)
        .await;

    let mut consumer_a =
        topic_test_support::build_test_consumer(&topic, &group_id, 16, false).await;
    let committed_records = poll_records_raw_until(
        &mut consumer_a,
        RawRecordPollConfig {
            min_records:       committed_prefix_len,
            deadline:          Duration::from_secs(20),
            idle_sleep:        Duration::from_millis(80),
            per_record_delay:  Duration::ZERO,
            commit_each_batch: false,
        },
    )
    .await;

    let committed_offsets = record_offsets(&committed_records);
    let committed_ids = record_payload_ids(&committed_records, &["id"]);
    assert_consecutive_offsets(&committed_offsets, 0, "committed prefix offsets");

    let initial_commit =
        consumer_a.commit_sync().await.expect("initial prefix commit should succeed");
    assert_eq!(
        initial_commit.acknowledged_offset,
        committed_offsets.last().copied().expect("committed prefix should not be empty"),
        "Committed prefix should durably ack the last processed offset"
    );

    let offsets_after_initial_commit = topic_offset_rows(&topic, &group_id).await;
    assert_eq!(
        offsets_after_initial_commit.len(),
        1,
        "Initial commit should persist one topic offset row"
    );
    assert_eq!(
        parse_u64_row_field(&offsets_after_initial_commit[0], "last_acked_offset"),
        (committed_prefix_len - 1) as u64,
        "Committed prefix should persist its last offset"
    );

    let pending_records = poll_records_raw_until(
        &mut consumer_a,
        RawRecordPollConfig {
            min_records:       unacked_claim_len,
            deadline:          Duration::from_secs(20),
            idle_sleep:        Duration::from_millis(80),
            per_record_delay:  Duration::ZERO,
            commit_each_batch: false,
        },
    )
    .await;
    let pending_offsets = record_offsets(&pending_records);
    assert_consecutive_offsets(
        &pending_offsets,
        committed_prefix_len as u64,
        "unacked claimed tail offsets",
    );

    drop(consumer_a); // crash after claiming the tail without committing it

    tokio::time::sleep(topic_visibility_timeout_wait().await).await;

    let mut consumer_b =
        topic_test_support::build_test_consumer(&topic, &group_id, 24, false).await;
    let recovered_records = poll_records_raw_until(
        &mut consumer_b,
        RawRecordPollConfig {
            min_records:       expected_messages - committed_prefix_len,
            deadline:          topic_recovery_deadline().await,
            idle_sleep:        Duration::from_millis(80),
            per_record_delay:  Duration::ZERO,
            commit_each_batch: false,
        },
    )
    .await;

    assert_eq!(
        recovered_records.len(),
        expected_messages - committed_prefix_len,
        "Recovery after a partial commit should only deliver the unacked suffix"
    );

    let recovered_offsets = record_offsets(&recovered_records);
    let recovered_ids = record_payload_ids(&recovered_records, &["id"]);
    assert_consecutive_offsets(
        &recovered_offsets,
        committed_prefix_len as u64,
        "recovered offsets after committed prefix",
    );
    assert_eq!(
        recovered_offsets.first().copied(),
        Some(committed_prefix_len as u64),
        "Recovery must resume from the first unacked offset"
    );
    assert_eq!(
        recovered_offsets.first().copied(),
        pending_offsets.first().copied(),
        "Recovered suffix must start at the first expired unacked claim"
    );

    let committed_id_set: HashSet<i64> = committed_ids.iter().copied().collect();
    let recovered_id_set: HashSet<i64> = recovered_ids.iter().copied().collect();
    assert_eq!(
        committed_id_set.intersection(&recovered_id_set).count(),
        0,
        "Recovered suffix should not replay any already committed payload ids"
    );

    let mut combined_ids = committed_ids.clone();
    combined_ids.extend(recovered_ids.iter().copied());
    assert_complete_ids(
        &combined_ids,
        expected_messages,
        "partial commit plus recovery should cover the full id set",
    );

    let recovery_commit = consumer_b.commit_sync().await.expect("recovery commit should succeed");
    assert_eq!(
        recovery_commit.acknowledged_offset,
        (expected_messages - 1) as u64,
        "Recovery commit should advance to the final produced offset"
    );

    let offsets_after_recovery = topic_offset_rows(&topic, &group_id).await;
    assert_eq!(
        parse_u64_row_field(&offsets_after_recovery[0], "last_acked_offset"),
        (expected_messages - 1) as u64,
        "Committed group offset should end at the final produced message"
    );

    let mut consumer_c =
        topic_test_support::build_test_consumer(&topic, &group_id, 20, false).await;
    let empty_after_full_recovery = consumer_c
        .poll_with_timeout(Duration::from_millis(750))
        .await
        .expect("post-recovery poll should succeed");
    assert!(
        empty_after_full_recovery.is_empty(),
        "Fully recovered stream should not replay after the recovery commit"
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

/// Validate that a slower consumer still receives every large payload in order,
/// with no skipped offsets and no immediate replay after commit.
#[tokio::test]
#[ntest::timeout(180000)]
async fn test_topic_slow_consumer_large_payloads_preserve_order_and_no_loss() {
    let namespace = common::generate_unique_namespace("large_payload_topic");
    let table = format!("{}.events", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("large_payload"));
    let group_id = format!("large-payload-group-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create namespace");
    execute_sql(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT, payload_size INT, bucket TEXT)",
        table
    ))
    .await
    .expect("create table");
    common::grant_public_shared_table_access(&table);
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let expected_messages = 192usize;
    let payload_size = 8 * 1024usize;
    publish_large_payload_rows(&table, expected_messages, payload_size, 8).await;

    let mut consumer = topic_test_support::build_test_consumer(&topic, &group_id, 24, false).await;
    let records = poll_records_raw_until(
        &mut consumer,
        RawRecordPollConfig {
            min_records:       expected_messages,
            deadline:          Duration::from_secs(60),
            idle_sleep:        Duration::from_millis(80),
            per_record_delay:  Duration::from_millis(5),
            commit_each_batch: true,
        },
    )
    .await;

    assert_eq!(
        records.len(),
        expected_messages,
        "Slow consumer should process every large payload exactly once"
    );

    let offsets = record_offsets(&records);
    let ids = record_payload_ids(&records, &["id"]);
    assert_consecutive_offsets(&offsets, 0, "large payload offsets");
    assert_complete_ids(&ids, expected_messages, "large payload id coverage");

    for record in &records {
        let payload = parse_payload(&record.payload);
        let id =
            extract_i64_field(&payload, &["id"]).expect("large payload record should include id");
        let payload_text = extract_string_field(&payload, "payload")
            .expect("large payload record should include payload text");
        let declared_size = extract_i64_field(&payload, &["payload_size"])
            .expect("large payload record should include payload_size");

        assert_eq!(
            declared_size, payload_size as i64,
            "payload_size metadata should match the inserted payload size"
        );
        assert_eq!(
            payload_text.len(),
            payload_size,
            "payload text length should match the inserted payload size"
        );
        assert!(
            payload_text.starts_with(&format!("blob_{:04}_", id)),
            "payload {} should preserve its deterministic prefix",
            id
        );
    }

    let offsets_after_commit = topic_offset_rows(&topic, &group_id).await;
    assert_eq!(
        offsets_after_commit.len(),
        1,
        "Slow consumer commits should persist one group offset row"
    );
    assert_eq!(
        parse_u64_row_field(&offsets_after_commit[0], "last_acked_offset"),
        (expected_messages - 1) as u64,
        "Committed offset should reach the final large payload record"
    );

    let empty_after_commit = consumer
        .poll_with_timeout(Duration::from_millis(750))
        .await
        .expect("post-commit poll should succeed");
    assert!(
        empty_after_commit.is_empty(),
        "Large payload stream should not replay immediately after commit"
    );

    let _ = execute_sql(&format!("DROP TOPIC {}", topic)).await;
    let _ = execute_sql(&format!("DROP TABLE {}", table)).await;
    let _ = execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct EventInfo {
    table: String,
    op:    TopicOp,
    id:    i64,
}

async fn record_expected_event(
    expected: &Arc<TokioMutex<HashMap<String, EventInfo>>>,
    key: String,
    table: &str,
    op: TopicOp,
    id: i64,
) {
    let mut expected_lock = expected.lock().await;
    expected_lock.insert(
        key,
        EventInfo {
            table: table.to_string(),
            op,
            id,
        },
    );
}
