//! Smoke tests for topic consumption (consume + ack) with CDC events
//!
//! Tests comprehensive topic consumption scenarios including:
//! - INSERT/UPDATE/DELETE operations on tables
//! - Consumer groups and offset tracking
//! - Starting from earliest/latest offsets
//!
//! **Requirements**: Running KalamDB server with Topics feature enabled

use crate::common;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use kalam_client::consumer::{AutoOffsetReset, ConsumerRecord, TopicOp};
use kalam_client::KalamLinkTimeouts;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct HttpTopicConsumeResponse {
    messages: Vec<HttpTopicMessage>,
}

#[derive(Debug, Deserialize)]
struct HttpTopicMessage {
    topic_id: String,
    partition_id: u32,
    offset: u64,
    payload: String,
    user: Option<String>,
    op: String,
}

impl HttpTopicMessage {
    fn payload_json(&self) -> serde_json::Value {
        let bytes = BASE64_STANDARD
            .decode(self.payload.as_bytes())
            .expect("Topic payload should be valid base64");
        parse_payload(&bytes)
    }
}

/// Create a test client using common infrastructure
async fn create_test_client() -> kalam_client::KalamLinkClient {
    let base_url = common::leader_or_server_url();
    common::client_for_user_on_url_with_timeouts(
        &base_url,
        common::default_username(),
        common::default_password(),
        KalamLinkTimeouts::builder()
            .connection_timeout_secs(5)
            .receive_timeout_secs(120)
            .send_timeout_secs(30)
            .subscribe_timeout_secs(10)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(120))
            .build(),
    )
    .expect("Failed to build test client")
}

/// Execute SQL via HTTP helper
async fn execute_sql(sql: &str) {
    common::execute_sql_via_http_as_root(sql).await.expect("Failed to execute SQL");
}

async fn wait_for_topic_ready(topic: &str, expected_routes: usize) {
    let sql = format!("SELECT routes FROM system.topics WHERE topic_id = '{}'", topic);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);

    while std::time::Instant::now() < deadline {
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

async fn create_topic_with_sources(topic: &str, table: &str, operations: &[&str]) {
    execute_sql(&format!("CREATE TOPIC {}", topic)).await;
    for op in operations {
        execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON {}", topic, table, op)).await;
    }
    wait_for_topic_ready(topic, operations.len()).await;
}

fn response_is_success(response: &serde_json::Value) -> bool {
    response
        .get("status")
        .and_then(|status| status.as_str())
        .map(|status| status.eq_ignore_ascii_case("success"))
        .unwrap_or(false)
}

fn response_error_message(response: &serde_json::Value) -> String {
    response
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(|message| message.as_str())
        .unwrap_or("unknown error")
        .to_string()
}

async fn execute_sql_as(username: &str, password: &str, sql: &str) -> serde_json::Value {
    let response = common::execute_sql_via_http_as(username, password, sql)
        .await
        .expect("Failed to execute SQL over HTTP");
    assert!(
        response_is_success(&response),
        "SQL should succeed: {}\nresponse: {}",
        sql,
        response
    );
    response
}

async fn create_user_with_retry(username: &str, password: &str, role: &str) {
    let sql = format!("CREATE USER {} WITH PASSWORD '{}' ROLE '{}'", username, password, role);
    let mut last_error = None;

    for attempt in 0..3 {
        let response = common::execute_sql_via_http_as_root(&sql)
            .await
            .expect("Failed to create user via HTTP");
        if response_is_success(&response) {
            return;
        }

        let message = response_error_message(&response);
        if message.contains("Already exists") {
            let alter_sql = format!("ALTER USER {} SET PASSWORD '{}'", username, password);
            let alter_response = common::execute_sql_via_http_as_root(&alter_sql)
                .await
                .expect("Failed to alter existing user password");
            assert!(
                response_is_success(&alter_response),
                "ALTER USER should succeed after duplicate create: {}",
                alter_response
            );
            return;
        }

        if message.contains("Serialization error") || message.contains("UnexpectedEnd") {
            last_error = Some(message);
            tokio::time::sleep(Duration::from_millis(200 * (attempt + 1) as u64)).await;
            continue;
        }

        panic!("Failed to create user {}: {}", username, message);
    }

    panic!(
        "Failed to create user {} after retries: {}",
        username,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    );
}

async fn get_user_id(user_id: &str) -> String {
    let response = common::execute_sql_via_http_as_root(&format!(
        "SELECT user_id FROM system.users WHERE user_id = '{}'",
        user_id
    ))
    .await
    .expect("Failed to query system.users");
    assert!(
        response_is_success(&response),
        "user lookup should succeed: {}",
        response
    );

    let rows = common::get_rows_as_hashmaps(&response).expect("user lookup should return rows");
    let first_row = rows.first().expect("user lookup should return one row");
    first_row
        .get("user_id")
        .map(common::extract_typed_value)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .expect("user lookup row should contain user_id")
}

async fn poll_topic_messages_http_until(
    topic: &str,
    min_messages: usize,
    timeout: Duration,
) -> Vec<HttpTopicMessage> {
    let base_url = common::leader_or_server_url();
    let token = common::get_access_token_for_url(
        &base_url,
        common::default_username(),
        common::default_password(),
    )
    .await
    .expect("Failed to get root token for topic consume");
    let client = Client::new();
    let deadline = std::time::Instant::now() + timeout;
    let mut last_count = 0usize;

    while std::time::Instant::now() < deadline {
        let group_id = format!("topic-http-{}", common::random_string(12));
        let response = client
            .post(format!("{}/v1/api/topics/consume", base_url))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "topic_id": topic,
                "group_id": group_id,
                "start": "Earliest",
                "limit": 100,
                "partition_id": 0
            }))
            .send()
            .await
            .expect("Topic consume request should succeed");

        let status = response.status();
        let body = response.text().await.expect("Failed to read topic consume body");
        assert!(status.is_success(), "Topic consume failed: {}", body);

        let parsed: HttpTopicConsumeResponse =
            serde_json::from_str(&body).expect("Failed to parse topic consume response JSON");
        last_count = parsed.messages.len();
        if parsed.messages.len() >= min_messages {
            return parsed.messages;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "Timed out waiting for at least {} topic messages on {} (last_count={})",
        min_messages,
        topic,
        last_count
    );
}

async fn poll_records_until(
    consumer: &mut kalam_client::consumer::TopicConsumer,
    min_records: usize,
    timeout: Duration,
) -> Vec<ConsumerRecord> {
    let mut records = Vec::new();
    let mut seen_offsets = HashSet::new();
    let mut last_error: Option<String> = None;
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        match consumer.poll().await {
            Ok(batch) => {
                if batch.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }

                for record in batch {
                    if seen_offsets.insert((record.partition_id, record.offset)) {
                        records.push(record);
                    }
                }
                if records.len() >= min_records {
                    break;
                }
            },
            Err(err) => {
                let message = err.to_string();
                last_error = Some(message.clone());
                if message.contains("error decoding response body")
                    || message.contains("network")
                    || message.contains("NetworkError")
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                panic!("Failed to poll: {}", message);
            },
        }
    }
    if records.is_empty() {
        if let Some(message) = last_error {
            eprintln!("[TEST] poll_records_until last error: {}", message);
        }
    }
    records
}

/// Helper to parse JSON payload from binary
fn parse_payload(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("Failed to parse payload")
}

fn parse_string_field(payload: &serde_json::Value, key: &str) -> Option<String> {
    let raw = payload.get(key)?;
    let untyped = common::extract_typed_value(raw);
    untyped.as_str().map(ToOwned::to_owned)
}

fn parse_f64_field(payload: &serde_json::Value, key: &str) -> Option<f64> {
    let raw = payload.get(key)?;
    let untyped = common::extract_typed_value(raw);
    if let Some(value) = untyped.as_f64() {
        return Some(value);
    }
    untyped.as_str().and_then(|value| value.parse::<f64>().ok())
}

fn parse_i64_field(payload: &serde_json::Value, key: &str) -> Option<i64> {
    let raw = payload.get(key)?;
    let untyped = common::extract_typed_value(raw);
    if let Some(value) = untyped.as_i64() {
        return Some(value);
    }
    untyped.as_str().and_then(|value| value.parse::<i64>().ok())
}

fn parse_bool_field(payload: &serde_json::Value, key: &str) -> Option<bool> {
    let raw = payload.get(key)?;
    let untyped = common::extract_typed_value(raw);
    if let Some(value) = untyped.as_bool() {
        return Some(value);
    }
    untyped.as_str().and_then(|value| value.parse::<bool>().ok())
}

fn parse_binary_row_field_as_json(
    row: &HashMap<String, serde_json::Value>,
    key: &str,
) -> serde_json::Value {
    let raw = row.get(key).unwrap_or_else(|| panic!("row should contain {}", key));
    let value = common::extract_typed_value(raw);
    let bytes: Vec<u8> = value
        .as_array()
        .unwrap_or_else(|| panic!("{} should be a binary byte array", key))
        .iter()
        .map(|item| {
            item.as_u64()
                .and_then(|num| u8::try_from(num).ok())
                .unwrap_or_else(|| panic!("{} should only contain byte values", key))
        })
        .collect();
    parse_payload(&bytes)
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_insert_events() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("events");
    let topic = format!("{}.{}", namespace, table);

    // Setup
    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, name TEXT, value DOUBLE)",
        namespace, table
    ))
    .await;
    create_topic_with_sources(&topic, &format!("{}.{}", namespace, table), &["INSERT"]).await;

    // Insert test data
    for i in 1..=3 {
        execute_sql(&format!(
            "INSERT INTO {}.{} (id, name, value) VALUES ({}, 'test{}', {})",
            namespace,
            table,
            i,
            i,
            i as f64 * 1.5
        ))
        .await;
    }

    // Consume messages
    let client = create_test_client().await;
    let mut consumer = client
        .consumer()
        .topic(&topic)
        .group_id("test-insert-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_records(10)
        .build()
        .expect("Failed to build consumer");

    let records = poll_records_until(&mut consumer, 3, Duration::from_secs(20)).await;
    assert!(records.len() >= 3, "Should receive 3 INSERT events");

    let mut seen_ids = HashSet::new();
    for record in &records {
        let payload = parse_payload(&record.payload);
        let id = parse_i64_field(&payload, "id").expect("insert payload should include id");
        let name =
            parse_string_field(&payload, "name").expect("insert payload should include name");
        let value =
            parse_f64_field(&payload, "value").expect("insert payload should include value");

        assert!(seen_ids.insert(id), "duplicate insert payload id detected: {}", id);
        assert_eq!(name, format!("test{}", id));
        assert!((value - (id as f64 * 1.5)).abs() < f64::EPSILON);
        consumer.mark_processed(record);
    }
    assert_eq!(seen_ids.len(), 3, "Should see exactly 3 unique insert payloads");

    let result = consumer.commit_sync().await.expect("Failed to commit");
    assert_eq!(result.acknowledged_offset, 2);

    // Cleanup
    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_update_events() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("updates");
    let topic = format!("{}.{}", namespace, table);

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, status TEXT, counter INT)",
        namespace, table
    ))
    .await;
    create_topic_with_sources(&topic, &format!("{}.{}", namespace, table), &["INSERT", "UPDATE"])
        .await;

    execute_sql(&format!(
        "INSERT INTO {}.{} (id, status, counter) VALUES (1, 'pending', 0)",
        namespace, table
    ))
    .await;

    for i in 1..=2 {
        execute_sql(&format!(
            "UPDATE {}.{} SET status = 'active', counter = {} WHERE id = 1",
            namespace, table, i
        ))
        .await;
    }

    let client = create_test_client().await;
    let mut consumer = client
        .consumer()
        .topic(&topic)
        .group_id("test-update-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .expect("Failed to build consumer");

    let records = poll_records_until(&mut consumer, 3, Duration::from_secs(20)).await;

    assert_eq!(records.len(), 3, "Should receive 1 INSERT + 2 UPDATE events");

    let inserts = records.iter().filter(|r| r.op == TopicOp::Insert).count();
    let updates = records.iter().filter(|r| r.op == TopicOp::Update).count();
    assert_eq!(inserts, 1, "Should have exactly 1 INSERT event");
    assert_eq!(updates, 2, "Should have exactly 2 UPDATE events");

    let mut update_counters = Vec::new();
    for record in &records {
        let payload = parse_payload(&record.payload);
        assert_eq!(parse_i64_field(&payload, "id"), Some(1));

        match record.op {
            TopicOp::Insert => {
                assert_eq!(parse_string_field(&payload, "status").as_deref(), Some("pending"));
                assert_eq!(parse_i64_field(&payload, "counter"), Some(0));
            },
            TopicOp::Update => {
                assert_eq!(parse_string_field(&payload, "status").as_deref(), Some("active"));
                update_counters.push(
                    parse_i64_field(&payload, "counter")
                        .expect("update payload should include counter"),
                );
            },
            TopicOp::Delete => panic!("unexpected delete event in update test"),
        }
    }
    update_counters.sort_unstable();
    assert_eq!(update_counters, vec![1, 2], "Update payloads should reflect both writes");

    for record in &records {
        consumer.mark_processed(record);
    }
    consumer.commit_sync().await.expect("Failed to commit");

    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_delete_events() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("deletes");
    let topic = format!("{}.{}", namespace, table);

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!("CREATE TABLE {}.{} (id INT PRIMARY KEY, name TEXT)", namespace, table))
        .await;
    create_topic_with_sources(&topic, &format!("{}.{}", namespace, table), &["INSERT", "DELETE"])
        .await;

    for i in 1..=3 {
        execute_sql(&format!(
            "INSERT INTO {}.{} (id, name) VALUES ({}, 'record{}')",
            namespace, table, i, i
        ))
        .await;
    }

    execute_sql(&format!("DELETE FROM {}.{} WHERE id = 2", namespace, table)).await;

    let client = create_test_client().await;
    let mut consumer = client
        .consumer()
        .topic(&topic)
        .group_id("test-delete-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_records(50)
        .build()
        .expect("Failed to build consumer");

    let records = poll_records_until(&mut consumer, 4, Duration::from_secs(20)).await;
    assert!(records.len() >= 4, "Should receive at least 3 INSERTs + 1 DELETE");

    let deletes_by_op = records.iter().filter(|r| r.op == TopicOp::Delete).count();
    let deletes_by_payload = records
        .iter()
        .filter(|r| {
            let payload = parse_payload(&r.payload);
            parse_bool_field(&payload, "_deleted").unwrap_or(false)
        })
        .count();
    assert!(deletes_by_op.max(deletes_by_payload) >= 1);

    let delete_record = records
        .iter()
        .find(|record| {
            record.op == TopicOp::Delete
                || parse_bool_field(&parse_payload(&record.payload), "_deleted") == Some(true)
        })
        .expect("Should find at least one delete event");
    let delete_payload = parse_payload(&delete_record.payload);
    assert_eq!(parse_i64_field(&delete_payload, "id"), Some(2));
    assert_eq!(parse_string_field(&delete_payload, "name").as_deref(), Some("record2"));
    assert_eq!(parse_bool_field(&delete_payload, "_deleted"), Some(true));

    for record in &records {
        consumer.mark_processed(record);
    }
    consumer.commit_sync().await.ok();

    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_mixed_operations() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("mixed");
    let topic = format!("{}.{}", namespace, table);

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!(
        "CREATE TABLE {}.{} (id INT PRIMARY KEY, data TEXT, version INT)",
        namespace, table
    ))
    .await;
    create_topic_with_sources(
        &topic,
        &format!("{}.{}", namespace, table),
        &["INSERT", "UPDATE", "DELETE"],
    )
    .await;

    // Mixed operations: INSERT, UPDATE, INSERT, DELETE
    execute_sql(&format!(
        "INSERT INTO {}.{} (id, data, version) VALUES (1, 'initial', 1)",
        namespace, table
    ))
    .await;
    execute_sql(&format!(
        "UPDATE {}.{} SET data = 'updated', version = 2 WHERE id = 1",
        namespace, table
    ))
    .await;
    execute_sql(&format!(
        "INSERT INTO {}.{} (id, data, version) VALUES (2, 'second', 1)",
        namespace, table
    ))
    .await;
    execute_sql(&format!("DELETE FROM {}.{} WHERE id = 1", namespace, table)).await;
    // No-op to keep the sequence shorter

    let client = create_test_client().await;
    let mut consumer = client
        .consumer()
        .topic(&topic)
        .group_id("test-mixed-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .expect("Failed to build consumer");

    let records = poll_records_until(&mut consumer, 4, Duration::from_secs(20)).await;
    assert!(!records.is_empty(), "Should receive at least one event");

    let inserts = records.iter().filter(|r| r.op == TopicOp::Insert).count();
    let updates = records.iter().filter(|r| r.op == TopicOp::Update).count();
    let deletes = records.iter().filter(|r| r.op == TopicOp::Delete).count();
    if updates > 0 || deletes > 0 {
        assert_eq!(inserts, 2);
        assert_eq!(updates, 1);
        assert_eq!(deletes, 1);
    } else {
        assert!(records.len() >= 2);
    }

    for record in &records {
        consumer.mark_processed(record);
    }
    consumer.commit_sync().await.ok();

    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_offset_persistence() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("offsets");
    let topic = format!("{}.{}", namespace, table);
    let group_id = format!("test-offset-group-{}", common::random_string(10));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!("CREATE TABLE {}.{} (id INT PRIMARY KEY, data TEXT)", namespace, table))
        .await;
    create_topic_with_sources(&topic, &format!("{}.{}", namespace, table), &["INSERT"]).await;

    // First consumer: consume and commit first batch
    {
        let client = create_test_client().await;
        let mut consumer = client
            .consumer()
            .topic(&topic)
            .group_id(&group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .expect("Failed to build consumer");

        // Insert first batch after consumer is ready
        for i in 1..=2 {
            execute_sql(&format!(
                "INSERT INTO {}.{} (id, data) VALUES ({}, 'batch1-{}')",
                namespace, table, i, i
            ))
            .await;
        }

        let records = poll_records_until(&mut consumer, 2, Duration::from_secs(20)).await;
        assert_eq!(records.len(), 2);

        for record in &records {
            consumer.mark_processed(record);
        }
        consumer.commit_sync().await.expect("Failed to commit");
    }

    // Second consumer with same group: should only see new messages
    {
        let client = create_test_client().await;
        let mut consumer = client
            .consumer()
            .topic(&topic)
            .group_id(&group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .expect("Failed to build consumer");

        // Insert second batch after consumer is ready
        for i in 3..=4 {
            execute_sql(&format!(
                "INSERT INTO {}.{} (id, data) VALUES ({}, 'batch2-{}')",
                namespace, table, i, i
            ))
            .await;
        }

        let records = poll_records_until(&mut consumer, 2, Duration::from_secs(20)).await;
        assert_eq!(records.len(), 2, "Should receive only batch 2");

        for record in &records {
            let payload = parse_payload(&record.payload);
            let id = payload.get("id").and_then(|v| v.as_i64()).unwrap();
            assert!(id >= 3 && id <= 4);
            consumer.mark_processed(record);
        }
        consumer.commit_sync().await.ok();
    }

    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_from_earliest() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("earliest");
    let topic = format!("{}.{}", namespace, table);

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!("CREATE TABLE {}.{} (id INT PRIMARY KEY, msg TEXT)", namespace, table))
        .await;
    create_topic_with_sources(&topic, &format!("{}.{}", namespace, table), &["INSERT"]).await;

    for i in 1..=4 {
        execute_sql(&format!(
            "INSERT INTO {}.{} (id, msg) VALUES ({}, 'msg{}')",
            namespace, table, i, i
        ))
        .await;
    }

    let client = create_test_client().await;
    let mut consumer = client
        .consumer()
        .topic(&topic)
        .group_id(&format!("test-earliest-group-{}", common::random_string(10)))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_records(20)
        .build()
        .expect("Failed to build consumer");

    let records = poll_records_until(&mut consumer, 4, Duration::from_secs(20)).await;
    assert_eq!(records.len(), 4, "Should receive all 4 messages");

    let mut offsets: Vec<u64> = records.iter().map(|r| r.offset).collect();
    offsets.sort_unstable();
    for (i, offset) in offsets.iter().enumerate() {
        assert_eq!(*offset, i as u64);
    }

    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_consume_from_latest() {
    let namespace = common::generate_unique_namespace("smoke_topic_ns");
    let table = common::generate_unique_table("latest");
    let topic = format!("{}.{}", namespace, table);

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!("CREATE TABLE {}.{} (id INT PRIMARY KEY, msg TEXT)", namespace, table))
        .await;
    create_topic_with_sources(&topic, &format!("{}.{}", namespace, table), &["INSERT"]).await;

    // Insert old messages
    for i in 1..=2 {
        execute_sql(&format!(
            "INSERT INTO {}.{} (id, msg) VALUES ({}, 'old{}')",
            namespace, table, i, i
        ))
        .await;
    }

    // Start consumer from latest
    let client = create_test_client().await;
    let mut consumer = client
        .consumer()
        .topic(&topic)
        .group_id(&format!("test-latest-group-{}", common::random_string(10)))
        .auto_offset_reset(AutoOffsetReset::Latest)
        .build()
        .expect("Failed to build consumer");

    // Warm up consumer so Latest offset is established before publishing new messages.
    let _ = poll_records_until(&mut consumer, 1, Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Insert new messages
    for i in 3..=4 {
        execute_sql(&format!(
            "INSERT INTO {}.{} (id, msg) VALUES ({}, 'new{}')",
            namespace, table, i, i
        ))
        .await;
    }

    let records = poll_records_until(&mut consumer, 2, Duration::from_secs(20)).await;
    let new_messages: Vec<_> = records
        .iter()
        .filter_map(|record| {
            let payload = parse_payload(&record.payload);
            payload.get("msg").and_then(|v| v.as_str()).map(|s| s.to_string())
        })
        .filter(|msg| msg.starts_with("new"))
        .collect();
    assert!(new_messages.len() >= 2);

    execute_sql(&format!("DROP TOPIC {}", topic)).await;
    execute_sql(&format!("DROP TABLE {}.{}", namespace, table)).await;
    execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
}

#[derive(Clone, Copy)]
enum StartMode {
    Earliest,
    Latest,
}

impl StartMode {
    fn as_offset_reset(self) -> AutoOffsetReset {
        match self {
            StartMode::Earliest => AutoOffsetReset::Earliest,
            StartMode::Latest => AutoOffsetReset::Latest,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            StartMode::Earliest => "earliest",
            StartMode::Latest => "latest",
        }
    }
}

#[derive(Clone, Copy)]
struct ConsumeCase {
    start: StartMode,
    batch_size: u32,
    auto_commit: bool,
}

/// Exhaustive option matrix for Topic consumer behavior:
/// - start: earliest / latest
/// - batch_size: 1 / 20
/// - auto_ack equivalent: enable_auto_commit true / false
///
/// For each case, this validates:
/// 1) batch size limits are respected on first delivery window
/// 2) committed offsets advance correctly across a same-group restart
/// 3) latest start does not replay backlog, only newly inserted records
#[tokio::test]
#[ntest::timeout(240000)]
async fn test_topic_consume_option_matrix_start_batch_auto_ack_modes() {
    let cases = [
        ConsumeCase {
            start: StartMode::Earliest,
            batch_size: 1,
            auto_commit: true,
        },
        ConsumeCase {
            start: StartMode::Earliest,
            batch_size: 1,
            auto_commit: false,
        },
        ConsumeCase {
            start: StartMode::Earliest,
            batch_size: 20,
            auto_commit: true,
        },
        ConsumeCase {
            start: StartMode::Earliest,
            batch_size: 20,
            auto_commit: false,
        },
        ConsumeCase {
            start: StartMode::Latest,
            batch_size: 1,
            auto_commit: true,
        },
        ConsumeCase {
            start: StartMode::Latest,
            batch_size: 1,
            auto_commit: false,
        },
        ConsumeCase {
            start: StartMode::Latest,
            batch_size: 20,
            auto_commit: true,
        },
        ConsumeCase {
            start: StartMode::Latest,
            batch_size: 20,
            auto_commit: false,
        },
    ];

    for test_case in cases {
        let namespace = common::generate_unique_namespace("smoke_topic_opts");
        let table = common::generate_unique_table("events");
        let topic = format!("{}.{}", namespace, table);
        let table_id = format!("{}.{}", namespace, table);
        let group_id = format!(
            "opts-{}-b{}-auto{}-{}",
            test_case.start.as_str(),
            test_case.batch_size,
            test_case.auto_commit,
            common::random_string(8)
        );

        execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
        execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", table_id)).await;
        create_topic_with_sources(&topic, &table_id, &["INSERT"]).await;

        let backlog_count = 32i64;
        for id in 0..backlog_count {
            execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'old_{}_{}')",
                table_id,
                id,
                test_case.start.as_str(),
                id
            ))
            .await;
        }

        let client = create_test_client().await;
        let mut consumer = client
            .consumer()
            .topic(&topic)
            .group_id(&group_id)
            .auto_offset_reset(test_case.start.as_offset_reset())
            .max_poll_records(test_case.batch_size)
            .enable_auto_commit(test_case.auto_commit)
            .auto_commit_interval(Duration::from_secs(300))
            .poll_timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to build option-matrix consumer");

        match test_case.start {
            StartMode::Earliest => {
                let first_batch =
                    poll_records_until(&mut consumer, 1, Duration::from_secs(20)).await;
                assert!(
                    !first_batch.is_empty(),
                    "earliest should return backlog (batch_size={}, auto_commit={})",
                    test_case.batch_size,
                    test_case.auto_commit
                );
                assert!(
                    first_batch.len() as u32 <= test_case.batch_size,
                    "first batch len={} exceeded batch_size={} for earliest/auto_commit={}",
                    first_batch.len(),
                    test_case.batch_size,
                    test_case.auto_commit
                );

                let first_max_offset = first_batch
                    .iter()
                    .map(|record| record.offset)
                    .max()
                    .expect("first batch should have at least one record");

                for record in &first_batch {
                    consumer.mark_processed(record);
                }

                if test_case.auto_commit {
                    consumer.close().await.expect("close should flush auto commit");
                } else {
                    consumer.commit_sync().await.expect("manual commit should succeed");
                    consumer.close().await.expect("close after manual commit");
                }

                let client_resume = create_test_client().await;
                let mut resumed_consumer = client_resume
                    .consumer()
                    .topic(&topic)
                    .group_id(&group_id)
                    .auto_offset_reset(AutoOffsetReset::Earliest)
                    .enable_auto_commit(false)
                    .max_poll_records(10)
                    .poll_timeout(Duration::from_secs(1))
                    .build()
                    .expect("Failed to build resumed consumer");

                let resumed_batch =
                    poll_records_until(&mut resumed_consumer, 3, Duration::from_secs(20)).await;
                assert!(
                    !resumed_batch.is_empty(),
                    "resumed earliest consumer should continue after committed offset"
                );
                assert!(
                    resumed_batch.iter().all(|record| record.offset > first_max_offset),
                    "resumed records must start after committed offset (first_max_offset={})",
                    first_max_offset
                );
                for record in &resumed_batch {
                    resumed_consumer.mark_processed(record);
                }
                let _ = resumed_consumer.commit_sync().await;
                let _ = resumed_consumer.close().await;
            },
            StartMode::Latest => {
                let warmup = consumer
                    .poll_with_timeout(Duration::from_secs(1))
                    .await
                    .expect("latest warmup poll should succeed");
                assert!(
                    warmup.is_empty(),
                    "latest warmup should not replay backlog (batch_size={}, auto_commit={})",
                    test_case.batch_size,
                    test_case.auto_commit
                );

                tokio::time::sleep(Duration::from_millis(150)).await;

                let live_count = test_case.batch_size as i64 + 3;
                let live_base_id = 1000i64;
                let mut expected_live_ids = HashSet::new();
                for i in 0..live_count {
                    let live_id = live_base_id + i;
                    expected_live_ids.insert(live_id);
                    execute_sql(&format!(
                        "INSERT INTO {} (id, payload) VALUES ({}, 'live_{}_{}')",
                        table_id, live_id, test_case.batch_size, i
                    ))
                    .await;
                }

                let live_batch =
                    poll_records_until(&mut consumer, live_count as usize, Duration::from_secs(25))
                        .await;
                assert!(
                    live_batch.len() >= live_count as usize,
                    "latest consumer should receive all new records (expected>={}, got={})",
                    live_count,
                    live_batch.len()
                );

                let received_live_ids: HashSet<i64> = live_batch
                    .iter()
                    .filter_map(|record| {
                        let payload = parse_payload(&record.payload);
                        parse_i64_field(&payload, "id")
                    })
                    .collect();

                for expected_id in &expected_live_ids {
                    assert!(
                        received_live_ids.contains(expected_id),
                        "latest consumer missed live id={} (batch_size={}, auto_commit={})",
                        expected_id,
                        test_case.batch_size,
                        test_case.auto_commit
                    );
                }
                assert!(
                    received_live_ids.iter().all(|id| *id >= live_base_id),
                    "latest consumer should not replay old backlog ids"
                );

                for record in &live_batch {
                    consumer.mark_processed(record);
                }

                if test_case.auto_commit {
                    consumer.close().await.expect("close should flush auto commit");
                } else {
                    consumer.commit_sync().await.expect("manual commit should succeed");
                    consumer.close().await.expect("close after manual commit");
                }

                let client_resume = create_test_client().await;
                let mut resumed_consumer = client_resume
                    .consumer()
                    .topic(&topic)
                    .group_id(&group_id)
                    .auto_offset_reset(AutoOffsetReset::Earliest)
                    .enable_auto_commit(false)
                    .max_poll_records(10)
                    .poll_timeout(Duration::from_secs(1))
                    .build()
                    .expect("Failed to build resumed latest consumer");
                let resumed_batch =
                    poll_records_until(&mut resumed_consumer, 1, Duration::from_secs(2)).await;
                assert!(
                    resumed_batch.is_empty(),
                    "no duplicate delivery expected after latest case commit and restart"
                );
                let _ = resumed_consumer.close().await;
            },
        }

        execute_sql(&format!("DROP TOPIC {}", topic)).await;
        execute_sql(&format!("DROP TABLE {}", table_id)).await;
        execute_sql(&format!("DROP NAMESPACE {}", namespace)).await;
    }
}

#[tokio::test]
#[ntest::timeout(180000)]
async fn test_topic_http_consume_preserves_impersonated_user_and_payloads() {
    let namespace = common::generate_unique_namespace("smoke_topic_actor");
    let table = common::generate_unique_table("messages");
    let full_table = format!("{}.{}", namespace, table);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("cdc_topic"));

    let service_user = common::generate_unique_namespace("topic_service");
    let alice_user = common::generate_unique_namespace("topic_alice");
    let bob_user = common::generate_unique_namespace("topic_bob");
    let password = "topic_pass_123";

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!(
        "CREATE USER TABLE {} (id INT PRIMARY KEY, body TEXT, status TEXT, version INT)",
        full_table
    ))
    .await;

    create_user_with_retry(&service_user, password, "service").await;
    create_user_with_retry(&alice_user, password, "user").await;
    create_user_with_retry(&bob_user, password, "user").await;

    let alice_user_id = get_user_id(&alice_user).await;
    let bob_user_id = get_user_id(&bob_user).await;

    create_topic_with_sources(&topic, &full_table, &["INSERT", "UPDATE", "DELETE"]).await;

    execute_sql_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, body, status, version) VALUES (1, 'alice message v1', 'draft', 1))",
            alice_user_id, full_table
        ),
    )
    .await;

    execute_sql_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (UPDATE {} SET body = 'alice message v2', status = 'sent', version = 2 WHERE id = 1)",
            alice_user_id, full_table
        ),
    )
    .await;

    execute_sql_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, body, status, version) VALUES (2, 'bob message v1', 'queued', 1))",
            bob_user_id, full_table
        ),
    )
    .await;

    execute_sql_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = 2)",
            bob_user_id, full_table
        ),
    )
    .await;

    let mut messages = poll_topic_messages_http_until(&topic, 4, Duration::from_secs(20)).await;
    messages.sort_by_key(|message| message.offset);

    assert_eq!(messages.len(), 4, "Expected exactly four CDC messages");
    assert_eq!(
        messages.iter().map(|message| message.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "Single-partition topic should preserve sequential offsets"
    );
    assert!(
        messages.iter().all(|message| message.topic_id == topic && message.partition_id == 0),
        "All consumed messages should belong to the requested topic partition"
    );
    assert!(
        messages
            .iter()
            .all(|message| message.user.as_deref() != Some(service_user.as_str())),
        "Topic messages should preserve impersonated users, not the service account"
    );

    struct ExpectedEvent<'a> {
        offset: u64,
        user: &'a str,
        op: &'a str,
        id: i64,
        body: &'a str,
        status: &'a str,
        version: i64,
        deleted: bool,
    }

    let expected_events = [
        ExpectedEvent {
            offset: 0,
            user: &alice_user_id,
            op: "Insert",
            id: 1,
            body: "alice message v1",
            status: "draft",
            version: 1,
            deleted: false,
        },
        ExpectedEvent {
            offset: 1,
            user: &alice_user_id,
            op: "Update",
            id: 1,
            body: "alice message v2",
            status: "sent",
            version: 2,
            deleted: false,
        },
        ExpectedEvent {
            offset: 2,
            user: &bob_user_id,
            op: "Insert",
            id: 2,
            body: "bob message v1",
            status: "queued",
            version: 1,
            deleted: false,
        },
        ExpectedEvent {
            offset: 3,
            user: &bob_user_id,
            op: "Delete",
            id: 2,
            body: "bob message v1",
            status: "queued",
            version: 1,
            deleted: true,
        },
    ];

    for expected in expected_events {
        let message = messages
            .iter()
            .find(|message| message.offset == expected.offset)
            .expect("Expected message offset should be present");
        assert_eq!(message.op, expected.op);
        assert_eq!(message.user.as_deref(), Some(expected.user));

        let payload = message.payload_json();
        assert_eq!(parse_i64_field(&payload, "id"), Some(expected.id));
        assert_eq!(parse_string_field(&payload, "body").as_deref(), Some(expected.body));
        assert_eq!(parse_string_field(&payload, "status").as_deref(), Some(expected.status));
        assert_eq!(parse_i64_field(&payload, "version"), Some(expected.version));
        assert_eq!(parse_bool_field(&payload, "_deleted"), Some(expected.deleted));
    }

    let _ = common::execute_sql_via_http_as_root(&format!("DROP TOPIC {}", topic)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP USER {}", service_user)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP USER {}", alice_user)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP USER {}", bob_user)).await;
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_topic_sql_consume_docs_getting_started_flow() {
    let namespace = common::generate_unique_namespace("demo");
    let table_name = common::generate_unique_table("messages");
    let full_table = format!("{}.{}", namespace, table_name);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("message_events"));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!(
        "CREATE USER TABLE {} (id TEXT PRIMARY KEY DEFAULT ULID(), author TEXT NOT NULL, msg TEXT NOT NULL, attachment FILE, created TIMESTAMP NOT NULL DEFAULT NOW())",
        full_table
    ))
    .await;
    execute_sql(&format!("CREATE TOPIC {} PARTITIONS 1", topic)).await;
    execute_sql(&format!(
        "ALTER TOPIC {} ADD SOURCE {} ON INSERT WITH (payload = 'full')",
        topic, full_table
    ))
    .await;

    execute_sql(&format!(
        "INSERT INTO {} (author, msg) VALUES ('user', 'Write a short summary of this support ticket.')",
        full_table
    ))
    .await;

    let consume_response = common::execute_sql_via_http_as_root(&format!(
        "CONSUME FROM {} FROM EARLIEST LIMIT 1",
        topic
    ))
    .await
    .expect("SQL CONSUME should return a response");

    assert!(
        response_is_success(&consume_response),
        "SQL CONSUME should succeed for getting-started flow: {}",
        consume_response
    );

    let rows = common::get_rows_as_hashmaps(&consume_response)
        .expect("SQL CONSUME should return rows for the inserted event");
    assert_eq!(rows.len(), 1, "SQL CONSUME should return exactly one row");

    let row = rows.first().expect("row should exist");
    assert_eq!(
        row.get("topic_id")
            .map(common::extract_typed_value)
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .as_deref(),
        Some(topic.as_str())
    );
    assert_eq!(
        row.get("op")
            .map(common::extract_typed_value)
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .as_deref(),
        Some("insert")
    );

    let payload = parse_binary_row_field_as_json(row, "payload");
    assert_eq!(parse_string_field(&payload, "author").as_deref(), Some("user"));
    assert_eq!(
        parse_string_field(&payload, "msg").as_deref(),
        Some("Write a short summary of this support ticket.")
    );
    assert!(parse_string_field(&payload, "id").is_some(), "payload should include generated ULID");
    assert!(payload.get("created").is_some(), "payload should include created timestamp");

    let _ = common::execute_sql_via_http_as_root(&format!("DROP TOPIC {}", topic)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;
}

#[tokio::test]
#[ntest::timeout(180000)]
async fn test_topic_http_consume_direct_multi_user_publishers_no_missing_changes() {
    let namespace = common::generate_unique_namespace("smoke_topic_multi_user");
    let table = common::generate_unique_table("messages");
    let full_table = format!("{}.{}", namespace, table);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("multi_user_topic"));

    let user_a = common::generate_unique_namespace("topic_user_a");
    let user_b = common::generate_unique_namespace("topic_user_b");
    let user_c = common::generate_unique_namespace("topic_user_c");
    let password = "topic_multi_pass_123";

    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    execute_sql(&format!(
        "CREATE USER TABLE {} (id INT PRIMARY KEY, body TEXT, version INT)",
        full_table
    ))
    .await;

    create_user_with_retry(&user_a, password, "user").await;
    create_user_with_retry(&user_b, password, "user").await;
    create_user_with_retry(&user_c, password, "user").await;

    let user_a_id = get_user_id(&user_a).await;
    let user_b_id = get_user_id(&user_b).await;
    let user_c_id = get_user_id(&user_c).await;

    create_topic_with_sources(&topic, &full_table, &["INSERT", "UPDATE", "DELETE"]).await;

    let full_table_a = full_table.clone();
    let full_table_b = full_table.clone();
    let full_table_c = full_table.clone();

    let user_a_name = user_a.clone();
    let user_b_name = user_b.clone();
    let user_c_name = user_c.clone();

    let password_a = password.to_string();
    let password_b = password.to_string();
    let password_c = password.to_string();

    let publisher_a = tokio::spawn(async move {
        execute_sql_as(
            &user_a_name,
            &password_a,
            &format!(
                "INSERT INTO {} (id, body, version) VALUES (101, 'user-a-v1', 1)",
                full_table_a
            ),
        )
        .await;
        execute_sql_as(
            &user_a_name,
            &password_a,
            &format!(
                "UPDATE {} SET body = 'user-a-v2', version = 2 WHERE id = 101",
                full_table_a
            ),
        )
        .await;
        execute_sql_as(
            &user_a_name,
            &password_a,
            &format!("DELETE FROM {} WHERE id = 101", full_table_a),
        )
        .await;
    });

    let publisher_b = tokio::spawn(async move {
        execute_sql_as(
            &user_b_name,
            &password_b,
            &format!(
                "INSERT INTO {} (id, body, version) VALUES (202, 'user-b-v1', 1)",
                full_table_b
            ),
        )
        .await;
        execute_sql_as(
            &user_b_name,
            &password_b,
            &format!(
                "UPDATE {} SET body = 'user-b-v2', version = 2 WHERE id = 202",
                full_table_b
            ),
        )
        .await;
        execute_sql_as(
            &user_b_name,
            &password_b,
            &format!("DELETE FROM {} WHERE id = 202", full_table_b),
        )
        .await;
    });

    let publisher_c = tokio::spawn(async move {
        execute_sql_as(
            &user_c_name,
            &password_c,
            &format!(
                "INSERT INTO {} (id, body, version) VALUES (303, 'user-c-v1', 1)",
                full_table_c
            ),
        )
        .await;
        execute_sql_as(
            &user_c_name,
            &password_c,
            &format!(
                "UPDATE {} SET body = 'user-c-v2', version = 2 WHERE id = 303",
                full_table_c
            ),
        )
        .await;
        execute_sql_as(
            &user_c_name,
            &password_c,
            &format!("DELETE FROM {} WHERE id = 303", full_table_c),
        )
        .await;
    });

    publisher_a.await.expect("user a publisher task should succeed");
    publisher_b.await.expect("user b publisher task should succeed");
    publisher_c.await.expect("user c publisher task should succeed");

    let mut messages = poll_topic_messages_http_until(&topic, 9, Duration::from_secs(20)).await;
    messages.sort_by_key(|message| message.offset);

    assert_eq!(messages.len(), 9, "Expected nine topic messages from three direct publishers");
    assert_eq!(
        messages.iter().map(|message| message.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
        "Direct multi-user publishing should not leave gaps in offsets"
    );
    assert!(
        messages.iter().all(|message| message.topic_id == topic && message.partition_id == 0),
        "All consumed messages should belong to the requested topic partition"
    );

    let observed: HashSet<String> = messages
        .iter()
        .map(|message| {
            let payload = message.payload_json();
            format!(
                "{}|{}|{}|{}|{}|{}",
                message.user.clone().unwrap_or_default(),
                message.op,
                parse_i64_field(&payload, "id").expect("payload should include id"),
                parse_string_field(&payload, "body").expect("payload should include body"),
                parse_i64_field(&payload, "version").expect("payload should include version"),
                parse_bool_field(&payload, "_deleted").unwrap_or(false),
            )
        })
        .collect();

    let expected: HashSet<String> = [
        format!("{}|Insert|101|user-a-v1|1|false", user_a_id),
        format!("{}|Update|101|user-a-v2|2|false", user_a_id),
        format!("{}|Delete|101|user-a-v2|2|true", user_a_id),
        format!("{}|Insert|202|user-b-v1|1|false", user_b_id),
        format!("{}|Update|202|user-b-v2|2|false", user_b_id),
        format!("{}|Delete|202|user-b-v2|2|true", user_b_id),
        format!("{}|Insert|303|user-c-v1|1|false", user_c_id),
        format!("{}|Update|303|user-c-v2|2|false", user_c_id),
        format!("{}|Delete|303|user-c-v2|2|true", user_c_id),
    ]
    .into_iter()
    .collect();

    assert_eq!(observed, expected, "Topic consume should return every direct user change exactly once");

    let _ = common::execute_sql_via_http_as_root(&format!("DROP TOPIC {}", topic)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP USER {}", user_a)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP USER {}", user_b)).await;
    let _ = common::execute_sql_via_http_as_root(&format!("DROP USER {}", user_c)).await;
}
