//! Smoke tests for topic consumption (consume + ack) with CDC events
//!
//! Tests comprehensive topic consumption scenarios including:
//! - INSERT/UPDATE/DELETE operations on tables
//! - Consumer groups and offset tracking
//! - Starting from earliest/latest offsets
//!
//! **Requirements**: Running KalamDB server with Topics feature enabled

use crate::common;
use kalam_client::consumer::{AutoOffsetReset, ConsumerRecord, TopicOp};
use kalam_client::KalamLinkTimeouts;
use std::collections::HashSet;
use std::time::Duration;

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
                    tokio::time::sleep(Duration::from_millis(120)).await;
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
                    tokio::time::sleep(Duration::from_millis(120)).await;
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

fn parse_i64_field(payload: &serde_json::Value, key: &str) -> Option<i64> {
    let raw = payload.get(key)?;
    let untyped = common::extract_typed_value(raw);
    if let Some(value) = untyped.as_i64() {
        return Some(value);
    }
    untyped.as_str().and_then(|value| value.parse::<i64>().ok())
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

    for record in &records {
        let payload = parse_payload(&record.payload);
        assert!(payload.get("id").is_some());
        consumer.mark_processed(record);
    }

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
            payload.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false)
        })
        .count();
    assert!(deletes_by_op.max(deletes_by_payload) >= 1);

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
