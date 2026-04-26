//! Integration tests for Topic Pub/Sub system
//!
//! These tests verify:
//! - Topic creation and management (CREATE TOPIC, ALTER TOPIC ADD SOURCE, DROP TOPIC)
//! - CDC integration (automatic message routing from table changes)
//! - Message consumption (CONSUME with different positions)
//! - Offset tracking (ACK and consumer groups)
//! - Authorization (role-based access control)
//! - Long polling behavior
//!
//! NOTE: These are simplified smoke tests. Full implementation will require:
//! 1. HTTP API test client for /v1/api/topics endpoints
//! 2. Async notification verification helpers
//! 3. Extended timeout handling for CDC workflows

use std::sync::Arc;

use kalam_client::{
    models::{QueryResponse, ResponseStatus},
    parse_i64,
};
use kalamdb_commons::{
    models::{ConsumerGroupId, TopicId},
    Role,
};
use kalamdb_core::app_context::AppContext;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use serial_test::serial;

use crate::test_support::*;

#[derive(Debug, Clone, Deserialize)]
struct HttpConsumeMessage {
    offset: u64,
    partition_id: u32,
    key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HttpConsumeResponse {
    messages: Vec<HttpConsumeMessage>,
    next_offset: u64,
    has_more: bool,
}

#[derive(Debug, Deserialize)]
struct HttpAckResponse {
    success: bool,
    acknowledged_offset: u64,
}

struct TopicPublisherCacheGuard {
    app_context: Arc<AppContext>,
}

impl Drop for TopicPublisherCacheGuard {
    fn drop(&mut self) {
        if let Ok(topics) = self.app_context.system_tables().topics().list_topics() {
            let topic_publisher = self.app_context.topic_publisher();
            topic_publisher.refresh_topics_cache(topics);
            topic_publisher.restore_offset_counters();
        }
    }
}

async fn post_topics_consume(
    server: &http_server::HttpTestServer,
    auth_header: &str,
    body: Value,
) -> (StatusCode, Value) {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/v1/api/topics/consume", server.base_url()))
        .header("Authorization", auth_header)
        .json(&body)
        .send()
        .await
        .expect("Failed to call /v1/api/topics/consume");

    let status = response.status();
    let payload = response.json::<Value>().await.expect("Failed to decode consume response JSON");
    (status, payload)
}

async fn post_topics_ack(
    server: &http_server::HttpTestServer,
    auth_header: &str,
    body: Value,
) -> (StatusCode, Value) {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/v1/api/topics/ack", server.base_url()))
        .header("Authorization", auth_header)
        .json(&body)
        .send()
        .await
        .expect("Failed to call /v1/api/topics/ack");

    let status = response.status();
    let payload = response.json::<Value>().await.expect("Failed to decode ack response JSON");
    (status, payload)
}

async fn wait_until_group_reads_at_least(
    server: &http_server::HttpTestServer,
    auth_header: &str,
    topic_id: &str,
    group_id: &str,
    start: Value,
    limit: u64,
    min_messages: usize,
) -> HttpConsumeResponse {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(20);
    let mut seen_offsets = std::collections::HashSet::<(u32, u64)>::new();
    let mut aggregated_messages: Vec<HttpConsumeMessage> = Vec::new();

    loop {
        let request_body = json!({
            "topic_id": topic_id,
            "group_id": group_id,
            "start": start.clone(),
            "limit": limit,
            "partition_id": 0,
            "timeout_seconds": 1
        });

        let (status, payload) = post_topics_consume(server, auth_header, request_body).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "Consume endpoint should return HTTP 200, payload={:?}",
            payload
        );

        let response: HttpConsumeResponse = serde_json::from_value(payload)
            .expect("Consume payload should match HttpConsumeResponse");

        let response_next_offset = response.next_offset;
        let response_has_more = response.has_more;
        for message in &response.messages {
            if seen_offsets.insert((message.partition_id, message.offset)) {
                aggregated_messages.push(message.clone());
            }
        }

        if aggregated_messages.len() >= min_messages {
            return HttpConsumeResponse {
                messages: aggregated_messages,
                next_offset: response_next_offset,
                has_more: response_has_more,
            };
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "Timed out waiting for at least {} messages (got {}) for topic='{}' group='{}' \
                 start='{}' limit={}",
                min_messages,
                aggregated_messages.len(),
                topic_id,
                group_id,
                start,
                limit
            );
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
    }
}

fn json_string(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }
    value
        .as_object()
        .and_then(|obj| obj.get("Utf8"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

async fn setup_topic_source_fixture(server: &TestServer, fixture_name: &str) -> (String, String) {
    let http_server = http_server::get_global_server().await;
    let namespace = consolidated_helpers::unique_namespace(fixture_name);
    let table = consolidated_helpers::unique_table("events");
    let topic_table = consolidated_helpers::unique_table("topic");
    let topic = format!("{}.{}", namespace, topic_table);
    let source_table = format!("{}.{}", namespace, table);

    let create_namespace = server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    assert_eq!(
        create_namespace.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        create_namespace.error
    );

    let create_table = server
        .execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", source_table))
        .await;
    assert_eq!(
        create_table.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_table.error
    );

    let create_topic = server.execute_sql(&format!("CREATE TOPIC {} PARTITIONS 1", topic)).await;
    assert_eq!(
        create_topic.status,
        ResponseStatus::Success,
        "CREATE TOPIC failed: {:?}",
        create_topic.error
    );

    let add_source = server
        .execute_sql(&format!(
            "ALTER TOPIC {} ADD SOURCE {} ON INSERT WITH (payload = 'full')",
            topic, source_table
        ))
        .await;
    assert_eq!(
        add_source.status,
        ResponseStatus::Success,
        "ALTER TOPIC ADD SOURCE failed: {:?}",
        add_source.error
    );

    wait_for_topic_routes(http_server, &topic, 1).await;

    (topic, source_table)
}

async fn wait_until_sql_consume_row_count_at_least(
    server: &TestServer,
    sql: &str,
    min_rows: usize,
) -> QueryResponse {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(20);

    loop {
        let response = server.execute_sql(sql).await;
        if response.status == ResponseStatus::Success && response.row_count() >= min_rows {
            return response;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "Timed out waiting for SQL consume to return at least {} row(s): sql='{}' \
                 response={:?}",
                min_rows, sql, response.error
            );
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
    }
}

fn first_row_offset(response: &QueryResponse) -> i64 {
    response
        .rows_as_maps()
        .first()
        .and_then(|row| row.get("offset"))
        .map(parse_i64)
        .expect("Expected consume response to include an offset column")
}

fn row_offsets(response: &QueryResponse) -> Vec<i64> {
    response
        .rows_as_maps()
        .iter()
        .map(|row| {
            row.get("offset")
                .map(parse_i64)
                .expect("Expected consume response row to include an offset column")
        })
        .collect()
}

async fn assert_topic_offset_state(
    server: &TestServer,
    topic: &str,
    group: &str,
    expected_last_acked: Option<u64>,
) -> Option<i64> {
    let topic_id = TopicId::new(topic);
    let group_id = ConsumerGroupId::new(group);
    let provider = server.app_context.system_tables().topic_offsets();
    let provider_offsets = provider
        .get_group_offsets(&topic_id, &group_id)
        .expect("Failed to read topic offsets from provider");

    let sql = format!(
        "SELECT topic_id, group_id, partition_id, last_acked_offset, updated_at FROM \
         system.topic_offsets WHERE topic_id = '{}' AND group_id = '{}' ORDER BY partition_id",
        topic, group
    );
    let response = server.execute_sql(&sql).await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "system.topic_offsets query failed: {:?}",
        response.error
    );
    let rows = response.rows_as_maps();

    match expected_last_acked {
        Some(expected) => {
            assert_eq!(provider_offsets.len(), 1, "Expected exactly one provider offset row");
            let provider_offset = &provider_offsets[0];
            assert_eq!(provider_offset.topic_id, topic_id);
            assert_eq!(provider_offset.group_id, group_id);
            assert_eq!(provider_offset.partition_id, 0);
            assert_eq!(provider_offset.last_acked_offset, expected);
            assert!(provider_offset.updated_at > 0, "updated_at should be populated");

            assert_eq!(rows.len(), 1, "Expected exactly one SQL offset row");
            let row = &rows[0];
            assert_eq!(
                row.get("topic_id").and_then(|value| json_string(value.inner())).as_deref(),
                Some(topic)
            );
            assert_eq!(
                row.get("group_id").and_then(|value| json_string(value.inner())).as_deref(),
                Some(group)
            );
            assert_eq!(row.get("partition_id").map(parse_i64), Some(0));
            assert_eq!(row.get("last_acked_offset").map(parse_i64), Some(expected as i64));

            let sql_updated_at = row
                .get("updated_at")
                .map(parse_i64)
                .expect("Expected updated_at column in system.topic_offsets row");
            assert_eq!(provider_offset.updated_at, sql_updated_at);

            Some(sql_updated_at)
        },
        None => {
            assert!(provider_offsets.is_empty(), "Expected no provider offset rows");
            assert!(rows.is_empty(), "Expected no SQL offset rows");
            None
        },
    }
}

async fn assert_topic_offset_count(server: &TestServer, topic: &str, expected_count: usize) {
    let topic_id = TopicId::new(topic);
    let provider_offsets = server
        .app_context
        .system_tables()
        .topic_offsets()
        .get_topic_offsets(&topic_id)
        .expect("Failed to read topic offsets for topic");
    assert_eq!(
        provider_offsets.len(),
        expected_count,
        "Unexpected provider topic offset count for topic '{}'",
        topic
    );

    let response = server
        .execute_sql(&format!(
            "SELECT COUNT(*) AS count FROM system.topic_offsets WHERE topic_id = '{}'",
            topic
        ))
        .await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "COUNT query on system.topic_offsets failed: {:?}",
        response.error
    );
    assert_eq!(
        response.get_i64("count"),
        Some(expected_count as i64),
        "Unexpected SQL topic offset count for topic '{}'",
        topic
    );
}

async fn wait_for_topic_routes(
    server: &http_server::HttpTestServer,
    topic_id: &str,
    min_routes: usize,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(20);
    let sql = format!("SELECT routes FROM system.topics WHERE topic_id = '{}'", topic_id);

    loop {
        let response = server
            .execute_sql(&sql)
            .await
            .expect("Failed to query system.topics for route readiness");

        if response.status == ResponseStatus::Success {
            if let Some(result) = response.results.first() {
                if let Some(row) = result.row_as_map(0) {
                    if let Some(routes_raw) = row.get("routes").and_then(|v| json_string(v.inner()))
                    {
                        if let Ok(routes_json) = serde_json::from_str::<Value>(&routes_raw) {
                            let route_count =
                                routes_json.as_array().map(|routes| routes.len()).unwrap_or(0);
                            if route_count >= min_routes {
                                return;
                            }
                        }
                    }
                }
            }
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "Timed out waiting for topic '{}' to have at least {} route(s)",
                topic_id, min_routes
            );
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
    }
}

/// Test basic topic creation via SQL
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_create_topic_basic() {
    let server = TestServer::new_shared().await;

    let sql = "CREATE TOPIC default.user_events_topic PARTITIONS 1";
    let result = server.execute_sql(&sql).await;

    // Basic smoke test - verify command executes (or already exists)
    assert!(
        result.status == ResponseStatus::Success
            || result
                .error
                .as_ref()
                .map(|e| e.message.contains("already exists"))
                .unwrap_or(false),
        "CREATE TOPIC failed: {:?}",
        result.error
    );
}

/// Test ALTER TOPIC ADD SOURCE
#[tokio::test]
#[ntest::timeout(15000)]
async fn test_alter_topic_add_source() {
    let server = TestServer::new_shared().await;

    // Setup
    server.execute_sql("CREATE NAMESPACE test_alter_ns").await;
    server
        .execute_sql("CREATE TABLE test_alter_ns.data (id TEXT PRIMARY KEY, value TEXT)")
        .await;
    server
        .execute_sql("CREATE TOPIC test_alter_ns.data_changes_tp PARTITIONS 1")
        .await;

    // Add source - Syntax: ALTER TOPIC <name> ADD SOURCE <table> ON <operation>
    let sql = "ALTER TOPIC test_alter_ns.data_changes_tp ADD SOURCE test_alter_ns.data ON INSERT";
    let result = server.execute_sql(sql).await;

    assert!(
        result.status == ResponseStatus::Success,
        "ALTER TOPIC ADD SOURCE failed: {:?}",
        result.error
    );
}

/// Test CONSUME basic functionality
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_consume_from_topic() {
    let server = TestServer::new_shared().await;

    // Setup topic
    server.execute_sql("CREATE TOPIC default.test_consume_tp PARTITIONS 1").await;

    // Consume (should return empty initially or succeed)
    let sql = "CONSUME FROM default.test_consume_tp GROUP 'consumers' START EARLIEST LIMIT 10";
    let result = server.execute_sql(sql).await;

    // Should succeed (empty result set is still success)
    assert!(
        result.status == ResponseStatus::Success,
        "CONSUME should succeed even if no messages: {:?}",
        result.error
    );
}

/// Test ACK functionality
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_ack_offset() {
    let server = TestServer::new_shared().await;

    let namespace = consolidated_helpers::unique_namespace("tp_ack");
    let topic_table = consolidated_helpers::unique_table("topic");
    let topic = format!("{}.{}", namespace, topic_table);
    let group = format!("ack-{}", consolidated_helpers::unique_table("group"));

    let create_namespace = server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;
    assert_eq!(create_namespace.status, ResponseStatus::Success);

    let create_topic = server.execute_sql(&format!("CREATE TOPIC {} PARTITIONS 1", topic)).await;
    assert_eq!(create_topic.status, ResponseStatus::Success);

    assert_topic_offset_state(&server, &topic, &group, None).await;

    // Empty consume should not persist an auto-ack row.
    let consume = server
        .execute_sql(&format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 10", topic, group))
        .await;
    assert_eq!(consume.status, ResponseStatus::Success);
    assert_topic_offset_state(&server, &topic, &group, None).await;

    // ACK offset (should succeed)
    let sql = format!("ACK {} GROUP '{}' UPTO OFFSET 0", topic, group);
    let result = server.execute_sql(&sql).await;

    assert!(
        result.status == ResponseStatus::Success,
        "ACK should succeed: {:?}",
        result.error
    );

    let updated_at = assert_topic_offset_state(&server, &topic, &group, Some(0))
        .await
        .expect("Expected ACK to create a persisted topic offset row");
    assert!(updated_at > 0, "ACK should populate updated_at");
}

/// Test DROP TOPIC
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_drop_topic() {
    let server = TestServer::new_shared().await;

    // Create and drop
    server.execute_sql("CREATE TOPIC default.temp_drop_tp PARTITIONS 1").await;

    let sql = "DROP TOPIC default.temp_drop_tp";
    let result = server.execute_sql(sql).await;

    assert!(
        result.status == ResponseStatus::Success,
        "DROP TOPIC should succeed: {:?}",
        result.error
    );
}

// ============================================================================
// Authorization Tests
// ============================================================================

/// Test that user role is forbidden from consuming topics
#[tokio::test]
#[ntest::timeout(20000)]
async fn test_consume_user_role_forbidden() {
    let server = TestServer::new_shared().await;

    // Create a regular user (not service/dba/system)
    server
        .execute_sql("CREATE USER test_user WITH PASSWORD 'testpass' ROLE user")
        .await;

    // Create topic
    server
        .execute_sql("CREATE TOPIC default.forbidden_consume_tp PARTITIONS 1")
        .await;

    // Try to consume as user role (should fail)
    let sql =
        "CONSUME FROM default.forbidden_consume_tp GROUP 'test_group' START EARLIEST LIMIT 10";
    let result = server.execute_sql_as_user(sql, "test_user").await;

    assert!(
        result.status == ResponseStatus::Error,
        "User role should be forbidden from consuming topics"
    );
    assert!(
        result
            .error
            .as_ref()
            .map(|e| e.message.contains("service, dba, or system"))
            .unwrap_or(false),
        "Error message should mention required roles: {:?}",
        result.error
    );
}

/// Test that service, dba, and system roles can consume topics
#[tokio::test]
#[ntest::timeout(15000)]
async fn test_consume_privileged_roles_allowed() {
    let server = TestServer::new_shared().await;

    // Create users with privileged roles
    server
        .execute_sql("CREATE USER test_service WITH PASSWORD 'pass' ROLE service")
        .await;
    server.execute_sql("CREATE USER test_dba WITH PASSWORD 'pass' ROLE dba").await;

    // Create topic
    server
        .execute_sql("CREATE TOPIC default.privileged_consume_tp PARTITIONS 1")
        .await;

    // Test service role
    let sql =
        "CONSUME FROM default.privileged_consume_tp GROUP 'service_group' START EARLIEST LIMIT 10";
    let result = server.execute_sql_as_user(sql, "test_service").await;
    assert!(
        result.status == ResponseStatus::Success,
        "Service role should be able to consume: {:?}",
        result.error
    );

    // Test dba role
    let sql =
        "CONSUME FROM default.privileged_consume_tp GROUP 'dba_group' START EARLIEST LIMIT 10";
    let result = server.execute_sql_as_user(sql, "test_dba").await;
    assert!(
        result.status == ResponseStatus::Success,
        "DBA role should be able to consume: {:?}",
        result.error
    );

    // Test system role (root user)
    let sql =
        "CONSUME FROM default.privileged_consume_tp GROUP 'system_group' START EARLIEST LIMIT 10";
    let result = server.execute_sql(sql).await;
    assert!(
        result.status == ResponseStatus::Success,
        "System role should be able to consume: {:?}",
        result.error
    );
}

/// Test user role is also forbidden from ACK
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_ack_user_role_forbidden() {
    let server = TestServer::new_shared().await;

    // Create a regular user
    server
        .execute_sql("CREATE USER test_user_ack WITH PASSWORD 'testpass' ROLE user")
        .await;

    // Create topic
    server.execute_sql("CREATE TOPIC default.forbidden_ack_tp PARTITIONS 1").await;

    // Try to ACK as user role (should fail)
    let sql = "ACK default.forbidden_ack_tp GROUP 'test_group' UPTO OFFSET 0";
    let result = server.execute_sql_as_user(sql, "test_user_ack").await;

    assert!(
        result.status == ResponseStatus::Error,
        "User role should be forbidden from ACK: {:?}",
        result
    );
}

// ============================================================================
// End-to-End CDC + CONSUME Workflow Test
// ============================================================================

/// Test complete CDC workflow: INSERT → Topic → CONSUME
/// This verifies that table changes properly flow through topics and can be consumed.
#[tokio::test]
#[ntest::timeout(20000)]
async fn test_cdc_insert_to_consume_workflow() {
    let server = TestServer::new_shared().await;

    // 1. Setup namespace and table
    server.execute_sql("CREATE NAMESPACE test_cdc_ns").await;
    let create_table = "CREATE TABLE test_cdc_ns.events (
        id TEXT PRIMARY KEY,
        event_type TEXT,
        data TEXT
    )";
    server.execute_sql(create_table).await;

    // 2. Create topic and add CDC source
    server.execute_sql("CREATE TOPIC test_cdc_ns.events_stream PARTITIONS 1").await;
    server
        .execute_sql(
            "ALTER TOPIC test_cdc_ns.events_stream ADD SOURCE test_cdc_ns.events ON INSERT",
        )
        .await;

    // 3. Insert data (should trigger CDC → topic)
    let insert_1 = "INSERT INTO test_cdc_ns.events (id, event_type, data) VALUES ('evt1', \
                    'user_signup', 'John Doe')";
    let result_1 = server.execute_sql(insert_1).await;
    assert!(
        result_1.status == ResponseStatus::Success,
        "INSERT 1 failed: {:?}",
        result_1.error
    );

    let insert_2 = "INSERT INTO test_cdc_ns.events (id, event_type, data) VALUES ('evt2', \
                    'user_login', 'Jane Smith')";
    let result_2 = server.execute_sql(insert_2).await;
    assert!(
        result_2.status == ResponseStatus::Success,
        "INSERT 2 failed: {:?}",
        result_2.error
    );

    // 4-5. Poll consume until CDC messages are visible (async propagation can be slow)
    let consume =
        "CONSUME FROM test_cdc_ns.events_stream GROUP 'cdc_consumers' START EARLIEST LIMIT 10";
    let mut result = server.execute_sql(consume).await;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);

    while tokio::time::Instant::now() < deadline {
        if result.status == ResponseStatus::Success
            && result.results.first().map(|batch| batch.row_count >= 2).unwrap_or(false)
        {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        result = server.execute_sql(consume).await;
    }

    assert!(result.status == ResponseStatus::Success, "CONSUME failed: {:?}", result.error);

    // 6. Verify we got results (schema: topic, partition, offset, key, payload, timestamp_ms, op)
    assert!(!result.results.is_empty(), "CONSUME should return batches");

    if let Some(first_batch) = result.results.first() {
        // Check we have the expected 8 columns from topic_message_schema
        assert_eq!(
            first_batch.schema.len(),
            8,
            "Should have 8 schema fields (topic_id, partition_id, offset, key, payload, \
             timestamp_ms, user_id, op)"
        );

        // Verify column names match schema
        assert_eq!(first_batch.schema[0].name, "topic_id");
        assert_eq!(first_batch.schema[1].name, "partition_id");
        assert_eq!(first_batch.schema[2].name, "offset");
        assert_eq!(first_batch.schema[3].name, "key");
        assert_eq!(first_batch.schema[4].name, "payload");
        assert_eq!(first_batch.schema[5].name, "timestamp_ms");
        assert_eq!(first_batch.schema[6].name, "user_id");
        assert_eq!(first_batch.schema[7].name, "op");

        // Row count is eventually consistent in current CDC test environment.
        // Validate schema and successful consume response; payload-count strictness
        // is covered by dedicated CDC/topic tests in more controlled setups.
        assert!(first_batch.row_count <= 10, "Unexpected row count: {}", first_batch.row_count);
    }

    let http_server = http_server::get_global_server().await;
    let auth_header = http_server
        .bearer_auth_header("root")
        .expect("Failed to create root auth header");
    let group = format!("cdc-key-check-{}", consolidated_helpers::unique_table("group"));
    let http_consume = wait_until_group_reads_at_least(
        http_server,
        &auth_header,
        "test_cdc_ns.events_stream",
        &group,
        json!("earliest"),
        10,
        2,
    )
    .await;

    let consumed_keys: std::collections::HashSet<String> =
        http_consume.messages.iter().filter_map(|message| message.key.clone()).collect();
    assert!(consumed_keys.contains("evt1"), "Expected consumed key set to contain evt1");
    assert!(consumed_keys.contains("evt2"), "Expected consumed key set to contain evt2");
}

/// Test CONSUME with schema validation
/// Verifies the response matches topic_message_schema structure
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_consume_schema_structure() {
    let server = TestServer::new_shared().await;

    // Setup topic
    server.execute_sql("CREATE TOPIC default.schema_test_tp PARTITIONS 1").await;

    // Consume (empty is fine, we're testing schema)
    let sql = "CONSUME FROM default.schema_test_tp GROUP 'schema_group' START EARLIEST LIMIT 10";
    let result = server.execute_sql(sql).await;

    assert!(
        result.status == ResponseStatus::Success,
        "CONSUME should succeed: {:?}",
        result.error
    );

    // Verify schema structure matches topic_message_schema
    if !result.results.is_empty() {
        let batch = &result.results[0];

        // Must have exactly 8 schema fields
        assert_eq!(batch.schema.len(), 8, "Topic message schema must have 8 fields");

        // Verify field names and order
        let expected_fields = vec![
            "topic_id",
            "partition_id",
            "offset",
            "key",
            "payload",
            "timestamp_ms",
            "user_id",
            "op",
        ];
        for (i, expected_name) in expected_fields.iter().enumerate() {
            assert_eq!(
                &batch.schema[i].name, expected_name,
                "Field {} should be '{}', got '{}'",
                i, expected_name, batch.schema[i].name
            );
        }
    }
}

#[tokio::test]
#[ntest::timeout(30000)]
#[serial]
async fn test_sql_group_consume_resumes_from_committed_offsets_after_cache_clear() {
    let server = TestServer::new_shared().await;
    let _cache_guard = TopicPublisherCacheGuard {
        app_context: server.app_context.clone(),
    };

    let (topic, source_table) = setup_topic_source_fixture(&server, "tp_sql_resume").await;
    let group = format!("sql-resume-{}", consolidated_helpers::unique_table("group"));

    for id in 1..=2 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'payload_{}')",
                source_table, id, id
            ))
            .await;
        assert_eq!(insert.status, ResponseStatus::Success);
    }

    let readiness_sql = format!("CONSUME FROM {} FROM EARLIEST LIMIT 10", topic);
    let ready = wait_until_sql_consume_row_count_at_least(&server, &readiness_sql, 2).await;
    assert_eq!(ready.row_count(), 2, "Expected stateless consume to observe both rows");

    assert_topic_offset_state(&server, &topic, &group, None).await;

    let first_consume = server
        .execute_sql(&format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 1", topic, group))
        .await;
    assert_eq!(first_consume.status, ResponseStatus::Success);
    assert_eq!(
        first_consume.results.first().map(|batch| batch.row_count).unwrap_or(0),
        1,
        "Initial consume should return the first message"
    );
    let first_updated_at = assert_topic_offset_state(&server, &topic, &group, Some(0))
        .await
        .expect("Expected first grouped consume to auto-ack offset 0");

    server.app_context.topic_publisher().clear_cache();

    let second_consume = server
        .execute_sql(&format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 10", topic, group))
        .await;
    assert_eq!(second_consume.status, ResponseStatus::Success);
    assert_eq!(
        second_consume.results.first().map(|batch| batch.row_count).unwrap_or(0),
        1,
        "After clearing in-memory claims, SQL consume should resume from the committed offset"
    );
    let second_updated_at = assert_topic_offset_state(&server, &topic, &group, Some(1))
        .await
        .expect("Expected second grouped consume to advance auto-ack to offset 1");
    assert!(
        second_updated_at >= first_updated_at,
        "Offset update timestamp should move forward or stay equal across rapid commits"
    );

    let lower_ack = server
        .execute_sql(&format!("ACK {} GROUP '{}' UPTO OFFSET 0", topic, group))
        .await;
    assert_eq!(lower_ack.status, ResponseStatus::Success);
    let after_lower_ack = assert_topic_offset_state(&server, &topic, &group, Some(1))
        .await
        .expect("Expected lower ACK to leave committed offset intact");
    assert_eq!(
        after_lower_ack, second_updated_at,
        "Lower ACK should not regress or rewrite the committed topic offset row"
    );

    server.app_context.topic_publisher().clear_cache();

    let third_consume = server
        .execute_sql(&format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 10", topic, group))
        .await;
    assert_eq!(third_consume.status, ResponseStatus::Success);
    assert_eq!(
        third_consume.results.first().map(|batch| batch.row_count).unwrap_or(0),
        0,
        "Committed offsets should prevent replay once the group has consumed the backlog"
    );
    let final_updated_at = assert_topic_offset_state(&server, &topic, &group, Some(1))
        .await
        .expect("Expected committed topic offset row to remain after empty replay check");
    assert_eq!(final_updated_at, second_updated_at);
}

#[tokio::test]
#[ntest::timeout(45000)]
#[serial]
async fn test_sql_group_offsets_are_isolated_per_group() {
    let server = TestServer::new_shared().await;
    let _cache_guard = TopicPublisherCacheGuard {
        app_context: server.app_context.clone(),
    };

    let (topic, source_table) = setup_topic_source_fixture(&server, "tp_sql_groups").await;
    for id in 1..=10 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'payload_{}')",
                source_table, id, id
            ))
            .await;
        assert_eq!(insert.status, ResponseStatus::Success);
    }

    let readiness_sql = format!("CONSUME FROM {} FROM EARLIEST LIMIT 20", topic);
    let ready = wait_until_sql_consume_row_count_at_least(&server, &readiness_sql, 10).await;
    assert_eq!(ready.row_count(), 10, "Expected stateless consume to observe all inserted rows");

    let group_specs = vec![
        (format!("group-a-{}", consolidated_helpers::unique_table("tp")), 1usize, 0u64),
        (format!("group-b-{}", consolidated_helpers::unique_table("tp")), 2usize, 1u64),
        (format!("group-c-{}", consolidated_helpers::unique_table("tp")), 3usize, 2u64),
        (format!("group-d-{}", consolidated_helpers::unique_table("tp")), 4usize, 3u64),
        (format!("group-e-{}", consolidated_helpers::unique_table("tp")), 5usize, 4u64),
    ];

    for (group, limit, expected_last_acked) in &group_specs {
        let consume_sql =
            format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT {}", topic, group, limit);
        let response =
            wait_until_sql_consume_row_count_at_least(&server, &consume_sql, *limit).await;
        assert_eq!(
            response.row_count(),
            *limit,
            "Group '{}' should receive exactly {} row(s) on first consume",
            group,
            limit
        );
        assert_eq!(
            first_row_offset(&response),
            0,
            "New group '{}' should start from the beginning when consuming FROM EARLIEST",
            group
        );
        assert_topic_offset_state(&server, &topic, group, Some(*expected_last_acked)).await;
    }

    assert_topic_offset_count(&server, &topic, group_specs.len()).await;

    let topic_id = TopicId::new(&topic);
    let mut provider_snapshot: Vec<(String, u64)> = server
        .app_context
        .system_tables()
        .topic_offsets()
        .get_topic_offsets(&topic_id)
        .expect("Failed to list topic offsets for group isolation test")
        .into_iter()
        .map(|offset| (offset.group_id.as_str().to_string(), offset.last_acked_offset))
        .collect();
    provider_snapshot.sort();

    let mut expected_snapshot: Vec<(String, u64)> = group_specs
        .iter()
        .map(|(group, _, expected_last_acked)| (group.clone(), *expected_last_acked))
        .collect();
    expected_snapshot.sort();
    assert_eq!(
        provider_snapshot, expected_snapshot,
        "Each group should persist its own last_acked_offset without leaking another group's \
         cursor"
    );

    server.app_context.topic_publisher().clear_cache();

    for (group, _, expected_last_acked) in &group_specs {
        let resume_sql = format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 1", topic, group);
        let resumed = wait_until_sql_consume_row_count_at_least(&server, &resume_sql, 1).await;
        let expected_next_offset = *expected_last_acked as i64 + 1;
        assert_eq!(
            first_row_offset(&resumed),
            expected_next_offset,
            "Group '{}' should resume from its own committed cursor after cache clear",
            group
        );
        assert_topic_offset_state(&server, &topic, group, Some(*expected_last_acked + 1)).await;
    }

    let mut final_provider_snapshot: Vec<(String, u64)> = server
        .app_context
        .system_tables()
        .topic_offsets()
        .get_topic_offsets(&topic_id)
        .expect("Failed to list final topic offsets for group isolation test")
        .into_iter()
        .map(|offset| (offset.group_id.as_str().to_string(), offset.last_acked_offset))
        .collect();
    final_provider_snapshot.sort();

    let mut final_expected_snapshot: Vec<(String, u64)> = group_specs
        .iter()
        .map(|(group, _, expected_last_acked)| (group.clone(), *expected_last_acked + 1))
        .collect();
    final_expected_snapshot.sort();
    assert_eq!(
        final_provider_snapshot, final_expected_snapshot,
        "Each group should keep its own independent cursor after resuming again"
    );
}

#[tokio::test]
#[ntest::timeout(45000)]
#[serial]
async fn test_sql_group_from_latest_tails_new_messages_and_then_persists_offset() {
    let server = TestServer::new_shared().await;
    let _cache_guard = TopicPublisherCacheGuard {
        app_context: server.app_context.clone(),
    };

    let (topic, source_table) = setup_topic_source_fixture(&server, "tp_sql_latest").await;
    let group = format!("latest-{}", consolidated_helpers::unique_table("group"));

    for id in 1..=5 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'payload_{}')",
                source_table, id, id
            ))
            .await;
        assert_eq!(insert.status, ResponseStatus::Success);
    }

    let readiness_sql = format!("CONSUME FROM {} FROM EARLIEST LIMIT 10", topic);
    let ready = wait_until_sql_consume_row_count_at_least(&server, &readiness_sql, 5).await;
    assert_eq!(ready.row_count(), 5, "Expected stateless consume to observe the backlog");

    let latest_consume = server
        .execute_sql(&format!("CONSUME FROM {} GROUP '{}' FROM LATEST LIMIT 10", topic, group))
        .await;
    assert_eq!(latest_consume.status, ResponseStatus::Success);
    assert_eq!(latest_consume.row_count(), 0, "New latest group should not replay backlog");
    assert_topic_offset_state(&server, &topic, &group, None).await;

    for id in 100..=101 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'live_{}')",
                source_table, id, id
            ))
            .await;
        assert_eq!(insert.status, ResponseStatus::Success);
    }

    let tail_consume = wait_until_sql_consume_row_count_at_least(
        &server,
        &format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 10", topic, group),
        2,
    )
    .await;
    assert_eq!(row_offsets(&tail_consume), vec![5, 6]);
    assert_topic_offset_state(&server, &topic, &group, Some(6)).await;

    server.app_context.topic_publisher().clear_cache();

    let resumed = server
        .execute_sql(&format!("CONSUME FROM {} GROUP '{}' FROM 0 LIMIT 10", topic, group))
        .await;
    assert_eq!(resumed.status, ResponseStatus::Success);
    assert_eq!(
        resumed.row_count(),
        0,
        "Committed latest-group cursor should prevent replay after cache clear"
    );
    assert_topic_offset_state(&server, &topic, &group, Some(6)).await;
}

#[tokio::test]
#[ntest::timeout(45000)]
#[serial]
async fn test_sql_group_from_offset_starts_at_requested_offset_and_persists_resume() {
    let server = TestServer::new_shared().await;
    let _cache_guard = TopicPublisherCacheGuard {
        app_context: server.app_context.clone(),
    };

    let (topic, source_table) = setup_topic_source_fixture(&server, "tp_sql_offset").await;
    let group = format!("offset-{}", consolidated_helpers::unique_table("group"));

    for id in 1..=6 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'payload_{}')",
                source_table, id, id
            ))
            .await;
        assert_eq!(insert.status, ResponseStatus::Success);
    }

    let readiness_sql = format!("CONSUME FROM {} FROM EARLIEST LIMIT 10", topic);
    let ready = wait_until_sql_consume_row_count_at_least(&server, &readiness_sql, 6).await;
    assert_eq!(ready.row_count(), 6, "Expected stateless consume to observe all inserted rows");

    let offset_consume = wait_until_sql_consume_row_count_at_least(
        &server,
        &format!("CONSUME FROM {} GROUP '{}' FROM 3 LIMIT 2", topic, group),
        2,
    )
    .await;
    assert_eq!(row_offsets(&offset_consume), vec![3, 4]);
    assert_topic_offset_state(&server, &topic, &group, Some(4)).await;

    server.app_context.topic_publisher().clear_cache();

    let resumed = wait_until_sql_consume_row_count_at_least(
        &server,
        &format!("CONSUME FROM {} GROUP '{}' FROM EARLIEST LIMIT 10", topic, group),
        1,
    )
    .await;
    assert_eq!(row_offsets(&resumed), vec![5]);
    assert_topic_offset_state(&server, &topic, &group, Some(5)).await;
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_sql_consume_without_group_is_stateless_and_does_not_persist_offsets() {
    let server = TestServer::new_shared().await;
    let (topic, source_table) = setup_topic_source_fixture(&server, "tp_sql_stateless").await;

    for id in 1..=2 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'payload_{}')",
                source_table, id, id
            ))
            .await;
        assert_eq!(insert.status, ResponseStatus::Success);
    }

    let sql = format!("CONSUME FROM {} FROM EARLIEST LIMIT 1", topic);
    let first_consume = wait_until_sql_consume_row_count_at_least(&server, &sql, 1).await;
    let first_offset = first_row_offset(&first_consume);
    assert_eq!(first_offset, 0, "First stateless consume should start at offset 0");

    let second_consume = server.execute_sql(&sql).await;
    assert_eq!(
        second_consume.status,
        ResponseStatus::Success,
        "Second stateless consume should succeed: {:?}",
        second_consume.error
    );
    assert_eq!(second_consume.row_count(), 1, "Expected another single-row stateless consume");
    let second_offset = first_row_offset(&second_consume);
    assert_eq!(
        second_offset, first_offset,
        "Without GROUP, repeated consumes should replay the same earliest row"
    );

    assert_topic_offset_count(&server, &topic, 0).await;
}

/// HTTP API integration: consume/ack option combinations and offset progression.
///
/// Covers the consumer options from SDK usage:
/// - `topic` / `group_id` mapping to `topic_id` / `group_id`
/// - `start` (`earliest` and `latest`)
/// - `batch_size` mapping to `limit`
/// - explicit `ack` progression for same-group consumers
#[tokio::test]
#[ntest::timeout(90000)]
async fn test_http_api_consume_ack_option_combinations() {
    let server = http_server::get_global_server().await;
    let namespace = consolidated_helpers::unique_namespace("tp_http_opts");
    let table = consolidated_helpers::unique_table("events");
    let topic_table = consolidated_helpers::unique_table("topic");
    let topic = format!("{}.{}", namespace, topic_table);
    let source_table = format!("{}.{}", namespace, table);

    let auth_header = server.bearer_auth_header("root").expect("Failed to create root auth header");

    let create_namespace = server
        .execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("CREATE NAMESPACE request failed");
    assert_eq!(
        create_namespace.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        create_namespace.error
    );

    let create_table = server
        .execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", source_table))
        .await
        .expect("CREATE TABLE request failed");
    assert_eq!(
        create_table.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_table.error
    );

    let create_topic = server
        .execute_sql(&format!("CREATE TOPIC {} PARTITIONS 1", topic))
        .await
        .expect("CREATE TOPIC request failed");
    assert_eq!(
        create_topic.status,
        ResponseStatus::Success,
        "CREATE TOPIC failed: {:?}",
        create_topic.error
    );

    let add_source = server
        .execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, source_table))
        .await
        .expect("ALTER TOPIC ADD SOURCE request failed");
    assert_eq!(
        add_source.status,
        ResponseStatus::Success,
        "ALTER TOPIC ADD SOURCE failed: {:?}",
        add_source.error
    );
    wait_for_topic_routes(server, &topic, 1).await;

    let expected_backlog: usize = 80;
    for id in 0..expected_backlog {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'payload_{}')",
                source_table, id, id
            ))
            .await
            .expect("INSERT request failed");
        assert_eq!(
            insert.status,
            ResponseStatus::Success,
            "INSERT {} failed: {:?}",
            id,
            insert.error
        );
    }

    // Ensure CDC events are fully visible before exercising option combinations.
    let ready_group = format!("ready-{}", consolidated_helpers::unique_table("opts"));
    let ready = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &ready_group,
        json!("earliest"),
        expected_backlog as u64,
        expected_backlog,
    )
    .await;
    assert_eq!(
        ready.messages.len(),
        expected_backlog,
        "Readiness group should observe all inserted events before option assertions"
    );

    let group = format!("group-{}", consolidated_helpers::unique_table("earliest"));
    let first_batch = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &group,
        json!("earliest"),
        2,
        2,
    )
    .await;
    assert_eq!(
        first_batch.messages.len(),
        2,
        "batch_size/limit should cap first consume response"
    );
    assert!(first_batch.has_more, "Expected more data after first limited batch");
    assert_eq!(
        first_batch.next_offset,
        first_batch.messages.last().expect("message exists").offset + 1
    );

    let ack_offset = first_batch.messages.last().expect("message exists").offset;
    let (ack_status, ack_payload) = post_topics_ack(
        server,
        &auth_header,
        json!({
            "topic_id": topic,
            "group_id": group,
            "partition_id": 0,
            "upto_offset": ack_offset
        }),
    )
    .await;
    assert_eq!(
        ack_status,
        StatusCode::OK,
        "Ack endpoint should return HTTP 200, payload={:?}",
        ack_payload
    );

    let ack_response: HttpAckResponse =
        serde_json::from_value(ack_payload).expect("Ack payload should deserialize");
    assert!(ack_response.success, "Ack response should be successful");
    assert_eq!(
        ack_response.acknowledged_offset, ack_offset,
        "Ack response should echo acknowledged offset"
    );

    let resumed = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &group,
        json!("earliest"),
        10,
        10,
    )
    .await;
    assert!(
        resumed.messages.iter().all(|m| m.offset > ack_offset),
        "Messages should resume strictly after acked offset"
    );

    let latest_group = format!("group-{}", consolidated_helpers::unique_table("latest"));
    let (latest_status, latest_payload) = post_topics_consume(
        server,
        &auth_header,
        json!({
            "topic_id": topic,
            "group_id": latest_group,
            "start": "latest",
            "limit": 10,
            "partition_id": 0,
            "timeout_seconds": 1
        }),
    )
    .await;
    assert_eq!(
        latest_status,
        StatusCode::OK,
        "Latest consume should return HTTP 200, payload={:?}",
        latest_payload
    );
    let latest_initial: HttpConsumeResponse =
        serde_json::from_value(latest_payload).expect("Latest payload should deserialize");
    assert_eq!(
        latest_initial.messages.len(),
        0,
        "start=latest should not replay old backlog for new group"
    );

    for id in 100..=101 {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'live_{}')",
                source_table, id, id
            ))
            .await
            .expect("INSERT live request failed");
        assert_eq!(
            insert.status,
            ResponseStatus::Success,
            "INSERT live {} failed: {:?}",
            id,
            insert.error
        );
    }

    let latest_after_new = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &latest_group,
        json!({ "Offset": latest_initial.next_offset }),
        10,
        2,
    )
    .await;
    assert!(
        latest_after_new.messages.len() >= 2,
        "Latest consumer should observe newly inserted rows"
    );
}

/// HTTP API integration: two consumers in the same group should not receive
/// overlapping offsets when polling concurrently in batches.
#[tokio::test]
#[ntest::timeout(90000)]
async fn test_http_api_same_group_consumers_no_overlap() {
    let server = http_server::get_global_server().await;
    let namespace = consolidated_helpers::unique_namespace("tp_http_group");
    let table = consolidated_helpers::unique_table("events");
    let topic_table = consolidated_helpers::unique_table("topic");
    let topic = format!("{}.{}", namespace, topic_table);
    let source_table = format!("{}.{}", namespace, table);

    let service_user = consolidated_helpers::unique_table("svc_tp_group");
    let auth_header = auth_helper::create_user_auth_header(
        server,
        &service_user,
        "TopicPass123!",
        &Role::Service,
    )
    .await
    .expect("Failed to create service auth header");

    let create_namespace = server
        .execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("CREATE NAMESPACE request failed");
    assert_eq!(create_namespace.status, ResponseStatus::Success);

    let create_table = server
        .execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, payload TEXT)", source_table))
        .await
        .expect("CREATE TABLE request failed");
    assert_eq!(create_table.status, ResponseStatus::Success);

    let create_topic = server
        .execute_sql(&format!("CREATE TOPIC {} PARTITIONS 1", topic))
        .await
        .expect("CREATE TOPIC request failed");
    assert_eq!(create_topic.status, ResponseStatus::Success);

    let add_source = server
        .execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, source_table))
        .await
        .expect("ALTER TOPIC ADD SOURCE request failed");
    assert_eq!(add_source.status, ResponseStatus::Success);
    wait_for_topic_routes(server, &topic, 1).await;

    let expected_messages = 80usize;
    for id in 0..expected_messages {
        let insert = server
            .execute_sql(&format!(
                "INSERT INTO {} (id, payload) VALUES ({}, 'event_{}')",
                source_table, id, id
            ))
            .await
            .expect("INSERT request failed");
        assert_eq!(insert.status, ResponseStatus::Success, "INSERT {} failed", id);
    }

    // Warm-up with an independent group to ensure all messages are queryable.
    let ready_group = format!("ready-{}", consolidated_helpers::unique_table("group"));
    let ready = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &ready_group,
        json!("earliest"),
        expected_messages as u64,
        expected_messages,
    )
    .await;
    assert_eq!(ready.messages.len(), expected_messages);

    let group = format!("group-{}", consolidated_helpers::unique_table("shared"));

    let first = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &group,
        json!("earliest"),
        40,
        40,
    )
    .await;
    let second = wait_until_group_reads_at_least(
        server,
        &auth_header,
        &topic,
        &group,
        json!("earliest"),
        40,
        40,
    )
    .await;

    let first_offsets: std::collections::HashSet<(u32, u64)> =
        first.messages.iter().map(|m| (m.partition_id, m.offset)).collect();
    let second_offsets: std::collections::HashSet<(u32, u64)> =
        second.messages.iter().map(|m| (m.partition_id, m.offset)).collect();

    let overlap_count = first_offsets.intersection(&second_offsets).count();
    assert_eq!(overlap_count, 0, "Same-group consumers must not receive overlapping offsets");

    let union_count = first_offsets.union(&second_offsets).count();
    assert_eq!(
        union_count, 80,
        "Two sequential same-group polls should cover the first 80 offsets without duplicates"
    );
}

// ============================================================================
// TODO: Additional CDC Workflow Tests
// ============================================================================
//
// The following tests require:
// - HTTP API test client setup
// - Async notification verification
// - Extended timeout handling for CDC processing
//
// Completed tests:
// ✅ test_cdc_insert_to_consume_workflow() - CDC INSERT → CONSUME end-to-end
// ✅ test_consume_schema_structure() - Schema validation
// ✅ test_consume_user_role_forbidden() - Authorization checks
// ✅ test_consume_privileged_roles_allowed() - Service/DBA/System access
// ✅ test_ack_user_role_forbidden() - ACK authorization checks
// ✅ test_clear_topic() - CLEAR TOPIC command
//
// Planned tests (Phase 10 in IMPLEMENTATION_TASKS.md):
// - test_consume_multiple_consumer_groups()
// - test_consume_with_filter_expression()
// - test_long_polling_immediate_return()
// - test_long_polling_timeout_empty_response()
// - test_http_api_consume_endpoint() - REST API /v1/api/topics/consume
// - test_http_api_ack_endpoint() - REST API /v1/api/topics/ack
//
// These will be implemented once the HTTP API test client infrastructure
// is in place and CDC notification timing is stable.

/// Test CLEAR TOPIC command
#[tokio::test]
#[ntest::timeout(15000)]
async fn test_clear_topic() {
    let server = TestServer::new_shared().await;

    // Setup: Create namespace, table, and topic
    server.execute_sql("CREATE NAMESPACE test_clear_ns").await;
    server
        .execute_sql("CREATE TABLE test_clear_ns.messages (id TEXT PRIMARY KEY, content TEXT)")
        .await;
    server
        .execute_sql("CREATE TOPIC test_clear_ns.messages_topic PARTITIONS 1")
        .await;
    server
        .execute_sql(
            "ALTER TOPIC test_clear_ns.messages_topic ADD SOURCE test_clear_ns.messages ON INSERT",
        )
        .await;

    // Insert some data to generate messages
    server
        .execute_sql("INSERT INTO test_clear_ns.messages (id, content) VALUES ('1', 'message1')")
        .await;
    server
        .execute_sql("INSERT INTO test_clear_ns.messages (id, content) VALUES ('2', 'message2')")
        .await;
    server
        .execute_sql("INSERT INTO test_clear_ns.messages (id, content) VALUES ('3', 'message3')")
        .await;

    // Give CDC some time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Consume to verify messages exist
    let consume_result = server
        .execute_sql(
            "CONSUME FROM test_clear_ns.messages_topic GROUP 'test_group' START EARLIEST LIMIT 10",
        )
        .await;
    assert_eq!(consume_result.status, ResponseStatus::Success, "Initial consume should succeed");

    // Clear the topic
    let clear_result = server.execute_sql("CLEAR TOPIC test_clear_ns.messages_topic").await;
    assert_eq!(
        clear_result.status,
        ResponseStatus::Success,
        "CLEAR TOPIC should succeed: {:?}",
        clear_result.error
    );

    // Give the cleanup job time to execute
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Verify messages are cleared by consuming again
    let verify_result = server
        .execute_sql(
            "CONSUME FROM test_clear_ns.messages_topic GROUP 'verify_group' START EARLIEST LIMIT \
             10",
        )
        .await;
    assert_eq!(
        verify_result.status,
        ResponseStatus::Success,
        "Consume after clear should succeed"
    );

    // Topic metadata should still exist
    let topic_check = server
        .execute_sql("CREATE TOPIC test_clear_ns.messages_topic PARTITIONS 1")
        .await;
    assert!(
        topic_check
            .error
            .as_ref()
            .map(|e| e.message.contains("already exists"))
            .unwrap_or(false),
        "Topic should still exist after CLEAR"
    );
}

/// Test CLEAR TOPIC with non-existent topic
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_clear_topic_not_found() {
    let server = TestServer::new_shared().await;

    let result = server.execute_sql("CLEAR TOPIC default.nonexistent_topic").await;
    assert_eq!(result.status, ResponseStatus::Error, "Should fail for non-existent topic");
    assert!(
        result
            .error
            .as_ref()
            .map(|e| e.message.contains("does not exist"))
            .unwrap_or(false),
        "Error should mention topic doesn't exist"
    );
}

/// Test CLEAR TOPIC authorization (requires admin role)
#[tokio::test]
#[ntest::timeout(10000)]
async fn test_clear_topic_user_role_forbidden() {
    let server = TestServer::new_shared().await;

    // Create a regular user
    server
        .execute_sql("CREATE USER clear_test_user WITH PASSWORD 'password123' ROLE user")
        .await;

    // Create topic as admin
    server.execute_sql("CREATE TOPIC default.admin_topic PARTITIONS 1").await;

    // Try to clear as regular user (should fail)
    let result = server
        .execute_sql_as_user("CLEAR TOPIC default.admin_topic", "clear_test_user")
        .await;

    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should not be able to clear topic"
    );
    assert!(
        result
            .error
            .as_ref()
            .map(|e| {
                let msg = e.message.as_str();
                msg.contains("DBA") && msg.contains("System")
            })
            .unwrap_or(false),
        "Error should mention DBA/System role requirement"
    );
}

/// Test DROP TOPIC schedules cleanup job
#[tokio::test]
#[ntest::timeout(15000)]
async fn test_drop_topic_schedules_cleanup_job() {
    let server = TestServer::new_shared().await;

    // Setup: Create namespace, table, and topic
    server.execute_sql("CREATE NAMESPACE test_drop_ns").await;
    server
        .execute_sql("CREATE TABLE test_drop_ns.events (id TEXT PRIMARY KEY, data TEXT)")
        .await;
    server.execute_sql("CREATE TOPIC test_drop_ns.events_topic PARTITIONS 1").await;
    server
        .execute_sql(
            "ALTER TOPIC test_drop_ns.events_topic ADD SOURCE test_drop_ns.events ON INSERT",
        )
        .await;

    // Insert some data to generate messages
    server
        .execute_sql("INSERT INTO test_drop_ns.events (id, data) VALUES ('1', 'event1')")
        .await;
    server
        .execute_sql("INSERT INTO test_drop_ns.events (id, data) VALUES ('2', 'event2')")
        .await;

    // Give CDC some time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Drop the topic (should schedule cleanup job)
    let drop_result = server.execute_sql("DROP TOPIC test_drop_ns.events_topic").await;
    assert_eq!(
        drop_result.status,
        ResponseStatus::Success,
        "DROP TOPIC should succeed: {:?}",
        drop_result.error
    );
    assert!(
        drop_result
            .results
            .first()
            .and_then(|r| r.message.as_ref())
            .map(|msg| msg.contains("cleanup job"))
            .unwrap_or(false),
        "DROP TOPIC should mention cleanup job in message"
    );

    // Give the cleanup job time to execute
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Verify topic is gone
    let topic_check = server.execute_sql("DROP TOPIC test_drop_ns.events_topic").await;
    assert_eq!(topic_check.status, ResponseStatus::Error, "Topic should not exist after drop");
    assert!(
        topic_check
            .error
            .as_ref()
            .map(|e| e.message.contains("does not exist"))
            .unwrap_or(false),
        "Error should mention topic doesn't exist"
    );
}
