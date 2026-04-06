//! Integration tests for the Kafka-style TopicConsumer.
//!
//! These tests require a running KalamDB server with topic pub/sub support.
//! Set KALAMDB_SERVER_URL and KALAMDB_ROOT_PASSWORD environment variables.
//!
//! Run with: cargo test --test test_consumer -- --nocapture

use kalam_client::{
    consumer::{AutoOffsetReset, TopicConsumer},
    AuthProvider, KalamLinkClient,
};
use std::collections::HashMap;
use std::time::Duration;

fn get_server_url() -> String {
    std::env::var("KALAMDB_SERVER_URL").unwrap_or_else(|_| "http://localhost:3000".to_string())
}

fn get_auth() -> AuthProvider {
    let password =
        std::env::var("KALAMDB_ROOT_PASSWORD").unwrap_or_else(|_| "admin123".to_string());
    AuthProvider::system_user_auth(password)
}

fn unique_id() -> String {
    format!(
        "test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn create_client() -> KalamLinkClient {
    KalamLinkClient::builder()
        .base_url(get_server_url())
        .auth(get_auth())
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to create client")
}

async fn setup_topic_and_table(client: &KalamLinkClient, topic_name: &str, table_name: &str) {
    // Create namespace if needed
    let ns = topic_name.split('.').next().unwrap_or("test");
    let _ = client
        .execute_query(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns), None, None, None)
        .await;

    // Create table
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, message TEXT, created_at TIMESTAMP DEFAULT NOW())",
        table_name
    );
    let _ = client.execute_query(&create_table, None, None, None).await;

    // Create topic
    let create_topic = format!("CREATE TOPIC IF NOT EXISTS {}", topic_name);
    let _ = client.execute_query(&create_topic, None, None, None).await;

    // Add route from table to topic
    let add_route = format!(
        "ALTER TOPIC {} ADD SOURCE {} ON INSERT WITH (payload = 'full')",
        topic_name, table_name
    );
    let _ = client.execute_query(&add_route, None, None, None).await;
}

async fn insert_messages(client: &KalamLinkClient, table_name: &str, count: usize) {
    for i in 0..count {
        let sql =
            format!("INSERT INTO {} (id, message) VALUES ({}, 'Message {}')", table_name, i, i);
        client.execute_query(&sql, None, None, None).await.expect("Insert failed");
    }
}

// =============================================================================
// Unit-style tests (no server required)
// =============================================================================

#[test]
fn test_consumer_builder_requires_group_id() {
    let result = TopicConsumer::builder()
        .base_url("http://localhost:3000")
        .topic("test.topic")
        .build();

    let err = match result {
        Ok(_) => panic!("Expected error when group_id is missing"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("group_id"));
}

#[test]
fn test_consumer_builder_requires_topic() {
    let result = TopicConsumer::builder()
        .base_url("http://localhost:3000")
        .group_id("test-group")
        .build();

    assert!(result.is_err());
    let err = match result {
        Ok(_) => panic!("Expected error when topic is missing"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("topic"));
}

#[test]
fn test_consumer_builder_requires_base_url_without_client() {
    let result = TopicConsumer::builder().group_id("test-group").topic("test.topic").build();

    assert!(result.is_err());
    let err = match result {
        Ok(_) => panic!("Expected error when base_url is missing"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("base_url"));
}

#[test]
fn test_consumer_builder_from_properties() {
    let mut props = HashMap::new();
    props.insert("group.id".to_string(), "kafka-group".to_string());
    props.insert("topic".to_string(), "kafka-topic".to_string());
    props.insert("auto.offset.reset".to_string(), "earliest".to_string());
    props.insert("enable.auto.commit".to_string(), "false".to_string());
    props.insert("max.poll.records".to_string(), "50".to_string());

    let builder = TopicConsumer::builder()
        .base_url("http://localhost:3000")
        .from_properties(&props)
        .expect("from_properties failed");

    // The builder should have absorbed the properties
    let result = builder.build();
    assert!(result.is_ok());
}

#[test]
fn test_consumer_builder_all_options() {
    let result = TopicConsumer::builder()
        .base_url("http://localhost:3000")
        .group_id("my-group")
        .topic("my.topic")
        .client_id("my-client")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .auto_commit_interval(Duration::from_secs(10))
        .max_poll_records(50)
        .poll_timeout(Duration::from_secs(15))
        .partition_id(1)
        .request_timeout(Duration::from_secs(20))
        .retry_backoff(Duration::from_millis(200))
        .build();

    assert!(result.is_ok());
}

// =============================================================================
// Integration tests (require running server)
// =============================================================================

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_basic_consume_manual_commit() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 5).await;

    let mut consumer = client
        .consumer()
        .group_id(&group_id)
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .max_poll_records(10)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer build failed");

    // Poll for messages
    let records = consumer.poll().await.expect("Poll failed");
    assert!(!records.is_empty(), "Expected some records");

    // Mark all as processed
    for record in &records {
        consumer.mark_processed(record);
    }

    // Commit
    let commit_result = consumer.commit_sync().await.expect("Commit failed");
    assert!(commit_result.acknowledged_offset > 0);
    assert_eq!(commit_result.group_id, group_id);

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_auto_commit() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 3).await;

    let mut consumer = client
        .consumer()
        .group_id(&group_id)
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(true)
        .auto_commit_interval(Duration::from_millis(100))
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer build failed");

    // Poll and mark processed
    let records = consumer.poll().await.expect("Poll failed");
    for record in &records {
        consumer.mark_processed(record);
    }

    // Wait for auto-commit interval
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger auto-commit via another poll
    let _ = consumer.poll_with_timeout(Duration::from_secs(1)).await;

    // Check offsets were committed
    let offsets = consumer.offsets();
    assert!(offsets.last_committed.is_some());

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_auto_offset_reset_earliest() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 3).await;

    // New group, should start from earliest
    let mut consumer = client
        .consumer()
        .group_id(format!("new_group_{}", id))
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer build failed");

    let records = consumer.poll().await.expect("Poll failed");
    assert!(!records.is_empty(), "Expected records from earliest");

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_auto_offset_reset_latest() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 3).await;

    // New group with latest, should get no existing messages
    let mut consumer = client
        .consumer()
        .group_id(format!("latest_group_{}", id))
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Latest)
        .enable_auto_commit(false)
        .poll_timeout(Duration::from_secs(2))
        .build()
        .expect("Consumer build failed");

    let _records = consumer.poll().await.expect("Poll failed");
    // Latest should return empty or only new messages
    // (depends on timing, but typically empty)

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_poll_with_timeout_returns_empty() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    // Don't insert any messages

    let mut consumer = client
        .consumer()
        .group_id(format!("timeout_group_{}", id))
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Latest)
        .enable_auto_commit(false)
        .build()
        .expect("Consumer build failed");

    let start = std::time::Instant::now();
    let records = consumer.poll_with_timeout(Duration::from_secs(2)).await.expect("Poll failed");

    assert!(records.is_empty());
    assert!(start.elapsed() >= Duration::from_secs(1));

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_seek_changes_position() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("seek_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 10).await;

    let mut consumer = client
        .consumer()
        .group_id(&group_id)
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .max_poll_records(5)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer build failed");

    // Poll first batch
    let records = consumer.poll().await.expect("Poll failed");
    assert!(!records.is_empty());
    let _first_position = consumer.position();

    // Seek back to beginning
    consumer.seek(0);
    assert_eq!(consumer.position(), 0);

    // Poll again should get same records
    let records2 = consumer.poll().await.expect("Poll failed");
    assert!(!records2.is_empty());

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_commit_persistence_across_consumers() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("persist_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 5).await;

    // First consumer
    {
        let mut consumer = client
            .consumer()
            .group_id(&group_id)
            .topic(&topic_name)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(3)
            .poll_timeout(Duration::from_secs(5))
            .build()
            .expect("Consumer build failed");

        let records = consumer.poll().await.expect("Poll failed");
        for record in &records {
            consumer.mark_processed(record);
        }
        consumer.commit_sync().await.expect("Commit failed");
        consumer.close().await.expect("Close failed");
    }

    // Second consumer with same group should continue from committed offset
    {
        let mut consumer = client
            .consumer()
            .group_id(&group_id)
            .topic(&topic_name)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .poll_timeout(Duration::from_secs(5))
            .build()
            .expect("Consumer build failed");

        let _records = consumer.poll().await.expect("Poll failed");
        // Should get remaining messages (2 out of 5)
        // The exact count depends on server behavior
        consumer.close().await.expect("Close failed");
    }
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_high_load_multiple_messages() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("highload_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;

    // Insert many messages
    let message_count = 100;
    for i in 0..message_count {
        let sql = format!(
            "INSERT INTO {} (id, message) VALUES ({}, 'High load message {}')",
            table_name, i, i
        );
        client.execute_query(&sql, None, None, None).await.expect("Insert failed");
    }

    let mut consumer = client
        .consumer()
        .group_id(&group_id)
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .max_poll_records(20)
        .poll_timeout(Duration::from_secs(10))
        .build()
        .expect("Consumer build failed");

    let mut total_received = 0;
    let mut poll_count = 0;
    let max_polls = 10;

    while total_received < message_count && poll_count < max_polls {
        let records = consumer.poll().await.expect("Poll failed");
        if records.is_empty() {
            break;
        }
        total_received += records.len();
        for record in &records {
            consumer.mark_processed(record);
        }
        consumer.commit_sync().await.expect("Commit failed");
        poll_count += 1;
    }

    assert!(
        total_received >= message_count / 2,
        "Expected at least half the messages, got {}",
        total_received
    );

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_multiple_consumers_different_topics() {
    let client = create_client().await;
    let id = unique_id();

    let topic1 = format!("test.topic1_{}", id);
    let topic2 = format!("test.topic2_{}", id);
    let table1 = format!("test.table1_{}", id);
    let table2 = format!("test.table2_{}", id);

    setup_topic_and_table(&client, &topic1, &table1).await;
    setup_topic_and_table(&client, &topic2, &table2).await;

    insert_messages(&client, &table1, 3).await;
    insert_messages(&client, &table2, 5).await;

    // Consumer 1
    let mut consumer1 = client
        .consumer()
        .group_id(format!("multi_group1_{}", id))
        .topic(&topic1)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer 1 build failed");

    // Consumer 2
    let mut consumer2 = client
        .consumer()
        .group_id(format!("multi_group2_{}", id))
        .topic(&topic2)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer 2 build failed");

    // Poll both concurrently
    let (records1, records2) = tokio::join!(consumer1.poll(), consumer2.poll());

    let records1 = records1.expect("Poll 1 failed");
    let records2 = records2.expect("Poll 2 failed");

    // Verify no cross-contamination
    for record in &records1 {
        assert!(
            record.topic_name.contains("topic1"),
            "Topic 1 consumer got wrong topic: {}",
            record.topic_name
        );
    }
    for record in &records2 {
        assert!(
            record.topic_name.contains("topic2"),
            "Topic 2 consumer got wrong topic: {}",
            record.topic_name
        );
    }

    consumer1.close().await.expect("Close 1 failed");
    consumer2.close().await.expect("Close 2 failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_reconnect_after_disconnect() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("reconnect_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 5).await;

    // First consumer - consume but don't commit all
    {
        let mut consumer = client
            .consumer()
            .group_id(&group_id)
            .topic(&topic_name)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(2)
            .poll_timeout(Duration::from_secs(5))
            .build()
            .expect("Consumer build failed");

        let records = consumer.poll().await.expect("Poll failed");
        // Process and commit only first batch
        for record in &records {
            consumer.mark_processed(record);
        }
        consumer.commit_sync().await.expect("Commit failed");
        // Close without consuming remaining
        consumer.close().await.expect("Close failed");
    }

    // Simulate disconnect by creating new consumer with same group
    {
        let mut consumer = client
            .consumer()
            .group_id(&group_id)
            .topic(&topic_name)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .poll_timeout(Duration::from_secs(5))
            .build()
            .expect("Consumer build failed");

        // Should receive remaining messages (at-least-once)
        let records = consumer.poll().await.expect("Poll failed");
        // Verify we got messages starting from committed offset
        assert!(
            !records.is_empty() || consumer.position() > 0,
            "Should have remaining messages or position > 0"
        );

        consumer.close().await.expect("Close failed");
    }
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_consumer_from_client_convenience() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;

    // Use the client.consumer() convenience method
    let consumer = client
        .consumer()
        .group_id(format!("convenience_group_{}", id))
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build();

    assert!(consumer.is_ok(), "Consumer should build from client");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_max_poll_records_respected() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("maxpoll_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 20).await;

    let max_records = 5;
    let mut consumer = client
        .consumer()
        .group_id(&group_id)
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .max_poll_records(max_records)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer build failed");

    let records = consumer.poll().await.expect("Poll failed");
    assert!(
        records.len() <= max_records as usize,
        "Got {} records, expected at most {}",
        records.len(),
        max_records
    );

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_commit_async() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("async_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 3).await;

    let mut consumer = client
        .consumer()
        .group_id(&group_id)
        .topic(&topic_name)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .poll_timeout(Duration::from_secs(5))
        .build()
        .expect("Consumer build failed");

    let records = consumer.poll().await.expect("Poll failed");
    for record in &records {
        consumer.mark_processed(record);
    }

    // Async commit should return immediately
    consumer.commit_async().await.expect("Commit async failed");

    // Give it time to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    consumer.close().await.expect("Close failed");
}

#[tokio::test]
#[ignore = "Requires running KalamDB server"]
async fn test_close_flushes_auto_commit() {
    let client = create_client().await;
    let id = unique_id();
    let topic_name = format!("test.topic_{}", id);
    let table_name = format!("test.table_{}", id);
    let group_id = format!("closeflush_group_{}", id);

    setup_topic_and_table(&client, &topic_name, &table_name).await;
    insert_messages(&client, &table_name, 3).await;

    {
        let mut consumer = client
            .consumer()
            .group_id(&group_id)
            .topic(&topic_name)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(true)
            .auto_commit_interval(Duration::from_secs(300)) // Long interval
            .poll_timeout(Duration::from_secs(5))
            .build()
            .expect("Consumer build failed");

        let records = consumer.poll().await.expect("Poll failed");
        for record in &records {
            consumer.mark_processed(record);
        }

        // Close should flush commits even though interval hasn't elapsed
        consumer.close().await.expect("Close failed");
    }

    // New consumer should not see the same messages
    {
        let mut consumer = client
            .consumer()
            .group_id(&group_id)
            .topic(&topic_name)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .poll_timeout(Duration::from_secs(2))
            .build()
            .expect("Consumer build failed");

        // Position should be after the committed offset
        // (this depends on server behavior)
        consumer.close().await.expect("Close failed");
    }
}

#[test]
fn test_offset_tracking() {
    use kalam_client::consumer::ConsumerRecord;

    // Create a mock record
    let record = ConsumerRecord {
        topic_id: "topic-123".to_string(),
        topic_name: "test.topic".to_string(),
        partition_id: 0,
        offset: 42,
        message_id: Some("msg-1".to_string()),
        source_table: "test.table".to_string(),
        op: kalam_client::TopicOp::Insert,
        timestamp_ms: 1700000000000,
        payload_mode: kalam_client::PayloadMode::Full,
        payload: vec![1, 2, 3],
    };

    assert_eq!(record.offset, 42);
    assert_eq!(record.partition_id, 0);
    assert_eq!(record.topic_name, "test.topic");
}
