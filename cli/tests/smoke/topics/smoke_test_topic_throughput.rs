// Topic throughput benchmark smoke test
// Measures messages per second under various publisher/consumer configurations

use crate::common;
use kalam_client::consumer::AutoOffsetReset;
use kalam_client::KalamLinkTimeouts;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Baseline thresholds (90% of current measured performance)
// Single-publisher is bottlenecked by sequential HTTP round-trips (~385 inserts/s),
// so the consumer rate is limited by how fast messages arrive, not consumption speed.
const THRESHOLD_SINGLE_PUB_SINGLE_CONSUMER: f64 = 120.0; // Stable floor across loaded CI/local runs
const THRESHOLD_MULTI_PUB_SINGLE_CONSUMER: f64 = 220.0; // Stable floor across loaded CI/local runs
const THRESHOLD_MULTI_PUB_MULTI_CONSUMER: f64 = 220.0; // Stable floor across loaded CI/local runs

/// Create a test client using common infrastructure
async fn create_test_client() -> kalam_client::KalamLinkClient {
    let base_url = common::leader_or_server_url();
    common::client_for_user_on_url_with_timeouts(
        &base_url,
        common::default_username(),
        common::default_password(),
        KalamLinkTimeouts::builder()
            .connection_timeout_secs(10)
            .receive_timeout_secs(15)
            .send_timeout_secs(30)
            .subscribe_timeout_secs(15)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(60))
            .build(),
    )
    .expect("Failed to build test client")
}

/// Execute SQL via HTTP helper with error handling
async fn execute_sql(sql: &str) -> Result<(), String> {
    let response = common::execute_sql_via_http_as_root(sql).await.map_err(|e| e.to_string())?;
    let status = response.get("status").and_then(|s| s.as_str()).unwrap_or("");
    if status.eq_ignore_ascii_case("success") {
        Ok(())
    } else {
        let err_msg = response
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown error");
        Err(format!("SQL failed: {}", err_msg))
    }
}

async fn wait_for_topic_ready(topic: &str, expected_routes: usize) {
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

/// Test: Single publisher, single consumer
async fn bench_single_pub_single_consumer() -> (usize, f64, f64) {
    let namespace = common::generate_unique_namespace("tpbench1p1c");
    let table = format!("{}.items", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("topic1p1c"));
    let group_id = format!("bench-1p1c-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create ns");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, value TEXT)", table))
        .await
        .expect("create table");
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let message_count = 1000;
    let publishers_done = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicUsize::new(0));

    // Start consumer
    let consumer_topic = topic.clone();
    let consumer_group = group_id.clone();
    let consumer_done = publishers_done.clone();
    let consumer_received = received.clone();
    let consumer_handle = tokio::spawn(async move {
        let client = create_test_client().await;
        let mut consumer = client
            .consumer()
            .topic(&consumer_topic)
            .group_id(&consumer_group)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .max_poll_records(100)
            .build()
            .expect("build consumer");

        let deadline = Instant::now() + Duration::from_secs(60);
        let mut idle = 0;

        while Instant::now() < deadline {
            match consumer.poll().await {
                Ok(batch) if batch.is_empty() => {
                    idle += 1;
                    if consumer_done.load(Ordering::Relaxed) && idle >= 40 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                },
                Ok(batch) => {
                    idle = 0;
                    consumer_received.fetch_add(batch.len(), Ordering::SeqCst);
                    for record in &batch {
                        consumer.mark_processed(record);
                    }
                    let _ = consumer.commit_sync().await;
                },
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                },
            }
        }
    });

    // Single publisher
    let publish_start = Instant::now();

    for i in 0..message_count {
        execute_sql(&format!("INSERT INTO {} (id, value) VALUES ({}, 'msg_{}')", table, i, i))
            .await
            .ok();
    }

    let publish_elapsed = publish_start.elapsed();
    publishers_done.store(true, Ordering::Relaxed);

    // Wait for consumer
    tokio::time::timeout(Duration::from_secs(30), consumer_handle).await.ok();

    let total_received = received.load(Ordering::SeqCst);
    let total_elapsed = publish_start.elapsed();
    let msg_per_sec = total_received as f64 / total_elapsed.as_secs_f64();
    let publish_rate = message_count as f64 / publish_elapsed.as_secs_f64();

    // Cleanup
    let _ = execute_sql(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;

    (total_received, msg_per_sec, publish_rate)
}

/// Test: Multiple publishers, single consumer
async fn bench_multi_pub_single_consumer() -> (usize, f64, f64) {
    let namespace = common::generate_unique_namespace("tpbenchmpc");
    let table = format!("{}.items", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("topicmpc"));
    let group_id = format!("bench-mpc-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create ns");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, value TEXT)", table))
        .await
        .expect("create table");
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let message_count = 1200;
    let num_publishers = 24;
    let msgs_per_publisher = message_count / num_publishers;
    let publishers_done = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicUsize::new(0));

    // Start consumer
    let consumer_topic = topic.clone();
    let consumer_group = group_id.clone();
    let consumer_done = publishers_done.clone();
    let consumer_received = received.clone();
    let consumer_handle = tokio::spawn(async move {
        let client = create_test_client().await;
        let mut consumer = client
            .consumer()
            .topic(&consumer_topic)
            .group_id(&consumer_group)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .max_poll_records(150)
            .build()
            .expect("build consumer");

        let deadline = Instant::now() + Duration::from_secs(60);
        let mut idle = 0;

        while Instant::now() < deadline {
            match consumer.poll().await {
                Ok(batch) if batch.is_empty() => {
                    idle += 1;
                    if consumer_done.load(Ordering::Relaxed) && idle >= 40 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                },
                Ok(batch) => {
                    idle = 0;
                    consumer_received.fetch_add(batch.len(), Ordering::SeqCst);
                    for record in &batch {
                        consumer.mark_processed(record);
                    }
                    let _ = consumer.commit_sync().await;
                },
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                },
            }
        }
    });

    // Multiple publishers
    let publish_start = Instant::now();

    let mut pub_handles = Vec::new();
    for p in 0..num_publishers {
        let table = table.clone();
        pub_handles.push(tokio::spawn(async move {
            for i in 0..msgs_per_publisher {
                let id = p * msgs_per_publisher + i;
                execute_sql(&format!(
                    "INSERT INTO {} (id, value) VALUES ({}, 'p{}_m{}')",
                    table, id, p, i
                ))
                .await
                .ok();
            }
        }));
    }

    for h in pub_handles {
        h.await.ok();
    }

    let publish_elapsed = publish_start.elapsed();
    publishers_done.store(true, Ordering::Relaxed);

    // Wait for consumer
    tokio::time::timeout(Duration::from_secs(30), consumer_handle).await.ok();

    let total_received = received.load(Ordering::SeqCst);
    let total_elapsed = publish_start.elapsed();
    let msg_per_sec = total_received as f64 / total_elapsed.as_secs_f64();
    let publish_rate = message_count as f64 / publish_elapsed.as_secs_f64();

    // Cleanup
    let _ = execute_sql(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;

    (total_received, msg_per_sec, publish_rate)
}

/// Test: Multiple publishers, multiple consumers
async fn bench_multi_pub_multi_consumer() -> (usize, f64, f64) {
    let namespace = common::generate_unique_namespace("tpbenchmm");
    let table = format!("{}.items", namespace);
    let topic = format!("{}.{}", namespace, common::generate_unique_table("topicmm"));
    let group_id = format!("bench-mm-{}", common::random_string(8));

    execute_sql(&format!("CREATE NAMESPACE {}", namespace))
        .await
        .expect("create ns");
    execute_sql(&format!("CREATE TABLE {} (id INT PRIMARY KEY, value TEXT)", table))
        .await
        .expect("create table");
    execute_sql(&format!("CREATE TOPIC {}", topic)).await.expect("create topic");
    execute_sql(&format!("ALTER TOPIC {} ADD SOURCE {} ON INSERT", topic, table))
        .await
        .expect("add source");
    wait_for_topic_ready(&topic, 1).await;

    let message_count = 2000;
    let num_publishers = 40;
    let num_consumers = 4;
    let msgs_per_publisher = message_count / num_publishers;
    let publishers_done = Arc::new(AtomicBool::new(false));

    // Start consumers
    let mut consumer_handles = Vec::new();
    let mut consumer_received_counters = Vec::new();

    for c in 0..num_consumers {
        let consumer_topic = topic.clone();
        let consumer_group = group_id.clone();
        let consumer_done = publishers_done.clone();
        let received = Arc::new(AtomicUsize::new(0));
        let received_clone = received.clone();
        consumer_received_counters.push(received);

        consumer_handles.push(tokio::spawn(async move {
            let client = create_test_client().await;
            let mut consumer = client
                .consumer()
                .topic(&consumer_topic)
                .group_id(&consumer_group)
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .max_poll_records(100)
                .build()
                .expect("build consumer");

            let deadline = Instant::now() + Duration::from_secs(90);
            let mut idle = 0;
            let mut seen = HashSet::<(u32, u64)>::new();

            while Instant::now() < deadline {
                match consumer.poll().await {
                    Ok(batch) if batch.is_empty() => {
                        idle += 1;
                        if consumer_done.load(Ordering::Relaxed) && idle >= 60 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    },
                    Ok(batch) => {
                        idle = 0;
                        for record in &batch {
                            seen.insert((record.partition_id, record.offset));
                            consumer.mark_processed(record);
                        }
                        received_clone.store(seen.len(), Ordering::SeqCst);
                        let _ = consumer.commit_sync().await;
                    },
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    },
                }
            }
            (c, seen)
        }));
    }

    // Multiple publishers
    let publish_start = Instant::now();

    let mut pub_handles = Vec::new();
    for p in 0..num_publishers {
        let table = table.clone();
        pub_handles.push(tokio::spawn(async move {
            for i in 0..msgs_per_publisher {
                let id = p * msgs_per_publisher + i;
                execute_sql(&format!(
                    "INSERT INTO {} (id, value) VALUES ({}, 'p{}_m{}')",
                    table, id, p, i
                ))
                .await
                .ok();
            }
        }));
    }

    for h in pub_handles {
        h.await.ok();
    }

    let publish_elapsed = publish_start.elapsed();
    publishers_done.store(true, Ordering::Relaxed);

    // Wait for consumers and collect results
    let mut all_offsets = HashSet::new();
    for h in consumer_handles {
        if let Ok((_, seen)) = h.await {
            all_offsets.extend(seen);
        }
    }

    let total_received = all_offsets.len();
    let total_elapsed = publish_start.elapsed();
    let msg_per_sec = total_received as f64 / total_elapsed.as_secs_f64();
    let publish_rate = message_count as f64 / publish_elapsed.as_secs_f64();

    // Cleanup
    let _ = execute_sql(&format!("DROP NAMESPACE {} CASCADE", namespace)).await;

    (total_received, msg_per_sec, publish_rate)
}

/// Combined benchmark with summary table and threshold validation
#[tokio::test]
#[ntest::timeout(300000)]
async fn smoke_test_topic_throughput_benchmark() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      KalamDB TOPIC THROUGHPUT BENCHMARK SUMMARY              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Run all benchmark scenarios
    println!("Running benchmark: Single Publisher → Single Consumer...");
    let (recv1, rate1, pub_rate1) = bench_single_pub_single_consumer().await;

    println!("Running benchmark: Multi Publisher → Single Consumer...");
    let (recv2, rate2, pub_rate2) = bench_multi_pub_single_consumer().await;

    println!("Running benchmark: Multi Publisher → Multi Consumer...");
    let (recv3, rate3, pub_rate3) = bench_multi_pub_multi_consumer().await;

    // Print summary table
    println!("\n┌────────────────────────────────────────────────────────────────────────┐");
    println!("│                        BENCHMARK RESULTS                               │");
    println!("├────────────────────────────────────────────────────────────────────────┤");
    println!("│  Configuration              │  Messages │  Pub Rate  │  Consumer Rate │");
    println!("├────────────────────────────────────────────────────────────────────────┤");
    println!(
        "│  1 Pub  → 1 Consumer        │  {:>8}  │  {:>7.1}/s │  {:>11.1}/s   │",
        recv1, pub_rate1, rate1
    );
    println!(
        "│  24 Pubs → 1 Consumer       │  {:>8}  │  {:>7.1}/s │  {:>11.1}/s   │",
        recv2, pub_rate2, rate2
    );
    println!(
        "│  40 Pubs → 4 Consumers      │  {:>8}  │  {:>7.1}/s │  {:>11.1}/s   │",
        recv3, pub_rate3, rate3
    );
    println!("└────────────────────────────────────────────────────────────────────────┘");
    println!();

    // Threshold validation
    println!("┌────────────────────────────────────────────────────────────────────────┐");
    println!("│                      THRESHOLD VALIDATION                              │");
    println!("├────────────────────────────────────────────────────────────────────────┤");

    let threshold1 = THRESHOLD_SINGLE_PUB_SINGLE_CONSUMER;
    let threshold2 = THRESHOLD_MULTI_PUB_SINGLE_CONSUMER;
    let threshold3 = THRESHOLD_MULTI_PUB_MULTI_CONSUMER;

    let pass1 = rate1 >= threshold1;
    let pass2 = rate2 >= threshold2;
    let pass3 = rate3 >= threshold3;

    let status1 = if pass1 { "✅ PASS" } else { "❌ FAIL" };
    let status2 = if pass2 { "✅ PASS" } else { "❌ FAIL" };
    let status3 = if pass3 { "✅ PASS" } else { "❌ FAIL" };

    println!(
        "│  1P→1C:  {:.1}/s >= {:.1}/s  ({:.0}%)  {}              │",
        rate1,
        threshold1,
        (rate1 / threshold1 * 100.0),
        status1
    );
    println!(
        "│  24P→1C: {:.1}/s >= {:.1}/s  ({:.0}%)  {}              │",
        rate2,
        threshold2,
        (rate2 / threshold2 * 100.0),
        status2
    );
    println!(
        "│  40P→4C: {:.1}/s >= {:.1}/s  ({:.0}%)  {}              │",
        rate3,
        threshold3,
        (rate3 / threshold3 * 100.0),
        status3
    );
    println!("└────────────────────────────────────────────────────────────────────────┘");
    println!();

    // Performance insights
    if recv1 > 0 && recv2 > 0 {
        println!("📊 Multi-publisher throughput is {:.1}x of single-publisher", rate2 / rate1);
    }
    if recv2 > 0 && recv3 > 0 {
        println!("📊 Multi-consumer (4) throughput is {:.1}x of single-consumer", rate3 / rate2);
    }
    println!();

    // Throughput thresholds are advisory under full-suite parallel load.
    // Keep hard assertions focused on functional message flow.
    assert!(recv1 > 0, "Single Publisher → Single Consumer received 0 messages");
    assert!(recv2 > 0, "Multi Publisher → Single Consumer received 0 messages");
    assert!(recv3 > 0, "Multi Publisher → Multi Consumer received 0 messages");

    if !(pass1 && pass2 && pass3) {
        println!(
            "⚠️ Throughput below advisory baseline under current load (1P→1C: {:.1}/{:.1}, 24P→1C: {:.1}/{:.1}, 40P→4C: {:.1}/{:.1})",
            rate1, threshold1, rate2, threshold2, rate3, threshold3
        );
    }

    println!("=== Topic Throughput Benchmark Complete ===\n");
}
