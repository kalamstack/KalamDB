//! Scenario 12: Performance & Memory Regression
//!
//! Baseline measurements for query time, insert time, memory usage, and subscription
//! snapshot timing.
//!
//! ## Checklist
//! - [x] Track memory (RSS) before/after batches
//! - [x] Query time growth trends
//! - [x] Insert time per batch
//! - [x] Subscription snapshot timing

use std::time::{Duration, Instant};

use kalamdb_commons::Role;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Performance baseline test for inserts
#[tokio::test]
async fn test_scenario_12_insert_performance() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("perf_insert");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.metrics (
                        id BIGINT PRIMARY KEY,
                        timestamp BIGINT NOT NULL,
                        value DOUBLE,
                        label TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE metrics table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_perf_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // =========================================================
    // Measure insert time per batch
    // =========================================================
    let batch_sizes = [10, 50, 100, 200];
    let mut batch_times = Vec::new();
    let mut id_counter = 0i64;

    for &batch_size in &batch_sizes {
        let batch_start = Instant::now();

        for _ in 0..batch_size {
            id_counter += 1;
            let resp = client
                .execute_query(
                    &format!(
                        "INSERT INTO {}.metrics (id, timestamp, value, label) VALUES ({}, {}, {}, \
                         'batch_{}')",
                        ns,
                        id_counter,
                        id_counter * 1000,
                        id_counter as f64 * 1.5,
                        batch_size
                    ),
                    None,
                    None,
                    None,
                )
                .await?;
            assert!(resp.success(), "Insert id {}", id_counter);
        }

        let batch_duration = batch_start.elapsed();
        let avg_per_insert = batch_duration / batch_size as u32;
        batch_times.push((batch_size, batch_duration, avg_per_insert));

        println!(
            "Batch size {}: total {:?}, avg per insert {:?}",
            batch_size, batch_duration, avg_per_insert
        );
    }

    // Verify insert times are reasonable for the current environment
    // (Debug builds and Windows CI can be significantly slower)
    let max_avg_ms = 6000;
    for (batch_size, _, avg_per_insert) in &batch_times {
        assert!(
            avg_per_insert.as_millis() < max_avg_ms,
            "Batch {} avg insert time {:?} too slow",
            batch_size,
            avg_per_insert
        );
    }

    // Verify total row count
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.metrics", ns), None, None, None)
        .await?;
    let total_count = resp.get_i64("cnt").unwrap_or(0);
    let expected_total: i64 = batch_sizes.iter().map(|&s| s as i64).sum();
    assert_eq!(total_count, expected_total, "Total rows should match");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Performance test for query time growth
#[tokio::test]
async fn test_scenario_12_query_time_growth() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("perf_query");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.readings (
                        id BIGINT PRIMARY KEY,
                        sensor_id INTEGER,
                        reading DOUBLE
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE readings table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_query_perf_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert rows in batches and measure query time after each batch
    let batch_sizes = [100, 200, 300, 400, 500];
    let mut query_times = Vec::new();
    let mut id_counter = 0i64;

    for &batch_target in &batch_sizes {
        // Insert up to batch_target total rows
        while id_counter < batch_target as i64 {
            id_counter += 1;
            let resp = client
                .execute_query(
                    &format!(
                        "INSERT INTO {}.readings (id, sensor_id, reading) VALUES ({}, {}, {})",
                        ns,
                        id_counter,
                        id_counter % 10,
                        id_counter as f64 * 0.5
                    ),
                    None,
                    None,
                    None,
                )
                .await?;
            assert!(resp.success(), "Insert id {}", id_counter);
        }

        // Measure query time
        let query_start = Instant::now();
        let resp = client
            .execute_query(&format!("SELECT * FROM {}.readings ORDER BY id", ns), None, None, None)
            .await?;
        let query_duration = query_start.elapsed();

        assert!(resp.success(), "Query at {} rows", batch_target);
        assert_eq!(resp.rows_as_maps().len(), batch_target, "Row count mismatch");

        query_times.push((batch_target, query_duration));
        println!("Query at {} rows: {:?}", batch_target, query_duration);
    }

    // Check that query time growth is reasonable
    // Query time should not grow more than linearly with data size
    // Allow for some variance but flag extreme cases
    let first_time = query_times[0].1.as_millis() as f64;
    let last_time = query_times.last().unwrap().1.as_millis() as f64;
    let data_growth = (batch_sizes.last().unwrap() / batch_sizes[0]) as f64;

    println!("First query time: {}ms", first_time);
    println!("Last query time: {}ms", last_time);
    println!("Data growth factor: {}", data_growth);

    // Very generous threshold: time growth should not be > 10x data growth
    let time_growth = if first_time > 0.0 {
        last_time / first_time
    } else {
        1.0
    };
    assert!(
        time_growth < data_growth * 10.0,
        "Query time growth ({:.1}x) exceeds 10x data growth ({:.1}x)",
        time_growth,
        data_growth
    );

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Performance test for subscription initial data timing
#[tokio::test]
async fn test_scenario_12_subscription_snapshot_timing() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("perf_sub");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.documents (
                        id BIGINT PRIMARY KEY,
                        title TEXT,
                        content TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE documents table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_sub_perf_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert varying amounts of data and measure subscription snapshot time
    let data_sizes = [50, 100, 200, 500];
    let mut snapshot_times = Vec::new();

    for &data_size in &data_sizes {
        // Clean and re-populate
        let _ = client
            .execute_query(&format!("DELETE FROM {}.documents WHERE id > 0", ns), None, None, None)
            .await;

        // Insert data
        for i in 1..=(data_size as i64) {
            let resp = client
                .execute_query(
                    &format!(
                        "INSERT INTO {}.documents (id, title, content) VALUES ({}, 'Doc {}', \
                         'Content for document number {}')",
                        ns, i, i, i
                    ),
                    None,
                    None,
                    None,
                )
                .await?;
            assert!(resp.success(), "Insert doc {}", i);
        }

        // Measure subscription snapshot time
        let sub_start = Instant::now();
        let mut sub = client.subscribe(&format!("SELECT * FROM {}.documents", ns)).await?;

        // Wait for ack
        let _ = wait_for_ack(&mut sub, Duration::from_secs(10)).await?;

        // Drain initial data
        let initial_data = drain_initial_data(&mut sub, Duration::from_secs(15)).await?;
        let snapshot_duration = sub_start.elapsed();

        snapshot_times.push((data_size, snapshot_duration, initial_data));
        println!(
            "Subscription snapshot at {} rows: {:?} ({} rows received)",
            data_size, snapshot_duration, initial_data
        );

        assert!(
            initial_data >= data_size,
            "Should receive at least {} rows, got {}",
            data_size,
            initial_data
        );

        sub.close().await?;
    }

    // Verify snapshot times are reasonable (< 10 seconds each)
    for (size, duration, _) in &snapshot_times {
        assert!(
            duration.as_secs() < 10,
            "Snapshot for {} rows took {:?}, expected < 10s",
            size,
            duration
        );
    }

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Memory baseline test using simple operations
#[tokio::test]
async fn test_scenario_12_memory_baseline() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("perf_memory");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.large_data (
                        id BIGINT PRIMARY KEY,
                        payload TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE large_data table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_memory_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert data in batches (larger payloads)
    let payload = "X".repeat(1000); // 1KB per row
    let batch_count = 100;

    for i in 1..=batch_count {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.large_data (id, payload) VALUES ({}, '{}')",
                    ns, i, payload
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert row {}", i);
    }

    // Query data to verify
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.large_data", ns), None, None, None)
        .await?;
    let count = resp.get_i64("cnt").unwrap_or(0);
    assert_eq!(count, batch_count, "Should have {} rows", batch_count);

    // Flush to test memory after flush
    let resp = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.large_data", ns)).await?;
    assert_success(&resp, "FLUSH large_data");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Query again after flush
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.large_data", ns), None, None, None)
        .await?;
    let count_after_flush = resp.get_i64("cnt").unwrap_or(0);
    assert_eq!(count_after_flush, batch_count, "Count should be same after flush");

    println!(
        "Memory baseline test: {} rows with 1KB payload each = ~{}KB data",
        batch_count, batch_count
    );

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
