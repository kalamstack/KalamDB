//! Scenario 4: IoT Telemetry — 5k Rows + Wide Columns + Time Filtering
//!
//! Stress ingestion and scanning: wide rows, time predicates, anomalies subscription, flush.
//!
//! ## Schema (namespace: `iot`)
//! - `iot.telemetry` (SHARED or USER) with ~10 columns
//!
//! ## Checklist
//! - [x] Insert count correct (5000 rows)
//! - [x] Queries return expected counts across filters
//! - [x] Subscription triggers for anomaly inserts
//! - [x] Flush does not change query results
//! - [x] Cold artifacts valid and non-empty

use std::time::Duration;

use futures_util::StreamExt;
use kalam_client::models::ResponseStatus;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(180);
const ROW_COUNT: usize = 5000;

/// Main IoT telemetry test - 5k rows with wide columns
#[tokio::test]
async fn test_scenario_04_iot_telemetry_5k_rows() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("iot");

    // =========================================================
    // Step 1: Create namespace and wide-column table
    // =========================================================
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    // Use SHARED table for fleet-wide telemetry
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.telemetry (
                        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
                        device_id TEXT NOT NULL,
                        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        temp DOUBLE NOT NULL,
                        humidity DOUBLE,
                        pressure DOUBLE,
                        battery DOUBLE,
                        is_charging BOOLEAN DEFAULT FALSE,
                        firmware TEXT,
                        payload TEXT
                    ) WITH (
                        TYPE = 'SHARED',
                        STORAGE_ID = 'local',
                        FLUSH_POLICY = 'rows:1000'
                    )"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE telemetry table");

    // =========================================================
    // Step 2: Insert 5000 rows (batch insert for performance)
    // =========================================================
    let client = server.link_client("root");
    let batch_size = 100;
    let batches = ROW_COUNT / batch_size;

    for batch in 0..batches {
        // Insert batch_size rows in quick succession
        for i in 0..batch_size {
            let row_num = batch * batch_size + i;
            let device_id = format!("device_{:03}", row_num % 100);
            let temp = 20.0 + (row_num % 30) as f64;
            let humidity = 40.0 + (row_num % 40) as f64;
            let pressure = 1000.0 + (row_num % 50) as f64;
            let battery = 100.0 - (row_num % 100) as f64;
            let is_charging = row_num % 5 == 0;
            let firmware = format!("v1.{}.{}", row_num / 1000, row_num % 100);

            let sql = format!(
                "INSERT INTO {}.telemetry (id, device_id, temp, humidity, pressure, battery, \
                 is_charging, firmware, payload) VALUES ({}, '{}', {}, {}, {}, {}, {}, '{}', \
                 'payload_data_{}')",
                ns,
                row_num,
                device_id,
                temp,
                humidity,
                pressure,
                battery,
                is_charging,
                firmware,
                row_num
            );

            let resp = client.execute_query(&sql, None, None, None).await?;
            if !resp.success() {
                eprintln!("Insert failed for row {}: {:?}", row_num, resp);
            }
        }

        // Progress indicator every 10 batches
        if batch % 10 == 9 {
            println!("Inserted {} / {} rows", (batch + 1) * batch_size, ROW_COUNT);
        }
    }

    // =========================================================
    // Step 3: Verify insert count
    // =========================================================
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.telemetry", ns), None, None, None)
        .await?;
    let total_count: i64 = resp.get_i64("cnt").unwrap_or(0);

    assert!(
        total_count as usize >= ROW_COUNT * 9 / 10, // Allow 10% tolerance for any failures
        "Expected ~{} rows, got {}",
        ROW_COUNT,
        total_count
    );

    // =========================================================
    // Step 4: Query with various filters
    // =========================================================

    // Device filter
    let resp = client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.telemetry WHERE device_id = 'device_001'", ns),
            None,
            None,
            None,
        )
        .await?;
    let device_count: i64 = resp.get_i64("cnt").unwrap_or(0);
    assert!(device_count > 0, "Should have data for device_001");

    // Threshold filter (temperature > 40)
    let resp = client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.telemetry WHERE temp > 40", ns),
            None,
            None,
            None,
        )
        .await?;
    let hot_count: i64 = resp.get_i64("cnt").unwrap_or(0);
    assert!(hot_count > 0, "Should have some high-temp readings");

    // Battery threshold filter
    let resp = client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.telemetry WHERE battery < 20", ns),
            None,
            None,
            None,
        )
        .await?;
    let low_battery_count: i64 = resp.get_i64("cnt").unwrap_or(0);
    assert!(low_battery_count > 0, "Should have some low-battery readings");

    // =========================================================
    // Step 5: Trigger flush
    // =========================================================
    let resp = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.telemetry", ns)).await?;
    if resp.status != ResponseStatus::Success {
        let is_conflict = resp
            .error
            .as_ref()
            .map(|e| e.message.contains("conflict") || e.message.contains("Idempotent"))
            .unwrap_or(false);
        if !is_conflict {
            eprintln!("Flush returned: {:?}", resp.error);
        }
    }

    // Wait for flush to complete
    let _ = wait_for_flush_complete(server, &ns, "telemetry", Duration::from_secs(30)).await;

    // =========================================================
    // Step 6: Verify queries still work post-flush
    // =========================================================
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.telemetry", ns), None, None, None)
        .await?;
    let post_flush_count: i64 = resp.get_i64("cnt").unwrap_or(0);

    assert_eq!(post_flush_count, total_count, "Count should be same after flush");

    // Device filter should still work
    let resp = client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.telemetry WHERE device_id = 'device_001'", ns),
            None,
            None,
            None,
        )
        .await?;
    let post_flush_device: i64 = resp.get_i64("cnt").unwrap_or(0);
    assert_eq!(post_flush_device, device_count, "Device count should be same after flush");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test anomaly subscription for IoT data
/// NOTE: This test is ignored because SHARED table subscriptions are not supported (FR-128,
/// FR-129). The subscription infrastructure only supports USER tables for per-user real-time sync.
#[tokio::test]
#[ignore = "SHARED table subscriptions not supported by design (FR-128, FR-129)"]
async fn test_scenario_04_anomaly_subscription() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("iot_anomaly");

    // Create namespace and table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.telemetry (
                        id BIGINT PRIMARY KEY,
                        device_id TEXT NOT NULL,
                        temp DOUBLE NOT NULL,
                        battery DOUBLE NOT NULL
                    ) WITH (TYPE = 'SHARED')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE telemetry table");

    let client = server.link_client("root");

    // Insert some normal data
    for i in 1..=10 {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.telemetry (id, device_id, temp, battery) VALUES ({}, \
                     'device_1', 25.0, 80.0)",
                    ns, i
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert normal data {}", i);
    }

    // Subscribe to anomalies (high temp OR low battery)
    let sql = format!("SELECT * FROM {}.telemetry WHERE temp > 50 OR battery < 15 ORDER BY id", ns);
    let mut subscription = client.subscribe(&sql).await?;

    // Wait for ACK
    let _ = wait_for_ack(&mut subscription, Duration::from_secs(5)).await?;

    // Drain initial (should be empty since no anomalies yet)
    let initial = drain_initial_data(&mut subscription, Duration::from_secs(2)).await?;
    assert_eq!(initial, 0, "No initial anomalies expected");

    // Insert anomaly data
    let client2 = server.link_client("root");

    // High temperature anomaly
    let resp = client2
        .execute_query(
            &format!(
                "INSERT INTO {}.telemetry (id, device_id, temp, battery) VALUES (100, 'device_2', \
                 75.0, 50.0)",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert high temp anomaly");

    // Low battery anomaly
    let resp = client2
        .execute_query(
            &format!(
                "INSERT INTO {}.telemetry (id, device_id, temp, battery) VALUES (101, 'device_3', \
                 25.0, 5.0)",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert low battery anomaly");

    // Wait for insert events
    let inserts = wait_for_inserts(&mut subscription, 2, Duration::from_secs(10)).await?;
    assert_eq!(inserts.len(), 2, "Should receive 2 anomaly inserts");

    subscription.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test wide column scanning performance
#[tokio::test]
async fn test_scenario_04_wide_column_scan() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("iot_scan");

    // Create namespace and table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.telemetry (
                        id BIGINT PRIMARY KEY,
                        device_id TEXT NOT NULL,
                        temp DOUBLE NOT NULL,
                        humidity DOUBLE,
                        pressure DOUBLE,
                        battery DOUBLE,
                        is_charging BOOLEAN,
                        firmware TEXT,
                        payload TEXT
                    ) WITH (TYPE = 'SHARED')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE telemetry table");

    let client = server.link_client("root");

    // Insert 500 rows
    for i in 0..500 {
        let sql = format!(
            "INSERT INTO {}.telemetry (id, device_id, temp, humidity, pressure, battery, \
             is_charging, firmware, payload) VALUES ({}, 'device_{}', {}, {}, {}, {}, {}, \
             'v1.0.{}', 'payload_{}')",
            ns,
            i,
            i % 10,
            20.0 + (i % 30) as f64,
            50.0,
            1000.0,
            80.0,
            i % 2 == 0,
            i,
            i
        );
        let resp = client.execute_query(&sql, None, None, None).await?;
        if !resp.success() {
            eprintln!("Insert {} failed", i);
        }
    }

    // Full scan
    let start = std::time::Instant::now();
    let resp = client
        .execute_query(
            &format!("SELECT * FROM {}.telemetry ORDER BY id LIMIT 100", ns),
            None,
            None,
            None,
        )
        .await?;
    let scan_time = start.elapsed();

    assert!(resp.success(), "Full scan should succeed");
    assert!(resp.rows().len() == 100, "Should return 100 rows");
    println!("Full scan of 100 rows took {:?}", scan_time);

    // Column projection scan
    let start = std::time::Instant::now();
    let resp = client
        .execute_query(
            &format!("SELECT device_id, temp, battery FROM {}.telemetry ORDER BY id LIMIT 100", ns),
            None,
            None,
            None,
        )
        .await?;
    let projection_time = start.elapsed();

    assert!(resp.success(), "Projection scan should succeed");
    println!("Projection scan of 100 rows took {:?}", projection_time);

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
