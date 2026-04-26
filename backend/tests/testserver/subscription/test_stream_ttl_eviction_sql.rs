//! Stream TTL eviction test using SQL script
//!
//! This test executes the SQL script from test_stream_ttl.sql to validate
//! that stream tables with TTL properly evict old events.

use kalam_client::models::ResponseStatus;
use tokio::time::{sleep, Duration};

use super::test_support::consolidated_helpers::unique_namespace;

/// Test stream table TTL eviction using the SQL script approach
#[tokio::test]
async fn test_stream_ttl_eviction_from_sql_script() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("test_stream_ttl");
    let table = "events";

    // Create namespace
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Create stream table with 2-second TTL (matching the SQL script intent)
    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (
                        event_id TEXT PRIMARY KEY,
                        event_type TEXT,
                        timestamp BIGINT
                    ) WITH (
                        TYPE = 'STREAM',
                        TTL_SECONDS = 2,
                        STORAGE_ID = 'local'
                    )",
            ns, table
        ))
        .await?;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to create stream table: {:?}",
        resp.error
    );

    // Insert test events (matching SQL script)
    let resp = server
        .execute_sql(&format!(
            "INSERT INTO {}.{} (event_id, event_type, timestamp) VALUES ('evt1', 'click', 1000)",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "INSERT INTO {}.{} (event_id, event_type, timestamp) VALUES ('evt2', 'view', 2000)",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "INSERT INTO {}.{} (event_id, event_type, timestamp) VALUES ('evt3', 'purchase', 3000)",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Verify events are there
    let resp = server
        .execute_sql(&format!("SELECT * FROM {}.{} ORDER BY event_id", ns, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    let event_ids: std::collections::HashSet<_> = rows
        .iter()
        .filter_map(|row| row.get("event_id").and_then(|v| v.as_str()))
        .collect();
    for expected in ["evt1", "evt2", "evt3"] {
        assert!(event_ids.contains(expected), "Missing expected event_id {}", expected);
    }

    // Wait for TTL to expire (need to wait 3+ seconds for eviction to run)
    // The eviction job runs periodically, so we need to wait a bit longer
    sleep(Duration::from_secs(5)).await;

    // Query again - should have fewer or no events (depending on eviction timing)
    let resp = server.execute_sql(&format!("SELECT * FROM {}.{}", ns, table)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);
    let rows_after = resp.rows_as_maps();

    // After TTL expiration, we should have fewer rows than initially
    // (The exact count depends on eviction job timing, but it should be less than 3)
    assert!(
        rows_after.len() < 3,
        "Expected some events to be evicted, but still have {} events",
        rows_after.len()
    );

    println!(
        "Stream TTL eviction test: {} events before TTL, {} events after",
        3,
        rows_after.len()
    );
    Ok(())
}
