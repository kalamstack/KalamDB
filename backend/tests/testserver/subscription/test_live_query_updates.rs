//! Integration test for Live Query UPDATE detection via WebSocket

use super::test_support::consolidated_helpers::unique_namespace;
use kalam_client::models::ChangeEvent;
use kalam_client::models::ResponseStatus;
use tokio::time::Duration;

/// Test UPDATE detection with old/new values
#[tokio::test]
async fn test_live_query_detects_updates() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("test_updates");
    let table = "tasks";

    // Setup namespace and table as root
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (
                            id TEXT PRIMARY KEY,
                            title TEXT,
                            status TEXT,
                            updated_at BIGINT
                        ) WITH (
                            TYPE = 'USER',
                            STORAGE_ID = 'local'
                        )",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Insert initial row
    let resp = server
                .execute_sql(
                    &format!(
                        "INSERT INTO {}.{} (id, title, status, updated_at) VALUES ('task1', 'Test Task', 'pending', 1000)",
                        ns, table
                    )
                )
                .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Connect using the kalam-client SDK
    let client = server.link_client("root");

    let sql = format!("SELECT * FROM {}.{}", ns, table);
    let mut subscription = client.subscribe(&sql).await.expect("Failed to subscribe");

    // Consume Initial Data
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
             event = subscription.next() => {
                match event {
                    Some(Ok(ChangeEvent::InitialDataBatch { rows, batch_control, .. })) => {
                        assert_eq!(rows.len(), 1, "Should have 1 initial row");
                        if !batch_control.has_more {
                            break;
                        }
                    }
                    Some(Ok(ChangeEvent::Ack { .. })) => {}
                    Some(Ok(_other)) => {
                        // Ignore other events
                    }
                    Some(Err(e)) => panic!("Error receiving initial data: {:?}", e),
                    None => panic!("Stream ended before initial data"),
                }
             }
             _ = &mut timeout => panic!("Timed out waiting for initial data"),
        }
    }

    // Update the row
    let resp = server
        .execute_sql(&format!(
            "UPDATE {}.{} SET status = 'completed', updated_at = 2000 WHERE id = 'task1'",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Receive update notification
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
                    event = subscription.next() => {
                        match event {
                            Some(Ok(ChangeEvent::Update { rows, old_rows, .. })) => {
                                assert_eq!(rows.len(), 1);
                                assert_eq!(old_rows.len(), 1);

                                let new_row = &rows[0];
                                let old_row = &old_rows[0];

                                // Verify new values
                                assert_eq!(new_row.get("status").and_then(|v| v.as_str()), Some("completed"));
                                // BigInts are serialized as strings in JSON
                                let updated_at = new_row.get("updated_at")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok());
                                assert_eq!(updated_at, Some(2000));

                                // Verify old values
                                assert_eq!(old_row.get("status").and_then(|v| v.as_str()), Some("pending"));
                                let old_updated_at = old_row.get("updated_at")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok());
                                assert_eq!(old_updated_at, Some(1000));

                                break;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => panic!("Error receiving update: {:?}", e),
                            None => panic!("Stream ended before update"),
                        }
                    }
                    _ = &mut timeout => panic!("Timed out waiting for update"),
        }
    }
    Ok(())
}

/// Test UPDATE detection for subscriptions that use LIKE filters
#[tokio::test]
#[ntest::timeout(15000)]
async fn test_live_query_detects_updates_with_like_filter() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("test_updates_like");
    let table = "metrics";

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (
                            id TEXT PRIMARY KEY,
                            metric_name TEXT,
                            metric_value BIGINT,
                            updated_at BIGINT
                        ) WITH (
                            TYPE = 'USER',
                            STORAGE_ID = 'local'
                        )",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "INSERT INTO {}.{} (id, metric_name, metric_value, updated_at) VALUES ('metric1', 'open_files_other', 10, 1000)",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let client = server.link_client("root");
    let sql = format!("SELECT * FROM {}.{} WHERE metric_name LIKE 'open_files_%'", ns, table);
    let mut subscription = client.subscribe(&sql).await.expect("Failed to subscribe");

    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = subscription.next() => {
                match event {
                    Some(Ok(ChangeEvent::InitialDataBatch { rows, batch_control, .. })) => {
                        assert_eq!(rows.len(), 1, "Should have 1 initial matching row");
                        assert_eq!(
                            rows[0].get("metric_name").and_then(|value| value.as_str()),
                            Some("open_files_other")
                        );
                        if !batch_control.has_more {
                            break;
                        }
                    }
                    Some(Ok(ChangeEvent::Ack { .. })) => {}
                    Some(Ok(_)) => {}
                    Some(Err(error)) => panic!("Error receiving initial data: {:?}", error),
                    None => panic!("Stream ended before initial data"),
                }
            }
            _ = &mut timeout => panic!("Timed out waiting for initial data"),
        }
    }

    let resp = server
        .execute_sql(&format!(
            "UPDATE {}.{} SET metric_value = 11, updated_at = 2000 WHERE id = 'metric1'",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = subscription.next() => {
                match event {
                    Some(Ok(ChangeEvent::Update { rows, old_rows, .. })) => {
                        assert_eq!(rows.len(), 1, "Should receive one matching update row");

                        let new_row = &rows[0];

                        assert_eq!(
                            new_row.get("metric_name").and_then(|value| value.as_str()),
                            Some("open_files_other")
                        );
                        assert_eq!(
                            new_row
                                .get("metric_value")
                                .and_then(|value| value.as_str())
                                .and_then(|value| value.parse::<i64>().ok()),
                            Some(11)
                        );
                        if let Some(old_row) = old_rows.first() {
                            assert_eq!(
                                old_row
                                    .get("metric_value")
                                    .and_then(|value| value.as_str())
                                    .and_then(|value| value.parse::<i64>().ok()),
                                Some(10)
                            );
                        }
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => panic!("Error receiving update: {:?}", error),
                    None => panic!("Stream ended before update"),
                }
            }
            _ = &mut timeout => panic!("Timed out waiting for filtered update"),
        }
    }

    Ok(())
}
