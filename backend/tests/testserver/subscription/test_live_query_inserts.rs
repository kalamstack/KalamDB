//! Integration test for Live Query INSERT detection via WebSocket

use super::test_support::consolidated_helpers::unique_namespace;
use futures_util::StreamExt;
use kalam_client::models::ChangeEvent;
use kalam_client::models::ResponseStatus;
use tokio::time::Duration;

/// Test basic INSERT detection via live query subscription
#[tokio::test]
async fn test_live_query_detects_inserts() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("test_inserts");
    let table = "messages";

    // Create namespace and table as root
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (
                            id TEXT PRIMARY KEY,
                            content TEXT,
                            priority INT,
                            created_at BIGINT
                        ) WITH (
                            TYPE = 'USER',
                            STORAGE_ID = 'local'
                        )",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Connect using the kalam-client SDK
    let client = server.link_client("root");

    // Subscribe to live query
    let sql = format!("SELECT * FROM {}.{} ORDER BY id", ns, table);
    let mut subscription = client.subscribe(&sql).await.expect("Failed to subscribe");

    // Insert 10 rows
    for i in 0..10 {
        let resp = server
                    .execute_sql(
                        &format!(
                            "INSERT INTO {}.{} (id, content, priority, created_at) VALUES ('msg{}', 'Content {}', {}, {})",
                            ns, table, i, i, i % 3, 1000 + i
                        )
                    )
                    .await?;
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    // Verify notifications + initial data (no duplicates expected)
    let mut inserts_received = 0;
    let mut initial_rows_received = 0;
    // Await notifications with timeout
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = subscription.next() => {
                match event {
                    Some(Ok(ChangeEvent::Insert { rows, .. })) => {
                        inserts_received += rows.len();
                        // rows is Vec<JsonValue>
                        // We expect 1 row per insert statement as we did single row inserts
                        // Wait, does the notification batch rows?
                        // Insert { rows: Vec<JsonValue> }
                        // It might contain multiple rows if batched, but we insert one by one.
                        // Just counting "Insert" events might be enough if 1 event = 1 insert query.
                        // Or we should count rows len.
                        // Note: Server sends 1 notification per transaction commit usually.
                        // We are doing separate execute_sql calls.
                    }
                    Some(Ok(ChangeEvent::Ack { .. })) => {
                        // Ignore Ack
                    }
                    Some(Ok(ChangeEvent::InitialDataBatch { rows, .. })) => {
                        initial_rows_received += rows.len();
                    }
                    Some(Ok(other)) => {
                        println!("Received other event: {:?}", other);
                    }
                    Some(Err(e)) => {
                        panic!("Subscription error: {:?}", e);
                    }
                    None => {
                        panic!("Subscription stream ended unexpectedly");
                    }
                }

                if inserts_received + initial_rows_received >= 10 {
                    break;
                }
            }
            _ = &mut timeout => {
                panic!(
                    "Timed out waiting for 10 insert rows. Got: {} (initial: {}, live: {})",
                    inserts_received + initial_rows_received,
                    initial_rows_received,
                    inserts_received
                );
            }
        }
    }

    assert_eq!(
        inserts_received + initial_rows_received,
        10,
        "Expected 10 insert rows across initial data and live notifications"
    );
    Ok(())
}
