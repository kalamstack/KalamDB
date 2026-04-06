//! Integration test for Live Query DELETE detection via WebSocket

use super::test_support::consolidated_helpers::unique_namespace;
use kalam_client::models::ChangeEvent;
use kalam_client::models::ResponseStatus;
use tokio::time::Duration;

/// Test DELETE detection
#[tokio::test]
async fn test_live_query_detects_deletes() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("test_deletes");
    let table = "records";

    // Setup namespace and table as root
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.{} (
                            id TEXT PRIMARY KEY,
                            data TEXT
                        ) WITH (
                            TYPE = 'USER',
                            STORAGE_ID = 'local'
                        )",
            ns, table
        ))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Insert rows
    for i in 0..5 {
        let resp = server
            .execute_sql(&format!(
                "INSERT INTO {}.{} (id, data) VALUES ('rec{}', 'Data {}')",
                ns, table, i, i
            ))
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    // Connect using the kalam-client SDK
    let client = server.link_client("root");

    let sql = format!("SELECT * FROM {}.{}", ns, table);
    let mut subscription = client.subscribe(&sql).await.expect("Failed to subscribe");

    // Consume Initial Data
    let mut initial_count = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
             event = subscription.next() => {
                match event {
                    Some(Ok(ChangeEvent::InitialDataBatch { rows, batch_control, .. })) => {
                        initial_count += rows.len();
                        if !batch_control.has_more {
                            break;
                        }
                    }
                    Some(Ok(ChangeEvent::Ack { .. })) => {}
                    Some(Ok(_)) => {}
                    Some(Err(e)) => panic!("Error receiving initial data: {:?}", e),
                    None => panic!("Stream ended before initial data"),
                }
             }
             _ = &mut timeout => panic!("Timed out waiting for initial data"),
        }
    }
    assert_eq!(initial_count, 5, "Should have 5 initial rows");

    // Delete one row
    let resp = server
        .execute_sql(&format!("DELETE FROM {}.{} WHERE id = 'rec2'", ns, table))
        .await?;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Receive DELETE notification
    let mut delete_received = false;
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            event = subscription.next() => {
                match event {
                    Some(Ok(ChangeEvent::Delete { old_rows, .. })) => {
                        delete_received = true;
                        assert!(!old_rows.is_empty());
                        let old_row = &old_rows[0];
                        assert_eq!(old_row.get("id").and_then(|v| v.as_str()), Some("rec2"));
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => panic!("Error receiving delete: {:?}", e),
                    None => panic!("Stream ended before delete"),
                }
            }
            _ = &mut timeout => panic!("Timed out waiting for delete"),
        }
    }

    assert!(delete_received, "Should have received DELETE notification");
    Ok(())
}
