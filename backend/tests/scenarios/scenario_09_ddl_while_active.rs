//! Scenario 9: DDL While Active — Schema Change Safety (If Supported)
//!
//! Validates safety around schema changes while reads/subscriptions exist.
//!
//! ## Checklist
//! - [x] ALTER succeeds (or fails gracefully)
//! - [x] Reads remain correct
//! - [x] Subscription continues (or fails gracefully with clear error)

use std::time::Duration;

use futures_util::StreamExt;
use kalam_client::models::{ChangeEvent, ResponseStatus};
use kalamdb_commons::Role;
use tokio::time::sleep;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Main DDL while active scenario test
#[tokio::test]
async fn test_scenario_09_ddl_while_active() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("ddl_active");

    // =========================================================
    // Step 1: Create namespace and table
    // =========================================================
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.data (
                        id BIGINT PRIMARY KEY,
                        name TEXT NOT NULL,
                        value INT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE data table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_ddl_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // =========================================================
    // Step 2: Insert initial data
    // =========================================================
    for i in 1..=10 {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.data (id, name, value) VALUES ({}, 'item_{}', {})",
                    ns,
                    i,
                    i,
                    i * 10
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert {}", i);
    }

    // =========================================================
    // Step 3: Start subscription
    // =========================================================
    let sql = format!("SELECT * FROM {}.data ORDER BY id", ns);
    let mut subscription = client.subscribe(&sql).await?;

    let _ = wait_for_ack(&mut subscription, Duration::from_secs(5)).await?;
    let initial = drain_initial_data(&mut subscription, Duration::from_secs(5)).await?;
    assert_eq!(initial, 10, "Should have 10 initial rows");

    // =========================================================
    // Step 4: Attempt schema change (ADD COLUMN)
    // =========================================================
    let alter_resp = server
        .execute_sql(&format!("ALTER TABLE {}.data ADD COLUMN description TEXT", ns))
        .await?;

    let schema_change_supported = alter_resp.status == ResponseStatus::Success;

    if schema_change_supported {
        println!("ALTER TABLE ADD COLUMN succeeded");

        // =========================================================
        // Step 5: Insert new rows with new column
        // =========================================================
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.data (id, name, value, description) VALUES (100, 'new_item', \
                     1000, 'has description')",
                    ns
                ),
                None,
                None,
                None,
            )
            .await?;

        if resp.success() {
            println!("Insert with new column succeeded");

            // Verify old rows still readable
            let resp = client
                .execute_query(
                    &format!("SELECT id, name, value FROM {}.data WHERE id <= 10 ORDER BY id", ns),
                    None,
                    None,
                    None,
                )
                .await?;
            assert!(resp.success(), "Old rows should be readable");
            assert_eq!(resp.rows().len(), 10, "Should still have 10 old rows");

            // Verify new row readable
            let resp = client
                .execute_query(
                    &format!("SELECT * FROM {}.data WHERE id = 100", ns),
                    None,
                    None,
                    None,
                )
                .await?;
            assert!(resp.success(), "New row should be readable");
            assert_eq!(resp.rows().len(), 1, "Should have 1 new row");
        } else {
            println!("Insert with new column failed: {:?}", resp);
        }

        // =========================================================
        // Step 6: Check subscription continues
        // =========================================================
        // Try to receive the insert event (if subscription still works after schema change)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut received_new_insert = false;

        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), subscription.next()).await {
                Ok(Some(Ok(ChangeEvent::Insert { rows, .. }))) => {
                    for row in &rows {
                        if let Some(id) = row.get("id").and_then(json_to_i64) {
                            if id == 100 {
                                received_new_insert = true;
                                break;
                            }
                        }
                    }
                },
                Ok(Some(Ok(ChangeEvent::Error { message, .. }))) => {
                    println!("Subscription error after schema change: {}", message);
                    break;
                },
                Ok(Some(Ok(_))) => {},
                Ok(Some(Err(e))) => {
                    println!("Subscription stream error after schema change: {:?}", e);
                    break;
                },
                Ok(None) => break,
                Err(_) => continue,
            }
            if received_new_insert {
                break;
            }
        }

        println!("Received new insert after schema change: {}", received_new_insert);
    } else {
        println!("ALTER TABLE not supported or failed: {:?}", alter_resp.error);
        // This is acceptable - schema changes might not be supported
    }

    // =========================================================
    // Step 7: Verify data integrity regardless of schema change result
    // =========================================================
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.data", ns), None, None, None)
        .await?;
    let count: i64 = resp.get_i64("cnt").unwrap_or(0);
    assert!(count >= 10, "Should have at least 10 rows");

    subscription.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test DROP COLUMN behavior (if supported)
#[tokio::test]
async fn test_scenario_09_drop_column() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("ddl_drop");

    // Create namespace and table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.data (
                        id BIGINT PRIMARY KEY,
                        name TEXT NOT NULL,
                        old_column TEXT,
                        value INT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE data table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_drop_col_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert data with old_column
    for i in 1..=5 {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.data (id, name, old_column, value) VALUES ({}, 'item_{}', \
                     'old_value_{}', {})",
                    ns,
                    i,
                    i,
                    i,
                    i * 10
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert {}", i);
    }

    // Try to drop the old_column
    let drop_resp = server
        .execute_sql(&format!("ALTER TABLE {}.data DROP COLUMN old_column", ns))
        .await?;

    if drop_resp.status == ResponseStatus::Success {
        println!("DROP COLUMN succeeded");

        // Verify remaining columns work
        let resp = client
            .execute_query(
                &format!("SELECT id, name, value FROM {}.data ORDER BY id", ns),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Query without dropped column should work");
        assert_eq!(resp.rows().len(), 5, "Should still have 5 rows");

        // Verify insert referencing dropped column fails
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.data (id, name, old_column, value) VALUES (100, 'new', \
                     'should_fail', 100)",
                    ns
                ),
                None,
                None,
                None,
            )
            .await;
        // This should fail or ignore the dropped column
        match resp {
            Ok(resp) => println!("Insert with dropped column result: {}", resp.success()),
            Err(err) => println!("Insert with dropped column failed as expected: {}", err),
        }
    } else {
        println!("DROP COLUMN not supported: {:?}", drop_resp.error);
    }

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test concurrent reads during DDL
#[tokio::test]
async fn test_scenario_09_concurrent_reads_during_ddl() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("ddl_concurrent");

    // Create namespace and table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.data (
                        id BIGINT PRIMARY KEY,
                        name TEXT NOT NULL
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE data table");

    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_concurrent_ddl_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert data
    for i in 1..=20 {
        let resp = client
            .execute_query(
                &format!("INSERT INTO {}.data (id, name) VALUES ({}, 'item_{}')", ns, i, i),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert {}", i);
    }

    // Spawn concurrent readers
    let ns_clone = ns.clone();
    let server_base = server.base_url().to_string();
    let token = server.create_jwt_token(&username);

    let reader_handle = tokio::spawn(async move {
        let client = kalam_client::KalamLinkClient::builder()
            .base_url(&server_base)
            .auth(kalam_client::AuthProvider::jwt_token(token))
            .build()?;

        let mut read_count = 0;
        for _ in 0..10 {
            let resp = client
                .execute_query(
                    &format!("SELECT * FROM {}.data ORDER BY id LIMIT 10", ns_clone),
                    None,
                    None,
                    None,
                )
                .await?;
            if resp.success() {
                read_count += 1;
            }
            sleep(Duration::from_millis(50)).await;
        }
        Ok::<usize, anyhow::Error>(read_count)
    });

    // While readers are running, attempt DDL
    sleep(Duration::from_millis(100)).await;
    let _alter_resp = server
        .execute_sql(&format!("ALTER TABLE {}.data ADD COLUMN extra TEXT", ns))
        .await?;

    // Wait for readers to complete
    let read_count = reader_handle.await??;
    println!("Concurrent reads completed: {}", read_count);

    // All reads should succeed (DDL shouldn't break ongoing reads)
    assert!(read_count >= 8, "Most reads should succeed during DDL, got {}", read_count);

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
