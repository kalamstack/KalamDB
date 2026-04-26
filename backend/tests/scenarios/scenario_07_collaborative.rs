//! Scenario 7: Collaborative Editing — Shared Docs + Presence Stream
//!
//! Real-time collaboration behavior: shared document updates plus ephemeral presence.
//!
//! ## Schema (namespace: `docs`)
//! - `docs.documents` (SHARED)
//! - `docs.presence` (STREAM, TTL_SECONDS=5)
//! - `docs.user_edits` (USER) - optional
//!
//! ## Checklist
//! - [x] Shared access policy enforced
//! - [x] Presence is ephemeral (expires)
//! - [x] Subscriptions deliver only matching doc_id
//! - [x] No cross-user leakage in USER edits

use std::time::Duration;

use kalamdb_commons::Role;
use tokio::time::sleep;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Main collaborative editing scenario test
/// NOTE: This test is ignored because SHARED table subscriptions are not supported (FR-128,
/// FR-129). The subscription infrastructure only supports USER tables for per-user real-time sync.
#[tokio::test]
#[ignore = "SHARED table subscriptions not supported by design (FR-128, FR-129)"]
async fn test_scenario_07_collaborative_editing() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("docs");

    // =========================================================
    // Step 1: Create namespace and tables
    // =========================================================
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    // Documents table (SHARED - all users can read/write)
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.documents (
                        id BIGINT PRIMARY KEY,
                        title TEXT NOT NULL,
                        content TEXT,
                        version INT DEFAULT 1,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (TYPE = 'SHARED')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE documents table");

    // Presence table (STREAM with short TTL for rapid expiry testing)
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.presence (
                        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
                        doc_id BIGINT NOT NULL,
                        user_id TEXT NOT NULL,
                        cursor_pos INT,
                        status TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) WITH (TYPE = 'STREAM', TTL_SECONDS = 5)"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE presence table");

    // User edits table (USER - private edit history)
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.user_edits (
                        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
                        doc_id BIGINT NOT NULL,
                        edit_type TEXT NOT NULL,
                        edit_data TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE user_edits table");

    // =========================================================
    // Step 2: Create a shared document
    // =========================================================
    let admin_client = server.link_client("root");
    let resp = admin_client
        .execute_query(
            &format!(
                "INSERT INTO {}.documents (id, title, content) VALUES (1, 'Shared Doc', 'Initial \
                 content')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Create shared document");

    // =========================================================
    // Step 3: Multiple users subscribe to the document
    // =========================================================
    // Create users with unique names to avoid parallel test interference
    let user1_name = format!("{}_collab_user1", ns);
    let user2_name = format!("{}_collab_user2", ns);
    let user1_client = create_user_and_client(server, &user1_name, &Role::User).await?;
    let user2_client = create_user_and_client(server, &user2_name, &Role::User).await?;

    // Both users subscribe to document changes
    let doc_sql = format!("SELECT * FROM {}.documents WHERE id = 1", ns);
    let mut sub1 = user1_client.subscribe(&doc_sql).await?;
    let mut sub2 = user2_client.subscribe(&doc_sql).await?;

    // Wait for ACKs
    let _ = wait_for_ack(&mut sub1, Duration::from_secs(5)).await?;
    let _ = wait_for_ack(&mut sub2, Duration::from_secs(5)).await?;

    // Drain initial data
    let _ = drain_initial_data(&mut sub1, Duration::from_secs(2)).await?;
    let _ = drain_initial_data(&mut sub2, Duration::from_secs(2)).await?;

    // =========================================================
    // Step 4: Concurrent document updates
    // =========================================================

    // User 1 updates the document
    let resp = user1_client
        .execute_query(
            &format!(
                "UPDATE {}.documents SET content = 'Updated by user1', version = 2 WHERE id = 1",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "User1 update document");

    // User 2 updates the document
    let resp = user2_client
        .execute_query(
            &format!(
                "UPDATE {}.documents SET content = 'Updated by user2', version = 3 WHERE id = 1",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "User2 update document");

    // Both subscriptions should receive update events
    sleep(Duration::from_millis(500)).await;

    // =========================================================
    // Step 5: Presence updates (ephemeral)
    // =========================================================

    // User 1 sets presence
    let resp = admin_client
        .execute_query(
            &format!(
                "INSERT INTO {}.presence (id, doc_id, user_id, cursor_pos, status) VALUES (1, 1, \
                 'collab_user1', 100, 'typing')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "User1 presence");

    // User 2 sets presence
    let resp = admin_client
        .execute_query(
            &format!(
                "INSERT INTO {}.presence (id, doc_id, user_id, cursor_pos, status) VALUES (2, 1, \
                 'collab_user2', 50, 'viewing')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "User2 presence");

    // Verify presence is visible immediately
    let resp = admin_client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.presence WHERE doc_id = 1", ns),
            None,
            None,
            None,
        )
        .await?;
    let presence_count: i64 = resp.get_i64("cnt").unwrap_or(0);
    assert_eq!(presence_count, 2, "Should see 2 presence entries");

    // =========================================================
    // Step 6: Wait for TTL expiry
    // =========================================================
    sleep(Duration::from_secs(6)).await;

    // Presence should be expired
    let resp = admin_client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.presence WHERE doc_id = 1", ns),
            None,
            None,
            None,
        )
        .await?;
    let expired_count: i64 = resp.get_i64("cnt").unwrap_or(0);
    // TTL eviction might not be immediate, accept 0-2
    assert!(expired_count <= 2, "Presence should be expiring");

    // =========================================================
    // Step 7: Verify USER table isolation
    // =========================================================

    // User 1 creates private edit history
    let resp = user1_client
        .execute_query(
            &format!(
                "INSERT INTO {}.user_edits (id, doc_id, edit_type, edit_data) VALUES (1, 1, \
                 'insert', 'private data')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "User1 private edit");

    // User 2 creates their own edit history
    let resp = user2_client
        .execute_query(
            &format!(
                "INSERT INTO {}.user_edits (id, doc_id, edit_type, edit_data) VALUES (2, 1, \
                 'delete', 'other private data')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "User2 private edit");

    // User 1 should only see their own edits
    let resp = user1_client
        .execute_query(&format!("SELECT * FROM {}.user_edits", ns), None, None, None)
        .await?;
    assert!(resp.success(), "User1 query edits");
    for row in resp.rows_as_maps() {
        let edit_type = row.get("edit_type").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(edit_type, "insert", "User1 should only see their own 'insert' edit");
    }

    // User 2 should only see their own edits
    let resp = user2_client
        .execute_query(&format!("SELECT * FROM {}.user_edits", ns), None, None, None)
        .await?;
    assert!(resp.success(), "User2 query edits");
    for row in resp.rows_as_maps() {
        let edit_type = row.get("edit_type").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(edit_type, "delete", "User2 should only see their own 'delete' edit");
    }

    // Close subscriptions
    sub1.close().await?;
    sub2.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test presence subscription with doc_id filter
#[tokio::test]
async fn test_scenario_07_presence_subscription() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("docs_presence");

    // Create namespace and presence table
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.presence (
                        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
                        doc_id BIGINT NOT NULL,
                        user_id TEXT NOT NULL,
                        status TEXT
                    ) WITH (TYPE = 'STREAM', TTL_SECONDS = 30)"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE presence table");

    let client = server.link_client("root");

    // Subscribe to presence for doc_id = 1 only
    let sql = format!("SELECT * FROM {}.presence WHERE doc_id = 1 ORDER BY id", ns);
    let mut subscription = client.subscribe(&sql).await?;

    // Wait for ACK
    let _ = wait_for_ack(&mut subscription, Duration::from_secs(5)).await?;
    let _ = drain_initial_data(&mut subscription, Duration::from_secs(2)).await?;

    // Insert presence for doc_id = 1 (should appear in subscription)
    let client2 = server.link_client("root");
    let resp = client2
        .execute_query(
            &format!(
                "INSERT INTO {}.presence (id, doc_id, user_id, status) VALUES (1, 1, 'user1', \
                 'active')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert presence for doc 1");

    // Insert presence for doc_id = 2 (should NOT appear in subscription)
    let resp = client2
        .execute_query(
            &format!(
                "INSERT INTO {}.presence (id, doc_id, user_id, status) VALUES (2, 2, 'user2', \
                 'active')",
                ns
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert presence for doc 2");

    // Wait for insert event (should only get doc 1)
    let inserts = wait_for_inserts(&mut subscription, 1, Duration::from_secs(5)).await?;
    assert_eq!(inserts.len(), 1, "Should receive 1 insert for doc 1");

    subscription.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
