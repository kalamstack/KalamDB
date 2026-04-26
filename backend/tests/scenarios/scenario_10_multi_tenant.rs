//! Scenario 10: Multi-Namespace Multi-Tenant — Controlled Sharing
//!
//! Validates isolation across namespaces/tenants and explicitly shared tables.
//!
//! ## Checklist
//! - [x] Namespace boundaries enforced
//! - [x] Shared table access correct
//! - [x] No subscription data leakage

use std::time::Duration;

use kalamdb_commons::Role;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Main multi-tenant scenario test
#[tokio::test]
async fn test_scenario_10_multi_tenant_isolation() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    // =========================================================
    // Step 1: Create multiple tenant namespaces
    // =========================================================
    let tenant_a = unique_ns("tenant_a");
    let tenant_b = unique_ns("tenant_b");
    let global = unique_ns("global");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", tenant_a)).await?;
    assert_success(&resp, "CREATE tenant_a namespace");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", tenant_b)).await?;
    assert_success(&resp, "CREATE tenant_b namespace");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", global)).await?;
    assert_success(&resp, "CREATE global namespace");

    // =========================================================
    // Step 2: Create USER tables in each tenant namespace
    // =========================================================
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.orders (
                        id BIGINT PRIMARY KEY,
                        customer_name TEXT NOT NULL,
                        amount DOUBLE
                    ) WITH (TYPE = 'USER')"#,
            tenant_a
        ))
        .await?;
    assert_success(&resp, "CREATE tenant_a.orders");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.orders (
                        id BIGINT PRIMARY KEY,
                        customer_name TEXT NOT NULL,
                        amount DOUBLE
                    ) WITH (TYPE = 'USER')"#,
            tenant_b
        ))
        .await?;
    assert_success(&resp, "CREATE tenant_b.orders");

    // =========================================================
    // Step 3: Create SHARED table in global namespace
    // =========================================================
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.feature_flags (
                        flag_name TEXT PRIMARY KEY,
                        enabled BOOLEAN NOT NULL,
                        description TEXT
                    ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'PUBLIC')"#,
            global
        ))
        .await?;
    assert_success(&resp, "CREATE global.feature_flags");

    // Seed feature flags
    let admin_client = server.link_client("root");
    for (flag, enabled) in [
        ("dark_mode", true),
        ("beta_features", false),
        ("analytics", true),
    ] {
        let resp = admin_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.feature_flags (flag_name, enabled, description) VALUES ('{}', \
                     {}, 'Flag: {}')",
                    global, flag, enabled, flag
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert flag {}", flag);
    }

    // =========================================================
    // Step 4: Tenant A inserts their data
    // =========================================================
    let tenant_a_user = format!("{}_tenant_a_user", tenant_a);
    let tenant_b_user = format!("{}_tenant_b_user", tenant_b);
    let tenant_a_client = create_user_and_client(server, &tenant_a_user, &Role::User).await?;
    for i in 1..=5 {
        let resp = tenant_a_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.orders (id, customer_name, amount) VALUES ({}, 'A Customer \
                     {}', {})",
                    tenant_a,
                    i,
                    i,
                    (i as f64) * 100.0
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Tenant A insert order {}", i);
    }

    // =========================================================
    // Step 5: Tenant B inserts their data
    // =========================================================
    let tenant_b_client = create_user_and_client(server, &tenant_b_user, &Role::User).await?;
    for i in 101..=105 {
        let resp = tenant_b_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.orders (id, customer_name, amount) VALUES ({}, 'B Customer \
                     {}', {})",
                    tenant_b,
                    i,
                    i,
                    (i as f64) * 50.0
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Tenant B insert order {}", i);
    }

    // =========================================================
    // Step 6: Verify tenant isolation (A cannot see B's data)
    // =========================================================
    let resp = tenant_a_client
        .execute_query(&format!("SELECT * FROM {}.orders", tenant_a), None, None, None)
        .await?;
    assert!(resp.success(), "Tenant A query own orders");
    assert_eq!(resp.rows_as_maps().len(), 5, "Tenant A should see 5 orders");

    // Check all customer names start with "A"
    for row in &resp.rows_as_maps() {
        let name = row.get("customer_name").and_then(|v| v.as_str()).unwrap_or("");
        assert!(
            name.starts_with("A Customer"),
            "Tenant A should only see A customers, got: {}",
            name
        );
    }

    let resp = tenant_b_client
        .execute_query(&format!("SELECT * FROM {}.orders", tenant_b), None, None, None)
        .await?;
    assert!(resp.success(), "Tenant B query own orders");
    assert_eq!(resp.rows_as_maps().len(), 5, "Tenant B should see 5 orders");

    // Check all customer names start with "B"
    for row in &resp.rows_as_maps() {
        let name = row.get("customer_name").and_then(|v| v.as_str()).unwrap_or("");
        assert!(
            name.starts_with("B Customer"),
            "Tenant B should only see B customers, got: {}",
            name
        );
    }

    // =========================================================
    // Step 7: Both tenants can read shared feature flags
    // =========================================================
    let resp = tenant_a_client
        .execute_query(
            &format!("SELECT * FROM {}.feature_flags ORDER BY flag_name", global),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Tenant A read shared flags");
    assert_eq!(resp.rows_as_maps().len(), 3, "Should see 3 flags");

    let resp = tenant_b_client
        .execute_query(
            &format!("SELECT * FROM {}.feature_flags ORDER BY flag_name", global),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Tenant B read shared flags");
    assert_eq!(resp.rows_as_maps().len(), 3, "Should see 3 flags");

    // =========================================================
    // Step 8: Cross-tenant table access should be blocked (if enforced)
    // =========================================================
    // Note: This depends on RBAC implementation
    // Tenant A trying to access tenant_b.orders might be blocked
    let resp = tenant_a_client
        .execute_query(&format!("SELECT * FROM {}.orders", tenant_b), None, None, None)
        .await?;
    // This might succeed (seeing 0 rows due to USER table isolation)
    // or fail (access denied)
    if resp.success() {
        // If successful, should see 0 rows (USER table isolation)
        assert!(
            resp.rows_as_maps().is_empty(),
            "Cross-tenant query should return 0 rows due to USER isolation"
        );
    } else {
        // Access denied is also acceptable
        println!("Cross-tenant access denied as expected");
    }

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", tenant_a)).await;
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", tenant_b)).await;
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", global)).await;
    Ok(())
}

/// Test subscription isolation across namespaces
#[tokio::test]
async fn test_scenario_10_subscription_namespace_isolation() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns_a = unique_ns("sub_ns_a");
    let ns_b = unique_ns("sub_ns_b");

    // Create namespaces and tables
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns_a)).await?;
    assert_success(&resp, "CREATE namespace A");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns_b)).await?;
    assert_success(&resp, "CREATE namespace B");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.events (
                        id BIGINT PRIMARY KEY,
                        data TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns_a
        ))
        .await?;
    assert_success(&resp, "CREATE ns_a.events");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.events (
                        id BIGINT PRIMARY KEY,
                        data TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns_b
        ))
        .await?;
    assert_success(&resp, "CREATE ns_b.events");

    let user_a = format!("{}_user_a", ns_a);
    let user_b = format!("{}_user_b", ns_b);
    let client_a = create_user_and_client(server, &user_a, &Role::User).await?;
    let client_b = create_user_and_client(server, &user_b, &Role::User).await?;

    // Subscribe to ns_a.events
    let mut sub_a = client_a
        .subscribe(&format!("SELECT * FROM {}.events ORDER BY id", ns_a))
        .await?;
    let _ = wait_for_ack(&mut sub_a, Duration::from_secs(5)).await?;
    let _ = drain_initial_data(&mut sub_a, Duration::from_secs(2)).await?;

    // Subscribe to ns_b.events
    let mut sub_b = client_b
        .subscribe(&format!("SELECT * FROM {}.events ORDER BY id", ns_b))
        .await?;
    let _ = wait_for_ack(&mut sub_b, Duration::from_secs(5)).await?;
    let _ = drain_initial_data(&mut sub_b, Duration::from_secs(2)).await?;

    // Insert to ns_a
    let resp = client_a
        .execute_query(
            &format!("INSERT INTO {}.events (id, data) VALUES (1, 'event_a')", ns_a),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert to ns_a");

    // Insert to ns_b
    let resp = client_b
        .execute_query(
            &format!("INSERT INTO {}.events (id, data) VALUES (1, 'event_b')", ns_b),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert to ns_b");

    // sub_a should only get ns_a event
    let inserts_a = wait_for_inserts(&mut sub_a, 1, Duration::from_secs(5)).await?;
    assert_eq!(inserts_a.len(), 1, "sub_a should receive 1 insert");

    // sub_b should only get ns_b event
    let inserts_b = wait_for_inserts(&mut sub_b, 1, Duration::from_secs(5)).await?;
    assert_eq!(inserts_b.len(), 1, "sub_b should receive 1 insert");

    sub_a.close().await?;
    sub_b.close().await?;

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns_a)).await;
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns_b)).await;
    Ok(())
}

/// Test same table name in different namespaces
#[tokio::test]
async fn test_scenario_10_same_table_name_different_namespaces() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns1 = unique_ns("same_name_1");
    let ns2 = unique_ns("same_name_2");

    // Create namespaces
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns1)).await?;
    assert_success(&resp, "CREATE namespace 1");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns2)).await?;
    assert_success(&resp, "CREATE namespace 2");

    // Create same table name "users" in both
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.users (
                        id BIGINT PRIMARY KEY,
                        name TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns1
        ))
        .await?;
    assert_success(&resp, "CREATE ns1.users");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.users (
                        id BIGINT PRIMARY KEY,
                        name TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns2
        ))
        .await?;
    assert_success(&resp, "CREATE ns2.users");

    let user1 = format!("{}_same_name_user1", ns1);
    let user2 = format!("{}_same_name_user2", ns2);
    let client1 = create_user_and_client(server, &user1, &Role::User).await?;
    let client2 = create_user_and_client(server, &user2, &Role::User).await?;

    // Insert different data
    let resp = client1
        .execute_query(
            &format!("INSERT INTO {}.users (id, name) VALUES (1, 'NS1 User')", ns1),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert to ns1.users");

    let resp = client2
        .execute_query(
            &format!("INSERT INTO {}.users (id, name) VALUES (1, 'NS2 User')", ns2),
            None,
            None,
            None,
        )
        .await?;
    assert!(resp.success(), "Insert to ns2.users");

    // Verify isolation
    let resp = client1
        .execute_query(&format!("SELECT name FROM {}.users WHERE id = 1", ns1), None, None, None)
        .await?;
    let name1 = resp.get_string("name").unwrap_or_default();
    assert_eq!(name1, "NS1 User", "ns1.users should have NS1 User");

    let resp = client2
        .execute_query(&format!("SELECT name FROM {}.users WHERE id = 1", ns2), None, None, None)
        .await?;
    let name2 = resp.get_string("name").unwrap_or_default();
    assert_eq!(name2, "NS2 User", "ns2.users should have NS2 User");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns1)).await;
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns2)).await;
    Ok(())
}
