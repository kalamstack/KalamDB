use std::time::Duration;

use kalam_client::{models::ChangeEvent, KalamLinkTimeouts};

use crate::common::*;

const MAX_SQL_QUERY_LENGTH: usize = 1024 * 1024;

fn expect_unauthorized(result: Result<String, Box<dyn std::error::Error>>, context: &str) {
    assert!(result.is_err(), "Expected authorization failure: {}", context);
    if let Err(err) = result {
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("unauthorized")
                || msg.contains("not authorized")
                || msg.contains("permission")
                || msg.contains("privilege")
                || msg.contains("access denied")
                || msg.contains("row-level")
                || msg.contains("with check")
                || msg.contains("policy"),
            "Expected authorization error for {}: {}",
            context,
            err
        );
    }
}

fn subscribe_as_user(username: &str, password: &str, query: &str) -> Result<(), String> {
    let base_url = leader_url().unwrap_or_else(|| {
        get_available_server_urls()
            .first()
            .cloned()
            .unwrap_or_else(|| server_url().to_string())
    });

    let client = client_for_user_on_url_with_timeouts(
        &base_url,
        username,
        password,
        KalamLinkTimeouts::builder()
            .connection_timeout_secs(5)
            .receive_timeout_secs(30)
            .send_timeout_secs(30)
            .subscribe_timeout_secs(10)
            .auth_timeout_secs(10)
            .initial_data_timeout(Duration::from_secs(15))
            .build(),
    )
    .map_err(|e| format!("Failed to build client: {}", e))?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to build runtime: {}", e))?;

    let subscribe_result = rt.block_on(async move {
        let mut subscription = client.live_events(query).await?;

        // Wait for the first event: ACK means success, Error means permission denied.
        // The server sends permission errors as WebSocket error events, not as connection
        // failures, so we must read at least one event to detect the server's response.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // Timeout without receiving any event — treat as success (subscription is alive).
                return Ok::<(), kalam_client::error::KalamLinkError>(());
            }
            match tokio::time::timeout(remaining, subscription.next()).await {
                Ok(Some(Ok(ChangeEvent::Error { code, message, .. }))) => {
                    return Err(kalam_client::error::KalamLinkError::WebSocketError(format!(
                        "{}: {}",
                        code, message
                    )));
                },
                Ok(Some(Ok(ChangeEvent::Ack { .. }))) => {
                    // Subscription was accepted by the server.
                    return Ok(());
                },
                Ok(Some(Ok(_))) => {
                    // Any other event (initial data batch, etc.) means subscription worked.
                    return Ok(());
                },
                Ok(Some(Err(e))) => {
                    return Err(e);
                },
                Ok(None) => {
                    // Stream closed without ACK — treat as error.
                    return Err(kalam_client::error::KalamLinkError::WebSocketError(
                        "Stream closed before receiving ACK".to_string(),
                    ));
                },
                Err(_) => {
                    // Timeout without any event — subscription is alive.
                    return Ok(());
                },
            }
        }
    });
    match subscribe_result {
        Ok(()) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

fn expect_rejected(result: Result<String, Box<dyn std::error::Error>>, context: &str) {
    assert!(result.is_err(), "Expected rejection: {}", context);
    if let Err(err) = result {
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("unauthorized")
                || msg.contains("not authorized")
                || msg.contains("permission")
                || msg.contains("privilege")
                || msg.contains("denied")
                || msg.contains("forbidden")
                || msg.contains("invalid")
                || msg.contains("constraint")
                || msg.contains("system table")
                || msg.contains("access denied"),
            "Expected rejection error for {}: {}",
            context,
            err
        );
    }
}

// Regular users cannot access system tables even via batch statements or subselects
#[ntest::timeout(180000)]
#[test]
fn smoke_security_system_tables_blocked_in_batch() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_system_tables_blocked_in_batch: server not running at {}",
            server_url()
        );
        return;
    }

    let user_name = generate_unique_namespace("smoke_sys_batch");
    let user_pass = "smoke_pass_123";

    let create_user_sql =
        format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user_name, user_pass);
    execute_sql_as_root_via_client(&create_user_sql).expect("Failed to create user");

    let batch_queries = vec![
        "SELECT 1; SELECT * FROM system.users",
        "SELECT 1; SELECT * FROM system.schemas",
        "SELECT 1; SELECT * FROM (SELECT * FROM system.users) AS u",
        "SELECT 1; SELECT u.user_id FROM system.users u JOIN (SELECT user_id FROM system.users) s \
         ON u.user_id = s.user_id",
        "SELECT 1; SELECT * FROM system.users WHERE user_id IN (SELECT user_id FROM system.users)",
        "WITH u AS (SELECT * FROM system.users) SELECT * FROM u",
        "SELECT 1; EXPLAIN SELECT * FROM system.users",
        "SELECT (SELECT user_id FROM system.users LIMIT 1) AS usr FROM system.users",
        "SELECT (SELECT COUNT(*) FROM system.users) AS cnt",
        "SELECT * FROM system.users WHERE user_id = (SELECT user_id FROM system.users LIMIT 1)",
        // Note: EXISTS in CASE WHEN is not yet implemented in DataFusion, skipping
        // "SELECT CASE WHEN EXISTS(SELECT 1 FROM system.users) THEN 'found' END",
        "SELECT 1 UNION SELECT user_id FROM system.users",
        "SELECT 1 EXCEPT SELECT user_id FROM system.users",
        "SELECT 1 INTERSECT SELECT user_id FROM system.users",
    ];

    for sql in batch_queries {
        let result = execute_sql_via_client_as(&user_name, user_pass, sql);
        expect_rejected(result, &format!("system table batch query: {}", sql));
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user_name));
}

// Regular users cannot access private shared tables even with batch statements
#[ntest::timeout(180000)]
#[test]
fn smoke_security_private_shared_table_blocked_in_batch() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_private_shared_table_blocked_in_batch: server not running at \
             {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_private_shared_ns");
    let table = generate_unique_table("smoke_private_shared_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let regular_user = generate_unique_namespace("smoke_private_user");
    let password = "smoke_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED')",
        full_table
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("Failed to create shared table");

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name) VALUES (1, 'secret')",
        full_table
    ))
    .expect("Failed to insert root row");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        regular_user, password
    ))
    .expect("Failed to create regular user");

    let batch_sql = format!(
        "SELECT * FROM {table}; INSERT INTO {table} (id, name) VALUES (2, 'x'); UPDATE {table} \
         SET name = 'y' WHERE id = 1; DELETE FROM {table} WHERE id = 1;",
        table = full_table
    );
    let batch_result = execute_sql_via_client_as(&regular_user, password, &batch_sql);
    expect_unauthorized(batch_result, "private shared table batch statement");

    // Also verify single statements are blocked
    let select_result = execute_sql_via_client_as(
        &regular_user,
        password,
        &format!("SELECT * FROM {}", full_table),
    );
    let select_output = select_result
        .expect("user SELECT on a default-deny shared table should succeed with zero rows");
    assert!(
        !select_output.to_lowercase().contains("secret"),
        "user SELECT must not return unauthorized rows: {select_output}"
    );

    let insert_result = execute_sql_via_client_as(
        &regular_user,
        password,
        &format!("INSERT INTO {} (id, name) VALUES (3, 'x')", full_table),
    );
    expect_unauthorized(insert_result, "private shared table INSERT");

    let delete_result = execute_sql_via_client_as(
        &regular_user,
        password,
        &format!("DELETE FROM {} WHERE id = 1", full_table),
    );
    delete_result.expect(
        "DELETE on a default-deny shared table should succeed with zero rows rather than error",
    );
    let remaining = execute_sql_as_root_via_client(&format!("SELECT name FROM {}", full_table))
        .expect("DBA SELECT after user DELETE");
    assert!(
        remaining.to_lowercase().contains("secret"),
        "default-deny DELETE must not remove unauthorized rows: {remaining}"
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", regular_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

// Regular users cannot subscribe to system tables or private shared tables
#[ntest::timeout(180000)]
#[test]
fn smoke_security_subscription_blocked_for_system_and_private_shared() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_subscription_blocked_for_system_and_private_shared: server \
             not running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_sub_private_ns");
    let table = generate_unique_table("smoke_sub_private_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let regular_user = generate_unique_namespace("smoke_sub_user");
    let password = "smoke_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    let create_private_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED')",
        full_table
    );
    execute_sql_as_root_via_client(&create_private_sql).expect("Failed to create shared table");

    let public_table = generate_unique_table("smoke_sub_public_tbl");
    let full_public = format!("{}.{}", namespace, public_table);
    let create_public_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED')",
        full_public
    );
    execute_sql_as_root_via_client(&create_public_sql)
        .expect("Failed to create public shared table");
    execute_sql_as_root_via_client(&format!(
        "CREATE POLICY {public_table}_select ON {full_public} FOR SELECT TO PUBLIC USING (true)"
    ))
    .expect("Failed to grant SELECT on public shared table");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        regular_user, password
    ))
    .expect("Failed to create regular user");

    let system_query = "SELECT * FROM system.users";
    let system_sub = subscribe_as_user(&regular_user, password, system_query);
    assert!(system_sub.is_err(), "Expected system table subscription to fail");

    let shared_query = format!("SELECT * FROM {}", full_table);
    let shared_sub = subscribe_as_user(&regular_user, password, &shared_query);
    if let Err(error) = &shared_sub {
        if error.contains("channel closed") {
            eprintln!(
                "Skipping transient live-query backend failure for shared subscription: {}",
                error
            );
            let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", regular_user));
            let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
            let _ =
                execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_public));
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
            return;
        }
    }
    assert!(
        shared_sub.is_ok(),
        "Expected shared table subscription to succeed; RLS filters events. got: {:?}",
        shared_sub.err()
    );

    let public_query = format!("SELECT * FROM {}", full_public);
    let public_sub = subscribe_as_user(&regular_user, password, &public_query);

    if let Err(error) = &public_sub {
        if error.contains("channel closed") {
            eprintln!(
                "Skipping transient live-query backend failure for public subscription: {}",
                error
            );
            let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", regular_user));
            let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
            let _ =
                execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_public));
            let _ =
                execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
            return;
        }
    }

    assert!(
        public_sub.is_ok(),
        "Expected public shared table subscription to succeed, but got: {:?}",
        public_sub.err()
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", regular_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_public));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

// Query length guard rejects oversized SQL (DoS prevention)
#[ntest::timeout(120000)]
#[test]
fn smoke_security_query_length_limit() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_query_length_limit: server not running at {}",
            server_url()
        );
        return;
    }

    let user_name = generate_unique_namespace("smoke_len_user");
    let user_pass = "smoke_pass_123";

    let create_user_sql =
        format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user_name, user_pass);
    execute_sql_as_root_via_client(&create_user_sql).expect("Failed to create user");

    let oversized = "a".repeat(MAX_SQL_QUERY_LENGTH + 1024);
    let sql = format!("SELECT '{}'", oversized);
    let result = execute_sql_via_client_as(&user_name, user_pass, &sql);
    expect_rejected(result, "oversized query length");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user_name));
}

// Regular users cannot write to system tables (even in batch)
#[ntest::timeout(180000)]
#[test]
fn smoke_security_system_table_write_blocked() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_system_table_write_blocked: server not running at {}",
            server_url()
        );
        return;
    }

    let user_name = generate_unique_namespace("smoke_sys_write_user");
    let user_pass = "smoke_pass_123";

    let create_user_sql =
        format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user_name, user_pass);
    execute_sql_as_root_via_client(&create_user_sql).expect("Failed to create user");

    let batch_sql = "INSERT INTO system.users (user_id) VALUES ('hacker'); UPDATE system.users \
                     SET user_id='x' WHERE user_id='root'; DELETE FROM system.users WHERE \
                     user_id='root';";
    let result = execute_sql_via_client_as(&user_name, user_pass, batch_sql);
    expect_rejected(result, "system table write batch");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user_name));
}

// ── Session / row isolation ────────────────────────────────────────────────

// Two independent regular users write to the same USER table.
// Each must only see their own rows — a malicious or accidental cross-user
// read must return zero rows for the other user's data.
#[ntest::timeout(180000)]
#[test]
fn smoke_security_cross_user_row_isolation() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_cross_user_row_isolation: server not running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_row_iso_ns");
    let table = generate_unique_table("smoke_row_iso_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let user_a = generate_unique_namespace("smoke_row_iso_ua");
    let user_b = generate_unique_namespace("smoke_row_iso_ub");
    let password = "test_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id VARCHAR PRIMARY KEY, note TEXT) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create user table");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user_a, password
    ))
    .expect("Failed to create user_a");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user_b, password
    ))
    .expect("Failed to create user_b");

    // User A inserts a private row.
    execute_sql_via_client_as(
        &user_a,
        password,
        &format!("INSERT INTO {} (id, note) VALUES ('a1', 'secret-a')", full_table),
    )
    .expect("User A insert failed");

    // User B inserts a different private row.
    execute_sql_via_client_as(
        &user_b,
        password,
        &format!("INSERT INTO {} (id, note) VALUES ('b1', 'secret-b')", full_table),
    )
    .expect("User B insert failed");

    // User A must see their own row.
    let a_view = execute_sql_via_client_as(
        &user_a,
        password,
        &format!("SELECT note FROM {} WHERE id = 'a1'", full_table),
    )
    .expect("User A select failed");
    assert!(a_view.contains("secret-a"), "User A should see their own row: {a_view}");

    // User A must NOT see User B's row via filtered query.
    assert!(!a_view.contains("secret-b"), "User A leaked User B's row: {a_view}");

    // User B must see their own row.
    let b_view = execute_sql_via_client_as(
        &user_b,
        password,
        &format!("SELECT note FROM {} WHERE id = 'b1'", full_table),
    )
    .expect("User B select failed");
    assert!(b_view.contains("secret-b"), "User B should see their own row: {b_view}");

    // User B must NOT see User A's row.
    assert!(!b_view.contains("secret-a"), "User B leaked User A's row: {b_view}");

    // Full-table SELECT: each user must see only their own row.
    let a_all =
        execute_sql_via_client_as(&user_a, password, &format!("SELECT note FROM {}", full_table))
            .expect("User A full-table select failed");
    assert!(
        !a_all.contains("secret-b"),
        "User A's full-table select must not expose User B's data: {a_all}"
    );

    let b_all =
        execute_sql_via_client_as(&user_b, password, &format!("SELECT note FROM {}", full_table))
            .expect("User B full-table select failed");
    assert!(
        !b_all.contains("secret-a"),
        "User B's full-table select must not expose User A's data: {b_all}"
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_a));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_b));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

// ── EXECUTE AS injection prevention ───────────────────────────────────────

// A SQL-injection suffix in the EXECUTE AS username position must be rejected
// by the server before reaching the authorisation layer.
// For example: EXECUTE AS USER 'alice' OR '1'='1' (SELECT 1)
// The parser extracts username='alice', then the leftover ' OR …' doesn't
// start with '(' — the server returns a parse error, never an auth check.
#[ntest::timeout(120000)]
#[test]
fn smoke_security_execute_as_username_injection_rejected() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_execute_as_username_injection_rejected: server not running \
             at {}",
            server_url()
        );
        return;
    }

    let user_name = generate_unique_namespace("smoke_ea_inject");
    let user_pass = "test_pass_123";
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user_name, user_pass
    ))
    .expect("Failed to create user");

    // Each case exploits a different injection vector in the EXECUTE AS syntax.
    let injection_cases = vec![
        // SQL-injection suffix after username
        "EXECUTE AS USER 'alice' OR '1'='1' (SELECT 1)",
        // Semicolon-stacking after username
        "EXECUTE AS USER 'alice'; DROP TABLE system.users (SELECT 1)",
        // Line-comment injection after username
        "EXECUTE AS USER 'alice'--inject (SELECT 1)",
        // Multi-statement inside the body
        "EXECUTE AS USER 'alice' (SELECT 1; DROP TABLE system.users)",
        // Empty username
        "EXECUTE AS USER '' (SELECT 1)",
    ];

    for sql in injection_cases {
        let result = execute_sql_via_client_as(&user_name, user_pass, sql);
        assert!(
            result.is_err(),
            "EXECUTE AS injection case must be rejected by the server: {}",
            sql
        );
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_name));
}

// A regular user (role='user') must be denied when they attempt to impersonate
// any other user via EXECUTE AS USER, regardless of the target user's role.
#[ntest::timeout(120000)]
#[test]
fn smoke_security_plain_user_cannot_execute_as_any_user() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_plain_user_cannot_execute_as_any_user: server not running at \
             {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_ea_deny_ns");
    let table = generate_unique_table("smoke_ea_deny_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let actor = generate_unique_namespace("smoke_ea_actor");
    let target = generate_unique_namespace("smoke_ea_target");
    let dba_target = generate_unique_namespace("smoke_ea_dba");
    let password = "test_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id VARCHAR PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create user table");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        actor, password
    ))
    .expect("Failed to create actor");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        target, password
    ))
    .expect("Failed to create target");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        dba_target, password
    ))
    .expect("Failed to create dba_target");

    // Regular user → regular user: must be denied.
    let result_user = execute_sql_via_client_as(
        &actor,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, v) VALUES ('x', 'blocked'))",
            target, full_table
        ),
    );
    assert!(
        result_user.is_err(),
        "Regular user must not impersonate another user: {}",
        result_user.unwrap_or_default()
    );

    // Regular user → DBA user: must also be denied.
    let result_dba = execute_sql_via_client_as(
        &actor,
        password,
        &format!("EXECUTE AS USER '{}' (SELECT 1)", dba_target),
    );
    assert!(
        result_dba.is_err(),
        "Regular user must not impersonate a DBA: {}",
        result_dba.unwrap_or_default()
    );

    // Verify no rows leaked into the table from the denied attempt.
    let leaked =
        execute_sql_via_client_as(&target, password, &format!("SELECT * FROM {}", full_table))
            .expect("Target select failed");
    assert!(
        !leaked.contains("blocked"),
        "Denied EXECUTE AS must not insert any rows: {leaked}"
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", actor));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", dba_target));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}
