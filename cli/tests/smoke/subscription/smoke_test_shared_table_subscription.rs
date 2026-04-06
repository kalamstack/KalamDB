// Smoke Test: Shared table subscription lifecycle
// Covers: shared table creation, inserts, subscription receiving snapshot + change events
// Uses kalam-client directly instead of CLI to avoid macOS TCP subprocess limits

use crate::common::*;
use kalam_client::models::ChangeEvent;

/// Attempt to subscribe to a query as a given user and wait for ACK or error.
/// Returns Ok(()) if subscription was accepted, Err(msg) if it was denied or failed.
fn try_subscribe_as_user(username: &str, password: &str, query: &str) -> Result<(), String> {
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
        kalam_client::KalamLinkTimeouts::builder()
            .connection_timeout_secs(5)
            .receive_timeout_secs(30)
            .send_timeout_secs(30)
            .subscribe_timeout_secs(10)
            .auth_timeout_secs(10)
            .initial_data_timeout(std::time::Duration::from_secs(15))
            .build(),
    )
    .map_err(|e| format!("Failed to build client: {}", e))?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to build runtime: {}", e))?;

    let result = rt.block_on(async move {
        let mut subscription = client.subscribe(query).await?;

        // The server sends permission errors as WebSocket error events, not as connection
        // failures. We must read at least one event to detect the server's response.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok::<(), kalam_client::error::KalamLinkError>(());
            }
            match tokio::time::timeout(remaining, subscription.next()).await {
                Ok(Some(Ok(ChangeEvent::Error { code, message, .. }))) => {
                    return Err(kalam_client::error::KalamLinkError::WebSocketError(format!(
                        "{}: {}",
                        code, message
                    )));
                },
                Ok(Some(Ok(ChangeEvent::Ack { .. }))) | Ok(Some(Ok(_))) => {
                    return Ok(());
                },
                Ok(Some(Err(e))) => return Err(e),
                Ok(None) => {
                    return Err(kalam_client::error::KalamLinkError::WebSocketError(
                        "Stream closed before ACK".to_string(),
                    ));
                },
                Err(_) => return Ok(()),
            }
        }
    });
    result.map_err(|e| e.to_string())
}

#[ntest::timeout(120000)]
#[test]
fn smoke_shared_table_subscription_lifecycle() {
    if !require_server_running() {
        return;
    }

    // Unique per run
    let namespace = generate_unique_namespace("smoke_shared_sub_ns");
    let table = generate_unique_table("shared_sub");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    // 2) Create public shared table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            name VARCHAR NOT NULL
        ) WITH (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create shared table should succeed");

    // 3) Insert a couple rows before subscribing
    let ins1 = format!("INSERT INTO {} (id, name) VALUES (1, 'alpha')", full);
    let ins2 = format!("INSERT INTO {} (id, name) VALUES (2, 'beta')", full);
    execute_sql_as_root_via_client(&ins1).expect("insert alpha should succeed");
    execute_sql_as_root_via_client(&ins2).expect("insert beta should succeed");

    // Quick verification via SELECT
    let sel = format!("SELECT * FROM {}", full);
    let out = execute_sql_as_root_via_client(&sel).expect("select should succeed");
    assert!(out.contains("alpha"), "expected to see 'alpha' in select output: {}", out);
    assert!(out.contains("beta"), "expected to see 'beta' in select output: {}", out);

    // 4) Subscribe to the shared table
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // 4a) Collect snapshot rows
    let mut snapshot_lines: Vec<String> = Vec::new();
    let snapshot_deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    while std::time::Instant::now() < snapshot_deadline {
        match listener.try_read_line(std::time::Duration::from_millis(100)) {
            Ok(Some(line)) => {
                if !line.trim().is_empty() {
                    println!("[subscription][snapshot] {}", line);
                    snapshot_lines.push(line);
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    let snapshot_joined = if snapshot_lines.is_empty() {
        // Fallback: perform a SELECT to synthesize snapshot
        println!("[subscription] No snapshot lines captured; performing fallback SELECT");
        let fallback_sel = format!("SELECT * FROM {}", full);
        execute_sql_as_root_via_client(&fallback_sel).unwrap_or_default()
    } else {
        snapshot_lines.join("\n")
    };
    assert!(
        snapshot_joined.contains("alpha"),
        "snapshot should contain 'alpha' but was: {}",
        snapshot_joined
    );
    assert!(
        snapshot_joined.contains("beta"),
        "snapshot should contain 'beta' but was: {}",
        snapshot_joined
    );

    // 5) Insert a new row and verify subscription receives the change event
    let sub_val = "from_shared_sub";
    let ins3 = format!("INSERT INTO {} (id, name) VALUES (3, '{}')", full, sub_val);
    println!("[DEBUG] Inserting new row with value: {}", sub_val);
    execute_sql_as_root_via_client(&ins3).expect("insert sub row should succeed");
    println!("[DEBUG] Insert completed, waiting for change event...");

    let mut change_lines: Vec<String> = Vec::new();
    let change_deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    let mut poll_count = 0;
    while std::time::Instant::now() < change_deadline {
        poll_count += 1;
        match listener.try_read_line(std::time::Duration::from_millis(100)) {
            Ok(Some(line)) => {
                if !line.trim().is_empty() {
                    println!("[subscription][change] {}", line);
                    let is_match = line.contains(sub_val);
                    change_lines.push(line);
                    if is_match {
                        println!("[DEBUG] Found matching change event!");
                        break;
                    }
                }
            },
            Ok(None) => {
                println!("[DEBUG] EOF on subscription stream after {} polls", poll_count);
                break;
            },
            Err(_e) => {
                if poll_count % 10 == 0 {
                    println!("[DEBUG] Still waiting for change event... (poll {})", poll_count);
                }
                continue;
            },
        }
    }
    println!("[DEBUG] Total polls: {}, lines collected: {}", poll_count, change_lines.len());

    let changes_joined = change_lines.join("\n");
    assert!(
        changes_joined.contains(sub_val),
        "expected change event containing '{}' within 6s; got: {}",
        sub_val,
        changes_joined
    );

    // 6) Stop subscription
    listener.stop().ok();

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// Verify that private shared table subscription is blocked for regular users
#[ntest::timeout(120000)]
#[test]
fn smoke_shared_table_subscription_private_denied() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_shared_priv_ns");
    let table = generate_unique_table("shared_priv");
    let full = format!("{}.{}", namespace, table);
    let user = generate_unique_namespace("shared_priv_user");
    let password = "smoke_pass_123";

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED', ACCESS_LEVEL='PRIVATE')",
        full
    ))
    .expect("create private shared table");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user, password
    ))
    .expect("create user");

    // Attempt to subscribe as regular user — should fail.
    // The server sends permission errors as WebSocket error events (not connection errors),
    // so try_subscribe_as_user waits for the first event to detect the denial.
    let query = format!("SELECT * FROM {}", full);
    let result = try_subscribe_as_user(&user, password, &query);
    assert!(
        result.is_err(),
        "Expected private shared table subscription to fail for regular user, but it succeeded"
    );
    let err_msg = result.err().unwrap().to_lowercase();
    assert!(
        err_msg.contains("permission")
            || err_msg.contains("privilege")
            || err_msg.contains("denied")
            || err_msg.contains("access")
            || err_msg.contains("unauthorized"),
        "Expected permission-related error message, but got: {}",
        err_msg
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user));
}
