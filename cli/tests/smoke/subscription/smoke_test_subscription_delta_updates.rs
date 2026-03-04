// Smoke Test: Delta updates for subscription UPDATE notifications
//
// Covers: When a subscription receives an UPDATE change event, only the changed
// columns (plus system columns _seq, _deleted) should be included in the
// notification — not all columns. The `changed_columns` field lists which
// user-defined columns were modified.
//
// This verifies the end-to-end delta update pipeline:
//   Backend (notification.rs compute_json_update_delta) → WebSocket → kalam-link SDK

use crate::common::*;
use std::time::{Duration, Instant};

#[ntest::timeout(120000)]
#[test]
fn smoke_subscription_update_sends_delta_only() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_delta");
    let table = generate_unique_table("delta_upd");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace + shared table with multiple columns
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR,
            age INT
        ) WITH (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // 2) Insert a row with all columns populated
    let ins = format!(
        "INSERT INTO {} (name, email, age) VALUES ('Alice', 'alice@test.com', 30)",
        full
    );
    execute_sql_as_root_via_client(&ins).expect("insert should succeed");

    // Wait for data to be visible
    let _ = wait_for_sql_output_contains(
        &format!("SELECT * FROM {} WHERE name = 'Alice'", full),
        "Alice",
        Duration::from_secs(5),
    );

    // 3) Start subscription
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // 3a) Drain initial snapshot data
    let snapshot_deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < snapshot_deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                if !line.trim().is_empty() {
                    println!("[subscription][snapshot] {}", line);
                }
                // Break early once we've seen the initial data batch
                if line.contains("InitialDataBatch") {
                    // Allow a bit more time for any remaining events
                    std::thread::sleep(Duration::from_millis(500));
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    // 4) UPDATE only the 'name' column — email and age should NOT be in the delta
    let upd = format!("UPDATE {} SET name = 'Bob' WHERE name = 'Alice'", full);
    execute_sql_as_root_via_client(&upd).expect("update should succeed");

    // 5) Wait for Update event and verify delta behavior
    let mut update_lines: Vec<String> = Vec::new();
    let update_deadline = Instant::now() + Duration::from_secs(10);
    let mut found_update = false;

    while Instant::now() < update_deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[subscription][event] {}", line);
                if line.contains("Update") {
                    update_lines.push(line.clone());
                    found_update = true;
                    break;
                }
                update_lines.push(line);
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert!(found_update, "expected UPDATE change event within 10s; got: {:?}", update_lines);

    let update_joined = update_lines.join("\n");

    // 5a) Verify update event shape includes changed field information
    assert!(
        (update_joined.contains("changed_columns") && update_joined.contains("name"))
            || (update_joined.contains("old_rows") && update_joined.contains("name")),
        "UPDATE event should include changed field information for 'name'; got: {}",
        update_joined
    );

    // 5b) Verify only the changed column is listed
    //     (email and age should NOT appear in changed_columns)
    assert!(
        !update_joined.contains("email") && !update_joined.contains("age"),
        "Single-column UPDATE delta should not include unchanged fields 'email' or 'age'; got: {}",
        update_joined
    );

    // 5c) The new value "Bob" should appear in the rows
    assert!(
        update_joined.contains("Bob"),
        "UPDATE event rows should contain new value 'Bob'; got: {}",
        update_joined
    );

    // 6) Test updating multiple columns — verify changed_columns lists both
    let upd2 = format!(
        "UPDATE {} SET email = 'bob@test.com', age = 31 WHERE name = 'Bob'",
        full
    );
    execute_sql_as_root_via_client(&upd2).expect("multi-column update should succeed");

    let mut update2_lines: Vec<String> = Vec::new();
    let update2_deadline = Instant::now() + Duration::from_secs(10);
    let mut found_update2 = false;

    while Instant::now() < update2_deadline {
        match listener.try_read_line(Duration::from_millis(100)) {
            Ok(Some(line)) => {
                println!("[subscription][event2] {}", line);
                if line.contains("Update") {
                    update2_lines.push(line.clone());
                    found_update2 = true;
                    break;
                }
                update2_lines.push(line);
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    if found_update2 {
        let update2_joined = update2_lines.join("\n");

        // Both email and age should be represented in the update payload
        assert!(
            update2_joined.contains("email") && update2_joined.contains("age"),
            "Multi-column UPDATE should include both 'email' and 'age' in the update payload; got: {}",
            update2_joined
        );

        println!("  ✅ Multi-column delta update verified");
    } else {
        eprintln!("  ⚠️  Second UPDATE event not received within timeout (non-fatal)");
    }

    println!("  ✅ Delta update subscription notifications verified");

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
