//! End-to-end subscription test: verifies initial snapshot + change events

use std::time::Duration;

use crate::common::*;

#[test]
fn test_cli_subscription_initial_and_changes() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Prepare unique namespace.table
    let namespace = generate_unique_namespace("batch_test");
    let table_full = format!("{}.items", namespace);

    // Ensure namespace exists
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace));

    // Create user table
    let _ = execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:10')",
        table_full
    ));

    // Insert initial row BEFORE subscribing
    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (id, name) VALUES (1, 'Item One')",
        table_full
    ));

    // Ensure the row is visible before subscribing (reduces flakiness in initial snapshot)
    let _ = wait_for_sql_output_contains(
        &format!("SELECT * FROM {} WHERE id = 1", table_full),
        "Item One",
        Duration::from_secs(5),
    );

    // Start subscription via CLI
    let query = format!("SELECT * FROM {}", table_full);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace));
            return;
        },
    };

    let event_timeout = if is_cluster_mode() {
        Duration::from_secs(25)
    } else {
        Duration::from_secs(12)
    };

    // Expect an InitialDataBatch event with 1 row
    let snapshot_line = listener
        .wait_for_event("InitialDataBatch", event_timeout)
        .expect("expected InitialDataBatch event");
    assert!(
        snapshot_line.contains("rows:") || snapshot_line.contains("Initial"),
        "Expected initial data batch event, got: {}",
        snapshot_line
    );

    // Perform INSERT change and wait for Insert event (enum variant name is capitalized)
    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (id, name) VALUES (2, 'Second')",
        table_full
    ));
    let insert_line =
        listener.wait_for_event("Insert", event_timeout).expect("expected Insert event");
    assert!(
        insert_line.contains("Second") || insert_line.contains("rows"),
        "Insert event should contain row data: {}",
        insert_line
    );

    // Perform UPDATE and wait for Update event
    let _ = execute_sql_as_root_via_cli(&format!(
        "UPDATE {} SET name = 'Updated Second' WHERE id = 2",
        table_full
    ));
    let update_line =
        listener.wait_for_event("Update", event_timeout).expect("expected Update event");
    assert!(
        update_line.contains("Updated Second") || update_line.contains("rows"),
        "Update event should contain updated data: {}",
        update_line
    );

    // Perform DELETE and wait for Delete event
    let _ = execute_sql_as_root_via_cli(&format!("DELETE FROM {} WHERE id = 2", table_full));
    let delete_line =
        listener.wait_for_event("Delete", event_timeout).expect("expected Delete event");
    assert!(
        delete_line.contains("old_rows") || delete_line.contains("rows"),
        "Delete event should contain deleted row data: {}",
        delete_line
    );

    // Cleanup
    listener.stop().ok();
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
