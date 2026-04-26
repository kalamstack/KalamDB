// Smoke Test: DML across wide columns for USER and SHARED tables
// Covers: insert -> check -> update -> check -> multi-column update -> check -> delete -> check
// Also verifies subscription delivers UPDATE and DELETE events

use std::time::Duration;

use crate::common::*;

fn create_namespace(ns: &str) {
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns));
}

fn extract_first_id_from_json(json_output: &str) -> Option<String> {
    // Parse JSON response and extract the first id value
    // Handles both number and string IDs (large integers are serialized as strings for JS safety)
    let value: serde_json::Value = serde_json::from_str(json_output).ok()?;
    let rows = get_rows_as_hashmaps(&value)?;
    let first_row = rows.first()?;
    let id_value = extract_typed_value(first_row.get("id")?);
    json_value_as_id(&id_value)
}

fn run_dml_sequence(full: &str, _is_shared: bool) {
    // insert row 1
    let ins1 = format!(
        "INSERT INTO {} (name, age, active, score, balance, note) VALUES ('alpha', 25, true, \
         12.5, 1000, 'note-a')",
        full
    );
    let out1 = execute_sql_as_root_via_client(&ins1).expect("insert 1 should succeed");
    assert!(out1.contains("1 rows affected"), "expected 1 row affected: {}", out1);

    // insert row 2
    let ins2 = format!(
        "INSERT INTO {} (name, age, active, score, balance, note) VALUES ('beta', 30, false, \
         99.9, 2000, 'note-b')",
        full
    );
    let out2 = execute_sql_as_root_via_client(&ins2).expect("insert 2 should succeed");
    assert!(out2.contains("1 rows affected"));

    // check rows exist
    let sel_all = format!(
        "SELECT id, name, age, active, score, balance, created_at, note FROM {} ORDER BY id",
        full
    );
    let out_sel = execute_sql_as_root_via_client(&sel_all).expect("select should succeed");
    assert!(
        out_sel.contains("alpha") && out_sel.contains("beta"),
        "expected both rows present: {}",
        out_sel
    );

    // parse alpha id via focused query to avoid brittle table parsing
    let sel_alpha_id = format!("SELECT id FROM {} WHERE name = 'alpha'", full);
    let out_alpha_json =
        execute_sql_as_root_via_client_json(&sel_alpha_id).expect("select alpha id");
    let id_alpha = extract_first_id_from_json(&out_alpha_json).expect("should parse alpha id");

    // update single column on alpha
    let upd1 = format!("UPDATE {} SET age = 26 WHERE id = {}", full, id_alpha);
    let out_upd1 = execute_sql_as_root_via_client(&upd1).expect("update 1 should succeed");
    assert!(out_upd1.contains("1 rows affected"));

    // check
    let out_chk1 = execute_sql_as_root_via_client(&sel_all).expect("post update select");
    assert!(out_chk1.contains("26"), "expected updated age 26: {}", out_chk1);

    // multi-column update on alpha
    let upd2 = format!(
        "UPDATE {} SET name='alpha2', age=42, active=false, score=88.75, balance=4321, \
         note='note-upd' WHERE id = {}",
        full, id_alpha
    );
    let out_upd2 = execute_sql_as_root_via_client(&upd2).expect("update 2 should succeed");
    assert!(out_upd2.contains("1 rows affected"));

    // check
    let out_chk2 = execute_sql_as_root_via_client(&sel_all).expect("post multi update select");
    assert!(
        out_chk2.contains("alpha2")
            && out_chk2.contains("42")
            && out_chk2.to_lowercase().contains("false"),
        "expected multi-column updates reflected: {}",
        out_chk2
    );

    // delete beta row by filtering on name to get id, then delete by id to respect pk equality
    let sel_beta = format!("SELECT id, name FROM {} WHERE name = 'beta'", full);
    let out_beta_json = execute_sql_as_root_via_client_json(&sel_beta).expect("select beta id");
    let beta_id = extract_first_id_from_json(&out_beta_json).expect("should parse beta id");

    let del = format!("DELETE FROM {} WHERE id = {}", full, beta_id);
    let out_del = execute_sql_as_root_via_client(&del).expect("delete should succeed");
    assert!(out_del.contains("1 rows affected"));

    // best-effort final check: ensure updated row still present
    let out_after = execute_sql_as_root_via_client(&sel_all).expect("final select after delete");
    assert!(out_after.contains("alpha2"), "expected updated row present: {}", out_after);

    // Note: subscription validations are covered in dedicated test below
}

#[ntest::timeout(180000)]
#[test]
fn smoke_user_table_dml_wide_columns() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("user_dml_wide");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // create USER table with 8+ columns of various types
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL,
            age INT,
            active BOOLEAN,
            score DOUBLE,
            balance BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note VARCHAR
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:10'
        )"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    run_dml_sequence(&full, false);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

#[ntest::timeout(180000)]
#[test]
fn smoke_shared_table_dml_wide_columns() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("shared_dml_wide");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    // create SHARED table with the same schema
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL,
            age INT,
            active BOOLEAN,
            score DOUBLE,
            balance BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note VARCHAR
        ) WITH (
            TYPE = 'SHARED',
            FLUSH_POLICY = 'rows:10'
        )"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create shared table should succeed");

    run_dml_sequence(&full, true);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

// Subscription coverage for UPDATE and DELETE notifications on a user table with
// _updated/_deleted columns.
#[ntest::timeout(180000)]
#[test]
fn smoke_subscription_update_delete_notifications() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("user_sub_dml");
    let full = format!("{}.{}", namespace, table);

    create_namespace(&namespace);

    let create_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR, updated_at TIMESTAMP, is_deleted \
         BOOLEAN, note VARCHAR) WITH (TYPE = 'USER')",
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // Insert initial row BEFORE subscribing
    let _ = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name, updated_at, is_deleted, note) VALUES (1, 'one', 1730497770045, \
         false, 'n1')",
        full
    ));

    // Small delay to ensure data is persisted

    // Start subscription
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");
    let mut retries = 0;

    // Collect all events during initial loading and wait for Ready state
    let mut all_events: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut initial_data_received = false;

    while std::time::Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(50)) {
            Ok(Some(line)) => {
                println!("[subscription] Event: {}", &line[..std::cmp::min(200, line.len())]);
                all_events.push(line.clone());
                if line.contains("SUBSCRIPTION_FAILED")
                    || line.contains("Subscription registration failed")
                {
                    if retries < 1 {
                        retries += 1;
                        listener.stop().ok();
                        listener = SubscriptionListener::start(&query)
                            .expect("subscription retry should start");
                        continue;
                    }
                    break;
                }
                // Check if we got initial data
                if line.contains("InitialDataBatch") || line.contains("Ack") {
                    initial_data_received = true;
                    // Check if batch is ready (no more initial data pending)
                    if line.contains("Ready") || !line.contains("has_more: true") {
                        break;
                    }
                }
            },
            Ok(None) => break,
            Err(_) => {
                if initial_data_received {
                    break;
                }
                continue;
            },
        }
    }

    assert!(initial_data_received, "Should have received initial data batch");

    // Small delay to ensure subscription is fully ready

    // UPDATE - use a unique value we can search for
    let update_value = format!("upd_{}", std::process::id());
    let _ = execute_sql_as_root_via_client(&format!(
        "UPDATE {} SET name='{}' WHERE id=1",
        full, update_value
    ));

    // Wait for update event - look for our unique value
    let mut found_update = false;
    let update_deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < update_deadline {
        match listener.try_read_line(Duration::from_millis(50)) {
            Ok(Some(line)) => {
                println!(
                    "[subscription] After UPDATE: {}",
                    &line[..std::cmp::min(200, line.len())]
                );
                all_events.push(line.clone());
                if line.contains(&update_value) || line.contains("Update") {
                    found_update = true;
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert!(
        found_update,
        "Should have received UPDATE event with value '{}'. All events: {:?}",
        update_value,
        all_events.iter().take(5).collect::<Vec<_>>()
    );

    // DELETE
    let _ = execute_sql_as_root_via_client(&format!("DELETE FROM {} WHERE id=1", full));

    // Wait for delete event
    let mut found_delete = false;
    let delete_deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < delete_deadline {
        match listener.try_read_line(Duration::from_millis(50)) {
            Ok(Some(line)) => {
                println!(
                    "[subscription] After DELETE: {}",
                    &line[..std::cmp::min(200, line.len())]
                );
                all_events.push(line.clone());
                if line.contains("Delete") || line.contains("_deleted\": Bool(true)") {
                    found_delete = true;
                    break;
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert!(
        found_delete,
        "Should have received DELETE event. All events: {:?}",
        all_events.iter().rev().take(5).collect::<Vec<_>>()
    );

    listener.stop().ok();

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
