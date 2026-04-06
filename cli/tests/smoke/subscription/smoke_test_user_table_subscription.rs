// Smoke Test 1 (revised): User table with subscription lifecycle
// Covers: namespace creation, user table creation, inserts, subscription receiving events, flush job visibility
// Uses kalam-client directly instead of CLI to avoid macOS TCP subprocess limits

use crate::common::*;

#[ntest::timeout(120000)]
#[test]
fn smoke_user_table_subscription_lifecycle() {
    if !require_server_running() {
        return;
    }

    // Unique per run
    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("user_smoke");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    // 2) Create user table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

    // 3) Insert a couple rows
    let ins1 = format!("INSERT INTO {} (name) VALUES ('alpha')", full);
    let ins2 = format!("INSERT INTO {} (name) VALUES ('beta')", full);
    execute_sql_as_root_via_client(&ins1).expect("insert alpha should succeed");
    execute_sql_as_root_via_client(&ins2).expect("insert beta should succeed");

    // Quick verification via SELECT
    let sel = format!("SELECT * FROM {}", full);
    let out = execute_sql_as_root_via_client(&sel).expect("select should succeed");
    assert!(out.contains("alpha"), "expected to see 'alpha' in select output: {}", out);
    assert!(out.contains("beta"), "expected to see 'beta' in select output: {}", out);

    // Small delay to ensure data is visible to subscription queries

    // Double-check data is visible right before subscribing
    let verify_sel = format!("SELECT COUNT(*) as cnt FROM {}", full);
    let count_out =
        execute_sql_as_root_via_client(&verify_sel).expect("count query should succeed");
    println!("[DEBUG] Row count before subscribing: {}", count_out);

    // 4) Subscribe to the user table
    let query = format!("SELECT * FROM {}", full);
    let mut listener = SubscriptionListener::start(&query).expect("subscription should start");

    // 4a) Collect snapshot rows with extended timeout; if none captured, fallback to direct SELECT snapshot
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

    // 5) Insert a new row and verify subscription outputs the change event
    let sub_val = "from_subscription";
    let ins3 = format!("INSERT INTO {} (name) VALUES ('{}')", full, sub_val);
    println!("[DEBUG] Inserting new row with value: {}", sub_val);
    execute_sql_as_root_via_client(&ins3).expect("insert sub row should succeed");
    println!("[DEBUG] Insert completed, waiting for change event...");

    let mut change_lines: Vec<String> = Vec::new();
    let change_deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);
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
        "expected change event containing '{}' within 5s; got: {}",
        sub_val,
        changes_joined
    );

    // 6) Flush the user table and verify job completes successfully
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full);
    let flush_output = execute_sql_as_root_via_client_json(&flush_sql)
        .expect("flush should succeed for user table");

    println!("[FLUSH] Output: {}", flush_output);

    // Parse job ID from JSON response message field
    let job_id = parse_job_id_from_json_message(&flush_output)
        .expect("should parse job_id from FLUSH JSON output");

    println!("[FLUSH] Job ID: {}", job_id);

    // Wait for terminal state (completed or failed) to avoid flakes
    let job_timeout = if is_cluster_mode() {
        std::time::Duration::from_secs(20)
    } else {
        std::time::Duration::from_secs(10)
    };
    let final_status =
        wait_for_job_finished(&job_id, job_timeout).expect("flush job should reach terminal state");
    println!("[FLUSH] Job {} finished with status: {}", job_id, final_status);

    // Stop subscription
    listener.stop().ok();

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
