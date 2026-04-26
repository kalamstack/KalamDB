// Smoke Test 3: System tables and user lifecycle
// Covers: SELECT from system tables, CREATE USER, verify presence, DROP USER, STORAGE FLUSH ALL job

use crate::common::*;

#[ntest::timeout(180_000)]
#[test]
fn smoke_system_tables_and_user_lifecycle() {
    if !is_server_running() {
        println!(
            "Skipping smoke_system_tables_and_user_lifecycle: server not running at {}",
            server_url()
        );
        return;
    }

    // 1) SELECT from system tables (allow empty but must succeed)
    let system_queries = [
        "SELECT * FROM system.jobs LIMIT 1",
        "SELECT * FROM system.users LIMIT 1",
        "SELECT * FROM system.live LIMIT 1",
        "SELECT * FROM system.schemas LIMIT 1",
        "SELECT * FROM system.namespaces LIMIT 1",
    ];
    for q in system_queries {
        let _ = execute_sql_as_root_via_client(q).expect("system table query should succeed");
    }

    // 2) CREATE USER and verify present in system.users
    let user_id = generate_unique_namespace("smoke_user");
    let pass = "S1mpleP@ss!";
    let create_user = format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user_id, pass);
    execute_sql_as_root_via_client(&create_user).expect("create user should succeed");

    // Use SELECT user_id to avoid column truncation in pretty-printed tables
    let users_out = execute_sql_as_root_via_client(&format!(
        "SELECT user_id FROM system.users WHERE user_id='{}'",
        user_id
    ))
    .expect("select user should succeed");
    assert!(
        users_out.contains(&user_id),
        "expected newly created user to be listed: {}",
        users_out
    );

    // 3) DROP USER and verify removed or soft-deleted
    let drop_user = format!("DROP USER '{}'", user_id);
    execute_sql_as_root_via_client(&drop_user).expect("drop user should succeed");

    let users_out2 = execute_sql_as_root_via_client(&format!(
        "SELECT * FROM system.users WHERE user_id='{}'",
        user_id
    ))
    .expect("select user after drop should succeed");
    assert!(
        !users_out2.contains(&format!("| {} |", user_id))
            || users_out2.to_lowercase().contains("deleted"),
        "user should be removed or marked deleted: {}",
        users_out2
    );

    // 4) STORAGE FLUSH ALL should enqueue jobs and complete successfully
    // Create a test namespace and user table first
    // Use a unique namespace per run to avoid cross-test collisions when tests run in parallel
    let test_ns = generate_unique_namespace("smoke_test_flush");
    let _ = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", test_ns));

    // Create a user table to flush (unique per run to avoid collisions)
    let unique_tbl = generate_unique_table("test_flush_table");
    let test_table = format!("{}.{}", test_ns, unique_tbl);
    let create_table_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, value VARCHAR) WITH (TYPE = 'USER', FLUSH_POLICY = \
         'rows:100')",
        test_table
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("create test table should succeed");

    // Insert some data
    let insert_sql = format!("INSERT INTO {} (id, value) VALUES (1, 'test')", test_table);
    execute_sql_as_root_via_client(&insert_sql).expect("insert should succeed");

    // Now flush all tables in the namespace
    let flush_output = execute_sql_as_root_via_client(&format!("STORAGE FLUSH ALL IN {}", test_ns))
        .expect("flush all tables in namespace should succeed");

    println!("[FLUSH ALL] Output: {}", flush_output);

    // Parse job IDs from flush all output
    let job_ids = parse_job_ids_from_flush_all_output(&flush_output)
        .expect("should parse job IDs from FLUSH ALL output");

    println!("[FLUSH ALL] Job IDs: {:?}", job_ids);
    assert!(!job_ids.is_empty(), "should have at least one job ID");

    // Verify each job has been recorded in system.jobs (no need to wait for completion here)
    for job_id in &job_ids {
        let q = format!("SELECT job_id FROM system.jobs WHERE job_id='{}'", job_id);
        let out = execute_sql_as_root_via_client(&q).expect("query system.jobs should succeed");
        // The pretty table may truncate long IDs; rely on row count footer instead
        assert!(
            out.contains("(1 row)"),
            "expected exactly one matching job row for id {}, got: {}",
            job_id,
            out
        );
    }
    println!("[FLUSH ALL] Verified {} jobs recorded in system.jobs", job_ids.len());

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", test_ns));
}
