// High-concurrency smoke test to ensure SELECT * queries don't starve under load
// Creates a user table with ~1k rows and fires parallel SELECT * queries.
//
// This test uses kalam-client directly instead of spawning CLI subprocesses,
// which avoids macOS TCP connection limits when running many parallel queries.
// The parallelism is handled within a single process using std threads that share
// a single tokio runtime.

use crate::common::*;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Keep enough parallelism to exercise concurrent reads without poisoning later smoke
// tests on externally managed servers that enforce stricter request/IP guards.
const PARALLEL_QUERIES: usize = 32;
const ROW_TARGET: usize = 500;
const INSERT_CHUNK_SIZE: usize = 1000;
const MAX_QUERY_DURATION: Duration = Duration::from_secs(60);
const MAX_ROWS_IN_TABLE: usize = 2_000;

fn print_phase0_explain_baseline(label: &str, query: &str) {
    let explain_sql = format!("EXPLAIN {}", query);
    let explain_output = execute_sql_as_root_via_client(&explain_sql)
        .unwrap_or_else(|error| panic!("phase-0 EXPLAIN failed for '{}': {}", explain_sql, error));
    println!("Phase-0 EXPLAIN baseline [{}]:\n{}", label, explain_output);

    let analyze_sql = format!("EXPLAIN ANALYZE {}", query);
    let analyze_output = execute_sql_as_root_via_client(&analyze_sql).unwrap_or_else(|error| {
        panic!(
            "phase-0 EXPLAIN ANALYZE failed for '{}': {}",
            analyze_sql,
            error
        )
    });
    println!(
        "Phase-0 EXPLAIN ANALYZE baseline [{}]:\n{}",
        label,
        analyze_output
    );
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_00_parallel_query_burst() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_00_parallel_query_burst: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Starting Parallel Query Burst Smoke Test ===\n");

    // Use a unique namespace to avoid cross-test / cross-run state (the server may persist data).
    let namespace = generate_unique_namespace("smoke_parallel");
    let table = "user_table";
    let full_table_name = format!("{}.{}", namespace, table);

    // Ensure namespace exists
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    // Create the user table with simple schema
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, value VARCHAR NOT NULL) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000')",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    // Discover current row count and max(id) so we can cap growth to MAX_ROWS_IN_TABLE
    let count_sql = format!("SELECT COUNT(*) AS total_count FROM {}", full_table_name);
    let count_before_output = execute_sql_as_root_via_client_json(&count_sql)
        .expect("COUNT(*) should succeed before inserts");
    let current_rows = extract_scalar(&count_before_output, "total_count") as usize;
    println!(
        "{} currently has {} rows (limit {}).",
        full_table_name, current_rows, MAX_ROWS_IN_TABLE
    );

    let max_id_sql = format!("SELECT COALESCE(MAX(id), -1) AS max_id FROM {}", full_table_name);
    let max_id_output =
        execute_sql_as_root_via_client_json(&max_id_sql).expect("MAX(id) query should succeed");
    let max_id = extract_scalar(&max_id_output, "max_id");
    // next_id is MAX(id) + 1, or 0 if table is empty
    let next_id = if max_id < 0 { 0 } else { (max_id + 1) as usize };

    if current_rows >= MAX_ROWS_IN_TABLE {
        println!(
            "Table already at/above {} rows; skipping insert phase to respect cap.",
            MAX_ROWS_IN_TABLE
        );
    } else {
        let target_total = (current_rows + ROW_TARGET).min(MAX_ROWS_IN_TABLE);
        let rows_to_insert = target_total - current_rows;
        println!(
            "Inserting {} rows to reach {} total (next id starts at {}).",
            rows_to_insert, target_total, next_id
        );

        let mut inserted = 0;
        while inserted < rows_to_insert {
            let batch_size = usize::min(INSERT_CHUNK_SIZE, rows_to_insert - inserted);
            let values = (0..batch_size)
                .map(|offset| {
                    let id = next_id + inserted + offset;
                    format!("({}, 'payload_{}')", id, id)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let insert_sql =
                format!("INSERT INTO {} (id, value) VALUES {}", full_table_name, values);
            execute_sql_as_root_via_client(&insert_sql).expect("INSERT chunk should succeed");
            inserted += batch_size;
        }
    }

    println!(
        "Issuing STORAGE FLUSH TABLE {} to force persistence before load test",
        full_table_name
    );
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    match execute_sql_as_root_via_client(&flush_sql) {
        Ok(output) => println!("STORAGE FLUSH TABLE acknowledged: {}", output.trim()),
        Err(err) => panic!("STORAGE FLUSH TABLE {} failed: {}", full_table_name, err),
    }

    // Verify row count via JSON output for precise validation
    let count_output = execute_sql_as_root_via_client_json(&count_sql)
        .expect("COUNT(*) should succeed for prepared table");
    let total_count = extract_scalar(&count_output, "total_count") as usize;
    println!("Verified row count: {}", total_count);

    print_phase0_explain_baseline(
        "parallel_query_burst_select_all",
        &format!("SELECT * FROM {}", full_table_name),
    );

    println!(
        "Loaded {} rows into {}. Launching {} parallel SELECT * queries...",
        total_count, full_table_name, PARALLEL_QUERIES
    );

    let select_sql = Arc::new(format!("SELECT * FROM {}", full_table_name));
    let overall_start = Instant::now();

    // Use std threads with the client-based execution to run parallel queries
    // Each thread uses the shared tokio runtime from execute_sql_as_root_via_client
    let mut handles = Vec::with_capacity(PARALLEL_QUERIES);

    for idx in 0..PARALLEL_QUERIES {
        let sql_clone = Arc::clone(&select_sql);
        let suite_start = overall_start;

        handles.push(std::thread::spawn(move || {
            let launch = Instant::now();
            let offset = launch.checked_duration_since(suite_start).unwrap_or_default();
            // Use client-based execution instead of CLI subprocess
            let result = execute_sql_as_root_via_client(&sql_clone).map_err(|e| format!("{}", e));
            let duration = launch.elapsed();
            (idx, result, offset, duration)
        }));
    }

    let mut durations = Vec::with_capacity(PARALLEL_QUERIES);

    for handle in handles {
        let (idx, result, offset, duration) = handle.join().expect("Query thread panicked");
        let output = result.unwrap_or_else(|e| panic!("Parallel query {} failed: {}", idx, e));
        assert!(
            output.contains("row") || output.contains("|"),
            "Query {} output did not look like a result set: {}",
            idx,
            output
        );
        println!(
            "Query #{:03} completed in {:?} (started +{:?} from burst start)",
            idx, duration, offset
        );
        if duration > MAX_QUERY_DURATION {
            println!(
                "⚠️  Query #{:03} exceeded {:?} (took {:?})",
                idx, MAX_QUERY_DURATION, duration
            );
        }
        durations.push(duration);
    }

    let max_duration = durations.iter().copied().max().unwrap_or_default();
    let avg_duration = if durations.is_empty() {
        Duration::from_secs(0)
    } else {
        let total_ns: u128 = durations.iter().map(|d| d.as_nanos()).sum();
        Duration::from_nanos((total_ns / durations.len() as u128) as u64)
    };

    println!(
        "Parallel query burst finished in {:?} (max {:?}, avg {:?})",
        overall_start.elapsed(),
        max_duration,
        avg_duration
    );
    println!(
        "Phase-0 baseline metric [parallel_burst_max_query_ms]={:.3}",
        max_duration.as_secs_f64() * 1000.0
    );
    println!(
        "Phase-0 baseline metric [parallel_burst_avg_query_ms]={:.3}",
        avg_duration.as_secs_f64() * 1000.0
    );
    println!(
        "Phase-0 baseline metric [parallel_burst_total_secs]={:.3}",
        overall_start.elapsed().as_secs_f64()
    );

    assert!(
        max_duration <= MAX_QUERY_DURATION,
        "Expected SELECT * calls to complete within {:?}; slowest took {:?}",
        MAX_QUERY_DURATION,
        max_duration
    );

    // Cleanup to avoid leaving state behind on the running server.
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n=== Parallel Query Burst Smoke Test Complete ===\n");
}

fn extract_scalar(json_output: &str, field: &str) -> i64 {
    let value: Value = serde_json::from_str(json_output)
        .unwrap_or_else(|e| panic!("Failed to parse JSON output: {} in: {}", e, json_output));

    let rows = get_rows_as_hashmaps(&value)
        .unwrap_or_else(|| panic!("JSON response missing rows: {}", json_output));
    let first_row = rows
        .first()
        .unwrap_or_else(|| panic!("JSON response has no rows: {}", json_output));
    let field_value = first_row
        .get(field)
        .unwrap_or_else(|| panic!("JSON response missing field '{}': {}", field, json_output));

    let field_value = extract_typed_value(field_value);

    // Handle both number and string representations (large ints are serialized as strings for JS safety)
    match &field_value {
        Value::Number(n) => n
            .as_i64()
            .unwrap_or_else(|| panic!("Field '{}' is not a valid i64: {}", field, json_output)),
        Value::String(s) => s.parse::<i64>().unwrap_or_else(|_| {
            panic!("Field '{}' string is not a valid i64: {}", field, json_output)
        }),
        _ => panic!("Field '{}' is neither a number nor a string: {}", field, json_output),
    }
}
