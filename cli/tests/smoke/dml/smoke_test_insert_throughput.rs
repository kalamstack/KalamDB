// Insert throughput benchmark smoke test
// Measures how many inserts per second KalamDB can handle under various conditions:
// - Single-row inserts
// - Batched inserts (multi-row VALUES)
// - Parallel inserts

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use crate::common::*;

// Test configuration
const SINGLE_INSERT_COUNT: usize = 100; // Single-row inserts to test
const BATCH_SIZE: usize = 100; // Rows per batch insert
const BATCH_COUNT: usize = 10; // Number of batch inserts
const PARALLEL_WORKERS: usize = 10; // Concurrent insert workers
const PARALLEL_INSERTS_PER_WORKER: usize = 50; // Inserts per worker

#[ntest::timeout(180000)]
#[test]
fn smoke_test_insert_throughput_single() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_insert_throughput_single: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Insert Throughput Benchmark: Single-Row Inserts ===\n");

    let namespace = generate_unique_namespace("insert_bench");
    let table = generate_unique_table("single");
    let full_table_name = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT NOT NULL, created_at TIMESTAMP \
         DEFAULT NOW())",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    // Warm up with a single insert
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, value) VALUES (0, 'warmup')",
        full_table_name
    ))
    .expect("Warmup insert should succeed");

    // Benchmark single-row inserts
    let start = Instant::now();
    let mut successful = 0;
    let mut failed = 0;

    for i in 1..=SINGLE_INSERT_COUNT {
        let sql = format!(
            "INSERT INTO {} (id, value) VALUES ({}, 'single_insert_{}')",
            full_table_name, i, i
        );
        match execute_sql_as_root_via_client(&sql) {
            Ok(_) => successful += 1,
            Err(e) => {
                failed += 1;
                if failed <= 3 {
                    eprintln!("Insert {} failed: {}", i, e);
                }
            },
        }
    }

    let elapsed = start.elapsed();
    let inserts_per_sec = if elapsed.as_secs_f64() > 0.0 {
        successful as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("────────────────────────────────────────────────────────");
    println!("SINGLE-ROW INSERT RESULTS:");
    println!("  Total inserts attempted: {}", SINGLE_INSERT_COUNT);
    println!("  Successful: {}", successful);
    println!("  Failed: {}", failed);
    println!("  Total time: {:.2?}", elapsed);
    println!("  ⚡ Throughput: {:.2} inserts/sec", inserts_per_sec);
    println!(
        "  Average latency: {:.2}ms per insert",
        elapsed.as_millis() as f64 / successful as f64
    );
    println!("────────────────────────────────────────────────────────\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    assert!(successful > 0, "Expected at least some successful inserts, got 0");
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_insert_throughput_batched() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_insert_throughput_batched: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Insert Throughput Benchmark: Batched Inserts ===\n");

    let namespace = generate_unique_namespace("insert_bench");
    let table = generate_unique_table("batch");
    let full_table_name = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT NOT NULL, batch_id INT)",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    // Benchmark batched inserts
    let start = Instant::now();
    let mut total_rows = 0;
    let mut successful_batches = 0;
    let mut failed_batches = 0;

    for batch_num in 0..BATCH_COUNT {
        let values: Vec<String> = (0..BATCH_SIZE)
            .map(|i| {
                let id = batch_num * BATCH_SIZE + i;
                format!("({}, 'batch_value_{}', {})", id, id, batch_num)
            })
            .collect();

        let sql = format!(
            "INSERT INTO {} (id, value, batch_id) VALUES {}",
            full_table_name,
            values.join(", ")
        );

        match execute_sql_as_root_via_client(&sql) {
            Ok(_) => {
                successful_batches += 1;
                total_rows += BATCH_SIZE;
            },
            Err(e) => {
                failed_batches += 1;
                if failed_batches <= 3 {
                    eprintln!("Batch {} failed: {}", batch_num, e);
                }
            },
        }
    }

    let elapsed = start.elapsed();
    let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
        total_rows as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let batches_per_sec = if elapsed.as_secs_f64() > 0.0 {
        successful_batches as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("────────────────────────────────────────────────────────");
    println!("BATCHED INSERT RESULTS (batch size = {}):", BATCH_SIZE);
    println!("  Total batches attempted: {}", BATCH_COUNT);
    println!("  Successful batches: {}", successful_batches);
    println!("  Failed batches: {}", failed_batches);
    println!("  Total rows inserted: {}", total_rows);
    println!("  Total time: {:.2?}", elapsed);
    println!("  ⚡ Row throughput: {:.2} rows/sec", rows_per_sec);
    println!("  ⚡ Batch throughput: {:.2} batches/sec", batches_per_sec);
    println!(
        "  Average latency: {:.2}ms per batch",
        elapsed.as_millis() as f64 / successful_batches as f64
    );
    println!("────────────────────────────────────────────────────────\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    assert!(total_rows > 0, "Expected at least some rows inserted, got 0");
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_insert_throughput_parallel() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_insert_throughput_parallel: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Insert Throughput Benchmark: Parallel Inserts ===\n");

    let namespace = generate_unique_namespace("insert_bench");
    let table = generate_unique_table("parallel");
    let full_table_name = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    // Use AUTO_INCREMENT to avoid PK conflicts between parallel workers
    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY AUTO_INCREMENT, worker_id INT, seq INT, value \
         TEXT)",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    let successful_inserts = Arc::new(AtomicUsize::new(0));
    let failed_inserts = Arc::new(AtomicUsize::new(0));
    let full_table_name = Arc::new(full_table_name);

    // Spawn parallel workers
    let start = Instant::now();
    let mut handles = Vec::with_capacity(PARALLEL_WORKERS);

    for worker_id in 0..PARALLEL_WORKERS {
        let table_name = Arc::clone(&full_table_name);
        let success_counter = Arc::clone(&successful_inserts);
        let fail_counter = Arc::clone(&failed_inserts);

        handles.push(std::thread::spawn(move || {
            let mut local_success = 0;
            let mut local_fail = 0;

            for seq in 0..PARALLEL_INSERTS_PER_WORKER {
                let sql = format!(
                    "INSERT INTO {} (worker_id, seq, value) VALUES ({}, {}, 'worker_{}_seq_{}')",
                    table_name, worker_id, seq, worker_id, seq
                );
                match execute_sql_as_root_via_client(&sql) {
                    Ok(_) => local_success += 1,
                    Err(_) => local_fail += 1,
                }
            }

            success_counter.fetch_add(local_success, Ordering::SeqCst);
            fail_counter.fetch_add(local_fail, Ordering::SeqCst);
            (worker_id, local_success, local_fail)
        }));
    }

    // Wait for all workers
    let mut worker_results = Vec::new();
    for handle in handles {
        let result = handle.join().expect("Worker thread panicked");
        worker_results.push(result);
    }

    let elapsed = start.elapsed();
    let total_success = successful_inserts.load(Ordering::SeqCst);
    let total_fail = failed_inserts.load(Ordering::SeqCst);
    let total_attempted = PARALLEL_WORKERS * PARALLEL_INSERTS_PER_WORKER;

    let inserts_per_sec = if elapsed.as_secs_f64() > 0.0 {
        total_success as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("────────────────────────────────────────────────────────");
    println!("PARALLEL INSERT RESULTS ({} workers):", PARALLEL_WORKERS);
    println!("  Inserts per worker: {}", PARALLEL_INSERTS_PER_WORKER);
    println!("  Total inserts attempted: {}", total_attempted);
    println!("  Successful: {}", total_success);
    println!("  Failed: {}", total_fail);
    println!("  Total time: {:.2?}", elapsed);
    println!("  ⚡ Throughput: {:.2} inserts/sec", inserts_per_sec);
    println!(
        "  Average latency: {:.2}ms per insert",
        elapsed.as_millis() as f64 / total_success.max(1) as f64
    );
    println!("\n  Per-worker breakdown:");
    for (worker_id, success, fail) in &worker_results {
        println!("    Worker {}: {} success, {} failed", worker_id, success, fail);
    }
    println!("────────────────────────────────────────────────────────\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", *full_table_name));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    assert!(total_success > 0, "Expected at least some successful inserts, got 0");
}

/// Combined benchmark that runs all tests and provides a summary
#[ntest::timeout(300000)]
#[test]
fn smoke_test_insert_throughput_summary() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_insert_throughput_summary: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      KalamDB INSERT THROUGHPUT BENCHMARK SUMMARY             ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let namespace = generate_unique_namespace("bench_summary");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    // Test 1: Single-row inserts
    let single_table = format!("{}.single_test", namespace);
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT)",
        single_table
    ))
    .expect("CREATE TABLE should succeed");

    let single_count = 200;
    let single_start = Instant::now();
    let mut single_success = 0;
    for i in 0..single_count {
        if execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, value) VALUES ({}, 'v{}')",
            single_table, i, i
        ))
        .is_ok()
        {
            single_success += 1;
        }
    }
    let single_elapsed = single_start.elapsed();
    let single_rate = single_success as f64 / single_elapsed.as_secs_f64();

    // Test 2: Batched inserts (100 rows per batch)
    let batch_table = format!("{}.batch_test", namespace);
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT)",
        batch_table
    ))
    .expect("CREATE TABLE should succeed");

    let batch_size = 100;
    let batch_count = 20;
    let batch_start = Instant::now();
    let mut batch_rows = 0;
    for batch in 0..batch_count {
        let values: Vec<String> = (0..batch_size)
            .map(|i| {
                let id = batch * batch_size + i;
                format!("({}, 'v{}')", id, id)
            })
            .collect();
        if execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, value) VALUES {}",
            batch_table,
            values.join(", ")
        ))
        .is_ok()
        {
            batch_rows += batch_size;
        }
    }
    let batch_elapsed = batch_start.elapsed();
    let batch_rate = batch_rows as f64 / batch_elapsed.as_secs_f64();

    // Test 3: Parallel inserts (10 threads, 100 each)
    let parallel_table = format!("{}.parallel_test", namespace);
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY AUTO_INCREMENT, worker INT, value TEXT)",
        parallel_table
    ))
    .expect("CREATE TABLE should succeed");

    let workers = 10;
    let per_worker = 100;
    let parallel_success = Arc::new(AtomicUsize::new(0));
    let parallel_table = Arc::new(parallel_table);

    let parallel_start = Instant::now();
    let handles: Vec<_> = (0..workers)
        .map(|w| {
            let table = Arc::clone(&parallel_table);
            let counter = Arc::clone(&parallel_success);
            std::thread::spawn(move || {
                for i in 0..per_worker {
                    if execute_sql_as_root_via_client(&format!(
                        "INSERT INTO {} (worker, value) VALUES ({}, 'w{}i{}')",
                        table, w, w, i
                    ))
                    .is_ok()
                    {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Worker panicked");
    }
    let parallel_elapsed = parallel_start.elapsed();
    let parallel_total = parallel_success.load(Ordering::SeqCst);
    let parallel_rate = parallel_total as f64 / parallel_elapsed.as_secs_f64();

    // Print summary
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│                    BENCHMARK RESULTS                       │");
    println!("├────────────────────────────────────────────────────────────┤");
    println!("│  Test Type              │  Rows   │  Time    │  Rate       │");
    println!("├────────────────────────────────────────────────────────────┤");
    println!(
        "│  Single-row inserts     │  {:>5}  │  {:>6.2}s │  {:>8.1}/s  │",
        single_success,
        single_elapsed.as_secs_f64(),
        single_rate
    );
    println!(
        "│  Batched (100/batch)    │  {:>5}  │  {:>6.2}s │  {:>8.1}/s  │",
        batch_rows,
        batch_elapsed.as_secs_f64(),
        batch_rate
    );
    println!(
        "│  Parallel (10 threads)  │  {:>5}  │  {:>6.2}s │  {:>8.1}/s  │",
        parallel_total,
        parallel_elapsed.as_secs_f64(),
        parallel_rate
    );
    println!("└────────────────────────────────────────────────────────────┘");
    println!();
    println!(
        "📊 Batched inserts are {:.1}x faster than single-row inserts",
        batch_rate / single_rate
    );
    println!(
        "📊 Parallel inserts are {:.1}x faster than single-row inserts",
        parallel_rate / single_rate
    );
    println!();

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    println!("=== Insert Throughput Benchmark Complete ===\n");
}
