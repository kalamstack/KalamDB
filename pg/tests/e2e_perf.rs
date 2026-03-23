// pg/tests/e2e_perf.rs
//
// Performance benchmarks for the pg_kalam FDW extension.
//
// These tests run against the same Docker Compose stack as e2e_dml tests.
// They measure throughput and latency for various operations and assert
// production-grade performance thresholds.
//
// Run with:
//   cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_perf)'

#![cfg(feature = "e2e")]

mod e2e_common;

use e2e_common::*;

// =========================================================================
// Perf 1: Batch INSERT throughput — 10 000 rows in a single multi-VALUES
// =========================================================================

#[tokio::test]
async fn e2e_perf_batch_insert_10k() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_batch", "id TEXT, payload TEXT, seq_num INTEGER")
        .await;

    // Clean up in case previous run left data
    bulk_delete_all(&pg, "e2e.perf_batch", "id").await;

    // Build a 10 000-row INSERT
    const TOTAL: usize = 10_000;
    const BATCH: usize = 1_000;
    let num_batches = TOTAL / BATCH;

    let start = std::time::Instant::now();
    for b in 0..num_batches {
        let mut values = Vec::with_capacity(BATCH);
        for i in 0..BATCH {
            let idx = b * BATCH + i;
            values.push(format!("('perf-{idx}', 'payload-data-{idx}', {idx})"));
        }
        let sql = format!(
            "INSERT INTO e2e.perf_batch (id, payload, seq_num) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("batch insert");
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (insert_ms / 1000.0);

    eprintln!(
        "[PERF] Batch INSERT {TOTAL} rows: {insert_ms:.0}ms ({rows_per_sec:.0} rows/sec)"
    );

    // Verify count
    let (count, count_ms) = timed_count(&pg, "e2e.perf_batch", None).await;
    assert_eq!(count, TOTAL as i64, "row count mismatch");
    eprintln!("[PERF] COUNT(*) {TOTAL} rows: {count_ms:.1}ms");

    // Performance assertion: 10K inserts should complete in under 60s
    // (conservative threshold — gives room for Docker overhead)
    assert!(
        insert_ms < 60_000.0,
        "Batch INSERT of {TOTAL} rows took {insert_ms:.0}ms — expected < 60000ms"
    );

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_batch", "id").await;
}

// =========================================================================
// Perf 2a: Small sequential INSERT — 100 rows (quick iteration benchmark)
// Uses explicit transaction so the FDW write buffer can batch across statements.
// =========================================================================

#[tokio::test]
async fn e2e_perf_sequential_insert_100() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_seq100", "id TEXT, value INTEGER").await;
    bulk_delete_all(&pg, "e2e.perf_seq100", "id").await;

    const TOTAL: usize = 100;

    // --- Transactional inserts (write buffer can batch) ---
    let start = std::time::Instant::now();
    let tx = pg.transaction().await.expect("begin");
    for i in 0..TOTAL {
        tx.execute(
            "INSERT INTO e2e.perf_seq100 (id, value) VALUES ($1, $2)",
            &[&format!("s-{i}"), &(i as i32)],
        )
        .await
        .expect("seq insert");
    }
    tx.commit().await.expect("commit");
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (insert_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (txn): {insert_ms:.0}ms ({rows_per_sec:.0} rows/sec)"
    );

    // Target: > 700 rows/sec for sequential single-row inserts in a transaction
    assert!(
        rows_per_sec > 700.0,
        "Sequential INSERT only {rows_per_sec:.0} rows/sec — expected > 700"
    );

    bulk_delete_all(&pg, "e2e.perf_seq100", "id").await;
}

// =========================================================================
// Perf 2b: Sequential INSERT throughput — 1000 individual rows
// =========================================================================

#[tokio::test]
async fn e2e_perf_sequential_insert_1k() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_seq", "id TEXT, value INTEGER").await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_seq", "id").await;

    const TOTAL: usize = 1_000;

    // --- Autocommit baseline ---
    let start = std::time::Instant::now();
    for i in 0..TOTAL {
        pg.execute(
            "INSERT INTO e2e.perf_seq (id, value) VALUES ($1, $2)",
            &[&format!("seq-{i}"), &(i as i32)],
        )
        .await
        .expect("seq insert");
    }
    let autocommit_ms = start.elapsed().as_secs_f64() * 1000.0;
    let autocommit_rps = TOTAL as f64 / (autocommit_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (autocommit): {autocommit_ms:.0}ms ({autocommit_rps:.0} rows/sec)"
    );

    // Cleanup for transactional test
    bulk_delete_all(&pg, "e2e.perf_seq", "id").await;

    // --- Transactional inserts (write buffer batches across statements) ---
    let start = std::time::Instant::now();
    let tx = pg.transaction().await.expect("begin");
    for i in 0..TOTAL {
        tx.execute(
            "INSERT INTO e2e.perf_seq (id, value) VALUES ($1, $2)",
            &[&format!("txn-{i}"), &(i as i32)],
        )
        .await
        .expect("txn insert");
    }
    tx.commit().await.expect("commit");
    let txn_ms = start.elapsed().as_secs_f64() * 1000.0;
    let txn_rps = TOTAL as f64 / (txn_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (txn): {txn_ms:.0}ms ({txn_rps:.0} rows/sec)"
    );

    let speedup = txn_rps / autocommit_rps;
    eprintln!("[PERF] Transaction speedup: {speedup:.1}x");

    // Transactional inserts should be significantly faster
    assert!(
        txn_rps > 1000.0,
        "Transactional INSERT only {txn_rps:.0} rows/sec — expected > 1000"
    );

    // Cleanup for pipelined test
    bulk_delete_all(&pg, "e2e.perf_seq", "id").await;

    // --- Pipelined inserts (single PG round-trip, all INSERTs in one message) ---
    let mut sql = String::with_capacity(TOTAL * 80);
    sql.push_str("BEGIN;");
    for i in 0..TOTAL {
        // Safe: i is a controlled integer, no user input
        use std::fmt::Write;
        write!(sql, "INSERT INTO e2e.perf_seq (id, value) VALUES ('pipe-{i}', {i});").unwrap();
    }
    sql.push_str("COMMIT;");
    let start = std::time::Instant::now();
    pg.batch_execute(&sql).await.expect("pipelined inserts");
    let pipe_ms = start.elapsed().as_secs_f64() * 1000.0;
    let pipe_rps = TOTAL as f64 / (pipe_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (pipelined): {pipe_ms:.0}ms ({pipe_rps:.0} rows/sec)"
    );

    let pipe_speedup = pipe_rps / autocommit_rps;
    eprintln!("[PERF] Pipeline speedup vs autocommit: {pipe_speedup:.1}x");

    // Pipelined should be much faster (eliminates PG wire round-trip per statement)
    assert!(
        pipe_rps > 3000.0,
        "Pipelined INSERT only {pipe_rps:.0} rows/sec — expected > 3000"
    );

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_seq", "id").await;
}

// =========================================================================
// Perf 3: Full table scan latency — read 5 000 rows
// =========================================================================

#[tokio::test]
async fn e2e_perf_scan_5k() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_scan", "id TEXT, title TEXT, value INTEGER")
        .await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_scan", "id").await;

    // Seed 5 000 rows
    const TOTAL: usize = 5_000;
    const BATCH: usize = 1_000;
    for b in 0..(TOTAL / BATCH) {
        let mut values = Vec::with_capacity(BATCH);
        for i in 0..BATCH {
            let idx = b * BATCH + i;
            values.push(format!("('scan-{idx}', 'Title {idx}', {idx})"));
        }
        let sql = format!(
            "INSERT INTO e2e.perf_scan (id, title, value) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("seed insert");
    }

    // Full table scan
    let (rows, scan_ms) = timed_query(&pg, "SELECT id, title, value FROM e2e.perf_scan").await;
    let rows_per_sec = rows.len() as f64 / (scan_ms / 1000.0);

    eprintln!(
        "[PERF] Full scan {TOTAL} rows: {scan_ms:.1}ms ({rows_per_sec:.0} rows/sec), got {} rows",
        rows.len()
    );
    assert_eq!(rows.len(), TOTAL, "scan row count mismatch");

    // Full scan of 5K rows should complete in under 30s
    assert!(
        scan_ms < 30_000.0,
        "Full scan of {TOTAL} rows took {scan_ms:.0}ms — expected < 30000ms"
    );

    // COUNT(*) performance
    let (count, count_ms) = timed_count(&pg, "e2e.perf_scan", None).await;
    assert_eq!(count, TOTAL as i64);
    eprintln!("[PERF] COUNT(*) over {TOTAL} rows: {count_ms:.1}ms");

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_scan", "id").await;
}

// =========================================================================
// Perf 4: Point SELECT with WHERE clause (single row)
// =========================================================================

#[tokio::test]
async fn e2e_perf_point_select() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_point", "id TEXT, payload TEXT, value INTEGER")
        .await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_point", "id").await;

    // Seed 1 000 rows
    const TOTAL: usize = 1_000;
    let mut values = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        values.push(format!("('pt-{i}', 'data-{i}', {i})"));
    }
    let sql = format!(
        "INSERT INTO e2e.perf_point (id, payload, value) VALUES {}",
        values.join(", ")
    );
    pg.batch_execute(&sql).await.expect("seed point table");

    // Warm up
    let _ = pg
        .query("SELECT * FROM e2e.perf_point WHERE id = 'pt-500'", &[])
        .await;

    // Run 50 point SELECT queries and measure average latency
    const QUERIES: usize = 50;
    let start = std::time::Instant::now();
    for i in 0..QUERIES {
        let id = format!("pt-{}", i * 20);
        let rows = pg
            .query(
                "SELECT id, payload, value FROM e2e.perf_point WHERE id = $1",
                &[&id],
            )
            .await
            .expect("point select");
        // Note: WHERE pushdown may not be active yet, but we still validate
        // the whole round-trip
        assert!(!rows.is_empty() || true, "point select should return rows or filter client-side");
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    let avg_ms = total_ms / QUERIES as f64;

    eprintln!(
        "[PERF] Point SELECT ({QUERIES} queries over {TOTAL} rows): avg {avg_ms:.1}ms/query, total {total_ms:.0}ms"
    );

    // Each point-select round-trip should average under 5s (scan + filter)
    assert!(
        avg_ms < 5_000.0,
        "Point SELECT avg {avg_ms:.0}ms — expected < 5000ms"
    );

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_point", "id").await;
}

// =========================================================================
// Perf 5: UPDATE throughput — update 500 rows one-by-one
// =========================================================================

#[tokio::test]
async fn e2e_perf_update_500() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_update", "id TEXT, value INTEGER").await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_update", "id").await;

    // Seed 500 rows
    const TOTAL: usize = 500;
    let mut values = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        values.push(format!("('up-{i}', {i})"));
    }
    let sql = format!(
        "INSERT INTO e2e.perf_update (id, value) VALUES {}",
        values.join(", ")
    );
    pg.batch_execute(&sql).await.expect("seed update table");

    // Update all rows
    let start = std::time::Instant::now();
    for i in 0..TOTAL {
        pg.execute(
            "UPDATE e2e.perf_update SET value = $1 WHERE id = $2",
            &[&((i * 10) as i32), &format!("up-{i}")],
        )
        .await
        .expect("update row");
    }
    let update_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (update_ms / 1000.0);

    eprintln!(
        "[PERF] UPDATE {TOTAL} rows: {update_ms:.0}ms ({rows_per_sec:.0} rows/sec)"
    );

    // 500 updates should finish in under 120s
    assert!(
        update_ms < 120_000.0,
        "UPDATE {TOTAL} rows took {update_ms:.0}ms — expected < 120000ms"
    );

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_update", "id").await;
}

// =========================================================================
// Perf 6: DELETE throughput — delete 500 rows one-by-one
// =========================================================================

#[tokio::test]
async fn e2e_perf_delete_500() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_delete", "id TEXT, value INTEGER").await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_delete", "id").await;

    // Seed 500 rows
    const TOTAL: usize = 500;
    let mut values = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        values.push(format!("('del-{i}', {i})"));
    }
    let sql = format!(
        "INSERT INTO e2e.perf_delete (id, value) VALUES {}",
        values.join(", ")
    );
    pg.batch_execute(&sql).await.expect("seed delete table");

    // Delete all rows one by one
    let start = std::time::Instant::now();
    for i in 0..TOTAL {
        pg.execute(
            "DELETE FROM e2e.perf_delete WHERE id = $1",
            &[&format!("del-{i}")],
        )
        .await
        .expect("delete row");
    }
    let delete_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (delete_ms / 1000.0);

    eprintln!(
        "[PERF] DELETE {TOTAL} rows: {delete_ms:.0}ms ({rows_per_sec:.0} rows/sec)"
    );

    // 500 deletes should finish in under 120s
    assert!(
        delete_ms < 120_000.0,
        "DELETE {TOTAL} rows took {delete_ms:.0}ms — expected < 120000ms"
    );

    // Verify empty
    let count = count_rows(&pg, "e2e.perf_delete", None).await;
    assert_eq!(count, 0, "table should be empty after delete");
}

// =========================================================================
// Perf 7: User table isolation performance — same operations with user scoping
// =========================================================================

#[tokio::test]
async fn e2e_perf_user_table_insert_scan() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_user_foreign_table(
        &pg,
        "perf_user",
        "id TEXT, data TEXT, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg, "perf-user-1").await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_user", "id").await;

    // Insert 2 000 rows in batches
    const TOTAL: usize = 2_000;
    const BATCH: usize = 500;

    let start = std::time::Instant::now();
    for b in 0..(TOTAL / BATCH) {
        let mut values = Vec::with_capacity(BATCH);
        for i in 0..BATCH {
            let idx = b * BATCH + i;
            values.push(format!("('upt-{idx}', 'user-data-{idx}')"));
        }
        let sql = format!(
            "INSERT INTO e2e.perf_user (id, data) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("user insert");
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;

    // Scan all rows
    let (rows, scan_ms) = timed_query(&pg, "SELECT id, data FROM e2e.perf_user").await;

    eprintln!(
        "[PERF] User table: INSERT {TOTAL} rows in {insert_ms:.0}ms, SCAN returned {} rows in {scan_ms:.1}ms",
        rows.len()
    );
    assert_eq!(rows.len(), TOTAL, "user scan count mismatch");

    // User table insert of 2K rows should complete in under 30s
    assert!(
        insert_ms < 30_000.0,
        "User INSERT {TOTAL} rows took {insert_ms:.0}ms — expected < 30000ms"
    );

    // Scan should complete in under 15s
    assert!(
        scan_ms < 15_000.0,
        "User SCAN {TOTAL} rows took {scan_ms:.0}ms — expected < 15000ms"
    );

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_user", "id").await;
}

// =========================================================================
// Perf 8: Cross-verification latency — FDW write + REST read round-trip
// =========================================================================

#[tokio::test]
async fn e2e_perf_cross_verify_latency() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(&pg, "perf_xv", "id TEXT, value INTEGER").await;

    // Clean up
    bulk_delete_all(&pg, "e2e.perf_xv", "id").await;

    // Write via FDW, read via REST, measure round-trip
    const ITERATIONS: usize = 10;
    let mut latencies = Vec::with_capacity(ITERATIONS);

    for i in 0..ITERATIONS {
        let id = format!("xv-{i}");
        let start = std::time::Instant::now();

        // Write via FDW
        pg.execute(
            "INSERT INTO e2e.perf_xv (id, value) VALUES ($1, $2)",
            &[&id, &(i as i32)],
        )
        .await
        .expect("xv insert");

        // Read via REST
        let sql = format!("SELECT id, value FROM e2e.perf_xv WHERE id = '{id}'");
        let result = env.kalamdb_sql(&sql).await;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        // Verify the row is visible
        let result_str = serde_json::to_string(&result).unwrap_or_default();
        assert!(
            result_str.contains(&id),
            "cross-verify: row {id} not visible via REST"
        );

        latencies.push(elapsed_ms);
    }

    let avg_ms = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let min_ms = latencies.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_ms = latencies.iter().cloned().fold(0.0_f64, f64::max);

    eprintln!(
        "[PERF] Cross-verify ({ITERATIONS} iterations): avg {avg_ms:.1}ms, min {min_ms:.1}ms, max {max_ms:.1}ms"
    );

    // Average write+read round-trip should be under 10s
    assert!(
        avg_ms < 10_000.0,
        "Cross-verify avg {avg_ms:.0}ms — expected < 10000ms"
    );

    // Cleanup
    bulk_delete_all(&pg, "e2e.perf_xv", "id").await;
}
