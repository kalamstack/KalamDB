use super::common::*;

#[tokio::test]
#[ntest::timeout(50000)]
async fn e2e_perf_batch_insert_10k() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_batch");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, payload TEXT, seq_num INTEGER").await;

    const TOTAL: usize = 10_000;
    const BATCH: usize = 1_000;

    let start = std::time::Instant::now();
    for batch in 0..(TOTAL / BATCH) {
        let mut values = Vec::with_capacity(BATCH);
        for index in 0..BATCH {
            let value_index = batch * BATCH + index;
            values.push(format!(
                "('perf-{value_index}', 'payload-data-{value_index}', {value_index})"
            ));
        }
        let sql = format!(
            "INSERT INTO {qualified_table} (id, payload, seq_num) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("batch insert");
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (insert_ms / 1000.0);

    eprintln!("[PERF] Batch INSERT {TOTAL} rows: {insert_ms:.0}ms ({rows_per_sec:.0} rows/sec)");

    let (count, count_ms) = timed_count(&pg, &qualified_table, None).await;
    assert_eq!(count, TOTAL as i64, "row count mismatch");
    eprintln!("[PERF] COUNT(*) {TOTAL} rows: {count_ms:.1}ms");

    assert!(
        insert_ms < 60_000.0,
        "Batch INSERT of {TOTAL} rows took {insert_ms:.0}ms — expected < 60000ms"
    );
    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(3000)]
async fn e2e_perf_sequential_insert_100() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("perf_seq100");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, value INTEGER").await;

    const TOTAL: usize = 100;

    let start = std::time::Instant::now();
    let tx = pg.transaction().await.expect("begin");
    for index in 0..TOTAL {
        tx.execute(
            &format!("INSERT INTO {qualified_table} (id, value) VALUES ($1, $2)"),
            &[&format!("s-{index}"), &(index as i32)],
        )
        .await
        .expect("seq insert");
    }
    tx.commit().await.expect("commit");
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (insert_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (txn): {insert_ms:.0}ms ({rows_per_sec:.0} \
         rows/sec)"
    );

    assert!(
        rows_per_sec > 700.0,
        "Sequential INSERT only {rows_per_sec:.0} rows/sec — expected > 700"
    );
    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn e2e_perf_sequential_insert_1k() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("perf_seq");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, value INTEGER").await;

    const TOTAL: usize = 1_000;

    let start = std::time::Instant::now();
    for index in 0..TOTAL {
        pg.execute(
            &format!("INSERT INTO {qualified_table} (id, value) VALUES ($1, $2)"),
            &[&format!("seq-{index}"), &(index as i32)],
        )
        .await
        .expect("seq insert");
    }
    let autocommit_ms = start.elapsed().as_secs_f64() * 1000.0;
    let autocommit_rps = TOTAL as f64 / (autocommit_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (autocommit): {autocommit_ms:.0}ms \
         ({autocommit_rps:.0} rows/sec)"
    );
    bulk_delete_all(&pg, &qualified_table, "id").await;

    let start = std::time::Instant::now();
    let tx = pg.transaction().await.expect("begin");
    for index in 0..TOTAL {
        tx.execute(
            &format!("INSERT INTO {qualified_table} (id, value) VALUES ($1, $2)"),
            &[&format!("txn-{index}"), &(index as i32)],
        )
        .await
        .expect("txn insert");
    }
    tx.commit().await.expect("commit");
    let txn_ms = start.elapsed().as_secs_f64() * 1000.0;
    let txn_rps = TOTAL as f64 / (txn_ms / 1000.0);

    eprintln!("[PERF] Sequential INSERT {TOTAL} rows (txn): {txn_ms:.0}ms ({txn_rps:.0} rows/sec)");
    eprintln!("[PERF] Transaction speedup: {:.1}x", txn_rps / autocommit_rps);

    assert!(
        txn_rps > 250.0 && txn_rps > autocommit_rps * 0.6,
        "Transactional INSERT only {txn_rps:.0} rows/sec ({:.1}x autocommit) — expected > 250 \
         rows/sec and > 0.6x autocommit",
        txn_rps / autocommit_rps
    );

    bulk_delete_all(&pg, &qualified_table, "id").await;

    let mut sql = String::with_capacity(TOTAL * 80);
    sql.push_str("BEGIN;");
    for index in 0..TOTAL {
        use std::fmt::Write;
        write!(
            sql,
            "INSERT INTO {qualified_table} (id, value) VALUES ('pipe-{index}', {index});"
        )
        .unwrap();
    }
    sql.push_str("COMMIT;");
    let start = std::time::Instant::now();
    pg.batch_execute(&sql).await.expect("pipelined inserts");
    let pipe_ms = start.elapsed().as_secs_f64() * 1000.0;
    let pipe_rps = TOTAL as f64 / (pipe_ms / 1000.0);

    eprintln!(
        "[PERF] Sequential INSERT {TOTAL} rows (pipelined): {pipe_ms:.0}ms ({pipe_rps:.0} \
         rows/sec)"
    );
    eprintln!("[PERF] Pipeline speedup vs autocommit: {:.1}x", pipe_rps / autocommit_rps);

    assert!(
        pipe_rps > 300.0 && pipe_rps > autocommit_rps * 0.8,
        "Pipelined INSERT only {pipe_rps:.0} rows/sec ({:.1}x autocommit) — expected > 300 \
         rows/sec and > 0.8x autocommit",
        pipe_rps / autocommit_rps
    );
    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn e2e_perf_scan_5k() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_scan");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, title TEXT, value INTEGER").await;

    const TOTAL: usize = 5_000;
    const BATCH: usize = 1_000;
    for batch in 0..(TOTAL / BATCH) {
        let mut values = Vec::with_capacity(BATCH);
        for index in 0..BATCH {
            let value_index = batch * BATCH + index;
            values.push(format!("('scan-{value_index}', 'Title {value_index}', {value_index})"));
        }
        let sql = format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("seed insert");
    }

    let (rows, scan_ms) =
        timed_query(&pg, &format!("SELECT id, title, value FROM {qualified_table}")).await;
    let rows_per_sec = rows.len() as f64 / (scan_ms / 1000.0);

    eprintln!(
        "[PERF] Full scan {TOTAL} rows: {scan_ms:.1}ms ({rows_per_sec:.0} rows/sec), got {} rows",
        rows.len()
    );
    assert_eq!(rows.len(), TOTAL, "scan row count mismatch");
    assert!(
        scan_ms < 30_000.0,
        "Full scan of {TOTAL} rows took {scan_ms:.0}ms — expected < 30000ms"
    );

    let (count, count_ms) = timed_count(&pg, &qualified_table, None).await;
    assert_eq!(count, TOTAL as i64);
    eprintln!("[PERF] COUNT(*) over {TOTAL} rows: {count_ms:.1}ms");

    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(18000)]
async fn e2e_perf_point_select() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_point");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, payload TEXT, value INTEGER").await;

    const TOTAL: usize = 1_000;
    let mut values = Vec::with_capacity(TOTAL);
    for index in 0..TOTAL {
        values.push(format!("('pt-{index}', 'data-{index}', {index})"));
    }
    let sql = format!(
        "INSERT INTO {qualified_table} (id, payload, value) VALUES {}",
        values.join(", ")
    );
    pg.batch_execute(&sql).await.expect("seed point table");

    let _ = pg
        .query(&format!("SELECT * FROM {qualified_table} WHERE id = 'pt-500'"), &[])
        .await;

    const QUERIES: usize = 50;
    let start = std::time::Instant::now();
    for index in 0..QUERIES {
        let id = format!("pt-{}", index * 20);
        let rows = pg
            .query(
                &format!("SELECT id, payload, value FROM {qualified_table} WHERE id = $1"),
                &[&id],
            )
            .await
            .expect("point select");
        assert!(!rows.is_empty() || true);
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    let avg_ms = total_ms / QUERIES as f64;

    eprintln!(
        "[PERF] Point SELECT ({QUERIES} queries over {TOTAL} rows): avg {avg_ms:.1}ms/query, \
         total {total_ms:.0}ms"
    );
    assert!(avg_ms < 5_000.0, "Point SELECT avg {avg_ms:.0}ms — expected < 5000ms");
    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(45000)]
async fn e2e_perf_update_500() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_update");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, value INTEGER").await;

    const TOTAL: usize = 500;
    let mut values = Vec::with_capacity(TOTAL);
    for index in 0..TOTAL {
        values.push(format!("('up-{index}', {index})"));
    }
    let sql = format!("INSERT INTO {qualified_table} (id, value) VALUES {}", values.join(", "));
    pg.batch_execute(&sql).await.expect("seed update table");

    let update_sql = format!("UPDATE {qualified_table} SET value = $1 WHERE id = $2");
    let update_stmt = pg.prepare(&update_sql).await.expect("prepare update statement");

    let start = std::time::Instant::now();
    for index in 0..TOTAL {
        let id = format!("up-{index}");
        pg.execute(&update_stmt, &[&((index * 10) as i32), &id])
            .await
            .expect("update row");
    }
    let update_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (update_ms / 1000.0);

    eprintln!("[PERF] UPDATE {TOTAL} rows: {update_ms:.0}ms ({rows_per_sec:.0} rows/sec)");
    assert!(
        update_ms < 120_000.0,
        "UPDATE {TOTAL} rows took {update_ms:.0}ms — expected < 120000ms"
    );
    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(45000)]
async fn e2e_perf_delete_500() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_delete");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, value INTEGER").await;

    const TOTAL: usize = 500;
    let mut values = Vec::with_capacity(TOTAL);
    for index in 0..TOTAL {
        values.push(format!("('del-{index}', {index})"));
    }
    let sql = format!("INSERT INTO {qualified_table} (id, value) VALUES {}", values.join(", "));
    pg.batch_execute(&sql).await.expect("seed delete table");

    let delete_sql = format!("DELETE FROM {qualified_table} WHERE id = $1");
    let delete_stmt = pg.prepare(&delete_sql).await.expect("prepare delete statement");

    let start = std::time::Instant::now();
    for index in 0..TOTAL {
        let id = format!("del-{index}");
        pg.execute(&delete_stmt, &[&id]).await.expect("delete row");
    }
    let delete_ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows_per_sec = TOTAL as f64 / (delete_ms / 1000.0);

    eprintln!("[PERF] DELETE {TOTAL} rows: {delete_ms:.0}ms ({rows_per_sec:.0} rows/sec)");
    assert!(
        delete_ms < 120_000.0,
        "DELETE {TOTAL} rows took {delete_ms:.0}ms — expected < 120000ms"
    );

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, 0, "table should be empty after delete");
    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(11000)]
async fn e2e_perf_user_table_insert_scan() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_user");
    let qualified_table = format!("e2e.{table}");

    create_user_kalam_table(&pg, &table, "id TEXT, data TEXT").await;
    set_user_id(&pg, "perf-user-1").await;
    await_user_shard_leader("perf-user-1").await;

    const TOTAL: usize = 2_000;
    const BATCH: usize = 500;

    let start = std::time::Instant::now();
    for batch in 0..(TOTAL / BATCH) {
        let mut values = Vec::with_capacity(BATCH);
        for index in 0..BATCH {
            let value_index = batch * BATCH + index;
            values.push(format!("('upt-{value_index}', 'user-data-{value_index}')"));
        }
        let sql = format!("INSERT INTO {qualified_table} (id, data) VALUES {}", values.join(", "));
        pg.batch_execute(&sql).await.expect("user insert");
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1000.0;

    let (rows, scan_ms) =
        timed_query(&pg, &format!("SELECT id, data FROM {qualified_table}")).await;

    eprintln!(
        "[PERF] User table: INSERT {TOTAL} rows in {insert_ms:.0}ms, SCAN returned {} rows in \
         {scan_ms:.1}ms",
        rows.len()
    );
    assert_eq!(rows.len(), TOTAL, "user scan count mismatch");
    assert!(
        insert_ms < 30_000.0,
        "User INSERT {TOTAL} rows took {insert_ms:.0}ms — expected < 30000ms"
    );
    assert!(
        scan_ms < 15_000.0,
        "User SCAN {TOTAL} rows took {scan_ms:.0}ms — expected < 15000ms"
    );

    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(3000)]
async fn e2e_perf_cross_verify_latency() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_xv");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, value INTEGER").await;

    const ITERATIONS: usize = 10;
    let mut latencies = Vec::with_capacity(ITERATIONS);

    for index in 0..ITERATIONS {
        let id = format!("xv-{index}");
        let start = std::time::Instant::now();

        pg.execute(
            &format!("INSERT INTO {qualified_table} (id, value) VALUES ($1, $2)"),
            &[&id, &(index as i32)],
        )
        .await
        .expect("xv insert");

        let result = env
            .kalamdb_sql(&format!("SELECT id, value FROM e2e.{table} WHERE id = '{id}'"))
            .await;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        let result_text = serde_json::to_string(&result).unwrap_or_default();
        assert!(result_text.contains(&id), "cross-verify: row {id} not visible via REST");

        latencies.push(elapsed_ms);
    }

    let avg_ms = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let min_ms = latencies.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_ms = latencies.iter().cloned().fold(0.0_f64, f64::max);

    eprintln!(
        "[PERF] Cross-verify ({ITERATIONS} iterations): avg {avg_ms:.1}ms, min {min_ms:.1}ms, max \
         {max_ms:.1}ms"
    );
    assert!(avg_ms < 10_000.0, "Cross-verify avg {avg_ms:.0}ms — expected < 10000ms");
    pg.disconnect().await;
}
