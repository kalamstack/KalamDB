use super::common::{
    bulk_delete_all, count_rows, create_shared_foreign_table, kalamdb_pid, pg_backend_pid,
    process_group_rss_kb, process_rss_kb, sample_process_group_peak_rss_kb,
    sample_process_peak_rss_kb, timed_query, unique_name, TestEnv,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[tokio::test]
#[ntest::timeout(32000)]
async fn e2e_perf_local_memory_stays_bounded_under_batch_insert_and_scan() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("perf_mem");
    let qualified_table = format!("e2e.{table}");
    let pid = kalamdb_pid();
    let baseline_rss_kb = process_rss_kb(pid);

    create_shared_foreign_table(&pg, &table, "id TEXT, payload TEXT, value INTEGER").await;
    bulk_delete_all(&pg, &qualified_table, "id").await;

    const TOTAL: usize = 8_000;
    const BATCH: usize = 1_000;

    let stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::clone(&stop);
    let sampler =
        tokio::spawn(async move { sample_process_peak_rss_kb(pid, 100, sampler_stop).await });

    let start = std::time::Instant::now();
    for batch in 0..(TOTAL / BATCH) {
        let mut values = Vec::with_capacity(BATCH);
        for index in 0..BATCH {
            let value_index = batch * BATCH + index;
            values.push(format!("('mem-{value_index}', 'payload-{value_index}', {value_index})"));
        }
        let sql = format!(
            "INSERT INTO {qualified_table} (id, payload, value) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("memory test batch insert");
    }

    let (_rows, scan_ms) =
        timed_query(&pg, &format!("SELECT id, payload, value FROM {qualified_table}")).await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    bulk_delete_all(&pg, &qualified_table, "id").await;
    tokio::time::sleep(std::time::Duration::from_millis(750)).await;

    stop.store(true, Ordering::Relaxed);
    let peak_rss_kb = sampler.await.expect("join rss sampler");
    let final_rss_kb = process_rss_kb(pid);
    let peak_delta_kb = peak_rss_kb.saturating_sub(baseline_rss_kb);
    let final_delta_kb = final_rss_kb.saturating_sub(baseline_rss_kb);

    eprintln!(
        "[PERF] Memory stability {TOTAL} rows: total {elapsed_ms:.1}ms, scan {scan_ms:.1}ms, baseline {} KB, peak {} KB, final {} KB, peak delta {} KB, final delta {} KB",
        baseline_rss_kb,
        peak_rss_kb,
        final_rss_kb,
        peak_delta_kb,
        final_delta_kb
    );

    assert!(
        elapsed_ms < 20_000.0,
        "batch insert + scan workload took {elapsed_ms:.0}ms — expected < 20000ms"
    );
    assert!(scan_ms < 10_000.0, "memory test scan took {scan_ms:.0}ms — expected < 10000ms");
    assert!(
        peak_delta_kb < 256 * 1024,
        "KalamDB RSS peak grew by {} MB — expected < 256 MB",
        peak_delta_kb / 1024
    );
    assert!(
        final_delta_kb < 96 * 1024,
        "KalamDB RSS retained {} MB after cleanup — expected < 96 MB",
        final_delta_kb / 1024
    );
}

#[tokio::test]
#[ntest::timeout(70000)]
async fn e2e_perf_multi_session_pg_extension_memory_stays_bounded() {
    let env = TestEnv::global().await;
    let coordinator = env.pg_connect().await;
    let table = unique_name("perf_multi_session");
    let qualified_table = format!("e2e.{table}");

    create_shared_foreign_table(&coordinator, &table, "id TEXT, payload TEXT, worker_id INTEGER")
        .await;
    bulk_delete_all(&coordinator, &qualified_table, "id").await;

    const SESSIONS: usize = 6;
    const BATCHES_PER_SESSION: usize = 5;
    const ROWS_PER_BATCH: usize = 80;

    let mut clients = Vec::with_capacity(SESSIONS);
    let mut pids = Vec::with_capacity(SESSIONS);
    for _ in 0..SESSIONS {
        let client = Arc::new(env.pg_connect().await);
        pids.push(pg_backend_pid(client.as_ref()).await);
        clients.push(client);
    }

    let baseline_rss_kb = process_group_rss_kb(&pids);
    let stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::clone(&stop);
    let sampler_pids = pids.clone();
    let sampler = tokio::spawn(async move {
        sample_process_group_peak_rss_kb(sampler_pids, 100, sampler_stop).await
    });

    let workload_start = std::time::Instant::now();
    let mut handles = Vec::with_capacity(SESSIONS);
    for (worker, client) in clients.iter().enumerate() {
        let client = Arc::clone(client);
        let qualified_table = qualified_table.clone();
        handles.push(tokio::spawn(async move {
            for batch in 0..BATCHES_PER_SESSION {
                let mut values = Vec::with_capacity(ROWS_PER_BATCH);
                for item in 0..ROWS_PER_BATCH {
                    let index = batch * ROWS_PER_BATCH + item;
                    values.push(format!(
                        "('sess-{worker}-{index}', 'payload-{worker}-{index}', {worker})"
                    ));
                }

                let insert_sql = format!(
                    "INSERT INTO {qualified_table} (id, payload, worker_id) VALUES {}",
                    values.join(", ")
                );
                client.batch_execute(&insert_sql).await.expect("multi-session batch insert");

                let rows = client
                    .query(
                        &format!(
                            "SELECT id, payload FROM {qualified_table} WHERE worker_id = $1 ORDER BY id LIMIT 5"
                        ),
                        &[&(worker as i32)],
                    )
                    .await
                    .expect("multi-session point read");
                assert!(!rows.is_empty(), "session {worker} should be able to read its inserted rows");

                let last_index = ((batch + 1) * ROWS_PER_BATCH) - 1;
                client
                    .execute(
                        &format!("UPDATE {qualified_table} SET payload = payload || '-u' WHERE id = $1"),
                        &[&format!("sess-{worker}-{last_index}")],
                    )
                    .await
                    .expect("multi-session update");

                let count_row = client
                    .query_one(
                        &format!("SELECT COUNT(*) FROM {qualified_table} WHERE worker_id = $1"),
                        &[&(worker as i32)],
                    )
                    .await
                    .expect("multi-session count");
                let count: i64 = count_row.get(0);
                assert!(count > 0, "session {worker} should observe persisted rows");
            }

            let ping = client.query_one("SELECT 1", &[]).await.expect("post-workload ping");
            let value: i32 = ping.get(0);
            assert_eq!(value, 1);
        }));
    }

    for handle in handles {
        handle.await.expect("join multi-session workload");
    }

    let elapsed_ms = workload_start.elapsed().as_secs_f64() * 1000.0;
    let total_expected = (SESSIONS * BATCHES_PER_SESSION * ROWS_PER_BATCH) as i64;
    let total_count = count_rows(&coordinator, &qualified_table, None).await;
    assert_eq!(
        total_count, total_expected,
        "multi-session workload should persist all inserted rows"
    );

    for client in &clients {
        let row = client
            .query_one("SELECT 1", &[])
            .await
            .expect("backend should remain alive after workload");
        let value: i32 = row.get(0);
        assert_eq!(value, 1);
    }

    bulk_delete_all(&coordinator, &qualified_table, "id").await;
    tokio::time::sleep(std::time::Duration::from_millis(750)).await;

    stop.store(true, Ordering::Relaxed);
    let peak_rss_kb = sampler.await.expect("join pg backend rss sampler");
    let final_rss_kb = process_group_rss_kb(&pids);
    let peak_delta_kb = peak_rss_kb.saturating_sub(baseline_rss_kb);
    let final_delta_kb = final_rss_kb.saturating_sub(baseline_rss_kb);

    eprintln!(
        "[PERF] Multi-session pg_kalam workload: sessions {}, rows {}, total {:.1}ms, baseline {} KB, peak {} KB, final {} KB, peak delta {} KB, final delta {} KB",
        SESSIONS,
        total_expected,
        elapsed_ms,
        baseline_rss_kb,
        peak_rss_kb,
        final_rss_kb,
        peak_delta_kb,
        final_delta_kb
    );

    assert!(
        elapsed_ms < 30_000.0,
        "multi-session workload took {elapsed_ms:.0}ms — expected < 30000ms"
    );
    assert!(
        peak_delta_kb < 192 * 1024,
        "PostgreSQL backend RSS peak grew by {} MB — expected < 192 MB",
        peak_delta_kb / 1024
    );
    assert!(
        final_delta_kb < 96 * 1024,
        "PostgreSQL backend RSS retained {} MB after workload — expected < 96 MB",
        final_delta_kb / 1024
    );
}
