use super::common::{count_rows, create_shared_kalam_table, unique_name, TestEnv};
use serde_json::Value;
use std::future::Future;
use std::time::Instant;

const BENCH_ITERATIONS_PER_RUN: usize = 16;
const BENCH_WARMUP_RUNS: usize = 1;
const BENCH_MEASURED_RUNS: usize = 3;
const SELECT_BREAKDOWN_SAMPLES: usize = 8;
const SMALL_ROW_BYTES: usize = 64;
const ONE_KB_ROW_BYTES: usize = 1024;
const TEN_KB_ROW_BYTES: usize = 10 * 1024;
const BENCH_COLUMNS: &str = "id TEXT, payload TEXT, status TEXT, version INTEGER";

struct BenchStats {
    run_totals_ms: Vec<f64>,
    median_total_ms: f64,
    median_avg_ms: f64,
    min_total_ms: f64,
    max_total_ms: f64,
}

struct SelectBreakdownStats {
    pg_id_only_query_median_ms: f64,
    pg_full_row_query_median_ms: f64,
    pg_decode_median_ms: f64,
    api_id_only_http_median_ms: f64,
    api_id_only_parse_median_ms: f64,
    api_full_row_http_median_ms: f64,
    api_full_row_parse_median_ms: f64,
}

impl BenchStats {
    fn from_run_totals(run_totals_ms: Vec<f64>) -> Self {
        assert!(
            !run_totals_ms.is_empty(),
            "bench stats require at least one measured run"
        );

        let median_total_ms = median_ms(&run_totals_ms);
        let median_avg_ms = median_total_ms / BENCH_ITERATIONS_PER_RUN as f64;
        let min_total_ms = run_totals_ms
            .iter()
            .copied()
            .min_by(f64::total_cmp)
            .expect("bench run totals should not be empty");
        let max_total_ms = run_totals_ms
            .iter()
            .copied()
            .max_by(f64::total_cmp)
            .expect("bench run totals should not be empty");

        Self {
            run_totals_ms,
            median_total_ms,
            median_avg_ms,
            min_total_ms,
            max_total_ms,
        }
    }
}

fn total_bench_runs() -> usize {
    BENCH_WARMUP_RUNS + BENCH_MEASURED_RUNS
}

fn bench_seed(run_index: usize, iteration: usize) -> usize {
    run_index * BENCH_ITERATIONS_PER_RUN + iteration
}

fn bench_row_id(prefix: &str, run_index: usize, iteration: usize) -> String {
    format!("{prefix}-r{run_index}-i{iteration}")
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1000.0
}

fn median_ms(samples: &[f64]) -> f64 {
    assert!(!samples.is_empty(), "median requires at least one sample");

    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;

    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn format_run_totals(run_totals_ms: &[f64]) -> String {
    run_totals_ms
        .iter()
        .map(|value| format!("{value:.1}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sql_row_column_string(result: &Value, column_index: usize) -> Option<String> {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|entry| entry["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.get(column_index))
        .and_then(|value| value.as_str().map(ToString::to_string))
}

fn benchmark_payload(bytes: usize, seed: usize) -> String {
    let base = format!("payload-{seed:04}-");
    let repeated = base.repeat(bytes.div_ceil(base.len()));
    repeated[..bytes].to_string()
}

fn log_benchmark(label: &str, payload_bytes: usize, stats: &BenchStats) {
    eprintln!(
        "[PERF] {label}: payload={}B warmup_runs={} measured_runs={} iterations/run={} run_totals_ms=[{}] median_total={:.1}ms median_avg={:.2}ms/op range={:.1}..{:.1}ms",
        payload_bytes,
        BENCH_WARMUP_RUNS,
        BENCH_MEASURED_RUNS,
        BENCH_ITERATIONS_PER_RUN,
        format_run_totals(&stats.run_totals_ms),
        stats.median_total_ms,
        stats.median_avg_ms,
        stats.min_total_ms,
        stats.max_total_ms,
    );
}

fn log_select_breakdown(label: &str, breakdown: &SelectBreakdownStats) {
    let inferred_pg_payload_fetch_ms =
        breakdown.pg_full_row_query_median_ms - breakdown.pg_id_only_query_median_ms;
    let inferred_api_payload_http_ms =
        breakdown.api_full_row_http_median_ms - breakdown.api_id_only_http_median_ms;
    let inferred_pg_over_http_ms =
        breakdown.pg_full_row_query_median_ms - breakdown.api_full_row_http_median_ms;

    eprintln!(
        "[PERF] {label} breakdown: samples={} pg_id_only_query_median={:.2}ms pg_full_row_query_median={:.2}ms pg_decode_median={:.2}ms api_id_only_http_median={:.2}ms api_id_only_parse_median={:.2}ms api_full_row_http_median={:.2}ms api_full_row_parse_median={:.2}ms inferred_pg_payload_fetch={:.2}ms inferred_api_payload_http={:.2}ms inferred_pg_over_http={:.2}ms",
        SELECT_BREAKDOWN_SAMPLES,
        breakdown.pg_id_only_query_median_ms,
        breakdown.pg_full_row_query_median_ms,
        breakdown.pg_decode_median_ms,
        breakdown.api_id_only_http_median_ms,
        breakdown.api_id_only_parse_median_ms,
        breakdown.api_full_row_http_median_ms,
        breakdown.api_full_row_parse_median_ms,
        inferred_pg_payload_fetch_ms,
        inferred_api_payload_http_ms,
        inferred_pg_over_http_ms,
    );
}

fn assert_benchmark(label: &str, stats: &BenchStats, max_total_ms: f64, max_avg_ms: f64) {
    assert!(
        stats.median_total_ms < max_total_ms,
        "{label} total {:.1}ms exceeded {:.1}ms",
        stats.median_total_ms,
        max_total_ms
    );
    assert!(
        stats.median_avg_ms < max_avg_ms,
        "{label} avg {:.2}ms/op exceeded {:.2}ms/op",
        stats.median_avg_ms,
        max_avg_ms
    );
}

async fn run_benchmark_rounds<F, Fut>(mut operation: F) -> BenchStats
where
    F: FnMut(usize, usize) -> Fut,
    Fut: Future<Output = ()>,
{
    for run_index in 0..BENCH_WARMUP_RUNS {
        for iteration in 0..BENCH_ITERATIONS_PER_RUN {
            operation(run_index, iteration).await;
        }
    }

    let mut run_totals_ms = Vec::with_capacity(BENCH_MEASURED_RUNS);
    for run_index in BENCH_WARMUP_RUNS..total_bench_runs() {
        let started_at = Instant::now();
        for iteration in 0..BENCH_ITERATIONS_PER_RUN {
            operation(run_index, iteration).await;
        }
        run_totals_ms.push(elapsed_ms(started_at));
    }

    BenchStats::from_run_totals(run_totals_ms)
}

async fn seed_rows(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    row_prefix: &str,
    payload_bytes: usize,
) {
    let insert_stmt = client
        .prepare(&format!(
            "INSERT INTO {qualified_table} (id, payload, status, version) VALUES ($1, $2, $3, $4)"
        ))
        .await
        .expect("prepare seed insert statement");

    for run_index in 0..total_bench_runs() {
        for iteration in 0..BENCH_ITERATIONS_PER_RUN {
            let id = bench_row_id(row_prefix, run_index, iteration);
            let payload = benchmark_payload(payload_bytes, bench_seed(run_index, iteration));
            client
                .execute(&insert_stmt, &[&id, &payload, &"seeded", &(iteration as i32)])
                .await
                .expect("seed benchmark row");
        }
    }
}

async fn measure_select_breakdown(
    env: &TestEnv,
    pg: &tokio_postgres::Client,
    qualified_table: &str,
    row_id: &str,
    payload_bytes: usize,
) -> SelectBreakdownStats {
    let id_only_stmt = pg
        .prepare(&format!("SELECT id FROM {qualified_table} WHERE id = $1"))
        .await
        .expect("prepare id-only select statement");
    let full_row_stmt = pg
        .prepare(&format!(
            "SELECT id, payload, status, version FROM {qualified_table} WHERE id = $1"
        ))
        .await
        .expect("prepare full-row select statement");
    let id_only_sql = format!("SELECT id FROM {qualified_table} WHERE id = '{row_id}'");
    let full_row_sql = format!(
        "SELECT id, payload, status, version FROM {qualified_table} WHERE id = '{row_id}'"
    );

    let mut pg_id_only_query_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);
    let mut pg_full_row_query_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);
    let mut pg_decode_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);
    let mut api_id_only_http_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);
    let mut api_id_only_parse_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);
    let mut api_full_row_http_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);
    let mut api_full_row_parse_ms = Vec::with_capacity(SELECT_BREAKDOWN_SAMPLES);

    for _ in 0..SELECT_BREAKDOWN_SAMPLES {
        let started_at = Instant::now();
        let row = pg
            .query_one(&id_only_stmt, &[&row_id])
            .await
            .expect("select id-only benchmark row");
        pg_id_only_query_ms.push(elapsed_ms(started_at));
        let selected_id: String = row.get(0);
        assert_eq!(selected_id, row_id, "pg id-only row id mismatch");

        let started_at = Instant::now();
        let row = pg
            .query_one(&full_row_stmt, &[&row_id])
            .await
            .expect("select full benchmark row");
        pg_full_row_query_ms.push(elapsed_ms(started_at));

        let started_at = Instant::now();
        let selected_id: String = row.get(0);
        let payload: String = row.get(1);
        let status: String = row.get(2);
        let version: i32 = row.get(3);
        pg_decode_ms.push(elapsed_ms(started_at));
        assert_eq!(selected_id, row_id, "pg full-row id mismatch");
        assert_eq!(payload.len(), payload_bytes, "pg full-row payload size mismatch");
        assert_eq!(status, "seeded", "pg full-row status mismatch");
        assert_eq!(version, 0, "pg full-row version mismatch");

        let started_at = Instant::now();
        let response_text = env.kalamdb_sql_text(&id_only_sql).await;
        api_id_only_http_ms.push(elapsed_ms(started_at));
        let started_at = Instant::now();
        let response_value: Value =
            serde_json::from_str(&response_text).expect("parse KalamDB id-only SQL response");
        api_id_only_parse_ms.push(elapsed_ms(started_at));
        let selected_id = sql_row_column_string(&response_value, 0)
            .expect("KalamDB id-only response should include row id");
        assert_eq!(selected_id, row_id, "KalamDB id-only row id mismatch");

        let started_at = Instant::now();
        let response_text = env.kalamdb_sql_text(&full_row_sql).await;
        api_full_row_http_ms.push(elapsed_ms(started_at));
        let started_at = Instant::now();
        let response_value: Value =
            serde_json::from_str(&response_text).expect("parse KalamDB full-row SQL response");
        api_full_row_parse_ms.push(elapsed_ms(started_at));
        let selected_id = sql_row_column_string(&response_value, 0)
            .expect("KalamDB full-row response should include row id");
        let payload = sql_row_column_string(&response_value, 1)
            .expect("KalamDB full-row response should include payload");
        let status = sql_row_column_string(&response_value, 2)
            .expect("KalamDB full-row response should include status");
        assert_eq!(selected_id, row_id, "KalamDB full-row id mismatch");
        assert_eq!(payload.len(), payload_bytes, "KalamDB full-row payload size mismatch");
        assert_eq!(status, "seeded", "KalamDB full-row status mismatch");
    }

    SelectBreakdownStats {
        pg_id_only_query_median_ms: median_ms(&pg_id_only_query_ms),
        pg_full_row_query_median_ms: median_ms(&pg_full_row_query_ms),
        pg_decode_median_ms: median_ms(&pg_decode_ms),
        api_id_only_http_median_ms: median_ms(&api_id_only_http_ms),
        api_id_only_parse_median_ms: median_ms(&api_id_only_parse_ms),
        api_full_row_http_median_ms: median_ms(&api_full_row_http_ms),
        api_full_row_parse_median_ms: median_ms(&api_full_row_parse_ms),
    }
}

async fn benchmark_insert(label: &str, payload_bytes: usize) -> BenchStats {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let client: &tokio_postgres::Client = &pg;
    let table = unique_name("perf_pglite_insert");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, BENCH_COLUMNS).await;

    let insert_stmt = pg
        .prepare(&format!(
            "INSERT INTO {qualified_table} (id, payload, status, version) VALUES ($1, $2, $3, $4)"
        ))
        .await
        .expect("prepare insert statement");

    let stats = run_benchmark_rounds(|run_index, iteration| {
        let id = bench_row_id("insert", run_index, iteration);
        let payload = benchmark_payload(payload_bytes, bench_seed(run_index, iteration));
        let insert_stmt = insert_stmt.clone();

        async move {
            client
                .execute(&insert_stmt, &[&id, &payload, &"inserted", &(iteration as i32)])
                .await
                .expect("insert benchmark row");
        }
    })
    .await;

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(
        count,
        (BENCH_ITERATIONS_PER_RUN * total_bench_runs()) as i64,
        "insert benchmark row count mismatch"
    );

    log_benchmark(label, payload_bytes, &stats);
    pg.disconnect().await;
    stats
}

async fn benchmark_select(label: &str, payload_bytes: usize) -> BenchStats {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let client: &tokio_postgres::Client = &pg;
    let table = unique_name("perf_pglite_select");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, BENCH_COLUMNS).await;
    seed_rows(&pg, &qualified_table, "select", payload_bytes).await;

    let select_stmt = pg
        .prepare(&format!(
            "SELECT id, payload, status, version FROM {qualified_table} WHERE id = $1"
        ))
        .await
        .expect("prepare select statement");

    let stats = run_benchmark_rounds(|run_index, iteration| {
        let id = bench_row_id("select", run_index, iteration);
        let select_stmt = select_stmt.clone();

        async move {
            let row = client
                .query_one(&select_stmt, &[&id])
                .await
                .expect("select benchmark row");
            let selected_id: String = row.get(0);
            let payload: String = row.get(1);
            assert_eq!(selected_id, id, "selected row id mismatch");
            assert_eq!(payload.len(), payload_bytes, "selected payload size mismatch");
        }
    })
    .await;

    log_benchmark(label, payload_bytes, &stats);

    if payload_bytes == TEN_KB_ROW_BYTES {
        let breakdown = measure_select_breakdown(
            env,
            &pg,
            &qualified_table,
            &bench_row_id("select", BENCH_WARMUP_RUNS, 0),
            payload_bytes,
        )
        .await;
        log_select_breakdown(label, &breakdown);
    }

    pg.disconnect().await;
    stats
}

async fn benchmark_update(label: &str, payload_bytes: usize) -> BenchStats {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let client: &tokio_postgres::Client = &pg;
    let table = unique_name("perf_pglite_update");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, BENCH_COLUMNS).await;
    seed_rows(&pg, &qualified_table, "update", payload_bytes).await;

    let update_stmt = pg
        .prepare(&format!(
            "UPDATE {qualified_table} SET payload = $1, status = $2, version = $3 WHERE id = $4"
        ))
        .await
        .expect("prepare update statement");

    let stats = run_benchmark_rounds(|run_index, iteration| {
        let id = bench_row_id("update", run_index, iteration);
        let next_payload = benchmark_payload(payload_bytes, bench_seed(run_index, iteration) + 10_000);
        let update_stmt = update_stmt.clone();

        async move {
            let rows = client
                .execute(&update_stmt, &[&next_payload, &"updated", &((iteration + 1) as i32), &id])
                .await
                .expect("update benchmark row");
            assert_eq!(rows, 1, "update should affect exactly one row");
        }
    })
    .await;

    let row = pg
        .query_one(
            &format!(
                "SELECT payload, status, version FROM {qualified_table} WHERE id = '{}'",
                bench_row_id("update", BENCH_WARMUP_RUNS, 0)
            ),
            &[],
        )
        .await
        .expect("select updated benchmark row");
    let payload: String = row.get(0);
    let status: String = row.get(1);
    let version: i32 = row.get(2);
    assert_eq!(payload.len(), payload_bytes, "updated payload size mismatch");
    assert_eq!(status, "updated", "updated status mismatch");
    assert_eq!(version, 1, "updated version mismatch");

    log_benchmark(label, payload_bytes, &stats);
    pg.disconnect().await;
    stats
}

async fn benchmark_delete(label: &str, payload_bytes: usize) -> BenchStats {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let client: &tokio_postgres::Client = &pg;
    let table = unique_name("perf_pglite_delete");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, BENCH_COLUMNS).await;
    seed_rows(&pg, &qualified_table, "delete", payload_bytes).await;

    let delete_stmt = pg
        .prepare(&format!("DELETE FROM {qualified_table} WHERE id = $1"))
        .await
        .expect("prepare delete statement");

    let stats = run_benchmark_rounds(|run_index, iteration| {
        let id = bench_row_id("delete", run_index, iteration);
        let delete_stmt = delete_stmt.clone();

        async move {
            let rows = client
                .execute(&delete_stmt, &[&id])
                .await
                .expect("delete benchmark row");
            assert_eq!(rows, 1, "delete should affect exactly one row");
        }
    })
    .await;

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, 0, "delete benchmark table should be empty");

    log_benchmark(label, payload_bytes, &stats);
    pg.disconnect().await;
    stats
}

#[tokio::test]
#[ntest::timeout(3100)]
async fn e2e_perf_pglite_insert_small_row() {
    let stats = benchmark_insert("pglite-style insert small row", SMALL_ROW_BYTES).await;
    assert_benchmark("insert small row", &stats, 42.0, 2.60);
}

#[tokio::test]
#[ntest::timeout(3400)]
async fn e2e_perf_pglite_select_small_row() {
    let stats = benchmark_select("pglite-style select small row", SMALL_ROW_BYTES).await;
    assert_benchmark("select small row", &stats, 70.0, 4.40);
}

#[tokio::test]
#[ntest::timeout(3800)]
async fn e2e_perf_pglite_update_small_row() {
    let stats = benchmark_update("pglite-style update small row", SMALL_ROW_BYTES).await;
    assert_benchmark("update small row", &stats, 110.0, 6.90);
}

#[tokio::test]
#[ntest::timeout(3900)]
async fn e2e_perf_pglite_delete_small_row() {
    let stats = benchmark_delete("pglite-style delete small row", SMALL_ROW_BYTES).await;
    assert_benchmark("delete small row", &stats, 105.0, 6.60);
}

#[tokio::test]
#[ntest::timeout(3100)]
async fn e2e_perf_pglite_insert_1kb_row() {
    let stats = benchmark_insert("pglite-style insert 1kb row", ONE_KB_ROW_BYTES).await;
    assert_benchmark("insert 1kb row", &stats, 48.0, 3.00);
}

#[tokio::test]
#[ntest::timeout(3900)]
async fn e2e_perf_pglite_select_1kb_row() {
    let stats = benchmark_select("pglite-style select 1kb row", ONE_KB_ROW_BYTES).await;
    assert_benchmark("select 1kb row", &stats, 72.0, 4.50);
}

#[tokio::test]
#[ntest::timeout(4000)]
async fn e2e_perf_pglite_update_1kb_row() {
    let stats = benchmark_update("pglite-style update 1kb row", ONE_KB_ROW_BYTES).await;
    assert_benchmark("update 1kb row", &stats, 125.0, 7.85);
}

#[tokio::test]
#[ntest::timeout(4100)]
async fn e2e_perf_pglite_delete_1kb_row() {
    let stats = benchmark_delete("pglite-style delete 1kb row", ONE_KB_ROW_BYTES).await;
    assert_benchmark("delete 1kb row", &stats, 108.0, 6.75);
}

#[tokio::test]
#[ntest::timeout(3500)]
async fn e2e_perf_pglite_insert_10kb_row() {
    let stats = benchmark_insert("pglite-style insert 10kb row", TEN_KB_ROW_BYTES).await;
    assert_benchmark("insert 10kb row", &stats, 112.0, 7.00);
}

#[tokio::test]
#[ntest::timeout(4300)]
async fn e2e_perf_pglite_select_10kb_row() {
    let stats = benchmark_select("pglite-style select 10kb row", TEN_KB_ROW_BYTES).await;
    assert_benchmark("select 10kb row", &stats, 75.0, 4.70);
}

#[tokio::test]
#[ntest::timeout(4600)]
async fn e2e_perf_pglite_update_10kb_row() {
    let stats = benchmark_update("pglite-style update 10kb row", TEN_KB_ROW_BYTES).await;
    assert_benchmark("update 10kb row", &stats, 190.0, 11.90);
}

#[tokio::test]
#[ntest::timeout(4300)]
async fn e2e_perf_pglite_delete_10kb_row() {
    let stats = benchmark_delete("pglite-style delete 10kb row", TEN_KB_ROW_BYTES).await;
    assert_benchmark("delete 10kb row", &stats, 128.0, 8.00);
}
