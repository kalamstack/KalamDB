use crate::common::*;

use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_ROWS: usize = 4_000;
const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_QUERY_LOOPS: usize = 8;
const DEFAULT_PEAK_DELTA_MB: u64 = 96;
const DEFAULT_RECOVERY_DELTA_MB: u64 = 40;
const DEFAULT_SETTLE_SECONDS: u64 = 10;
const DEFAULT_BASELINE_SAMPLES: usize = 3;
const DEFAULT_SAMPLE_DELAY_MS: u64 = 350;
const DEFAULT_WARMUP_SETTLE_SECONDS: u64 = 6;

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(fallback)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(fallback)
}

fn env_bool(name: &str, fallback: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(fallback)
}

fn log_sample(label: &str, sample: &ServerMemorySample) {
    println!(
        "[memory] {:<18} asserted={}MB reported={}MB rss={} pid={}",
        label,
        sample.asserted_mb(),
        sample.reported_mb,
        sample
            .rss_mb
            .map(|rss| format!("{}MB", rss))
            .unwrap_or_else(|| "n/a".to_string()),
        sample.pid.map(|pid| pid.to_string()).unwrap_or_else(|| "n/a".to_string())
    );
}

fn min_sample(samples: &[ServerMemorySample]) -> ServerMemorySample {
    samples
        .iter()
        .min_by_key(|sample| sample.asserted_mb())
        .cloned()
        .expect("expected at least one memory sample")
}

fn capture_baseline(samples: usize, delay: Duration) -> ServerMemorySample {
    let mut captured = Vec::with_capacity(samples);
    for idx in 0..samples {
        let sample = capture_server_memory_sample().expect("capture baseline memory sample");
        log_sample(&format!("baseline-{}", idx + 1), &sample);
        captured.push(sample);
        if idx + 1 < samples {
            thread::sleep(delay);
        }
    }
    min_sample(&captured)
}

fn capture_recovered_sample(settle_seconds: u64) -> ServerMemorySample {
    let mut captured = Vec::with_capacity(settle_seconds as usize + 1);
    let initial = capture_server_memory_sample().expect("capture initial recovered memory sample");
    log_sample("settle-0", &initial);
    captured.push(initial);

    for second in 1..=settle_seconds {
        thread::sleep(Duration::from_secs(1));
        let sample = capture_server_memory_sample().expect("capture recovered memory sample");
        log_sample(&format!("settle-{}", second), &sample);
        captured.push(sample);
    }

    min_sample(&captured)
}

fn build_payload(row_id: usize, payload_bytes: usize) -> String {
    let prefix = format!("row_{:06}_", row_id);
    let padding_len = payload_bytes.saturating_sub(prefix.len()).max(16);
    let padding = "x".repeat(padding_len);
    format!("{}{}", prefix, padding)
}

fn create_memory_test_table(namespace: &str, table: &str) -> String {
    let full_table = format!("{}.{}", namespace, table);
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, tenant TEXT NOT NULL, payload TEXT NOT NULL, seq BIGINT NOT NULL) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:500')",
        full_table
    ))
    .expect("create table should succeed");
    full_table
}

fn flush_and_drop_namespace(namespace: &str, full_table: &str) {
    let flush_json =
        execute_sql_as_root_via_client_json(&format!("STORAGE FLUSH TABLE {}", full_table))
            .expect("flush table should succeed");
    let job_id = parse_job_id_from_json_message(&flush_json)
        .or_else(|_| parse_job_id_from_flush_output(&flush_json))
        .expect("flush response should contain a job id");
    verify_job_completed(&job_id, Duration::from_secs(45)).expect("flush job should complete");

    execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace))
        .expect("drop namespace should succeed");
}

fn run_memory_workload(
    full_table: &str,
    total_rows: usize,
    batch_size: usize,
    payload_bytes: usize,
    query_loops: usize,
    peak_sample: &mut ServerMemorySample,
) -> (Duration, Duration) {
    let workload_start = Instant::now();
    let mut inserted = 0usize;

    while inserted < total_rows {
        let batch_rows = (total_rows - inserted).min(batch_size);
        let mut values = String::new();
        for offset in 0..batch_rows {
            let row_id = inserted + offset + 1;
            let tenant = format!("tenant_{:02}", row_id % 16);
            let payload = build_payload(row_id, payload_bytes);
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!("({}, '{}', '{}', {})", row_id, tenant, payload, row_id));
        }

        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, tenant, payload, seq) VALUES {}",
            full_table, values
        ))
        .expect("batch insert should succeed");

        inserted += batch_rows;
        let sample = capture_server_memory_sample().expect("capture insert memory sample");
        log_sample(&format!("after-insert-{}", inserted), &sample);
        if sample.asserted_mb() > peak_sample.asserted_mb() {
            *peak_sample = sample;
        }
    }

    let insert_elapsed = workload_start.elapsed();
    let rows_per_sec = total_rows as f64 / insert_elapsed.as_secs_f64().max(1e-6);
    println!(
        "[perf] inserted {} rows in {:.2}s ({:.1} rows/s)",
        total_rows,
        insert_elapsed.as_secs_f64(),
        rows_per_sec
    );

    let read_start = Instant::now();
    for loop_idx in 0..query_loops {
        execute_sql_as_root_via_client(&format!(
            "SELECT COUNT(*) AS cnt FROM {} WHERE tenant = 'tenant_{:02}'",
            full_table,
            loop_idx % 16
        ))
        .expect("count query should succeed");
        execute_sql_as_root_via_client(&format!(
            "SELECT id, tenant, seq FROM {} WHERE tenant = 'tenant_{:02}' ORDER BY id LIMIT 50",
            full_table,
            loop_idx % 16
        ))
        .expect("scan query should succeed");

        let sample = capture_server_memory_sample().expect("capture query memory sample");
        log_sample(&format!("after-query-{}", loop_idx + 1), &sample);
        if sample.asserted_mb() > peak_sample.asserted_mb() {
            *peak_sample = sample;
        }
    }

    let read_elapsed = read_start.elapsed();
    println!(
        "[perf] completed {} read loops in {:.2}s",
        query_loops,
        read_elapsed.as_secs_f64()
    );

    (insert_elapsed, read_elapsed)
}

#[ignore = "Measures shared server memory; run explicitly against an otherwise idle live server"]
#[ntest::timeout(50000)]
#[test]
fn smoke_test_server_memory_regression() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_test_server_memory_regression: server not running at {}",
            server_url()
        );
        return;
    }

    let total_rows = env_usize("KALAMDB_MEM_TEST_ROWS", DEFAULT_ROWS);
    let batch_size = env_usize("KALAMDB_MEM_TEST_BATCH", DEFAULT_BATCH_SIZE).max(1);
    let payload_bytes = env_usize("KALAMDB_MEM_TEST_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES).max(32);
    let query_loops = env_usize("KALAMDB_MEM_TEST_QUERY_LOOPS", DEFAULT_QUERY_LOOPS).max(1);
    let peak_budget_mb = env_u64("KALAMDB_MEM_TEST_PEAK_DELTA_MB", DEFAULT_PEAK_DELTA_MB);
    let recovery_budget_mb =
        env_u64("KALAMDB_MEM_TEST_RECOVERY_DELTA_MB", DEFAULT_RECOVERY_DELTA_MB);
    let settle_seconds = env_u64("KALAMDB_MEM_TEST_SETTLE_SECS", DEFAULT_SETTLE_SECONDS).max(1);
    let baseline_samples =
        env_usize("KALAMDB_MEM_TEST_BASELINE_SAMPLES", DEFAULT_BASELINE_SAMPLES).max(1);
    let enable_warmup = env_bool("KALAMDB_MEM_TEST_WARMUP", true);
    let warmup_rows = env_usize("KALAMDB_MEM_TEST_WARMUP_ROWS", total_rows);
    let warmup_query_loops = env_usize("KALAMDB_MEM_TEST_WARMUP_QUERY_LOOPS", query_loops).max(1);
    let warmup_settle_seconds =
        env_u64("KALAMDB_MEM_TEST_WARMUP_SETTLE_SECS", DEFAULT_WARMUP_SETTLE_SECONDS).max(1);
    let sample_delay =
        Duration::from_millis(env_u64("KALAMDB_MEM_TEST_SAMPLE_DELAY_MS", DEFAULT_SAMPLE_DELAY_MS));

    println!(
        "[config] rows={} batch={} payload_bytes={} query_loops={} peak_budget={}MB recovery_budget={}MB settle={}s warmup={} warmup_rows={} warmup_query_loops={} warmup_settle={}s",
        total_rows,
        batch_size,
        payload_bytes,
        query_loops,
        peak_budget_mb,
        recovery_budget_mb,
        settle_seconds,
        enable_warmup,
        warmup_rows,
        warmup_query_loops,
        warmup_settle_seconds
    );

    if enable_warmup && warmup_rows > 0 {
        println!(
            "[warmup] priming server memory and query paths before measuring steady-state recovery"
        );
        let warmup_namespace = generate_unique_namespace("perf_mem_warm_ns");
        let warmup_table = generate_unique_table("perf_mem_warm_table");
        let warmup_full_table = create_memory_test_table(&warmup_namespace, &warmup_table);
        let mut warmup_peak =
            capture_server_memory_sample().expect("capture warmup baseline sample");

        let _ = run_memory_workload(
            &warmup_full_table,
            warmup_rows,
            batch_size,
            payload_bytes,
            warmup_query_loops,
            &mut warmup_peak,
        );
        flush_and_drop_namespace(&warmup_namespace, &warmup_full_table);

        let warmup_recovered = capture_recovered_sample(warmup_settle_seconds);
        println!(
            "[warmup] peak={}MB recovered={}MB after {}s settle",
            warmup_peak.asserted_mb(),
            warmup_recovered.asserted_mb(),
            warmup_settle_seconds
        );
    }

    let namespace = generate_unique_namespace("perf_mem_ns");
    let table = generate_unique_table("perf_mem_table");
    let full_table = create_memory_test_table(&namespace, &table);

    let baseline = capture_baseline(baseline_samples, sample_delay);
    log_sample("baseline", &baseline);
    if baseline.rss_mb.is_none() {
        println!(
            "[memory] external RSS unavailable for {}; relying on system.stats memory_usage_mb",
            server_url()
        );
    }

    let mut peak_sample = baseline.clone();
    let (insert_elapsed, read_elapsed) = run_memory_workload(
        &full_table,
        total_rows,
        batch_size,
        payload_bytes,
        query_loops,
        &mut peak_sample,
    );
    flush_and_drop_namespace(&namespace, &full_table);

    let settled = capture_recovered_sample(settle_seconds);

    let baseline_mb = baseline.asserted_mb();
    let peak_mb = peak_sample.asserted_mb();
    let settled_mb = settled.asserted_mb();
    let peak_delta_mb = peak_mb.saturating_sub(baseline_mb);
    let recovery_delta_mb = settled_mb.saturating_sub(baseline_mb);

    println!(
        "[summary] insert_time={:.2}s read_time={:.2}s baseline={}MB peak={}MB recovered={}MB peak_delta={}MB recovery_delta={}MB",
        insert_elapsed.as_secs_f64(),
        read_elapsed.as_secs_f64(),
        baseline_mb,
        peak_mb,
        settled_mb,
        peak_delta_mb,
        recovery_delta_mb
    );

    assert!(
        peak_delta_mb <= peak_budget_mb,
        "server memory spiked too far above baseline: baseline={}MB peak={}MB delta={}MB budget={}MB",
        baseline_mb,
        peak_mb,
        peak_delta_mb,
        peak_budget_mb
    );
    assert!(
        recovery_delta_mb <= recovery_budget_mb,
        "server memory did not recover near baseline after flush + cleanup: baseline={}MB recovered={}MB delta={}MB budget={}MB",
        baseline_mb,
        settled_mb,
        recovery_delta_mb,
        recovery_budget_mb
    );
}
