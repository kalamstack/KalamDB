use serde_json::Value;

use super::common::{unique_name, TestEnv};

fn large_json_text_payload() -> String {
    let items = (0..4096).map(|index| format!("item-{index:04}")).collect::<Vec<_>>();
    serde_json::to_string(&items).expect("serialize large json payload")
}

fn probe_allocations(probe: &Value) -> &Value {
    probe.get("allocations").expect("allocations payload")
}

fn probe_counters(probe: &Value) -> &Value {
    probe.get("counters").expect("counters payload")
}

async fn run_probe(pg: &tokio_postgres::Client, sql: &str, value: &str) -> Value {
    let row = pg.query_one(sql, &[&value]).await.expect("run conversion probe");
    let payload: String = row.get(0);
    serde_json::from_str(&payload).expect("parse conversion probe payload")
}

#[tokio::test]
#[ntest::timeout(1500)]
async fn e2e_perf_text_to_pg_fast_path_has_zero_rust_heap_allocations() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let payload = format!("{}-{}", "alphabet".repeat(16 * 1024), unique_name("textprobe"));

    let probe = run_probe(&pg, "SELECT kalam_test_probe_text_to_pg($1)", &payload).await;
    let allocations = probe_allocations(&probe);
    let counters = probe_counters(&probe);

    assert_eq!(probe["matched"], Value::Bool(true));
    assert_eq!(counters["text_to_pg_fast_path_calls"], Value::from(1));
    assert_eq!(counters["text_to_pg_fast_path_bytes"], Value::from(payload.len() as u64));
    assert_eq!(allocations["allocations"], Value::from(0));
    assert_eq!(allocations["peak_live_bytes"], Value::from(0));

    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(1500)]
async fn e2e_perf_jsonb_to_pg_stays_within_bounded_rust_allocations() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let payload = large_json_text_payload();

    let probe = run_probe(&pg, "SELECT kalam_test_probe_jsonb_to_pg($1)", &payload).await;
    let allocations = probe_allocations(&probe);
    let counters = probe_counters(&probe);

    assert_eq!(probe["matched"], Value::Bool(true));
    assert_eq!(counters["jsonb_to_pg_input_calls"], Value::from(1));
    assert_eq!(counters["jsonb_to_pg_input_bytes"], Value::from(payload.len() as u64));
    assert!(
        allocations["allocations"].as_u64().unwrap_or(u64::MAX) <= 2,
        "jsonb write probe allocated too many Rust objects: {probe}"
    );
    assert!(
        allocations["peak_live_bytes"].as_u64().unwrap_or(u64::MAX)
            <= payload.len() as u64 + 16 * 1024,
        "jsonb write probe used too much transient Rust heap: {probe}"
    );

    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(1500)]
async fn e2e_perf_json_to_scalar_stays_within_bounded_rust_allocations() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let payload = large_json_text_payload();

    let probe = run_probe(&pg, "SELECT kalam_test_probe_json_to_scalar($1)", &payload).await;
    let allocations = probe_allocations(&probe);
    let counters = probe_counters(&probe);

    assert_eq!(probe["matched"], Value::Bool(true));
    assert_eq!(counters["json_from_pg_fast_path_calls"], Value::from(1));
    assert_eq!(counters["json_from_pg_fast_path_bytes"], Value::from(payload.len() as u64));
    assert!(
        allocations["allocations"].as_u64().unwrap_or(u64::MAX) <= 4,
        "json scalar probe allocated too many Rust objects: {probe}"
    );
    assert!(
        allocations["peak_live_bytes"].as_u64().unwrap_or(u64::MAX)
            <= (payload.len() as u64 * 2) + 16 * 1024,
        "json scalar probe used too much transient Rust heap: {probe}"
    );

    pg.disconnect().await;
}

#[tokio::test]
#[ntest::timeout(1500)]
async fn e2e_perf_jsonb_to_scalar_stays_within_bounded_rust_allocations() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let payload = large_json_text_payload();

    let probe = run_probe(&pg, "SELECT kalam_test_probe_jsonb_to_scalar($1)", &payload).await;
    let allocations = probe_allocations(&probe);
    let counters = probe_counters(&probe);

    assert_eq!(probe["matched"], Value::Bool(true));
    assert_eq!(counters["jsonb_from_pg_fast_path_calls"], Value::from(1));
    assert!(
        allocations["allocations"].as_u64().unwrap_or(u64::MAX) <= 4,
        "jsonb scalar probe allocated too many Rust objects: {probe}"
    );
    assert!(
        allocations["peak_live_bytes"].as_u64().unwrap_or(u64::MAX)
            <= (payload.len() as u64 * 2) + 16 * 1024,
        "jsonb scalar probe used too much transient Rust heap: {probe}"
    );

    pg.disconnect().await;
}
