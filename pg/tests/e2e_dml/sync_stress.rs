use super::common::{count_rows, create_shared_kalam_table, unique_name, TestEnv};
use futures_util::future::join_all;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

type SqlRow = BTreeMap<String, Value>;

fn sql_result_rows(result: &Value) -> Vec<SqlRow> {
    let Some(result_entry) = result["results"].as_array().and_then(|results| results.first())
    else {
        return Vec::new();
    };

    let columns = result_entry["schema"]
        .as_array()
        .map(|schema| {
            schema
                .iter()
                .filter_map(|column| column["name"].as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    result_entry["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.as_array())
                .map(|row| {
                    columns.iter().cloned().zip(row.iter().cloned()).collect::<BTreeMap<_, _>>()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn find_row_by_id<'a>(rows: &'a [SqlRow], id: &str) -> &'a SqlRow {
    rows.iter()
        .find(|row| row.get("id").and_then(Value::as_str) == Some(id))
        .unwrap_or_else(|| panic!("missing API row for id {id}; rows={rows:?}"))
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|raw| i64::try_from(raw).ok()))
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|raw| raw as f64))
        .or_else(|| value.as_u64().map(|raw| raw as f64))
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<f64>().ok()))
}

fn value_as_bool(value: &Value) -> Option<bool> {
    value.as_bool().or_else(|| {
        value.as_str().and_then(|raw| match raw.to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
    })
}

fn row_string<'a>(row: &'a SqlRow, column: &str) -> Option<&'a str> {
    row.get(column).and_then(Value::as_str)
}

fn row_i64(row: &SqlRow, column: &str) -> i64 {
    let value = row
        .get(column)
        .unwrap_or_else(|| panic!("missing column {column} in row {row:?}"));
    value_as_i64(value)
        .unwrap_or_else(|| panic!("column {column} is not an i64-compatible value: {value:?}"))
}

fn row_f64(row: &SqlRow, column: &str) -> f64 {
    let value = row
        .get(column)
        .unwrap_or_else(|| panic!("missing column {column} in row {row:?}"));
    value_as_f64(value)
        .unwrap_or_else(|| panic!("column {column} is not an f64-compatible value: {value:?}"))
}

fn row_bool(row: &SqlRow, column: &str) -> bool {
    let value = row
        .get(column)
        .unwrap_or_else(|| panic!("missing column {column} in row {row:?}"));
    value_as_bool(value)
        .unwrap_or_else(|| panic!("column {column} is not a bool-compatible value: {value:?}"))
}

async fn wait_for_api_sql_rows(sql: &str, expected_count: usize, timeout: Duration) -> Vec<SqlRow> {
    let env = TestEnv::global().await;
    let deadline = Instant::now() + timeout;

    loop {
        let rows = sql_result_rows(&env.kalamdb_sql(sql).await);
        if rows.len() == expected_count {
            return rows;
        }

        if Instant::now() >= deadline {
            panic!("API rows for SQL did not reach expected count {expected_count}: {sql}; rows={rows:?}");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_pg_count(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    expected_count: i64,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;

    loop {
        let count = count_rows(client, qualified_table, None).await;
        if count == expected_count {
            return;
        }

        if Instant::now() >= deadline {
            panic!(
                "row count for {qualified_table} did not become {expected_count}; last count={count}"
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_bidirectional_typed_roundtrip_between_pg_and_api() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("typed_sync");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(
        &pg,
        &table,
        "id TEXT, label VARCHAR, attempts SMALLINT, qty INTEGER, total BIGINT, ratio REAL, score DOUBLE PRECISION, active BOOLEAN, notes TEXT",
    )
    .await;

    pg.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, label, attempts, qty, total, ratio, score, active, notes) VALUES \
            ('pg-typed-1', 'alpha', 3, 25, 7000000, 1.5, 9.875, true, 'leader''s note'), \
            ('pg-typed-2', 'beta', 0, -5, 42, -3.25, 0.125, false, NULL);"
    ))
    .await
    .expect("insert typed rows through PostgreSQL");

    let api_rows = wait_for_api_sql_rows(
        &format!(
            "SELECT id, label, attempts, qty, total, ratio, score, active, notes FROM {qualified_table} WHERE id IN ('pg-typed-1', 'pg-typed-2') ORDER BY id"
        ),
        2,
        Duration::from_secs(5),
    )
    .await;
    let first = find_row_by_id(&api_rows, "pg-typed-1");
    assert_eq!(row_string(first, "label"), Some("alpha"));
    assert_eq!(row_i64(first, "attempts"), 3);
    assert_eq!(row_i64(first, "qty"), 25);
    assert_eq!(row_i64(first, "total"), 7_000_000);
    assert!((row_f64(first, "ratio") - 1.5).abs() < 0.001);
    assert!((row_f64(first, "score") - 9.875).abs() < 0.001);
    assert!(row_bool(first, "active"));
    assert_eq!(row_string(first, "notes"), Some("leader's note"));

    let second = find_row_by_id(&api_rows, "pg-typed-2");
    assert_eq!(row_string(second, "label"), Some("beta"));
    assert_eq!(row_i64(second, "attempts"), 0);
    assert_eq!(row_i64(second, "qty"), -5);
    assert_eq!(row_i64(second, "total"), 42);
    assert!((row_f64(second, "ratio") + 3.25).abs() < 0.001);
    assert!((row_f64(second, "score") - 0.125).abs() < 0.001);
    assert!(!row_bool(second, "active"));
    assert!(second.get("notes").is_some_and(Value::is_null));

    env.kalamdb_sql(&format!(
        "INSERT INTO {qualified_table} (id, label, attempts, qty, total, ratio, score, active, notes) VALUES \
            ('api-typed-1', 'from-api', 7, 11, 123456, 2.5, 4.25, true, 'api inserted')"
    ))
    .await;

    let api_inserted = pg
        .query_one(
            &format!(
                "SELECT label, attempts, qty, total, ratio, score, active, notes FROM {qualified_table} WHERE id = $1"
            ),
            &[&"api-typed-1"],
        )
        .await
        .expect("read API-inserted row from PostgreSQL");
    assert_eq!(api_inserted.get::<_, String>(0), "from-api");
    assert_eq!(api_inserted.get::<_, i16>(1), 7);
    assert_eq!(api_inserted.get::<_, i32>(2), 11);
    assert_eq!(api_inserted.get::<_, i64>(3), 123_456);
    assert!((api_inserted.get::<_, f32>(4) - 2.5).abs() < 0.001);
    assert!((api_inserted.get::<_, f64>(5) - 4.25).abs() < 0.001);
    assert!(api_inserted.get::<_, bool>(6));
    assert_eq!(api_inserted.get::<_, Option<String>>(7), Some("api inserted".to_string()));

    env.kalamdb_sql(&format!(
        "UPDATE {qualified_table} SET label = 'updated-from-api', qty = 30, active = false, notes = 'api edit' WHERE id = 'pg-typed-1'"
    ))
    .await;

    let api_updated_in_pg = pg
        .query_one(
            &format!("SELECT label, qty, active, notes FROM {qualified_table} WHERE id = $1"),
            &[&"pg-typed-1"],
        )
        .await
        .expect("read API-updated row from PostgreSQL");
    assert_eq!(api_updated_in_pg.get::<_, String>(0), "updated-from-api");
    assert_eq!(api_updated_in_pg.get::<_, i32>(1), 30);
    assert!(!api_updated_in_pg.get::<_, bool>(2));
    assert_eq!(api_updated_in_pg.get::<_, Option<String>>(3), Some("api edit".to_string()));

    pg.execute(
        &format!(
            "UPDATE {qualified_table} SET label = $1, total = $2, score = $3, notes = $4 WHERE id = $5"
        ),
        &[&"updated-from-pg", &654_321_i64, &5.5_f64, &"pg edit", &"api-typed-1"],
    )
    .await
    .expect("update API-created row through PostgreSQL");

    let api_view = wait_for_api_sql_rows(
        &format!(
            "SELECT id, label, total, score, notes FROM {qualified_table} WHERE id = 'api-typed-1'"
        ),
        1,
        Duration::from_secs(5),
    )
    .await;
    let api_view_row = find_row_by_id(&api_view, "api-typed-1");
    assert_eq!(row_string(api_view_row, "label"), Some("updated-from-pg"));
    assert_eq!(row_i64(api_view_row, "total"), 654_321);
    assert!((row_f64(api_view_row, "score") - 5.5).abs() < 0.001);
    assert_eq!(row_string(api_view_row, "notes"), Some("pg edit"));

    env.kalamdb_sql(&format!("DELETE FROM {qualified_table} WHERE id = 'pg-typed-2'"))
        .await;
    let deleted_in_api = count_rows(&pg, &qualified_table, Some("id = 'pg-typed-2'")).await;
    assert_eq!(deleted_in_api, 0, "API delete should be visible in PostgreSQL");

    pg.execute(&format!("DELETE FROM {qualified_table} WHERE id = $1"), &[&"api-typed-1"])
        .await
        .expect("delete API-created row through PostgreSQL");

    let remaining_rows = wait_for_api_sql_rows(
        &format!("SELECT id FROM {qualified_table} ORDER BY id"),
        1,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(row_string(&remaining_rows[0], "id"), Some("pg-typed-1"));

    pg.disconnect_and_wait_for_session_cleanup().await;
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn e2e_parallel_transactional_inserts_and_updates_stay_consistent() {
    const WORKERS: usize = 6;
    const ROWS_PER_WORKER: usize = 15;

    let env = TestEnv::global().await;
    let coordinator = env.pg_connect().await;
    let table = unique_name("parallel_tx");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(
        &coordinator,
        &table,
        "id TEXT, worker INTEGER, ordinal INTEGER, status TEXT, amount BIGINT",
    )
    .await;

    for worker in 0..WORKERS {
        coordinator
            .execute(
                &format!(
                    "INSERT INTO {qualified_table} (id, worker, ordinal, status, amount) VALUES ($1, $2, $3, $4, $5)"
                ),
                &[
                    &format!("seed-{worker}"),
                    &(worker as i32),
                    &(-1_i32),
                    &"seed",
                    &(1_000_i64 + worker as i64),
                ],
            )
            .await
            .expect("seed parallel update row");
    }

    let barrier = Arc::new(Barrier::new(WORKERS));
    let workers = (0..WORKERS).map(|worker| {
        let barrier = Arc::clone(&barrier);
        let qualified_table = qualified_table.clone();
        async move {
            let env = TestEnv::global().await;
            let mut pg = env.pg_connect().await;
            barrier.wait().await;

            let tx = pg.transaction().await.expect("begin worker transaction");

            let values = (0..ROWS_PER_WORKER)
                .map(|ordinal| {
                    format!(
                        "('wrk-{worker}-{ordinal}', {worker}, {ordinal}, 'inserted-{worker}', {})",
                        10_000_i64 + (worker as i64 * 100) + ordinal as i64
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            tx.batch_execute(&format!(
                "INSERT INTO {qualified_table} (id, worker, ordinal, status, amount) VALUES {values};"
            ))
            .await
            .expect("insert worker batch");

            tx.execute(
                &format!(
                    "UPDATE {qualified_table} SET status = $1, amount = amount + $2 WHERE id = $3"
                ),
                &[
                    &format!("updated-{worker}"),
                    &(500_i64 + worker as i64),
                    &format!("seed-{worker}"),
                ],
            )
            .await
            .expect("update worker seed row");

            tx.commit().await.expect("commit worker transaction");
            pg.disconnect_and_wait_for_session_cleanup().await;
        }
    });

    join_all(workers).await;

    let expected_total = (WORKERS + (WORKERS * ROWS_PER_WORKER)) as i64;
    wait_for_pg_count(&coordinator, &qualified_table, expected_total, Duration::from_secs(5)).await;

    let pg_rows = coordinator
        .query(
            &format!("SELECT id, status, amount FROM {qualified_table} WHERE id LIKE 'seed-%' ORDER BY id"),
            &[],
        )
        .await
        .expect("read updated seed rows from PostgreSQL");
    assert_eq!(pg_rows.len(), WORKERS);
    for (worker, row) in pg_rows.iter().enumerate() {
        assert_eq!(row.get::<_, String>(0), format!("seed-{worker}"));
        assert_eq!(row.get::<_, String>(1), format!("updated-{worker}"));
        assert_eq!(row.get::<_, i64>(2), 1_000_i64 + worker as i64 + 500_i64 + worker as i64);
    }

    let api_total = wait_for_api_sql_rows(
        &format!("SELECT COUNT(*) AS total_rows FROM {qualified_table} LIMIT 1"),
        1,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(row_i64(&api_total[0], "total_rows"), expected_total);

    let api_inserted = wait_for_api_sql_rows(
        &format!(
            "SELECT COUNT(*) AS inserted_rows FROM {qualified_table} WHERE id LIKE 'wrk-%' LIMIT 1"
        ),
        1,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(row_i64(&api_inserted[0], "inserted_rows"), (WORKERS * ROWS_PER_WORKER) as i64);

    let api_seed_rows = wait_for_api_sql_rows(
        &format!("SELECT id, status, amount FROM {qualified_table} WHERE id LIKE 'seed-%' ORDER BY id LIMIT {}", WORKERS + 1),
        WORKERS,
        Duration::from_secs(5),
    )
    .await;
    for worker in 0..WORKERS {
        let seed_id = format!("seed-{worker}");
        let expected_status = format!("updated-{worker}");
        let seed = find_row_by_id(&api_seed_rows, &seed_id);
        assert_eq!(row_string(seed, "status"), Some(expected_status.as_str()));
        assert_eq!(row_i64(seed, "amount"), 1_000_i64 + worker as i64 + 500_i64 + worker as i64);
    }

    let api_sample_rows = wait_for_api_sql_rows(
        &format!(
            "SELECT id, worker, ordinal, status, amount FROM {qualified_table} \
             WHERE id IN ('wrk-0-0', 'wrk-3-7', 'wrk-5-14') ORDER BY id LIMIT 4"
        ),
        3,
        Duration::from_secs(5),
    )
    .await;
    let sample_a = find_row_by_id(&api_sample_rows, "wrk-0-0");
    assert_eq!(row_i64(sample_a, "worker"), 0);
    assert_eq!(row_i64(sample_a, "ordinal"), 0);
    assert_eq!(row_string(sample_a, "status"), Some("inserted-0"));
    assert_eq!(row_i64(sample_a, "amount"), 10_000);

    let sample_b = find_row_by_id(&api_sample_rows, "wrk-3-7");
    assert_eq!(row_i64(sample_b, "worker"), 3);
    assert_eq!(row_i64(sample_b, "ordinal"), 7);
    assert_eq!(row_string(sample_b, "status"), Some("inserted-3"));
    assert_eq!(row_i64(sample_b, "amount"), 10_307);

    let sample_c = find_row_by_id(&api_sample_rows, "wrk-5-14");
    assert_eq!(row_i64(sample_c, "worker"), 5);
    assert_eq!(row_i64(sample_c, "ordinal"), 14);
    assert_eq!(row_string(sample_c, "status"), Some("inserted-5"));
    assert_eq!(row_i64(sample_c, "amount"), 10_514);

    coordinator.disconnect_and_wait_for_session_cleanup().await;
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn e2e_transaction_rollback_discards_insert_update_delete_in_pg_and_api() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("rollback_guard");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, title TEXT, value INTEGER, active BOOLEAN")
        .await;

    pg.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, title, value, active) VALUES \
            ('keep-1', 'baseline one', 10, true), \
            ('keep-2', 'baseline two', 20, false);"
    ))
    .await
    .expect("seed rollback test rows");

    let tx = pg.transaction().await.expect("begin rollback transaction");
    tx.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, title, value, active) VALUES ('temp-insert', 'rollback me', 999, true); \
         UPDATE {qualified_table} SET title = 'changed in tx', value = 111, active = false WHERE id = 'keep-1'; \
         DELETE FROM {qualified_table} WHERE id = 'keep-2';"
    ))
    .await
    .expect("apply transactional insert/update/delete");

    let visible_inside_tx: i64 = tx
        .query_one(&format!("SELECT COUNT(*) FROM {qualified_table}"), &[])
        .await
        .expect("count rows inside rollback transaction")
        .get(0);
    assert_eq!(
        visible_inside_tx, 2,
        "transactional view should include insert and delete effects"
    );

    tx.rollback().await.expect("rollback full transactional mutation set");

    let pg_rows = pg
        .query(
            &format!("SELECT id, title, value, active FROM {qualified_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("query rows after rollback");
    assert_eq!(pg_rows.len(), 2);
    assert_eq!(pg_rows[0].get::<_, String>(0), "keep-1");
    assert_eq!(pg_rows[0].get::<_, String>(1), "baseline one");
    assert_eq!(pg_rows[0].get::<_, i32>(2), 10);
    assert!(pg_rows[0].get::<_, bool>(3));
    assert_eq!(pg_rows[1].get::<_, String>(0), "keep-2");
    assert_eq!(pg_rows[1].get::<_, String>(1), "baseline two");
    assert_eq!(pg_rows[1].get::<_, i32>(2), 20);
    assert!(!pg_rows[1].get::<_, bool>(3));

    let api_rows = wait_for_api_sql_rows(
        &format!("SELECT id, title, value, active FROM {qualified_table} ORDER BY id"),
        2,
        Duration::from_secs(5),
    )
    .await;
    let keep_one = find_row_by_id(&api_rows, "keep-1");
    assert_eq!(row_string(keep_one, "title"), Some("baseline one"));
    assert_eq!(row_i64(keep_one, "value"), 10);
    assert!(row_bool(keep_one, "active"));
    let keep_two = find_row_by_id(&api_rows, "keep-2");
    assert_eq!(row_string(keep_two, "title"), Some("baseline two"));
    assert_eq!(row_i64(keep_two, "value"), 20);
    assert!(!row_bool(keep_two, "active"));

    pg.disconnect_and_wait_for_session_cleanup().await;
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn e2e_disconnect_abort_discards_uncommitted_changes_in_pg_and_api() {
    let env = TestEnv::global().await;
    let coordinator = env.pg_connect().await;
    let table = unique_name("disconnect_abort");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&coordinator, &table, "id TEXT, title TEXT, value INTEGER").await;

    coordinator
        .batch_execute(&format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES \
                ('base-1', 'before disconnect', 10), \
                ('base-2', 'should survive', 20);"
        ))
        .await
        .expect("seed disconnect-abort rows");

    let pg = env.pg_connect().await;
    pg.batch_execute("BEGIN")
        .await
        .expect("begin SQL transaction before disconnect");
    pg.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, title, value) VALUES ('temp-drop', 'should vanish', 999); \
         UPDATE {qualified_table} SET title = 'mutated before disconnect', value = 77 WHERE id = 'base-1'; \
         DELETE FROM {qualified_table} WHERE id = 'base-2';"
    ))
    .await
    .expect("apply uncommitted mutations before disconnect");

    let visible_inside_tx: i64 = pg
        .query_one(&format!("SELECT COUNT(*) FROM {qualified_table}"), &[])
        .await
        .expect("count rows inside SQL transaction")
        .get(0);
    assert_eq!(
        visible_inside_tx, 2,
        "transactional session should observe its uncommitted state"
    );

    pg.disconnect_and_wait_for_session_cleanup().await;

    wait_for_pg_count(&coordinator, &qualified_table, 2, Duration::from_secs(5)).await;
    let pg_rows = coordinator
        .query(&format!("SELECT id, title, value FROM {qualified_table} ORDER BY id"), &[])
        .await
        .expect("query rows after disconnect abort");
    assert_eq!(pg_rows.len(), 2);
    assert_eq!(pg_rows[0].get::<_, String>(0), "base-1");
    assert_eq!(pg_rows[0].get::<_, String>(1), "before disconnect");
    assert_eq!(pg_rows[0].get::<_, i32>(2), 10);
    assert_eq!(pg_rows[1].get::<_, String>(0), "base-2");
    assert_eq!(pg_rows[1].get::<_, String>(1), "should survive");
    assert_eq!(pg_rows[1].get::<_, i32>(2), 20);

    let api_rows = wait_for_api_sql_rows(
        &format!("SELECT id, title, value FROM {qualified_table} ORDER BY id"),
        2,
        Duration::from_secs(5),
    )
    .await;
    let base_one = find_row_by_id(&api_rows, "base-1");
    assert_eq!(row_string(base_one, "title"), Some("before disconnect"));
    assert_eq!(row_i64(base_one, "value"), 10);
    let base_two = find_row_by_id(&api_rows, "base-2");
    assert_eq!(row_string(base_two, "title"), Some("should survive"));
    assert_eq!(row_i64(base_two, "value"), 20);

    coordinator.disconnect_and_wait_for_session_cleanup().await;
}
