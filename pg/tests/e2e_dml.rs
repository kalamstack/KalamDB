// pg/tests/e2e_dml.rs
//
// End-to-end DML tests for the pg_kalam FDW extension.
//
// These tests target a local pgrx PostgreSQL instance and a locally running
// KalamDB server.
//
// Prerequisites:
//   1. KalamDB server running:          cd backend && cargo run
//   2. pgrx PG16 set up:               pg/scripts/pgrx-test-setup.sh
//
// Run:
//   cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_dml)'

#![cfg(feature = "e2e")]

mod e2e_common;

use e2e_common::*;

fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{ts}_{n}")
}

fn postgres_error_text(error: &tokio_postgres::Error) -> String {
    if let Some(db_error) = error.as_db_error() {
        let mut parts = vec![db_error.message().to_string()];
        if let Some(detail) = db_error.detail() {
            parts.push(detail.to_string());
        }
        if let Some(hint) = db_error.hint() {
            parts.push(hint.to_string());
        }
        parts.join(" | ")
    } else {
        error.to_string()
    }
}

// =========================================================================
// Test 1: Bulk INSERT 5 000 rows → verify count → DELETE all → verify empty
// =========================================================================

#[tokio::test]
async fn e2e_bulk_insert_delete_shared_table() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("items");
    let qualified_table = format!("e2e.{table}");

    // --- setup foreign table ---
    create_shared_foreign_table(
        &pg,
        &table,
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;

    // --- insert 5 000 rows in batches of 500 ---
    const TOTAL: i64 = 5_000;
    const BATCH: i64 = 500;

    for batch_start in (0..TOTAL).step_by(BATCH as usize) {
        let mut values = Vec::with_capacity(BATCH as usize);
        for i in batch_start..batch_start + BATCH {
            values.push(format!("('bulk-{i}', 'Item {i}', {i})"));
        }
        let sql = format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("batch insert");
    }

    // --- verify count ---
    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, TOTAL, "expected {TOTAL} rows after bulk insert");

    // --- delete all rows ---
    bulk_delete_all(&pg, &qualified_table, "id").await;

    // --- verify empty ---
    let count_after = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count_after, 0, "expected 0 rows after delete-all");
}

// =========================================================================
// Test 2: INSERT few rows → UPDATE them → verify updates
// =========================================================================

#[tokio::test]
async fn e2e_insert_update_shared_table() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("items");
    let qualified_table = format!("e2e.{table}");

    // --- setup foreign table ---
    create_shared_foreign_table(
        &pg,
        &table,
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;

    // Clean up any leftover rows from a previous run.
    delete_all(&pg, &qualified_table, "id").await;

    // --- insert 5 rows ---
    pg.batch_execute(
        &format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES \
             ('u1', 'Alpha', 10), \
             ('u2', 'Beta',  20), \
             ('u3', 'Gamma', 30), \
             ('u4', 'Delta', 40), \
             ('u5', 'Epsilon', 50);"
        )
    )
    .await
    .expect("insert 5 rows");

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, 5, "expected 5 rows after insert");

    // --- update each row ---
    for (id, new_title, new_value) in [
        ("u1", "Alpha-Updated", 110),
        ("u2", "Beta-Updated", 120),
        ("u3", "Gamma-Updated", 130),
        ("u4", "Delta-Updated", 140),
        ("u5", "Epsilon-Updated", 150),
    ] {
        pg.execute(
            &format!("UPDATE {qualified_table} SET title = $1, value = $2 WHERE id = $3"),
            &[&new_title, &new_value, &id],
        )
        .await
        .expect("update row");
    }

    // --- verify updates ---
    let rows = pg
        .query(
            &format!("SELECT id, title, value FROM {qualified_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("select after update");

    assert_eq!(rows.len(), 5);

    let expected = [
        ("u1", "Alpha-Updated", 110),
        ("u2", "Beta-Updated", 120),
        ("u3", "Gamma-Updated", 130),
        ("u4", "Delta-Updated", 140),
        ("u5", "Epsilon-Updated", 150),
    ];

    for (row, (eid, etitle, evalue)) in rows.iter().zip(expected.iter()) {
        let id: &str = row.get(0);
        let title: &str = row.get(1);
        let value: i32 = row.get(2);
        assert_eq!(id, *eid, "id mismatch");
        assert_eq!(title, *etitle, "title mismatch for {eid}");
        assert_eq!(value, *evalue, "value mismatch for {eid}");
    }

    // --- cleanup ---
    delete_all(&pg, &qualified_table, "id").await;
}

// =========================================================================
// Test 3: User table — different user_ids see different rows
// =========================================================================

#[tokio::test]
async fn e2e_user_table_isolation() {
    let env = TestEnv::global().await;
    let table = unique_name("profiles");
    let qualified_table = format!("e2e.{table}");

    // -- user A session --
    let pg_a = env.pg_connect().await;
    create_user_foreign_table(
        &pg_a,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg_a, "user-a").await;

    // -- user B session --
    let pg_b = env.pg_connect().await;
    set_user_id(&pg_b, "user-b").await;

    // Clean up
    delete_all(&pg_a, &qualified_table, "id").await;
    delete_all(&pg_b, &qualified_table, "id").await;

    // Insert rows as user A
    pg_a.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, name, age) VALUES \
         ('a1', 'Alice', 30), ('a2', 'Ada', 25);"
    ))
    .await
    .expect("user-a insert");

    // Insert rows as user B
    pg_b.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, name, age) VALUES \
         ('b1', 'Bob', 40), ('b2', 'Blake', 35), ('b3', 'Bea', 28);"
    ))
    .await
    .expect("user-b insert");

    // user A sees only 2 rows
    let count_a = count_rows(&pg_a, &qualified_table, None).await;
    assert_eq!(count_a, 2, "user-a should see 2 rows");

    // user B sees only 3 rows
    let count_b = count_rows(&pg_b, &qualified_table, None).await;
    assert_eq!(count_b, 3, "user-b should see 3 rows");

    // Cleanup
    delete_all(&pg_a, &qualified_table, "id").await;
    delete_all(&pg_b, &qualified_table, "id").await;
}

// =========================================================================
// Test 4: Cross-verify — FDW write is visible via KalamDB REST API
// =========================================================================

#[tokio::test]
async fn e2e_cross_verify_fdw_to_rest() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("items");
    let qualified_table = format!("e2e.{table}");

    create_shared_foreign_table(
        &pg,
        &table,
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;

    // Insert via FDW
    pg.batch_execute(
        &format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES ('xv-rest', 'CrossVerify', 999);"
        )
    )
    .await
    .expect("cross-verify insert");

    // Read via KalamDB REST
    let result = env
        .kalamdb_sql(&format!(
            "SELECT id, title, value FROM {qualified_table} WHERE id = 'xv-rest'"
        ))
        .await;
    let result_str = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_str.contains("xv-rest"),
        "FDW-inserted row should be visible via REST API: {result_str}"
    );

    // Cleanup
    pg.execute(
        &format!("DELETE FROM {qualified_table} WHERE id = $1"),
        &[&"xv-rest"],
    )
    .await
    .expect("cleanup cross-verify row");
}

// =========================================================================
// Test 5: Duplicate primary key inserts must fail instead of being swallowed
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_duplicate_primary_key_insert_fails() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("profiles");
    let qualified_table = format!("e2e.{table}");

    create_user_foreign_table(
        &pg,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg, "dup-user").await;

    delete_all(&pg, &qualified_table, "id").await;

    pg.execute(
        &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
        &[&"dup-1", &"Alice", &30_i32],
    )
    .await
    .expect("first insert should succeed");

    let err = pg
        .execute(
            &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
            &[&"dup-1", &"Alice 2", &31_i32],
        )
        .await
        .expect_err("duplicate primary key insert should fail");

    let message = postgres_error_text(&err);
    assert!(
        message.contains("Primary key violation")
            || message.contains("already exists")
            || message.contains("appears multiple times")
            || message.contains("db error"),
        "unexpected duplicate key error: {message}"
    );

    let count = count_rows(&pg, &qualified_table, Some("id = 'dup-1'")).await;
    assert_eq!(count, 1, "first insert should remain committed after duplicate failure");
}

// =========================================================================
// Test 6: Local foreign table without remote KalamDB table must fail on insert
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_insert_without_backing_kalamdb_table_fails() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("missing_profiles");

    pg.batch_execute(
        &format!(
            "CREATE SCHEMA IF NOT EXISTS app; \
             DROP FOREIGN TABLE IF EXISTS app.{table}; \
             CREATE FOREIGN TABLE app.{table} ( \
                 id TEXT, \
                 name TEXT, \
                 age INTEGER, \
                 _userid TEXT, \
                 _seq BIGINT, \
                 _deleted BOOLEAN \
             ) SERVER kalam_server \
             OPTIONS (namespace 'app', \"table\" '{table}', table_type 'user');"
        ),
    )
    .await
    .expect("create local-only foreign table");
    env.kalamdb_sql(&format!("DROP USER TABLE IF EXISTS app.{table}"))
        .await;

    set_user_id(&pg, "user-1").await;

    let err = pg
        .execute(
            &format!("INSERT INTO app.{table} (id, name, age) VALUES ($1, $2, $3)"),
            &[&"p1", &"Alice", &30_i32],
        )
        .await
        .expect_err("insert without backing KalamDB table should fail");

    let message = postgres_error_text(&err);
    assert!(
        message.contains("table not found")
            || message.contains("provider not found")
            || message.contains("db error"),
        "unexpected missing table error: {message}"
    );
}

// =========================================================================
// Test 7: INSERT / UPDATE / DELETE via FDW changes KalamDB rows directly
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_dml_changes_are_visible_in_kalamdb() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("direct_sync");

    create_shared_foreign_table(
        &pg,
        &table,
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;

    pg.execute(
        &format!("INSERT INTO e2e.{table} (id, title, value) VALUES ($1, $2, $3)"),
        &[&"sync-1", &"Created in PG", &10_i32],
    )
    .await
    .expect("insert should succeed");

    let inserted = env
        .kalamdb_sql(&format!(
            "SELECT id, title, value FROM e2e.{table} WHERE id = 'sync-1'"
        ))
        .await;
    let inserted_text = serde_json::to_string(&inserted).unwrap_or_default();
    assert!(
        inserted_text.contains("sync-1") && inserted_text.contains("Created in PG"),
        "insert should be visible in KalamDB: {inserted_text}"
    );

    pg.execute(
        &format!("UPDATE e2e.{table} SET title = $1, value = $2 WHERE id = $3"),
        &[&"Updated in PG", &99_i32, &"sync-1"],
    )
    .await
    .expect("update should succeed");

    let updated = env
        .kalamdb_sql(&format!(
            "SELECT id, title, value FROM e2e.{table} WHERE id = 'sync-1'"
        ))
        .await;
    let updated_text = serde_json::to_string(&updated).unwrap_or_default();
    assert!(
        updated_text.contains("Updated in PG") && updated_text.contains("99"),
        "update should be visible in KalamDB: {updated_text}"
    );

    pg.execute(
        &format!("DELETE FROM e2e.{table} WHERE id = $1"),
        &[&"sync-1"],
    )
    .await
    .expect("delete should succeed");

    let deleted = env
        .kalamdb_sql(&format!("SELECT id FROM e2e.{table} WHERE id = 'sync-1'"))
        .await;
    let deleted_text = serde_json::to_string(&deleted).unwrap_or_default();
    assert!(
        !deleted_text.contains("sync-1"),
        "deleted row should no longer be visible in KalamDB: {deleted_text}"
    );
}

// =========================================================================
// Test 8: SELECT filters and joins with local PostgreSQL tables work
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_select_filters_and_postgres_join_work() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("join_items");

    create_shared_foreign_table(
        &pg,
        &table,
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;
    delete_all(&pg, &format!("e2e.{table}"), "id").await;

    pg.batch_execute(&format!(
        "INSERT INTO e2e.{table} (id, title, value) VALUES \
         ('j1', 'Alpha', 10), \
         ('j2', 'Beta', 20), \
         ('j3', 'Gamma', 30);"
    ))
    .await
    .expect("insert join test rows");

    pg.batch_execute(
        "CREATE TEMP TABLE local_meta (
            id TEXT PRIMARY KEY,
            segment TEXT NOT NULL
        );",
    )
    .await
    .expect("create temp table");

    pg.batch_execute(
        "INSERT INTO local_meta (id, segment) VALUES
         ('j1', 'bronze'),
         ('j2', 'silver'),
         ('j3', 'gold');",
    )
    .await
    .expect("insert local metadata");

    let rows = pg
        .query(
            &format!(
                "SELECT f.id, f.title, m.segment
                 FROM e2e.{table} AS f
                 JOIN local_meta AS m ON m.id = f.id
                 WHERE f.value >= 20
                 ORDER BY f.id"
            ),
            &[],
        )
        .await
        .expect("filter + join query");

    assert_eq!(rows.len(), 2, "expected two joined rows after filter");
    let first: (&str, &str, &str) = (rows[0].get(0), rows[0].get(1), rows[0].get(2));
    let second: (&str, &str, &str) = (rows[1].get(0), rows[1].get(1), rows[1].get(2));
    assert_eq!(first, ("j2", "Beta", "silver"));
    assert_eq!(second, ("j3", "Gamma", "gold"));
}

// =========================================================================
// Test 9: User table scan without kalam.user_id returns a clear error
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_user_table_scan_without_user_id_fails_clearly() {
    let env = TestEnv::global().await;
    let table = unique_name("profiles_missing_uid");
    let writer = env.pg_connect().await;

    create_user_foreign_table(
        &writer,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&writer, "scan-user").await;
    writer
        .execute(
            &format!("INSERT INTO e2e.{table} (id, name, age) VALUES ($1, $2, $3)"),
            &[&"scan-1", &"Alice", &30_i32],
        )
        .await
        .expect("seed user table row");

    let reader = env.pg_connect().await;

    let err = reader
        .query_one(&format!("SELECT id FROM e2e.{table} LIMIT 1"), &[])
        .await
        .expect_err("user table scan without session user should fail");

    let message = postgres_error_text(&err);
    assert!(
        message.contains("user_id"),
        "missing user_id error should mention user_id: {message}"
    );
}

// =========================================================================
// Test 10: Offline KalamDB server produces a connection error
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_offline_kalamdb_server_reports_connection_error() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let server_name = unique_name("offline_server");
    let table_name = unique_name("offline_items");

    pg.batch_execute("CREATE SCHEMA IF NOT EXISTS e2e;")
        .await
        .expect("create e2e schema");

    pg.batch_execute(&format!(
        "CREATE SERVER {server_name}
            FOREIGN DATA WRAPPER pg_kalam
            OPTIONS (host '127.0.0.1', port '1');"
    ))
    .await
    .expect("create offline foreign server");

    let err = pg
        .batch_execute(&format!(
            "CREATE FOREIGN TABLE e2e.{table_name} (
                id TEXT,
                title TEXT,
                value INTEGER
            ) SERVER {server_name}
            OPTIONS (namespace 'e2e', \"table\" '{table_name}', table_type 'shared');"
        ))
        .await
        .expect_err("offline server create should fail");

    let message = postgres_error_text(&err);
    assert!(
        message.contains("KalamDB server")
            || message.contains("could not connect")
            || message.contains("unreachable"),
        "offline server error should be user-facing: {message}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS e2e.{table_name};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SERVER IF EXISTS {server_name} CASCADE;"))
        .await
        .ok();
}

// =========================================================================
// Test 11: search_path-driven schema mirroring works without namespace option
// =========================================================================

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_search_path_schema_mirror_works_without_namespace_option() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let schema = unique_name("search_ns");
    let table = unique_name("search_items");

    pg.batch_execute(&format!(
        "CREATE SCHEMA IF NOT EXISTS {schema};
         SET search_path TO {schema};
         CREATE FOREIGN TABLE {table} (
             id TEXT,
             title TEXT,
             value INTEGER
         ) SERVER kalam_server
         OPTIONS (table_type 'shared');"
    ))
    .await
    .expect("create mirrored foreign table via search_path");

    pg.execute(
        &format!("INSERT INTO {schema}.{table} (id, title, value) VALUES ($1, $2, $3)"),
        &[&"spath-1", &"From search_path", &7_i32],
    )
    .await
    .expect("insert through schema-mirrored foreign table");

    let rows = pg
        .query(
            &format!("SELECT id, title, value FROM {schema}.{table} WHERE id = $1"),
            &[&"spath-1"],
        )
        .await
        .expect("select through schema-mirrored foreign table");
    assert_eq!(rows.len(), 1, "expected one mirrored row through PostgreSQL");

    let result = env
        .kalamdb_sql(&format!(
            "SELECT id, title, value FROM {schema}.{table} WHERE id = 'spath-1'"
        ))
        .await;
    let result_text = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_text.contains("spath-1") && result_text.contains("From search_path"),
        "search_path-mirrored foreign table should write into KalamDB namespace {schema}: {result_text}"
    );

    pg.batch_execute(&format!(
        "RESET search_path;
         DROP FOREIGN TABLE IF EXISTS {schema}.{table};
         DROP SCHEMA IF EXISTS {schema} CASCADE;"
    ))
    .await
    .ok();
}
