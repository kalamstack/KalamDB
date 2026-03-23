// pg/tests/e2e_dml.rs
//
// End-to-end DML tests for the pg_kalam FDW extension.
//
// These tests require Docker and the `kalamdb-pg:latest` image.
// Run with:
//   cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e)'
//
// The first invocation starts docker-compose.test.yml automatically and
// authenticates against the KalamDB server.

#![cfg(feature = "e2e")]

mod e2e_common;

use e2e_common::*;

// =========================================================================
// Test 1: Bulk INSERT 5 000 rows → verify count → DELETE all → verify empty
// =========================================================================

#[tokio::test]
async fn e2e_bulk_insert_delete_shared_table() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    // --- setup foreign table ---
    create_shared_foreign_table(
        &pg,
        "items",
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
            "INSERT INTO e2e.items (id, title, value) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("batch insert");
    }

    // --- verify count ---
    let count = count_rows(&pg, "e2e.items", None).await;
    assert_eq!(count, TOTAL, "expected {TOTAL} rows after bulk insert");

    // --- delete all rows ---
    delete_all(&pg, "e2e.items", "id").await;

    // --- verify empty ---
    let count_after = count_rows(&pg, "e2e.items", None).await;
    assert_eq!(count_after, 0, "expected 0 rows after delete-all");
}

// =========================================================================
// Test 2: INSERT few rows → UPDATE them → verify updates
// =========================================================================

#[tokio::test]
async fn e2e_insert_update_shared_table() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    // --- setup foreign table ---
    create_shared_foreign_table(
        &pg,
        "items",
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;

    // Clean up any leftover rows from a previous run.
    delete_all(&pg, "e2e.items", "id").await;

    // --- insert 5 rows ---
    pg.batch_execute(
        "INSERT INTO e2e.items (id, title, value) VALUES \
         ('u1', 'Alpha', 10), \
         ('u2', 'Beta',  20), \
         ('u3', 'Gamma', 30), \
         ('u4', 'Delta', 40), \
         ('u5', 'Epsilon', 50);"
    )
    .await
    .expect("insert 5 rows");

    let count = count_rows(&pg, "e2e.items", None).await;
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
            "UPDATE e2e.items SET title = $1, value = $2 WHERE id = $3",
            &[&new_title, &new_value, &id],
        )
        .await
        .expect("update row");
    }

    // --- verify updates ---
    let rows = pg
        .query(
            "SELECT id, title, value FROM e2e.items ORDER BY id",
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
    delete_all(&pg, "e2e.items", "id").await;
}

// =========================================================================
// Test 3: User table — different user_ids see different rows
// =========================================================================

#[tokio::test]
async fn e2e_user_table_isolation() {
    let env = TestEnv::global().await;

    // -- user A session --
    let pg_a = env.pg_connect().await;
    create_user_foreign_table(
        &pg_a,
        "profiles",
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg_a, "user-a").await;

    // -- user B session --
    let pg_b = env.pg_connect().await;
    set_user_id(&pg_b, "user-b").await;

    // Clean up
    delete_all(&pg_a, "e2e.profiles", "id").await;
    delete_all(&pg_b, "e2e.profiles", "id").await;

    // Insert rows as user A
    pg_a.batch_execute(
        "INSERT INTO e2e.profiles (id, name, age) VALUES \
         ('a1', 'Alice', 30), ('a2', 'Ada', 25);"
    )
    .await
    .expect("user-a insert");

    // Insert rows as user B
    pg_b.batch_execute(
        "INSERT INTO e2e.profiles (id, name, age) VALUES \
         ('b1', 'Bob', 40), ('b2', 'Blake', 35), ('b3', 'Bea', 28);"
    )
    .await
    .expect("user-b insert");

    // user A sees only 2 rows
    let count_a = count_rows(&pg_a, "e2e.profiles", None).await;
    assert_eq!(count_a, 2, "user-a should see 2 rows");

    // user B sees only 3 rows
    let count_b = count_rows(&pg_b, "e2e.profiles", None).await;
    assert_eq!(count_b, 3, "user-b should see 3 rows");

    // Cleanup
    delete_all(&pg_a, "e2e.profiles", "id").await;
    delete_all(&pg_b, "e2e.profiles", "id").await;
}

// =========================================================================
// Test 4: Cross-verify — FDW write is visible via KalamDB REST API
// =========================================================================

#[tokio::test]
async fn e2e_cross_verify_fdw_to_rest() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;

    create_shared_foreign_table(
        &pg,
        "items",
        "id TEXT, title TEXT, value INTEGER",
    )
    .await;

    // Insert via FDW
    pg.batch_execute(
        "INSERT INTO e2e.items (id, title, value) VALUES ('xv-rest', 'CrossVerify', 999);"
    )
    .await
    .expect("cross-verify insert");

    // Read via KalamDB REST
    let result = env
        .kalamdb_sql("SELECT id, title, value FROM e2e.items WHERE id = 'xv-rest'")
        .await;
    let result_str = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_str.contains("xv-rest"),
        "FDW-inserted row should be visible via REST API: {result_str}"
    );

    // Cleanup
    pg.execute(
        "DELETE FROM e2e.items WHERE id = $1",
        &[&"xv-rest"],
    )
    .await
    .expect("cleanup cross-verify row");
}
