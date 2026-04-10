use super::common::{
    await_user_shard_leader, count_rows, create_shared_foreign_table, create_user_foreign_table,
    delete_all, retry_transient_user_leader_error, set_user_id, unique_name, TestEnv,
};

#[tokio::test]
async fn e2e_bulk_insert_delete_shared_table() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("items");
    let qualified_table = format!("e2e.{table}");

    create_shared_foreign_table(&pg, &table, "id TEXT, title TEXT, value INTEGER").await;

    const TOTAL: i64 = 5_000;
    const BATCH: i64 = 500;

    for batch_start in (0..TOTAL).step_by(BATCH as usize) {
        let mut values = Vec::with_capacity(BATCH as usize);
        for index in batch_start..batch_start + BATCH {
            values.push(format!("('bulk-{index}', 'Item {index}', {index})"));
        }
        let sql = format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES {}",
            values.join(", ")
        );
        pg.batch_execute(&sql).await.expect("batch insert");
    }

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, TOTAL, "expected {TOTAL} rows after bulk insert");

    super::common::bulk_delete_all(&pg, &qualified_table, "id").await;

    let count_after = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count_after, 0, "expected 0 rows after delete-all");
}

#[tokio::test]
async fn e2e_insert_update_shared_table() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("items");
    let qualified_table = format!("e2e.{table}");

    create_shared_foreign_table(&pg, &table, "id TEXT, title TEXT, value INTEGER").await;
    delete_all(&pg, &qualified_table, "id").await;

    pg.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, title, value) VALUES \
             ('u1', 'Alpha', 10), \
             ('u2', 'Beta',  20), \
             ('u3', 'Gamma', 30), \
             ('u4', 'Delta', 40), \
             ('u5', 'Epsilon', 50);"
    ))
    .await
    .expect("insert 5 rows");

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, 5, "expected 5 rows after insert");

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

    let rows = pg
        .query(&format!("SELECT id, title, value FROM {qualified_table} ORDER BY id"), &[])
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

    for (row, (expected_id, expected_title, expected_value)) in rows.iter().zip(expected.iter()) {
        let id: &str = row.get(0);
        let title: &str = row.get(1);
        let value: i32 = row.get(2);
        assert_eq!(id, *expected_id, "id mismatch");
        assert_eq!(title, *expected_title, "title mismatch for {expected_id}");
        assert_eq!(value, *expected_value, "value mismatch for {expected_id}");
    }

    delete_all(&pg, &qualified_table, "id").await;
}

#[tokio::test]
async fn e2e_user_table_isolation() {
    let env = TestEnv::global().await;
    let table = unique_name("profiles");
    let qualified_table = format!("e2e.{table}");

    let pg_a = env.pg_connect().await;
    create_user_foreign_table(
        &pg_a,
        &table,
        "id TEXT, name TEXT, age INTEGER",
    )
    .await;
    set_user_id(&pg_a, "user-a").await;
    await_user_shard_leader("user-a").await;

    let pg_b = env.pg_connect().await;
    set_user_id(&pg_b, "user-b").await;
    await_user_shard_leader("user-b").await;

        let user_a_insert = format!(
        "INSERT INTO {qualified_table} (id, name, age) VALUES \
            ('a1', 'Alice', 30), ('a2', 'Ada', 25);"
        );
        retry_transient_user_leader_error("user-a insert", || pg_a.batch_execute(&user_a_insert)).await;

        let user_b_insert = format!(
        "INSERT INTO {qualified_table} (id, name, age) VALUES \
            ('b1', 'Bob', 40), ('b2', 'Blake', 35), ('b3', 'Bea', 28);"
        );
        retry_transient_user_leader_error("user-b insert", || pg_b.batch_execute(&user_b_insert)).await;

    let count_a = count_rows(&pg_a, &qualified_table, None).await;
    assert_eq!(count_a, 2, "user-a should see 2 rows");

    let count_b = count_rows(&pg_b, &qualified_table, None).await;
    assert_eq!(count_b, 3, "user-b should see 3 rows");

    delete_all(&pg_a, &qualified_table, "id").await;
    delete_all(&pg_b, &qualified_table, "id").await;
}
