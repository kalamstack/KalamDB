use super::common::{
    await_user_shard_leader, count_rows, create_user_foreign_table, postgres_error_text,
    retry_transient_user_leader_error, same_user_shard_pair, set_user_id, unique_name, TestEnv,
};

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_transaction_begin_commit_persists_rows() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("profiles_tx_commit");
    let qualified_table = format!("e2e.{table}");

    create_user_foreign_table(
        &pg,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg, "txn-commit-user").await;
    await_user_shard_leader("txn-commit-user").await;

    let tx = pg.transaction().await.expect("begin");
    tx.execute(
        &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
        &[&"commit-1", &"Alice", &30_i32],
    )
    .await
    .expect("insert first row");
    tx.execute(
        &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
        &[&"commit-2", &"Bob", &31_i32],
    )
    .await
    .expect("insert second row");
    tx.commit().await.expect("commit");

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, 2, "committed transaction should persist both rows");
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_transaction_begin_rollback_discards_rows() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("profiles_tx_rollback");
    let qualified_table = format!("e2e.{table}");

    create_user_foreign_table(
        &pg,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg, "txn-rollback-user").await;
    await_user_shard_leader("txn-rollback-user").await;

    let tx = pg.transaction().await.expect("begin");
    tx.execute(
        &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
        &[&"rollback-1", &"Alice", &30_i32],
    )
    .await
    .expect("insert row");
    tx.rollback().await.expect("rollback");

    let count = count_rows(&pg, &qualified_table, None).await;
    assert_eq!(count, 0, "rolled back transaction should not persist rows");
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_transaction_duplicate_primary_key_commit_fails() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("profiles_tx_duplicate");
    let qualified_table = format!("e2e.{table}");

    create_user_foreign_table(
        &pg,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;
    set_user_id(&pg, "txn-duplicate-user").await;
    await_user_shard_leader("txn-duplicate-user").await;

    let tx = pg.transaction().await.expect("begin");
    tx.execute(
        &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
        &[&"dup-1", &"Alice", &30_i32],
    )
    .await
    .expect("first insert should stage");
    tx.execute(
        &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
        &[&"dup-1", &"Alice 2", &31_i32],
    )
    .await
    .expect("duplicate insert should stage until commit");

    let err = tx
        .commit()
        .await
        .expect_err("duplicate primary key insert should fail at commit");

    let message = postgres_error_text(&err);
    assert!(
        message.contains("Primary key violation")
            || message.contains("already exists")
            || message.contains("appears multiple times")
            || message.contains("db error"),
        "unexpected duplicate key error: {message}"
    );

    let reader = env.pg_connect().await;
    set_user_id(&reader, "txn-duplicate-user").await;
    await_user_shard_leader("txn-duplicate-user").await;
    let count = count_rows(&reader, &qualified_table, Some("id = 'dup-1'")) .await;
    assert_eq!(count, 0, "failed commit should leave no committed rows");
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn e2e_transaction_switching_user_id_keeps_rows_in_separate_user_scopes() {
    let env = TestEnv::global().await;
    let table = unique_name("profiles_tx_user_scope");
    let qualified_table = format!("e2e.{table}");
    let (first_user_id, second_user_id) =
        same_user_shard_pair("txn-scope-user-a", "txn-scope-user-b").await;
    let pg = env.pg_connect().await;

    create_user_foreign_table(
        &pg,
        &table,
        "id TEXT, name TEXT, age INTEGER, _userid TEXT, _seq BIGINT, _deleted BOOLEAN",
    )
    .await;

    await_user_shard_leader(&first_user_id).await;
    await_user_shard_leader(&second_user_id).await;

    let (visible_a_in_tx, visible_b_in_tx) = retry_transient_user_leader_error(
        "multi-user transaction inflight visibility",
        || {
            let env = &env;
            let qualified_table = qualified_table.clone();
            let first_user_id = first_user_id.clone();
            let second_user_id = second_user_id.clone();

            async move {
                let mut pg = env.pg_connect().await;
                let tx = pg.transaction().await?;

                tx.batch_execute(&format!("SET LOCAL kalam.user_id = '{first_user_id}'"))
                    .await?;
                tx.execute(
                    &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
                    &[&"profile-1", &"Alice", &30_i32],
                )
                .await?;
                tx.execute(
                    &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
                    &[&"profile-2", &"Ava", &31_i32],
                )
                .await?;

                tx.batch_execute(&format!("SET LOCAL kalam.user_id = '{second_user_id}'"))
                    .await?;
                tx.execute(
                    &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
                    &[&"profile-1", &"Bob", &40_i32],
                )
                .await?;
                tx.execute(
                    &format!("INSERT INTO {qualified_table} (id, name, age) VALUES ($1, $2, $3)"),
                    &[&"profile-2", &"Bea", &41_i32],
                )
                .await?;

                let visible_b = tx
                    .query(
                        &format!("SELECT id, name FROM {qualified_table} ORDER BY id"),
                        &[],
                    )
                    .await?
                    .iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                    .collect::<Vec<_>>();

                tx.batch_execute(&format!("SET LOCAL kalam.user_id = '{first_user_id}'"))
                    .await?;
                let visible_a = tx
                    .query(
                        &format!("SELECT id, name FROM {qualified_table} ORDER BY id"),
                        &[],
                    )
                    .await?
                    .iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                    .collect::<Vec<_>>();

                tx.commit().await?;
                Ok((visible_a, visible_b))
            }
        },
    )
    .await;

    assert_eq!(visible_a_in_tx, vec![
        ("profile-1".to_string(), "Alice".to_string()),
        ("profile-2".to_string(), "Ava".to_string()),
    ]);
    assert_eq!(visible_b_in_tx, vec![
        ("profile-1".to_string(), "Bob".to_string()),
        ("profile-2".to_string(), "Bea".to_string()),
    ]);

    let reader_a = env.pg_connect().await;
    set_user_id(&reader_a, &first_user_id).await;
    await_user_shard_leader(&first_user_id).await;
    let rows_a = reader_a
        .query(
            &format!("SELECT id, name FROM {qualified_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("query user-a rows");
    let visible_a = rows_a
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(visible_a, vec![
        ("profile-1".to_string(), "Alice".to_string()),
        ("profile-2".to_string(), "Ava".to_string()),
    ]);

    let reader_b = env.pg_connect().await;
    set_user_id(&reader_b, &second_user_id).await;
    await_user_shard_leader(&second_user_id).await;
    let rows_b = reader_b
        .query(
            &format!("SELECT id, name FROM {qualified_table} ORDER BY id"),
            &[],
        )
        .await
        .expect("query user-b rows");
    let visible_b = rows_b
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(visible_b, vec![
        ("profile-1".to_string(), "Bob".to_string()),
        ("profile-2".to_string(), "Bea".to_string()),
    ]);
}