use super::common::{
    await_user_shard_leader, count_rows, create_shared_kalam_table, create_user_kalam_table,
    pg_backend_pid, postgres_error_text, retry_transient_user_leader_error,
    same_user_shard_pair, set_user_id, unique_name, TestEnv,
};
use serde_json::Value;
use std::time::{Duration, Instant};

fn sql_rows(result: &Value) -> Vec<Vec<Value>> {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|entry| entry["rows"].as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.as_array().cloned())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn string_cell(row: &[Value], index: usize) -> Option<String> {
    row.get(index).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    })
}

fn i64_cell(row: &[Value], index: usize) -> Option<i64> {
    row.get(index).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|raw| i64::try_from(raw).ok()))
            .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
    })
}

async fn fetch_session_rows(env: &TestEnv, backend_pid: u32) -> Vec<Vec<Value>> {
    sql_rows(
        &env.kalamdb_sql(&format!(
            "SELECT session_id, state, transaction_id, transaction_state \
             FROM system.sessions \
             WHERE backend_pid = {backend_pid} \
             ORDER BY last_seen_at DESC"
        ))
        .await,
    )
}

async fn fetch_transaction_rows(env: &TestEnv, transaction_id: &str) -> Vec<Vec<Value>> {
    sql_rows(
        &env.kalamdb_sql(&format!(
            "SELECT transaction_id, owner_id, origin, state, write_count \
             FROM system.transactions \
             WHERE transaction_id = '{transaction_id}'"
        ))
        .await,
    )
}

async fn wait_for_active_pg_transaction(
    env: &TestEnv,
    backend_pid: u32,
    timeout: Duration,
) -> (String, Vec<Value>) {
    let deadline = Instant::now() + timeout;

    loop {
        let session_rows = fetch_session_rows(env, backend_pid).await;
        if let Some(transaction_id) = session_rows
            .iter()
            .find_map(|row| string_cell(row, 2).filter(|value| !value.is_empty()))
        {
            let transaction_rows = fetch_transaction_rows(env, &transaction_id).await;
            if let Some(transaction_row) = transaction_rows.first() {
                return (transaction_id, transaction_row.clone());
            }
        }

        if Instant::now() >= deadline {
            panic!(
                "backend_pid {backend_pid} did not expose an active pg transaction within timeout"
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_transaction_cleanup(env: &TestEnv, transaction_id: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;

    loop {
        if fetch_transaction_rows(env, transaction_id).await.is_empty() {
            return;
        }

        if Instant::now() >= deadline {
            panic!(
                "transaction {transaction_id} remained in system.transactions past cleanup timeout"
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_transaction_begin_commit_persists_rows() {
    let env = TestEnv::global().await;
    let mut pg = env.pg_connect().await;
    let table = unique_name("profiles_tx_commit");
    let qualified_table = format!("e2e.{table}");

    create_user_kalam_table(&pg, &table, "id TEXT, name TEXT, age INTEGER").await;
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

    create_user_kalam_table(&pg, &table, "id TEXT, name TEXT, age INTEGER").await;
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

    create_user_kalam_table(&pg, &table, "id TEXT, name TEXT, age INTEGER").await;
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
    let count = count_rows(&reader, &qualified_table, Some("id = 'dup-1'")).await;
    assert_eq!(count, 0, "failed commit should leave no committed rows");
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn e2e_transaction_mixed_native_and_kalamdb_rows_share_one_remote_transaction() {
    let env = TestEnv::global().await;
    let setup_pg = env.pg_connect().await;
    let foreign_table = unique_name("shared_tx_mixed_commit");
    let qualified_foreign_table = format!("e2e.{foreign_table}");
    let native_table = unique_name("native_tx_mixed_commit");
    let qualified_native_table = format!("public.{native_table}");

    create_shared_kalam_table(&setup_pg, &foreign_table, "id TEXT, name TEXT").await;
    setup_pg
        .batch_execute(&format!(
            "CREATE TABLE {qualified_native_table} (id TEXT PRIMARY KEY, note TEXT NOT NULL)"
        ))
        .await
        .expect("create native postgres table");

    let mut pg = env.pg_connect().await;
    let backend_pid = pg_backend_pid(&pg).await;
    let pre_foreign_rows = fetch_session_rows(env, backend_pid).await;
    assert!(
        pre_foreign_rows
            .iter()
            .all(|row| string_cell(row, 2).is_none()),
        "remote transaction should not exist before the first foreign statement: {pre_foreign_rows:?}"
    );

    let tx = pg.transaction().await.expect("begin mixed transaction");
    let native_count_before: i64 = tx
        .query_one(&format!("SELECT COUNT(*) FROM {qualified_native_table}"), &[])
        .await
        .expect("count native rows before insert")
        .get(0);
    assert_eq!(native_count_before, 0);

    tx.execute(
        &format!("INSERT INTO {qualified_native_table} (id, note) VALUES ($1, $2)"),
        &[&"native-1", &"before-foreign"],
    )
    .await
    .expect("insert first native row");

    let still_no_remote_rows = fetch_session_rows(env, backend_pid).await;
    assert!(
        still_no_remote_rows
            .iter()
            .all(|row| string_cell(row, 2).is_none()),
        "native PostgreSQL work alone should not open a KalamDB transaction: {still_no_remote_rows:?}"
    );

    tx.execute(
        &format!("INSERT INTO {qualified_foreign_table} (id, name) VALUES ($1, $2)"),
        &[&"foreign-1", &"Alice"],
    )
    .await
    .expect("insert first foreign row");

    let (transaction_id, transaction_row) =
        wait_for_active_pg_transaction(env, backend_pid, Duration::from_secs(3)).await;
    assert_eq!(string_cell(&transaction_row, 2).as_deref(), Some("PgRpc"));
    assert_eq!(string_cell(&transaction_row, 3).as_deref(), Some("open_write"));
    assert_eq!(i64_cell(&transaction_row, 4), Some(1));

    tx.execute(
        &format!("INSERT INTO {qualified_native_table} (id, note) VALUES ($1, $2)"),
        &[&"native-2", &"after-foreign"],
    )
    .await
    .expect("insert second native row");

    tx.execute(
        &format!("INSERT INTO {qualified_foreign_table} (id, name) VALUES ($1, $2)"),
        &[&"foreign-2", &"Bob"],
    )
    .await
    .expect("insert second foreign row");

    let session_rows = fetch_session_rows(env, backend_pid).await;
    assert_eq!(string_cell(&session_rows[0], 2).as_deref(), Some(transaction_id.as_str()));

    let repeated_transaction_rows = fetch_transaction_rows(env, &transaction_id).await;
    assert_eq!(repeated_transaction_rows.len(), 1);
    assert_eq!(string_cell(&repeated_transaction_rows[0], 0).as_deref(), Some(transaction_id.as_str()));
    assert_eq!(string_cell(&repeated_transaction_rows[0], 2).as_deref(), Some("PgRpc"));
    assert_eq!(string_cell(&repeated_transaction_rows[0], 3).as_deref(), Some("open_write"));
    assert_eq!(i64_cell(&repeated_transaction_rows[0], 4), Some(2));

    tx.commit().await.expect("commit mixed native + foreign transaction");

    wait_for_transaction_cleanup(env, &transaction_id, Duration::from_secs(5)).await;

    let pg_reader = env.pg_connect().await;
    let native_rows = pg_reader
        .query(
            &format!(
                "SELECT id, note FROM {qualified_native_table} ORDER BY id"
            ),
            &[],
        )
        .await
        .expect("query committed native rows");
    let committed_native_rows = native_rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        committed_native_rows,
        vec![
            ("native-1".to_string(), "before-foreign".to_string()),
            ("native-2".to_string(), "after-foreign".to_string()),
        ]
    );

    let api_rows = sql_rows(
        &env.kalamdb_sql(&format!(
            "SELECT id, name FROM {qualified_foreign_table} ORDER BY id"
        ))
        .await,
    );
    assert_eq!(api_rows.len(), 2);
    assert_eq!(string_cell(&api_rows[0], 0).as_deref(), Some("foreign-1"));
    assert_eq!(string_cell(&api_rows[0], 1).as_deref(), Some("Alice"));
    assert_eq!(string_cell(&api_rows[1], 0).as_deref(), Some("foreign-2"));
    assert_eq!(string_cell(&api_rows[1], 1).as_deref(), Some("Bob"));
}

#[tokio::test]
#[ntest::timeout(12000)]
async fn e2e_transaction_kalamdb_commit_failure_rolls_back_native_postgres_rows() {
    let env = TestEnv::global().await;
    let setup_pg = env.pg_connect().await;
    let foreign_table = unique_name("shared_tx_mixed_abort");
    let qualified_foreign_table = format!("e2e.{foreign_table}");
    let native_table = unique_name("native_tx_mixed_abort");
    let qualified_native_table = format!("public.{native_table}");

    create_shared_kalam_table(&setup_pg, &foreign_table, "id TEXT, name TEXT").await;
    setup_pg
        .batch_execute(&format!(
            "CREATE TABLE {qualified_native_table} (id TEXT PRIMARY KEY, note TEXT NOT NULL)"
        ))
        .await
        .expect("create native postgres table");

    let mut pg = env.pg_connect().await;
    let backend_pid = pg_backend_pid(&pg).await;

    let tx = pg.transaction().await.expect("begin mixed failure transaction");
    tx.execute(
        &format!("INSERT INTO {qualified_native_table} (id, note) VALUES ($1, $2)"),
        &[&"native-1", &"before-foreign-failure"],
    )
    .await
    .expect("insert first native row");
    tx.execute(
        &format!("INSERT INTO {qualified_foreign_table} (id, name) VALUES ($1, $2)"),
        &[&"dup-1", &"Alice"],
    )
    .await
    .expect("stage first foreign row");
    tx.execute(
        &format!("INSERT INTO {qualified_foreign_table} (id, name) VALUES ($1, $2)"),
        &[&"dup-1", &"Alice duplicate"],
    )
    .await
    .expect("stage duplicate foreign row until commit");
    tx.execute(
        &format!("INSERT INTO {qualified_native_table} (id, note) VALUES ($1, $2)"),
        &[&"native-2", &"after-foreign-failure"],
    )
    .await
    .expect("insert second native row");

    let (transaction_id, transaction_row) =
        wait_for_active_pg_transaction(env, backend_pid, Duration::from_secs(3)).await;
    assert_eq!(string_cell(&transaction_row, 2).as_deref(), Some("PgRpc"));
    assert_eq!(string_cell(&transaction_row, 3).as_deref(), Some("open_write"));
    assert_eq!(i64_cell(&transaction_row, 4), Some(2));

    let err = tx
        .commit()
        .await
        .expect_err("duplicate foreign key should abort mixed transaction at commit");
    let message = postgres_error_text(&err);
    assert!(
        message.contains("Primary key violation")
            || message.contains("already exists")
            || message.contains("appears multiple times")
            || message.contains("db error"),
        "unexpected duplicate key error: {message}"
    );

    wait_for_transaction_cleanup(env, &transaction_id, Duration::from_secs(5)).await;

    let pg_reader = env.pg_connect().await;
    let native_count = count_rows(&pg_reader, &qualified_native_table, None).await;
    assert_eq!(
        native_count, 0,
        "PostgreSQL native rows should roll back when KalamDB commit aborts the transaction"
    );

    let api_rows = sql_rows(
        &env.kalamdb_sql(&format!(
            "SELECT id, name FROM {qualified_foreign_table} WHERE id = 'dup-1'"
        ))
        .await,
    );
    assert!(
        api_rows.is_empty(),
        "failed mixed commit should not leave committed KalamDB rows behind: {api_rows:?}"
    );
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

    create_user_kalam_table(&pg, &table, "id TEXT, name TEXT, age INTEGER").await;

    await_user_shard_leader(&first_user_id).await;
    await_user_shard_leader(&second_user_id).await;

    let (visible_a_in_tx, visible_b_in_tx) =
        retry_transient_user_leader_error("multi-user transaction inflight visibility", || {
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
                    .query(&format!("SELECT id, name FROM {qualified_table} ORDER BY id"), &[])
                    .await?
                    .iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                    .collect::<Vec<_>>();

                tx.batch_execute(&format!("SET LOCAL kalam.user_id = '{first_user_id}'"))
                    .await?;
                let visible_a = tx
                    .query(&format!("SELECT id, name FROM {qualified_table} ORDER BY id"), &[])
                    .await?
                    .iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                    .collect::<Vec<_>>();

                tx.commit().await?;
                Ok((visible_a, visible_b))
            }
        })
        .await;

    assert_eq!(
        visible_a_in_tx,
        vec![
            ("profile-1".to_string(), "Alice".to_string()),
            ("profile-2".to_string(), "Ava".to_string()),
        ]
    );
    assert_eq!(
        visible_b_in_tx,
        vec![
            ("profile-1".to_string(), "Bob".to_string()),
            ("profile-2".to_string(), "Bea".to_string()),
        ]
    );

    let reader_a = env.pg_connect().await;
    set_user_id(&reader_a, &first_user_id).await;
    await_user_shard_leader(&first_user_id).await;
    let rows_a = reader_a
        .query(&format!("SELECT id, name FROM {qualified_table} ORDER BY id"), &[])
        .await
        .expect("query user-a rows");
    let visible_a = rows_a
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        visible_a,
        vec![
            ("profile-1".to_string(), "Alice".to_string()),
            ("profile-2".to_string(), "Ava".to_string()),
        ]
    );

    let reader_b = env.pg_connect().await;
    set_user_id(&reader_b, &second_user_id).await;
    await_user_shard_leader(&second_user_id).await;
    let rows_b = reader_b
        .query(&format!("SELECT id, name FROM {qualified_table} ORDER BY id"), &[])
        .await
        .expect("query user-b rows");
    let visible_b = rows_b
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        visible_b,
        vec![
            ("profile-1".to_string(), "Bob".to_string()),
            ("profile-2".to_string(), "Bea".to_string()),
        ]
    );
}
