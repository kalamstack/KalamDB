use super::common::{
    await_user_shard_leader, count_rows, create_user_foreign_table, kalamdb_grpc_target,
    postgres_error_text, retry_transient_user_leader_error, set_user_id, unique_name, TestEnv,
};

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
        "id TEXT, name TEXT, age INTEGER",
    )
    .await;
    set_user_id(&pg, "dup-user").await;
    await_user_shard_leader("dup-user").await;

    let first_insert_sql =
        format!("INSERT INTO {qualified_table} (id, name, age) VALUES ('dup-1', 'Alice', 30)");
    retry_transient_user_leader_error("first duplicate test insert", || {
        pg.batch_execute(&first_insert_sql)
    })
    .await;

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

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_insert_without_backing_kalamdb_table_fails() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("missing_profiles");

    pg.batch_execute(&format!(
        "CREATE SCHEMA IF NOT EXISTS app; \
             DROP FOREIGN TABLE IF EXISTS app.{table}; \
             CREATE FOREIGN TABLE app.{table} ( \
                 id TEXT, \
                 name TEXT, \
                 age INTEGER \
             ) SERVER kalam_server \
             OPTIONS (namespace 'app', \"table\" '{table}', table_type 'user');"
    ))
    .await
    .expect("create local-only foreign table");
    env.kalamdb_sql(&format!("DROP USER TABLE IF EXISTS app.{table}")).await;

    set_user_id(&pg, "user-1").await;
    await_user_shard_leader("user-1").await;

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

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_user_table_scan_without_user_id_fails_clearly() {
    let env = TestEnv::global().await;
    let table = unique_name("profiles_missing_uid");
    let writer = env.pg_connect().await;

    create_user_foreign_table(
        &writer,
        &table,
        "id TEXT, name TEXT, age INTEGER",
    )
    .await;
    set_user_id(&writer, "scan-user").await;
    await_user_shard_leader("scan-user").await;
    let seed_insert_sql =
        format!("INSERT INTO e2e.{table} (id, name, age) VALUES ('scan-1', 'Alice', 30)");
    retry_transient_user_leader_error("seed user table row", || {
        writer.batch_execute(&seed_insert_sql)
    })
    .await;

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

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_invalid_auth_header_metadata_fails_clearly() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let server_name = unique_name("invalid_auth_server");
    let table_name = unique_name("invalid_auth_items");
    let (grpc_host, grpc_port) = kalamdb_grpc_target();

    pg.batch_execute("CREATE SCHEMA IF NOT EXISTS e2e;")
        .await
        .expect("create e2e schema");
    pg.batch_execute(&format!(
        "CREATE SERVER {server_name}
            FOREIGN DATA WRAPPER pg_kalam
            OPTIONS (
                host '{grpc_host}',
                port '{grpc_port}',
                auth_header E'Bearer broken\\nvalue'
            );"
    ))
    .await
    .expect("create foreign server with invalid auth metadata");

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
        .expect_err("invalid auth_header metadata should fail before creating the foreign table");

    let message = postgres_error_text(&err);
    assert!(
        message.contains("invalid auth_header") || message.contains("metadata value"),
        "unexpected invalid auth_header error: {message}"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS e2e.{table_name};"))
        .await
        .ok();
    pg.batch_execute(&format!("DROP SERVER IF EXISTS {server_name} CASCADE;"))
        .await
        .ok();
}
