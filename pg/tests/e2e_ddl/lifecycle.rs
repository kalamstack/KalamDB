use kalam_client::{AuthProvider, KalamLinkClient};
use std::time::{Duration, Instant};

use super::common::{ensure_schema_exists, require_ddl_env, unique_name};

fn kalamdb_server_url() -> String {
    std::env::var("KALAMDB_SERVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn kalamlink_client(bearer_token: &str) -> KalamLinkClient {
    KalamLinkClient::builder()
        .base_url(kalamdb_server_url())
        .auth(AuthProvider::jwt_token(bearer_token.to_string()))
        .build()
        .expect("build KalamLink client")
}

async fn wait_for_postgres_row(
    pg: &tokio_postgres::Client,
    select_sql: &str,
    row_id: &str,
) -> tokio_postgres::Row {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if let Some(row) = pg.query_opt(select_sql, &[&row_id]).await.expect("query Postgres row") {
            return row;
        }

        if Instant::now() >= deadline {
            panic!("Postgres did not observe row '{row_id}' within timeout");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn assert_file_json_text(
    attachment_text: &str,
    expected_name: &str,
    expected_mime: &str,
    expected_size: usize,
) -> serde_json::Value {
    let attachment_json: serde_json::Value =
        serde_json::from_str(attachment_text).expect("parse Postgres jsonb text");
    assert_eq!(attachment_json["name"].as_str(), Some(expected_name));
    assert_eq!(attachment_json["mime"].as_str(), Some(expected_mime));
    assert_eq!(attachment_json["size"].as_u64(), Some(expected_size as u64));
    assert!(!attachment_json["id"].as_str().unwrap_or_default().is_empty());
    assert!(!attachment_json["sub"].as_str().unwrap_or_default().is_empty());
    assert!(!attachment_json["sha256"].as_str().unwrap_or_default().is_empty());
    attachment_json
}

#[tokio::test]
async fn e2e_ddl_create_shared_table() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("shared_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            title TEXT,
            value INTEGER
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE TABLE USING kalamdb");
    env.wait_for_kalamdb_table_exists(ns, &table).await;

    assert!(
        env.kalamdb_table_exists(ns, &table).await,
        "KalamDB table {ns}.{table} should exist after CREATE TABLE USING kalamdb"
    );

    let cols = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Created {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()), "should have 'id' column");
    assert!(cols.contains(&"title".to_string()), "should have 'title' column");
    assert!(cols.contains(&"value".to_string()), "should have 'value' column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_create_user_table() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("user_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            age INTEGER
        ) USING kalamdb WITH (type = 'user');"
    );
    pg.batch_execute(&sql).await.expect("CREATE TABLE USING kalamdb (user)");
    env.wait_for_kalamdb_table_exists(ns, &table).await;

    assert!(
        env.kalamdb_table_exists(ns, &table).await,
        "KalamDB user table {ns}.{table} should exist"
    );

    let cols = env.kalamdb_columns(ns, &table).await;
    eprintln!("[DDL] Created user table {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()), "should have 'id' column");
    assert!(cols.contains(&"name".to_string()), "should have 'name' column");
    assert!(cols.contains(&"age".to_string()), "should have 'age' column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_create_file_column_mirrors_as_jsonb() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("file_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            attachment FILE
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql)
        .await
        .expect("CREATE TABLE USING kalamdb with FILE column");
    env.wait_for_kalamdb_table_exists(ns, &table).await;

    let local_type: String = pg
        .query_one(
            "SELECT format_type(a.atttypid, a.atttypmod)
               FROM pg_attribute a
               JOIN pg_class c ON a.attrelid = c.oid
               JOIN pg_namespace n ON c.relnamespace = n.oid
              WHERE n.nspname = $1
                AND c.relname = $2
                AND a.attname = 'attachment'
                AND a.attnum > 0
                AND NOT a.attisdropped",
            &[&ns, &table],
        )
        .await
        .expect("resolve mirrored attachment column type")
        .get(0);

    assert_eq!(local_type, "jsonb");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_create_json_column_preserves_local_json_type() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("json_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            payload JSON
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql)
        .await
        .expect("CREATE TABLE USING kalamdb with JSON column");
    env.wait_for_kalamdb_table_exists(ns, &table).await;

    let local_type: String = pg
        .query_one(
            "SELECT format_type(a.atttypid, a.atttypmod)
               FROM pg_attribute a
               JOIN pg_class c ON a.attrelid = c.oid
               JOIN pg_namespace n ON c.relnamespace = n.oid
              WHERE n.nspname = $1
                AND c.relname = $2
                AND a.attname = 'payload'
                AND a.attnum > 0
                AND NOT a.attisdropped",
            &[&ns, &table],
        )
        .await
        .expect("resolve mirrored payload column type")
        .get(0);

    assert_eq!(local_type, "json");

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
#[ntest::timeout(1200)]
async fn e2e_ddl_file_column_roundtrip_via_kalamlink() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let namespace = "ddl_test";
    let table = unique_name("file_roundtrip");
    let row_id = unique_name("file_row");
    let file_name = "hello.txt";
    let file_mime = "text/plain";
    let file_bytes = b"hello from kalamlink".to_vec();

    ensure_schema_exists(&pg, namespace).await;

    let create_sql = format!(
        "CREATE TABLE {namespace}.{table} (
            id TEXT,
            attachment FILE
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&create_sql)
        .await
        .expect("CREATE TABLE USING kalamdb with FILE column for KalamLink roundtrip");
    env.wait_for_kalamdb_table_exists(namespace, &table).await;

    let client = KalamLinkClient::builder()
        .base_url(kalamdb_server_url())
        .auth(AuthProvider::jwt_token(env.bearer_token.clone()))
        .build()
        .expect("build KalamLink client");

    let insert_sql = format!(
        "INSERT INTO {namespace}.{table} (id, attachment) VALUES ('{row_id}', FILE(\"attachment\"))"
    );
    let insert_result = client
        .execute_with_files(
            &insert_sql,
            vec![("attachment", file_name, file_bytes.clone(), Some(file_mime))],
            None,
            None,
        )
        .await
        .expect("insert FILE row via KalamLink");
    assert!(insert_result.success(), "KalamLink insert should succeed");

    let select_sql = format!(
        "SELECT attachment::text,
                jsonb_typeof(attachment),
                attachment->>'name',
                attachment->>'mime',
                (attachment->>'size')::bigint,
                attachment->>'sha256'
           FROM {namespace}.{table}
          WHERE id = $1"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let row = loop {
        if let Some(row) =
            pg.query_opt(&select_sql, &[&row_id]).await.expect("query Postgres FILE row")
        {
            break row;
        }

        if Instant::now() >= deadline {
            panic!(
                "Postgres did not observe KalamLink-inserted FILE row for {}.{} within timeout",
                namespace, table
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let attachment_text: String = row.get(0);
    let attachment_kind: String = row.get(1);
    let attachment_name: String = row.get(2);
    let attachment_mime: String = row.get(3);
    let attachment_size: i64 = row.get(4);
    let attachment_sha256: String = row.get(5);

    assert_eq!(attachment_kind, "object");
    assert_eq!(attachment_name, file_name);
    assert_eq!(attachment_mime, file_mime);
    assert_eq!(attachment_size, file_bytes.len() as i64);
    assert!(!attachment_sha256.is_empty(), "sha256 should be populated");

    let attachment_json: serde_json::Value =
        serde_json::from_str(&attachment_text).expect("parse Postgres jsonb text");
    assert_eq!(attachment_json["name"].as_str(), Some(file_name));
    assert_eq!(attachment_json["mime"].as_str(), Some(file_mime));
    assert_eq!(attachment_json["size"].as_u64(), Some(file_bytes.len() as u64));
    assert!(!attachment_json["id"].as_str().unwrap_or_default().is_empty());
    assert!(!attachment_json["sub"].as_str().unwrap_or_default().is_empty());
    assert_eq!(attachment_json["sha256"].as_str(), Some(attachment_sha256.as_str()));

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {namespace}.{table};"))
        .await
        .ok();
}

#[tokio::test]
#[ntest::timeout(1200)]
async fn e2e_ddl_multiple_file_columns_roundtrip_via_kalamlink() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let namespace = "ddl_test";
    let table = unique_name("file_multi");
    let row_id = unique_name("file_multi_row");
    let avatar_name = "avatar.png";
    let avatar_mime = "image/png";
    let avatar_bytes = b"png-avatar-bytes".to_vec();
    let contract_name = "contract.pdf";
    let contract_mime = "application/pdf";
    let contract_bytes = b"pdf-contract-bytes".to_vec();

    ensure_schema_exists(&pg, namespace).await;

    let create_sql = format!(
        "CREATE TABLE {namespace}.{table} (
            id TEXT,
            avatar FILE,
            contract FILE
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&create_sql)
        .await
        .expect("CREATE TABLE USING kalamdb with multiple FILE columns");
    env.wait_for_kalamdb_table_exists(namespace, &table).await;

    let client = kalamlink_client(&env.bearer_token);
    let insert_sql = format!(
        "INSERT INTO {namespace}.{table} (id, avatar, contract) VALUES ('{row_id}', FILE(\"avatar\"), FILE(\"contract\"))"
    );
    let insert_result = client
        .execute_with_files(
            &insert_sql,
            vec![
                ("avatar", avatar_name, avatar_bytes.clone(), Some(avatar_mime)),
                ("contract", contract_name, contract_bytes.clone(), Some(contract_mime)),
            ],
            None,
            None,
        )
        .await
        .expect("insert multi-FILE row via KalamLink");
    assert!(insert_result.success(), "multi-FILE KalamLink insert should succeed");

    let select_sql = format!(
        "SELECT avatar::text, contract::text
           FROM {namespace}.{table}
          WHERE id = $1"
    );
    let row = wait_for_postgres_row(&pg, &select_sql, &row_id).await;
    let avatar_text: String = row.get(0);
    let contract_text: String = row.get(1);

    assert_file_json_text(&avatar_text, avatar_name, avatar_mime, avatar_bytes.len());
    assert_file_json_text(&contract_text, contract_name, contract_mime, contract_bytes.len());

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {namespace}.{table};"))
        .await
        .ok();
}

#[tokio::test]
#[ntest::timeout(1200)]
async fn e2e_ddl_file_update_via_kalamlink_is_visible_in_postgres() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let namespace = "ddl_test";
    let table = unique_name("file_update");
    let row_id = unique_name("file_update_row");
    let initial_file_name = "draft.txt";
    let initial_file_mime = "text/plain";
    let initial_file_bytes = b"draft file contents".to_vec();
    let updated_file_name = "final.txt";
    let updated_file_mime = "text/plain";
    let updated_file_bytes = b"final file contents with more bytes".to_vec();

    ensure_schema_exists(&pg, namespace).await;

    let create_sql = format!(
        "CREATE TABLE {namespace}.{table} (
            id TEXT,
            attachment FILE
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&create_sql)
        .await
        .expect("CREATE TABLE USING kalamdb with FILE column for update test");
    env.wait_for_kalamdb_table_exists(namespace, &table).await;

    let client = kalamlink_client(&env.bearer_token);
    let insert_sql = format!(
        "INSERT INTO {namespace}.{table} (id, attachment) VALUES ('{row_id}', FILE(\"attachment\"))"
    );
    let insert_result = client
        .execute_with_files(
            &insert_sql,
            vec![(
                "attachment",
                initial_file_name,
                initial_file_bytes.clone(),
                Some(initial_file_mime),
            )],
            None,
            None,
        )
        .await
        .expect("insert initial FILE row via KalamLink");
    assert!(insert_result.success(), "initial FILE insert should succeed");

    let select_sql = format!(
        "SELECT attachment::text
           FROM {namespace}.{table}
          WHERE id = $1"
    );
    let initial_row = wait_for_postgres_row(&pg, &select_sql, &row_id).await;
    let initial_text: String = initial_row.get(0);
    let initial_json = assert_file_json_text(
        &initial_text,
        initial_file_name,
        initial_file_mime,
        initial_file_bytes.len(),
    );
    let initial_sha256 = initial_json["sha256"]
        .as_str()
        .expect("initial sha256 should be present")
        .to_string();

    let update_sql = format!(
        "UPDATE {namespace}.{table} SET attachment = FILE(\"attachment\") WHERE id = '{row_id}'"
    );
    let update_result = client
        .execute_with_files(
            &update_sql,
            vec![(
                "attachment",
                updated_file_name,
                updated_file_bytes.clone(),
                Some(updated_file_mime),
            )],
            None,
            None,
        )
        .await
        .expect("update FILE row via KalamLink");
    assert!(update_result.success(), "FILE update should succeed");

    let deadline = Instant::now() + Duration::from_secs(5);
    let updated_json = loop {
        let row = pg
            .query_one(&select_sql, &[&row_id])
            .await
            .expect("query updated Postgres FILE row");
        let updated_text: String = row.get(0);
        let updated_json: serde_json::Value =
            serde_json::from_str(&updated_text).expect("parse updated Postgres jsonb text");

        if updated_json["name"].as_str() == Some(updated_file_name) {
            break updated_json;
        }

        if Instant::now() >= deadline {
            panic!(
                "Postgres did not observe updated FILE metadata for {}.{} within timeout",
                namespace, table
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(updated_json["name"].as_str(), Some(updated_file_name));
    assert_eq!(updated_json["mime"].as_str(), Some(updated_file_mime));
    assert_eq!(updated_json["size"].as_u64(), Some(updated_file_bytes.len() as u64));
    assert_ne!(updated_json["sha256"].as_str(), Some(initial_sha256.as_str()));

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {namespace}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_alter_add_column() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("alter_add");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            name TEXT
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE TABLE USING kalamdb");
    let cols_before = env
        .wait_for_kalamdb_columns(ns, &table, "base columns to include name", |columns| {
            columns.iter().any(|column| column == "name")
        })
        .await;
    eprintln!("[DDL] Before ALTER: columns = {cols_before:?}");
    assert!(cols_before.contains(&"name".to_string()));

    let alter_sql = format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN score INTEGER;");
    pg.batch_execute(&alter_sql).await.expect("ALTER ADD COLUMN");
    let cols_after = env
        .wait_for_kalamdb_columns(ns, &table, "added columns to include score", |columns| {
            columns.iter().any(|column| column == "score")
        })
        .await;
    eprintln!("[DDL] After ALTER ADD: columns = {cols_after:?}");
    assert!(
        cols_after.contains(&"score".to_string()),
        "KalamDB should have 'score' column after ALTER ADD COLUMN"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_alter_drop_column() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("alter_drop");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            description TEXT
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE TABLE USING kalamdb");
    let cols_before = env
        .wait_for_kalamdb_columns(ns, &table, "base columns to include description", |columns| {
            columns.iter().any(|column| column == "description")
        })
        .await;
    eprintln!("[DDL] Before DROP COLUMN: columns = {cols_before:?}");
    assert!(cols_before.contains(&"description".to_string()));

    let alter_sql = format!("ALTER FOREIGN TABLE {ns}.{table} DROP COLUMN description;");
    pg.batch_execute(&alter_sql).await.expect("ALTER DROP COLUMN");
    let cols_after = env
        .wait_for_kalamdb_columns(ns, &table, "dropped columns to exclude description", |columns| {
            !columns.iter().any(|column| column == "description")
        })
        .await;
    eprintln!("[DDL] After DROP COLUMN: columns = {cols_after:?}");
    assert!(
        !cols_after.contains(&"description".to_string()),
        "KalamDB should NOT have 'description' column after ALTER DROP COLUMN"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {ns}.{table};"))
        .await
        .ok();
}

#[tokio::test]
async fn e2e_ddl_drop_table() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("drop_tbl");
    ensure_schema_exists(&pg, ns).await;

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            data TEXT
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql).await.expect("CREATE TABLE USING kalamdb");
    env.wait_for_kalamdb_table_exists(ns, &table).await;
    assert!(env.kalamdb_table_exists(ns, &table).await, "table should exist before DROP");

    let drop_sql = format!("DROP FOREIGN TABLE {ns}.{table};");
    pg.batch_execute(&drop_sql).await.expect("DROP FOREIGN TABLE");
    env.wait_for_kalamdb_table_absent(ns, &table).await;

    assert!(
        !env.kalamdb_table_exists(ns, &table).await,
        "KalamDB table {ns}.{table} should NOT exist after DROP FOREIGN TABLE"
    );
}

#[tokio::test]
async fn e2e_ddl_drop_if_exists_no_error() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let result = pg.batch_execute("DROP FOREIGN TABLE IF EXISTS nonexistent_table_xyz;").await;
    assert!(
        result.is_ok(),
        "DROP FOREIGN TABLE IF EXISTS should not error for non-existent table"
    );
}

#[tokio::test]
async fn e2e_ddl_full_lifecycle() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = "ddl_test";
    let table = unique_name("lifecycle");
    ensure_schema_exists(&pg, ns).await;

    let create_sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            name TEXT
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&create_sql).await.expect("CREATE");
    env.wait_for_kalamdb_table_exists(ns, &table).await;
    assert!(env.kalamdb_table_exists(ns, &table).await);

    pg.batch_execute(&format!(
        "INSERT INTO {ns}.{table} (id, name) VALUES ('k1', 'Alice'), ('k2', 'Bob');"
    ))
    .await
    .expect("INSERT");

    let rows = pg
        .query(&format!("SELECT id, name FROM {ns}.{table} ORDER BY id"), &[])
        .await
        .expect("SELECT");
    assert_eq!(rows.len(), 2, "should have 2 rows");
    let first_name: &str = rows[0].get(1);
    assert_eq!(first_name, "Alice");

    pg.batch_execute(&format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN email TEXT;"))
        .await
        .expect("ALTER ADD");
    let cols = env
        .wait_for_kalamdb_columns(ns, &table, "added columns to include email", |columns| {
            columns.iter().any(|column| column == "email")
        })
        .await;
    assert!(cols.contains(&"email".to_string()), "should have email column");

    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table};"))
        .await
        .expect("DROP");
    env.wait_for_kalamdb_table_absent(ns, &table).await;
    assert!(!env.kalamdb_table_exists(ns, &table).await);
}

#[tokio::test]
async fn e2e_ddl_schema_qualified_create() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let ns = unique_name("schemans");
    let table = unique_name("sqtbl");

    pg.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {ns};"))
        .await
        .expect("CREATE SCHEMA");

    let sql = format!(
        "CREATE TABLE {ns}.{table} (
            id TEXT,
            name TEXT,
            age INTEGER
        ) USING kalamdb WITH (type = 'shared');"
    );
    pg.batch_execute(&sql)
        .await
        .expect("CREATE TABLE USING kalamdb (schema-qualified)");
    env.wait_for_kalamdb_table_exists(&ns, &table).await;

    assert!(
        env.kalamdb_table_exists(&ns, &table).await,
        "KalamDB table {ns}.{table} should exist after schema-qualified CREATE"
    );

    let cols = env
        .wait_for_kalamdb_columns(&ns, &table, "schema-qualified columns to exist", |columns| {
            columns.iter().any(|column| column == "id")
                && columns.iter().any(|column| column == "name")
                && columns.iter().any(|column| column == "age")
        })
        .await;
    eprintln!("[DDL] Schema-qualified create {ns}.{table}, columns: {cols:?}");
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"name".to_string()));
    assert!(cols.contains(&"age".to_string()));

    pg.batch_execute(&format!("ALTER FOREIGN TABLE {ns}.{table} ADD COLUMN email TEXT;"))
        .await
        .expect("ALTER ADD COLUMN (schema-qualified)");
    let cols_after = env
        .wait_for_kalamdb_columns(
            &ns,
            &table,
            "schema-qualified alter to include email",
            |columns| columns.iter().any(|column| column == "email"),
        )
        .await;
    assert!(
        cols_after.contains(&"email".to_string()),
        "should have email column after ALTER"
    );

    pg.batch_execute(&format!("DROP FOREIGN TABLE {ns}.{table};"))
        .await
        .expect("DROP FOREIGN TABLE (schema-qualified)");
    env.wait_for_kalamdb_table_absent(&ns, &table).await;
    assert!(
        !env.kalamdb_table_exists(&ns, &table).await,
        "table should not exist after DROP"
    );

    pg.batch_execute(&format!("DROP SCHEMA IF EXISTS {ns} CASCADE;")).await.ok();
}

#[tokio::test]
async fn e2e_ddl_non_kalam_server_ignored() {
    let env = require_ddl_env!();
    let pg = env.pg_connect().await;

    let result = pg
        .batch_execute(
            "DROP FOREIGN TABLE IF EXISTS dummy_file_table;
             DROP SERVER IF EXISTS dummy_server CASCADE;
             DROP FOREIGN DATA WRAPPER IF EXISTS dummy_fdw CASCADE;
             CREATE FOREIGN DATA WRAPPER dummy_fdw;
             CREATE SERVER dummy_server FOREIGN DATA WRAPPER dummy_fdw;
             CREATE FOREIGN TABLE IF NOT EXISTS dummy_file_table (line TEXT)
                 SERVER dummy_server;
             DROP FOREIGN TABLE IF EXISTS dummy_file_table;
             DROP SERVER IF EXISTS dummy_server CASCADE;
             DROP FOREIGN DATA WRAPPER IF EXISTS dummy_fdw CASCADE;",
        )
        .await;
    assert!(result.is_ok(), "DDL on non-kalam foreign tables should be silently ignored");
}
