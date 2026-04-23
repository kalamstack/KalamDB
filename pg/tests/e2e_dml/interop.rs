use super::common::{
    await_user_shard_leader, count_rows, create_shared_kalam_table, create_user_kalam_table,
    delete_all, retry_transient_user_leader_error, set_user_id, unique_name, TestEnv,
};

struct KalamTestUser {
    username: String,
    user_id: String,
}

fn sql_first_cell_string(result: &serde_json::Value) -> Option<String> {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|entry| entry["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.first())
        .and_then(|value| value.as_str().map(ToString::to_string))
}

fn sql_first_cell_i64(result: &serde_json::Value) -> Option<i64> {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|entry| entry["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.first())
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|raw| i64::try_from(raw).ok()))
                .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
        })
}

async fn create_kalam_test_user(env: &TestEnv, prefix: &str) -> KalamTestUser {
    let user_id = unique_name(prefix);
    let password = format!("pw_{user_id}");

    env.kalamdb_sql(&format!("CREATE USER '{user_id}' WITH PASSWORD '{password}' ROLE user"))
        .await;

    let lookup = env
        .kalamdb_sql(&format!("SELECT user_id FROM system.users WHERE user_id = '{user_id}'"))
        .await;
    let user_id = sql_first_cell_string(&lookup)
        .unwrap_or_else(|| panic!("expected user_id for Kalam test user {user_id}"));

    KalamTestUser {
        username: user_id.clone(),
        user_id,
    }
}

async fn wait_for_execute_as_user_count(
    env: &TestEnv,
    user_id: &str,
    qualified_table: &str,
    row_id: &str,
    expected: i64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        let result = env
            .kalamdb_sql(&format!(
                "EXECUTE AS USER '{user_id}' (SELECT COUNT(*) FROM {qualified_table} WHERE id = '{row_id}')"
            ))
            .await;
        let count = sql_first_cell_i64(&result).unwrap_or_default();
        if count == expected {
            return;
        }

        if std::time::Instant::now() >= deadline {
            panic!(
                "EXECUTE AS USER '{user_id}' expected count {expected} for {qualified_table}.{row_id}, got {count}"
            );
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn e2e_cross_verify_fdw_to_rest() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("items");
    let qualified_table = format!("e2e.{table}");

    create_shared_kalam_table(&pg, &table, "id TEXT, title TEXT, value INTEGER").await;

    pg.batch_execute(&format!(
        "INSERT INTO {qualified_table} (id, title, value) VALUES ('xv-rest', 'CrossVerify', 999);"
    ))
    .await
    .expect("cross-verify insert");

    let result = env
        .kalamdb_sql(&format!(
            "SELECT id, title, value FROM {qualified_table} WHERE id = 'xv-rest'"
        ))
        .await;
    let result_text = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_text.contains("xv-rest"),
        "FDW-inserted row should be visible via REST API: {result_text}"
    );

    pg.execute(&format!("DELETE FROM {qualified_table} WHERE id = $1"), &[&"xv-rest"])
        .await
        .expect("cleanup cross-verify row");
}

#[tokio::test]
#[ntest::timeout(4000)]
async fn e2e_dml_changes_are_visible_in_kalamdb() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("direct_sync");

    create_shared_kalam_table(&pg, &table, "id TEXT, title TEXT, value INTEGER").await;

    pg.execute(
        &format!("INSERT INTO e2e.{table} (id, title, value) VALUES ($1, $2, $3)"),
        &[&"sync-1", &"Created in PG", &10_i32],
    )
    .await
    .expect("insert should succeed");

    let inserted = env
        .kalamdb_sql(&format!("SELECT id, title, value FROM e2e.{table} WHERE id = 'sync-1'"))
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
        .kalamdb_sql(&format!("SELECT id, title, value FROM e2e.{table} WHERE id = 'sync-1'"))
        .await;
    let updated_text = serde_json::to_string(&updated).unwrap_or_default();
    assert!(
        updated_text.contains("Updated in PG") && updated_text.contains("99"),
        "update should be visible in KalamDB: {updated_text}"
    );

    pg.execute(&format!("DELETE FROM e2e.{table} WHERE id = $1"), &[&"sync-1"])
        .await
        .expect("delete should succeed");

    let deleted = env
        .kalamdb_sql(&format!("SELECT COUNT(*) FROM e2e.{table} WHERE id = 'sync-1'"))
        .await;
    let deleted_count = sql_first_cell_i64(&deleted).unwrap_or_default();
    assert_eq!(deleted_count, 0, "deleted row should no longer be visible in KalamDB");
}

#[tokio::test]
#[ntest::timeout(7000)]
async fn e2e_select_filters_and_postgres_join_work() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("join_items");

    create_shared_kalam_table(&pg, &table, "id TEXT, title TEXT, value INTEGER").await;
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

#[tokio::test]
#[ntest::timeout(4000)]
async fn e2e_shared_tables_can_join_each_other_in_postgres() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let customers_table = unique_name("join_shared_customers");
    let orders_table = unique_name("join_shared_orders");

    create_shared_kalam_table(&pg, &customers_table, "id TEXT, name TEXT").await;
    create_shared_kalam_table(&pg, &orders_table, "id TEXT, customer_id TEXT, total INTEGER").await;

    pg.batch_execute(&format!(
        "INSERT INTO e2e.{customers_table} (id, name) VALUES \
         ('c1', 'Alice'), \
         ('c2', 'Bob');"
    ))
    .await
    .expect("insert shared customers");

    pg.batch_execute(&format!(
        "INSERT INTO e2e.{orders_table} (id, customer_id, total) VALUES \
         ('o1', 'c1', 15), \
         ('o2', 'c1', 20), \
         ('o3', 'c2', 30);"
    ))
    .await
    .expect("insert shared orders");

    let rows = pg
        .query(
            &format!(
                "SELECT c.id, c.name, o.id, o.total
                   FROM e2e.{customers_table} AS c
                   JOIN e2e.{orders_table} AS o ON o.customer_id = c.id
                  ORDER BY c.id, o.id"
            ),
            &[],
        )
        .await
        .expect("join shared tables query");

    assert_eq!(rows.len(), 3, "expected three joined rows across shared tables");
    let first: (&str, &str, &str, i32) =
        (rows[0].get(0), rows[0].get(1), rows[0].get(2), rows[0].get(3));
    let second: (&str, &str, &str, i32) =
        (rows[1].get(0), rows[1].get(1), rows[1].get(2), rows[1].get(3));
    let third: (&str, &str, &str, i32) =
        (rows[2].get(0), rows[2].get(1), rows[2].get(2), rows[2].get(3));
    assert_eq!(first, ("c1", "Alice", "o1", 15));
    assert_eq!(second, ("c1", "Alice", "o2", 20));
    assert_eq!(third, ("c2", "Bob", "o3", 30));
}

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
         CREATE TABLE {table} (
             id TEXT,
             title TEXT,
             value INTEGER
         ) USING kalamdb WITH (type = 'shared');"
    ))
    .await
    .expect("create mirrored Kalam table via search_path");

    pg.execute(
        &format!("INSERT INTO {schema}.{table} (id, title, value) VALUES ($1, $2, $3)"),
        &[&"spath-1", &"From search_path", &7_i32],
    )
    .await
    .expect("insert through schema-mirrored Kalam table");

    let rows = pg
        .query(
            &format!("SELECT id, title, value FROM {schema}.{table} WHERE id = $1"),
            &[&"spath-1"],
        )
        .await
        .expect("select through schema-mirrored Kalam table");
    assert_eq!(rows.len(), 1, "expected one mirrored row through PostgreSQL");

    let result = env
        .kalamdb_sql(&format!("SELECT id, title, value FROM {schema}.{table} WHERE id = 'spath-1'"))
        .await;
    let result_text = serde_json::to_string(&result).unwrap_or_default();
    assert!(
        result_text.contains("spath-1") && result_text.contains("From search_path"),
        "search_path-mirrored Kalam table should write into KalamDB namespace {schema}: {result_text}"
    );

    pg.batch_execute(&format!(
        "RESET search_path;
         DROP FOREIGN TABLE IF EXISTS {schema}.{table};
         DROP SCHEMA IF EXISTS {schema} CASCADE;"
    ))
    .await
    .ok();
}

#[tokio::test]
#[ntest::timeout(4000)]
async fn e2e_user_tables_can_join_each_other_in_postgres() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let user_id = unique_name("join_user");
    let profiles_table = unique_name("join_user_profiles");
    let memberships_table = unique_name("join_user_memberships");

    create_user_kalam_table(&pg, &profiles_table, "id TEXT, display_name TEXT").await;
    create_user_kalam_table(&pg, &memberships_table, "id TEXT, profile_id TEXT, plan TEXT").await;

    await_user_shard_leader(&user_id).await;
    set_user_id(&pg, &user_id).await;

    let insert_profiles_sql = format!(
        "INSERT INTO e2e.{profiles_table} (id, display_name) VALUES \
         ('u1', 'Alice'), \
         ('u2', 'Bob');"
    );
    retry_transient_user_leader_error("insert user profiles", || {
        pg.batch_execute(&insert_profiles_sql)
    })
    .await;

    let insert_memberships_sql = format!(
        "INSERT INTO e2e.{memberships_table} (id, profile_id, plan) VALUES \
         ('m1', 'u1', 'pro'), \
         ('m2', 'u2', 'team');"
    );
    retry_transient_user_leader_error("insert user memberships", || {
        pg.batch_execute(&insert_memberships_sql)
    })
    .await;

    let join_sql = format!(
        "SELECT p.id, p.display_name, m.plan
           FROM e2e.{profiles_table} AS p
           JOIN e2e.{memberships_table} AS m ON m.profile_id = p.id
          ORDER BY p.id"
    );
    let rows =
        retry_transient_user_leader_error("join user tables query", || pg.query(&join_sql, &[]))
            .await;

    assert_eq!(rows.len(), 2, "expected two joined rows across user tables");
    let first: (&str, &str, &str) = (rows[0].get(0), rows[0].get(1), rows[0].get(2));
    let second: (&str, &str, &str) = (rows[1].get(0), rows[1].get(1), rows[1].get(2));
    assert_eq!(first, ("u1", "Alice", "pro"));
    assert_eq!(second, ("u2", "Bob", "team"));
}

#[tokio::test]
#[ntest::timeout(2500)]
async fn e2e_user_table_explicit_userid_routes_to_target_user() {
    let env = TestEnv::global().await;
    let pg = env.pg_connect().await;
    let table = unique_name("userid_route");
    let qualified_table = format!("e2e.{table}");
    let row_id = unique_name("uid_row");
    let writer = create_kalam_test_user(env, "pg_writer_user").await;
    let target = create_kalam_test_user(env, "pg_target_user").await;

    create_user_kalam_table(&pg, &table, "id TEXT, body TEXT").await;

    await_user_shard_leader(&writer.user_id).await;
    await_user_shard_leader(&target.user_id).await;

    set_user_id(&pg, &writer.user_id).await;
    let insert_sql = format!(
        "INSERT INTO {qualified_table} (id, body, _userid) VALUES ('{row_id}', 'routed-via-explicit-userid', '{}')",
        target.user_id
    );

    retry_transient_user_leader_error("explicit _userid insert", || pg.batch_execute(&insert_sql))
        .await;

    let writer_count = count_rows(&pg, &qualified_table, Some(&format!("id = '{row_id}'"))).await;
    assert_eq!(writer_count, 0, "writer session should not see row routed to explicit _userid");

    set_user_id(&pg, &target.user_id).await;
    let target_count = count_rows(&pg, &qualified_table, Some(&format!("id = '{row_id}'"))).await;
    assert_eq!(target_count, 1, "target session should see explicitly routed row");

    wait_for_execute_as_user_count(env, &target.user_id, &qualified_table, &row_id, 1).await;
    wait_for_execute_as_user_count(env, &writer.user_id, &qualified_table, &row_id, 0).await;

    let root_result = env
        .kalamdb_sql(&format!("SELECT COUNT(*) FROM {qualified_table} WHERE id = '{row_id}'"))
        .await;
    let root_count = sql_first_cell_i64(&root_result).unwrap_or_default();
    assert_eq!(root_count, 1, "root query should confirm the routed row exists");

    env.kalamdb_sql(&format!("DROP USER IF EXISTS '{}'", writer.username)).await;
    env.kalamdb_sql(&format!("DROP USER IF EXISTS '{}'", target.username)).await;
}
