//! SQL-based user management commands over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;

// TODO: Cannot migrate to get_global_server() pattern yet.
// This test requires a config override: `enforce_password_complexity = true`.
// Once we support per-test config overrides for the global server, this can be migrated.
#[tokio::test]
#[ntest::timeout(300000)] // 300 seconds - user SQL commands test (fresh server + bcrypt is slow in debug)
#[ignore = "Hangs in debug mode: fresh server startup + bcrypt cost 12 exceeds timeout. Run with \
            --release."]
async fn test_user_sql_commands_over_http() {
    super::test_support::http_server::with_http_test_server_config(
        |cfg| {
            cfg.auth.enforce_password_complexity = true;
        },
        |server| {
            Box::pin(async move {
                let admin_auth = server.bearer_auth_header("root")?;

                // CREATE USER with password
                let sql = "CREATE USER 'alice' WITH PASSWORD 'SecurePass123!' ROLE developer EMAIL 'alice@example.com'";
                let result = server.execute_sql_with_auth(sql, &admin_auth).await?;
                assert_eq!(
                    result.status,
                    ResponseStatus::Success,
                    "CREATE USER should succeed: {:?}",
                    result.error
                );

                let query = "SELECT * FROM system.users WHERE user_id = 'alice'";
                let result = server.execute_sql_with_auth(query, &admin_auth).await?;
                assert!(!result.results.is_empty());
                let rows = result.rows_as_maps();
                assert_eq!(rows.len(), 1);
                let row = &rows[0];
                assert_eq!(row.get("user_id").unwrap().as_str().unwrap(), "alice");
                assert_eq!(row.get("auth_type").unwrap().as_str().unwrap(), "password");
                // developer -> service (current mapping)
                assert_eq!(row.get("role").unwrap().as_str().unwrap(), "service");
                assert_eq!(
                    row.get("email").unwrap().as_str().unwrap(),
                    "alice@example.com"
                );

                // CREATE USER with OAuth
                let sql = r#"CREATE USER 'bob' WITH OAUTH '{"provider": "google", "subject": "12345"}' ROLE viewer EMAIL 'bob@example.com'"#;
                let result = server.execute_sql_with_auth(sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Success);

                let query = "SELECT * FROM system.users WHERE user_id = 'bob'";
                let result = server.execute_sql_with_auth(query, &admin_auth).await?;
                let rows = result.rows_as_maps();
                let row = &rows[0];
                assert_eq!(row.get("user_id").unwrap().as_str().unwrap(), "bob");
                assert_eq!(row.get("auth_type").unwrap().as_str().unwrap(), "oauth");
                assert_eq!(row.get("role").unwrap().as_str().unwrap(), "user");

                // Authorization checks (regular user cannot CREATE USER)
                let create_regular =
                    "CREATE USER 'regular_user' WITH PASSWORD 'Pass123!A' ROLE user";
                let _ = server
                    .execute_sql_with_auth(create_regular, &admin_auth)
                    .await?;

                let regular_auth = server.bearer_auth_header("regular_user")?;
                let sql = "CREATE USER 'charlie' WITH PASSWORD 'TestPass123!B' ROLE user";
                let result = server.execute_sql_with_auth(sql, &regular_auth).await?;
                assert_eq!(
                    result.status,
                    ResponseStatus::Error,
                    "Regular user should not be able to create users"
                );

                // ALTER USER SET PASSWORD
                let create_sql = "CREATE USER 'dave' WITH PASSWORD 'OldPass123!C' ROLE user";
                server.execute_sql_with_auth(create_sql, &admin_auth).await?;

                let query = "SELECT password_hash FROM system.users WHERE user_id = 'dave'";
                let result = server.execute_sql_with_auth(query, &admin_auth).await?;
                let rows = result.rows_as_maps();
                let old_hash = rows[0]
                    .get("password_hash")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string();

                let alter_sql = "ALTER USER 'dave' SET PASSWORD 'NewPass456!D'";
                let result = server.execute_sql_with_auth(alter_sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Success);

                let result = server.execute_sql_with_auth(query, &admin_auth).await?;
                let rows = result.rows_as_maps();
                let new_hash = rows[0]
                    .get("password_hash")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string();
                assert_ne!(old_hash, new_hash);

                // ALTER USER SET ROLE
                let create_sql = "CREATE USER 'eve' WITH PASSWORD 'Password123!E' ROLE user";
                server.execute_sql_with_auth(create_sql, &admin_auth).await?;

                let alter_sql = "ALTER USER 'eve' SET ROLE dba";
                let result = server.execute_sql_with_auth(alter_sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Success);

                let query = "SELECT role FROM system.users WHERE user_id = 'eve'";
                let result = server.execute_sql_with_auth(query, &admin_auth).await?;
                let rows = result.rows_as_maps();
                let role = rows[0].get("role").unwrap().as_str().unwrap();
                assert_eq!(role, "dba");

                // DROP USER soft delete
                let create_sql = "CREATE USER 'frank' WITH PASSWORD 'Password123!F' ROLE user";
                server.execute_sql_with_auth(create_sql, &admin_auth).await?;

                let drop_sql = "DROP USER 'frank'";
                let result = server.execute_sql_with_auth(drop_sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Success);

                let query_deleted = "SELECT deleted_at FROM system.users WHERE user_id = 'frank' AND deleted_at IS NOT NULL";
                let result = server.execute_sql_with_auth(query_deleted, &admin_auth).await?;
                let rows = result.rows_as_maps();
                assert_eq!(rows.len(), 1);
                assert!(!rows[0].get("deleted_at").unwrap().is_null());

                // Weak password rejection (when enforce_password_complexity = true)
                let weak_passwords = [
                    "password",
                    "123456",
                    "qwerty",
                    "admin",
                    "letmein",
                    "welcome",
                    "monkey",
                ];
                for weak_pass in weak_passwords {
                    let sql = format!(
                        "CREATE USER 'weak_user_{}' WITH PASSWORD '{}' ROLE user",
                        weak_pass, weak_pass
                    );
                    let result = server.execute_sql_with_auth(&sql, &admin_auth).await?;
                    assert_eq!(result.status, ResponseStatus::Error);
                }

                // Duplicate user error
                let create_sql =
                    "CREATE USER 'duplicate_test' WITH PASSWORD 'Password123!G' ROLE user";
                server.execute_sql_with_auth(create_sql, &admin_auth).await?;
                let result = server.execute_sql_with_auth(create_sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Error);

                // Not found
                let alter_sql = "ALTER USER 'nonexistent_user' SET ROLE dba";
                let result = server.execute_sql_with_auth(alter_sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Error);

                // DROP USER IF EXISTS
                let drop_sql = "DROP USER IF EXISTS 'user_that_never_existed'";
                let result = server.execute_sql_with_auth(drop_sql, &admin_auth).await?;
                assert_eq!(result.status, ResponseStatus::Success);

                Ok(())
            })
        },
    )
    .await
    .expect("with_http_test_server_config");
}
