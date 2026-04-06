//! Naming validation tests over the real HTTP SQL API.

use super::test_support::consolidated_helpers::unique_namespace;
use kalam_client::models::ResponseStatus;

#[tokio::test]
#[ntest::timeout(60000)] // 60 seconds - naming validation test
async fn test_naming_validation_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    // Reserved namespace names
    let reserved_names = ["system", "sys", "root", "kalamdb"];
    for name in reserved_names {
        let sql = format!("CREATE NAMESPACE {}", name);
        let response = server.execute_sql(&sql).await?;
        assert_eq!(
            response.status,
            ResponseStatus::Error,
            "Should reject reserved namespace name '{}'",
            name
        );
    }

    // Valid namespace names
    let valid_names = [
        unique_namespace("validns1"),
        unique_namespace("validns2"),
        unique_namespace("validns3"),
    ];
    for name in valid_names {
        let sql = format!("CREATE NAMESPACE {}", name);
        let response = server.execute_sql(&sql).await?;
        assert_eq!(
            response.status,
            ResponseStatus::Success,
            "Should accept valid namespace name '{}'",
            name
        );
        let _ = server.execute_sql(&format!("DROP NAMESPACE {}", name)).await;
    }

    // Reserved column names
    let test_cols = unique_namespace("test_cols");
    let response = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", test_cols))
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let reserved_columns = ["_seq", "_deleted"];
    for col_name in reserved_columns {
        let table_name = format!("test_{}", col_name.replace('_', ""));
        let sql = format!(
            "CREATE TABLE {}.{} ({} TEXT PRIMARY KEY) WITH (TYPE = 'USER')",
            test_cols, table_name, col_name
        );
        let response = server.execute_sql(&sql).await?;
        assert_eq!(
            response.status,
            ResponseStatus::Error,
            "Should reject reserved column name '{}'",
            col_name
        );
    }
    let _ = server.execute_sql(&format!("DROP NAMESPACE {}", test_cols)).await;

    // Valid column names
    let ns = unique_namespace("test_valid_cols");
    let response = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let valid_columns = [("user_id", "users"), ("firstName", "profiles")];
    for (col_name, table_name) in valid_columns {
        let sql = format!(
            "CREATE TABLE {}.{} ({} TEXT PRIMARY KEY) WITH (TYPE = 'USER')",
            ns, table_name, col_name
        );
        let response = server.execute_sql(&sql).await?;
        assert_eq!(
            response.status,
            ResponseStatus::Success,
            "Should accept valid column name '{}'",
            col_name
        );
    }
    let _ = server.execute_sql(&format!("DROP NAMESPACE {}", ns)).await;

    // No auto id injection / basic CRUD
    let test_no_id = unique_namespace("test_no_id");
    let response = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", test_no_id))
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let sql = format!(
        "CREATE TABLE {}.messages (message TEXT PRIMARY KEY, content TEXT) WITH (TYPE = 'USER')",
        test_no_id
    );
    let response = server.execute_sql(&sql).await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let insert_sql = format!(
        "INSERT INTO {}.messages (message, content) VALUES ('msg1', 'Hello')",
        test_no_id
    );
    let response = server.execute_sql(&insert_sql).await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let response = server
        .execute_sql(&format!("SELECT message, content FROM {}.messages", test_no_id))
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let _ = server.execute_sql(&format!("DROP NAMESPACE {}", test_no_id)).await;

    // Users can use "id" as a column name
    let test_user_id = unique_namespace("test_user_id");
    let response = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", test_user_id))
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);
    let sql = format!(
        "CREATE TABLE {}.products (id TEXT PRIMARY KEY, name TEXT) WITH (TYPE = 'USER')",
        test_user_id
    );
    let response = server.execute_sql(&sql).await?;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Should allow user-defined 'id' column"
    );
    let _ = server.execute_sql(&format!("DROP NAMESPACE {}", test_user_id)).await;
    Ok(())
}
