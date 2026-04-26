//! Stream table DML checks over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;

use super::test_support::consolidated_helpers::unique_namespace;

async fn create_stream_table(
    server: &super::test_support::http_server::HttpTestServer,
    namespace: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    let create_sql = format!(
        r#"CREATE TABLE {}.{} (
            id INT,
            event_type VARCHAR,
            data VARCHAR
        ) WITH (
            TYPE = 'STREAM',
            TTL_SECONDS = 3600
        )"#,
        namespace, table_name
    );

    let response = server.execute_sql(&create_sql).await?;
    anyhow::ensure!(
        response.status == ResponseStatus::Success,
        "Failed to create stream table: {:?}",
        response.error
    );
    Ok(())
}

#[tokio::test]
async fn test_stream_tables_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let ns = unique_namespace("test_st");

    let response = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_eq!(response.status, ResponseStatus::Success);

    create_stream_table(server, &ns, "events").await?;

    // Basic insert
    let response = server
        .execute_sql(&format!(
            "INSERT INTO {}.events (id, event_type, data) VALUES (1, 'user_login', 'user_123')",
            ns
        ))
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);

    // Multiple inserts
    for (id, et, data) in [
        (2, "page_view", "page_home"),
        (3, "user_logout", "user_123"),
    ] {
        let response = server
            .execute_sql(&format!(
                "INSERT INTO {}.events (id, event_type, data) VALUES ({}, '{}', '{}')",
                ns, id, et, data
            ))
            .await?;
        assert_eq!(response.status, ResponseStatus::Success);
    }

    // Insert without system columns (stream tables shouldn't require them)
    let response = server
        .execute_sql(&format!(
            "INSERT INTO {}.events (id, event_type, data) VALUES (4, 'test_event', 'test_data')",
            ns
        ))
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);
    Ok(())
}
