//! Transactional quickstart validation over the real HTTP SQL API.

use std::time::Duration;

use super::test_support::consolidated_helpers::{get_count_value, unique_namespace};
use kalam_client::models::ResponseStatus;
use tokio::time::{sleep, Instant};

async fn create_messages_table(
    server: &super::test_support::http_server::HttpTestServer,
    namespace: &str,
) -> anyhow::Result<()> {
    let resp = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.messages (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED', STORAGE_ID='local')",
            namespace
        ))
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "CREATE messages table failed: {:?}",
        resp.error
    );

    Ok(())
}

async fn create_typing_events_table(
    server: &super::test_support::http_server::HttpTestServer,
    namespace: &str,
) -> anyhow::Result<()> {
    let resp = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success);

    let resp = server
        .execute_sql(&format!(
            "CREATE TABLE {}.typing_events (id BIGINT PRIMARY KEY, conversation_id BIGINT, event_type TEXT, created_at_ms BIGINT) WITH (TYPE='STREAM', TTL_SECONDS=3600)",
            namespace
        ))
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "CREATE typing_events table failed: {:?}",
        resp.error
    );

    Ok(())
}

async fn count_rows(
    server: &super::test_support::http_server::HttpTestServer,
    sql: &str,
) -> anyhow::Result<i64> {
    let resp = server.execute_sql(sql).await?;
    anyhow::ensure!(resp.status == ResponseStatus::Success, "count query failed: {:?}", resp.error);
    Ok(get_count_value(&resp, 0))
}

async fn message_row_count(
    server: &super::test_support::http_server::HttpTestServer,
    namespace: &str,
    row_id: i64,
) -> anyhow::Result<i64> {
    count_rows(
        server,
        &format!("SELECT COUNT(*) AS cnt FROM {}.messages WHERE id = {}", namespace, row_id),
    )
    .await
}

async fn typing_event_count(
    server: &super::test_support::http_server::HttpTestServer,
    namespace: &str,
) -> anyhow::Result<i64> {
    count_rows(server, &format!("SELECT COUNT(*) AS cnt FROM {}.typing_events", namespace)).await
}

async fn active_sql_batch_count(
    server: &super::test_support::http_server::HttpTestServer,
) -> anyhow::Result<i64> {
    count_rows(
        server,
        "SELECT COUNT(*) AS cnt FROM system.transactions WHERE origin = 'SqlBatch'",
    )
    .await
}

async fn wait_for_no_sql_batch_transactions(
    server: &super::test_support::http_server::HttpTestServer,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let active = active_sql_batch_count(server).await?;
        if active == 0 {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "expected no active SqlBatch transactions, found {}",
            active
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ntest::timeout(2000)]
async fn test_sql_transaction_commit_and_sequential_blocks_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("tx_http_commit");

    create_messages_table(server, &namespace).await?;

    let resp = server
        .execute_sql(&format!(
            "BEGIN; INSERT INTO {}.messages (id, name) VALUES (3001, 'rest'); INSERT INTO {}.messages (id, name) VALUES (3002, 'rest-2'); COMMIT;",
            namespace, namespace
        ))
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "commit batch failed: {:?}",
        resp.error
    );

    anyhow::ensure!(message_row_count(server, &namespace, 3001).await? == 1);
    anyhow::ensure!(message_row_count(server, &namespace, 3002).await? == 1);

    let resp = server
        .execute_sql(&format!(
            "BEGIN; INSERT INTO {}.messages (id, name) VALUES (3010, 'kept'); COMMIT; BEGIN; INSERT INTO {}.messages (id, name) VALUES (3011, 'dropped'); ROLLBACK;",
            namespace, namespace
        ))
        .await?;
    anyhow::ensure!(
        resp.status == ResponseStatus::Success,
        "sequential transaction blocks failed: {:?}",
        resp.error
    );

    anyhow::ensure!(message_row_count(server, &namespace, 3010).await? == 1);
    anyhow::ensure!(message_row_count(server, &namespace, 3011).await? == 0);
    wait_for_no_sql_batch_transactions(server).await?;

    Ok(())
}

#[tokio::test]
#[ntest::timeout(2000)]
async fn test_sql_transaction_statement_failure_rolls_back_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("tx_http_error");

    create_messages_table(server, &namespace).await?;

    let resp = server
        .execute_sql(&format!(
            "BEGIN; INSERT INTO {}.messages (id, name) VALUES (3003, 'ok'); INSERT INTO {}.messages (id, missing_col) VALUES (3004, 'bad'); COMMIT;",
            namespace, namespace
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Error, "expected error response");
    anyhow::ensure!(message_row_count(server, &namespace, 3003).await? == 0);
    anyhow::ensure!(message_row_count(server, &namespace, 3004).await? == 0);
    wait_for_no_sql_batch_transactions(server).await?;

    Ok(())
}

#[tokio::test]
#[ntest::timeout(2000)]
async fn test_sql_transaction_unclosed_request_rolls_back_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("tx_http_unclosed");

    create_messages_table(server, &namespace).await?;

    let resp = server
        .execute_sql(&format!(
            "BEGIN; INSERT INTO {}.messages (id, name) VALUES (3012, 'orphaned');",
            namespace
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Error, "expected error response");
    anyhow::ensure!(message_row_count(server, &namespace, 3012).await? == 0);
    wait_for_no_sql_batch_transactions(server).await?;

    Ok(())
}

#[tokio::test]
#[ntest::timeout(2000)]
async fn test_sql_transaction_rejects_stream_table_writes_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("tx_http_stream");

    create_typing_events_table(server, &namespace).await?;

    let resp = server
        .execute_sql(&format!(
            "BEGIN; INSERT INTO {}.typing_events (id, conversation_id, event_type, created_at_ms) VALUES (9001, 7, 'typing', 1700000000000); COMMIT;",
            namespace
        ))
        .await?;
    anyhow::ensure!(resp.status == ResponseStatus::Error, "expected error response");
    anyhow::ensure!(
        resp.error
            .as_ref()
            .map(|error| error
                .message
                .contains("stream tables are not supported inside explicit transactions"))
            .unwrap_or(false),
        "unexpected error: {:?}",
        resp.error
    );
    anyhow::ensure!(typing_event_count(server, &namespace).await? == 0);
    wait_for_no_sql_batch_transactions(server).await?;

    Ok(())
}
