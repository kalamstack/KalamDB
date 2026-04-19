//! Mixed-privilege SQL batch authorization tests over the real HTTP SQL API.

use super::test_support::auth_helper::create_user_auth_header_default;
use super::test_support::consolidated_helpers::{get_count, unique_namespace, unique_table};
use kalam_client::models::ResponseStatus;

#[tokio::test]
#[ntest::timeout(60000)] // 60 seconds - mixed batch authorization regression test
async fn test_regular_user_batch_rejects_admin_statement_without_side_effects(
) -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    let namespace = unique_namespace("batch_auth");
    let username = unique_table("batch_user");
    let forbidden_username = unique_table("blocked_user");
    let auth = create_user_auth_header_default(server, &username).await?;

    let create_namespace = server
        .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .await?;
    anyhow::ensure!(
        create_namespace.status == ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        create_namespace.error
    );

    let create_table = server
        .execute_sql_with_auth(
            &format!(
                "CREATE TABLE {}.notes (id BIGINT PRIMARY KEY, content TEXT) WITH (TYPE='USER', STORAGE_ID='local')",
                namespace
            ),
            &auth,
        )
        .await?;
    anyhow::ensure!(
        create_table.status == ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_table.error
    );

    let batch = format!(
        "INSERT INTO {}.notes (id, content) VALUES (1, 'first'); \
         CREATE USER '{}' WITH PASSWORD 'SecurePass123!A' ROLE user; \
         INSERT INTO {}.notes (id, content) VALUES (2, 'second')",
        namespace, forbidden_username, namespace
    );

    let batch_response = server.execute_sql_with_auth(&batch, &auth).await?;
    anyhow::ensure!(
        batch_response.status == ResponseStatus::Error,
        "mixed-privilege batch should fail for regular user: {:?}",
        batch_response
    );
    anyhow::ensure!(
        batch_response.error.is_some(),
        "mixed-privilege batch should return an error payload"
    );

    let count_response = server
        .execute_sql_with_auth(
            &format!("SELECT COUNT(*) AS cnt FROM {}.notes", namespace),
            &auth,
        )
        .await?;
    anyhow::ensure!(
        count_response.status == ResponseStatus::Success,
        "post-failure row count query failed: {:?}",
        count_response.error
    );
    anyhow::ensure!(
        get_count(&count_response) == 0,
        "authorized statements in the rejected batch should not leave side effects"
    );

    let user_lookup = server
        .execute_sql(&format!(
            "SELECT COUNT(*) AS cnt FROM system.users WHERE user_id = '{}'",
            forbidden_username
        ))
        .await?;
    anyhow::ensure!(
        user_lookup.status == ResponseStatus::Success,
        "system.users lookup failed: {:?}",
        user_lookup.error
    );
    anyhow::ensure!(
        get_count(&user_lookup) == 0,
        "forbidden CREATE USER statement should not create a user"
    );

    Ok(())
}
