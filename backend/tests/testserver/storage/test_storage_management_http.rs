//! Storage management smoke tests over the real HTTP SQL API.
//!
//! Consolidates the most important coverage from the legacy integration suite:
//! - Default `local` storage exists
//! - CREATE/ALTER/DROP STORAGE
//! - Template validation
//! - Prevent dropping in-use storage

use kalam_client::models::ResponseStatus;

use super::test_support::consolidated_helpers::{unique_namespace, unique_table};

#[tokio::test]
async fn test_storage_management_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::start_http_test_server().await?;

    let result: anyhow::Result<()> = async {
        // Default storage exists
        let resp = server.execute_sql("SHOW STORAGES").await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "SHOW STORAGES failed: {:?}",
            resp.error
        );
        let rows = resp.results.first().map(|r| r.rows_as_maps()).unwrap_or_default();
        anyhow::ensure!(
            rows.iter()
                .any(|r| r.get("storage_id").and_then(|v| v.as_str()) == Some("local")),
            "expected local storage in SHOW STORAGES"
        );

        let storage_id = unique_table("archive");
        let base = server.data_path().join("storages").join(&storage_id);

        // CREATE STORAGE (filesystem)
        let sql = format!(
            r#"
                CREATE STORAGE {storage_id}
                TYPE filesystem
                NAME 'Archive Storage'
                DESCRIPTION 'Cold storage for tests'
                PATH '{path}'
                SHARED_TABLES_TEMPLATE 'shared/{{namespace}}/{{tableName}}'
                USER_TABLES_TEMPLATE 'users/{{namespace}}/{{tableName}}/{{userId}}'
                "#,
            path = base.display()
        );

        let resp = server.execute_sql(&sql).await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "CREATE STORAGE failed: {:?}",
            resp.error
        );

        // Verify system.storages row
        let resp = server
            .execute_sql(&format!(
                "SELECT storage_id, storage_type, storage_name FROM system.storages WHERE \
                 storage_id = '{}'",
                storage_id
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        let rows = resp.rows_as_maps();
        anyhow::ensure!(rows.len() == 1);
        anyhow::ensure!(rows[0].get("storage_type").and_then(|v| v.as_str()) == Some("filesystem"));

        // Duplicate storage_id should error
        let resp = server
            .execute_sql(
                r#"
                    CREATE STORAGE local
                    TYPE filesystem
                    NAME 'Dup'
                    PATH '/tmp'
                    SHARED_TABLES_TEMPLATE 'shared/{namespace}/{tableName}'
                    USER_TABLES_TEMPLATE 'user/{namespace}/{tableName}/{userId}'
                    "#,
            )
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Error);

        // Template validation should error (missing {userId})
        let bad_id = unique_table("badtpl");
        let bad_path = server.data_path().join("storages").join(&bad_id);
        let resp = server
            .execute_sql(&format!(
                r#"
                    CREATE STORAGE {bad_id}
                    TYPE filesystem
                    NAME 'Bad Templates'
                    PATH '{path}'
                    SHARED_TABLES_TEMPLATE 'shared/{{namespace}}/{{tableName}}'
                    USER_TABLES_TEMPLATE 'users/{{namespace}}/{{tableName}}'
                    "#,
                path = bad_path.display()
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Error);

        // ALTER STORAGE
        let resp = server
            .execute_sql(&format!(
                "ALTER STORAGE {} SET NAME 'Archive Storage Updated'",
                storage_id
            ))
            .await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "ALTER STORAGE failed: {:?}",
            resp.error
        );

        let resp = server
            .execute_sql(&format!(
                "SELECT storage_name FROM system.storages WHERE storage_id = '{}'",
                storage_id
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);
        let row = resp
            .results
            .first()
            .and_then(|r| r.row_as_map(0))
            .ok_or_else(|| anyhow::anyhow!("Missing storage row"))?;
        anyhow::ensure!(
            row.get("storage_name").and_then(|v| v.as_str()) == Some("Archive Storage Updated")
        );

        // Prevent dropping an in-use storage
        let ns = unique_namespace("stg");
        let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server
            .execute_sql(&format!(
                "CREATE TABLE {}.t (id INT PRIMARY KEY, v TEXT) WITH (TYPE='SHARED', \
                 STORAGE_ID='{}')",
                ns, storage_id
            ))
            .await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server.execute_sql(&format!("DROP STORAGE {}", storage_id)).await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Error,
            "expected DROP STORAGE in-use to error"
        );

        // After dropping table, dropping storage should succeed
        let resp = server.execute_sql(&format!("DROP TABLE {}.t", ns)).await?;
        anyhow::ensure!(resp.status == ResponseStatus::Success);

        let resp = server.execute_sql(&format!("DROP STORAGE {}", storage_id)).await?;
        anyhow::ensure!(
            resp.status == ResponseStatus::Success,
            "DROP STORAGE failed: {:?}",
            resp.error
        );
        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}
