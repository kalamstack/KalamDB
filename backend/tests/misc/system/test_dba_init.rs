//! Integration tests for bootstrap-managed dba.* tables.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::models::UserId;
use kalamdb_dba::{
    models::{NotificationRow, StatsRow},
    DbaRegistry,
};
use ntest::timeout;

use super::test_support::TestServer;

#[tokio::test]
#[timeout(30000)]
async fn test_dba_namespace_and_tables_created_on_startup() {
    let server = TestServer::new_shared().await;

    let namespace_response = server
        .execute_sql("SELECT namespace_id FROM system.namespaces WHERE namespace_id = 'dba'")
        .await;
    assert_eq!(
        namespace_response.status,
        ResponseStatus::Success,
        "Failed to query system.namespaces: {:?}",
        namespace_response.error
    );
    assert_eq!(
        namespace_response.rows_as_maps().len(),
        1,
        "Expected dba namespace to be present on startup"
    );

    let tables_response = server
        .execute_sql(
            "SELECT table_name, table_type FROM system.schemas WHERE namespace_id = 'dba' AND \
             is_latest = true ORDER BY table_name",
        )
        .await;
    assert_eq!(
        tables_response.status,
        ResponseStatus::Success,
        "Failed to query system.schemas: {:?}",
        tables_response.error
    );

    let rows = tables_response.rows_as_maps();
    assert_eq!(rows.len(), 2, "Expected 2 dba bootstrap tables");
    assert_eq!(
        rows[0].get("table_name").and_then(|value| value.as_str()),
        Some("notifications")
    );
    assert_eq!(rows[1].get("table_name").and_then(|value| value.as_str()), Some("stats"));

    let stats_response = server
        .execute_sql(
            "SELECT metric_name FROM dba.stats WHERE metric_name = 'memory_usage_mb' LIMIT 1",
        )
        .await;
    assert_eq!(stats_response.status, ResponseStatus::Success);
    assert_eq!(
        stats_response.rows_as_maps().len(),
        1,
        "Expected startup stats snapshot to be recorded"
    );
}

#[tokio::test]
#[timeout(30000)]
async fn test_dba_repositories_write_through_normal_table_paths() {
    let server = TestServer::new_shared().await;
    let dba = DbaRegistry::new(server.app_context.clone());

    let sampled_at = chrono::Utc::now().timestamp_millis();

    let statistic = StatsRow {
        id: format!("node-1:memory_usage_mb:{}", sampled_at),
        node_id: "node-1".to_string(),
        metric_name: "memory_usage_mb".to_string(),
        metric_value: 512.0,
        metric_unit: Some("MB".to_string()),
        sampled_at,
    };
    dba.stats().upsert(statistic).await.expect("upsert stat sample");

    let notification = NotificationRow {
        id: "notif-1".to_string(),
        user_id: UserId::new("dba_repo_user"),
        title: "Background job finished".to_string(),
        body: Some("Flush completed successfully".to_string()),
        is_read: false,
        created_at: chrono::Utc::now().timestamp_millis(),
        updated_at: chrono::Utc::now().timestamp_millis(),
    };
    dba.notifications()
        .upsert(notification.clone())
        .await
        .expect("upsert notification");

    let statistics_response = server
        .execute_sql(
            "SELECT metric_name, metric_value FROM dba.stats WHERE metric_name = \
             'memory_usage_mb' AND node_id = 'node-1'",
        )
        .await;
    assert_eq!(statistics_response.status, ResponseStatus::Success);
    assert_eq!(statistics_response.rows_as_maps().len(), 1);

    let notification_response = server
        .execute_sql("SELECT id, title FROM dba.notifications WHERE id = 'notif-1'")
        .await;
    assert_eq!(notification_response.status, ResponseStatus::Success);
    assert_eq!(notification_response.rows_as_maps().len(), 1);
}
