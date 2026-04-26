//! Integration tests for public SQL error redaction.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;
use uuid::Uuid;

use super::test_support::TestServer;

#[actix_web::test]
#[ntest::timeout(45000)]
async fn test_non_admin_sql_errors_redact_table_details() {
    let server = TestServer::new_shared().await;
    let username = format!("sql_redaction_{}", Uuid::new_v4().simple());
    let namespace = format!("secret_ns_{}", Uuid::new_v4().simple());
    let table_name = format!("hidden_table_{}", Uuid::new_v4().simple());
    let sql = format!("SELECT * FROM {}.{}", namespace, table_name);

    server.create_user(&username, "StrongPass123!", Role::User).await;

    let user_response = server.execute_sql_as_user(&sql, &username).await;
    assert_eq!(user_response.status, ResponseStatus::Error);
    let user_error = user_response.error.expect("user response should include an error payload");
    assert_eq!(user_error.message, "SQL statement failed");
    assert!(user_error.details.is_none());
    assert!(!user_error.message.contains(&namespace));
    assert!(!user_error.message.contains(&table_name));

    let admin_response = server.execute_sql(&sql).await;
    assert_eq!(admin_response.status, ResponseStatus::Error);
    let admin_error = admin_response.error.expect("admin response should include an error payload");
    let admin_details = admin_error.details.unwrap_or_default();
    let admin_text = format!("{} {}", admin_error.message, admin_details);
    assert!(admin_text.contains(&namespace));
    assert!(admin_text.contains(&table_name));
}
