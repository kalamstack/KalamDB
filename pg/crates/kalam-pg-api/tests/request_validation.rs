use datafusion_common::ScalarValue;
use kalam_pg_api::{InsertRequest, RemoteSessionContext, ScanRequest, TenantContext};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{NamespaceId, TableName, UserId};
use kalamdb_commons::{TableId, TableType};

#[test]
fn user_table_scan_requires_user_id() {
    let request = ScanRequest::new(
        TableId::new(NamespaceId::new("app"), TableName::new("messages")),
        TableType::User,
        TenantContext::anonymous(),
    );

    let err = request.validate().expect_err("user tables should require user context");
    assert!(err.to_string().contains("user_id"));
}

#[test]
fn shared_table_scan_allows_missing_user_id() {
    let request = ScanRequest::new(
        TableId::new(NamespaceId::new("app"), TableName::new("messages")),
        TableType::Shared,
        TenantContext::anonymous(),
    );

    assert!(request.validate().is_ok());
}

#[test]
fn tenant_context_uses_explicit_user_id() {
    let tenant = TenantContext::with_user_id(UserId::new("u_123"));
    assert_eq!(tenant.effective_user_id(), Some(&UserId::new("u_123")));
}

#[test]
fn user_table_insert_requires_user_id() {
    let request = InsertRequest::new(
        TableId::new(NamespaceId::new("app"), TableName::new("messages")),
        TableType::User,
        TenantContext::anonymous(),
        vec![Row::from_vec(vec![(
            "id".to_string(),
            ScalarValue::Int32(Some(1)),
        )])],
    );

    let err = request.validate().expect_err("user table inserts should require user context");
    assert!(err.to_string().contains("user_id"));
}

#[test]
fn insert_requires_rows() {
    let request = InsertRequest::new(
        TableId::new(NamespaceId::new("app"), TableName::new("messages")),
        TableType::Shared,
        TenantContext::anonymous(),
        Vec::new(),
    );

    let err = request
        .validate()
        .expect_err("insert requests without rows should fail validation");
    assert!(err.to_string().contains("at least one row"));
}

#[test]
fn remote_session_requires_non_empty_session_id() {
    let err = RemoteSessionContext::new("   ", Some("app"), None::<String>)
        .expect_err("empty remote session ids must be rejected");
    assert!(err.to_string().contains("session_id"));
}

#[test]
fn remote_session_requires_non_empty_current_schema() {
    let err = RemoteSessionContext::new("pg-backend-1", Some("   "), None::<String>)
        .expect_err("empty schemas must be rejected");
    assert!(err.to_string().contains("current_schema"));
}

#[test]
fn scan_request_validates_remote_session_context() {
    let mut request = ScanRequest::new(
        TableId::new(NamespaceId::new("app"), TableName::new("messages")),
        TableType::User,
        TenantContext::with_user_id(UserId::new("u_123")),
    );
    request.remote_session = Some(
        RemoteSessionContext::new("pg-backend-1", Some("public"), Some("tx-1"))
            .expect("valid remote session context"),
    );

    assert!(request.validate().is_ok());
}

#[test]
fn tenant_context_allows_explicit_override_of_session_user_id() {
    let ctx = TenantContext::new(Some(UserId::new("u_explicit")), Some(UserId::new("u_session")));
    ctx.validate().expect("explicit override should succeed");
    assert_eq!(
        ctx.effective_user_id(),
        Some(&UserId::new("u_explicit")),
        "explicit user_id should take precedence"
    );
}
