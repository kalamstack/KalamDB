use async_trait::async_trait;
use bytes::Bytes;
use kalamdb_auth::{create_and_sign_token, services::unified::init_auth_config, AuthError, AuthResult, UserRepository};
use kalamdb_configs::{AuthSettings, OAuthSettings};
use kalamdb_pg::{
    BeginTransactionRequest, CloseSessionRequest, CommitTransactionRequest, DeleteRequest,
    ExecuteQueryRpcRequest, ExecuteSqlRpcRequest, InsertRequest, KalamPgService, MutationResult,
    OpenSessionRequest, OperationExecutor, PgService, PgServiceServer, PingRequest,
    RollbackTransactionRequest, ScanRequest, ScanResult, UpdateRequest,
};
use kalamdb_system::providers::storages::models::StorageMode;
use kalamdb_system::{AuthType, Role, User};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tonic::Request;

const VALID_DBA_BASIC_AUTH: &str = "Basic cGdfYnJpZGdlX3VzZXI6c2VjcmV0LXBhc3M=";

fn init_test_auth_settings() -> AuthSettings {
    let auth = AuthSettings::default();
    init_auth_config(&auth, &OAuthSettings::default());
    auth
}

fn service() -> KalamPgService {
    KalamPgService::new(false, None)
}

fn plain_request<T>(payload: T) -> Request<T> {
    Request::new(payload)
}

fn auth_request<T>(payload: T, auth_header: &str) -> Request<T> {
    let mut request = Request::new(payload);
    request
        .metadata_mut()
        .insert("authorization", auth_header.parse().expect("valid authorization metadata"));
    request
}

#[derive(Clone)]
struct StaticUserRepo {
    user: User,
}

impl StaticUserRepo {
    fn new(role: Role) -> Self {
        Self {
            user: User {
                user_id: kalamdb_commons::UserId::new("pg_bridge_user"),
                password_hash: "$2b$12$unusedhashunusedhashunusedhashunusedhashunusedhashu".to_string(),
                role,
                email: Some("pg-bridge@example.com".to_string()),
                auth_type: AuthType::Password,
                auth_data: None,
                storage_mode: StorageMode::Table,
                storage_id: None,
                failed_login_attempts: 0,
                locked_until: None,
                last_login_at: None,
                created_at: 0,
                updated_at: 0,
                last_seen: None,
                deleted_at: None,
            },
        }
    }

    fn with_password_hash(role: Role, password_hash: String) -> Self {
        let mut repo = Self::new(role);
        repo.user.password_hash = password_hash;
        repo
    }
}

async fn password_repo(role: Role) -> Arc<dyn UserRepository> {
    let password_hash = kalamdb_auth::security::password::hash_password("secret-pass", None)
        .await
        .expect("hash password for pg auth test");
    Arc::new(StaticUserRepo::with_password_hash(role, password_hash))
}

#[async_trait]
impl UserRepository for StaticUserRepo {
    async fn get_user_by_id(&self, user_id: &kalamdb_commons::UserId) -> AuthResult<User> {
        if &self.user.user_id == user_id {
            Ok(self.user.clone())
        } else {
            Err(AuthError::UserNotFound(format!("User '{}' not found", user_id)))
        }
    }

    async fn update_user(&self, _user: &User) -> AuthResult<()> {
        Ok(())
    }

    async fn create_user(&self, _user: User) -> AuthResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingExecutor {
    begin_calls: AtomicUsize,
    commit_calls: AtomicUsize,
    rollback_calls: AtomicUsize,
}

#[derive(Default)]
struct BeginRollbackNotFoundExecutor {
    begin_calls: AtomicUsize,
}

#[derive(Default)]
struct CommitNotFoundExecutor;

#[derive(Default)]
struct RollbackCommittedExecutor;

#[derive(Default)]
struct SqlExecutor;

#[derive(Default)]
struct CapturingSqlExecutor {
    last_query_sql: Mutex<Option<String>>,
}

#[async_trait]
impl OperationExecutor for RecordingExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_insert(
        &self,
        _request: InsertRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_update(
        &self,
        _request: UpdateRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_delete(
        &self,
        _request: DeleteRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn begin_transaction(&self, _session_id: &str) -> Result<Option<String>, tonic::Status> {
        self.begin_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d".to_string()))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(transaction_id.to_string()))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        self.rollback_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(transaction_id.to_string()))
    }

    async fn execute_sql(&self, _sql: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_query(
        &self,
        _sql: &str,
    ) -> Result<(String, Vec<bytes::Bytes>), tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }
}

#[async_trait]
impl OperationExecutor for BeginRollbackNotFoundExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_insert(
        &self,
        _request: InsertRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_update(
        &self,
        _request: UpdateRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_delete(
        &self,
        _request: DeleteRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn begin_transaction(&self, _session_id: &str) -> Result<Option<String>, tonic::Status> {
        let begin_call = self.begin_calls.fetch_add(1, Ordering::Relaxed);
        let transaction_id = if begin_call == 0 {
            "tx-stale-1"
        } else {
            "tx-replacement-2"
        };
        Ok(Some(transaction_id.to_string()))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::failed_precondition(format!(
            "transaction '{}' not found",
            transaction_id
        )))
    }

    async fn execute_sql(&self, _sql: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_query(
        &self,
        _sql: &str,
    ) -> Result<(String, Vec<bytes::Bytes>), tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }
}

#[async_trait]
impl OperationExecutor for CommitNotFoundExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_insert(
        &self,
        _request: InsertRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_update(
        &self,
        _request: UpdateRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_delete(
        &self,
        _request: DeleteRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn begin_transaction(&self, _session_id: &str) -> Result<Option<String>, tonic::Status> {
        Ok(Some("tx-commit-missing".to_string()))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::failed_precondition(format!(
            "transaction '{}' not found during commit",
            transaction_id
        )))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_sql(&self, _sql: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_query(
        &self,
        _sql: &str,
    ) -> Result<(String, Vec<bytes::Bytes>), tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }
}

#[async_trait]
impl OperationExecutor for RollbackCommittedExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_insert(
        &self,
        _request: InsertRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_update(
        &self,
        _request: UpdateRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_delete(
        &self,
        _request: DeleteRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn begin_transaction(&self, _session_id: &str) -> Result<Option<String>, tonic::Status> {
        Ok(Some("tx-close-committed".to_string()))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::failed_precondition(format!(
            "cannot rollback transaction '{}' while it is committed",
            transaction_id
        )))
    }

    async fn execute_sql(&self, _sql: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_query(
        &self,
        _sql: &str,
    ) -> Result<(String, Vec<bytes::Bytes>), tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }
}

#[async_trait]
impl OperationExecutor for SqlExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_insert(
        &self,
        _request: InsertRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_update(
        &self,
        _request: UpdateRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_delete(
        &self,
        _request: DeleteRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn begin_transaction(&self, _session_id: &str) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_sql(&self, _sql: &str) -> Result<String, tonic::Status> {
        Ok("ok".to_string())
    }

    async fn execute_query(&self, _sql: &str) -> Result<(String, Vec<Bytes>), tonic::Status> {
        Ok(("ok".to_string(), Vec::new()))
    }
}

#[async_trait]
impl OperationExecutor for CapturingSqlExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_insert(
        &self,
        _request: InsertRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_update(
        &self,
        _request: UpdateRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_delete(
        &self,
        _request: DeleteRequest,
    ) -> Result<MutationResult, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn begin_transaction(&self, _session_id: &str) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn commit_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn rollback_transaction(
        &self,
        _session_id: &str,
        _transaction_id: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_sql(&self, _sql: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::unimplemented("not needed for this test"))
    }

    async fn execute_query(&self, sql: &str) -> Result<(String, Vec<Bytes>), tonic::Status> {
        self.last_query_sql
            .lock()
            .expect("capture query sql")
            .replace(sql.to_string());
        Ok(("ok".to_string(), Vec::new()))
    }
}

// ---------------------------------------------------------------------------
// Basic tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ping_succeeds() {
    let service = service();
    let resp = service.ping(plain_request(PingRequest {})).await;
    assert!(resp.is_ok());
    assert!(resp.unwrap().into_inner().ok);
}

#[tokio::test]
async fn open_session_ignores_client_session_id() {
    let service = service();
    // Server generates its own session ID regardless of what the client sends.
    let resp = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "".to_string(),
            current_schema: None,
        }))
        .await;
    assert!(resp.is_ok());
    let inner = resp.unwrap().into_inner();
    assert!(!inner.session_id.is_empty(), "server must issue a session ID");
}

#[tokio::test]
async fn open_session_returns_session_and_schema() {
    let service = service();
    let resp = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-1".to_string(),
            current_schema: Some("tenant_x".to_string()),
        }))
        .await;
    assert!(resp.is_ok());
    let inner = resp.unwrap().into_inner();
    assert!(!inner.session_id.is_empty(), "server must issue a session ID");
    assert_eq!(inner.current_schema.as_deref(), Some("tenant_x"));
}

#[tokio::test]
async fn open_session_generates_distinct_session_ids() {
    let service = service();
    let resp1 = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: Some("ns_a".to_string()),
        }))
        .await
        .unwrap()
        .into_inner();

    let resp2 = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: Some("ns_b".to_string()),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_ne!(resp1.session_id, resp2.session_id, "each open_session must produce a unique ID");
    assert_eq!(resp1.current_schema.as_deref(), Some("ns_a"));
    assert_eq!(resp2.current_schema.as_deref(), Some("ns_b"));
}

// ---------------------------------------------------------------------------
// Transaction lifecycle tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn begin_transaction_succeeds() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-tx-1".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let resp = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-1".to_string(),
        }))
        .await;
    assert!(resp.is_ok());
    let tx_id = resp.unwrap().into_inner().transaction_id;
    assert!(!tx_id.is_empty());
}

#[tokio::test]
async fn begin_transaction_rejects_empty_session_id() {
    let service = service();
    let resp = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "".to_string(),
        }))
        .await;
    assert!(resp.is_err());
    assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn commit_transaction_succeeds() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-tx-2".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-2".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    let resp = service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: "pg-tx-2".to_string(),
            transaction_id: tx_id.clone(),
        }))
        .await;
    assert!(resp.is_ok());
    assert_eq!(resp.unwrap().into_inner().transaction_id, tx_id);
}

#[tokio::test]
async fn rollback_transaction_succeeds() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-tx-3".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-3".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    let resp = service
        .rollback_transaction(plain_request(RollbackTransactionRequest {
            session_id: "pg-tx-3".to_string(),
            transaction_id: tx_id.clone(),
        }))
        .await;
    assert!(resp.is_ok());
    assert_eq!(resp.unwrap().into_inner().transaction_id, tx_id);
}

#[tokio::test]
async fn double_begin_auto_rollbacks_stale_and_starts_new() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-tx-4".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx1 = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-4".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    // Second begin should auto-rollback the stale transaction and return a new one
    let tx2 = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-4".to_string(),
        }))
        .await
        .expect("double begin should auto-rollback stale tx")
        .into_inner()
        .transaction_id;

    assert_ne!(tx1, tx2);
}

#[tokio::test]
async fn commit_with_wrong_tx_id_fails() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-tx-5".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-5".to_string(),
        }))
        .await
        .unwrap();

    let resp = service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: "pg-tx-5".to_string(),
            transaction_id: "wrong-id".to_string(),
        }))
        .await;
    assert!(resp.is_err());
    assert_eq!(resp.unwrap_err().code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn begin_after_commit_succeeds() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-tx-6".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-6".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: "pg-tx-6".to_string(),
            transaction_id: tx_id,
        }))
        .await
        .unwrap();

    // Should be able to begin a new transaction after commit
    let resp = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-tx-6".to_string(),
        }))
        .await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn execute_sql_closes_ephemeral_idle_session() {
    let service = KalamPgService::new(false, None).with_operation_executor(Arc::new(SqlExecutor));

    service
        .execute_sql(plain_request(ExecuteSqlRpcRequest {
            session_id: "pg-ephemeral-sql".to_string(),
            sql: "CREATE NAMESPACE IF NOT EXISTS test_ns".to_string(),
        }))
        .await
        .unwrap();

    assert!(service.session_registry().get("pg-ephemeral-sql").is_none());
}

#[tokio::test]
async fn execute_query_closes_ephemeral_idle_session() {
    let service = KalamPgService::new(false, None).with_operation_executor(Arc::new(SqlExecutor));

    service
        .execute_query(plain_request(ExecuteQueryRpcRequest {
            session_id: "pg-ephemeral-query".to_string(),
            sql: "SELECT 1".to_string(),
        }))
        .await
        .unwrap();

    assert!(service.session_registry().get("pg-ephemeral-query").is_none());
}

#[tokio::test]
async fn execute_sql_keeps_preexisting_session_open() {
    let service = KalamPgService::new(false, None).with_operation_executor(Arc::new(SqlExecutor));

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    service
        .execute_sql(plain_request(ExecuteSqlRpcRequest {
            session_id: session_id.clone(),
            sql: "CREATE TABLE ignored".to_string(),
        }))
        .await
        .unwrap();

    let session = service
        .session_registry()
        .get(&session_id)
        .expect("preexisting session should remain open");
    assert_eq!(session.last_method(), Some("ExecuteSql"));
}

#[tokio::test]
async fn execute_query_keeps_preexisting_session_open() {
    let service = KalamPgService::new(false, None).with_operation_executor(Arc::new(SqlExecutor));

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    service
        .execute_query(plain_request(ExecuteQueryRpcRequest {
            session_id: session_id.clone(),
            sql: "SELECT 1".to_string(),
        }))
        .await
        .unwrap();

    let session = service
        .session_registry()
        .get(&session_id)
        .expect("preexisting session should remain open");
    assert_eq!(session.last_method(), Some("ExecuteQuery"));
}

#[tokio::test]
async fn execute_query_passes_json_operator_sql_through_without_rewrite() {
    // With datafusion-functions-json the -> / ->> operators are handled by
    // DataFusion's planner, so the PG service must forward the raw SQL.
    let executor = Arc::new(CapturingSqlExecutor::default());
    let service = KalamPgService::new(false, None).with_operation_executor(executor.clone());

    service
        .execute_query(plain_request(ExecuteQueryRpcRequest {
            session_id: "pg-json-query".to_string(),
            sql: "SELECT doc->>'name' AS name FROM docs".to_string(),
        }))
        .await
        .unwrap();

    let captured = executor
        .last_query_sql
        .lock()
        .expect("captured sql")
        .clone()
        .expect("query sql should be captured");

    // SQL is now forwarded as-is; DataFusion handles the operator natively.
    assert_eq!(
        captured,
        "SELECT doc->>'name' AS name FROM docs"
    );
}

#[tokio::test]
async fn transaction_rpcs_delegate_to_configured_operation_executor() {
    let executor = Arc::new(RecordingExecutor::default());
    let service = KalamPgService::new(false, None).with_operation_executor(executor.clone());

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    assert_eq!(tx_id, "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
    assert_eq!(executor.begin_calls.load(Ordering::Relaxed), 1);

    service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: session_id.clone(),
            transaction_id: tx_id.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(executor.commit_calls.load(Ordering::Relaxed), 1);

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    service
        .rollback_transaction(plain_request(RollbackTransactionRequest {
            session_id: session_id.clone(),
            transaction_id: tx_id,
        }))
        .await
        .unwrap();
    assert_eq!(executor.rollback_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn close_session_rolls_back_via_configured_operation_executor() {
    let executor = Arc::new(RecordingExecutor::default());
    let service = KalamPgService::new(false, None).with_operation_executor(executor.clone());

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;
    assert!(!tx_id.is_empty());

    service
        .close_session(plain_request(CloseSessionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap();

    assert_eq!(executor.rollback_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn begin_transaction_reclaims_stale_remote_transaction_via_executor() {
    let executor = Arc::new(RecordingExecutor::default());
    let service = KalamPgService::new(false, None).with_operation_executor(executor.clone());

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap();

    service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap();

    assert_eq!(executor.begin_calls.load(Ordering::Relaxed), 2);
    assert_eq!(executor.rollback_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn begin_transaction_reconciles_local_state_when_stale_remote_tx_is_missing() {
    let executor = Arc::new(BeginRollbackNotFoundExecutor::default());
    let service = KalamPgService::new(false, None).with_operation_executor(executor);

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let first_tx = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    let replacement_tx = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    assert_eq!(first_tx, "tx-stale-1");
    assert_eq!(replacement_tx, "tx-replacement-2");

    let session = service
        .session_registry()
        .get(&session_id)
        .expect("session remains open");
    assert_eq!(session.transaction_id(), Some("tx-replacement-2"));
}

#[tokio::test]
async fn commit_transaction_clears_local_state_when_remote_tx_is_already_gone() {
    let executor = Arc::new(CommitNotFoundExecutor);
    let service = KalamPgService::new(false, None).with_operation_executor(executor);

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    let err = service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: session_id.clone(),
            transaction_id: tx_id.clone(),
        }))
        .await
        .expect_err("remote missing transaction should still report an error");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let session = service
        .session_registry()
        .get(&session_id)
        .expect("session remains open");
    assert_eq!(session.transaction_id(), None);
    assert_eq!(session.transaction_state(), None);
}

#[tokio::test]
async fn close_session_succeeds_when_remote_tx_is_already_committed() {
    let executor = Arc::new(RollbackCommittedExecutor);
    let service = KalamPgService::new(false, None).with_operation_executor(executor);

    let session_id = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: String::new(),
            current_schema: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .session_id;

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;
    assert_eq!(tx_id, "tx-close-committed");

    service
        .close_session(plain_request(CloseSessionRequest {
            session_id: session_id.clone(),
        }))
        .await
        .expect("close_session should be best-effort for terminal remote tx state");

    assert!(service.session_registry().get(&session_id).is_none());
}

// ---------------------------------------------------------------------------
// PgServiceServer type check
// ---------------------------------------------------------------------------

#[test]
fn pg_service_server_builds_from_impl() {
    let service = service();
    let _server = PgServiceServer::new(service);
}

#[tokio::test]
async fn ping_accepts_valid_dba_bearer_token() {
    let auth = init_test_auth_settings();
    let repo: Arc<dyn UserRepository> = Arc::new(StaticUserRepo::new(Role::Dba));
    let service = KalamPgService::new(false, None).with_bearer_auth(Arc::clone(&repo));
    let user_id = kalamdb_commons::UserId::new("pg_bridge_user");
    let (token, _) = create_and_sign_token(&user_id, &Role::Dba, None, None, &auth.jwt_secret)
        .expect("create bearer token");

    let response = service
        .ping(auth_request(PingRequest {}, &format!("Bearer {}", token)))
        .await
        .expect("DBA bearer token should authenticate");

    assert!(response.into_inner().ok);
}

#[tokio::test]
async fn ping_accepts_valid_dba_basic_auth() {
    let service = KalamPgService::new(false, None).with_bearer_auth(password_repo(Role::Dba).await);

    let response = service
        .ping(auth_request(PingRequest {}, VALID_DBA_BASIC_AUTH))
        .await
        .expect("DBA basic auth should authenticate");

    assert!(response.into_inner().ok);
}

#[tokio::test]
async fn open_session_accepts_valid_dba_basic_auth() {
    let service = KalamPgService::new(false, None).with_bearer_auth(password_repo(Role::Dba).await);

    let response = service
        .open_session(auth_request(
            OpenSessionRequest {
                session_id: String::new(),
                current_schema: Some("tenant_x".to_string()),
            },
            VALID_DBA_BASIC_AUTH,
        ))
        .await
        .expect("DBA basic auth should open a PG RPC session")
        .into_inner();

    assert!(!response.session_id.is_empty());
    assert_eq!(response.current_schema.as_deref(), Some("tenant_x"));
}

#[tokio::test]
async fn ping_rejects_non_admin_bearer_token() {
    let auth = init_test_auth_settings();
    let repo: Arc<dyn UserRepository> = Arc::new(StaticUserRepo::new(Role::User));
    let service = KalamPgService::new(false, None).with_bearer_auth(Arc::clone(&repo));
    let user_id = kalamdb_commons::UserId::new("pg_bridge_user");
    let (token, _) = create_and_sign_token(&user_id, &Role::User, None, None, &auth.jwt_secret)
        .expect("create bearer token");

    let error = service
        .ping(auth_request(PingRequest {}, &format!("Bearer {}", token)))
        .await
        .expect_err("non-admin bearer token should be rejected");

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn legacy_static_header_still_authenticates_when_bearer_auth_enabled() {
    let repo: Arc<dyn UserRepository> = Arc::new(StaticUserRepo::new(Role::Dba));
    let service = KalamPgService::new(false, Some("Bearer legacy-shared-secret".to_string()))
        .with_bearer_auth(repo);

    let response = service
        .ping(auth_request(PingRequest {}, "Bearer legacy-shared-secret"))
        .await
        .expect("legacy shared secret should still authenticate");

    assert!(response.into_inner().ok);
}
