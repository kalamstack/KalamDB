use async_trait::async_trait;
use bytes::Bytes;
use kalamdb_pg::{
    BeginTransactionRequest, CloseSessionRequest, CommitTransactionRequest, DeleteRequest,
    ExecuteQueryRpcRequest, ExecuteSqlRpcRequest, InsertRequest, KalamPgService, MutationResult,
    OpenSessionRequest, OperationExecutor, PgService, PgServiceServer, PingRequest,
    RollbackTransactionRequest, ScanRequest, ScanResult, UpdateRequest,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tonic::Request;

fn service() -> KalamPgService {
    KalamPgService::new(false, None)
}

fn plain_request<T>(payload: T) -> Request<T> {
    Request::new(payload)
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
async fn open_session_rejects_empty_session_id() {
    let service = service();
    let resp = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "".to_string(),
            current_schema: None,
        }))
        .await;
    assert!(resp.is_err());
    assert_eq!(resp.unwrap_err().code(), tonic::Code::InvalidArgument);
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
    assert_eq!(inner.session_id, "pg-1");
    assert_eq!(inner.current_schema.as_deref(), Some("tenant_x"));
}

#[tokio::test]
async fn open_session_updates_schema_on_existing_session() {
    let service = service();
    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-2".to_string(),
            current_schema: Some("ns_a".to_string()),
        }))
        .await
        .unwrap();

    let resp = service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-2".to_string(),
            current_schema: Some("ns_b".to_string()),
        }))
        .await
        .unwrap();

    assert_eq!(resp.into_inner().current_schema.as_deref(), Some("ns_b"));
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

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-existing-sql".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    service
        .execute_sql(plain_request(ExecuteSqlRpcRequest {
            session_id: "pg-existing-sql".to_string(),
            sql: "CREATE TABLE ignored".to_string(),
        }))
        .await
        .unwrap();

    let session = service
        .session_registry()
        .get("pg-existing-sql")
        .expect("preexisting session should remain open");
    assert_eq!(session.last_method(), Some("ExecuteSql"));
}

#[tokio::test]
async fn execute_query_keeps_preexisting_session_open() {
    let service = KalamPgService::new(false, None).with_operation_executor(Arc::new(SqlExecutor));

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-existing-query".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    service
        .execute_query(plain_request(ExecuteQueryRpcRequest {
            session_id: "pg-existing-query".to_string(),
            sql: "SELECT 1".to_string(),
        }))
        .await
        .unwrap();

    let session = service
        .session_registry()
        .get("pg-existing-query")
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

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-321-deadbeef".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-321-deadbeef".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    assert_eq!(tx_id, "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
    assert_eq!(executor.begin_calls.load(Ordering::Relaxed), 1);

    service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: "pg-321-deadbeef".to_string(),
            transaction_id: tx_id.clone(),
        }))
        .await
        .unwrap();
    assert_eq!(executor.commit_calls.load(Ordering::Relaxed), 1);

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-321-deadbeef".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    service
        .rollback_transaction(plain_request(RollbackTransactionRequest {
            session_id: "pg-321-deadbeef".to_string(),
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

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-close-1".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-close-1".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;
    assert!(!tx_id.is_empty());

    service
        .close_session(plain_request(CloseSessionRequest {
            session_id: "pg-close-1".to_string(),
        }))
        .await
        .unwrap();

    assert_eq!(executor.rollback_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn begin_transaction_reclaims_stale_remote_transaction_via_executor() {
    let executor = Arc::new(RecordingExecutor::default());
    let service = KalamPgService::new(false, None).with_operation_executor(executor.clone());

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-stale-1".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-stale-1".to_string(),
        }))
        .await
        .unwrap();

    service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-stale-1".to_string(),
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

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-stale-reconcile".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let first_tx = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-stale-reconcile".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    let replacement_tx = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-stale-reconcile".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    assert_eq!(first_tx, "tx-stale-1");
    assert_eq!(replacement_tx, "tx-replacement-2");

    let session = service
        .session_registry()
        .get("pg-stale-reconcile")
        .expect("session remains open");
    assert_eq!(session.transaction_id(), Some("tx-replacement-2"));
}

#[tokio::test]
async fn commit_transaction_clears_local_state_when_remote_tx_is_already_gone() {
    let executor = Arc::new(CommitNotFoundExecutor);
    let service = KalamPgService::new(false, None).with_operation_executor(executor);

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-commit-reconcile".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-commit-reconcile".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;

    let err = service
        .commit_transaction(plain_request(CommitTransactionRequest {
            session_id: "pg-commit-reconcile".to_string(),
            transaction_id: tx_id.clone(),
        }))
        .await
        .expect_err("remote missing transaction should still report an error");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let session = service
        .session_registry()
        .get("pg-commit-reconcile")
        .expect("session remains open");
    assert_eq!(session.transaction_id(), None);
    assert_eq!(session.transaction_state(), None);
}

#[tokio::test]
async fn close_session_succeeds_when_remote_tx_is_already_committed() {
    let executor = Arc::new(RollbackCommittedExecutor);
    let service = KalamPgService::new(false, None).with_operation_executor(executor);

    service
        .open_session(plain_request(OpenSessionRequest {
            session_id: "pg-close-reconcile".to_string(),
            current_schema: None,
        }))
        .await
        .unwrap();

    let tx_id = service
        .begin_transaction(plain_request(BeginTransactionRequest {
            session_id: "pg-close-reconcile".to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .transaction_id;
    assert_eq!(tx_id, "tx-close-committed");

    service
        .close_session(plain_request(CloseSessionRequest {
            session_id: "pg-close-reconcile".to_string(),
        }))
        .await
        .expect("close_session should be best-effort for terminal remote tx state");

    assert!(service.session_registry().get("pg-close-reconcile").is_none());
}

// ---------------------------------------------------------------------------
// PgServiceServer type check
// ---------------------------------------------------------------------------

#[test]
fn pg_service_server_builds_from_impl() {
    let service = service();
    let _server = PgServiceServer::new(service);
}
