use kalamdb_pg::{
    BeginTransactionRequest, CloseSessionRequest, CommitTransactionRequest, DeleteRequest,
    InsertRequest, KalamPgService, MutationResult, OpenSessionRequest, OperationExecutor,
    PgService, PgServiceServer, PingRequest, RollbackTransactionRequest, ScanRequest,
    ScanResult, UpdateRequest,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

// ---------------------------------------------------------------------------
// PgServiceServer type check
// ---------------------------------------------------------------------------

#[test]
fn pg_service_server_builds_from_impl() {
    let service = service();
    let _server = PgServiceServer::new(service);
}
