//! End-to-end gRPC round-trip tests for all PG extension statement types.
//!
//! Each test stands up a tonic server with a mock `OperationExecutor`, connects
//! a `RemoteKalamClient`, and exercises a full round-trip:
//!     client → gRPC → KalamPgService → OperationExecutor → response → client

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::RemoteServerConfig;
use kalamdb_pg::{
    DeleteRequest, InsertRequest, KalamPgService, MutationResult, OperationExecutor,
    PgServiceServer, ScanRequest, ScanResult, UpdateRequest,
};
use tonic::Status;

// ---------------------------------------------------------------------------
// Mock executor
// ---------------------------------------------------------------------------

/// A mock executor that returns predictable results for every operation.
struct MockExecutor;

#[async_trait]
impl OperationExecutor for MockExecutor {
    async fn execute_scan(&self, _request: ScanRequest) -> Result<ScanResult, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    None,
                ])),
            ],
        )
        .expect("build batch");
        Ok(ScanResult {
            batches: vec![batch],
        })
    }

    async fn execute_insert(&self, request: InsertRequest) -> Result<MutationResult, Status> {
        Ok(MutationResult {
            affected_rows: request.rows.len() as u64,
        })
    }

    async fn execute_update(&self, _request: UpdateRequest) -> Result<MutationResult, Status> {
        Ok(MutationResult { affected_rows: 1 })
    }

    async fn execute_delete(&self, _request: DeleteRequest) -> Result<MutationResult, Status> {
        Ok(MutationResult { affected_rows: 1 })
    }

    async fn execute_sql(&self, sql: &str) -> Result<String, Status> {
        Ok(format!("executed: {sql}"))
    }

    async fn execute_query(&self, sql: &str) -> Result<(String, Vec<bytes::Bytes>), Status> {
        Ok((format!("executed: {sql}"), Vec::new()))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spin up a KalamPgService with the mock executor on the given port.
async fn start_server(port: u16) {
    let bind_addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("bind addr");
    let service = KalamPgService::new(false, None)
        .with_operation_executor(Arc::new(MockExecutor));

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PgServiceServer::new(service))
            .serve(bind_addr)
            .await
            .expect("serve pg grpc");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn connect(port: u16) -> RemoteKalamClient {
    RemoteKalamClient::connect(
        RemoteServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            ..Default::default()
        },
    )
    .await
    .expect("connect client")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ntest::timeout(10000)]
async fn scan_returns_arrow_batches() {
    start_server(59981).await;
    let client = connect(59981).await;

    client
        .open_session("sess-scan", Some("app"))
        .await
        .expect("open session");

    let response = client
        .scan("app", "messages", "shared", "sess-scan", None, vec![], None)
        .await
        .expect("scan");

    assert_eq!(response.batches.len(), 1);
    let batch = &response.batches[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column");
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 3);

    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column");
    assert_eq!(name_col.value(0), "alice");
    assert_eq!(name_col.value(1), "bob");
    assert!(name_col.is_null(2));
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn scan_with_projection_and_limit() {
    start_server(59982).await;
    let client = connect(59982).await;

    client
        .open_session("sess-proj", Some("app"))
        .await
        .expect("open session");

    let response = client
        .scan(
            "app",
            "messages",
            "shared",
            "sess-proj",
            None,
            vec!["id".to_string()],
            Some(1),
        )
        .await
        .expect("scan with projection");

    // The mock always returns the same data regardless of projection/limit,
    // but this verifies the round-trip doesn't fail with these parameters.
    assert!(!response.batches.is_empty());
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn insert_single_row() {
    start_server(59983).await;
    let client = connect(59983).await;

    client
        .open_session("sess-ins", Some("app"))
        .await
        .expect("open session");

    let response = client
        .insert(
            "app",
            "messages",
            "shared",
            "sess-ins",
            None,
            vec![r#"{"id":{"Int64":"10"},"name":{"Utf8":"charlie"}}"#.to_string()],
        )
        .await
        .expect("insert");

    assert_eq!(response.affected_rows, 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn insert_multiple_rows() {
    start_server(59984).await;
    let client = connect(59984).await;

    client
        .open_session("sess-ins-multi", Some("app"))
        .await
        .expect("open session");

    let response = client
        .insert(
            "app",
            "messages",
            "shared",
            "sess-ins-multi",
            None,
            vec![
                r#"{"id":{"Int64":"20"},"name":{"Utf8":"dave"}}"#.to_string(),
                r#"{"id":{"Int64":"21"},"name":{"Utf8":"eve"}}"#.to_string(),
                r#"{"id":{"Int64":"22"},"name":{"Utf8":"frank"}}"#.to_string(),
            ],
        )
        .await
        .expect("insert multiple");

    assert_eq!(response.affected_rows, 3);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn insert_with_user_id() {
    start_server(59985).await;
    let client = connect(59985).await;

    client
        .open_session("sess-ins-uid", Some("app"))
        .await
        .expect("open session");

    let response = client
        .insert(
            "app",
            "messages",
            "user",
            "sess-ins-uid",
            Some("user-42"),
            vec![r#"{"id":{"Int64":"30"},"name":{"Utf8":"grace"}}"#.to_string()],
        )
        .await
        .expect("insert with user_id");

    assert_eq!(response.affected_rows, 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn update_single_row() {
    start_server(59986).await;
    let client = connect(59986).await;

    client
        .open_session("sess-upd", Some("app"))
        .await
        .expect("open session");

    let response = client
        .update(
            "app",
            "messages",
            "shared",
            "sess-upd",
            None,
            "1",
            r#"{"name":{"Utf8":"alice-updated"}}"#,
        )
        .await
        .expect("update");

    assert_eq!(response.affected_rows, 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn update_with_user_id() {
    start_server(59987).await;
    let client = connect(59987).await;

    client
        .open_session("sess-upd-uid", Some("app"))
        .await
        .expect("open session");

    let response = client
        .update(
            "app",
            "messages",
            "user",
            "sess-upd-uid",
            Some("user-42"),
            "1",
            r#"{"name":{"Utf8":"updated-name"}}"#,
        )
        .await
        .expect("update with user_id");

    assert_eq!(response.affected_rows, 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn delete_single_row() {
    start_server(59988).await;
    let client = connect(59988).await;

    client
        .open_session("sess-del", Some("app"))
        .await
        .expect("open session");

    let response = client
        .delete("app", "messages", "shared", "sess-del", None, "1")
        .await
        .expect("delete");

    assert_eq!(response.affected_rows, 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn delete_with_user_id() {
    start_server(59989).await;
    let client = connect(59989).await;

    client
        .open_session("sess-del-uid", Some("app"))
        .await
        .expect("open session");

    let response = client
        .delete(
            "app",
            "messages",
            "user",
            "sess-del-uid",
            Some("user-42"),
            "1",
        )
        .await
        .expect("delete with user_id");

    assert_eq!(response.affected_rows, 1);
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn transaction_lifecycle_with_dml() {
    start_server(59990).await;
    let client = connect(59990).await;

    let session = client
        .open_session("sess-tx", Some("app"))
        .await
        .expect("open session");
    assert_eq!(session.session_id, "sess-tx");

    // Begin transaction
    let tx_id = client
        .begin_transaction("sess-tx")
        .await
        .expect("begin tx");
    assert!(!tx_id.is_empty());

    // Insert within transaction
    let ins_resp = client
        .insert(
            "app",
            "messages",
            "shared",
            "sess-tx",
            None,
            vec![r#"{"id":{"Int64":"100"},"name":{"Utf8":"tx-row"}}"#.to_string()],
        )
        .await
        .expect("insert in tx");
    assert_eq!(ins_resp.affected_rows, 1);

    // Update within transaction
    let upd_resp = client
        .update(
            "app",
            "messages",
            "shared",
            "sess-tx",
            None,
            "100",
            r#"{"name":{"Utf8":"tx-row-updated"}}"#,
        )
        .await
        .expect("update in tx");
    assert_eq!(upd_resp.affected_rows, 1);

    // Commit transaction
    client
        .commit_transaction("sess-tx", &tx_id)
        .await
        .expect("commit tx");
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn transaction_rollback() {
    start_server(59991).await;
    let client = connect(59991).await;

    client
        .open_session("sess-rb", Some("app"))
        .await
        .expect("open session");

    let tx_id = client
        .begin_transaction("sess-rb")
        .await
        .expect("begin tx");

    // Insert then rollback
    client
        .insert(
            "app",
            "messages",
            "shared",
            "sess-rb",
            None,
            vec![r#"{"id":{"Int64":"200"},"name":{"Utf8":"will-be-rolled-back"}}"#.to_string()],
        )
        .await
        .expect("insert in tx");

    client
        .rollback_transaction("sess-rb", &tx_id)
        .await
        .expect("rollback tx");
}

