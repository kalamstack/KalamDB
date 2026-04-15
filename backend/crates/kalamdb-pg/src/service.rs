#[cfg(feature = "server")]
use std::sync::Arc;

#[cfg(feature = "server")]
use async_trait::async_trait;
use tonic::codegen::*;
#[cfg(feature = "server")]
use tonic::Request;
use tonic::{Response, Status};
use tonic_prost::ProstCodec;

#[cfg(feature = "server")]
use crate::operation_executor::{self, OperationExecutor};
#[cfg(feature = "server")]
use crate::{LivePgTransaction, RemotePgSession, SessionRegistry};

const PG_SERVICE_NAME: &str = "kalamdb.pg.PgService";

#[derive(Clone, PartialEq, prost::Message)]
pub struct PingRequest {}

#[derive(Clone, PartialEq, prost::Message)]
pub struct PingResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct OpenSessionRequest {
    #[prost(string, tag = "1")]
    pub session_id: String,
    #[prost(string, optional, tag = "2")]
    pub current_schema: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct OpenSessionResponse {
    #[prost(string, tag = "1")]
    pub session_id: String,
    #[prost(string, optional, tag = "2")]
    pub current_schema: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CloseSessionRequest {
    #[prost(string, tag = "1")]
    pub session_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CloseSessionResponse {}

// ---------------------------------------------------------------------------
// Scan / Mutation RPC messages
// ---------------------------------------------------------------------------

/// Equality filter pushed down from PostgreSQL WHERE clauses.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ScanFilterExpression {
    #[prost(string, tag = "1")]
    pub column: String,
    /// Operator: "eq" for equality.
    #[prost(string, tag = "2")]
    pub op: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ScanRpcRequest {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub table_name: String,
    #[prost(string, tag = "3")]
    pub table_type: String,
    #[prost(string, tag = "4")]
    pub session_id: String,
    #[prost(string, optional, tag = "5")]
    pub user_id: Option<String>,
    /// Projected column names (empty = all columns).
    #[prost(string, repeated, tag = "6")]
    pub columns: Vec<String>,
    #[prost(uint64, optional, tag = "7")]
    pub limit: Option<u64>,
    /// Equality filters pushed down from PostgreSQL WHERE clauses.
    #[prost(message, repeated, tag = "8")]
    pub filters: Vec<ScanFilterExpression>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ScanRpcResponse {
    /// Arrow IPC-encoded record batches (one bytes entry per batch).
    #[prost(bytes = "bytes", repeated, tag = "1")]
    pub ipc_batches: Vec<bytes::Bytes>,
    /// Arrow IPC-encoded schema (for empty result sets).
    #[prost(bytes = "bytes", optional, tag = "2")]
    pub schema_ipc: Option<bytes::Bytes>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct InsertRpcRequest {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub table_name: String,
    #[prost(string, tag = "3")]
    pub table_type: String,
    #[prost(string, tag = "4")]
    pub session_id: String,
    #[prost(string, optional, tag = "5")]
    pub user_id: Option<String>,
    /// Each entry is a JSON-encoded row (`{"col": value, ...}`).
    #[prost(string, repeated, tag = "6")]
    pub rows_json: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct InsertRpcResponse {
    #[prost(uint64, tag = "1")]
    pub affected_rows: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct UpdateRpcRequest {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub table_name: String,
    #[prost(string, tag = "3")]
    pub table_type: String,
    #[prost(string, tag = "4")]
    pub session_id: String,
    #[prost(string, optional, tag = "5")]
    pub user_id: Option<String>,
    #[prost(string, tag = "6")]
    pub pk_value: String,
    /// JSON-encoded column updates (`{"col": value, ...}`).
    #[prost(string, tag = "7")]
    pub updates_json: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct UpdateRpcResponse {
    #[prost(uint64, tag = "1")]
    pub affected_rows: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct DeleteRpcRequest {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub table_name: String,
    #[prost(string, tag = "3")]
    pub table_type: String,
    #[prost(string, tag = "4")]
    pub session_id: String,
    #[prost(string, optional, tag = "5")]
    pub user_id: Option<String>,
    #[prost(string, tag = "6")]
    pub pk_value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct DeleteRpcResponse {
    #[prost(uint64, tag = "1")]
    pub affected_rows: u64,
}

// ---------------------------------------------------------------------------
// DDL (execute_sql) RPC messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecuteSqlRpcRequest {
    #[prost(string, tag = "1")]
    pub sql: String,
    #[prost(string, tag = "2")]
    pub session_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecuteSqlRpcResponse {
    #[prost(bool, tag = "1")]
    pub success: bool,
    #[prost(string, tag = "2")]
    pub message: String,
}

// ---------------------------------------------------------------------------
// Execute query (kalam_exec) RPC messages
// ---------------------------------------------------------------------------

/// Request to execute an arbitrary SQL statement against KalamDB and return results.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecuteQueryRpcRequest {
    #[prost(string, tag = "1")]
    pub sql: String,
    #[prost(string, tag = "2")]
    pub session_id: String,
}

/// Response for execute_query. For SELECT queries the result rows are encoded
/// as Arrow IPC stream bytes (one entry per RecordBatch). For DML/DDL the
/// `ipc_batches` is empty and `message` carries the status string.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecuteQueryRpcResponse {
    #[prost(bool, tag = "1")]
    pub success: bool,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(bytes = "bytes", repeated, tag = "3")]
    pub ipc_batches: Vec<bytes::Bytes>,
}

// ---------------------------------------------------------------------------
// Transaction RPC messages
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, prost::Message)]
pub struct BeginTransactionRequest {
    #[prost(string, tag = "1")]
    pub session_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct BeginTransactionResponse {
    #[prost(string, tag = "1")]
    pub transaction_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CommitTransactionRequest {
    #[prost(string, tag = "1")]
    pub session_id: String,
    #[prost(string, tag = "2")]
    pub transaction_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CommitTransactionResponse {
    #[prost(string, tag = "1")]
    pub transaction_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RollbackTransactionRequest {
    #[prost(string, tag = "1")]
    pub session_id: String,
    #[prost(string, tag = "2")]
    pub transaction_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RollbackTransactionResponse {
    #[prost(string, tag = "1")]
    pub transaction_id: String,
}

#[cfg(feature = "server")]
pub fn open_session_response_to_session(response: OpenSessionResponse) -> RemotePgSession {
    RemotePgSession::new(response.session_id)
        .with_current_schema(response.current_schema.as_deref())
}

pub mod pg_service_client {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct PgServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl PgServiceClient<tonic::transport::Channel> {
        pub fn new(channel: tonic::transport::Channel) -> Self {
            let inner = tonic::client::Grpc::new(channel);
            Self { inner }
        }
    }

    impl<T> PgServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError> + std::fmt::Debug,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub async fn ping(
            &mut self,
            request: impl tonic::IntoRequest<PingRequest>,
        ) -> Result<Response<PingResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;

            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/Ping");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "Ping"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn open_session(
            &mut self,
            request: impl tonic::IntoRequest<OpenSessionRequest>,
        ) -> Result<Response<OpenSessionResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;

            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/OpenSession");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "OpenSession"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn close_session(
            &mut self,
            request: impl tonic::IntoRequest<CloseSessionRequest>,
        ) -> Result<Response<CloseSessionResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;

            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/CloseSession");
            let mut request = request.into_request();
            request
                .extensions_mut()
                .insert(GrpcMethod::new(PG_SERVICE_NAME, "CloseSession"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn scan(
            &mut self,
            request: impl tonic::IntoRequest<ScanRpcRequest>,
        ) -> Result<Response<ScanRpcResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/Scan");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "Scan"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn insert(
            &mut self,
            request: impl tonic::IntoRequest<InsertRpcRequest>,
        ) -> Result<Response<InsertRpcResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/Insert");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "Insert"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn update(
            &mut self,
            request: impl tonic::IntoRequest<UpdateRpcRequest>,
        ) -> Result<Response<UpdateRpcResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/Update");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "Update"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<DeleteRpcRequest>,
        ) -> Result<Response<DeleteRpcResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/Delete");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "Delete"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn begin_transaction(
            &mut self,
            request: impl tonic::IntoRequest<BeginTransactionRequest>,
        ) -> Result<Response<BeginTransactionResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/BeginTransaction");
            let mut request = request.into_request();
            request
                .extensions_mut()
                .insert(GrpcMethod::new(PG_SERVICE_NAME, "BeginTransaction"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn commit_transaction(
            &mut self,
            request: impl tonic::IntoRequest<CommitTransactionRequest>,
        ) -> Result<Response<CommitTransactionResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/CommitTransaction");
            let mut request = request.into_request();
            request
                .extensions_mut()
                .insert(GrpcMethod::new(PG_SERVICE_NAME, "CommitTransaction"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn rollback_transaction(
            &mut self,
            request: impl tonic::IntoRequest<RollbackTransactionRequest>,
        ) -> Result<Response<RollbackTransactionResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/RollbackTransaction");
            let mut request = request.into_request();
            request
                .extensions_mut()
                .insert(GrpcMethod::new(PG_SERVICE_NAME, "RollbackTransaction"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn execute_sql(
            &mut self,
            request: impl tonic::IntoRequest<ExecuteSqlRpcRequest>,
        ) -> Result<Response<ExecuteSqlRpcResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/ExecuteSql");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "ExecuteSql"));
            self.inner.unary(request, path, codec).await
        }

        pub async fn execute_query(
            &mut self,
            request: impl tonic::IntoRequest<ExecuteQueryRpcRequest>,
        ) -> Result<Response<ExecuteQueryRpcResponse>, Status> {
            self.inner.ready().await.map_err(|error| {
                Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", error))
            })?;
            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/ExecuteQuery");
            let mut request = request.into_request();
            request
                .extensions_mut()
                .insert(GrpcMethod::new(PG_SERVICE_NAME, "ExecuteQuery"));
            self.inner.unary(request, path, codec).await
        }
    }
}

#[cfg(feature = "server")]
pub mod pg_service_server {
    use super::*;

    #[async_trait]
    pub trait PgService: Send + Sync + 'static {
        async fn ping(
            &self,
            request: Request<PingRequest>,
        ) -> Result<Response<PingResponse>, Status>;

        async fn open_session(
            &self,
            request: Request<OpenSessionRequest>,
        ) -> Result<Response<OpenSessionResponse>, Status>;

        async fn close_session(
            &self,
            request: Request<CloseSessionRequest>,
        ) -> Result<Response<CloseSessionResponse>, Status>;

        async fn scan(
            &self,
            request: Request<ScanRpcRequest>,
        ) -> Result<Response<ScanRpcResponse>, Status>;

        async fn insert(
            &self,
            request: Request<InsertRpcRequest>,
        ) -> Result<Response<InsertRpcResponse>, Status>;

        async fn update(
            &self,
            request: Request<UpdateRpcRequest>,
        ) -> Result<Response<UpdateRpcResponse>, Status>;

        async fn delete(
            &self,
            request: Request<DeleteRpcRequest>,
        ) -> Result<Response<DeleteRpcResponse>, Status>;

        async fn begin_transaction(
            &self,
            request: Request<BeginTransactionRequest>,
        ) -> Result<Response<BeginTransactionResponse>, Status>;

        async fn commit_transaction(
            &self,
            request: Request<CommitTransactionRequest>,
        ) -> Result<Response<CommitTransactionResponse>, Status>;

        async fn rollback_transaction(
            &self,
            request: Request<RollbackTransactionRequest>,
        ) -> Result<Response<RollbackTransactionResponse>, Status>;

        async fn execute_sql(
            &self,
            request: Request<ExecuteSqlRpcRequest>,
        ) -> Result<Response<ExecuteSqlRpcResponse>, Status>;

        async fn execute_query(
            &self,
            request: Request<ExecuteQueryRpcRequest>,
        ) -> Result<Response<ExecuteQueryRpcResponse>, Status>;
    }

    #[derive(Debug, Clone)]
    pub struct PgServiceServer<T: PgService> {
        inner: Arc<T>,
    }

    impl<T: PgService> PgServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(inner),
            }
        }
    }

    impl<T: PgService> tonic::server::NamedService for PgServiceServer<T> {
        const NAME: &'static str = PG_SERVICE_NAME;
    }

    impl<T, B> tonic::codegen::Service<http::Request<B>> for PgServiceServer<T>
    where
        T: PgService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = Arc::clone(&self.inner);

            match req.uri().path() {
                "/kalamdb.pg.PgService/Ping" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = PingSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/OpenSession" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = OpenSessionSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/CloseSession" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = CloseSessionSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/Scan" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = ScanSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/Insert" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = InsertSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/Update" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = UpdateSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/Delete" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = DeleteSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/BeginTransaction" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = BeginTransactionSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/CommitTransaction" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = CommitTransactionSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/RollbackTransaction" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = RollbackTransactionSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/ExecuteSql" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = ExecuteSqlSvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.pg.PgService/ExecuteQuery" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = ExecuteQuerySvc(inner);
                        let response = grpc.unary(method, req).await;
                        Ok(response)
                    };
                    Box::pin(fut)
                },
                _ => Box::pin(async move {
                    let mut builder = http::Response::builder();
                    builder = builder.status(200).header("grpc-status", "12");
                    Ok(builder.body(tonic::body::Body::empty()).unwrap())
                }),
            }
        }
    }

    struct PingSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<PingRequest> for PingSvc<T> {
        type Response = PingResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<PingRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.ping(request).await };
            Box::pin(fut)
        }
    }

    struct OpenSessionSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<OpenSessionRequest> for OpenSessionSvc<T> {
        type Response = OpenSessionResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<OpenSessionRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.open_session(request).await };
            Box::pin(fut)
        }
    }

    struct CloseSessionSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<CloseSessionRequest> for CloseSessionSvc<T> {
        type Response = CloseSessionResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<CloseSessionRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.close_session(request).await };
            Box::pin(fut)
        }
    }

    struct ScanSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<ScanRpcRequest> for ScanSvc<T> {
        type Response = ScanRpcResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<ScanRpcRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.scan(request).await };
            Box::pin(fut)
        }
    }

    struct InsertSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<InsertRpcRequest> for InsertSvc<T> {
        type Response = InsertRpcResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<InsertRpcRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.insert(request).await };
            Box::pin(fut)
        }
    }

    struct UpdateSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<UpdateRpcRequest> for UpdateSvc<T> {
        type Response = UpdateRpcResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<UpdateRpcRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.update(request).await };
            Box::pin(fut)
        }
    }

    struct DeleteSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<DeleteRpcRequest> for DeleteSvc<T> {
        type Response = DeleteRpcResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<DeleteRpcRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.delete(request).await };
            Box::pin(fut)
        }
    }

    struct BeginTransactionSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<BeginTransactionRequest> for BeginTransactionSvc<T> {
        type Response = BeginTransactionResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<BeginTransactionRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.begin_transaction(request).await };
            Box::pin(fut)
        }
    }

    struct CommitTransactionSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<CommitTransactionRequest>
        for CommitTransactionSvc<T>
    {
        type Response = CommitTransactionResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<CommitTransactionRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.commit_transaction(request).await };
            Box::pin(fut)
        }
    }

    struct RollbackTransactionSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<RollbackTransactionRequest>
        for RollbackTransactionSvc<T>
    {
        type Response = RollbackTransactionResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<RollbackTransactionRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.rollback_transaction(request).await };
            Box::pin(fut)
        }
    }

    struct ExecuteSqlSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<ExecuteSqlRpcRequest> for ExecuteSqlSvc<T> {
        type Response = ExecuteSqlRpcResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<ExecuteSqlRpcRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.execute_sql(request).await };
            Box::pin(fut)
        }
    }

    struct ExecuteQuerySvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<ExecuteQueryRpcRequest> for ExecuteQuerySvc<T> {
        type Response = ExecuteQueryRpcResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<ExecuteQueryRpcRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.execute_query(request).await };
            Box::pin(fut)
        }
    }
}

pub use pg_service_client::PgServiceClient;
#[cfg(feature = "server")]
pub use pg_service_server::{PgService, PgServiceServer};

#[cfg(feature = "server")]
#[derive(Clone)]
pub struct KalamPgService {
    /// When true, authorize via client certificate CN (mTLS) using kalamdb-server-auth.
    mtls_enabled: bool,
    /// Pre-shared token for non-mTLS authentication (e.g. `Bearer <token>`).
    /// When set, the `authorization` gRPC metadata must match this value.
    expected_auth_header: Option<String>,
    session_registry: Arc<SessionRegistry>,
    operation_executor: Option<Arc<dyn OperationExecutor>>,
}

#[cfg(feature = "server")]
impl std::fmt::Debug for KalamPgService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalamPgService")
            .field("mtls_enabled", &self.mtls_enabled)
            .field("has_operation_executor", &self.operation_executor.is_some())
            .finish()
    }
}

#[cfg(feature = "server")]
impl Default for KalamPgService {
    fn default() -> Self {
        Self {
            mtls_enabled: false,
            expected_auth_header: None,
            session_registry: Arc::new(SessionRegistry::default()),
            operation_executor: None,
        }
    }
}

#[cfg(feature = "server")]
impl KalamPgService {
    pub fn new(mtls_enabled: bool, expected_auth_header: Option<String>) -> Self {
        let expected_auth_header = expected_auth_header
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        if !mtls_enabled && expected_auth_header.is_none() {
            log::warn!(
                "PG RPC service is running without mTLS or pg_auth_token; requests are unauthenticated"
            );
        }

        Self {
            mtls_enabled,
            expected_auth_header,
            session_registry: Arc::new(SessionRegistry::default()),
            operation_executor: None,
        }
    }

    pub fn with_operation_executor(mut self, executor: Arc<dyn OperationExecutor>) -> Self {
        self.operation_executor = Some(executor);
        self
    }

    pub fn set_operation_executor(&mut self, executor: Arc<dyn OperationExecutor>) {
        self.operation_executor = Some(executor);
    }

    fn operation_executor(&self) -> Result<&dyn OperationExecutor, Status> {
        self.operation_executor
            .as_deref()
            .ok_or_else(|| Status::unavailable("Operation executor not configured"))
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if self.mtls_enabled {
            kalamdb_server_auth::RpcCaller::require_pg_extension(request)?;
            return Ok(());
        }
        // Non-mTLS: validate pre-shared token if configured.
        if let Some(expected) = &self.expected_auth_header {
            let provided = request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if provided != expected.as_str() {
                return Err(Status::unauthenticated("invalid or missing pg auth token"));
            }
        }
        Ok(())
    }

    pub fn session_registry(&self) -> Arc<SessionRegistry> {
        Arc::clone(&self.session_registry)
    }

    pub fn snapshot_with_live_transactions<I>(&self, active_transactions: I) -> Vec<RemotePgSession>
    where
        I: IntoIterator<Item = LivePgTransaction>,
    {
        self.session_registry.snapshot_with_live_transactions(active_transactions)
    }

    fn close_ephemeral_idle_session_if_created(&self, session_id: &str, had_session: bool) {
        if had_session {
            return;
        }

        let should_close = self
            .session_registry
            .get(session_id)
            .map(|session| session.transaction_id().is_none())
            .unwrap_or(false);

        if should_close {
            self.session_registry.close_session(session_id);
        }
    }

    fn should_reconcile_local_transaction_state(status: &Status) -> bool {
        if !matches!(status.code(), tonic::Code::FailedPrecondition | tonic::Code::NotFound) {
            return false;
        }

        let message = status.message();
        message.contains("not found")
            || message.contains("no active transaction")
            || message.contains("already committed")
            || message.contains("already rolled back")
            || message.contains("timed out and was aborted")
            || message.contains("timed_out")
            || message.contains("because it was aborted")
            || message.contains(" was aborted")
            || message.contains(" is aborted")
            || message.contains("while it is committed")
            || message.contains("while it is committing")
            || message.contains("while it is rolling_back")
            || message.contains("while it is rolled_back")
    }

    fn tracked_transaction_id(&self, session_id: &str) -> Option<String> {
        self.session_registry
            .get(session_id)
            .and_then(|session| session.transaction_id().map(ToOwned::to_owned))
    }

    fn reconcile_local_transaction_state(
        &self,
        session_id: &str,
        transaction_id: &str,
        rpc_name: &str,
        status: &Status,
    ) {
        self.session_registry
            .clear_transaction_state_if_matches(session_id, Some(transaction_id));
        log::debug!(
            "PG {}: cleared local transaction bookkeeping for session '{}' tx '{}' after executor returned {}: {}",
            rpc_name,
            session_id,
            transaction_id,
            status.code(),
            status.message()
        );
    }

    async fn cleanup_after_terminal_transaction_error(
        &self,
        session_id: &str,
        transaction_id: &str,
        rpc_name: &str,
        status: &Status,
    ) {
        if let Some(executor) = self.operation_executor.as_deref() {
            match executor.rollback_transaction(session_id, transaction_id).await {
                Ok(_) => {
                    log::debug!(
                        "PG {}: finalized terminal transaction cleanup for session '{}' tx '{}' via rollback",
                        rpc_name,
                        session_id,
                        transaction_id
                    );
                },
                Err(rollback_status)
                    if Self::should_reconcile_local_transaction_state(&rollback_status) =>
                {
                    log::debug!(
                        "PG {}: rollback cleanup for session '{}' tx '{}' also returned terminal state {}: {}",
                        rpc_name,
                        session_id,
                        transaction_id,
                        rollback_status.code(),
                        rollback_status.message()
                    );
                },
                Err(rollback_status) => {
                    log::warn!(
                        "PG {}: rollback cleanup for session '{}' tx '{}' failed after terminal error {}: {}",
                        rpc_name,
                        session_id,
                        transaction_id,
                        rollback_status.code(),
                        rollback_status.message()
                    );
                },
            }
        }

        self.reconcile_local_transaction_state(session_id, transaction_id, rpc_name, status);
    }

    async fn cleanup_current_transaction_after_terminal_error(
        &self,
        session_id: &str,
        rpc_name: &str,
        status: &Status,
    ) {
        if let Some(transaction_id) = self.tracked_transaction_id(session_id) {
            self.cleanup_after_terminal_transaction_error(
                session_id,
                &transaction_id,
                rpc_name,
                status,
            )
            .await;
        }
    }

    async fn handle_statement_transaction_result<T>(
        &self,
        session_id: &str,
        rpc_name: &str,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        match result {
            Ok(value) => Ok(value),
            Err(status) if Self::should_reconcile_local_transaction_state(&status) => {
                self.cleanup_current_transaction_after_terminal_error(
                    session_id, rpc_name, &status,
                )
                .await;
                Err(status)
            },
            Err(status) => Err(status),
        }
    }

    async fn handle_explicit_transaction_result<T>(
        &self,
        session_id: &str,
        transaction_id: &str,
        rpc_name: &str,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        match result {
            Ok(value) => Ok(value),
            Err(status) if Self::should_reconcile_local_transaction_state(&status) => {
                self.cleanup_after_terminal_transaction_error(
                    session_id,
                    transaction_id,
                    rpc_name,
                    &status,
                )
                .await;
                Err(status)
            },
            Err(status) => Err(status),
        }
    }

    fn record_session_activity<T>(
        &self,
        session_id: &str,
        current_schema: Option<&str>,
        method: &str,
        request: &Request<T>,
    ) -> RemotePgSession {
        let client_addr = request.remote_addr().map(|addr| addr.to_string());
        self.session_registry.open_or_get_with_context(
            session_id,
            current_schema,
            client_addr.as_deref(),
            Some(method),
        )
    }
}

#[cfg(feature = "server")]
#[async_trait]
impl PgService for KalamPgService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(PingResponse { ok: true }))
    }

    async fn open_session(
        &self,
        request: Request<OpenSessionRequest>,
    ) -> Result<Response<OpenSessionResponse>, Status> {
        self.authorize(&request)?;

        let remote_addr = request.remote_addr().map(|addr| addr.to_string());
        let request = request.into_inner();
        let session_id = request.session_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        let current_schema = request
            .current_schema
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let session = self.session_registry.open_or_get_with_context(
            session_id,
            current_schema,
            remote_addr.as_deref(),
            Some("OpenSession"),
        );

        Ok(Response::new(OpenSessionResponse {
            session_id: session.session_id().to_string(),
            current_schema: session.current_schema().map(ToOwned::to_owned),
        }))
    }

    async fn close_session(
        &self,
        request: Request<CloseSessionRequest>,
    ) -> Result<Response<CloseSessionResponse>, Status> {
        self.authorize(&request)?;

        let request = request.into_inner();
        let session_id = request.session_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        if let Some(session) = self.session_registry.get(session_id) {
            if let Some(transaction_id) = session.transaction_id().map(ToOwned::to_owned) {
                if let Some(executor) = self.operation_executor.as_deref() {
                    match executor.rollback_transaction(session_id, &transaction_id).await {
                        Ok(_) => {},
                        Err(status) if Self::should_reconcile_local_transaction_state(&status) => {
                            self.reconcile_local_transaction_state(
                                session_id,
                                &transaction_id,
                                "close_session",
                                &status,
                            );
                        },
                        Err(status) => {
                            log::warn!(
                                "PG close_session: proceeding after remote rollback error for session '{}' tx '{}': {}",
                                session_id,
                                transaction_id,
                                status
                            );
                        },
                    }
                }
            }
        }

        self.session_registry.close_session(session_id);
        log::debug!("PG session closed: {}", session_id);

        Ok(Response::new(CloseSessionResponse {}))
    }

    async fn scan(
        &self,
        request: Request<ScanRpcRequest>,
    ) -> Result<Response<ScanRpcResponse>, Status> {
        self.authorize(&request)?;
        let session_id = request.get_ref().session_id.trim().to_string();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        self.record_session_activity(session_id.as_str(), None, "Scan", &request);
        let inner = request.into_inner();
        log::debug!("PG scan: {}.{} type={}", inner.namespace, inner.table_name, inner.table_type);
        let domain_req = operation_executor::scan_request_from_rpc(&inner)?;
        let domain_result = self
            .handle_statement_transaction_result(
                session_id.as_str(),
                "scan",
                self.operation_executor()?.execute_scan(domain_req).await,
            )
            .await?;
        let response = operation_executor::scan_result_to_rpc(domain_result)?;
        Ok(Response::new(response))
    }

    async fn insert(
        &self,
        request: Request<InsertRpcRequest>,
    ) -> Result<Response<InsertRpcResponse>, Status> {
        self.authorize(&request)?;
        let session_id = request.get_ref().session_id.trim().to_string();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        self.record_session_activity(session_id.as_str(), None, "Insert", &request);
        self.session_registry.mark_transaction_writes(session_id.as_str());
        let inner = request.into_inner();
        log::debug!(
            "PG insert: {}.{} rows={}",
            inner.namespace,
            inner.table_name,
            inner.rows_json.len()
        );
        let domain_req = operation_executor::insert_request_from_rpc(&inner)?;
        let result = self
            .handle_statement_transaction_result(
                session_id.as_str(),
                "insert",
                self.operation_executor()?.execute_insert(domain_req).await,
            )
            .await?;
        Ok(Response::new(InsertRpcResponse {
            affected_rows: result.affected_rows,
        }))
    }

    async fn update(
        &self,
        request: Request<UpdateRpcRequest>,
    ) -> Result<Response<UpdateRpcResponse>, Status> {
        self.authorize(&request)?;
        let session_id = request.get_ref().session_id.trim().to_string();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        self.record_session_activity(session_id.as_str(), None, "Update", &request);
        self.session_registry.mark_transaction_writes(session_id.as_str());
        let inner = request.into_inner();
        log::debug!("PG update: {}.{}", inner.namespace, inner.table_name);
        let domain_req = operation_executor::update_request_from_rpc(&inner)?;
        let result = self
            .handle_statement_transaction_result(
                session_id.as_str(),
                "update",
                self.operation_executor()?.execute_update(domain_req).await,
            )
            .await?;
        Ok(Response::new(UpdateRpcResponse {
            affected_rows: result.affected_rows,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRpcRequest>,
    ) -> Result<Response<DeleteRpcResponse>, Status> {
        self.authorize(&request)?;
        let session_id = request.get_ref().session_id.trim().to_string();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        self.record_session_activity(session_id.as_str(), None, "Delete", &request);
        self.session_registry.mark_transaction_writes(session_id.as_str());
        let inner = request.into_inner();
        log::debug!("PG delete: {}.{}", inner.namespace, inner.table_name);
        let domain_req = operation_executor::delete_request_from_rpc(&inner)?;
        let result = self
            .handle_statement_transaction_result(
                session_id.as_str(),
                "delete",
                self.operation_executor()?.execute_delete(domain_req).await,
            )
            .await?;
        Ok(Response::new(DeleteRpcResponse {
            affected_rows: result.affected_rows,
        }))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        self.authorize(&request)?;
        let remote_addr = request.remote_addr().map(|addr| addr.to_string());
        let inner = request.into_inner();
        let session_id = inner.session_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        // Ensure session exists
        self.session_registry.open_or_get_with_context(
            session_id,
            None,
            remote_addr.as_deref(),
            Some("BeginTransaction"),
        );

        if let Some(session) = self.session_registry.get(session_id) {
            if let Some(stale_transaction_id) = session.transaction_id().map(ToOwned::to_owned) {
                log::warn!(
                    "PG begin_transaction: auto-rolling back stale transaction '{}' for session '{}'",
                    stale_transaction_id,
                    session_id
                );

                if let Some(executor) = self.operation_executor.as_deref() {
                    match executor.rollback_transaction(session_id, &stale_transaction_id).await {
                        Ok(_) => {},
                        Err(status) if Self::should_reconcile_local_transaction_state(&status) => {
                            self.reconcile_local_transaction_state(
                                session_id,
                                &stale_transaction_id,
                                "begin_transaction",
                                &status,
                            );
                        },
                        Err(status) => return Err(status),
                    }
                }

                self.session_registry
                    .clear_transaction_state_if_matches(session_id, Some(&stale_transaction_id));
            }
        }

        let transaction_id = if let Some(executor) = self.operation_executor.as_deref() {
            match executor.begin_transaction(session_id).await? {
                Some(transaction_id) => self
                    .session_registry
                    .begin_transaction_with_id(session_id, transaction_id.as_str())
                    .map_err(Status::failed_precondition)?,
                None => self
                    .session_registry
                    .begin_transaction(session_id)
                    .map_err(Status::failed_precondition)?,
            }
        } else {
            self.session_registry
                .begin_transaction(session_id)
                .map_err(Status::failed_precondition)?
        };

        log::debug!("PG begin_transaction: session={} tx={}", session_id, transaction_id);

        Ok(Response::new(BeginTransactionResponse { transaction_id }))
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        self.authorize(&request)?;
        let remote_addr = request.remote_addr().map(|addr| addr.to_string());
        let inner = request.into_inner();
        let session_id = inner.session_id.trim();
        let transaction_id = inner.transaction_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        if transaction_id.is_empty() {
            return Err(Status::invalid_argument("transaction_id must not be empty"));
        }

        self.session_registry.open_or_get_with_context(
            session_id,
            None,
            remote_addr.as_deref(),
            Some("CommitTransaction"),
        );

        let committed_id = if let Some(executor) = self.operation_executor.as_deref() {
            let committed_id = match self
                .handle_explicit_transaction_result(
                    session_id,
                    transaction_id,
                    "commit_transaction",
                    executor.commit_transaction(session_id, transaction_id).await,
                )
                .await?
            {
                Some(committed_id) => committed_id,
                None => transaction_id.to_string(),
            };
            self.session_registry
                .commit_transaction(session_id, committed_id.as_str())
                .map_err(Status::failed_precondition)?
        } else {
            self.session_registry
                .commit_transaction(session_id, transaction_id)
                .map_err(Status::failed_precondition)?
        };

        log::debug!("PG commit_transaction: session={} tx={}", session_id, committed_id);

        Ok(Response::new(CommitTransactionResponse {
            transaction_id: committed_id,
        }))
    }

    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        self.authorize(&request)?;
        let remote_addr = request.remote_addr().map(|addr| addr.to_string());
        let inner = request.into_inner();
        let session_id = inner.session_id.trim();
        let transaction_id = inner.transaction_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        self.session_registry.open_or_get_with_context(
            session_id,
            None,
            remote_addr.as_deref(),
            Some("RollbackTransaction"),
        );

        let rolled_back_id = if let Some(executor) = self.operation_executor.as_deref() {
            let rolled_back_id = match self
                .handle_explicit_transaction_result(
                    session_id,
                    transaction_id,
                    "rollback_transaction",
                    executor.rollback_transaction(session_id, transaction_id).await,
                )
                .await?
            {
                Some(rolled_back_id) => rolled_back_id,
                None => transaction_id.to_string(),
            };
            self.session_registry
                .rollback_transaction(session_id, rolled_back_id.as_str())
                .map_err(Status::failed_precondition)?
        } else {
            self.session_registry
                .rollback_transaction(session_id, transaction_id)
                .map_err(Status::failed_precondition)?
        };

        log::debug!("PG rollback_transaction: session={} tx={}", session_id, rolled_back_id);

        Ok(Response::new(RollbackTransactionResponse {
            transaction_id: rolled_back_id,
        }))
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRpcRequest>,
    ) -> Result<Response<ExecuteSqlRpcResponse>, Status> {
        self.authorize(&request)?;
        let remote_addr = request.remote_addr().map(|addr| addr.to_string());
        let inner = request.into_inner();
        let sql = inner.sql.trim();
        let session_id = inner.session_id.trim();
        if sql.is_empty() {
            return Err(Status::invalid_argument("sql must not be empty"));
        }
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        let had_session = self.session_registry.get(session_id).is_some();
        self.session_registry.open_or_get_with_context(
            session_id,
            None,
            remote_addr.as_deref(),
            Some("ExecuteSql"),
        );

        let statement_kind = sql.split_whitespace().next().unwrap_or("UNKNOWN");
        log::debug!("PG execute_sql: kind={}", statement_kind);
        let result = self.operation_executor()?.execute_sql(sql).await;
        self.close_ephemeral_idle_session_if_created(session_id, had_session);
        let result = result?;
        Ok(Response::new(ExecuteSqlRpcResponse {
            success: true,
            message: result,
        }))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRpcRequest>,
    ) -> Result<Response<ExecuteQueryRpcResponse>, Status> {
        self.authorize(&request)?;
        let remote_addr = request.remote_addr().map(|addr| addr.to_string());
        let inner = request.into_inner();
        let sql = inner.sql.trim();
        let session_id = inner.session_id.trim();
        if sql.is_empty() {
            return Err(Status::invalid_argument("sql must not be empty"));
        }
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        let had_session = self.session_registry.get(session_id).is_some();
        self.session_registry.open_or_get_with_context(
            session_id,
            None,
            remote_addr.as_deref(),
            Some("ExecuteQuery"),
        );

        let statement_kind = sql.split_whitespace().next().unwrap_or("UNKNOWN");
        log::debug!("PG execute_query: kind={}", statement_kind);
        let result = self.operation_executor()?.execute_query(sql).await;
        self.close_ephemeral_idle_session_if_created(session_id, had_session);
        let (message, ipc_batches) = result?;
        Ok(Response::new(ExecuteQueryRpcResponse {
            success: true,
            message,
            ipc_batches,
        }))
    }
}
