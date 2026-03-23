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
use crate::operation_executor::{
    self, OperationExecutor,
};
#[cfg(feature = "server")]
use crate::{RemotePgSession, SessionRegistry};

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
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "CloseSession"));
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
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/BeginTransaction");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "BeginTransaction"));
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
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/CommitTransaction");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "CommitTransaction"));
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
            let path = http::uri::PathAndQuery::from_static("/kalamdb.pg.PgService/RollbackTransaction");
            let mut request = request.into_request();
            request.extensions_mut().insert(GrpcMethod::new(PG_SERVICE_NAME, "RollbackTransaction"));
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

    impl<T: PgService> tonic::server::UnaryService<CommitTransactionRequest> for CommitTransactionSvc<T> {
        type Response = CommitTransactionResponse;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, request: Request<CommitTransactionRequest>) -> Self::Future {
            let inner = Arc::clone(&self.0);
            let fut = async move { inner.commit_transaction(request).await };
            Box::pin(fut)
        }
    }

    struct RollbackTransactionSvc<T: PgService>(Arc<T>);

    impl<T: PgService> tonic::server::UnaryService<RollbackTransactionRequest> for RollbackTransactionSvc<T> {
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

        let _ = self.session_registry.open_or_get(session_id);
        let session = self
            .session_registry
            .update(session_id, current_schema, None)
            .unwrap_or_else(|| {
                RemotePgSession::new(session_id).with_current_schema(current_schema)
            });

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

        self.session_registry.remove(session_id);
        log::debug!("PG session closed: {}", session_id);

        Ok(Response::new(CloseSessionResponse {}))
    }

    async fn scan(
        &self,
        request: Request<ScanRpcRequest>,
    ) -> Result<Response<ScanRpcResponse>, Status> {
        self.authorize(&request)?;
        let inner = request.into_inner();
        log::debug!(
            "PG scan: {}.{} type={}",
            inner.namespace,
            inner.table_name,
            inner.table_type
        );
        let domain_req = operation_executor::scan_request_from_rpc(&inner)?;
        let domain_result = self.operation_executor()?.execute_scan(domain_req).await?;
        let response = operation_executor::scan_result_to_rpc(domain_result)?;
        Ok(Response::new(response))
    }

    async fn insert(
        &self,
        request: Request<InsertRpcRequest>,
    ) -> Result<Response<InsertRpcResponse>, Status> {
        self.authorize(&request)?;
        let inner = request.into_inner();
        log::debug!(
            "PG insert: {}.{} rows={}",
            inner.namespace,
            inner.table_name,
            inner.rows_json.len()
        );
        let domain_req = operation_executor::insert_request_from_rpc(&inner)?;
        let result = self.operation_executor()?.execute_insert(domain_req).await?;
        Ok(Response::new(InsertRpcResponse {
            affected_rows: result.affected_rows,
        }))
    }

    async fn update(
        &self,
        request: Request<UpdateRpcRequest>,
    ) -> Result<Response<UpdateRpcResponse>, Status> {
        self.authorize(&request)?;
        let inner = request.into_inner();
        log::debug!(
            "PG update: {}.{} pk={}",
            inner.namespace,
            inner.table_name,
            inner.pk_value
        );
        let domain_req = operation_executor::update_request_from_rpc(&inner)?;
        let result = self.operation_executor()?.execute_update(domain_req).await?;
        Ok(Response::new(UpdateRpcResponse {
            affected_rows: result.affected_rows,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRpcRequest>,
    ) -> Result<Response<DeleteRpcResponse>, Status> {
        self.authorize(&request)?;
        let inner = request.into_inner();
        log::debug!(
            "PG delete: {}.{} pk={}",
            inner.namespace,
            inner.table_name,
            inner.pk_value
        );
        let domain_req = operation_executor::delete_request_from_rpc(&inner)?;
        let result = self.operation_executor()?.execute_delete(domain_req).await?;
        Ok(Response::new(DeleteRpcResponse {
            affected_rows: result.affected_rows,
        }))
    }

    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        self.authorize(&request)?;
        let inner = request.into_inner();
        let session_id = inner.session_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        // Ensure session exists
        self.session_registry.open_or_get(session_id);

        let transaction_id = self
            .session_registry
            .begin_transaction(session_id)
            .map_err(|e| Status::failed_precondition(e))?;

        log::debug!("PG begin_transaction: session={} tx={}", session_id, transaction_id);

        Ok(Response::new(BeginTransactionResponse { transaction_id }))
    }

    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        self.authorize(&request)?;
        let inner = request.into_inner();
        let session_id = inner.session_id.trim();
        let transaction_id = inner.transaction_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        if transaction_id.is_empty() {
            return Err(Status::invalid_argument(
                "transaction_id must not be empty",
            ));
        }

        let committed_id = self
            .session_registry
            .commit_transaction(session_id, transaction_id)
            .map_err(|e| Status::failed_precondition(e))?;

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
        let inner = request.into_inner();
        let session_id = inner.session_id.trim();
        let transaction_id = inner.transaction_id.trim();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }

        let rolled_back_id = self
            .session_registry
            .rollback_transaction(session_id, transaction_id)
            .map_err(|e| Status::failed_precondition(e))?;

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
        let inner = request.into_inner();
        let sql = inner.sql.trim();
        if sql.is_empty() {
            return Err(Status::invalid_argument("sql must not be empty"));
        }

        log::debug!("PG execute_sql: {}", sql);
        let result = self.operation_executor()?.execute_sql(sql).await?;
        Ok(Response::new(ExecuteSqlRpcResponse {
            success: true,
            message: result,
        }))
    }
}
