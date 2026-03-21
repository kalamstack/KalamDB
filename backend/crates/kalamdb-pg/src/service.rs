use std::sync::Arc;

use async_trait::async_trait;
use tonic::codegen::*;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tonic_prost::ProstCodec;

use crate::query_executor::PgQueryExecutor;
use crate::{RemotePgSession, SessionRegistry};

const AUTHORIZATION_HEADER: &str = "authorization";
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
    }
}

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
}

pub use pg_service_client::PgServiceClient;
pub use pg_service_server::{PgService, PgServiceServer};

#[derive(Clone)]
pub struct KalamPgService {
    expected_auth_header: Option<String>,
    session_registry: Arc<SessionRegistry>,
    query_executor: Option<Arc<dyn PgQueryExecutor>>,
}

impl std::fmt::Debug for KalamPgService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalamPgService")
            .field("has_auth", &self.expected_auth_header.is_some())
            .field("has_query_executor", &self.query_executor.is_some())
            .finish()
    }
}

impl Default for KalamPgService {
    fn default() -> Self {
        Self {
            expected_auth_header: None,
            session_registry: Arc::new(SessionRegistry::default()),
            query_executor: None,
        }
    }
}

impl KalamPgService {
    pub fn new(expected_auth_header: Option<String>) -> Self {
        Self {
            expected_auth_header: expected_auth_header
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            session_registry: Arc::new(SessionRegistry::default()),
            query_executor: None,
        }
    }

    pub fn with_query_executor(mut self, executor: Arc<dyn PgQueryExecutor>) -> Self {
        self.query_executor = Some(executor);
        self
    }

    pub fn set_query_executor(&mut self, executor: Arc<dyn PgQueryExecutor>) {
        self.query_executor = Some(executor);
    }

    fn query_executor(&self) -> Result<&dyn PgQueryExecutor, Status> {
        self.query_executor
            .as_deref()
            .ok_or_else(|| Status::unavailable("Query executor not configured"))
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(expected_auth_header) = self.expected_auth_header.as_deref() else {
            return Ok(());
        };

        let received_auth_header = request
            .metadata()
            .get(AUTHORIZATION_HEADER)
            .ok_or_else(|| Status::unauthenticated("Missing authorization metadata"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid authorization metadata"))?;

        if received_auth_header != expected_auth_header {
            return Err(Status::permission_denied("Invalid authorization metadata"));
        }

        Ok(())
    }

    pub fn session_registry(&self) -> Arc<SessionRegistry> {
        Arc::clone(&self.session_registry)
    }

    pub fn authorization_metadata_value(
        auth_header: &str,
    ) -> Result<MetadataValue<tonic::metadata::Ascii>, Status> {
        MetadataValue::try_from(auth_header.trim())
            .map_err(|_| Status::invalid_argument("Invalid authorization metadata"))
    }
}

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
        let response = self.query_executor()?.execute_scan(inner).await?;
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
        let response = self.query_executor()?.execute_insert(inner).await?;
        Ok(Response::new(response))
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
        let response = self.query_executor()?.execute_update(inner).await?;
        Ok(Response::new(response))
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
        let response = self.query_executor()?.execute_delete(inner).await?;
        Ok(Response::new(response))
    }
}
