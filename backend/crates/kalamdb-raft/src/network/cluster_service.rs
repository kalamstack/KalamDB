//! Cluster gRPC Service
//!
//! Provides the gRPC service definition for inter-node cluster communication
//! that is *not* part of Raft consensus (SQL forwarding, liveness, diagnostics).
//!
//! ## Service: `kalamdb.cluster.ClusterService`
//!
//! Each cluster operation has its own explicit gRPC method for discoverability.
//! The service is co-hosted on the same gRPC port as the Raft consensus service
//! and shares the same peer channel pool.
//!
//! ## Adding a new cluster RPC
//!
//! 1. Define `FooRequest` / `FooResponse` prost messages below
//! 2. Add `async fn foo(...)` to the `cluster_server::ClusterService` trait
//! 3. Add the path routing in `ClusterServer::call()`
//! 4. Add the client method in `cluster_client::ClusterServiceClient`
//! 5. Add the handler method in [`super::cluster_handler::ClusterMessageHandler`]

use tonic_prost::ProstCodec;

// ─── Request/Response Messages ──────────────────────────────────────────────
// All message types live in the `models` sub-module; they are re-exported here
// so that the rest of this file (client, server, tests) can use them directly.
pub use super::models::{
    ForwardSqlRequest, ForwardSqlResponse, ForwardSqlResponsePayload, GetNodeInfoRequest,
    GetNodeInfoResponse, PingRequest, PingResponse,
};

// ─── gRPC Client ────────────────────────────────────────────────────────────

pub mod cluster_client {
    use tonic::codegen::*;

    use super::*;

    /// Cluster service gRPC client
    #[derive(Debug, Clone)]
    pub struct ClusterServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl ClusterServiceClient<tonic::transport::Channel> {
        /// Create a new client from a channel
        pub fn new(channel: tonic::transport::Channel) -> Self {
            let inner = tonic::client::Grpc::new(channel);
            Self { inner }
        }
    }

    impl<T> ClusterServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError> + std::fmt::Debug,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        /// Forward SQL write request to leader node.
        pub async fn forward_sql(
            &mut self,
            request: impl tonic::IntoRequest<ForwardSqlRequest>,
        ) -> std::result::Result<tonic::Response<ForwardSqlResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", e))
            })?;

            let codec = ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/kalamdb.cluster.ClusterService/ForwardSql");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("kalamdb.cluster.ClusterService", "ForwardSql"));
            self.inner.unary(req, path, codec).await
        }

        /// Ping a peer node.
        pub async fn ping(
            &mut self,
            request: impl tonic::IntoRequest<PingRequest>,
        ) -> std::result::Result<tonic::Response<PingResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", e))
            })?;

            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.cluster.ClusterService/Ping");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("kalamdb.cluster.ClusterService", "Ping"));
            self.inner.unary(req, path, codec).await
        }

        /// Request live node statistics from a peer.
        pub async fn get_node_info(
            &mut self,
            request: impl tonic::IntoRequest<GetNodeInfoRequest>,
        ) -> std::result::Result<tonic::Response<GetNodeInfoResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", e))
            })?;

            let codec = ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/kalamdb.cluster.ClusterService/GetNodeInfo");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("kalamdb.cluster.ClusterService", "GetNodeInfo"));
            self.inner.unary(req, path, codec).await
        }
    }
}

// ─── gRPC Server ────────────────────────────────────────────────────────────

pub mod cluster_server {
    use std::sync::Arc;

    use tonic::codegen::*;

    use super::*;

    /// Cluster service trait — implement this to handle incoming cluster RPCs.
    #[async_trait::async_trait]
    pub trait ClusterService: std::marker::Send + std::marker::Sync + 'static {
        /// Handle a follower->leader SQL forwarding request.
        async fn forward_sql(
            &self,
            request: tonic::Request<ForwardSqlRequest>,
        ) -> std::result::Result<tonic::Response<ForwardSqlResponse>, tonic::Status>;

        /// Handle a ping from a peer node.
        async fn ping(
            &self,
            request: tonic::Request<PingRequest>,
        ) -> std::result::Result<tonic::Response<PingResponse>, tonic::Status>;

        /// Handle a request to return live node statistics.
        async fn get_node_info(
            &self,
            request: tonic::Request<GetNodeInfoRequest>,
        ) -> std::result::Result<tonic::Response<GetNodeInfoResponse>, tonic::Status>;
    }

    /// Cluster service tonic server wrapper
    #[derive(Debug, Clone)]
    pub struct ClusterServer<T: ClusterService> {
        inner: Arc<T>,
    }

    impl<T: ClusterService> ClusterServer<T> {
        pub fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(inner),
            }
        }

        pub fn from_arc(inner: Arc<T>) -> Self {
            Self { inner }
        }
    }

    impl<T: ClusterService> tonic::server::NamedService for ClusterServer<T> {
        const NAME: &'static str = "kalamdb.cluster.ClusterService";
    }

    impl<T, B> tonic::codegen::Service<http::Request<B>> for ClusterServer<T>
    where
        T: ClusterService,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();

            match req.uri().path() {
                "/kalamdb.cluster.ClusterService/ForwardSql" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = ForwardSqlSvc(inner);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.cluster.ClusterService/Ping" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = PingSvc(inner);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.cluster.ClusterService/GetNodeInfo" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = GetNodeInfoSvc(inner);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
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

    struct ForwardSqlSvc<T: ClusterService>(Arc<T>);

    impl<T: ClusterService> tonic::server::UnaryService<ForwardSqlRequest> for ForwardSqlSvc<T> {
        type Response = ForwardSqlResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<ForwardSqlRequest>) -> Self::Future {
            let inner = self.0.clone();
            let fut = async move { inner.forward_sql(request).await };
            Box::pin(fut)
        }
    }

    struct PingSvc<T: ClusterService>(Arc<T>);

    impl<T: ClusterService> tonic::server::UnaryService<PingRequest> for PingSvc<T> {
        type Response = PingResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<PingRequest>) -> Self::Future {
            let inner = self.0.clone();
            let fut = async move { inner.ping(request).await };
            Box::pin(fut)
        }
    }

    struct GetNodeInfoSvc<T: ClusterService>(Arc<T>);

    impl<T: ClusterService> tonic::server::UnaryService<GetNodeInfoRequest> for GetNodeInfoSvc<T> {
        type Response = GetNodeInfoResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<GetNodeInfoRequest>) -> Self::Future {
            let inner = self.0.clone();
            let fut = async move { inner.get_node_info(request).await };
            Box::pin(fut)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_forward_sql_response_payload_roundtrip() {
        let resp = ForwardSqlResponsePayload {
            status_code: 200,
            body: b"ok".to_vec(),
        };
        assert_eq!(resp.status_code, 200);
        assert_eq!(resp.body, b"ok".to_vec());
    }
}
