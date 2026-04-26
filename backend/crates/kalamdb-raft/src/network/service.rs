//! Raft gRPC Service
//!
//! Provides the gRPC service for handling incoming Raft RPCs.
//!
//! Includes support for:
//! - Raft consensus RPCs (vote, append_entries, install_snapshot)
//! - Client proposal forwarding (forward proposals from followers to leader)

use std::{io::ErrorKind, time::Duration};

use kalamdb_pg::{KalamPgService, PgServiceServer};
use tokio::{sync::oneshot, time::sleep};
use tonic::{
    transport::{Certificate, Identity, ServerTlsConfig},
    Request, Response, Status,
};
use tonic_prost::ProstCodec;

/// Raft RPC request message
#[derive(Clone, PartialEq, prost::Message)]
pub struct RaftRpcRequest {
    /// Raft group ID (e.g., "MetaSystem", "DataUserShard(5)")
    #[prost(string, tag = "1")]
    pub group_id: String,

    /// RPC type: "vote", "append_entries", "install_snapshot"
    #[prost(string, tag = "2")]
    pub rpc_type: String,

    /// Serialized RPC payload
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// Raft RPC response message
#[derive(Clone, PartialEq, prost::Message)]
pub struct RaftRpcResponse {
    /// Serialized response payload
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,

    /// Error message if any
    #[prost(string, tag = "2")]
    pub error: String,
}

/// Client proposal request (for forwarding from follower to leader)
#[derive(Clone, PartialEq, prost::Message)]
pub struct ClientProposalRequest {
    /// Raft group ID (e.g., "MetaSystem", "DataUserShard(5)")
    #[prost(string, tag = "1")]
    pub group_id: String,

    /// Serialized command payload
    #[prost(bytes = "vec", tag = "2")]
    pub command: Vec<u8>,
}

/// Client proposal response
#[derive(Clone, PartialEq, prost::Message)]
pub struct ClientProposalResponse {
    /// True if proposal was successful
    #[prost(bool, tag = "1")]
    pub success: bool,

    /// Serialized response payload (if successful)
    #[prost(bytes = "vec", tag = "2")]
    pub payload: Vec<u8>,

    /// Error message (if not successful)
    #[prost(string, tag = "3")]
    pub error: String,

    /// Leader node ID hint (if we're not the leader)
    #[prost(uint64, optional, tag = "4")]
    pub leader_hint: Option<u64>,

    /// Log index of the applied entry (for read-your-writes consistency)
    /// Followers should wait for this index to be applied locally before
    /// returning to the client.
    #[prost(uint64, tag = "5")]
    pub log_index: u64,
}

/// Generated gRPC client module
pub mod raft_client {
    use tonic::codegen::*;

    use super::*;

    /// Raft RPC client
    #[derive(Debug, Clone)]
    pub struct RaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl RaftClient<tonic::transport::Channel> {
        /// Create a new client from a channel
        pub fn new(channel: tonic::transport::Channel) -> Self {
            let inner = tonic::client::Grpc::new(channel);
            Self { inner }
        }
    }

    impl<T> RaftClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError> + std::fmt::Debug,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        /// Send a Raft RPC
        pub async fn raft_rpc(
            &mut self,
            request: impl tonic::IntoRequest<RaftRpcRequest>,
        ) -> std::result::Result<tonic::Response<RaftRpcResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", e))
            })?;

            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.raft.Raft/RaftRpc");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("kalamdb.raft.Raft", "RaftRpc"));
            self.inner.unary(req, path, codec).await
        }

        /// Forward a client proposal to the leader
        pub async fn client_proposal(
            &mut self,
            request: impl tonic::IntoRequest<ClientProposalRequest>,
        ) -> std::result::Result<tonic::Response<ClientProposalResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service not ready: {:?}", e))
            })?;

            let codec = ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/kalamdb.raft.Raft/ClientProposal");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("kalamdb.raft.Raft", "ClientProposal"));
            self.inner.unary(req, path, codec).await
        }
    }
}

/// Generated gRPC server module
pub mod raft_server {
    use tonic::codegen::*;

    use super::*;

    /// Raft service trait
    #[async_trait::async_trait]
    pub trait Raft: std::marker::Send + std::marker::Sync + 'static {
        /// Handle a Raft RPC
        async fn raft_rpc(
            &self,
            request: tonic::Request<RaftRpcRequest>,
        ) -> std::result::Result<tonic::Response<RaftRpcResponse>, tonic::Status>;

        /// Handle a client proposal (for leader forwarding)
        async fn client_proposal(
            &self,
            request: tonic::Request<ClientProposalRequest>,
        ) -> std::result::Result<tonic::Response<ClientProposalResponse>, tonic::Status>;
    }

    /// Raft service server
    #[derive(Debug, Clone)]
    pub struct RaftServer<T: Raft> {
        inner: Arc<T>,
    }

    impl<T: Raft> RaftServer<T> {
        pub fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(inner),
            }
        }

        pub fn from_arc(inner: Arc<T>) -> Self {
            Self { inner }
        }
    }

    impl<T: Raft> tonic::server::NamedService for RaftServer<T> {
        const NAME: &'static str = "kalamdb.raft.Raft";
    }

    impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftServer<T>
    where
        T: Raft,
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
                "/kalamdb.raft.Raft/RaftRpc" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = RaftRpcSvc(inner);
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                },
                "/kalamdb.raft.Raft/ClientProposal" => {
                    let fut = async move {
                        let mut grpc = tonic::server::Grpc::new(ProstCodec::default());
                        let method = ClientProposalSvc(inner);
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

    struct RaftRpcSvc<T: Raft>(Arc<T>);

    impl<T: Raft> tonic::server::UnaryService<RaftRpcRequest> for RaftRpcSvc<T> {
        type Response = RaftRpcResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<RaftRpcRequest>) -> Self::Future {
            let inner = self.0.clone();
            let fut = async move { inner.raft_rpc(request).await };
            Box::pin(fut)
        }
    }

    struct ClientProposalSvc<T: Raft>(Arc<T>);

    impl<T: Raft> tonic::server::UnaryService<ClientProposalRequest> for ClientProposalSvc<T> {
        type Response = ClientProposalResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<ClientProposalRequest>) -> Self::Future {
            let inner = self.0.clone();
            let fut = async move { inner.client_proposal(request).await };
            Box::pin(fut)
        }
    }
}

use std::sync::Arc;

use crate::manager::RaftManager;

/// Raft gRPC service implementation
#[derive(Clone)]
pub struct RaftService {
    /// Reference to the Raft manager
    manager: Arc<RaftManager>,
}

impl RaftService {
    /// Create a new Raft service
    pub fn new(manager: Arc<RaftManager>) -> Self {
        Self { manager }
    }
}

#[async_trait::async_trait]
impl raft_server::Raft for RaftService {
    async fn raft_rpc(
        &self,
        request: Request<RaftRpcRequest>,
    ) -> Result<Response<RaftRpcResponse>, Status> {
        self.manager.authorize_incoming_rpc(&request)?;
        let req = request.into_inner();

        // Parse group ID
        let group_id = req
            .group_id
            .parse::<crate::GroupId>()
            .map_err(|e| Status::invalid_argument(format!("Invalid group ID: {}", e)))?;

        // Route to appropriate Raft group
        let result = match req.rpc_type.as_str() {
            "vote" => self.manager.handle_vote(group_id, &req.payload).await,
            "append_entries" => self.manager.handle_append_entries(group_id, &req.payload).await,
            "install_snapshot" => {
                self.manager.handle_install_snapshot(group_id, &req.payload).await
            },
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unknown RPC type: {}",
                    req.rpc_type
                )));
            },
        };

        match result {
            Ok(payload) => Ok(Response::new(RaftRpcResponse {
                payload,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(RaftRpcResponse {
                payload: Vec::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn client_proposal(
        &self,
        request: Request<ClientProposalRequest>,
    ) -> Result<Response<ClientProposalResponse>, Status> {
        self.manager.authorize_incoming_rpc(&request)?;
        let req = request.into_inner();

        // Parse group ID
        let group_id = req
            .group_id
            .parse::<crate::GroupId>()
            .map_err(|e| Status::invalid_argument(format!("Invalid group ID: {}", e)))?;

        // Check if we are the leader for this group
        if !self.manager.is_leader(group_id) {
            let leader = self.manager.current_leader(group_id);
            log::debug!(
                "ClientProposal for {} received but not leader (leader is {:?})",
                group_id,
                leader
            );
            return Ok(Response::new(ClientProposalResponse {
                success: false,
                payload: Vec::new(),
                error: format!("Not leader for group {}", group_id),
                leader_hint: leader.map(|n| n.as_u64()),
                log_index: 0,
            }));
        }

        // We are the leader - propose the command locally
        let result = self.manager.propose_for_group_with_index(group_id, req.command).await;

        match result {
            Ok((payload, log_index)) => Ok(Response::new(ClientProposalResponse {
                success: true,
                payload,
                error: String::new(),
                leader_hint: None,
                log_index,
            })),
            Err(e) => Ok(Response::new(ClientProposalResponse {
                success: false,
                payload: Vec::new(),
                error: e.to_string(),
                leader_hint: self.manager.current_leader(group_id).map(|n| n.as_u64()),
                log_index: 0,
            })),
        }
    }
}

/// Start the Raft RPC server
///
/// Spawns a tokio task that listens for incoming Raft RPCs on the configured address.
/// This must be called after creating the RaftManager to enable inter-node communication.
///
/// The `advertise_addr` is typically a hostname:port like "kalamdb-node1:9090".
/// This function binds to the advertised address when possible; if the address
/// is not a valid socket address (e.g., hostname), it falls back to
/// "0.0.0.0:PORT" to listen on all interfaces.
///
/// The `cluster_handler` is used to dispatch incoming cluster messages
/// (notifications, cache invalidation, etc.) to the application layer.
/// Both the Raft consensus service and the cluster messaging service
/// are hosted on the same gRPC port.
///
/// Returns an error if the server fails to start (e.g., port already in use).
pub async fn start_rpc_server(
    manager: Arc<RaftManager>,
    advertise_addr: String,
    cluster_handler: Arc<dyn super::cluster_handler::ClusterMessageHandler>,
    pg_service: Option<Arc<KalamPgService>>,
) -> Result<(), crate::RaftError> {
    // Extract port from advertise_addr (e.g., "kalamdb-node1:9090" -> 9090)
    let port = advertise_addr.rsplit(':').next().ok_or_else(|| {
        crate::RaftError::Internal(format!(
            "Invalid advertise address '{}': missing port",
            advertise_addr
        ))
    })?;

    let (addr, bind_addr) = match advertise_addr.parse::<std::net::SocketAddr>() {
        Ok(addr) => {
            if addr.ip().is_unspecified() {
                log::info!(
                    "RPC address '{}' uses a wildcard bind — listening on all interfaces",
                    advertise_addr
                );
            }
            (addr, advertise_addr.clone())
        },
        Err(_) => {
            let bind_addr = format!("0.0.0.0:{}", port);
            let addr: std::net::SocketAddr = bind_addr.parse().map_err(|e| {
                crate::RaftError::Internal(format!("Invalid bind address '{}': {}", bind_addr, e))
            })?;
            (addr, bind_addr)
        },
    };

    // Try to bind the TCP listener first to detect port conflicts early
    let listener = bind_rpc_listener(addr, &bind_addr).await?;

    // Raft consensus service (vote, append_entries, install_snapshot, client_proposal)
    let raft_service = RaftService::new(Arc::clone(&manager));
    let raft_server = raft_server::RaftServer::new(raft_service);

    // Cluster messaging service (forward_sql, ping, get_node_info)
    let cluster_service =
        super::cluster_handler::ClusterServiceImpl::new(cluster_handler, Arc::clone(&manager));
    let cluster_server =
        super::cluster_service::cluster_server::ClusterServer::new(cluster_service);

    let rpc_tls = manager.config().rpc_tls.clone();
    let server_tls = if rpc_tls.enabled {
        let ca_pem = rpc_tls.load_ca_cert().map_err(|e| {
            crate::RaftError::Config(format!("Failed loading cluster CA cert: {}", e))
        })?;
        let cert_pem = rpc_tls
            .load_server_cert()
            .map_err(|e| crate::RaftError::Config(format!("Failed loading node cert: {}", e)))?;
        let key_pem = rpc_tls
            .load_server_key()
            .map_err(|e| crate::RaftError::Config(format!("Failed loading node key: {}", e)))?;

        let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem));

        if rpc_tls.require_client_cert {
            tls = tls.client_ca_root(Certificate::from_pem(ca_pem));
        }

        Some(tls)
    } else {
        None
    };

    log::debug!("Starting Raft RPC server on {} (advertising as {})", addr, advertise_addr);

    // Use oneshot channel to report startup success/failure
    let (tx, rx) = oneshot::channel::<Result<(), String>>();
    let bind_addr_clone = bind_addr.clone();

    // Spawn the server in a background task
    tokio::spawn(async move {
        // Signal that we've started (we already successfully bound)
        let _ = tx.send(Ok(()));

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let mut server_builder = tonic::transport::Server::builder();
        if let Some(tls) = server_tls {
            match server_builder.tls_config(tls) {
                Ok(builder) => {
                    server_builder = builder;
                },
                Err(e) => {
                    log::error!("Failed to configure Raft RPC TLS on {}: {}", bind_addr_clone, e);
                    return;
                },
            }
        }

        let server_builder = server_builder.add_service(raft_server).add_service(cluster_server);

        let result = if let Some(pg_service) = pg_service {
            server_builder
                .add_service(PgServiceServer::new(pg_service.as_ref().clone()))
                .serve_with_incoming(incoming)
                .await
        } else {
            server_builder.serve_with_incoming(incoming).await
        };

        if let Err(e) = result {
            log::error!("Raft RPC server error on {}: {}", bind_addr_clone, e);
        }
    });

    // Wait for startup confirmation (with timeout)
    match tokio::time::timeout(tokio::time::Duration::from_secs(5), rx).await {
        Ok(Ok(Ok(()))) => {
            log::info!(
                "✓ Raft RPC server started on {} (advertising as {})",
                bind_addr,
                advertise_addr
            );
            Ok(())
        },
        Ok(Ok(Err(e))) => {
            Err(crate::RaftError::Internal(format!("Raft RPC server failed to start: {}", e)))
        },
        Ok(Err(_)) => {
            // Channel closed unexpectedly
            Err(crate::RaftError::Internal(
                "Raft RPC server startup channel closed unexpectedly".to_string(),
            ))
        },
        Err(_) => {
            // Timeout
            Err(crate::RaftError::Internal(
                "Raft RPC server startup timed out after 5 seconds".to_string(),
            ))
        },
    }
}

async fn bind_rpc_listener(
    addr: std::net::SocketAddr,
    bind_addr: &str,
) -> Result<tokio::net::TcpListener, crate::RaftError> {
    let retry_ephemeral = addr.port() == 0;
    let mut last_error = None;

    for _ in 0..20 {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => return Ok(listener),
            Err(err)
                if retry_ephemeral
                    && matches!(err.kind(), ErrorKind::AddrNotAvailable | ErrorKind::AddrInUse) =>
            {
                last_error = Some(err);
                sleep(Duration::from_millis(50)).await;
            },
            Err(err) => {
                return Err(crate::RaftError::Internal(format!(
                    "Failed to bind Raft RPC server to {}: {}. Is the port already in use?",
                    bind_addr, err
                )));
            },
        }
    }

    let err = last_error.unwrap_or_else(|| {
        std::io::Error::new(
            ErrorKind::AddrNotAvailable,
            "failed to bind ephemeral Raft RPC listener after retries",
        )
    });

    Err(crate::RaftError::Internal(format!(
        "Failed to bind Raft RPC server to {}: {}. Is the port already in use?",
        bind_addr, err
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_message() {
        let req = RaftRpcRequest {
            group_id: "MetaSystem".to_string(),
            rpc_type: "vote".to_string(),
            payload: vec![1, 2, 3],
        };

        assert_eq!(req.group_id, "MetaSystem");
        assert_eq!(req.rpc_type, "vote");
    }
}
