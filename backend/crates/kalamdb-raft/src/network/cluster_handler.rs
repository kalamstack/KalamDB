//! Cluster message handler trait
//!
//! Defines the [`ClusterMessageHandler`] trait that higher-level crates
//! (e.g. `kalamdb-core`) implement to handle incoming cluster gRPC messages.
//!
//! This decouples `kalamdb-raft` from application-level services.
//! The handler is injected at startup and called by the gRPC service impl.
//!
//! ## Adding a new handler method
//!
//! 1. Add a method to [`ClusterMessageHandler`] with a default "unimplemented" impl
//! 2. Add the corresponding gRPC method in [`super::cluster_service`]
//! 3. Wire it in [`ClusterServiceImpl`]

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::cluster_service::cluster_server::ClusterService;
use super::models::{
    ForwardSqlRequest, ForwardSqlResponse, ForwardSqlResponsePayload, GetNodeInfoRequest,
    GetNodeInfoResponse, PingRequest, PingResponse,
};
use crate::manager::RaftManager;

/// Trait for handling incoming cluster messages.
///
/// Implemented by `kalamdb-core` to dispatch cluster messages to
/// application-level services (SQL forwarding, peer liveness, node info, etc.).
///
/// Each gRPC method maps to a handler method here. New methods should
/// be added with a default implementation that returns `Unimplemented`
/// so that existing implementations are not broken.
#[async_trait::async_trait]
pub trait ClusterMessageHandler: Send + Sync + 'static {
    /// Handle SQL write forwarding from a follower to the leader.
    async fn handle_forward_sql(
        &self,
        req: ForwardSqlRequest,
    ) -> Result<ForwardSqlResponsePayload, String>;

    /// Handle peer liveness ping.
    async fn handle_ping(&self, req: PingRequest) -> Result<(), String>;

    /// Handle a request for live node statistics.
    ///
    /// The receiver should return its own local metrics (groups leading, last
    /// applied log, Raft term, system metadata, etc.).
    ///
    /// A default implementation returns an error so that existing
    /// implementations are not silently broken.
    async fn handle_get_node_info(
        &self,
        req: GetNodeInfoRequest,
    ) -> Result<GetNodeInfoResponse, String>;
}

/// gRPC service impl that delegates to a [`ClusterMessageHandler`].
///
/// This is the bridge between the tonic gRPC layer and the application handler.
pub struct ClusterServiceImpl {
    handler: Arc<dyn ClusterMessageHandler>,
    manager: Arc<RaftManager>,
}

impl ClusterServiceImpl {
    /// Create a new cluster service impl with the given handler.
    pub fn new(handler: Arc<dyn ClusterMessageHandler>, manager: Arc<RaftManager>) -> Self {
        Self { handler, manager }
    }
}

impl Clone for ClusterServiceImpl {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            manager: Arc::clone(&self.manager),
        }
    }
}

impl std::fmt::Debug for ClusterServiceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterServiceImpl").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn forward_sql(
        &self,
        request: Request<ForwardSqlRequest>,
    ) -> Result<Response<ForwardSqlResponse>, Status> {
        let req = request.into_inner();

        // Spawn on a separate task so long-running SQL execution does not block
        // the tonic worker thread handling other gRPC requests.
        let handler = Arc::clone(&self.handler);
        let result = tokio::task::spawn(async move { handler.handle_forward_sql(req).await })
            .await
            .map_err(|e| Status::internal(format!("Task join error: {}", e)))?;

        match result {
            Ok(payload) => Ok(Response::new(ForwardSqlResponse {
                status_code: payload.status_code as u32,
                body: payload.body,
                error: String::new(),
            })),
            Err(e) => {
                log::warn!("ClusterService::forward_sql handler error: {}", e);
                Ok(Response::new(ForwardSqlResponse {
                    status_code: 500,
                    body: Vec::new(),
                    error: e,
                }))
            },
        }
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();

        match self.handler.handle_ping(req).await {
            Ok(()) => Ok(Response::new(PingResponse {
                success: true,
                error: String::new(),
                node_id: self.manager.node_id().as_u64(),
            })),
            Err(e) => {
                log::warn!("ClusterService::ping handler error: {}", e);
                Ok(Response::new(PingResponse {
                    success: false,
                    error: e,
                    node_id: self.manager.node_id().as_u64(),
                }))
            },
        }
    }

    async fn get_node_info(
        &self,
        request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        let req = request.into_inner();

        match self.handler.handle_get_node_info(req).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => {
                log::warn!("ClusterService::get_node_info handler error: {}", e);
                Ok(Response::new(GetNodeInfoResponse {
                    success: false,
                    error: e,
                    node_id: self.manager.node_id().as_u64(),
                    ..Default::default()
                }))
            },
        }
    }
}

/// A no-op handler used in single-node mode or when no handler is registered.
///
/// All methods log a trace-level message and return success (no-op).
pub struct NoOpClusterHandler;

#[async_trait::async_trait]
impl ClusterMessageHandler for NoOpClusterHandler {
    async fn handle_forward_sql(
        &self,
        _req: ForwardSqlRequest,
    ) -> Result<ForwardSqlResponsePayload, String> {
        Err("forward_sql is unavailable in no-op cluster handler".to_string())
    }

    async fn handle_ping(&self, _req: PingRequest) -> Result<(), String> {
        Ok(())
    }

    async fn handle_get_node_info(
        &self,
        _req: GetNodeInfoRequest,
    ) -> Result<GetNodeInfoResponse, String> {
        Err("get_node_info is unavailable in no-op cluster handler".to_string())
    }
}
