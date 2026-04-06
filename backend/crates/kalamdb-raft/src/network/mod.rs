//! Raft Network Layer
//!
//! This module provides gRPC-based networking for Raft communication
//! between cluster nodes.
//!
//! ## Components
//!
//! ### Raft Consensus (service `kalamdb.raft.Raft`)
//!
//! - [`RaftNetwork`]: Network implementation for a single Raft group
//! - [`RaftNetworkFactory`]: Creates network instances for each group
//! - [`RaftService`]: gRPC service for handling incoming Raft RPCs
//! - [`start_rpc_server`]: Starts the gRPC server for incoming Raft RPCs
//! - [`ClientProposalRequest`], [`ClientProposalResponse`]: Types for leader forwarding
//!
//! ### Cluster Messaging (service `kalamdb.cluster.ClusterService`)
//!
//! - [`cluster_service`]: gRPC message types and service definition
//! - [`cluster_handler`]: Handler trait for incoming cluster messages
//! - [`cluster_client`]: Outbound client for sending cluster messages

#[allow(clippy::module_inception)]
mod network;
pub mod service;

// Cluster messaging modules
pub mod cluster_client;
pub mod cluster_handler;
pub mod cluster_service;
pub mod models;

pub use network::{RaftNetwork, RaftNetworkFactory};
pub use service::raft_client::RaftClient;
pub use service::{start_rpc_server, ClientProposalRequest, ClientProposalResponse, RaftService};

// Cluster messaging re-exports
pub use cluster_client::ClusterClient;
pub use cluster_handler::{ClusterMessageHandler, ClusterServiceImpl, NoOpClusterHandler};
pub use models::{
    forward_sql_param, ForwardSqlParam, ForwardSqlRequest, ForwardSqlResponse,
    ForwardSqlResponsePayload, GetNodeInfoRequest, GetNodeInfoResponse, PingRequest,
    PingResponse,
};
