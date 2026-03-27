//! Raft Network Implementation
//!
//! Provides the network transport for Raft RPCs using gRPC (tonic).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{
    RPCOption, RaftNetwork as OpenRaftNetwork, RaftNetworkFactory as OpenRaftNetworkFactory,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

use kalamdb_commons::models::NodeId;

use crate::manager::{PeerNode, RpcTlsConfig};
use crate::storage::{KalamNode, KalamTypeConfig};
use crate::GroupId;

/// Simple connection error wrapper for openraft compatibility
#[derive(Debug)]
struct ConnectionError(String);

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConnectionError {}

#[derive(Debug, Clone)]
struct RpcAuthIdentity {
    cluster_id: String,
    node_id: NodeId,
}

#[derive(Debug, Clone)]
struct RpcTlsMaterial {
    ca_pem: Vec<u8>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    peer_server_names: HashMap<NodeId, String>,
}

#[derive(Debug)]
struct ConnectionState {
    last_attempt: Instant,
    last_log: Instant,
    retry_count: u64,
    is_unreachable: bool,
}

impl ConnectionState {
    fn new(now: Instant, retry_interval: Duration) -> Self {
        let initial = now.checked_sub(retry_interval).unwrap_or(now);
        Self {
            last_attempt: initial,
            last_log: initial,
            retry_count: 0,
            is_unreachable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionTracker {
    group_id: GroupId,
    retry_interval_ms: Arc<AtomicU64>,
    states: Arc<dashmap::DashMap<u64, ConnectionState>>,
}

impl ConnectionTracker {
    fn new(group_id: GroupId, retry_interval: Duration) -> Self {
        Self {
            group_id,
            retry_interval_ms: Arc::new(AtomicU64::new(retry_interval.as_millis() as u64)),
            states: Arc::new(dashmap::DashMap::new()),
        }
    }

    fn set_retry_interval(&self, interval: Duration) {
        self.retry_interval_ms.store(interval.as_millis() as u64, Ordering::Relaxed);
    }

    fn retry_interval(&self) -> Duration {
        Duration::from_millis(self.retry_interval_ms.load(Ordering::Relaxed).max(1))
    }

    fn should_attempt(&self, target: u64) -> bool {
        let now = Instant::now();
        let retry_interval = self.retry_interval();
        let mut entry = self
            .states
            .entry(target)
            .or_insert_with(|| ConnectionState::new(now, retry_interval));

        if !entry.is_unreachable {
            entry.last_attempt = now;
            return true;
        }

        if now.duration_since(entry.last_attempt) >= retry_interval {
            entry.last_attempt = now;
            true
        } else {
            false
        }
    }

    fn record_failure(&self, target: u64, error: &str) {
        let now = Instant::now();
        let retry_interval = self.retry_interval();
        let mut entry = self
            .states
            .entry(target)
            .or_insert_with(|| ConnectionState::new(now, retry_interval));

        entry.retry_count = entry.retry_count.saturating_add(1);
        entry.is_unreachable = true;

        if now.duration_since(entry.last_log) >= retry_interval {
            entry.last_log = now;
            log::warn!(
                "Raft node {} in group {} left the cluster - trying to reconnect #{} (interval={}ms): {}",
                target,
                self.group_id,
                entry.retry_count,
                retry_interval.as_millis(),
                error
            );
        }
    }

    fn record_success(&self, target: u64) {
        let now = Instant::now();
        let retry_interval = self.retry_interval();
        let mut entry = self
            .states
            .entry(target)
            .or_insert_with(|| ConnectionState::new(now, retry_interval));

        if entry.is_unreachable {
            log::info!(
                "Raft node {} in group {} reconnected after {} retries",
                target,
                self.group_id,
                entry.retry_count
            );
        }

        entry.retry_count = 0;
        entry.is_unreachable = false;
    }
}

/// Network implementation for a single Raft group
pub struct RaftNetwork {
    /// Target node ID
    target: u64,
    /// Group ID for this network
    group_id: GroupId,
    /// Connect channel
    channel: Channel,
    /// Connection retry tracker
    connection_tracker: ConnectionTracker,
    /// Outgoing RPC auth identity (node + cluster)
    auth_identity: RpcAuthIdentity,
}

impl RaftNetwork {
    /// Create a new network instance
    fn new(
        target: u64,
        _target_node: KalamNode,
        group_id: GroupId,
        channel: Channel,
        connection_tracker: ConnectionTracker,
        auth_identity: RpcAuthIdentity,
    ) -> Self {
        Self {
            target,
            group_id,
            channel,
            connection_tracker,
            auth_identity,
        }
    }

    /// Get the gRPC channel
    fn get_channel(&self) -> Result<Channel, ConnectionError> {
        Ok(self.channel.clone())
    }

    fn add_outgoing_rpc_metadata<T>(
        &self,
        request: &mut tonic::Request<T>,
    ) -> Result<(), ConnectionError> {
        let cluster_id =
            tonic::metadata::MetadataValue::try_from(self.auth_identity.cluster_id.as_str())
                .map_err(|e| ConnectionError(format!("Invalid cluster_id metadata: {}", e)))?;
        let node_id = tonic::metadata::MetadataValue::try_from(
            self.auth_identity.node_id.as_u64().to_string(),
        )
        .map_err(|e| ConnectionError(format!("Invalid node_id metadata: {}", e)))?;

        request.metadata_mut().insert("x-kalamdb-cluster-id", cluster_id);
        request.metadata_mut().insert("x-kalamdb-node-id", node_id);
        Ok(())
    }
}

impl OpenRaftNetwork<KalamTypeConfig> for RaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KalamTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, KalamNode, RaftError<u64>>> {
        if !self.connection_tracker.should_attempt(self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&ConnectionError(
                "reconnect backoff".to_string(),
            ))));
        }

        // Get channel
        let channel =
            self.get_channel().map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Serialize request
        let request_bytes = crate::state_machine::encode(&rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Create gRPC request
        let mut client = crate::network::service::raft_client::RaftClient::new(channel);

        let mut grpc_request = tonic::Request::new(crate::network::service::RaftRpcRequest {
            group_id: self.group_id.to_string(),
            rpc_type: "append_entries".to_string(),
            payload: request_bytes,
        });
        self.add_outgoing_rpc_metadata(&mut grpc_request)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Send request
        let response = match client.raft_rpc(grpc_request).await {
            Ok(response) => {
                self.connection_tracker.record_success(self.target);
                response
            },
            Err(e) => {
                self.connection_tracker.record_failure(self.target, &e.to_string());
                return Err(RPCError::Network(NetworkError::new(&e)));
            },
        };

        // Deserialize response
        let inner = response.into_inner();
        if !inner.error.is_empty() {
            self.connection_tracker.record_failure(self.target, &inner.error);
            return Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )));
        }

        let result: AppendEntriesResponse<u64> = crate::state_machine::decode(&inner.payload)
            .map_err(|e| {
                self.connection_tracker.record_failure(self.target, &e.to_string());
                RPCError::Network(NetworkError::new(&e))
            })?;

        Ok(result)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<KalamTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, KalamNode, RaftError<u64, InstallSnapshotError>>,
    > {
        if !self.connection_tracker.should_attempt(self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&ConnectionError(
                "reconnect backoff".to_string(),
            ))));
        }

        // Get channel
        let channel =
            self.get_channel().map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Serialize request
        let request_bytes = crate::state_machine::encode(&rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Create gRPC request
        let mut client = crate::network::service::raft_client::RaftClient::new(channel);

        let mut grpc_request = tonic::Request::new(crate::network::service::RaftRpcRequest {
            group_id: self.group_id.to_string(),
            rpc_type: "install_snapshot".to_string(),
            payload: request_bytes,
        });
        self.add_outgoing_rpc_metadata(&mut grpc_request)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Send request
        let response = match client.raft_rpc(grpc_request).await {
            Ok(response) => {
                self.connection_tracker.record_success(self.target);
                response
            },
            Err(e) => {
                self.connection_tracker.record_failure(self.target, &e.to_string());
                return Err(RPCError::Network(NetworkError::new(&e)));
            },
        };

        // Deserialize response
        let inner = response.into_inner();
        if !inner.error.is_empty() {
            self.connection_tracker.record_failure(self.target, &inner.error);
            return Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )));
        }

        let result: InstallSnapshotResponse<u64> = crate::state_machine::decode(&inner.payload)
            .map_err(|e| {
                self.connection_tracker.record_failure(self.target, &e.to_string());
                RPCError::Network(NetworkError::new(&e))
            })?;

        Ok(result)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, KalamNode, RaftError<u64>>> {
        if !self.connection_tracker.should_attempt(self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&ConnectionError(
                "reconnect backoff".to_string(),
            ))));
        }

        // Get channel
        let channel =
            self.get_channel().map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Serialize request
        let request_bytes = crate::state_machine::encode(&rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Create gRPC request
        let mut client = crate::network::service::raft_client::RaftClient::new(channel);

        let mut grpc_request = tonic::Request::new(crate::network::service::RaftRpcRequest {
            group_id: self.group_id.to_string(),
            rpc_type: "vote".to_string(),
            payload: request_bytes,
        });
        self.add_outgoing_rpc_metadata(&mut grpc_request)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // Send request
        let response = match client.raft_rpc(grpc_request).await {
            Ok(response) => {
                self.connection_tracker.record_success(self.target);
                response
            },
            Err(e) => {
                self.connection_tracker.record_failure(self.target, &e.to_string());
                return Err(RPCError::Network(NetworkError::new(&e)));
            },
        };

        // Deserialize response
        let inner = response.into_inner();
        if !inner.error.is_empty() {
            self.connection_tracker.record_failure(self.target, &inner.error);
            return Err(RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(openraft::error::Fatal::Panicked),
            )));
        }

        let result: VoteResponse<u64> =
            crate::state_machine::decode(&inner.payload).map_err(|e| {
                self.connection_tracker.record_failure(self.target, &e.to_string());
                RPCError::Network(NetworkError::new(&e))
            })?;

        Ok(result)
    }
}

/// Factory for creating network instances
#[derive(Clone)]
pub struct RaftNetworkFactory {
    /// Group ID for this factory
    group_id: GroupId,
    /// Known nodes in the cluster
    nodes: Arc<RwLock<HashMap<NodeId, KalamNode>>>,
    /// Cached gRPC channels (node_id -> channel)
    channels: Arc<dashmap::DashMap<NodeId, Channel>>,
    /// Connection retry tracker
    connection_tracker: ConnectionTracker,
    /// Outgoing RPC auth identity (set during group start)
    auth_identity: Arc<RwLock<Option<RpcAuthIdentity>>>,
    /// Optional TLS client material + peer server-name mappings
    tls_material: Arc<RwLock<Option<RpcTlsMaterial>>>,
}

impl RaftNetworkFactory {
    /// Create a new network factory
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            channels: Arc::new(dashmap::DashMap::new()),
            connection_tracker: ConnectionTracker::new(group_id, Duration::from_secs(3)),
            auth_identity: Arc::new(RwLock::new(None)),
            tls_material: Arc::new(RwLock::new(None)),
        }
    }

    /// Configure the minimum interval between reconnect attempts
    pub fn set_reconnect_interval(&self, interval: Duration) {
        self.connection_tracker.set_retry_interval(interval);
    }

    /// Configure outgoing RPC metadata identity for this node.
    pub fn configure_rpc_auth_identity(&self, node_id: NodeId, cluster_id: String) {
        let mut guard = self.auth_identity.write();
        *guard = Some(RpcAuthIdentity {
            cluster_id,
            node_id,
        });
    }

    /// Add outgoing node identity metadata to a gRPC request.
    pub fn add_outgoing_rpc_metadata<T>(
        &self,
        request: &mut tonic::Request<T>,
    ) -> Result<(), crate::RaftError> {
        let auth_identity = self.auth_identity.read().clone().ok_or_else(|| {
            crate::RaftError::Config("RPC auth identity is not configured".to_string())
        })?;

        let cluster_id = tonic::metadata::MetadataValue::try_from(
            auth_identity.cluster_id.as_str(),
        )
        .map_err(|e| crate::RaftError::Internal(format!("Invalid cluster_id metadata: {}", e)))?;
        let node_id =
            tonic::metadata::MetadataValue::try_from(auth_identity.node_id.as_u64().to_string())
                .map_err(|e| {
                    crate::RaftError::Internal(format!("Invalid node_id metadata: {}", e))
                })?;

        request.metadata_mut().insert("x-kalamdb-cluster-id", cluster_id);
        request.metadata_mut().insert("x-kalamdb-node-id", node_id);
        Ok(())
    }

    /// Configure TLS/mTLS material for outgoing channels.
    pub fn configure_rpc_tls(
        &self,
        tls: &RpcTlsConfig,
        peers: &[PeerNode],
    ) -> Result<(), crate::RaftError> {
        if !tls.enabled {
            let mut guard = self.tls_material.write();
            *guard = None;
            self.channels.clear();
            return Ok(());
        }

        let ca_pem = tls.load_ca_cert().map_err(|e| {
            crate::RaftError::Config(format!("Failed loading cluster CA cert: {}", e))
        })?;
        let cert_pem = tls.load_server_cert().map_err(|e| {
            crate::RaftError::Config(format!("Failed loading node cert: {}", e))
        })?;
        let key_pem = tls.load_server_key().map_err(|e| {
            crate::RaftError::Config(format!("Failed loading node key: {}", e))
        })?;

        let mut peer_server_names = HashMap::new();
        for peer in peers {
            let server_name = if let Some(name) = peer.rpc_server_name.as_ref() {
                name.clone()
            } else {
                Self::rpc_host_from_addr(&peer.rpc_addr)?
            };
            peer_server_names.insert(peer.node_id, server_name);
        }

        let mut guard = self.tls_material.write();
        *guard = Some(RpcTlsMaterial {
            ca_pem,
            cert_pem,
            key_pem,
            peer_server_names,
        });
        self.channels.clear();
        Ok(())
    }

    fn rpc_host_from_addr(rpc_addr: &str) -> Result<String, crate::RaftError> {
        let (host, _port) = rpc_addr.rsplit_once(':').ok_or_else(|| {
            crate::RaftError::Config(format!("Invalid rpc_addr '{}': expected host:port", rpc_addr))
        })?;
        if host.is_empty() {
            return Err(crate::RaftError::Config(format!(
                "Invalid rpc_addr '{}': host is empty",
                rpc_addr
            )));
        }
        Ok(host.to_string())
    }

    fn build_channel(&self, node_id: NodeId, node: &KalamNode) -> Result<Channel, ConnectionError> {
        let tls_material = self.tls_material.read().clone();
        let (scheme, endpoint) = if tls_material.is_some() {
            ("https", format!("https://{}", node.rpc_addr))
        } else {
            ("http", format!("http://{}", node.rpc_addr))
        };

        let mut endpoint = Channel::from_shared(endpoint)
            .map_err(|e| ConnectionError(format!("Invalid {} endpoint: {}", scheme, e)))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));

        if let Some(material) = tls_material {
            let server_name =
                material.peer_server_names.get(&node_id).cloned().unwrap_or_else(|| {
                    Self::rpc_host_from_addr(&node.rpc_addr)
                        .unwrap_or_else(|_| "localhost".to_string())
                });
            let tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(material.ca_pem))
                .identity(Identity::from_pem(material.cert_pem, material.key_pem))
                .domain_name(server_name);
            endpoint = endpoint
                .tls_config(tls_config)
                .map_err(|e| ConnectionError(format!("Failed configuring TLS endpoint: {}", e)))?;
        }

        Ok(endpoint.connect_lazy())
    }

    /// Register a node in the cluster
    pub fn register_node(&self, node_id: NodeId, node: KalamNode) {
        let mut nodes = self.nodes.write();
        nodes.insert(node_id, node);
    }

    /// Remove a node from the cluster
    pub fn unregister_node(&self, node_id: NodeId) {
        let mut nodes = self.nodes.write();
        nodes.remove(&node_id);
        self.channels.remove(&node_id);
    }

    /// Get node info by node ID (for leader forwarding)
    pub fn get_node(&self, node_id: NodeId) -> Option<KalamNode> {
        let nodes = self.nodes.read();
        nodes.get(&node_id).cloned()
    }

    /// Get or create a gRPC channel to a peer node.
    ///
    /// This is used by [`super::cluster_client::ClusterClient`] for non-Raft
    /// cluster communication. Channels are lazily connected and cached.
    pub fn get_or_create_channel(&self, node_id: NodeId) -> Option<Channel> {
        // Return existing cached channel
        if let Some(ch) = self.channels.get(&node_id) {
            return Some(ch.clone());
        }

        // Look up the node's rpc_addr and create a lazy channel
        let node = {
            let nodes = self.nodes.read();
            nodes.get(&node_id).cloned()
        };

        let node = node?;
        let ch = match self.build_channel(node_id, &node) {
            Ok(ch) => ch,
            Err(e) => {
                log::error!(
                    "Failed to build channel for node {} in group {}: {}",
                    node_id,
                    self.group_id,
                    e
                );
                return None;
            },
        };

        self.channels.insert(node_id, ch.clone());
        Some(ch)
    }

    /// Get all registered peer node IDs and their info.
    ///
    /// Used by [`super::cluster_client::ClusterClient`] for broadcasting.
    pub fn get_all_peers(&self) -> Vec<(NodeId, KalamNode)> {
        let nodes = self.nodes.read();
        nodes.iter().map(|(&id, node)| (id, node.clone())).collect()
    }
}

impl OpenRaftNetworkFactory<KalamTypeConfig> for RaftNetworkFactory {
    type Network = RaftNetwork;

    async fn new_client(&mut self, target: u64, node: &KalamNode) -> Self::Network {
        // Register the node if not already known
        let target_node_id = NodeId::from(target);
        self.register_node(target_node_id, node.clone());

        // Get or create channel
        let channel = if let Some(ch) = self.channels.get(&target_node_id) {
            ch.clone()
        } else {
            let ch = self.build_channel(target_node_id, node).unwrap_or_else(|e| {
                log::error!(
                    "[group {}] Cannot build gRPC channel to node {}: {}. \
                        Check network configuration and peer addresses.",
                    self.group_id,
                    target_node_id,
                    e
                );
                // Channel creation is a fatal configuration error; the Raft node
                // cannot communicate with its peers without a valid channel.
                panic!("Failed to build channel for node {}: {}", target_node_id, e)
            });

            self.channels.insert(target_node_id, ch.clone());
            ch
        };

        let auth_identity = self.auth_identity.read().clone().unwrap_or_else(|| {
            log::error!(
                "[group {}] RPC auth identity is not configured. \
                Ensure configure_rpc_auth_identity() is called before the Raft node starts.",
                self.group_id
            );
            panic!("RPC auth identity is not configured for group {}", self.group_id)
        });

        RaftNetwork::new(
            target,
            node.clone(),
            self.group_id,
            channel,
            self.connection_tracker.clone(),
            auth_identity,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_factory_creation() {
        let factory = RaftNetworkFactory::new(GroupId::Meta);

        factory.register_node(NodeId::from(1), KalamNode::new("127.0.0.1:9000", "127.0.0.1:8080"));

        let nodes = factory.nodes.read();
        assert!(nodes.contains_key(&NodeId::from(1)));
    }
}
