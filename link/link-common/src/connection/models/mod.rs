//! Connection-level models: options, protocol messages, HTTP utilities.

pub mod client_message;
pub mod cluster_health_response;
pub mod connection_options;
pub mod health_check_response;
pub mod protocol;
pub mod server_message;

pub use client_message::ClientMessage;
pub use cluster_health_response::{ClusterHealthResponse, ClusterNodeHealth};
pub use connection_options::{ConnectionOptions, HttpVersion};
pub use health_check_response::HealthCheckResponse;
pub use protocol::{CompressionType, ProtocolOptions, SerializationType};
pub use server_message::ServerMessage;
