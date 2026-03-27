//! Connection-level models: options, protocol messages, HTTP utilities.

pub mod client_message;
pub mod connection_options;
pub mod health_check_response;
pub mod server_message;

pub use client_message::ClientMessage;
pub use connection_options::{ConnectionOptions, HttpVersion};
pub use health_check_response::HealthCheckResponse;
pub use server_message::ServerMessage;
