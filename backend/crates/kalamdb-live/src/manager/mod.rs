//! Live query manager module
//!
//! Supports both WebSocket subscriptions and HTTP long-polling consumers

pub mod connections_manager;
pub mod queries_manager;

pub use connections_manager::ConnectionsManager;
pub use queries_manager::LiveQueryManager;
