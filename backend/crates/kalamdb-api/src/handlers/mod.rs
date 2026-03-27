//! HTTP request handlers
//!
//! This module provides HTTP handlers for the KalamDB REST API and WebSocket endpoints.
//!
//! ## Handler Organization
//!
//! Handlers are organized into sub-folders by functionality:
//!
//! - `auth/` - Authentication handlers (login, logout, me, refresh, setup)
//! - `sql/` - SQL execution handlers (execute, forward, helpers)
//! - `ws/` - WebSocket handlers for live queries
//! - `topics/` - Topic pub/sub handlers (consume, ack)
//! - `cluster/` - Cluster health and management handlers
//! - `files/` - File download handlers
//! - `health/` - Health check handlers (liveness/readiness probes)
//!
//! Each sub-folder contains:
//! - `mod.rs` - Module root with exports
//! - `models/` - Request/response models
//! - Individual handler files (one per route)

// New organized handler modules
pub mod auth;
pub mod cluster;
pub mod files;
pub mod health;
pub mod sql;
pub mod topics;
pub mod ws;

// Re-export for convenient access
pub use auth::{
    login_handler, logout_handler, me_handler, refresh_handler, server_setup_handler,
    setup_status_handler,
};
pub use cluster::cluster_health_handler;
pub use files::download_export;
pub use files::download_file;
pub use health::{healthz_handler, readyz_handler};
pub use sql::execute_sql_v1;
pub use topics::{ack_handler, consume_handler};
pub use ws::websocket_handler;
