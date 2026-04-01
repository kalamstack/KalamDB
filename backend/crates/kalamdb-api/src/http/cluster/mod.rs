//! Cluster health and status handlers
//!
//! Provides endpoints for monitoring cluster health and OpenRaft metrics.
//!
//! ## Endpoints
//! - GET /v1/api/cluster/health - Cluster health with OpenRaft metrics (localhost only)
//!
//! Access is restricted to:
//! - Requests from localhost/same machine only

pub mod models;

mod health;

pub(crate) use health::cluster_health_handler;
