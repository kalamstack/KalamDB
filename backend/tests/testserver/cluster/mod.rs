//! Cluster Command and View Tests
//!
//! Tests covering:
//! - CLUSTER commands over HTTP
//! - system.cluster and system.cluster_groups views
//! - Snapshot creation and reuse

// Re-export test_support from parent
pub(super) use super::test_support;

mod test_cluster_commands_http;
mod test_cluster_health_http;
mod test_cluster_snapshots_http;
mod test_cluster_views_http;
