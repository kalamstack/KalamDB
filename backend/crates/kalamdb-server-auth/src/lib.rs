//! Server-to-server authentication for KalamDB gRPC endpoints.
//!
//! This crate centralises mTLS caller identification so that both the
//! Raft inter-node service and the PostgreSQL extension bridge share a
//! single code path for TLS certificate validation and CN-based routing.
//!
//! # Caller types
//!
//! After the TLS handshake the server reads the client certificate CN:
//!
//! | CN pattern              | Caller type    |
//! |-------------------------|----------------|
//! | `kalamdb-node-{id}`     | Cluster node   |
//! | `kalamdb-pg-{name}`     | PG extension   |
//! | anything else           | Rejected       |

mod caller;
mod cn;

pub use caller::{CallerIdentity, RpcCaller};
pub use cn::extract_cn_from_der;
