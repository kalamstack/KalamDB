use std::ops::Deref;

use tonic::{Request, Status};

use crate::cn::extract_cn_from_der;

/// CN prefix for PostgreSQL FDW extension clients.
const PG_CN_PREFIX: &str = "kalamdb-pg-";
/// CN prefix for cluster nodes.
const NODE_CN_PREFIX: &str = "kalamdb-node-";

/// Identified caller after mTLS certificate validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerIdentity {
    /// PostgreSQL extension bridge (CN = `kalamdb-pg-{name}`).
    PgExtension { name: String },
    /// Cluster node (CN = `kalamdb-node-{id}`).
    ClusterNode { raw_id: String },
}

/// Server-to-server RPC caller identification.
///
/// Validates the peer TLS certificate and classifies the caller based on
/// its CN pattern. All server-to-server auth logic lives here.
pub struct RpcCaller;

impl RpcCaller {
    /// Identify the caller from a gRPC request's peer certificate.
    ///
    /// Returns the caller identity on success, or a gRPC `Status` error
    /// if the peer certificate is missing, unparseable, or has an
    /// unrecognised CN pattern.
    pub fn identify<T>(request: &Request<T>) -> Result<CallerIdentity, Status> {
        let peer_certs = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("Missing peer TLS certificate"))?;
        if peer_certs.is_empty() {
            return Err(Status::unauthenticated("Missing peer TLS certificate"));
        }

        let cert_der = peer_certs[0].deref();
        let cn = extract_cn_from_der(cert_der)
            .map_err(|e| Status::unauthenticated(format!("Invalid peer certificate: {e}")))?;

        Self::classify_cn(&cn)
    }

    /// Require that the peer is a PG extension client.
    ///
    /// Convenience wrapper that calls [`identify`] and rejects non-PG callers.
    pub fn require_pg_extension<T>(request: &Request<T>) -> Result<String, Status> {
        match Self::identify(request)? {
            CallerIdentity::PgExtension { name } => {
                log::info!("mTLS authorized PG extension client: CN=kalamdb-pg-{}", name);
                Ok(name)
            },
            CallerIdentity::ClusterNode { raw_id } => Err(Status::permission_denied(format!(
                "Cluster node 'kalamdb-node-{}' is not a valid PG extension identity",
                raw_id
            ))),
        }
    }

    /// Require that the peer is a cluster node.
    ///
    /// Convenience wrapper that calls [`identify`] and rejects non-node callers.
    pub fn require_cluster_node<T>(request: &Request<T>) -> Result<String, Status> {
        match Self::identify(request)? {
            CallerIdentity::ClusterNode { raw_id } => Ok(raw_id),
            CallerIdentity::PgExtension { name } => Err(Status::permission_denied(format!(
                "PG extension 'kalamdb-pg-{}' is not a valid cluster node identity",
                name
            ))),
        }
    }

    /// Classify a CN string into a caller identity.
    fn classify_cn(cn: &str) -> Result<CallerIdentity, Status> {
        if let Some(name) = cn.strip_prefix(PG_CN_PREFIX) {
            if name.is_empty() {
                return Err(Status::permission_denied("PG extension CN has empty name suffix"));
            }
            return Ok(CallerIdentity::PgExtension {
                name: name.to_string(),
            });
        }

        if let Some(raw_id) = cn.strip_prefix(NODE_CN_PREFIX) {
            if raw_id.is_empty() {
                return Err(Status::permission_denied("Cluster node CN has empty id suffix"));
            }
            return Ok(CallerIdentity::ClusterNode {
                raw_id: raw_id.to_string(),
            });
        }

        Err(Status::permission_denied(format!(
            "Unrecognised peer CN '{}': expected 'kalamdb-pg-{{name}}' or 'kalamdb-node-{{id}}'",
            cn
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_pg_extension() {
        let id = RpcCaller::classify_cn("kalamdb-pg-myapp").unwrap();
        assert_eq!(
            id,
            CallerIdentity::PgExtension {
                name: "myapp".to_string(),
            }
        );
    }

    #[test]
    fn classify_cluster_node() {
        let id = RpcCaller::classify_cn("kalamdb-node-3").unwrap();
        assert_eq!(
            id,
            CallerIdentity::ClusterNode {
                raw_id: "3".to_string(),
            }
        );
    }

    #[test]
    fn classify_unknown_cn() {
        let err = RpcCaller::classify_cn("random-cert").unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn classify_empty_pg_name_rejected() {
        let err = RpcCaller::classify_cn("kalamdb-pg-").unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn classify_empty_node_id_rejected() {
        let err = RpcCaller::classify_cn("kalamdb-node-").unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}
