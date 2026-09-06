//! Immutable, host-authenticated procedure identity.

use kalamdb_commons::{NamespaceId, Role, UserId};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationMetadata {
    pub caller:         UserId,
    pub effective_user: UserId,
    pub role:           Role,
    pub namespace:      NamespaceId,
    pub request_id:     String,
}
