use std::sync::Arc;

use chrono::Utc;
use kalamdb_commons::{
    models::{AuditLogId, ConnectionInfo},
    UserId,
};
use kalamdb_core::app_context::AppContext;
use kalamdb_system::AuditLogEntry;
use uuid::Uuid;

pub(crate) async fn record_admin_login(
    app_context: &Arc<AppContext>,
    user_id: &UserId,
    connection_info: &ConnectionInfo,
) {
    let timestamp = Utc::now().timestamp_millis();
    let entry = AuditLogEntry {
        audit_id: AuditLogId::from(format!("audit_{}_{}", timestamp, Uuid::new_v4().simple())),
        timestamp,
        actor_user_id: user_id.clone(),
        action: "LOGIN".to_string(),
        target: format!("user:{}", user_id.as_str()),
        details: None,
        ip_address: connection_info.remote_addr.as_deref().map(ToString::to_string),
        subject_user_id: None,
    };

    if let Err(error) = app_context.system_tables().audit_logs().append_async(entry).await {
        log::warn!("Failed to persist admin login audit entry: {}", error);
    }
}
