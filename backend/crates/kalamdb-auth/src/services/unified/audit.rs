use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use kalamdb_commons::UserId;

use super::types::AuthRequest;

/// Extract user ID from auth request for audit logging.
pub fn extract_user_id_for_audit(request: &AuthRequest) -> UserId {
    match request {
        AuthRequest::Header(header) => {
            if header.starts_with("Bearer ") {
                extract_jwt_sub_unsafe(header.strip_prefix("Bearer ").unwrap_or(""))
            } else {
                UserId::anonymous()
            }
        },
        AuthRequest::Credentials { user, .. } => {
            UserId::try_new(user.clone()).unwrap_or_else(|_| UserId::anonymous())
        },
        AuthRequest::Jwt { token } => extract_jwt_sub_unsafe(token),
    }
}

fn extract_jwt_sub_unsafe(token: &str) -> UserId {
    let mut parts = token.splitn(3, '.');
    let _header = parts.next();
    let payload = parts.next();
    let signature = parts.next();
    if payload.is_none() || signature.is_none() {
        return UserId::anonymous();
    }

    if let Ok(payload_bytes) = URL_SAFE_NO_PAD.decode(payload.unwrap()) {
        if let Ok(payload_str) = String::from_utf8(payload_bytes) {
            if let Ok(claims) = serde_json::from_str::<serde_json::Value>(&payload_str) {
                if let Some(sub) = claims.get("sub").and_then(|v| v.as_str()) {
                    return UserId::try_new(sub.to_string())
                        .unwrap_or_else(|_| UserId::anonymous());
                }
            }
        }
    }
    UserId::anonymous()
}
