//! Unified authentication module for HTTP and WebSocket handlers.

mod audit;
mod bearer;
mod password;
mod types;

use crate::errors::error::{AuthError, AuthResult};
use crate::models::context::AuthenticatedUser;
use crate::providers::jwt_config;
use crate::repository::user_repo::UserRepository;
use crate::services::login_tracker::LoginTracker;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tracing::Instrument;

use bearer::authenticate_bearer;
use password::authenticate_user_password;
pub use types::{AuthMethod, AuthRequest, AuthenticationResult};

pub use audit::extract_user_id_for_audit;

/// Cached login tracker instance.
static LOGIN_TRACKER: Lazy<LoginTracker> = Lazy::new(LoginTracker::new);

/// Initialize auth configuration from server settings.
pub fn init_auth_config(
    auth: &kalamdb_configs::AuthSettings,
    oauth: &kalamdb_configs::OAuthSettings,
) {
    let mut issuer_audiences = std::collections::HashMap::new();

    if let Some(client_id) = &oauth.providers.google.client_id {
        if !oauth.providers.google.issuer.is_empty() {
            issuer_audiences.insert(oauth.providers.google.issuer.clone(), client_id.clone());
        }
    }
    if let Some(client_id) = &oauth.providers.github.client_id {
        if !oauth.providers.github.issuer.is_empty() {
            issuer_audiences.insert(oauth.providers.github.issuer.clone(), client_id.clone());
        }
    }
    if let Some(client_id) = &oauth.providers.azure.client_id {
        if !oauth.providers.azure.issuer.is_empty() {
            issuer_audiences.insert(oauth.providers.azure.issuer.clone(), client_id.clone());
        }
    }
    if let Some(client_id) = &oauth.providers.firebase.client_id {
        if !oauth.providers.firebase.issuer.is_empty() {
            issuer_audiences.insert(oauth.providers.firebase.issuer.clone(), client_id.clone());
        }
    }

    jwt_config::init_jwt_config(&auth.jwt_secret, &auth.jwt_trusted_issuers, issuer_audiences);
}

/// Authenticate a request using the unified authentication flow.
pub async fn authenticate(
    request: AuthRequest,
    connection_info: &kalamdb_commons::models::ConnectionInfo,
    repo: &Arc<dyn UserRepository>,
) -> AuthResult<AuthenticationResult> {
    let request_kind = match &request {
        AuthRequest::Header(_) => "header",
        AuthRequest::Credentials { .. } => "credentials",
        AuthRequest::Jwt { .. } => "jwt",
    };

    let span = tracing::info_span!(
        "auth.check",
        auth_request_kind = request_kind,
        is_localhost = connection_info.is_localhost(),
        role = tracing::field::Empty,
        user = tracing::field::Empty
    );

    async move {
        match request {
            AuthRequest::Header(header) => {
                authenticate_header(&header, connection_info, repo).await
            },
            AuthRequest::Credentials { user, password } => {
                authenticate_credentials(&user, &password, connection_info, repo).await
            },
            AuthRequest::Jwt { token } => {
                let user = authenticate_bearer(&token, connection_info, repo).await?;
                record_authenticated_span(&user);
                Ok(AuthenticationResult {
                    user,
                    method: AuthMethod::Bearer,
                })
            },
        }
    }
    .instrument(span)
    .await
}

async fn authenticate_header(
    auth_header: &str,
    connection_info: &kalamdb_commons::models::ConnectionInfo,
    repo: &Arc<dyn UserRepository>,
) -> AuthResult<AuthenticationResult> {
    if auth_header.starts_with("Basic ") {
        Err(AuthError::InvalidCredentials(
            "Basic authentication is not supported. Use Bearer token or login endpoint."
                .to_string(),
        ))
    } else if auth_header.starts_with("Bearer") {
        let token = auth_header.strip_prefix("Bearer").unwrap_or("").trim();
        if token.is_empty() {
            return Err(AuthError::MalformedAuthorization("Bearer token missing".to_string()));
        }

        let user = authenticate_bearer(token, connection_info, repo).await?;
        record_authenticated_span(&user);

        Ok(AuthenticationResult {
            user,
            method: AuthMethod::Bearer,
        })
    } else {
        Err(AuthError::MalformedAuthorization(
            "Authorization header must start with 'Basic ' or 'Bearer '".to_string(),
        ))
    }
}

async fn authenticate_credentials(
    user_id_str: &str,
    password: &str,
    connection_info: &kalamdb_commons::models::ConnectionInfo,
    repo: &Arc<dyn UserRepository>,
) -> AuthResult<AuthenticationResult> {
    let user = authenticate_user_password(user_id_str, password, connection_info, repo).await?;
    Ok(AuthenticationResult {
        user,
        method: AuthMethod::Direct,
    })
}

fn record_authenticated_span(user: &AuthenticatedUser) {
    tracing::Span::current().record("role", format!("{:?}", user.role).as_str());
    tracing::Span::current().record("user", user.user_id.as_str());
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_commons::UserId;

    #[test]
    fn test_auth_method_debug() {
        assert_eq!(format!("{:?}", AuthMethod::Basic), "Basic");
        assert_eq!(format!("{:?}", AuthMethod::Bearer), "Bearer");
        assert_eq!(format!("{:?}", AuthMethod::Direct), "Direct");
    }

    #[test]
    fn test_extract_user_id_from_credentials() {
        let request = AuthRequest::Credentials {
            user: "testuser".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(extract_user_id_for_audit(&request), UserId::from("testuser"));
    }

    #[test]
    fn test_extract_user_id_from_bearer_header() {
        let request = AuthRequest::Header("Bearer some.jwt.token".to_string());
        assert_eq!(extract_user_id_for_audit(&request), UserId::anonymous());
    }

    #[test]
    fn test_extract_user_id_from_jwt_with_sub() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(r#"{"sub":"user_from_sub","exp":9999999999}"#);
        let signature = "fake_signature";

        let token = format!("{}.{}.{}", header, payload, signature);
        let request = AuthRequest::Jwt { token };
        assert_eq!(extract_user_id_for_audit(&request), UserId::from("user_from_sub"));
    }

    #[test]
    fn test_extract_user_id_from_invalid_jwt() {
        let request = AuthRequest::Jwt {
            token: "invalid_token".to_string(),
        };
        assert_eq!(extract_user_id_for_audit(&request), UserId::anonymous());
    }

    #[test]
    fn test_extract_user_id_from_bearer_header_with_jwt() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(r#"{"sub":"bearer_user","exp":9999999999}"#);
        let signature = "fake_signature";

        let token = format!("{}.{}.{}", header, payload, signature);
        let request = AuthRequest::Header(format!("Bearer {}", token));
        assert_eq!(extract_user_id_for_audit(&request), UserId::from("bearer_user"));
    }

    #[cfg(feature = "websocket")]
    #[test]
    fn test_from_ws_auth_credentials_jwt() {
        use kalamdb_commons::websocket_auth::WsAuthCredentials;

        let ws_creds = WsAuthCredentials::Jwt {
            token: "my.jwt.token".to_string(),
        };

        let auth_request: AuthRequest = ws_creds.into();
        match auth_request {
            AuthRequest::Jwt { token } => {
                assert_eq!(token, "my.jwt.token");
            },
            _ => panic!("Expected Jwt variant"),
        }
    }
}
