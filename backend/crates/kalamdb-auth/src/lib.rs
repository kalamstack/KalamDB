// KalamDB Authentication Library
// Provides password hashing, JWT validation, Basic Auth, and authorization
//
// SINGLE SOURCE OF TRUTH: All authentication logic goes through the `unified` module.
// Both HTTP (/sql endpoint) and WebSocket handlers use the same authentication flow.

pub mod authorization;
pub mod errors;
pub mod helpers;
pub mod models;
pub mod providers;
pub mod repository;
pub mod security;
pub mod services;

// Re-export commonly used types
pub use errors::error::{AuthError, AuthResult};
pub use helpers::cookie::{
    create_auth_cookie, create_logout_cookie, create_refresh_cookie, create_refresh_logout_cookie,
    extract_auth_token, extract_refresh_token, CookieConfig, AUTH_COOKIE_NAME,
};
pub use helpers::extractor::{AuthExtractError, AuthSessionExtractor};
// Re-export unified session type from kalamdb-session
pub use kalamdb_session::AuthSession;
// Re-export items needed by extractor
pub use helpers::ip_extractor::{extract_client_ip_secure, is_localhost_address};
pub use models::impersonation::{ImpersonationContext, ImpersonationOrigin};
pub use providers::jwt_auth::{
    create_and_sign_refresh_token, create_and_sign_token, generate_jwt_token, refresh_jwt_token,
    JwtClaims, TokenType, DEFAULT_JWT_EXPIRY_HOURS, KALAMDB_ISSUER,
};
pub use repository::user_repo::UserRepository;
pub use services::login_tracker::{LoginTracker, LoginTrackingConfig};
pub use services::unified::{
    authenticate, extract_username_for_audit, AuthMethod, AuthRequest, AuthenticationResult,
};
