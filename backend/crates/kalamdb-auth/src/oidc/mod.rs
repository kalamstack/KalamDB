//! Internal OIDC support for external JWT issuer discovery and JWKS validation.

mod claims;
mod config;
mod error;
mod utils;
mod validator;

pub use claims::{JwtClaims, TokenType, DEFAULT_JWT_EXPIRY_HOURS};
pub(crate) use config::OidcConfig;
pub(crate) use error::OidcError;
pub(crate) use utils::{extract_algorithm_unverified, extract_issuer_unverified};
pub(crate) use validator::OidcValidator;
