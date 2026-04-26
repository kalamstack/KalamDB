// JWT configuration cache, trusted issuer parsing, and OIDC validator registry.

use std::collections::HashMap;

use once_cell::sync::OnceCell;
use tokio::sync::RwLock;

use crate::{
    errors::error::{AuthError, AuthResult},
    oidc::{OidcConfig, OidcValidator},
    providers::jwt_auth,
};

/// Cached JWT configuration for performance.
///
/// Holds the HS256 shared secret for internal tokens, the list of trusted
/// issuers, and a registry of per-issuer `OidcValidator` instances for
/// external (RS256/ES256) token verification.
pub struct JwtConfig {
    pub secret: String,
    pub trusted_issuers: Vec<String>,
    pub issuer_audiences: HashMap<String, String>,

    /// Per-issuer OIDC validators (lazily populated on first request).
    /// Key = issuer URL, Value = OidcValidator (owns its own JWKS cache).
    oidc_validators: RwLock<HashMap<String, OidcValidator>>,
}

static JWT_CONFIG: OnceCell<JwtConfig> = OnceCell::new();

/// Initialize JWT configuration from server settings.
///
/// This should be called once at startup after loading server.toml and applying
/// environment overrides. If not called, defaults are used.
pub fn init_jwt_config(
    secret: &str,
    trusted_issuers: &str,
    issuer_audiences: HashMap<String, String>,
) {
    let config = JwtConfig {
        secret: secret.to_string(),
        trusted_issuers: parse_trusted_issuers(trusted_issuers),
        issuer_audiences,
        oidc_validators: RwLock::new(HashMap::new()),
    };
    let _ = JWT_CONFIG.set(config);
}

pub fn get_jwt_config() -> &'static JwtConfig {
    JWT_CONFIG.get_or_init(|| JwtConfig {
        secret: kalamdb_configs::defaults::default_auth_jwt_secret(),
        trusted_issuers: parse_trusted_issuers(
            &kalamdb_configs::defaults::default_auth_jwt_trusted_issuers(),
        ),
        issuer_audiences: HashMap::new(),
        oidc_validators: RwLock::new(HashMap::new()),
    })
}

impl JwtConfig {
    /// Get or create an `OidcValidator` for the given issuer.
    ///
    /// On first call for an issuer, performs OIDC Discovery to resolve the
    /// `jwks_uri`, then creates and caches the validator. Subsequent calls
    /// return the existing validator which maintains its own JWKS key cache.
    pub(crate) async fn get_oidc_validator(&self, issuer: &str) -> AuthResult<OidcValidator> {
        // Fast path: read lock
        {
            let validators = self.oidc_validators.read().await;
            if let Some(validator) = validators.get(issuer) {
                return Ok(validator.clone());
            }
        }

        // Slow path: OIDC discovery + write lock
        let client_id = self.issuer_audiences.get(issuer).cloned();
        let config = OidcConfig::discover(issuer.to_string(), client_id).await.map_err(|e| {
            AuthError::MalformedAuthorization(format!(
                "OIDC discovery failed for issuer '{}': {}",
                issuer, e
            ))
        })?;

        let validator = OidcValidator::new(config);

        let mut validators = self.oidc_validators.write().await;
        validators.entry(issuer.to_string()).or_insert_with(|| validator.clone());

        Ok(validator)
    }
}

fn parse_trusted_issuers(input: &str) -> Vec<String> {
    let mut issuers: Vec<String> = input
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Always include the internal issuer so local accounts (admin etc.) keep working
    if !issuers.contains(&jwt_auth::KALAMDB_ISSUER.to_string()) {
        issuers.insert(0, jwt_auth::KALAMDB_ISSUER.to_string());
    }
    issuers
}
