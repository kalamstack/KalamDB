use crate::oidc::OidcError;

#[derive(Debug, serde::Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
}

/// Configuration for a single OIDC issuer.
#[derive(Debug, Clone)]
pub(crate) struct OidcConfig {
    pub issuer_url: String,
    pub client_id: Option<String>,
    pub jwks_uri: String,
}

impl OidcConfig {
    pub fn new(issuer_url: String, client_id: Option<String>, jwks_uri: String) -> Self {
        Self {
            issuer_url,
            client_id,
            jwks_uri,
        }
    }

    pub async fn discover(
        issuer_url: String,
        client_id: Option<String>,
    ) -> Result<Self, OidcError> {
        let jwks_uri = Self::discover_jwks_uri(&issuer_url).await?;
        Ok(Self::new(issuer_url, client_id, jwks_uri))
    }

    async fn discover_jwks_uri(issuer_url: &str) -> Result<String, OidcError> {
        let base = issuer_url.trim_end_matches('/');
        let discovery_url = format!("{}/.well-known/openid-configuration", base);

        log::debug!("OIDC discovery: fetching {}", discovery_url);

        let response = reqwest::get(&discovery_url).await.map_err(|error| {
            OidcError::DiscoveryFailed(format!(
                "Failed to fetch OIDC discovery from '{}': {}",
                discovery_url, error
            ))
        })?;

        if !response.status().is_success() {
            return Err(OidcError::DiscoveryFailed(format!(
                "OIDC discovery request to '{}' returned status {}",
                discovery_url,
                response.status()
            )));
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();

        if !content_type.starts_with("application/json") {
            return Err(OidcError::DiscoveryFailed(format!(
                "Unexpected Content-Type from '{}': '{}', expected 'application/json'",
                discovery_url, content_type
            )));
        }

        let discovery: OidcDiscovery = response.json().await.map_err(|error| {
            OidcError::DiscoveryFailed(format!(
                "Failed to parse OIDC discovery JSON from '{}': {}",
                discovery_url, error
            ))
        })?;

        log::debug!("OIDC discovery: jwks_uri = {}", discovery.jwks_uri);
        Ok(discovery.jwks_uri)
    }
}
