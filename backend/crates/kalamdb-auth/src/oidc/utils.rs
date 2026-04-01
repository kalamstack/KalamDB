use crate::oidc::OidcError;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::{decode_header, Algorithm};

/// Extract the `alg` field from the JWT header without verifying the signature.
pub(crate) fn extract_algorithm_unverified(token: &str) -> Result<Algorithm, OidcError> {
    let header = decode_header(token).map_err(|error| {
        OidcError::JwtValidationFailed(format!("Invalid JWT header: {}", error))
    })?;

    Ok(header.alg)
}

/// Extract the `iss` claim from the JWT payload without verifying the signature.
pub(crate) fn extract_issuer_unverified(token: &str) -> Result<String, OidcError> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() < 2 {
        return Err(OidcError::JwtValidationFailed(
            "Invalid JWT format: less than 2 segments".into(),
        ));
    }

    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|error| {
        OidcError::JwtValidationFailed(format!("Invalid JWT payload base64: {}", error))
    })?;

    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).map_err(|error| {
        OidcError::JwtValidationFailed(format!("Invalid JWT payload JSON: {}", error))
    })?;

    payload
        .get("iss")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
        .ok_or_else(|| OidcError::JwtValidationFailed("Missing 'iss' claim".into()))
}
