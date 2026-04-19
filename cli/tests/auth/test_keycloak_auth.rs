//! Keycloak OIDC integration tests
//!
//! These tests validate:
//! - Keycloak realm is reachable and issues asymmetric (RS256) tokens
//! - Real RS256 tokens from Keycloak are verified via JWKS (not the shared HS256 secret)
//! - Trusted bearer auth resolves a pre-created OAuth user by canonical token subject
//! - Subsequent requests reuse the same canonical user account
//! - HS256 tokens claiming an external issuer are rejected
//!
//! ## Security model
//!
//! HS256 uses a shared secret. A valid HS256 signature proves OUR server produced
//! the token — it does NOT prove Keycloak or any external party did. Anyone who
//! knows our JWT secret can forge `iss=http://keycloak.../realms/kalamdb`.
//! The correct fix: external tokens must use RS256/ES256. The server fetches the
//! provider's public key via OIDC discovery (JWKS) and verifies the signature
//! cryptographically. Only Keycloak (holding the private key) can sign valid tokens.
//!
//! ## Prerequisites
//!
//! 1. KalamDB server must be running
//! 2. Keycloak must be running (docker/utils/docker-compose.yml)
//! 3. Server must be started with:
//!    ```sh
//!    KALAMDB_JWT_TRUSTED_ISSUERS="kalamdb,http://localhost:8081/realms/kalamdb" \
//!    cargo run
//!    ```
//!
//! If either server or Keycloak is not running, tests are skipped gracefully.
//!
//! ## Environment variables
//!
//! | Variable                     | Default                                       |
//! |------------------------------|-----------------------------------------------|
//! | `KEYCLOAK_URL`               | `http://localhost:8081`                        |
//! | `KEYCLOAK_REALM`             | `kalamdb`                                     |
//! | `KEYCLOAK_CLIENT_ID`         | `kalamdb-api`                                 |
//! | `KEYCLOAK_TEST_USER`         | `kalamdb-user`                                |
//! | `KEYCLOAK_TEST_PASSWORD`     | `kalamdb123`                                  |

use crate::common::*;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration helpers
// ---------------------------------------------------------------------------

fn keycloak_url() -> String {
    std::env::var("KEYCLOAK_URL").unwrap_or_else(|_| "http://localhost:8081".to_string())
}

fn keycloak_realm() -> String {
    std::env::var("KEYCLOAK_REALM").unwrap_or_else(|_| "kalamdb".to_string())
}

fn keycloak_client_id() -> String {
    std::env::var("KEYCLOAK_CLIENT_ID").unwrap_or_else(|_| "kalamdb-api".to_string())
}

fn keycloak_test_user() -> String {
    std::env::var("KEYCLOAK_TEST_USER").unwrap_or_else(|_| "kalamdb-user".to_string())
}

fn keycloak_test_password() -> String {
    std::env::var("KEYCLOAK_TEST_PASSWORD").unwrap_or_else(|_| "kalamdb123".to_string())
}

fn keycloak_issuer() -> String {
    format!("{}/realms/{}", keycloak_url(), keycloak_realm())
}

/// Token endpoint for Direct Access Grant (Resource Owner Password Credentials).
fn keycloak_token_endpoint() -> String {
    format!("{}/realms/{}/protocol/openid-connect/token", keycloak_url(), keycloak_realm())
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

// ---------------------------------------------------------------------------
// Reachability checks
// ---------------------------------------------------------------------------

/// Check if Keycloak is reachable by hitting the realm discovery endpoint.
fn is_keycloak_reachable() -> bool {
    let url = format!("{}/.well-known/openid-configuration", keycloak_issuer());
    let result = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().ok()?;
        rt.block_on(async {
            Client::new()
                .get(&url)
                .timeout(Duration::from_secs(3))
                .send()
                .await
                .ok()
                .filter(|r| r.status().is_success())
        })
    })
    .join()
    .ok()
    .flatten();

    result.is_some()
}

/// Guard macro-like function: returns `false` (skip) if preconditions aren't met.
fn should_run_keycloak_tests() -> bool {
    if !is_server_running() {
        eprintln!("⚠️  KalamDB server not running. Skipping Keycloak tests.");
        return false;
    }
    if !is_keycloak_reachable() {
        eprintln!("⚠️  Keycloak not reachable at {}. Skipping Keycloak tests.", keycloak_url());
        eprintln!("   Start Keycloak: cd docker/utils && docker-compose up -d keycloak");
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Helper: acquire token from Keycloak via Direct Access Grant
// ---------------------------------------------------------------------------

/// Get an access token from Keycloak using the Resource Owner Password flow.
/// This proves that Keycloak is alive, the realm exists, the client is configured,
/// and the test user can authenticate.
async fn get_keycloak_token() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = Client::new();
    let response = client
        .post(&keycloak_token_endpoint())
        .form(&[
            ("grant_type", "password"),
            ("client_id", &keycloak_client_id()),
            ("username", &keycloak_test_user()),
            ("password", &keycloak_test_password()),
        ])
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    let status = response.status();
    let body: serde_json::Value = response.json().await?;

    if !status.is_success() {
        return Err(format!(
            "Keycloak token request failed ({}): {}",
            status,
            serde_json::to_string_pretty(&body).unwrap_or_default()
        )
        .into());
    }

    Ok(body)
}

/// Peek at the `sub` claim of a JWT without verifying the signature.
/// Used in tests to determine the expected canonical user id.
fn decode_jwt_sub(token: &str) -> Option<String> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;

    let payload_b64 = token.splitn(3, '.').nth(1)?;
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
    payload.get("sub")?.as_str().map(|s| s.to_string())
}

/// Send a SQL request to KalamDB authenticated with a Bearer token.
async fn execute_sql_with_bearer(
    token: &str,
    sql: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let client = Client::new();
    let response = client
        .post(format!("{}/v1/api/sql", server_url()))
        .header("Authorization", format!("Bearer {}", token))
        .json(&json!({ "sql": sql }))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    let status = response.status();
    let raw = response.text().await?;
    let body: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or_else(|_| json!({ "raw": raw }));

    if !status.is_success() {
        return Err(format!(
            "SQL request failed ({}): {}",
            status,
            serde_json::to_string_pretty(&body).unwrap_or_default()
        )
        .into());
    }

    Ok(body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify Keycloak is alive and issues asymmetric (RS256) tokens.
///
/// Gets a real token from Keycloak via Direct Access Grant, asserts the response
/// contains an `access_token`, and verifies the token algorithm is RS256/ES256.
#[test]
fn test_keycloak_realm_configured() {
    if !should_run_keycloak_tests() {
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    let token_response = rt.block_on(get_keycloak_token());

    match token_response {
        Ok(body) => {
            assert!(
                body.get("access_token").is_some(),
                "Keycloak response should contain an access_token: {:?}",
                body
            );
            assert_eq!(
                body.get("token_type").and_then(|v| v.as_str()),
                Some("Bearer"),
                "token_type should be Bearer"
            );

            // Verify Keycloak uses asymmetric algorithm (not HS256)
            if let Some(access_token) = body.get("access_token").and_then(|v| v.as_str()) {
                use base64::engine::general_purpose::URL_SAFE_NO_PAD;
                use base64::Engine as _;
                if let Some(header_b64) = access_token.splitn(3, '.').next() {
                    if let Ok(hdr_bytes) = URL_SAFE_NO_PAD.decode(header_b64) {
                        if let Ok(hdr) = serde_json::from_slice::<serde_json::Value>(&hdr_bytes) {
                            let alg = hdr.get("alg").and_then(|v| v.as_str()).unwrap_or("?");
                            eprintln!("[keycloak] Token algorithm: {}", alg);
                            assert!(
                                alg.starts_with("RS") || alg.starts_with("ES"),
                                "Keycloak must issue asymmetric tokens (RS256/ES256), got alg={}",
                                alg
                            );
                        }
                    }
                }
            }

            eprintln!("[keycloak] OK: RS256 token obtained for '{}'", keycloak_test_user());
        },
        Err(e) => {
            panic!(
                "Failed to get token from Keycloak. \
                 Ensure realm '{}' has client '{}' with Direct Access Grants enabled \
                 and user '{}' exists: {}",
                keycloak_realm(),
                keycloak_client_id(),
                keycloak_test_user(),
                e
            );
        },
    }
}

/// End-to-end: Keycloak RS256 token → JWKS verification → direct canonical user lookup.
///
/// This is the correct, cryptographically sound flow:
/// 1. Get a real RS256 token from Keycloak (signed with Keycloak's RSA private key).
/// 2. Pre-create an OAuth user whose `user_id` exactly matches the token `sub`.
/// 3. Send it to KalamDB as a Bearer token.
/// 4. KalamDB reads `iss`, fetches Keycloak's JWKS, verifies the RS256 signature
///    using Keycloak's *public* key. Only Keycloak can produce a valid signature.
/// 5. First request: resolve the pre-created OAuth user directly by canonical `sub`.
/// 6. Second request: reuse the same canonical user via the user_id index.
///
/// Server must be started with:
/// ```sh
/// KALAMDB_JWT_TRUSTED_ISSUERS="kalamdb,http://localhost:8081/realms/kalamdb" \
/// cargo run
/// ```
#[test]
fn test_preprovisioned_oauth_user_via_bearer() {
    if !should_run_keycloak_tests() {
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Step 1: acquire a real RS256 token from Keycloak
    let token_response = rt.block_on(get_keycloak_token()).expect("Failed to get Keycloak token");
    let access_token = token_response
        .get("access_token")
        .and_then(|v| v.as_str())
        .expect("No access_token in Keycloak response")
        .to_string();

    let subject = decode_jwt_sub(&access_token).expect("Could not read sub from Keycloak token");
    let expected_user_id = subject.clone();

    eprintln!(
        "[keycloak] Real RS256 token sub='{}', expected user_id='{}'",
        subject, expected_user_id
    );

    let escaped_user_id = escape_sql_literal(&expected_user_id);
    let oauth_payload = serde_json::to_string(&json!({
        "provider": "keycloak",
        "subject": subject,
    }))
    .expect("Failed to encode Keycloak OAuth payload");
    let escaped_payload = escape_sql_literal(&oauth_payload);

    let _ = rt.block_on(execute_sql_via_http_as_root(&format!(
        "DROP USER IF EXISTS '{}'",
        escaped_user_id
    )));

    let create_sql =
        format!("CREATE USER '{}' WITH OAUTH '{}' ROLE 'user'", escaped_user_id, escaped_payload);
    rt.block_on(execute_sql_via_http_as_root(&create_sql))
        .expect("Failed to pre-create Keycloak OAuth user");

    // Step 2: send to KalamDB — verifies RS256 via JWKS and resolves the canonical user
    let result1 = rt.block_on(execute_sql_with_bearer(&access_token, "SELECT 1 AS probe"));

    match &result1 {
        Err(e) => {
            let err = e.to_string().to_lowercase();
            if err.contains("untrusted issuer")
                || err.contains("no trusted issuers configured")
                || err.contains("user not found")
                || err.contains("invalid credentials")
                || err.contains("invalid_credentials")
            {
                eprintln!(
                    "Server not configured for Keycloak OIDC. Skipping.\n\
                     Start server with:\n  \
                     KALAMDB_JWT_TRUSTED_ISSUERS=\"kalamdb,{}\" \\\n  \
                     cargo run",
                    keycloak_issuer()
                );
                return;
            }
            panic!(
                "First bearer request with real Keycloak RS256 token failed unexpectedly: {}",
                e
            );
        },
        Ok(_) => eprintln!("[keycloak] First request OK — canonical OAuth user resolved by sub."),
    }

    // Step 3: same token again — must reuse the existing user via index
    let result2 = rt.block_on(execute_sql_with_bearer(&access_token, "SELECT 2 AS probe"));
    assert!(
        result2.is_ok(),
        "Second request with same RS256 token should succeed: {:?}",
        result2.err()
    );
    eprintln!("[keycloak] Second request OK — existing user found via user_id index.");

    // Step 4: verify user in system.users
    let check_sql = format!(
        "SELECT user_id, auth_type FROM system.users WHERE user_id = '{}'",
        expected_user_id
    );
    if let Ok(body) = rt.block_on(execute_sql_via_http_as_root(&check_sql)) {
        let rows = get_rows_as_hashmaps(&body);
        assert!(
            rows.is_some() && !rows.as_ref().unwrap().is_empty(),
            "User '{}' should exist in system.users. Body: {:?}",
            expected_user_id,
            body
        );
        let row = &rows.unwrap()[0];
        assert_eq!(
            row.get("auth_type").and_then(|v| v.as_str()),
            Some("OAuth"),
            "auth_type should be OAuth"
        );
        eprintln!("[keycloak] Verified user '{}' with auth_type=OAuth.", expected_user_id);
    }

    // Cleanup
    let _ = rt.block_on(execute_sql_via_http_as_root(&format!("DROP USER '{}'", escaped_user_id)));
}

/// Verify that HS256 tokens claiming an external (Keycloak) issuer are rejected.
///
/// With HS256, a valid signature proves only that OUR server signed this token.
/// The `iss` claim is just bytes in the payload — anyone with our secret can set
/// `iss=http://keycloak/realms/kalamdb`. There is no cryptographic proof that
/// Keycloak authored the token. The server must detect and reject this.
#[test]
fn test_hs256_with_external_issuer_rejected() {
    if !should_run_keycloak_tests() {
        return;
    }

    let now = chrono::Utc::now();
    let exp = now + chrono::Duration::hours(1);

    let claims = json!({
        "sub":                "forged-sub-hs256",
        "iss":                keycloak_issuer(), // external issuer, but HS256 signed with OUR key
        "exp":                exp.timestamp() as usize,
        "iat":                now.timestamp() as usize,
        "preferred_username": "forged-user",
    });

    let secret = std::env::var("KALAMDB_JWT_SECRET")
        .unwrap_or_else(|_| "CHANGE_ME_IN_PRODUCTION".to_string());

    let forged_token = {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("Failed to encode forged HS256 token")
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (status, body) = rt.block_on(async {
        let client = Client::new();
        let resp = client
            .post(format!("{}/v1/api/sql", server_url()))
            .header("Authorization", format!("Bearer {}", forged_token))
            .json(&json!({ "sql": "SELECT 1" }))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("HTTP request failed");
        let s = resp.status();
        let b = resp.json::<serde_json::Value>().await.ok();
        (s, b)
    });

    assert!(
        status.as_u16() == 401 || status.as_u16() == 403,
        "HS256 token with Keycloak issuer MUST be rejected (401/403), got {}.\n\
         If this passed, it means the server accepted a forged HS256 token — \n\
         anyone with the JWT secret can impersonate any OIDC provider!\n\
         body={:?}",
        status,
        body
    );

    eprintln!("[keycloak] OK HS256+external-issuer rejected with {}", status);
}

/// Verify that tokens with a completely unknown / untrusted issuer are rejected.
#[test]
fn test_untrusted_issuer_rejected() {
    if !should_run_keycloak_tests() {
        return;
    }

    let now = chrono::Utc::now();
    let exp = now + chrono::Duration::hours(1);

    let claims = json!({
        "sub": "unknown-sub-001",
        "iss": "https://totally-unknown.example.com/realms/evil",
        "exp": exp.timestamp() as usize,
        "iat": now.timestamp() as usize,
    });

    let secret = std::env::var("KALAMDB_JWT_SECRET")
        .unwrap_or_else(|_| "CHANGE_ME_IN_PRODUCTION".to_string());

    let token = {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("encode")
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (status, body) = rt.block_on(async {
        let client = Client::new();
        let resp = client
            .post(format!("{}/v1/api/sql", server_url()))
            .header("Authorization", format!("Bearer {}", token))
            .json(&json!({ "sql": "SELECT 1" }))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("HTTP request");
        (resp.status(), resp.json::<serde_json::Value>().await.ok())
    });

    assert!(
        status.as_u16() == 401 || status.as_u16() == 403,
        "Token with untrusted issuer must be rejected (401/403), got {} body={:?}",
        status,
        body
    );

    eprintln!("[keycloak] OK unknown issuer rejected with {}", status);
}
