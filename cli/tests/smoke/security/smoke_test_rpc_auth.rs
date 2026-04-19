//! Smoke tests – RPC / HTTP endpoint authentication
//!
//! Verifies that every protected HTTP endpoint rejects requests that carry no
//! credentials, wrong credentials, or forged tokens with the correct HTTP 401
//! status.
//!
//! These are end-to-end tests that require a running KalamDB server.
//! They exercise the real auth middleware, not stubs.
//!
//! Run with:
//!   cargo nextest run --test smoke smoke_security_rpc_auth

use crate::common::*;
use base64::{engine::general_purpose, Engine as _};
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build reqwest client")
}

fn sql_url() -> String {
    format!("{}/v1/api/sql", server_url())
}

fn me_url() -> String {
    format!("{}/v1/api/auth/me", server_url())
}

fn refresh_url() -> String {
    format!("{}/v1/api/auth/refresh", server_url())
}

fn health_url() -> String {
    format!("{}/health", server_url())
}

fn login_url() -> String {
    format!("{}/v1/api/auth/login", server_url())
}

/// Synchronously run a future on a fresh tokio runtime (smoke tests are sync).
fn block<F: std::future::Future<Output = T>, T>(fut: F) -> T {
    tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(fut)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// POST /v1/api/sql without any Authorization header must return 401.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_sql_no_auth_returns_401() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_sql_no_auth_returns_401: server not running");
        return;
    }

    let status = block(async {
        http_client()
            .post(sql_url())
            .json(&json!({ "sql": "SELECT 1" }))
            .send()
            .await
            .expect("Request failed")
            .status()
    });

    assert_eq!(
        status.as_u16(),
        401,
        "SQL endpoint without auth header must return 401, got {}",
        status
    );
}

/// POST /v1/api/sql with a syntactically invalid Bearer token must return 401.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_sql_invalid_bearer_returns_401() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_sql_invalid_bearer_returns_401: server not running");
        return;
    }

    let status = block(async {
        http_client()
            .post(sql_url())
            .header("Authorization", "Bearer not.a.valid.jwt")
            .json(&json!({ "sql": "SELECT 1" }))
            .send()
            .await
            .expect("Request failed")
            .status()
    });

    assert_eq!(
        status.as_u16(),
        401,
        "SQL endpoint with malformed JWT must return 401, got {}",
        status
    );
}

/// POST /v1/api/sql with a correctly-structured but forged JWT must return 401.
/// The forged token uses `alg: none` and a system role claim – a classic bypass.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_sql_forged_jwt_alg_none_returns_401() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_sql_forged_jwt_alg_none_returns_401: server not running");
        return;
    }

    // header={"alg":"none"} payload={"sub":"attacker","role":"system","exp":9999999999}
    let forged = "eyJhbGciOiJub25lIn0.\
        eyJzdWIiOiJhdHRhY2tlciIsInJvbGUiOiJzeXN0ZW0iLCJleHAiOjk5OTk5OTk5OTl9.";

    let status = block(async {
        http_client()
            .post(sql_url())
            .header("Authorization", format!("Bearer {}", forged))
            .json(&json!({ "sql": "SELECT * FROM system.users" }))
            .send()
            .await
            .expect("Request failed")
            .status()
    });

    assert_eq!(
        status.as_u16(),
        401,
        "Forged alg:none JWT must be rejected with 401, got {}",
        status
    );
}

/// POST /v1/api/sql using HTTP Basic auth must return 401.
/// The SQL endpoint accepts only Bearer tokens; Basic is valid only on the
/// login endpoint.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_sql_basic_auth_returns_401() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_sql_basic_auth_returns_401: server not running");
        return;
    }

    // base64("admin:kalamdb123") — valid credentials, wrong scheme for /sql
    let basic = "Basic YWRtaW46a2FsYW1kYjEyMw==";

    let status = block(async {
        http_client()
            .post(sql_url())
            .header("Authorization", basic)
            .json(&json!({ "sql": "SELECT 1" }))
            .send()
            .await
            .expect("Request failed")
            .status()
    });

    assert!(
        status.as_u16() == 401 || status.as_u16() == 403,
        "SQL endpoint with Basic auth must return 401/403, got {}",
        status
    );
}

/// Protected non-login auth endpoints must reject Basic auth.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_non_login_auth_endpoints_reject_basic_auth() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_rpc_non_login_auth_endpoints_reject_basic_auth: server not running"
        );
        return;
    }

    block(async {
        let auth_header = reqwest::header::HeaderValue::from_str(&format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{}:{}", admin_username(), admin_password()))
        ))
        .expect("valid basic auth header");

        let me_status = http_client()
            .get(me_url())
            .header(reqwest::header::AUTHORIZATION, auth_header.clone())
            .send()
            .await
            .expect("/auth/me request failed")
            .status();

        let refresh_status = http_client()
            .post(refresh_url())
            .header(reqwest::header::AUTHORIZATION, auth_header)
            .send()
            .await
            .expect("/auth/refresh request failed")
            .status();

        assert_eq!(
            me_status.as_u16(),
            401,
            "/auth/me with Basic auth must return 401, got {}",
            me_status
        );
        assert_eq!(
            refresh_status.as_u16(),
            401,
            "/auth/refresh with Basic auth must return 401, got {}",
            refresh_status
        );
    });
}

/// GET /v1/api/auth/me without credentials must return 401.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_me_no_auth_returns_401() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_me_no_auth_returns_401: server not running");
        return;
    }

    let status =
        block(async { http_client().get(me_url()).send().await.expect("Request failed").status() });

    assert_eq!(
        status.as_u16(),
        401,
        "/auth/me without credentials must return 401, got {}",
        status
    );
}

/// GET /health (public probe endpoint) must return 200 without any credentials.
/// If this returns 401 the load-balancer health checks would break.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_health_is_public() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_health_is_public: server not running");
        return;
    }

    let status = block(async {
        http_client().get(health_url()).send().await.expect("Request failed").status()
    });

    assert!(status.is_success(), "/health must be publicly accessible (2xx), got {}", status);
}

/// POST /v1/api/auth/login with wrong password must return 401.
/// The error message must NOT reveal whether the user exists (timing-safe,
/// generic message).
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_login_wrong_password_returns_401_generic_message() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_rpc_login_wrong_password_returns_401_generic_message: server not running"
        );
        return;
    }

    let (status, body) = block(async {
        let response = http_client()
            .post(login_url())
            .json(&json!({
                "user": "admin",
                "password": "this-is-definitely-wrong-password-xyz"
            }))
            .send()
            .await
            .expect("Request failed");
        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap_or_default();
        (status, body)
    });

    assert_eq!(
        status.as_u16(),
        401,
        "Login with wrong password must return 401, got {}",
        status
    );

    // The error message must not betray whether the user exists or not.
    let msg = body
        .get("message")
        .or_else(|| body.get("error").and_then(|e| e.get("message")))
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_lowercase();

    assert!(
        !msg.contains("not found") && !msg.contains("no user"),
        "Login error must not reveal whether the user exists. Got: {}",
        msg
    );
}

/// POST /v1/api/auth/login with a non-existent username must return 401 and the
/// same generic message as a wrong password — prevents user enumeration.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_login_nonexistent_user_matches_wrong_password_response() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_rpc_login_nonexistent_user_matches_wrong_password_response: server not running"
        );
        return;
    }

    let (status_real_user, msg_real_user) = block(async {
        let resp = http_client()
            .post(login_url())
            .json(&json!({ "user": "admin", "password": "wrong-pass-abc123" }))
            .send()
            .await
            .expect("Request failed");
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body
            .get("message")
            .or_else(|| body.get("error").and_then(|e| e.get("message")))
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_lowercase();
        (status, msg)
    });

    let (status_fake_user, msg_fake_user) = block(async {
        let resp = http_client()
            .post(login_url())
            .json(&json!({
                "user": "this_user_definitely_does_not_exist_xyz",
                "password": "wrong-pass-abc123"
            }))
            .send()
            .await
            .expect("Request failed");
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let msg = body
            .get("message")
            .or_else(|| body.get("error").and_then(|e| e.get("message")))
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_lowercase();
        (status, msg)
    });

    assert_eq!(status_real_user.as_u16(), 401, "Wrong password for real user must return 401");
    assert_eq!(
        status_fake_user.as_u16(),
        401,
        "Non-existent user must also return 401 (no user enumeration)"
    );

    // Both messages must be semantically equivalent (both indicate auth failure,
    // neither pinpoints whether the user exists).
    assert!(
        !msg_fake_user.contains("not found") && !msg_fake_user.contains("no user"),
        "Non-existent user error must not say 'not found'. Got: {}",
        msg_fake_user
    );
    assert_eq!(
        msg_real_user, msg_fake_user,
        "Error messages for wrong-password vs non-existent user must be identical \
        to prevent user enumeration. real_user='{}', fake_user='{}'",
        msg_real_user, msg_fake_user
    );
}

/// A `user`-role account attempting to read `system.users` via the SQL
/// endpoint must be rejected even with a valid Bearer token.
#[ntest::timeout(60000)]
#[test]
fn smoke_rpc_user_role_cannot_read_system_users() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_user_role_cannot_read_system_users: server not running");
        return;
    }

    let username = generate_unique_namespace("smoke_rpc_sec_user");
    let password = "Smoke_rpc_123!";

    // Create a regular user via root
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        username, password
    ))
    .expect("Failed to create test user");

    let result = execute_sql_via_client_as(&username, password, "SELECT * FROM system.users");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", username));

    assert!(
        result.is_err(),
        "Regular user must NOT be able to read system.users through the HTTP SQL API"
    );
    if let Err(e) = result {
        let msg = e.to_string().to_lowercase();
        assert!(
            msg.contains("unauthorized")
                || msg.contains("permission")
                || msg.contains("access denied")
                || msg.contains("privilege")
                || msg.contains("not allowed"),
            "Expected a permission error, got: {}",
            e
        );
    }
}

/// A `user`-role account attempting to escalate its own role to `system`
/// via the SQL endpoint must be rejected.
#[ntest::timeout(60000)]
#[test]
fn smoke_rpc_user_cannot_escalate_own_role() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_user_cannot_escalate_own_role: server not running");
        return;
    }

    let username = generate_unique_namespace("smoke_rpc_escalate");
    let password = "Smoke_rpc_456!";

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        username, password
    ))
    .expect("Failed to create test user");

    let escalate_result = execute_sql_via_client_as(
        &username,
        password,
        &format!("ALTER USER {} SET ROLE 'system'", username),
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", username));

    assert!(
        escalate_result.is_err(),
        "Regular user must NOT be able to escalate their own role"
    );
}

/// Sending a completely empty request body to the SQL endpoint must return
/// 400 (bad request), not 500 or a silent success.
#[ntest::timeout(30000)]
#[test]
fn smoke_rpc_sql_empty_body_returns_error() {
    if !is_server_running() {
        eprintln!("Skipping smoke_rpc_sql_empty_body_returns_error: server not running");
        return;
    }

    // First obtain a valid token so we bypass the auth gate and reach body parsing.
    let token = block(async {
        get_access_token(default_username(), default_password())
            .await
            .expect("Failed to obtain access token")
    });

    let status = block(async {
        http_client()
            .post(sql_url())
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .body("")  // empty body
            .send()
            .await
            .expect("Request failed")
            .status()
    });

    assert!(
        status.as_u16() >= 400 && status.as_u16() < 500,
        "Empty body with valid auth must return a 4xx error, got {}",
        status
    );
}
