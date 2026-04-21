use kalam_pg_common::RemoteAuthMode;
use kalam_pg_fdw::ServerOptions;
use std::collections::BTreeMap;

#[test]
fn parses_remote_server_options() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse remote server options");

    assert_eq!(parsed.remote.as_ref().expect("remote config").host, "127.0.0.1");
    assert_eq!(parsed.remote.as_ref().expect("remote config").port, 50051);
    assert_eq!(
        parsed.remote.as_ref().expect("remote config").auth_mode,
        RemoteAuthMode::None
    );
}

#[test]
fn rejects_missing_host() {
    let options = BTreeMap::from([("port".to_string(), "50051".to_string())]);

    let err = ServerOptions::parse(&options).expect_err("missing host should fail");
    assert!(err.to_string().contains("host"));
}

#[test]
fn rejects_missing_port() {
    let options = BTreeMap::from([("host".to_string(), "127.0.0.1".to_string())]);

    let err = ServerOptions::parse(&options).expect_err("missing port should fail");
    assert!(err.to_string().contains("port"));
}

#[test]
fn parses_tls_server_options() {
    let options = BTreeMap::from([
        ("host".to_string(), "kalam.example.com".to_string()),
        ("port".to_string(), "9188".to_string()),
        (
            "ca_cert".to_string(),
            "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_string(),
        ),
        (
            "client_cert".to_string(),
            "-----BEGIN CERTIFICATE-----\nclient\n-----END CERTIFICATE-----".to_string(),
        ),
        (
            "client_key".to_string(),
            "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----".to_string(),
        ),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse TLS server options");
    let remote = parsed.remote.as_ref().expect("remote config");
    assert_eq!(remote.host, "kalam.example.com");
    assert_eq!(remote.port, 9188);
    assert!(remote.tls_enabled());
    assert!(remote.ca_cert.is_some());
    assert!(remote.client_cert.is_some());
    assert!(remote.client_key.is_some());
    assert!(remote.endpoint_uri().starts_with("https://"));
}

#[test]
fn rejects_client_cert_without_key() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        (
            "client_cert".to_string(),
            "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----".to_string(),
        ),
    ]);

    let err = ServerOptions::parse(&options).expect_err("client_cert without key should fail");
    assert!(err.to_string().contains("client_cert"));
    assert!(err.to_string().contains("client_key"));
}

#[test]
fn rejects_client_key_without_cert() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        (
            "client_key".to_string(),
            "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----".to_string(),
        ),
    ]);

    let err = ServerOptions::parse(&options).expect_err("client_key without cert should fail");
    assert!(err.to_string().contains("client_cert"));
    assert!(err.to_string().contains("client_key"));
}

#[test]
fn non_tls_uses_http_scheme() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse non-TLS options");
    let remote = parsed.remote.as_ref().expect("remote config");
    assert!(!remote.tls_enabled());
    assert_eq!(remote.endpoint_uri(), "http://127.0.0.1:50051");
}

#[test]
fn legacy_auth_header_defaults_to_static_header_mode() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        ("auth_header".to_string(), "Bearer legacy-secret".to_string()),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse legacy auth_header options");
    let remote = parsed.remote.as_ref().expect("remote config");
    assert_eq!(remote.auth_mode, RemoteAuthMode::StaticHeader);
    assert_eq!(remote.auth_header.as_deref(), Some("Bearer legacy-secret"));
}

#[test]
fn parses_account_login_server_options() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        ("auth_mode".to_string(), "account_login".to_string()),
        ("login_user".to_string(), "pg_dba".to_string()),
        ("login_password".to_string(), "super-secret".to_string()),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse account_login options");
    let remote = parsed.remote.as_ref().expect("remote config");
    assert_eq!(remote.auth_mode, RemoteAuthMode::AccountLogin);
    assert_eq!(remote.login_user.as_deref(), Some("pg_dba"));
    assert_eq!(remote.login_password.as_deref(), Some("super-secret"));
}

#[test]
fn account_login_requires_login_user() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        ("auth_mode".to_string(), "account_login".to_string()),
        ("login_password".to_string(), "super-secret".to_string()),
    ]);

    let err = ServerOptions::parse(&options).expect_err("account_login without login_user should fail");
    assert!(err.to_string().contains("login_user"));
}

#[test]
fn static_header_rejects_account_login_fields() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        ("auth_mode".to_string(), "static_header".to_string()),
        ("auth_header".to_string(), "Bearer legacy-secret".to_string()),
        ("login_user".to_string(), "pg_dba".to_string()),
    ]);

    let err = ServerOptions::parse(&options).expect_err("static_header with login fields should fail");
    assert!(err.to_string().contains("account_login"));
}
