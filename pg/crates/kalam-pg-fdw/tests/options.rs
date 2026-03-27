use kalam_pg_fdw::{ServerOptions, TableOptions};
use kalamdb_commons::models::{NamespaceId, TableName};
use kalamdb_commons::{TableId, TableType};
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
}

#[test]
fn rejects_missing_host() {
    let options = BTreeMap::from([
        ("port".to_string(), "50051".to_string()),
    ]);

    let err = ServerOptions::parse(&options).expect_err("missing host should fail");
    assert!(err.to_string().contains("host"));
}

#[test]
fn rejects_missing_port() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
    ]);

    let err = ServerOptions::parse(&options).expect_err("missing port should fail");
    assert!(err.to_string().contains("port"));
}

#[test]
fn parses_table_options() {
    let options = BTreeMap::from([
        ("namespace".to_string(), "app".to_string()),
        ("table".to_string(), "messages".to_string()),
        ("table_type".to_string(), "user".to_string()),
    ]);

    let parsed = TableOptions::parse(&options).expect("parse table options");

    assert_eq!(
        parsed.table_id,
        TableId::new(NamespaceId::new("app"), TableName::new("messages"))
    );
    assert_eq!(parsed.table_type, TableType::User);
}

#[test]
fn table_options_require_namespace() {
    let options = BTreeMap::from([
        ("table".to_string(), "messages".to_string()),
        ("table_type".to_string(), "user".to_string()),
    ]);

    let err = TableOptions::parse(&options).expect_err("missing namespace should fail");
    assert!(err.to_string().contains("namespace"));
}

#[test]
fn parses_tls_server_options() {
    let options = BTreeMap::from([
        ("host".to_string(), "kalam.example.com".to_string()),
        ("port".to_string(), "9188".to_string()),
        ("ca_cert".to_string(), "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----".to_string()),
        ("client_cert".to_string(), "-----BEGIN CERTIFICATE-----\nclient\n-----END CERTIFICATE-----".to_string()),
        ("client_key".to_string(), "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----".to_string()),
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
        ("client_cert".to_string(), "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----".to_string()),
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
        ("client_key".to_string(), "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----".to_string()),
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
