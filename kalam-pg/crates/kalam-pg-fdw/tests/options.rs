use kalam_pg_fdw::{ServerOptions, TableOptions};
use kalamdb_commons::models::{NamespaceId, TableName};
use kalamdb_commons::{TableId, TableType};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[test]
fn parses_embedded_server_options() {
    let options = BTreeMap::from([
        ("storage_base_path".to_string(), "/tmp/kalam-pg".to_string()),
        ("node_id".to_string(), "pg-node-1".to_string()),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse server options");

    assert_eq!(
        parsed
            .embedded_runtime
            .as_ref()
            .expect("embedded runtime config")
            .storage_base_path,
        PathBuf::from("/tmp/kalam-pg")
    );
    assert_eq!(
        parsed.embedded_runtime.as_ref().expect("embedded runtime config").node_id,
        "pg-node-1"
    );
    assert_eq!(parsed.remote, None);
}

#[test]
fn parses_remote_server_options() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
    ]);

    let parsed = ServerOptions::parse(&options).expect("parse remote server options");

    assert_eq!(parsed.remote.as_ref().expect("remote config").host, "127.0.0.1");
    assert_eq!(parsed.remote.as_ref().expect("remote config").port, 50051);
    assert_eq!(parsed.embedded_runtime, None);
}

#[test]
fn rejects_mixed_remote_and_embedded_server_options() {
    let options = BTreeMap::from([
        ("host".to_string(), "127.0.0.1".to_string()),
        ("port".to_string(), "50051".to_string()),
        ("storage_base_path".to_string(), "/tmp/kalam-pg".to_string()),
    ]);

    let err = ServerOptions::parse(&options).expect_err("mixed server options should fail");
    assert!(err.to_string().contains("remote") && err.to_string().contains("embedded"));
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
