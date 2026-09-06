//! Golden tests for local contract generation (F4).

use std::fs;

use kalamdb_sql::{canonical_contract_hash, compile_contract_sql};
use tempfile::TempDir;

use crate::workflow::{
    project::config::{KalamProjectConfig, SchemaMode, SchemaSection, SchemaTarget},
    schema::{
        dart::generate_dart_source,
        gen::generate_languages,
        naming::{assign_names, NamingOptions},
        rust::generate_rust_source,
        typescript::{generate_client_source, generate_contracts_source},
        LanguageTarget,
    },
    test_support::minimal_sql_project_config,
};

const GOLDEN_SQL: &str = r#"
CREATE SCHEMA chat;
CREATE TYPE chat.address AS (city TEXT, country TEXT);
CREATE TYPE chat.status AS ENUM ('active', 'blocked');
CREATE TABLE chat.users (
  id BIGINT PRIMARY KEY,
  email TEXT NOT NULL,
  address chat.address,
  nickname TEXT,
  status chat.status NOT NULL
) ROW TYPE chat.user;
CREATE PROCEDURE chat.create_message(user_id TEXT, body TEXT NOT NULL)
RETURNS chat.user
LANGUAGE TS;
"#;

fn golden_snapshot() -> kalamdb_sql::contracts::ContractSnapshot {
    compile_contract_sql(GOLDEN_SQL, "public").expect("compile golden sql")
}

#[test]
fn all_targets_embed_the_same_contract_hash() {
    let snapshot = golden_snapshot();
    let hash = canonical_contract_hash(&snapshot);
    let names = assign_names(
        &snapshot,
        NamingOptions {
            unqualified_names: false,
        },
    )
    .unwrap();
    let ts = generate_client_source(&snapshot, &hash, &names);
    let contracts = generate_contracts_source(&snapshot, &hash, &names);
    let dart = generate_dart_source(&snapshot, &hash, &names);
    let rust = generate_rust_source(&snapshot, &hash, &names);
    let marker = format!("contract_hash: {hash}");
    assert!(ts.contains(&marker));
    assert!(contracts.contains(&marker));
    assert!(dart.contains(&marker));
    assert!(rust.contains(&marker));
    assert_eq!(hash.len(), 64);
}

#[test]
fn golden_nullability_nested_struct_alias_and_codecs() {
    let snapshot = golden_snapshot();
    let hash = canonical_contract_hash(&snapshot);
    let names = assign_names(
        &snapshot,
        NamingOptions {
            unqualified_names: false,
        },
    )
    .unwrap();
    let ts = generate_client_source(&snapshot, &hash, &names);
    let dart = generate_dart_source(&snapshot, &hash, &names);
    let rust = generate_rust_source(&snapshot, &hash, &names);

    assert!(ts.contains("export type ChatAddress"));
    assert!(ts.contains("address: ChatAddress | null"));
    assert!(ts.contains("nickname: string | null"));
    assert!(ts.contains("email: string;"));
    assert!(ts.contains("export type ChatUser"));
    assert!(ts.contains("export type ChatUsers = ChatUser"));
    assert!(ts.contains("createMessage:"));
    assert!(ts.contains("chat: {"));

    assert!(dart.contains("final class ChatAddress {"));
    assert!(dart.contains("ChatAddress? address"));
    assert!(dart.contains("String? nickname"));
    assert!(dart.contains("typedef ChatUsers = ChatUser;"));
    assert!(dart.contains("ChatAddress.fromJson"));

    assert!(rust.contains("pub struct ChatAddress"));
    assert!(rust.contains("pub address: Option<ChatAddress>"));
    assert!(rust.contains("pub nickname: Option<String>"));
    assert!(rust.contains("pub type ChatUsers = ChatUser;"));
}

#[test]
fn unqualified_names_emit_short_idents() {
    let snapshot = golden_snapshot();
    let hash = canonical_contract_hash(&snapshot);
    let names = assign_names(
        &snapshot,
        NamingOptions {
            unqualified_names: true,
        },
    )
    .unwrap();
    let ts = generate_client_source(&snapshot, &hash, &names);
    assert!(ts.contains("export type Address"));
    assert!(ts.contains("export type User"));
    assert!(ts.contains("createMessage:"));
    assert!(ts.contains("chat: {"));
    assert!(!ts.contains("export type ChatUser"));
}

#[test]
fn scaffold_writes_once_and_refuses_missing_export() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    fs::write(root.join("schema.sql"), GOLDEN_SQL).unwrap();
    let mut config = minimal_sql_project_config();
    config.schema = SchemaSection {
        mode:      SchemaMode::Sql,
        path:      Some("schema.sql".into()),
        watch:     false,
        languages: vec!["typescript".into()],
        targets:   [(
            "typescript".into(),
            SchemaTarget {
                output:            "src/generated/kalam.ts".into(),
                unqualified_names: false,
            },
        )]
        .into(),
    };

    generate_languages(root, &config, &[LanguageTarget::TypeScript], None).unwrap();

    let impl_path = root.join("functions/src/chat/create_message.ts");
    let original = fs::read_to_string(&impl_path).unwrap();
    assert!(original.contains("export default defineProcedure<ChatCreateMessage>"));
    assert!(original.contains("../../.kalam/generated/contracts"));
    fs::write(
        &impl_path,
        original.replace("throw new Error(\"not implemented\")", "return input as never"),
    )
    .unwrap();

    generate_languages(root, &config, &[LanguageTarget::TypeScript], None).unwrap();
    let after = fs::read_to_string(&impl_path).unwrap();
    assert!(after.contains("return input as never"));
    assert!(!after.contains("not implemented"));

    let registry = fs::read_to_string(root.join("functions/.kalam/generated/registry.ts")).unwrap();
    assert!(registry.contains("from \"../../src/chat/create_message\""));
    assert!(registry.contains("\"chat.create_message\""));

    fs::write(&impl_path, "export const broken = 1;\n").unwrap();
    let err = generate_languages(root, &config, &[LanguageTarget::TypeScript], None).unwrap_err();
    assert!(err.to_string().contains("export default"), "{err}");
}

#[test]
fn generate_does_not_require_a_server_url() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    fs::write(
        root.join("schema.sql"),
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);",
    )
    .unwrap();
    let config = KalamProjectConfig {
        schema: SchemaSection {
            mode:      SchemaMode::Sql,
            path:      Some("schema.sql".into()),
            watch:     false,
            languages: vec!["typescript".into(), "dart".into(), "rust".into()],
            targets:   [
                (
                    "typescript".into(),
                    SchemaTarget {
                        output:            "src/generated/kalam.ts".into(),
                        unqualified_names: false,
                    },
                ),
                (
                    "dart".into(),
                    SchemaTarget {
                        output:            "lib/generated/kalam.dart".into(),
                        unqualified_names: false,
                    },
                ),
                (
                    "rust".into(),
                    SchemaTarget {
                        output:            "src/generated/kalam.rs".into(),
                        unqualified_names: false,
                    },
                ),
            ]
            .into(),
        },
        ..minimal_sql_project_config()
    };
    generate_languages(
        root,
        &config,
        &[
            LanguageTarget::TypeScript,
            LanguageTarget::Dart,
            LanguageTarget::Rust,
        ],
        None,
    )
    .unwrap();
    let ts = fs::read_to_string(root.join("src/generated/kalam.ts")).unwrap();
    let dart = fs::read_to_string(root.join("lib/generated/kalam.dart")).unwrap();
    let rust = fs::read_to_string(root.join("src/generated/kalam.rs")).unwrap();
    let hash_line = ts
        .lines()
        .find(|line| line.contains("contract_hash:"))
        .expect("ts hash")
        .to_string();
    assert!(dart.contains(&hash_line[3..]) || dart.contains(hash_line.trim_start_matches("// ")));
    assert!(rust.contains(hash_line.trim_start_matches("// ")));
    assert!(ts.contains("export const users"));
    assert!(dart.contains("KalamTableSpec<Users>"));
}
