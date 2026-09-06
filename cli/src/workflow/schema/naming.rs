//! Shared generated identifiers for TypeScript, Dart, and Rust.

use std::collections::BTreeMap;

use kalamdb_sql::contracts::ContractSnapshot;

use crate::error::{CLIError, Result};

pub const DEFAULT_SCHEMA: &str = "public";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamingOptions {
    pub unqualified_names: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedNames {
    /// Qualified type id (`chat.user`) → generated type ident (`ChatUser` / `User`).
    pub types:    BTreeMap<String, String>,
    /// Qualified routine id → generated procedure contract ident.
    pub routines: BTreeMap<String, String>,
}

impl AssignedNames {
    pub fn type_ident(&self, type_id: &str) -> &str {
        self.types.get(type_id).map(String::as_str).unwrap_or("Unknown")
    }

    pub fn routine_ident(&self, routine_id: &str) -> &str {
        self.routines.get(routine_id).map(String::as_str).unwrap_or("Unknown")
    }
}

pub fn assign_names(snapshot: &ContractSnapshot, options: NamingOptions) -> Result<AssignedNames> {
    let mut claimed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut types = BTreeMap::new();
    for (id, ty) in &snapshot.types {
        let ident = generated_type_ident(&ty.schema, &ty.name, options.unqualified_names);
        claimed.entry(ident.clone()).or_default().push(id.clone());
        types.insert(id.clone(), ident);
    }
    let mut routines = BTreeMap::new();
    for (id, routine) in &snapshot.routines {
        let ident = generated_type_ident(&routine.schema, &routine.name, options.unqualified_names);
        claimed.entry(ident.clone()).or_default().push(format!("procedure {id}"));
        routines.insert(id.clone(), ident);
    }

    let collisions: Vec<String> = claimed
        .into_iter()
        .filter(|(_, owners)| owners.len() > 1)
        .map(|(ident, owners)| format!("'{ident}' <- {}", owners.join(", ")))
        .collect();
    if !collisions.is_empty() {
        return Err(CLIError::ConfigurationError(format!(
            "generated type name collision (call paths stay nested; names are not flattened): {}. \
             Set unqualified_names = false or rename one of the SQL objects",
            collisions.join("; ")
        )));
    }

    Ok(AssignedNames { types, routines })
}

pub fn generated_type_ident(schema: &str, name: &str, unqualified_names: bool) -> String {
    let local = pascal_case(name);
    if unqualified_names || schema.eq_ignore_ascii_case(DEFAULT_SCHEMA) {
        local
    } else {
        format!("{}{local}", pascal_case(schema))
    }
}

pub fn value_ident(schema: &str, name: &str, unqualified_names: bool) -> String {
    let local = camel_case(name);
    if unqualified_names || schema.eq_ignore_ascii_case(DEFAULT_SCHEMA) {
        local
    } else {
        format!("{}{}", camel_case(schema), pascal_case(name))
    }
}

pub fn method_ident(name: &str) -> String {
    camel_case(name)
}

pub fn schema_object_ident(schema: &str) -> String {
    sanitize_js_ident(&camel_case(schema))
}

pub fn contract_hash_line(hash: &str) -> String {
    format!("contract_hash: {hash}")
}

pub fn pascal_case(value: &str) -> String {
    let mut name = String::new();
    let mut capitalize = true;
    for ch in value.chars() {
        if ch == '_' || ch == '-' || ch == '.' {
            capitalize = true;
            continue;
        }
        if capitalize {
            for upper in ch.to_uppercase() {
                name.push(upper);
            }
            capitalize = false;
        } else {
            name.push(ch);
        }
    }
    if name.is_empty() {
        "Value".to_string()
    } else {
        name
    }
}

pub fn camel_case(value: &str) -> String {
    let pascal = pascal_case(value);
    let mut chars = pascal.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().collect::<String>() + chars.as_str(),
        None => "value".to_string(),
    }
}

pub fn sanitize_js_ident(value: &str) -> String {
    let mut ident: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if ident.is_empty() || ident.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        ident.insert(0, '_');
    }
    if JS_KEYWORDS.contains(&ident.as_str()) {
        ident.push('_');
    }
    ident
}

const JS_KEYWORDS: &[&str] = &[
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "import",
    "in",
    "instanceof",
    "new",
    "null",
    "return",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "void",
    "while",
    "with",
    "yield",
    "await",
    "let",
    "static",
    "implements",
    "interface",
    "package",
    "private",
    "protected",
    "public",
];

#[cfg(test)]
mod tests {
    use kalamdb_sql::compile_contract_sql;

    use super::*;

    #[test]
    fn schema_prefixed_names_keep_nested_identity() {
        let snapshot = compile_contract_sql(
            "CREATE SCHEMA chat; CREATE TYPE chat.user AS (id TEXT); CREATE PROCEDURE \
             chat.create_message(body TEXT) LANGUAGE TS;",
            "public",
        )
        .unwrap();
        let names = assign_names(
            &snapshot,
            NamingOptions {
                unqualified_names: false,
            },
        )
        .unwrap();
        assert_eq!(names.type_ident("chat.user"), "ChatUser");
        assert_eq!(names.routine_ident("chat.create_message"), "ChatCreateMessage");
        assert_eq!(schema_object_ident("chat"), "chat");
        assert_eq!(method_ident("create_message"), "createMessage");
    }

    #[test]
    fn unqualified_names_fail_on_collision_without_flattening_paths() {
        let snapshot = compile_contract_sql(
            "CREATE SCHEMA chat; CREATE SCHEMA app;
             CREATE TYPE chat.user AS (id TEXT);
             CREATE TYPE app.user AS (id TEXT);",
            "public",
        )
        .unwrap();
        let err = assign_names(
            &snapshot,
            NamingOptions {
                unqualified_names: true,
            },
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("collision"), "{message}");
        assert!(message.contains("chat.user"), "{message}");
        assert!(message.contains("app.user"), "{message}");
        assert!(!message.contains("kalam.chat"), "{message}");
    }
}
