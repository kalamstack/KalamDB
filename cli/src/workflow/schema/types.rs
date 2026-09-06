//! Map contract fields onto generated TypeScript, Dart, and Rust types.

use kalamdb_sql::contracts::ContractField;

use crate::workflow::schema::naming::AssignedNames;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetLang {
    TypeScript,
    Dart,
    Rust,
}

pub fn render_field_type(field: &ContractField, names: &AssignedNames, lang: TargetLang) -> String {
    let mut inner = named_or_builtin(field, names, lang);
    if field.is_array {
        inner = match lang {
            TargetLang::TypeScript => format!("{inner}[]"),
            TargetLang::Dart => format!("List<{inner}>"),
            TargetLang::Rust => format!("Vec<{inner}>"),
        };
    }
    if field.not_null {
        inner
    } else {
        match lang {
            TargetLang::TypeScript => format!("{inner} | null"),
            TargetLang::Dart => format!("{inner}?"),
            TargetLang::Rust => format!("Option<{inner}>"),
        }
    }
}

fn named_or_builtin(field: &ContractField, names: &AssignedNames, lang: TargetLang) -> String {
    if let Some(type_id) = &field.type_id {
        return names.type_ident(type_id.as_str()).to_string();
    }
    let sql_name = field
        .data_type
        .map(|data_type| data_type.sql_name())
        .unwrap_or_else(|| field.type_name.clone());
    builtin(&sql_name, lang).to_string()
}

pub fn builtin(sql_type: &str, lang: TargetLang) -> &'static str {
    let normalized = sql_type.trim().trim_end_matches(',').to_ascii_uppercase();
    let base = normalized.split('(').next().unwrap_or(&normalized);
    match (lang, base) {
        (TargetLang::TypeScript, "BOOLEAN" | "BOOL") => "boolean",
        (TargetLang::TypeScript, "INT" | "INTEGER" | "INT4" | "SMALLINT" | "INT2" | "SERIAL") => {
            "number"
        },
        (TargetLang::TypeScript, "BIGINT" | "INT8" | "INT64" | "BIGSERIAL") => "bigint",
        (TargetLang::TypeScript, "DECIMAL" | "NUMERIC") => "string",
        (TargetLang::TypeScript, "BYTES" | "BYTEA" | "BLOB" | "BINARY") => "Uint8Array",
        (TargetLang::TypeScript, "JSON" | "JSONB") => "JsonValue",
        (TargetLang::TypeScript, _) => "string",

        (TargetLang::Dart, "BOOLEAN" | "BOOL") => "bool",
        (
            TargetLang::Dart,
            "INT" | "INTEGER" | "SMALLINT" | "BIGINT" | "INT2" | "INT4" | "INT8" | "INT64"
            | "UINT64" | "SERIAL" | "BIGSERIAL",
        ) => "int",
        (
            TargetLang::Dart,
            "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL",
        ) => "double",
        (TargetLang::Dart, "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" | "DATE" | "TIME") => {
            "DateTime"
        },
        (TargetLang::Dart, "JSON" | "JSONB" | "FILE") => "Map<String, Object?>",
        (TargetLang::Dart, "BYTES" | "BYTEA" | "BLOB" | "BINARY") => "String",
        (TargetLang::Dart, "EMBEDDING") => "List<double>",
        (TargetLang::Dart, _) => "String",

        (TargetLang::Rust, "BOOLEAN" | "BOOL") => "bool",
        (TargetLang::Rust, "SMALLINT" | "INT2") => "i16",
        (TargetLang::Rust, "INT" | "INTEGER" | "INT4" | "SERIAL") => "i32",
        (TargetLang::Rust, "BIGINT" | "INT8" | "INT64" | "BIGSERIAL") => "i64",
        (TargetLang::Rust, "FLOAT" | "FLOAT4" | "REAL") => "f32",
        (TargetLang::Rust, "DOUBLE" | "FLOAT8") => "f64",
        (TargetLang::Rust, "BYTES" | "BYTEA" | "BLOB" | "BINARY") => "Vec<u8>",
        (TargetLang::Rust, "JSON" | "JSONB") => "serde_json::Value",
        (TargetLang::Rust, _) => "String",
    }
}

pub fn is_named_composite(field: &ContractField) -> bool {
    field.type_id.is_some()
}
