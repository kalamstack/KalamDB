//! Schema loading for sql and remote modes.

use std::{collections::BTreeMap, path::Path};

use chrono::Utc;
use kalamdb_commons::NamespaceId;
use kalamdb_sql::{canonical_contract_hash, compile_contract, ContractSnapshot, ContractSource};

use crate::{
    error::{CLIError, Result},
    workflow::{
        io::read_schema_file,
        project::{
            config::{KalamProjectConfig, SchemaMode},
            identifiers::{parse_table_name, parse_table_ref},
        },
        schema::{
            model::{ColumnDefinition, SchemaOrigin, SchemaSnapshot, TableDefinition, TableKind},
            naming::DEFAULT_SCHEMA,
        },
    },
};

pub fn load_schema_snapshot(
    project_root: &Path,
    config: &KalamProjectConfig,
) -> Result<SchemaSnapshot> {
    match config.schema.mode {
        SchemaMode::Sql => load_from_sql_file(project_root, config),
        SchemaMode::Remote => Err(CLIError::ConfigurationError(
            "remote schema mode requires `kalam schema pull`; use sql mode for local files".into(),
        )),
    }
}

pub fn load_from_sql_file(
    project_root: &Path,
    config: &KalamProjectConfig,
) -> Result<SchemaSnapshot> {
    let path = config
        .schema_source_path(project_root)
        .ok_or_else(|| CLIError::ConfigurationError("schema.path is not configured".into()))?;

    let sql = read_schema_file(&path)?;

    parse_sql_schema(&sql).map(|mut snapshot| {
        snapshot.origin = SchemaOrigin::File;
        snapshot
    })
}

pub fn compile_project_contract(
    project_root: &Path,
    config: &KalamProjectConfig,
) -> Result<(ContractSnapshot, String)> {
    match config.schema.mode {
        SchemaMode::Sql => {},
        SchemaMode::Remote => {
            return Err(CLIError::ConfigurationError(
                "schema generation requires local SQL files; set schema.mode = 'sql'".into(),
            ));
        },
    }
    let path = config
        .schema_source_path(project_root)
        .ok_or_else(|| CLIError::ConfigurationError("schema.path is not configured".into()))?;
    if !path.exists() {
        return Err(CLIError::FileError(format!(
            "schema file '{}' does not exist",
            path.display()
        )));
    }

    let mut owned = Vec::new();
    if path.is_dir() {
        let mut files = Vec::new();
        collect_sql_files(&path, &mut files)?;
        files.sort();
        for file in files {
            let sql = read_schema_file(&file)?;
            let display = file.strip_prefix(project_root).unwrap_or(&file).display().to_string();
            owned.push((display, sql));
        }
    } else {
        let sql = read_schema_file(&path)?;
        let display = path.strip_prefix(project_root).unwrap_or(&path).display().to_string();
        owned.push((display, sql));
    }

    if owned.is_empty() {
        owned.push(("<empty>".to_string(), String::new()));
    }

    let sources: Vec<ContractSource<'_>> = owned
        .iter()
        .map(|(path, sql)| ContractSource {
            path: path.as_str(),
            sql:  sql.as_str(),
        })
        .collect();
    let snapshot = compile_contract(&sources, DEFAULT_SCHEMA).map_err(|err| {
        CLIError::ConfigurationError(format!("failed to compile schema contract: {}", err.message))
    })?;
    let hash = canonical_contract_hash(&snapshot);
    Ok((snapshot, hash))
}

fn collect_sql_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir).map_err(|error| {
        CLIError::FileError(format!("failed to read schema directory '{}': {error}", dir.display()))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            CLIError::FileError(format!(
                "failed to read schema directory '{}': {error}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_sql_files(&path, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
            out.push(path);
        }
    }
    Ok(())
}

pub fn parse_sql_schema(sql: &str) -> Result<SchemaSnapshot> {
    let mut tables = BTreeMap::new();
    let normalized = normalize_sql(sql);
    let statements = split_create_table_statements(&normalized);
    // pg_kalam / Drizzle files mix ordinary Postgres tables with
    // `USING kalamdb`. Native Kalam schema.sql files use plain
    // `CREATE TABLE` and must still generate. Skip unspecified tables
    // only when this file already contains at least one kalamdb table.
    let skip_plain_postgres = statements.iter().any(|statement| uses_kalamdb(statement));

    for statement in statements {
        if let Some(table) = parse_create_table(&statement, skip_plain_postgres)? {
            tables.insert(table.name.clone(), table);
        }
    }

    Ok(SchemaSnapshot {
        origin: SchemaOrigin::File,
        tables,
        captured_at: Utc::now(),
    })
}

pub fn pull_remote_schema(
    _project_root: &Path,
    _config: &KalamProjectConfig,
    _url: &str,
    _namespace: &NamespaceId,
) -> Result<SchemaSnapshot> {
    Err(CLIError::ConfigurationError(
        "schema pull requires a connected KalamDB server; start the server and authenticate, then \
         retry `kalam schema pull`"
            .into(),
    ))
}

fn normalize_sql(sql: &str) -> String {
    sql.lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("--") {
                String::new()
            } else {
                trimmed.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_create_table_statements(sql: &str) -> Vec<String> {
    let upper = sql.to_ascii_uppercase();
    let mut starts = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel) = upper[search_from..].find("CREATE ") {
        let abs = search_from + rel;
        let after_create = upper[abs + "CREATE ".len()..].trim_start();
        if after_create.starts_with("TABLE")
            || after_create.starts_with("USER TABLE")
            || after_create.starts_with("SHARED TABLE")
            || after_create.starts_with("STREAM TABLE")
        {
            starts.push(abs);
        }
        search_from = abs + "CREATE ".len();
    }

    let mut statements = Vec::with_capacity(starts.len());
    for (index, start) in starts.iter().enumerate() {
        let end = starts.get(index + 1).copied().unwrap_or(sql.len());
        let mut segment = sql[*start..end].trim().to_string();
        if let Some(semi) = segment.find(';') {
            segment.truncate(semi);
        }
        if !segment.is_empty() {
            statements.push(segment);
        }
    }
    statements
}

fn parse_create_table(
    statement: &str,
    skip_plain_postgres: bool,
) -> Result<Option<TableDefinition>> {
    let Some((mut kind, rest)) = strip_create_table_prefix(statement) else {
        return Ok(None);
    };

    let (name, body, suffix) = parse_table_name_and_body(rest)?;
    if let Some(kalam_kind) = parse_using_kalamdb_kind(&suffix) {
        kind = kalam_kind;
    } else if skip_plain_postgres && kind == TableKind::Unspecified {
        return Ok(None);
    }
    let columns = parse_columns(&body)?;

    Ok(Some(TableDefinition {
        name,
        kind,
        columns,
    }))
}

fn uses_kalamdb(statement: &str) -> bool {
    statement.to_ascii_uppercase().contains("USING KALAMDB")
}

fn strip_create_table_prefix(statement: &str) -> Option<(TableKind, &str)> {
    let trimmed = statement.trim();
    if let Some(rest) = strip_ascii_prefix(trimmed, "CREATE USER TABLE") {
        Some((TableKind::User, rest.trim()))
    } else if let Some(rest) = strip_ascii_prefix(trimmed, "CREATE SHARED TABLE") {
        Some((TableKind::Shared, rest.trim()))
    } else if let Some(rest) = strip_ascii_prefix(trimmed, "CREATE STREAM TABLE") {
        Some((TableKind::Stream, rest.trim()))
    } else if let Some(rest) = strip_ascii_prefix(trimmed, "CREATE TABLE") {
        Some((TableKind::Unspecified, rest.trim()))
    } else {
        None
    }
}

fn strip_ascii_prefix<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    if value.len() < prefix.len() {
        return None;
    }
    if value.get(..prefix.len())?.eq_ignore_ascii_case(prefix) {
        Some(&value[prefix.len()..])
    } else {
        None
    }
}

fn parse_table_name_and_body(rest: &str) -> Result<(String, String, String)> {
    let rest = rest.trim();
    let open_paren = rest
        .find('(')
        .ok_or_else(|| CLIError::ParseError("expected '(' after table name".into()))?;
    let mut name_part = rest[..open_paren].trim();
    if let Some(stripped) = strip_ascii_prefix(name_part, "IF NOT EXISTS") {
        name_part = stripped.trim();
    }
    let name = name_part.trim_matches('"').trim_matches('`').trim_matches('\'').to_string();
    validate_parsed_table_name(&name)?;

    let close_paren = matching_paren_close(rest, open_paren)
        .ok_or_else(|| CLIError::ParseError("expected ')' closing column list".into()))?;
    let body = rest[open_paren + 1..close_paren].to_string();
    let suffix = rest[close_paren + 1..].trim().to_string();
    Ok((name, body, suffix))
}

fn matching_paren_close(value: &str, open_paren: usize) -> Option<usize> {
    let mut depth = 0u32;
    for (offset, ch) in value[open_paren..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(open_paren + offset);
                }
            },
            _ => {},
        }
    }
    None
}

fn parse_using_kalamdb_kind(suffix: &str) -> Option<TableKind> {
    let upper = suffix.to_ascii_uppercase();
    if !upper.contains("USING KALAMDB") {
        return None;
    }
    let normalized = upper.replace('"', "'").replace(' ', "");
    if normalized.contains("TYPE='STREAM'") {
        Some(TableKind::Stream)
    } else if normalized.contains("TYPE='SHARED'") {
        Some(TableKind::Shared)
    } else if normalized.contains("TYPE='USER'") {
        Some(TableKind::User)
    } else {
        Some(TableKind::Unspecified)
    }
}

fn validate_parsed_table_name(name: &str) -> Result<()> {
    if name.contains('.') {
        parse_table_ref(name)?;
    } else {
        parse_table_name(name)?;
    }
    Ok(())
}

fn parse_columns(body: &str) -> Result<Vec<ColumnDefinition>> {
    let mut columns = Vec::new();
    let mut table_primary_keys = Vec::new();
    for part in split_column_parts(body) {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("PRIMARY KEY") {
            table_primary_keys.extend(parse_primary_key_columns(trimmed));
            continue;
        }
        if upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CONSTRAINT")
        {
            continue;
        }

        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.len() < 2 {
            continue;
        }

        let name = tokens[0].trim_matches('"').trim_matches('`').to_string();
        let sql_type = tokens[1].trim_end_matches(',').to_string();
        let upper_joined = trimmed.to_ascii_uppercase();
        let nullable = !upper_joined.contains("NOT NULL");
        let primary_key = upper_joined.contains("PRIMARY KEY");

        columns.push(ColumnDefinition {
            name,
            sql_type,
            nullable,
            primary_key,
        });
    }

    if !table_primary_keys.is_empty() {
        for column in &mut columns {
            if table_primary_keys.iter().any(|name| name.eq_ignore_ascii_case(&column.name)) {
                column.primary_key = true;
                column.nullable = false;
            }
        }
    }
    Ok(columns)
}

fn parse_primary_key_columns(constraint: &str) -> Vec<String> {
    let Some(open) = constraint.find('(') else {
        return Vec::new();
    };
    let Some(close) = constraint.rfind(')') else {
        return Vec::new();
    };
    if close <= open {
        return Vec::new();
    }
    constraint[open + 1..close]
        .split(',')
        .map(|part| part.trim().trim_matches('"').trim_matches('`').to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

fn split_column_parts(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth: u32 = 0;

    for ch in body.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            },
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            },
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            },
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table_statement() {
        let sql = r#"
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
"#;
        let snapshot = parse_sql_schema(sql).unwrap();
        let users = snapshot.tables.get("users").unwrap();
        assert_eq!(users.columns.len(), 2);
        assert_eq!(users.columns[0].name, "id");
        assert!(!users.columns[1].nullable);
        assert!(users.columns[0].primary_key);
        assert_eq!(users.kind, TableKind::Unspecified);
    }

    #[test]
    fn parse_create_user_and_stream_tables() {
        let sql = r#"
CREATE USER TABLE IF NOT EXISTS app.todos (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL
);
CREATE STREAM TABLE app.events (
  id BIGINT NOT NULL,
  payload JSON,
  PRIMARY KEY (id)
);
"#;
        let snapshot = parse_sql_schema(sql).unwrap();
        let todos = snapshot.tables.get("app.todos").unwrap();
        assert_eq!(todos.kind, TableKind::User);
        assert!(todos.columns[0].primary_key);

        let events = snapshot.tables.get("app.events").unwrap();
        assert_eq!(events.kind, TableKind::Stream);
        assert!(events.columns.iter().any(|column| column.name == "id" && column.primary_key));
    }

    #[test]
    fn parse_mixed_drizzle_keeps_only_kalamdb_tables() {
        let sql = r#"
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  content JSONB NOT NULL,
  attachment FILE,
  created TEXT NOT NULL
) USING kalamdb WITH (
  type = 'user',
  flush_policy = 'rows:100000,interval:10800'
);
CREATE TABLE IF NOT EXISTS notifications (
  id TEXT PRIMARY KEY,
  template_id TEXT NOT NULL,
  params JSONB NOT NULL,
  created TEXT NOT NULL
) USING kalamdb WITH (type = 'user');
"#;
        let snapshot = parse_sql_schema(sql).unwrap();
        assert_eq!(snapshot.tables.len(), 2);
        assert!(snapshot.tables.contains_key("messages"));
        assert!(snapshot.tables.contains_key("notifications"));
        assert!(!snapshot.tables.contains_key("users"));
        assert!(!snapshot.tables.contains_key("conversations"));
        assert_eq!(snapshot.tables["messages"].kind, TableKind::User);
    }

    #[test]
    fn parse_postgres_using_kalamdb_user_tables() {
        let sql = r#"
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  content JSONB NOT NULL,
  attachment FILE,
  created TEXT NOT NULL
) USING kalamdb WITH (
  type = 'user',
  flush_policy = 'rows:100000,interval:10800'
);
"#;
        let snapshot = parse_sql_schema(sql).unwrap();
        let messages = snapshot.tables.get("messages").unwrap();
        assert_eq!(messages.kind, TableKind::User);
        assert_eq!(messages.columns.len(), 4);
        assert_eq!(messages.columns[1].sql_type, "JSONB");
        assert_eq!(messages.columns[2].sql_type, "FILE");
        assert!(messages.columns[2].nullable);
    }
}
