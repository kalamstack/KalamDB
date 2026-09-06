//! Compile multi-file SQL into a [`ContractSnapshot`].

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::DataType;
use kalamdb_commons::models::{NamespaceId, RoutineId, TypeId};

use super::{
    arrow::resolve_arrow_type,
    snapshot::{
        ContractField, ContractRoutine, ContractSnapshot, ContractTable, ContractTableKind,
        ContractType, ContractTypeKind,
    },
    ContractError,
};
use crate::{
    ddl::{
        create_type::{matching_paren, parse_type_reference, split_qualified_ident, take_ident},
        AlterTypeOperation, AlterTypeStatement, CreateNamespaceStatement, CreateProcedureStatement,
        CreateSchemaStatement, CreateTypeBody, CreateTypeStatement, GrantExecuteStatement,
        RevokeExecuteStatement, SetSearchPathStatement, TypeReference, UseNamespaceStatement,
    },
    split_statements,
};

#[derive(Debug, Clone, Copy)]
pub struct ContractSource<'a> {
    pub path: &'a str,
    pub sql:  &'a str,
}

pub fn compile_contract_sql(
    sql: &str,
    default_schema: &str,
) -> Result<ContractSnapshot, ContractError> {
    compile_contract(&[ContractSource { path: "<sql>", sql }], default_schema)
}

pub fn compile_contract(
    sources: &[ContractSource<'_>],
    default_schema: &str,
) -> Result<ContractSnapshot, ContractError> {
    let default_schema = fold_ident(default_schema);
    let mut schemas = BTreeSet::new();
    schemas.insert(default_schema.clone());

    let mut tables: BTreeMap<String, RawTable> = BTreeMap::new();
    let mut types: BTreeMap<String, RawType> = BTreeMap::new();
    let mut routines: BTreeMap<String, ContractRoutine> = BTreeMap::new();
    let mut table_alias: HashMap<String, TypeId> = HashMap::new();
    let mut alters = Vec::new();
    let mut pending_grants = Vec::new();
    let mut pending_revokes = Vec::new();

    let mut ordered: Vec<ContractSource<'_>> = sources.to_vec();
    ordered.sort_by_key(|source| source.path);

    for source in &ordered {
        let statements = split_statements(source.sql)
            .map_err(|err| ContractError::new(format!("{}: {err}", source.path)))?;
        let mut search_path = vec![default_schema.clone()];
        for stmt in statements {
            apply_statement(
                source.path,
                &stmt,
                &mut search_path,
                &default_schema,
                &mut schemas,
                &mut tables,
                &mut types,
                &mut routines,
                &mut table_alias,
                &mut alters,
                &mut pending_grants,
                &mut pending_revokes,
            )?;
        }
    }

    for alter in alters {
        apply_alter(&alter, &mut types)?;
    }
    for grant in pending_grants {
        let routine = routines.get_mut(grant.routine_id.as_str()).ok_or_else(|| {
            ContractError::new(format!("GRANT EXECUTE on unknown procedure '{}'", grant.routine_id))
        })?;
        routine.grants.insert(grant.grantee);
    }
    for revoke in pending_revokes {
        if let Some(routine) = routines.get_mut(revoke.routine_id.as_str()) {
            routine.grants.remove(&revoke.grantee);
        }
    }

    resolve_snapshot(schemas, tables, types, routines, table_alias)
}

struct RawTable {
    schema: String,
    name:   String,
    kind:   ContractTableKind,
    fields: Vec<ContractField>,
}

enum RawType {
    Composite {
        schema: String,
        fields: Vec<ContractField>,
    },
    Enum {
        schema: String,
        labels: Vec<String>,
    },
    Alias {
        schema:   String,
        table_id: String,
    },
}

fn apply_statement(
    path: &str,
    sql: &str,
    search_path: &mut Vec<String>,
    default_schema: &str,
    schemas: &mut BTreeSet<String>,
    tables: &mut BTreeMap<String, RawTable>,
    types: &mut BTreeMap<String, RawType>,
    routines: &mut BTreeMap<String, ContractRoutine>,
    table_alias: &mut HashMap<String, TypeId>,
    alters: &mut Vec<AlterTypeStatement>,
    pending_grants: &mut Vec<GrantExecuteStatement>,
    pending_revokes: &mut Vec<RevokeExecuteStatement>,
) -> Result<(), ContractError> {
    let current = search_path.first().cloned().unwrap_or_else(|| default_schema.to_string());
    let ns = NamespaceId::new(current.clone());

    if let Ok(stmt) = SetSearchPathStatement::parse(sql) {
        *search_path = stmt.schemas.iter().map(|s| fold_ident(s.as_str())).collect();
        schemas.extend(search_path.iter().cloned());
        return Ok(());
    }
    if let Ok(stmt) = UseNamespaceStatement::parse(sql) {
        let schema = fold_ident(stmt.namespace.as_str());
        schemas.insert(schema.clone());
        *search_path = vec![schema];
        return Ok(());
    }
    if let Ok(stmt) = CreateSchemaStatement::parse(sql) {
        schemas.insert(fold_ident(stmt.name.as_str()));
        return Ok(());
    }
    if let Ok(stmt) = CreateNamespaceStatement::parse(sql) {
        schemas.insert(fold_ident(stmt.name.as_str()));
        return Ok(());
    }
    if starts_ci(sql, "CREATE TYPE") {
        let stmt = CreateTypeStatement::parse(sql, &ns).map_err(err(path))?;
        ingest_create_type(stmt, &current, types, table_alias)?;
        return Ok(());
    }
    if starts_ci(sql, "ALTER TYPE") {
        let stmt = AlterTypeStatement::parse(sql, &ns).map_err(err(path))?;
        alters.push(stmt);
        return Ok(());
    }
    if starts_ci(sql, "CREATE PROCEDURE") || starts_ci(sql, "CREATE OR REPLACE PROCEDURE") {
        let stmt = CreateProcedureStatement::parse(sql, &ns).map_err(err(path))?;
        ingest_procedure(stmt, &current, routines)?;
        return Ok(());
    }
    if starts_ci(sql, "GRANT EXECUTE") {
        pending_grants.push(GrantExecuteStatement::parse(sql, &ns).map_err(err(path))?);
        return Ok(());
    }
    if starts_ci(sql, "REVOKE EXECUTE") {
        pending_revokes.push(RevokeExecuteStatement::parse(sql, &ns).map_err(err(path))?);
        return Ok(());
    }
    if looks_like_create_table(sql) {
        ingest_table(sql, &current, tables, types, table_alias)?;
        return Ok(());
    }
    Ok(())
}

fn ingest_create_type(
    stmt: CreateTypeStatement,
    current_schema: &str,
    types: &mut BTreeMap<String, RawType>,
    table_alias: &mut HashMap<String, TypeId>,
) -> Result<(), ContractError> {
    let schema = fold_ident(stmt.namespace_id.as_str());
    let name = fold_ident(&stmt.name);
    let type_id = format!("{schema}.{name}");
    if types.contains_key(&type_id) {
        return Err(ContractError::new(format!("duplicate type '{type_id}'")));
    }
    let raw = match stmt.body {
        CreateTypeBody::Composite { fields } => RawType::Composite {
            schema: schema.clone(),
            fields: fields
                .into_iter()
                .map(|field| field_from_ref(field.name, field.type_ref, &schema))
                .collect(),
        },
        CreateTypeBody::Enum { labels } => RawType::Enum {
            schema: schema.clone(),
            labels,
        },
        CreateTypeBody::FromTable {
            table_namespace_id,
            table_name,
        } => {
            let table_schema = table_namespace_id
                .as_ref()
                .map(|ns| fold_ident(ns.as_str()))
                .unwrap_or_else(|| current_schema.to_string());
            let table_id = format!("{table_schema}.{}", fold_ident(&table_name));
            let alias_id = TypeId::new(type_id.clone());
            if let Some(existing) = table_alias.insert(table_id.clone(), alias_id.clone()) {
                return Err(ContractError::new(format!(
                    "table '{table_id}' already has row alias '{}'",
                    existing
                )));
            }
            RawType::Alias { schema, table_id }
        },
    };
    types.insert(type_id, raw);
    Ok(())
}

fn ingest_procedure(
    stmt: CreateProcedureStatement,
    current_schema: &str,
    routines: &mut BTreeMap<String, ContractRoutine>,
) -> Result<(), ContractError> {
    let schema = fold_ident(stmt.namespace_id.as_str());
    let name = fold_ident(&stmt.name);
    let routine_id = RoutineId::from_parts(Some(&stmt.namespace_id), &name);
    let key = routine_id.as_str().to_string();
    if routines.contains_key(&key) && !stmt.or_replace {
        return Err(ContractError::new(format!("duplicate procedure '{key}'")));
    }
    let parameters = stmt
        .parameters
        .into_iter()
        .map(|param| field_from_ref(param.name, param.type_ref, current_schema))
        .collect();
    let return_type = stmt
        .return_type
        .map(|ty| field_from_ref("returns".to_string(), ty, current_schema));
    routines.insert(
        key,
        ContractRoutine {
            routine_id,
            schema,
            name,
            parameters,
            return_type,
            language: stmt.language,
            security: stmt.security,
            body: stmt.body,
            grants: BTreeSet::new(),
        },
    );
    Ok(())
}

fn ingest_table(
    sql: &str,
    current_schema: &str,
    tables: &mut BTreeMap<String, RawTable>,
    types: &mut BTreeMap<String, RawType>,
    table_alias: &mut HashMap<String, TypeId>,
) -> Result<(), ContractError> {
    let (schema, name, fields, row_alias) = parse_contract_table(sql, current_schema)?;
    let table_id = format!("{schema}.{name}");
    if tables.contains_key(&table_id) {
        return Err(ContractError::new(format!("duplicate table '{table_id}'")));
    }
    if let Some(alias) = row_alias {
        let alias_key = alias.as_str().to_string();
        if types.contains_key(&alias_key) {
            return Err(ContractError::new(format!("duplicate type '{alias_key}'")));
        }
        if let Some(existing) = table_alias.insert(table_id.clone(), alias.clone()) {
            return Err(ContractError::new(format!(
                "table '{table_id}' already has row alias '{}'",
                existing
            )));
        }
        types.insert(
            alias_key,
            RawType::Alias {
                schema:   schema.clone(),
                table_id: table_id.clone(),
            },
        );
    }
    tables.insert(
        table_id,
        RawTable {
            schema,
            name,
            kind: table_kind_from_sql(sql),
            fields,
        },
    );
    Ok(())
}

fn parse_contract_table(
    sql: &str,
    current_schema: &str,
) -> Result<(String, String, Vec<ContractField>, Option<TypeId>), ContractError> {
    let rest = strip_create_table_prefix(sql)
        .ok_or_else(|| ContractError::new("expected CREATE TABLE"))?;
    let rest = if starts_ci(rest, "IF NOT EXISTS") {
        rest["IF NOT EXISTS".len()..].trim_start()
    } else {
        rest
    };
    let (qual, after) = split_qualified_ident(rest).map_err(ContractError::new)?;
    let schema = qual
        .namespace_id
        .as_ref()
        .map(|ns| fold_ident(ns.as_str()))
        .unwrap_or_else(|| current_schema.to_string());
    let name = fold_ident(&qual.name);
    let after = after.trim_start();
    if !after.starts_with('(') {
        return Err(ContractError::new("expected column list"));
    }
    let close =
        matching_paren(after).ok_or_else(|| ContractError::new("unterminated column list"))?;
    let body = &after[1..close];
    let mut fields = Vec::new();
    for part in crate::ddl::create_type::split_top_level(body, ',') {
        let part = part.trim();
        if part.is_empty() || starts_ci(part, "PRIMARY KEY") || starts_ci(part, "CONSTRAINT") {
            continue;
        }
        fields.push(parse_table_column(part, &schema)?);
    }
    let mut leftover = after[close + 1..].trim_start();
    let mut row_alias = None;
    if starts_ci(leftover, "ROW TYPE") {
        leftover = leftover["ROW TYPE".len()..].trim_start();
        let (alias, rest) = split_qualified_ident(leftover).map_err(ContractError::new)?;
        let alias_schema = alias
            .namespace_id
            .as_ref()
            .map(|ns| fold_ident(ns.as_str()))
            .unwrap_or_else(|| schema.clone());
        if alias_schema != schema {
            return Err(ContractError::new(
                "row alias and source table must remain in the same schema",
            ));
        }
        row_alias = Some(TypeId::from_parts(
            Some(&NamespaceId::new(&alias_schema)),
            &fold_ident(&alias.name),
        ));
        leftover = rest.trim_start();
        let _ = leftover;
    }
    Ok((schema, name, fields, row_alias))
}

fn parse_table_column(input: &str, schema: &str) -> Result<ContractField, ContractError> {
    let (name, rest) = take_ident(input).map_err(ContractError::new)?;
    let (mut type_ref, rest) =
        parse_type_reference(rest.trim_start()).map_err(ContractError::new)?;
    let leftover = rest.to_ascii_uppercase();
    if leftover.contains("NOT NULL") || leftover.contains("PRIMARY KEY") {
        type_ref.not_null = true;
    }
    if leftover.contains("NONEMPTY") {
        type_ref.nonempty = true;
    }
    Ok(field_from_ref(name, type_ref, schema))
}

fn strip_create_table_prefix(sql: &str) -> Option<&str> {
    let trimmed = sql.trim();
    if starts_ci(trimmed, "CREATE USER TABLE") {
        Some(trimmed["CREATE USER TABLE".len()..].trim_start())
    } else if starts_ci(trimmed, "CREATE SHARED TABLE") {
        Some(trimmed["CREATE SHARED TABLE".len()..].trim_start())
    } else if starts_ci(trimmed, "CREATE STREAM TABLE") {
        Some(trimmed["CREATE STREAM TABLE".len()..].trim_start())
    } else if starts_ci(trimmed, "CREATE TABLE") {
        Some(trimmed["CREATE TABLE".len()..].trim_start())
    } else {
        None
    }
}

fn looks_like_create_table(sql: &str) -> bool {
    strip_create_table_prefix(sql).is_some()
}

fn table_kind_from_sql(sql: &str) -> ContractTableKind {
    let trimmed = sql.trim();
    if starts_ci(trimmed, "CREATE USER TABLE") {
        return ContractTableKind::User;
    }
    if starts_ci(trimmed, "CREATE SHARED TABLE") {
        return ContractTableKind::Shared;
    }
    if starts_ci(trimmed, "CREATE STREAM TABLE") {
        return ContractTableKind::Stream;
    }
    using_kalamdb_kind(sql).unwrap_or(ContractTableKind::Unspecified)
}

fn using_kalamdb_kind(sql: &str) -> Option<ContractTableKind> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("USING KALAMDB") {
        return None;
    }
    let normalized = upper.replace('"', "'").replace(' ', "");
    if normalized.contains("TYPE='STREAM'") {
        Some(ContractTableKind::Stream)
    } else if normalized.contains("TYPE='SHARED'") {
        Some(ContractTableKind::Shared)
    } else if normalized.contains("TYPE='USER'") {
        Some(ContractTableKind::User)
    } else {
        Some(ContractTableKind::Unspecified)
    }
}

fn apply_alter(
    stmt: &AlterTypeStatement,
    types: &mut BTreeMap<String, RawType>,
) -> Result<(), ContractError> {
    let key = stmt.type_id.as_str().to_ascii_lowercase();
    let raw = types.get_mut(&key).ok_or_else(|| {
        ContractError::new(format!("ALTER TYPE on unknown type '{}'", stmt.type_id))
    })?;
    match (&mut *raw, &stmt.operation) {
        (
            RawType::Composite { fields, schema, .. },
            AlterTypeOperation::AddAttribute { field, type_ref },
        ) => {
            fields.push(field_from_ref(field.clone(), type_ref.clone(), schema));
            Ok(())
        },
        (RawType::Composite { fields, .. }, AlterTypeOperation::DropAttribute { field, .. }) => {
            fields.retain(|item| item.name != fold_ident(field));
            Ok(())
        },
        (RawType::Composite { fields, .. }, AlterTypeOperation::RenameAttribute { from, to }) => {
            let from = fold_ident(from);
            let to = fold_ident(to);
            if let Some(field) = fields.iter_mut().find(|item| item.name == from) {
                field.name = to;
            }
            Ok(())
        },
        (
            RawType::Composite { fields, schema, .. },
            AlterTypeOperation::AlterAttributeType { field, type_ref },
        ) => {
            let name = fold_ident(field);
            if let Some(existing) = fields.iter_mut().find(|item| item.name == name) {
                *existing = field_from_ref(name, type_ref.clone(), schema);
            }
            Ok(())
        },
        (_, AlterTypeOperation::SetSchema { schema }) => {
            match raw {
                RawType::Composite {
                    schema: current, ..
                }
                | RawType::Enum {
                    schema: current, ..
                }
                | RawType::Alias {
                    schema: current, ..
                } => {
                    *current = fold_ident(schema.as_str());
                },
            }
            Ok(())
        },
        _ => Err(ContractError::new(format!(
            "ALTER TYPE '{}' is not valid for this type kind",
            stmt.type_id
        ))),
    }
}

fn field_from_ref(name: String, type_ref: TypeReference, current_schema: &str) -> ContractField {
    let (type_name, type_id) = if let Some(data_type) = type_ref.builtin_data_type() {
        (data_type.sql_name(), None)
    } else {
        let schema = type_ref
            .namespace_id
            .as_ref()
            .map(|ns| fold_ident(ns.as_str()))
            .unwrap_or_else(|| current_schema.to_string());
        let type_name = fold_ident(&type_ref.name);
        let id = TypeId::from_parts(Some(&NamespaceId::new(&schema)), &type_name);
        (id.to_string(), Some(id))
    };
    ContractField {
        name: fold_ident(&name),
        type_name,
        type_id,
        data_type: type_ref.data_type,
        is_array: type_ref.is_array,
        not_null: type_ref.not_null,
        nonempty: type_ref.nonempty,
    }
}

fn resolve_snapshot(
    schemas: BTreeSet<String>,
    tables: BTreeMap<String, RawTable>,
    types: BTreeMap<String, RawType>,
    routines: BTreeMap<String, ContractRoutine>,
    table_alias: HashMap<String, TypeId>,
) -> Result<ContractSnapshot, ContractError> {
    for table_id in tables.keys() {
        if types.contains_key(table_id) {
            return Err(ContractError::new(format!(
                "type '{table_id}' collides with implicit table row type"
            )));
        }
    }

    let mut kind_map: HashMap<String, (ContractTypeKind, Option<DataType>)> = HashMap::new();
    let mut snapshot_types = BTreeMap::new();
    let mut snapshot_tables = BTreeMap::new();

    for (table_id, table) in &tables {
        snapshot_tables.insert(
            table_id.clone(),
            ContractTable {
                table_id:     table_id.clone(),
                schema:       table.schema.clone(),
                name:         table.name.clone(),
                kind:         table.kind,
                row_type_id:  TypeId::new(table_id.clone()),
                row_alias_id: table_alias.get(table_id).cloned(),
                fields:       table.fields.clone(),
            },
        );
        kind_map.insert(
            table_id.clone(),
            (
                ContractTypeKind::ImplicitTableRow {
                    table_id: table_id.clone(),
                    fields:   table.fields.clone(),
                },
                None,
            ),
        );
    }

    for (type_id, raw) in &types {
        let kind = match raw {
            RawType::Composite { fields, .. } => ContractTypeKind::Composite {
                fields: fields.clone(),
            },
            RawType::Enum { labels, .. } => ContractTypeKind::Enum {
                labels: labels.clone(),
            },
            RawType::Alias { table_id, .. } => {
                if !snapshot_tables.contains_key(table_id) {
                    return Err(ContractError::new(format!(
                        "row alias '{type_id}' references missing table '{table_id}'"
                    )));
                }
                ContractTypeKind::RowAlias {
                    source: TypeId::new(table_id.clone()),
                }
            },
        };
        kind_map.insert(type_id.clone(), (kind, None));
    }

    for type_id in kind_map.keys().cloned().collect::<Vec<_>>() {
        let mut visiting = Vec::new();
        let type_ref = TypeReference {
            namespace_id: None,
            name:         type_id.clone(),
            data_type:    None,
            is_array:     false,
            not_null:     true,
            nonempty:     false,
        };
        let arrow = resolve_arrow_type(&type_ref, &kind_map, &mut visiting)?;
        if let Some(entry) = kind_map.get_mut(&type_id) {
            entry.1 = Some(arrow);
        }
    }

    for (type_id, (kind, arrow)) in &kind_map {
        let (schema, name) = split_id(type_id);
        snapshot_types.insert(
            type_id.clone(),
            ContractType {
                type_id: TypeId::new(type_id.clone()),
                schema,
                name,
                kind: kind.clone(),
                arrow: arrow.clone().ok_or_else(|| {
                    ContractError::new(format!("failed to resolve Arrow type for '{type_id}'"))
                })?,
            },
        );
    }

    for routine in routines.values() {
        for field in routine.parameters.iter().chain(routine.return_type.iter()) {
            if let Some(type_id) = &field.type_id {
                if !kind_map.contains_key(type_id.as_str()) {
                    return Err(ContractError::new(format!(
                        "unknown type '{}' in procedure '{}'",
                        type_id, routine.routine_id
                    )));
                }
            }
        }
    }

    Ok(ContractSnapshot {
        schemas,
        tables: snapshot_tables,
        types: snapshot_types,
        routines,
    })
}

fn split_id(id: &str) -> (String, String) {
    match id.rsplit_once('.') {
        Some((schema, name)) => (schema.to_string(), name.to_string()),
        None => (String::new(), id.to_string()),
    }
}

fn fold_ident(value: &str) -> String {
    value.trim().trim_matches('"').to_ascii_lowercase()
}

fn starts_ci(sql: &str, prefix: &str) -> bool {
    let sql = sql.trim_start();
    sql.len() >= prefix.len()
        && sql.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

fn err(path: &str) -> impl Fn(String) -> ContractError + '_ {
    move |message| ContractError::new(format!("{path}: {message}"))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::contracts::{canonical_contract_hash, diff_contracts};

    #[test]
    fn search_path_and_use_canonicalize() {
        let via_search = compile_contract_sql(
            "SET search_path TO chat; CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT \
             NULL);",
            "public",
        )
        .unwrap();
        let via_use = compile_contract_sql(
            "USE chat; CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL);",
            "public",
        )
        .unwrap();
        assert_eq!(canonical_contract_hash(&via_search), canonical_contract_hash(&via_use));
        assert!(via_search.types.contains_key("chat.users"));
        assert!(matches!(
            via_search.types["chat.users"].kind,
            ContractTypeKind::ImplicitTableRow { .. }
        ));
    }

    #[test]
    fn multi_file_order_independent() {
        let type_sql = "CREATE SCHEMA chat; CREATE TYPE chat.address AS (city TEXT, country TEXT);";
        let table_sql =
            "CREATE TABLE chat.customers (id BIGINT PRIMARY KEY, address chat.address);";
        let a = compile_contract(
            &[
                ContractSource {
                    path: "a.sql",
                    sql:  table_sql,
                },
                ContractSource {
                    path: "b.sql",
                    sql:  type_sql,
                },
            ],
            "public",
        )
        .unwrap();
        let b = compile_contract(
            &[
                ContractSource {
                    path: "b.sql",
                    sql:  type_sql,
                },
                ContractSource {
                    path: "a.sql",
                    sql:  table_sql,
                },
            ],
            "public",
        )
        .unwrap();
        assert_eq!(canonical_contract_hash(&a), canonical_contract_hash(&b));
        assert!(matches!(a.types["chat.address"].arrow, DataType::Struct(_)));
        assert!(matches!(a.types["chat.customers"].arrow, DataType::Struct(_)));
    }

    #[test]
    fn row_alias_does_not_copy_fields() {
        let snapshot = compile_contract_sql(
            "CREATE SCHEMA chat;
             CREATE TABLE chat.users (
               id BIGINT PRIMARY KEY,
               first_name TEXT NOT NULL
             ) ROW TYPE chat.user;
             CREATE TYPE chat.person FROM TABLE chat.users;",
            "public",
        );
        assert!(snapshot.is_err(), "second alias for the same table must fail");

        let snapshot = compile_contract_sql(
            "CREATE SCHEMA chat;
             CREATE TABLE chat.users (
               id BIGINT PRIMARY KEY,
               first_name TEXT NOT NULL
             ) ROW TYPE chat.user;",
            "public",
        )
        .unwrap();
        match &snapshot.types["chat.user"].kind {
            ContractTypeKind::RowAlias { source } => {
                assert_eq!(source.as_str(), "chat.users");
            },
            other => panic!("expected alias, got {other:?}"),
        }
        assert!(matches!(snapshot.types["chat.user"].arrow, DataType::Struct(_)));
    }

    #[test]
    fn arrays_resolve_to_list() {
        let snapshot = compile_contract_sql(
            "CREATE SCHEMA chat;
             CREATE TYPE chat.recipient_result AS (user_id TEXT NOT NULL);
             CREATE TYPE chat.send_message_result AS (
               recipients chat.recipient_result[] NOT NULL NONEMPTY
             );",
            "public",
        )
        .unwrap();
        assert!(matches!(snapshot.types["chat.send_message_result"].arrow, DataType::Struct(_)));
        let DataType::Struct(fields) = &snapshot.types["chat.send_message_result"].arrow else {
            panic!("struct");
        };
        assert!(matches!(fields[0].data_type(), DataType::List(_)));
    }

    #[test]
    fn comments_and_formatting_do_not_change_hash() {
        let compact = compile_contract_sql(
            "CREATE SCHEMA chat; CREATE TYPE chat.address AS (city TEXT, country TEXT);",
            "public",
        )
        .unwrap();
        let pretty = compile_contract_sql(
            "-- addresses\nCREATE SCHEMA chat;\n\nCREATE TYPE chat.address AS (\n  city TEXT,\n  \
             country TEXT\n);",
            "public",
        )
        .unwrap();
        assert_eq!(canonical_contract_hash(&compact), canonical_contract_hash(&pretty));
    }

    #[test]
    fn cycles_and_missing_refs_fail() {
        let cycle =
            compile_contract_sql("CREATE TYPE a AS (b b); CREATE TYPE b AS (a a);", "public")
                .unwrap_err();
        assert!(cycle.message.contains("cycle"));

        let missing = compile_contract_sql(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, addr missing.address);",
            "public",
        )
        .unwrap_err();
        assert!(missing.message.contains("unknown type"));
    }

    #[test]
    fn table_kind_follows_create_and_using_kalamdb() {
        let snapshot = compile_contract_sql(
            "CREATE USER TABLE chat.todos (id TEXT PRIMARY KEY);
             CREATE STREAM TABLE chat.events (id BIGINT PRIMARY KEY);
             CREATE TABLE chat.messages (id TEXT PRIMARY KEY) USING kalamdb WITH (type = 'user');",
            "public",
        )
        .unwrap();
        assert_eq!(snapshot.tables["chat.todos"].kind, ContractTableKind::User);
        assert_eq!(snapshot.tables["chat.events"].kind, ContractTableKind::Stream);
        assert_eq!(snapshot.tables["chat.messages"].kind, ContractTableKind::User);
    }

    #[test]
    fn collision_with_implicit_row_type_fails() {
        let err = compile_contract_sql(
            "CREATE TABLE chat.users (id BIGINT PRIMARY KEY);
             CREATE TYPE chat.users AS (id TEXT);",
            "public",
        )
        .unwrap_err();
        assert!(err.message.contains("collides") || err.message.contains("duplicate"));
    }

    #[test]
    fn diff_includes_type_evolution_security_and_grants() {
        let before = compile_contract_sql(
            "CREATE SCHEMA chat;
             CREATE TYPE chat.address AS (city TEXT);
             CREATE PROCEDURE chat.get_user(id TEXT)
             RETURNS chat.address
             LANGUAGE SQL
             SECURITY INVOKER
             AS $$ SELECT 1; $$;
             GRANT EXECUTE ON PROCEDURE chat.get_user TO PUBLIC;",
            "public",
        )
        .unwrap();
        let after = compile_contract_sql(
            "CREATE SCHEMA chat;
             CREATE TYPE chat.address AS (city TEXT, country TEXT);
             CREATE PROCEDURE chat.get_user(id TEXT)
             RETURNS chat.address
             LANGUAGE SQL
             SECURITY DEFINER
             AS $$ SELECT 1; $$;
             GRANT EXECUTE ON PROCEDURE chat.get_user TO user;",
            "public",
        )
        .unwrap();
        let diff = diff_contracts(&before, &after);
        let joined = diff.statements.join("\n");
        assert!(joined.contains("ALTER TYPE chat.address ADD ATTRIBUTE country TEXT"));
        assert!(joined.contains("SECURITY DEFINER"));
        assert!(joined.contains("REVOKE EXECUTE ON PROCEDURE chat.get_user FROM PUBLIC"));
        assert!(joined.contains("GRANT EXECUTE ON PROCEDURE chat.get_user TO user"));
    }
}
