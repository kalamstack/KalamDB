mod create_policy;
mod index;
mod table;
mod topic;

use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use crate::{
    diff::SchemaDiffError,
    model::Schema,
    parser::{
        create_policy::{attach_policies, is_create_policy, parse_create_policy},
        table::{
            extract_kalam_table_kind, parse_create_namespace, remove_kalam_table_kind,
            table_from_create,
        },
        topic::{
            attach_topic_sources, is_alter_topic_add_source, is_create_topic,
            parse_alter_topic_add_source, parse_create_topic, parse_drop_topic,
        },
    },
    sql::{
        extract_with_options, normalize_object_key, strip_trailing_with_options,
        trim_leading_sql_comments,
    },
};

pub(crate) fn parse_schema(path: &str, sql: &str) -> Result<Schema, SchemaDiffError> {
    let dialect = PostgreSqlDialect {};
    let mut schema = Schema::default();
    let mut pending_topic_sources = Vec::new();
    let mut pending_policies = Vec::new();
    let mut pending_indexes = Vec::new();

    let statements = kalamdb_sql::split_statements(sql).map_err(|err| SchemaDiffError::Parse {
        message: format!("{path}: {err}"),
    })?;

    for raw_stmt in statements {
        let raw_stmt = raw_stmt.trim();

        if raw_stmt.is_empty() {
            continue;
        }

        let custom_stmt = trim_leading_sql_comments(raw_stmt);

        if custom_stmt.is_empty() {
            continue;
        }

        if let Some(namespace) = parse_create_namespace(custom_stmt) {
            schema.namespaces.insert(normalize_object_key(&namespace));
            continue;
        }

        if is_create_topic(custom_stmt) {
            let topic = parse_create_topic(path, custom_stmt)?;

            if schema.topics.contains_key(&topic.key) {
                return Err(SchemaDiffError::Parse {
                    message: format!("{path}: duplicate topic definition for {}", topic.name_sql),
                });
            }

            schema.topics.insert(topic.key.clone(), topic);
            continue;
        }

        if is_alter_topic_add_source(custom_stmt) {
            pending_topic_sources.push(parse_alter_topic_add_source(path, custom_stmt)?);
            continue;
        }

        if is_create_policy(custom_stmt) {
            pending_policies.push(parse_create_policy(path, custom_stmt)?);
            continue;
        }

        if crate::parser::index::is_index_ddl(custom_stmt) {
            pending_indexes.push(crate::parser::index::parse_index_ddl(path, custom_stmt)?);
            continue;
        }

        if crate::sql::is_contract_ddl(custom_stmt) {
            continue;
        }

        if let Some(topic_key) = parse_drop_topic(path, custom_stmt)? {
            schema.topics.remove(&topic_key);
            continue;
        }

        let kind_from_prefix = extract_kalam_table_kind(custom_stmt);
        let with_options = extract_with_options(custom_stmt);
        let parseable_stmt = crate::sql::strip_row_type_clause(&strip_trailing_with_options(
            &remove_kalam_table_kind(custom_stmt),
        ));

        let parsed = Parser::parse_sql(&dialect, &parseable_stmt).map_err(|source| {
            SchemaDiffError::Parse {
                message: format!(
                    "{path}: failed to parse statement:\n{raw_stmt}\nparser error: {source}"
                ),
            }
        })?;

        for stmt in parsed {
            match stmt {
                Statement::CreateTable(create_table) => {
                    let table =
                        table_from_create(create_table, kind_from_prefix, with_options.clone())
                            .map_err(|message| SchemaDiffError::Parse { message })?;

                    if schema.tables.contains_key(&table.key) {
                        return Err(SchemaDiffError::Parse {
                            message: format!(
                                "{path}: duplicate table definition for {}",
                                table.name_sql
                            ),
                        });
                    }

                    schema.tables.insert(table.key.clone(), table);
                },
                Statement::CreateSchema { schema_name, .. } => {
                    schema.namespaces.insert(normalize_object_key(&schema_name.to_string()));
                },
                _ => {},
            }
        }
    }

    attach_topic_sources(path, &mut schema, pending_topic_sources)?;
    attach_policies(path, &mut schema, pending_policies)?;
    crate::parser::index::attach_indexes(path, &mut schema, pending_indexes)?;

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TableKind;

    #[test]
    fn shared_table_kind_survives_leading_line_comment() {
        let schema = parse_schema(
            "schema.sql",
            "-- Rooms everyone can create. SELECT is limited to rooms the user belongs \
             to.\nCREATE SHARED TABLE IF NOT EXISTS chat_demo.rooms (\nid TEXT PRIMARY \
             KEY,\ntitle TEXT NOT NULL\n);",
        )
        .expect("parse shared table after comment");

        let table = schema.tables.get("chat_demo.rooms").expect("rooms table");
        assert_eq!(table.kind, Some(TableKind::Shared));
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn stream_table_ttl_survives_leading_line_comment() {
        let schema = parse_schema(
            "schema.sql",
            "-- Live thinking / typing rows.\nCREATE STREAM TABLE IF NOT EXISTS \
             chat_demo.agent_events (\nid BIGINT PRIMARY KEY,\nstage TEXT NOT NULL\n) WITH \
             (TTL_SECONDS = 10);",
        )
        .expect("parse stream table after comment");

        let table = schema.tables.get("chat_demo.agent_events").expect("events table");
        assert_eq!(table.kind, Some(TableKind::Stream));
        assert_eq!(table.options.get("TTL_SECONDS").map(String::as_str), Some("10"));
    }

    #[test]
    fn drop_table_after_leading_comment_does_not_fail_parse() {
        let schema = parse_schema(
            "schema.sql",
            "-- This file is the source of truth for `kalam dev`.\nDROP TABLE IF EXISTS \
             chat_demo.rooms;\nCREATE SHARED TABLE IF NOT EXISTS chat_demo.rooms (\nid TEXT \
             PRIMARY KEY\n);",
        )
        .expect("parse drop plus create after comments");

        let table = schema.tables.get("chat_demo.rooms").expect("rooms table");
        assert_eq!(table.kind, Some(TableKind::Shared));
    }

    #[test]
    fn user_table_kind_survives_leading_block_comment() {
        let schema = parse_schema(
            "schema.sql",
            "/* per-user inbox */\nCREATE USER TABLE chat_demo.inbox (id TEXT PRIMARY KEY);",
        )
        .expect("parse user table after block comment");

        let table = schema.tables.get("chat_demo.inbox").expect("inbox table");
        assert_eq!(table.kind, Some(TableKind::User));
    }
}
