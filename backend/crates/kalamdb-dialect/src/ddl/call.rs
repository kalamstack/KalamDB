//! CALL procedure parser.

use kalamdb_commons::models::{NamespaceId, RoutineCall, RoutineId};

use crate::ddl::{
    column_default::parse_call_argument_sql, create_type::split_qualified_ident, DdlResult,
};

/// Parsed `CALL schema.name(args...)`. Same `RoutineCall` as column `DEFAULT fn(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallStatement {
    pub call: RoutineCall,
}

impl CallStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        if trimmed.len() < 4 || !trimmed[..4].eq_ignore_ascii_case("CALL") {
            return Err("Expected CALL statement".to_string());
        }
        let rest = trimmed[4..].trim_start();
        let (qual, after) = split_qualified_ident(rest)?;
        let after = after.trim_start();
        if !after.starts_with('(') {
            return Err("Expected argument list after procedure name".to_string());
        }
        let close = crate::ddl::create_type::matching_paren(after)
            .ok_or_else(|| "Unterminated CALL argument list".to_string())?;
        let leftover = after[close + 1..].trim();
        if !leftover.is_empty() {
            return Err(format!("Unexpected tokens after CALL arguments: '{leftover}'"));
        }
        let args_body = &after[1..close];
        let mut arguments = Vec::new();
        if !args_body.trim().is_empty() {
            for part in crate::ddl::create_type::split_top_level(args_body, ',') {
                arguments.push(parse_call_argument_sql(part.trim())?);
            }
        }
        let namespace_id = qual.namespace_or(default_namespace);
        Ok(Self {
            call: RoutineCall::new(
                RoutineId::from_parts(Some(&namespace_id), &qual.name),
                arguments,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{CallArgument, KalamDataType};

    use super::*;

    #[test]
    fn parse_call_positional_and_placeholder() {
        let ns = NamespaceId::new("app");
        let stmt = CallStatement::parse("CALL api.echo('hi', $1, 7, true, NULL)", &ns).unwrap();
        assert_eq!(stmt.call.routine_id.as_str(), "api.echo");
        assert_eq!(stmt.call.routine_id.namespace_id(), Some(NamespaceId::new("api")));
        assert_eq!(
            stmt.call.arguments,
            vec![
                CallArgument::text("hi"),
                CallArgument::Placeholder(1),
                CallArgument::bigint(7),
                CallArgument::boolean(true),
                CallArgument::Null,
            ]
        );
    }

    #[test]
    fn parse_call_uses_default_namespace() {
        let ns = NamespaceId::new("chat");
        let stmt = CallStatement::parse("CALL ping()", &ns).unwrap();
        assert_eq!(stmt.call.routine_id.as_str(), "chat.ping");
        assert!(stmt.call.arguments.is_empty());
    }

    #[test]
    fn parse_call_accepts_catalog_typed_arguments() {
        let ns = NamespaceId::new("app");
        let stmt = CallStatement::parse(
            "CALL api.echo(CAST('{\"ok\":true}' AS JSON), UUID \
             '550e8400-e29b-41d4-a716-446655440000', X'ff00')",
            &ns,
        )
        .unwrap();
        assert_eq!(
            stmt.call.arguments,
            vec![
                CallArgument::json(serde_json::json!({"ok": true})),
                CallArgument::uuid("550e8400-e29b-41d4-a716-446655440000"),
                CallArgument::typed(KalamDataType::Bytes, serde_json::json!([255, 0]),),
            ]
        );
    }
}
