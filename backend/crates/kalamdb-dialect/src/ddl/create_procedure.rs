//! CREATE PROCEDURE / DROP PROCEDURE parsers.

use kalamdb_commons::models::{NamespaceId, RoutineId, RoutineSecurityMode};

use crate::ddl::{
    create_type::{parse_type_reference, split_qualified_ident, take_ident, TypeReference},
    DdlResult,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcedureParameter {
    pub name:     String,
    pub type_ref: TypeReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProcedureStatement {
    pub routine_id:   RoutineId,
    pub namespace_id: NamespaceId,
    pub name:         String,
    pub or_replace:   bool,
    pub parameters:   Vec<ProcedureParameter>,
    pub return_type:  Option<TypeReference>,
    pub language:     Option<String>,
    pub security:     RoutineSecurityMode,
    pub body:         Option<String>,
}

impl CreateProcedureStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        let rest = if upper.starts_with("CREATE OR REPLACE PROCEDURE") {
            &trimmed["CREATE OR REPLACE PROCEDURE".len()..]
        } else if upper.starts_with("CREATE PROCEDURE") {
            &trimmed["CREATE PROCEDURE".len()..]
        } else {
            return Err("Expected CREATE PROCEDURE statement".to_string());
        };
        let or_replace = upper.starts_with("CREATE OR REPLACE PROCEDURE");
        let rest = rest.trim_start();
        let (qual, after) = split_qualified_ident(rest)?;
        let namespace_id = qual.namespace_or(default_namespace);
        let procedure_name = qual.name.to_ascii_lowercase();
        let after = after.trim_start();
        if !after.starts_with('(') {
            return Err("Expected parameter list after procedure name".to_string());
        }
        let close = crate::ddl::create_type::matching_paren(after)
            .ok_or_else(|| "Unterminated procedure parameter list".to_string())?;
        let params_body = &after[1..close];
        let mut parameters = Vec::new();
        if !params_body.trim().is_empty() {
            for part in crate::ddl::create_type::split_top_level(params_body, ',') {
                let part = part.trim();
                let (name, rest) = take_ident(part)?;
                let (mut type_ref, leftover) = parse_type_reference(rest.trim_start())?;
                let leftover_upper = leftover.trim().to_ascii_uppercase();
                if leftover_upper.contains("NOT NULL") {
                    type_ref.not_null = true;
                }
                if leftover_upper.contains("NONEMPTY") {
                    type_ref.nonempty = true;
                }
                if leftover_upper.contains("ROW TYPE") {
                    // Alias marker; type name is still the referenced table/type.
                }
                parameters.push(ProcedureParameter { name, type_ref });
            }
        }

        let mut rest = after[close + 1..].trim_start();
        let mut return_type = None;
        let mut language = None;
        let mut security = RoutineSecurityMode::Invoker;
        let mut body = None;

        loop {
            let rest_upper = rest.to_ascii_uppercase();
            if rest_upper.starts_with("RETURNS") {
                let after_returns = rest["RETURNS".len()..].trim_start();
                let after_returns = strip_row_type_prefix(after_returns);
                let (ty, leftover) = parse_type_reference(after_returns)?;
                return_type = Some(ty);
                rest = leftover.trim_start();
                continue;
            }
            if rest_upper.starts_with("LANGUAGE") {
                let after_lang = rest["LANGUAGE".len()..].trim_start();
                let (lang, leftover) = take_ident(after_lang)?;
                language = Some(lang.to_ascii_uppercase());
                rest = leftover.trim_start();
                continue;
            }
            if rest_upper.starts_with("SECURITY INVOKER") {
                security = RoutineSecurityMode::Invoker;
                rest = rest["SECURITY INVOKER".len()..].trim_start();
                continue;
            }
            if rest_upper.starts_with("SECURITY DEFINER") {
                security = RoutineSecurityMode::Definer;
                rest = rest["SECURITY DEFINER".len()..].trim_start();
                continue;
            }
            if rest_upper.starts_with("AS") {
                body = Some(parse_procedure_body(rest["AS".len()..].trim_start())?);
                break;
            }
            if rest.is_empty() {
                break;
            }
            return Err(format!("Unexpected procedure clause starting at '{rest}'"));
        }

        Ok(Self {
            routine_id: RoutineId::from_parts(Some(&namespace_id), &procedure_name),
            namespace_id,
            name: procedure_name,
            or_replace,
            parameters,
            return_type,
            language,
            security,
            body,
        })
    }
}

fn strip_row_type_prefix(input: &str) -> &str {
    if input.len() >= 8 && input[..8].eq_ignore_ascii_case("ROW TYPE") {
        input[8..].trim_start()
    } else {
        input
    }
}

fn parse_procedure_body(input: &str) -> DdlResult<String> {
    let input = input.trim();
    if input.starts_with("$$") {
        let rest = &input[2..];
        let end = rest
            .find("$$")
            .ok_or_else(|| "Unterminated dollar-quoted procedure body".to_string())?;
        return Ok(rest[..end].to_string());
    }
    if input.starts_with('\'') {
        return crate::ddl::create_type::parse_sql_string(input);
    }
    Err("Procedure body must be dollar-quoted ($$ ... $$) or a string literal".to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropProcedureStatement {
    pub routine_id: RoutineId,
    pub if_exists:  bool,
}

impl DropProcedureStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("DROP PROCEDURE") {
            return Err("Expected DROP PROCEDURE statement".to_string());
        }
        let mut rest = trimmed["DROP PROCEDURE".len()..].trim_start();
        let if_exists = rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("IF EXISTS");
        if if_exists {
            rest = rest["IF EXISTS".len()..].trim_start();
        }
        let (qual, _) = split_qualified_ident(rest)?;
        let namespace_id = qual.namespace_or(default_namespace);
        Ok(Self {
            routine_id: RoutineId::from_parts(Some(&namespace_id), &qual.name),
            if_exists,
        })
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::NamespaceId;

    use super::*;

    #[test]
    fn parse_procedure_signature() {
        let stmt = CreateProcedureStatement::parse(
            "CREATE PROCEDURE chat.get_user(user_id TEXT NOT NULL)
             RETURNS ROW TYPE chat.users
             LANGUAGE SQL
             SECURITY INVOKER
             AS $$ SELECT * FROM chat.users WHERE id = user_id; $$",
            &NamespaceId::new("app"),
        )
        .unwrap();
        assert_eq!(stmt.routine_id.as_str(), "chat.get_user");
        assert_eq!(stmt.security, RoutineSecurityMode::Invoker);
        assert_eq!(stmt.language.as_deref(), Some("SQL"));
        assert!(stmt.body.unwrap().contains("SELECT"));
        assert_eq!(stmt.return_type.unwrap().name, "users");
    }
}
