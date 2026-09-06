//! PostgreSQL-style CREATE SCHEMA (alias of CREATE NAMESPACE).

use kalamdb_commons::models::NamespaceId;

use crate::ddl::DdlResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchemaStatement {
    pub name:          NamespaceId,
    pub if_not_exists: bool,
}

impl CreateSchemaStatement {
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("CREATE SCHEMA") {
            return Err("Expected CREATE SCHEMA statement".to_string());
        }
        let mut rest = trimmed["CREATE SCHEMA".len()..].trim_start();
        let if_not_exists = rest.len() >= 13 && rest[..13].eq_ignore_ascii_case("IF NOT EXISTS");
        if if_not_exists {
            rest = rest["IF NOT EXISTS".len()..].trim_start();
        }
        let (name, leftover) = crate::ddl::create_type::take_ident(rest)?;
        if !leftover.trim().is_empty() {
            return Err("Unexpected tokens after CREATE SCHEMA name".to_string());
        }
        Ok(Self {
            name: NamespaceId::new(name),
            if_not_exists,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_schema() {
        let stmt = CreateSchemaStatement::parse("CREATE SCHEMA IF NOT EXISTS chat").unwrap();
        assert_eq!(stmt.name.as_str(), "chat");
        assert!(stmt.if_not_exists);
    }
}
