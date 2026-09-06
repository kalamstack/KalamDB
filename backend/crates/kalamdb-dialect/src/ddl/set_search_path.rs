//! SET search_path session clause (canonicalizes with USE).

use kalamdb_commons::models::NamespaceId;

use crate::ddl::DdlResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetSearchPathStatement {
    pub schemas: Vec<NamespaceId>,
}

impl SetSearchPathStatement {
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("SET SEARCH_PATH") {
            return Err("Expected SET search_path statement".to_string());
        }
        let rest = trimmed["SET SEARCH_PATH".len()..].trim_start();
        let rest = if rest.to_ascii_uppercase().starts_with("TO ") {
            rest[3..].trim_start()
        } else if rest.starts_with('=') {
            rest[1..].trim_start()
        } else {
            return Err("Expected TO or = after SET search_path".to_string());
        };
        let mut schemas = Vec::new();
        for part in rest.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, leftover) = crate::ddl::create_type::take_ident(part)?;
            if !leftover.trim().is_empty() {
                return Err("Unexpected tokens in search_path list".to_string());
            }
            schemas.push(NamespaceId::new(name));
        }
        if schemas.is_empty() {
            return Err("SET search_path requires at least one schema".to_string());
        }
        Ok(Self { schemas })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_search_path_to() {
        let stmt = SetSearchPathStatement::parse("SET search_path TO chat").unwrap();
        assert_eq!(stmt.schemas[0].as_str(), "chat");
    }

    #[test]
    fn parse_search_path_eq() {
        let stmt = SetSearchPathStatement::parse("SET search_path = chat, public").unwrap();
        assert_eq!(stmt.schemas.len(), 2);
    }
}
