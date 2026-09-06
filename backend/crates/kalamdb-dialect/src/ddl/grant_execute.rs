//! GRANT / REVOKE EXECUTE ON PROCEDURE parsers.

use kalamdb_commons::models::{NamespaceId, RoutineId};

use crate::ddl::{create_type::split_qualified_ident, DdlResult};

/// Principal that can be granted EXECUTE on a procedure.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExecuteGrantee {
    Public,
    User,
    Service,
    Role(String),
}

impl ExecuteGrantee {
    pub fn parse(value: &str) -> DdlResult<Self> {
        let (ident, leftover) = crate::ddl::create_type::take_ident(value)?;
        if !leftover.trim().is_empty() {
            return Err("Unexpected tokens after EXECUTE grantee".to_string());
        }
        Ok(Self::from_ident(&ident))
    }

    pub fn from_ident(ident: &str) -> Self {
        match ident.to_ascii_uppercase().as_str() {
            "PUBLIC" => Self::Public,
            "USER" => Self::User,
            "SERVICE" => Self::Service,
            _ => Self::Role(ident.to_ascii_lowercase()),
        }
    }

    pub fn as_sql(&self) -> String {
        match self {
            Self::Public => "PUBLIC".to_string(),
            Self::User => "user".to_string(),
            Self::Service => "service".to_string(),
            Self::Role(name) => name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantExecuteStatement {
    pub routine_id: RoutineId,
    pub grantee:    ExecuteGrantee,
}

impl GrantExecuteStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        parse_execute_acl(sql, default_namespace, true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeExecuteStatement {
    pub routine_id: RoutineId,
    pub grantee:    ExecuteGrantee,
}

impl RevokeExecuteStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let grant = parse_execute_acl(sql, default_namespace, false)?;
        Ok(Self {
            routine_id: grant.routine_id,
            grantee:    grant.grantee,
        })
    }
}

fn parse_execute_acl(
    sql: &str,
    default_namespace: &NamespaceId,
    is_grant: bool,
) -> DdlResult<GrantExecuteStatement> {
    let trimmed = sql.trim().trim_end_matches(';');
    let expected = if is_grant {
        "GRANT EXECUTE ON PROCEDURE"
    } else {
        "REVOKE EXECUTE ON PROCEDURE"
    };
    if trimmed.len() < expected.len() || !trimmed[..expected.len()].eq_ignore_ascii_case(expected) {
        return Err(format!("Expected {expected}"));
    }
    let rest = trimmed[expected.len()..].trim_start();
    let (qual, after) = split_qualified_ident(rest)?;
    let after = after.trim_start();
    let connector = if is_grant { "TO" } else { "FROM" };
    if after.len() < connector.len() || !after[..connector.len()].eq_ignore_ascii_case(connector) {
        return Err(format!("Expected {connector} after procedure name"));
    }
    let grantee = ExecuteGrantee::parse(after[connector.len()..].trim_start())?;
    let namespace_id = qual.namespace_or(default_namespace);
    Ok(GrantExecuteStatement {
        routine_id: RoutineId::from_parts(Some(&namespace_id), &qual.name),
        grantee,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_grant_and_revoke() {
        let ns = NamespaceId::new("app");
        let grant = GrantExecuteStatement::parse(
            "GRANT EXECUTE ON PROCEDURE api.create_order TO user",
            &ns,
        )
        .unwrap();
        assert_eq!(grant.routine_id.as_str(), "api.create_order");
        assert_eq!(grant.grantee, ExecuteGrantee::User);

        let revoke = RevokeExecuteStatement::parse(
            "REVOKE EXECUTE ON PROCEDURE api.create_order FROM PUBLIC",
            &ns,
        )
        .unwrap();
        assert_eq!(revoke.grantee, ExecuteGrantee::Public);
    }
}
