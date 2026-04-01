use crate::context::SessionUserContext;
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use kalamdb_commons::models::{NamespaceId, ReadContext, Role, TableId, TableName, UserId};
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_session::{
    can_access_shared_table, can_access_system_table, can_access_user_table,
    can_write_shared_table, can_write_user_table, shared_table_access_level, SessionError,
};

#[inline]
pub fn session_error_to_datafusion(err: SessionError) -> DataFusionError {
    DataFusionError::Plan(err.to_string())
}

pub fn extract_session_context(session: &dyn Session) -> Result<&SessionUserContext, SessionError> {
    session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or(SessionError::InvalidSessionState("Expected SessionState".to_string()))?
        .config()
        .options()
        .extensions
        .get::<SessionUserContext>()
        .ok_or(SessionError::SessionContextNotFound)
}

pub fn extract_user_role(session: &dyn Session) -> Role {
    extract_session_context(session).map(|ctx| ctx.role).unwrap_or(Role::User)
}

pub fn extract_user_id(session: &dyn Session) -> UserId {
    extract_session_context(session)
        .map(|ctx| ctx.user_id.clone())
        .unwrap_or_else(|_| UserId::anonymous())
}

pub fn extract_user_context(session: &dyn Session) -> Result<(&UserId, Role), SessionError> {
    let ctx = extract_session_context(session)?;
    Ok((&ctx.user_id, ctx.role))
}

pub fn extract_full_user_context(
    session: &dyn Session,
) -> Result<(&UserId, Role, ReadContext), SessionError> {
    let ctx = extract_session_context(session)?;
    Ok((&ctx.user_id, ctx.role, ctx.read_context))
}

pub fn check_system_table_access(
    session: &dyn Session,
    table_id: &TableId,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);

    if can_access_system_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            role,
            reason: "System tables require 'dba' or 'system' role.".to_string(),
        })
    }
}

pub fn check_user_table_access(
    session: &dyn Session,
    table_id: &TableId,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);

    if can_access_user_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            role,
            reason: "User tables require user/service or admin role".to_string(),
        })
    }
}

pub fn check_user_table_write_access(
    session: &dyn Session,
    table_id: &TableId,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);

    if can_write_user_table(role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_id.namespace_id().clone(),
            table_name: table_id.table_name().clone(),
            role,
            reason: "User table write denied due to insufficient privileges.".to_string(),
        })
    }
}

pub fn check_shared_table_access(
    session: &dyn Session,
    table_def: &TableDefinition,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);
    let access_level = shared_table_access_level(table_def);

    if can_access_shared_table(access_level, role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_def.namespace_id.clone(),
            table_name: table_def.table_name.clone(),
            role,
            reason: format!("Shared table access denied (access_level={:?})", access_level),
        })
    }
}

pub fn check_shared_table_write_access(
    session: &dyn Session,
    table_def: &TableDefinition,
) -> Result<(), SessionError> {
    let role = extract_user_role(session);
    let access_level = shared_table_access_level(table_def);

    if can_write_shared_table(access_level, role) {
        Ok(())
    } else {
        Err(SessionError::AccessDenied {
            namespace_id: table_def.namespace_id.clone(),
            table_name: table_def.table_name.clone(),
            role,
            reason: format!("Shared table write denied (access_level={:?})", access_level),
        })
    }
}

pub fn check_system_table_access_by_name(
    session: &dyn Session,
    namespace_id: &NamespaceId,
    table_name: &TableName,
) -> Result<(), SessionError> {
    let table_id = TableId::new(namespace_id.clone(), table_name.clone());
    check_system_table_access(session, &table_id)
}

pub struct PermissionChecker;

impl PermissionChecker {
    pub fn check_system_table(session: &dyn Session, table_id: &TableId) -> DataFusionResult<()> {
        check_system_table_access(session, table_id).map_err(session_error_to_datafusion)
    }

    pub fn check_user_table(session: &dyn Session, table_id: &TableId) -> DataFusionResult<()> {
        check_user_table_access(session, table_id).map_err(session_error_to_datafusion)
    }

    pub fn check_shared_table(
        session: &dyn Session,
        table_def: &TableDefinition,
    ) -> DataFusionResult<()> {
        check_shared_table_access(session, table_def).map_err(session_error_to_datafusion)
    }

    pub fn check_shared_table_write(
        session: &dyn Session,
        table_def: &TableDefinition,
    ) -> DataFusionResult<()> {
        check_shared_table_write_access(session, table_def).map_err(session_error_to_datafusion)
    }

    pub fn check_user_table_write(
        session: &dyn Session,
        table_id: &TableId,
    ) -> DataFusionResult<()> {
        check_user_table_write_access(session, table_id).map_err(session_error_to_datafusion)
    }

    #[inline]
    pub fn is_admin(session: &dyn Session) -> bool {
        matches!(extract_user_role(session), Role::System | Role::Dba)
    }

    #[inline]
    pub fn get_role(session: &dyn Session) -> Role {
        extract_user_role(session)
    }

    #[inline]
    pub fn get_user_id(session: &dyn Session) -> UserId {
        extract_user_id(session)
    }
}
