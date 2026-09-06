//! EXECUTE ACL evaluation for SQL procedures.

use kalamdb_commons::{
    models::{RoutineGrantee, RoutineId, UserId},
    Role,
};
use kalamdb_system::{CatalogRoutine, CatalogStores};

use crate::error::KalamDbError;

pub fn require_execute(
    stores: &CatalogStores,
    routine: &CatalogRoutine,
    user_id: &UserId,
    role: Role,
) -> Result<(), KalamDbError> {
    if matches!(role, Role::Dba | Role::System) {
        return Ok(());
    }
    if routine.owner == *user_id {
        return Ok(());
    }
    if matches!(role, Role::Anonymous) {
        return Err(KalamDbError::Unauthorized(
            "anonymous users cannot execute procedures".to_string(),
        ));
    }
    let grants = stores.list_grants(&routine.routine_id).map_err(|error| {
        KalamDbError::CatalogError(format!("failed to load routine grants: {error}"))
    })?;
    let allowed = grants.iter().any(|grant| grantee_matches(&grant.grantee, role));
    if allowed {
        Ok(())
    } else {
        Err(KalamDbError::PermissionDenied(format!(
            "EXECUTE denied on procedure {}",
            routine.routine_id
        )))
    }
}

fn grantee_matches(grantee: &RoutineGrantee, role: Role) -> bool {
    match grantee {
        RoutineGrantee::Public => true,
        RoutineGrantee::User => role == Role::User,
        RoutineGrantee::Service => role == Role::Service,
        RoutineGrantee::Role(name) => role.as_str().eq_ignore_ascii_case(name),
    }
}

pub fn module_id_for(routine_id: &RoutineId) -> kalamdb_commons::FunctionModuleId {
    kalamdb_commons::FunctionModuleId::new(routine_id.as_str())
}
