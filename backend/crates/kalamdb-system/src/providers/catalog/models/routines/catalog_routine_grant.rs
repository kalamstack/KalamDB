use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{RoutineGrantId, RoutineGrantee, RoutineId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.routine_grants` row.
#[table(
    name = "routine_grants",
    comment = "EXECUTE ACLs independent of table and RLS policy state"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogRoutineGrant {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "routine_id:grantee"
    )]
    pub grant_id:   RoutineGrantId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Granted routine"
    )]
    pub routine_id: RoutineId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Json),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "EXECUTE grantee"
    )]
    pub grantee:    RoutineGrantee,
}

impl kalamdb_commons::KSerializable for CatalogRoutineGrant {}
