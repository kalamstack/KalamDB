use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{NamespaceId, RoutineId, RoutineSecurityMode, TypeId, UserId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.routines` row.
#[table(name = "routines", comment = "SQL procedures")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogRoutine {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Schema-qualified routine identity"
    )]
    pub routine_id:       RoutineId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Owning schema"
    )]
    pub namespace_id:     NamespaceId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unqualified routine name"
    )]
    pub name:             String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Routine owner"
    )]
    pub owner:            UserId,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "SECURITY INVOKER or DEFINER"
    )]
    pub security:         RoutineSecurityMode,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Implementation language"
    )]
    #[serde(default)]
    pub language:         Option<String>,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional SQL/source body"
    )]
    #[serde(default)]
    pub body:             Option<String>,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Named return type when not VOID"
    )]
    #[serde(default)]
    pub return_type_id:   Option<TypeId>,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Resolved return type name"
    )]
    #[serde(default)]
    pub return_type_name: Option<String>,
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "True when the return is SETOF / array"
    )]
    pub return_is_array:  bool,
    #[column(
        id = 11,
        ordinal = 11,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Return NOT NULL"
    )]
    pub return_not_null:  bool,
    #[column(
        id = 12,
        ordinal = 12,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional routine comment"
    )]
    #[serde(default)]
    pub comment:          Option<String>,
    #[column(
        id = 13,
        ordinal = 13,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Builtin return KalamDataType when the return is not a named CREATE TYPE"
    )]
    #[serde(default)]
    pub return_data_type: Option<KalamDataType>,
}

impl kalamdb_commons::KSerializable for CatalogRoutine {}
