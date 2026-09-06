use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{RoutineId, RoutineParameterId, TypeId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.routine_parameters` row.
#[table(name = "routine_parameters", comment = "Procedure arguments")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogRoutineParameter {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "routine_id:ordinal"
    )]
    pub parameter_id: RoutineParameterId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Parent routine"
    )]
    pub routine_id:   RoutineId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Parameter name"
    )]
    pub name:         String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Declaration order"
    )]
    pub ordinal:      i32,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Named type reference when the parameter is not a primitive"
    )]
    #[serde(default)]
    pub type_id:      Option<TypeId>,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Resolved type name"
    )]
    pub type_name:    String,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "True when the parameter is an array"
    )]
    pub is_array:     bool,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "NOT NULL"
    )]
    pub not_null:     bool,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "NONEMPTY for arrays"
    )]
    pub nonempty:     bool,
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Builtin KalamDataType when the parameter is not a named CREATE TYPE"
    )]
    #[serde(default)]
    pub data_type:    Option<KalamDataType>,
}

impl kalamdb_commons::KSerializable for CatalogRoutineParameter {}

impl CatalogRoutineParameter {
    pub fn builtin_data_type(&self) -> Option<KalamDataType> {
        self.data_type.or_else(|| {
            if self.type_id.is_some() {
                None
            } else {
                KalamDataType::from_sql_name(&self.type_name)
            }
        })
    }
}
