use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{CatalogTypeKind, NamespaceId, TableId, TypeId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.types` row.
#[table(name = "types", comment = "Named, implicit, and alias SQL types")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogType {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Schema-qualified type identity"
    )]
    pub type_id:        TypeId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Owning schema"
    )]
    pub namespace_id:   NamespaceId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unqualified type name"
    )]
    pub name:           String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "implicit_table_row | row_alias | composite | enum"
    )]
    pub kind:           CatalogTypeKind,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Source table for an implicit row type"
    )]
    #[serde(default)]
    pub table_id:       Option<TableId>,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Source type for a row alias"
    )]
    #[serde(default)]
    pub source_type_id: Option<TypeId>,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional type comment"
    )]
    #[serde(default)]
    pub comment:        Option<String>,
}

impl kalamdb_commons::KSerializable for CatalogType {}
