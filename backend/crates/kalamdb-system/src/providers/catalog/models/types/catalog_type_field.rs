use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{TypeFieldId, TypeId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.type_fields` row (composite field, implicit row field, or enum label).
#[table(
    name = "type_fields",
    comment = "Fields and enum labels for catalog types"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogTypeField {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "type_id:field_name"
    )]
    pub type_field_id: TypeFieldId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Parent catalog type"
    )]
    pub type_id:       TypeId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Field or enum label name"
    )]
    pub name:          String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Declaration order"
    )]
    pub ordinal:       i32,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Named type reference when the field is not a primitive"
    )]
    #[serde(default)]
    pub field_type_id: Option<TypeId>,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Resolved type name (primitive or named)"
    )]
    pub type_name:     String,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "True when the field is an array"
    )]
    pub is_array:      bool,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "NOT NULL"
    )]
    pub not_null:      bool,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "NONEMPTY for arrays"
    )]
    pub nonempty:      bool,
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Builtin KalamDataType when the field is not a named CREATE TYPE"
    )]
    #[serde(default)]
    pub data_type:     Option<KalamDataType>,
}

impl kalamdb_commons::KSerializable for CatalogTypeField {}

impl CatalogTypeField {
    pub fn new(
        type_id: TypeId,
        name: impl Into<String>,
        ordinal: i32,
        field_type_id: Option<TypeId>,
        data_type: Option<KalamDataType>,
        type_name: impl Into<String>,
        is_array: bool,
        not_null: bool,
        nonempty: bool,
    ) -> Result<Self, String> {
        let name = name.into();
        Ok(Self {
            type_field_id: TypeFieldId::new(&type_id, &name)?,
            type_id,
            name,
            ordinal,
            field_type_id,
            type_name: type_name.into(),
            is_array,
            not_null,
            nonempty,
            data_type,
        })
    }

    pub fn from_column(
        type_id: &TypeId,
        column: &kalamdb_commons::schemas::ColumnDefinition,
    ) -> Result<Self, String> {
        let ordinal = i32::try_from(column.ordinal_position).unwrap_or(i32::MAX);
        Self::new(
            type_id.clone(),
            column.column_name.clone(),
            ordinal,
            None,
            Some(column.data_type),
            column.data_type.sql_name(),
            false,
            !column.is_nullable,
            false,
        )
    }

    pub fn builtin_data_type(&self) -> Option<KalamDataType> {
        self.data_type.or_else(|| {
            if self.field_type_id.is_some() {
                None
            } else {
                KalamDataType::from_sql_name(&self.type_name)
            }
        })
    }
}
