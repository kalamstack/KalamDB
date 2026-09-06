//! In-memory contract snapshot produced by the canonical compiler.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;
use kalamdb_commons::{
    models::{RoutineId, RoutineSecurityMode, TypeId},
    KalamDataType,
};

use crate::ddl::ExecuteGrantee;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractField {
    pub name:      String,
    pub type_name: String,
    pub type_id:   Option<TypeId>,
    pub data_type: Option<KalamDataType>,
    pub is_array:  bool,
    pub not_null:  bool,
    pub nonempty:  bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractTypeKind {
    ImplicitTableRow {
        table_id: String,
        fields:   Vec<ContractField>,
    },
    RowAlias {
        source: TypeId,
    },
    Composite {
        fields: Vec<ContractField>,
    },
    Enum {
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractType {
    pub type_id: TypeId,
    pub schema:  String,
    pub name:    String,
    pub kind:    ContractTypeKind,
    pub arrow:   DataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContractTableKind {
    #[default]
    Unspecified,
    User,
    Shared,
    Stream,
}

impl ContractTableKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::User => "user",
            Self::Shared => "shared",
            Self::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractTable {
    pub table_id:     String,
    pub schema:       String,
    pub name:         String,
    pub kind:         ContractTableKind,
    pub row_type_id:  TypeId,
    pub row_alias_id: Option<TypeId>,
    pub fields:       Vec<ContractField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractRoutine {
    pub routine_id:  RoutineId,
    pub schema:      String,
    pub name:        String,
    pub parameters:  Vec<ContractField>,
    pub return_type: Option<ContractField>,
    pub language:    Option<String>,
    pub security:    RoutineSecurityMode,
    pub body:        Option<String>,
    pub grants:      BTreeSet<ExecuteGrantee>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContractSnapshot {
    pub schemas:  BTreeSet<String>,
    pub tables:   BTreeMap<String, ContractTable>,
    pub types:    BTreeMap<String, ContractType>,
    pub routines: BTreeMap<String, ContractRoutine>,
}
