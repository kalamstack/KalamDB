use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{ArtifactId, FunctionModuleId, FunctionRevisionId, FunctionRuntime},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.function_revisions` row.
#[table(
    name = "function_revisions",
    comment = "Immutable function module revisions"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogFunctionRevision {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "module:artifact"
    )]
    pub revision_id:   FunctionRevisionId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Parent module"
    )]
    pub module_id:     FunctionModuleId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Content-addressed artifact"
    )]
    pub artifact_id:   ArtifactId,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Contract snapshot hash"
    )]
    pub contract_hash: String,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Host ABI version"
    )]
    pub abi_version:   i32,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "typescript | wasm"
    )]
    pub runtime:       FunctionRuntime,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Revision create time (ms)"
    )]
    pub created_at:    i64,
}

impl kalamdb_commons::KSerializable for CatalogFunctionRevision {}
