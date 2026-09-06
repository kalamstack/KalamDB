use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{FunctionModuleId, FunctionRevisionId, FunctionRuntime},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.function_modules` row.
#[table(name = "function_modules", comment = "Function module active revision")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogFunctionModule {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Module identity"
    )]
    pub module_id:          FunctionModuleId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "typescript | wasm"
    )]
    pub runtime:            FunctionRuntime,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Active revision id"
    )]
    #[serde(default)]
    pub active_revision_id: Option<FunctionRevisionId>,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Contract snapshot hash for the active revision"
    )]
    #[serde(default)]
    pub contract_hash:      Option<String>,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Host ABI version"
    )]
    pub abi_version:        i32,
}

impl kalamdb_commons::KSerializable for CatalogFunctionModule {}
