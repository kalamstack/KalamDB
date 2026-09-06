use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{ArtifactId, FunctionRuntime},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.function_artifacts` row.
#[table(
    name = "function_artifacts",
    comment = "Content-addressed function artifacts"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogFunctionArtifact {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "SHA-256 of artifact bytes"
    )]
    pub artifact_id: ArtifactId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Artifact size in bytes"
    )]
    pub size_bytes:  i64,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "typescript | wasm"
    )]
    pub runtime:     FunctionRuntime,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "First-seen time (ms)"
    )]
    pub created_at:  i64,
}

impl kalamdb_commons::KSerializable for CatalogFunctionArtifact {}
