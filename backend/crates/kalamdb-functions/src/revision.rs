//! Loaded module revision metadata.

use std::sync::Arc;

use bytes::Bytes;
use kalamdb_commons::{ArtifactId, FunctionModuleId, FunctionRevisionId, FunctionRuntime};

use crate::limits::ABI_VERSION;

/// Immutable metadata for a module revision the runtime can load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleRevision {
    pub module_id:     FunctionModuleId,
    pub revision_id:   FunctionRevisionId,
    pub artifact_id:   ArtifactId,
    pub runtime:       FunctionRuntime,
    pub abi_version:   u32,
    pub contract_hash: String,
    pub source:        Arc<str>,
    pub wasm:          Bytes,
}

impl ModuleRevision {
    pub fn byte_len(&self) -> usize {
        self.source.len() + self.wasm.len()
    }
    pub fn typescript_fixture(source: impl Into<String>) -> Self {
        let source = source.into();
        let artifact_id = ArtifactId::new("fixture");
        let module_id = FunctionModuleId::new("backend");
        Self {
            revision_id: FunctionRevisionId::from_module_artifact(&module_id, &artifact_id),
            module_id,
            artifact_id,
            runtime: FunctionRuntime::Typescript,
            abi_version: ABI_VERSION,
            contract_hash: "fixture".to_string(),
            source: source.into(),
            wasm: Bytes::new(),
        }
    }
}
