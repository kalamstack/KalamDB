//! Upload hashed artifacts and CAS-activate a module revision.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kalamdb_commons::{ArtifactId, FunctionModuleId, FunctionRevisionId, FunctionRuntime};
use kalamdb_filestore::StorageCached;
use kalamdb_system::{
    error::SystemError, ActivateFunctionOutcome, CatalogFunctionArtifact, CatalogFunctionModule,
    CatalogFunctionRevision, CatalogStores,
};

use crate::{
    error::{FunctionsError, Result},
    hash::hash_artifact_bytes,
    limits::ABI_VERSION,
    revision::ModuleRevision,
};

/// Stages artifact bytes in filestore and CAS-swaps the module active pointer.
pub struct FunctionActivation {
    stores: CatalogStores,
}

impl FunctionActivation {
    pub fn new(stores: CatalogStores) -> Self {
        Self { stores }
    }

    pub fn stores(&self) -> &CatalogStores {
        &self.stores
    }

    /// Content-addressed upload. Same bytes yield the same artifact id.
    pub async fn upload(
        &self,
        storage: &StorageCached,
        bytes: &[u8],
        runtime: FunctionRuntime,
    ) -> Result<CatalogFunctionArtifact> {
        let expected = hash_artifact_bytes(bytes);
        let (artifact_id, put) = storage
            .put_function_artifact(Bytes::copy_from_slice(bytes))
            .await
            .map_err(|error| FunctionsError::Storage(error.to_string()))?;
        if artifact_id != expected {
            return Err(FunctionsError::Invalid(format!(
                "artifact hash mismatch: expected {}, got {}",
                expected, artifact_id
            )));
        }
        let artifact = CatalogFunctionArtifact {
            artifact_id,
            size_bytes: put.size as i64,
            runtime,
            created_at: now_ms(),
        };
        Ok(artifact)
    }

    /// Persist artifact + revision rows, then CAS the module pointer.
    ///
    /// Interruption after the row writes and before the pointer swap leaves the
    /// previous revision active.
    pub fn activate(
        &self,
        module_id: FunctionModuleId,
        artifact: CatalogFunctionArtifact,
        contract_hash: impl Into<String>,
        expected_revision_id: Option<&FunctionRevisionId>,
    ) -> Result<ActivateFunctionOutcome> {
        let (module, revision, artifact) =
            Self::prepared_activation(module_id, artifact, contract_hash);
        self.stores
            .activate_function_revision(module, revision, artifact, expected_revision_id)
            .map_err(map_system_error)
    }

    pub fn prepared_activation(
        module_id: FunctionModuleId,
        artifact: CatalogFunctionArtifact,
        contract_hash: impl Into<String>,
    ) -> (CatalogFunctionModule, CatalogFunctionRevision, CatalogFunctionArtifact) {
        let revision_id =
            FunctionRevisionId::from_module_artifact(&module_id, &artifact.artifact_id);
        let revision = CatalogFunctionRevision {
            revision_id:   revision_id.clone(),
            module_id:     module_id.clone(),
            artifact_id:   artifact.artifact_id.clone(),
            contract_hash: contract_hash.into(),
            abi_version:   ABI_VERSION as i32,
            runtime:       artifact.runtime,
            created_at:    now_ms(),
        };
        let module = CatalogFunctionModule {
            module_id,
            runtime: artifact.runtime,
            active_revision_id: Some(revision_id),
            contract_hash: Some(revision.contract_hash.clone()),
            abi_version: ABI_VERSION as i32,
        };
        (module, revision, artifact)
    }

    pub fn active_module(
        &self,
        module_id: &FunctionModuleId,
    ) -> Result<Option<CatalogFunctionModule>> {
        self.stores.get_function_module(module_id).map_err(map_system_error)
    }

    /// Load source for a specific revision (including inactive ones).
    pub async fn load_revision(
        &self,
        storage: &StorageCached,
        revision_id: &FunctionRevisionId,
    ) -> Result<ModuleRevision> {
        let revision = self
            .stores
            .get_function_revision(revision_id)
            .map_err(map_system_error)?
            .ok_or_else(|| {
                FunctionsError::Invalid(format!("function revision not found: {revision_id}"))
            })?;
        let source = storage
            .get_function_artifact(&revision.artifact_id)
            .await
            .map_err(|error| FunctionsError::Storage(error.to_string()))?;
        let (source, wasm) = match revision.runtime {
            FunctionRuntime::Typescript => (
                String::from_utf8(source.data.to_vec()).map_err(|error| {
                    FunctionsError::Invalid(format!("artifact is not utf8: {error}"))
                })?,
                Bytes::new(),
            ),
            FunctionRuntime::Wasm => (String::new(), source.data),
        };
        Ok(ModuleRevision {
            module_id: revision.module_id,
            revision_id: revision.revision_id,
            artifact_id: revision.artifact_id,
            runtime: revision.runtime,
            abi_version: revision.abi_version as u32,
            contract_hash: revision.contract_hash,
            source: source.into(),
            wasm,
        })
    }

    pub async fn load_active(
        &self,
        storage: &StorageCached,
        module_id: &FunctionModuleId,
    ) -> Result<ModuleRevision> {
        let module = self.active_module(module_id)?.ok_or_else(|| {
            FunctionsError::Invalid(format!("function module not found: {module_id}"))
        })?;
        let revision_id = module.active_revision_id.ok_or_else(|| {
            FunctionsError::Invalid(format!("function module {module_id} has no active revision"))
        })?;
        self.load_revision(storage, &revision_id).await
    }

    pub fn rollback(
        &self,
        module_id: FunctionModuleId,
        previous_artifact: ArtifactId,
        contract_hash: impl Into<String>,
        expected_revision_id: Option<&FunctionRevisionId>,
        runtime: FunctionRuntime,
    ) -> Result<ActivateFunctionOutcome> {
        let artifact = self
            .stores
            .get_function_artifact(&previous_artifact)
            .map_err(map_system_error)?
            .unwrap_or(CatalogFunctionArtifact {
                artifact_id: previous_artifact,
                size_bytes: 0,
                runtime,
                created_at: now_ms(),
            });
        self.activate(module_id, artifact, contract_hash, expected_revision_id)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn map_system_error(error: SystemError) -> FunctionsError {
    match error {
        SystemError::Conflict(message) => {
            let (expected, actual) = parse_stale(&message);
            FunctionsError::StaleRevision { expected, actual }
        },
        other => FunctionsError::Storage(other.to_string()),
    }
}

fn parse_stale(message: &str) -> (String, String) {
    // "stale function revision: expected X, actual Y"
    let expected = message
        .split("expected ")
        .nth(1)
        .and_then(|rest| rest.split(',').next())
        .unwrap_or("unknown")
        .trim()
        .to_string();
    let actual = message.split("actual ").nth(1).unwrap_or("unknown").trim().to_string();
    (expected, actual)
}

#[cfg(test)]
mod tests {
    use std::{env, sync::Arc};

    use datafusion_common::ScalarValue;
    use kalamdb_commons::{
        models::ids::StorageId, FunctionModuleId, FunctionRevisionId, FunctionRuntime, RoutineId,
    };
    use kalamdb_filestore::StorageCached;
    use kalamdb_store::test_utils::InMemoryBackend;
    use kalamdb_system::{
        providers::storages::models::StorageType, ActivateFunctionOutcome, CatalogStores, Storage,
    };
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        limits::RuntimeLimits,
        v8_adapter::{V8Session, FIXTURE_SOURCE},
        value::RoutineValue,
    };

    fn test_storage() -> Storage {
        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = env::temp_dir().join(format!("function_activation_test_{unique_id}"));
        std::fs::create_dir_all(&temp_dir).ok();
        let now = now_ms();
        Storage {
            storage_id:             StorageId::from("functions"),
            storage_name:           "functions".to_string(),
            description:            None,
            storage_type:           StorageType::Filesystem,
            base_directory:         temp_dir.to_string_lossy().to_string(),
            credentials:            None,
            config_json:            None,
            shared_tables_template: "{namespace}/{tableName}".to_string(),
            user_tables_template:   "{namespace}/{tableName}/{userId}".to_string(),
            created_at:             now,
            updated_at:             now,
        }
    }

    #[tokio::test]
    #[ntest::timeout(30000)]
    async fn upload_activate_load_rollback_and_interrupt() {
        let storage = StorageCached::with_default_timeouts(test_storage());
        let activation =
            FunctionActivation::new(CatalogStores::new(Arc::new(InMemoryBackend::new())));
        let module_id = FunctionModuleId::new("backend");

        let artifact_v1 = activation
            .upload(&storage, FIXTURE_SOURCE.as_bytes(), FunctionRuntime::Typescript)
            .await
            .unwrap();
        let outcome = activation
            .activate(module_id.clone(), artifact_v1.clone(), "contract-v1", None)
            .unwrap();
        assert_eq!(outcome, ActivateFunctionOutcome::Activated);

        let loaded = activation.load_active(&storage, &module_id).await.unwrap();
        let mut session = V8Session::load(loaded, RuntimeLimits::default()).unwrap();
        let echoed = session
            .invoke(
                &RoutineId::new("echo"),
                &[RoutineValue::new(ScalarValue::Int32(Some(7)))],
                &CancellationToken::new(),
            )
            .unwrap();
        assert_eq!(echoed.value, ScalarValue::Int32(Some(7)));

        let v2_source = "function kalamInvoke(name, args) { return args[0] + 1; }\n";
        let artifact_v2 = activation
            .upload(&storage, v2_source.as_bytes(), FunctionRuntime::Typescript)
            .await
            .unwrap();
        let v1_revision =
            FunctionRevisionId::from_module_artifact(&module_id, &artifact_v1.artifact_id);
        let outcome = activation
            .activate(module_id.clone(), artifact_v2.clone(), "contract-v2", Some(&v1_revision))
            .unwrap();
        assert_eq!(outcome, ActivateFunctionOutcome::Activated);

        let stale = activation.activate(
            module_id.clone(),
            artifact_v1.clone(),
            "contract-v1",
            Some(&v1_revision),
        );
        assert!(matches!(stale, Err(FunctionsError::StaleRevision { .. })));
        let still_v2 = activation.active_module(&module_id).unwrap().unwrap();
        assert_eq!(
            still_v2.active_revision_id,
            Some(FunctionRevisionId::from_module_artifact(&module_id, &artifact_v2.artifact_id))
        );

        // Interruption: stage v1 rows again without swapping if we only upsert revision.
        // Pointer stays on v2 because expected does not match after failed CAS.
        let v2_revision = still_v2.active_revision_id.clone().unwrap();
        activation
            .rollback(
                module_id.clone(),
                artifact_v1.artifact_id.clone(),
                "contract-v1",
                Some(&v2_revision),
                FunctionRuntime::Typescript,
            )
            .unwrap();
        let rolled = activation.active_module(&module_id).unwrap().unwrap();
        assert_eq!(rolled.active_revision_id, Some(v1_revision));

        let old = activation
            .load_revision(
                &storage,
                &FunctionRevisionId::from_module_artifact(&module_id, &artifact_v2.artifact_id),
            )
            .await
            .unwrap();
        assert!(old.source.contains("args[0] + 1"));
    }

    #[tokio::test]
    #[ntest::timeout(30000)]
    async fn same_revision_activate_is_noop() {
        let storage = StorageCached::with_default_timeouts(test_storage());
        let activation =
            FunctionActivation::new(CatalogStores::new(Arc::new(InMemoryBackend::new())));
        let module_id = FunctionModuleId::new("backend");
        let artifact = activation
            .upload(&storage, FIXTURE_SOURCE.as_bytes(), FunctionRuntime::Typescript)
            .await
            .unwrap();
        activation
            .activate(module_id.clone(), artifact.clone(), "contract", None)
            .unwrap();
        let outcome = activation.activate(module_id, artifact, "contract", None).unwrap();
        assert_eq!(outcome, ActivateFunctionOutcome::NoOp);
    }
}
