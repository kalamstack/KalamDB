//! Shared EntityStores for contract catalog tables.

use std::sync::Arc;

use kalamdb_commons::{
    models::{
        ArtifactId, FunctionModuleId, FunctionRevisionId, RoutineGrantId, RoutineId,
        RoutineParameterId, TriggerAttemptId, TriggerId, TypeFieldId, TypeId,
    },
    CatalogTypeKind, KSerializable, StorageKey, SystemTable,
};
use kalamdb_store::{entity_store::EntityStore, IndexedEntityStore, StorageBackend};

use super::models::{
    CatalogFunctionArtifact, CatalogFunctionModule, CatalogFunctionRevision, CatalogRoutine,
    CatalogRoutineGrant, CatalogRoutineParameter, CatalogTrigger, CatalogTriggerAttempt,
    CatalogType, CatalogTypeField,
};
use crate::error::SystemError;

type TypesStore = IndexedEntityStore<TypeId, CatalogType>;
type TypeFieldsStore = IndexedEntityStore<TypeFieldId, CatalogTypeField>;
type RoutinesStore = IndexedEntityStore<RoutineId, CatalogRoutine>;
type RoutineParametersStore = IndexedEntityStore<RoutineParameterId, CatalogRoutineParameter>;
type RoutineGrantsStore = IndexedEntityStore<RoutineGrantId, CatalogRoutineGrant>;
type FunctionModulesStore = IndexedEntityStore<FunctionModuleId, CatalogFunctionModule>;
type FunctionRevisionsStore = IndexedEntityStore<FunctionRevisionId, CatalogFunctionRevision>;
type FunctionArtifactsStore = IndexedEntityStore<ArtifactId, CatalogFunctionArtifact>;
type TriggersStore = IndexedEntityStore<TriggerId, CatalogTrigger>;
type TriggerAttemptsStore = IndexedEntityStore<TriggerAttemptId, CatalogTriggerAttempt>;

/// Shared RocksDB-backed stores for types, fields, routines, parameters, grants,
/// and function module revisions.
#[derive(Clone)]
pub struct CatalogStores {
    pub types:              TypesStore,
    pub type_fields:        TypeFieldsStore,
    pub routines:           RoutinesStore,
    pub routine_parameters: RoutineParametersStore,
    pub routine_grants:     RoutineGrantsStore,
    pub function_modules:   FunctionModulesStore,
    pub function_revisions: FunctionRevisionsStore,
    pub function_artifacts: FunctionArtifactsStore,
    pub triggers:           TriggersStore,
    pub trigger_attempts:   TriggerAttemptsStore,
}

impl std::fmt::Debug for CatalogStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogStores").finish_non_exhaustive()
    }
}

/// Result of CAS-activating a function revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivateFunctionOutcome {
    Activated,
    NoOp,
}

impl CatalogStores {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            types:              IndexedEntityStore::new(
                backend.clone(),
                SystemTable::Types.column_family_name().expect("Types is a table"),
                Vec::new(),
            ),
            type_fields:        IndexedEntityStore::new(
                backend.clone(),
                SystemTable::TypeFields.column_family_name().expect("TypeFields is a table"),
                Vec::new(),
            ),
            routines:           IndexedEntityStore::new(
                backend.clone(),
                SystemTable::Routines.column_family_name().expect("Routines is a table"),
                Vec::new(),
            ),
            routine_parameters: IndexedEntityStore::new(
                backend.clone(),
                SystemTable::RoutineParameters
                    .column_family_name()
                    .expect("RoutineParameters is a table"),
                Vec::new(),
            ),
            routine_grants:     IndexedEntityStore::new(
                backend.clone(),
                SystemTable::RoutineGrants
                    .column_family_name()
                    .expect("RoutineGrants is a table"),
                Vec::new(),
            ),
            function_modules:   IndexedEntityStore::new(
                backend.clone(),
                SystemTable::FunctionModules
                    .column_family_name()
                    .expect("FunctionModules is a table"),
                Vec::new(),
            ),
            function_revisions: IndexedEntityStore::new(
                backend.clone(),
                SystemTable::FunctionRevisions
                    .column_family_name()
                    .expect("FunctionRevisions is a table"),
                Vec::new(),
            ),
            function_artifacts: IndexedEntityStore::new(
                backend.clone(),
                SystemTable::FunctionArtifacts
                    .column_family_name()
                    .expect("FunctionArtifacts is a table"),
                Vec::new(),
            ),
            triggers:           IndexedEntityStore::new(
                backend.clone(),
                SystemTable::Triggers.column_family_name().expect("Triggers is a table"),
                Vec::new(),
            ),
            trigger_attempts:   IndexedEntityStore::new(
                backend,
                SystemTable::TriggerAttempts
                    .column_family_name()
                    .expect("TriggerAttempts is a table"),
                Vec::new(),
            ),
        }
    }

    pub fn upsert_type(&self, catalog_type: CatalogType) -> Result<(), SystemError> {
        put_model(&self.types, &catalog_type.type_id, &catalog_type)
    }

    pub fn get_type(&self, type_id: &TypeId) -> Result<Option<CatalogType>, SystemError> {
        get_model(&self.types, type_id)
    }

    pub fn list_types(&self) -> Result<Vec<CatalogType>, SystemError> {
        list_models(&self.types)
    }

    pub fn upsert_type_field(&self, field: CatalogTypeField) -> Result<(), SystemError> {
        put_model(&self.type_fields, &field.type_field_id, &field)
    }

    pub fn list_type_fields(&self, type_id: &TypeId) -> Result<Vec<CatalogTypeField>, SystemError> {
        let all: Vec<CatalogTypeField> = list_models(&self.type_fields)?;
        let mut fields: Vec<CatalogTypeField> =
            all.into_iter().filter(|field| &field.type_id == type_id).collect();
        fields.sort_by_key(|field| field.ordinal);
        Ok(fields)
    }

    pub fn replace_type_fields(
        &self,
        type_id: &TypeId,
        fields: Vec<CatalogTypeField>,
    ) -> Result<(), SystemError> {
        let keys: Vec<TypeFieldId> = self
            .list_type_fields(type_id)?
            .into_iter()
            .map(|field| field.type_field_id)
            .collect();
        if !keys.is_empty() {
            self.type_fields.delete_batch(&keys)?;
        }
        for field in fields {
            self.upsert_type_field(field)?;
        }
        Ok(())
    }

    pub fn upsert_routine(&self, routine: CatalogRoutine) -> Result<(), SystemError> {
        put_model(&self.routines, &routine.routine_id, &routine)
    }

    pub fn get_routine(
        &self,
        routine_id: &RoutineId,
    ) -> Result<Option<CatalogRoutine>, SystemError> {
        get_model(&self.routines, routine_id)
    }

    pub fn list_routines(&self) -> Result<Vec<CatalogRoutine>, SystemError> {
        list_models(&self.routines)
    }

    pub fn upsert_parameter(&self, parameter: CatalogRoutineParameter) -> Result<(), SystemError> {
        put_model(&self.routine_parameters, &parameter.parameter_id, &parameter)
    }

    pub fn replace_parameters(
        &self,
        routine_id: &RoutineId,
        parameters: Vec<CatalogRoutineParameter>,
    ) -> Result<(), SystemError> {
        let keys: Vec<RoutineParameterId> = self
            .list_parameters(routine_id)?
            .into_iter()
            .map(|parameter| parameter.parameter_id)
            .collect();
        if !keys.is_empty() {
            self.routine_parameters.delete_batch(&keys)?;
        }
        for parameter in parameters {
            self.upsert_parameter(parameter)?;
        }
        Ok(())
    }

    pub fn list_parameters(
        &self,
        routine_id: &RoutineId,
    ) -> Result<Vec<CatalogRoutineParameter>, SystemError> {
        let all: Vec<CatalogRoutineParameter> = list_models(&self.routine_parameters)?;
        let mut parameters: Vec<CatalogRoutineParameter> = all
            .into_iter()
            .filter(|parameter| &parameter.routine_id == routine_id)
            .collect();
        parameters.sort_by_key(|parameter| parameter.ordinal);
        Ok(parameters)
    }

    pub fn upsert_grant(&self, grant: CatalogRoutineGrant) -> Result<(), SystemError> {
        put_model(&self.routine_grants, &grant.grant_id, &grant)
    }

    pub fn list_grants(
        &self,
        routine_id: &RoutineId,
    ) -> Result<Vec<CatalogRoutineGrant>, SystemError> {
        let all: Vec<CatalogRoutineGrant> = list_models(&self.routine_grants)?;
        let mut grants: Vec<CatalogRoutineGrant> =
            all.into_iter().filter(|grant| &grant.routine_id == routine_id).collect();
        grants.sort_by(|left, right| left.grant_id.as_str().cmp(right.grant_id.as_str()));
        Ok(grants)
    }

    pub fn delete_grant(&self, grant_id: &RoutineGrantId) -> Result<(), SystemError> {
        self.routine_grants.delete(grant_id)?;
        Ok(())
    }

    pub fn upsert_function_artifact(
        &self,
        artifact: CatalogFunctionArtifact,
    ) -> Result<(), SystemError> {
        put_model(&self.function_artifacts, &artifact.artifact_id, &artifact)
    }

    pub fn get_function_artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<Option<CatalogFunctionArtifact>, SystemError> {
        get_model(&self.function_artifacts, artifact_id)
    }

    pub fn upsert_function_revision(
        &self,
        revision: CatalogFunctionRevision,
    ) -> Result<(), SystemError> {
        put_model(&self.function_revisions, &revision.revision_id, &revision)
    }

    pub fn get_function_revision(
        &self,
        revision_id: &FunctionRevisionId,
    ) -> Result<Option<CatalogFunctionRevision>, SystemError> {
        get_model(&self.function_revisions, revision_id)
    }

    pub fn get_function_module(
        &self,
        module_id: &FunctionModuleId,
    ) -> Result<Option<CatalogFunctionModule>, SystemError> {
        get_model(&self.function_modules, module_id)
    }

    /// Stage artifact + revision rows, then CAS the module active pointer.
    ///
    /// Artifact/revision writes happen before the pointer swap so an interruption
    /// leaves the previous revision active.
    pub fn activate_function_revision(
        &self,
        module: CatalogFunctionModule,
        revision: CatalogFunctionRevision,
        artifact: CatalogFunctionArtifact,
        expected_revision_id: Option<&FunctionRevisionId>,
    ) -> Result<ActivateFunctionOutcome, SystemError> {
        self.upsert_function_artifact(artifact)?;
        self.upsert_function_revision(revision.clone())?;

        let current = self.get_function_module(&module.module_id)?;
        let current_revision = current.as_ref().and_then(|row| row.active_revision_id.clone());
        if current_revision.as_ref() == Some(&revision.revision_id) {
            return Ok(ActivateFunctionOutcome::NoOp);
        }
        if current_revision.as_ref() != expected_revision_id {
            let actual = current_revision
                .map(|id| id.into_string())
                .unwrap_or_else(|| "none".to_string());
            let expected = expected_revision_id
                .map(|id| id.as_str().to_string())
                .unwrap_or_else(|| "none".to_string());
            return Err(SystemError::Conflict(format!(
                "stale function revision: expected {expected}, actual {actual}"
            )));
        }

        put_model(&self.function_modules, &module.module_id, &module)?;
        Ok(ActivateFunctionOutcome::Activated)
    }

    pub fn drop_type(&self, type_id: &TypeId) -> Result<(), SystemError> {
        if self.get_type(type_id)?.is_none() {
            return Err(SystemError::NotFound(format!("type not found: {type_id}")));
        }

        for catalog_type in self.list_types()? {
            if catalog_type.kind == CatalogTypeKind::RowAlias
                && catalog_type.source_type_id.as_ref() == Some(type_id)
            {
                return Err(SystemError::InvalidOperation(format!(
                    "cannot drop type {type_id}: referenced by row alias {}",
                    catalog_type.type_id
                )));
            }
        }

        let fields: Vec<CatalogTypeField> = list_models(&self.type_fields)?;
        for field in fields {
            if field.field_type_id.as_ref() == Some(type_id) && &field.type_id != type_id {
                return Err(SystemError::InvalidOperation(format!(
                    "cannot drop type {type_id}: referenced by {}.{}",
                    field.type_id, field.name
                )));
            }
        }

        for routine in self.list_routines()? {
            if routine.return_type_id.as_ref() == Some(type_id) {
                return Err(SystemError::InvalidOperation(format!(
                    "cannot drop type {type_id}: referenced by routine {}",
                    routine.routine_id
                )));
            }
        }

        let parameters: Vec<CatalogRoutineParameter> = list_models(&self.routine_parameters)?;
        for parameter in parameters {
            if parameter.type_id.as_ref() == Some(type_id) {
                return Err(SystemError::InvalidOperation(format!(
                    "cannot drop type {type_id}: referenced by routine parameter {}.{}",
                    parameter.routine_id, parameter.name
                )));
            }
        }

        let field_keys = self
            .list_type_fields(type_id)?
            .into_iter()
            .map(|field| field.type_field_id)
            .collect::<Vec<_>>();
        if !field_keys.is_empty() {
            self.type_fields.delete_batch(&field_keys)?;
        }
        self.types.delete(type_id)?;
        Ok(())
    }

    pub fn drop_routine(&self, routine_id: &RoutineId) -> Result<(), SystemError> {
        if self.get_routine(routine_id)?.is_none() {
            return Err(SystemError::NotFound(format!("routine not found: {routine_id}")));
        }

        let parameter_keys = self
            .list_parameters(routine_id)?
            .into_iter()
            .map(|parameter| parameter.parameter_id)
            .collect::<Vec<_>>();
        if !parameter_keys.is_empty() {
            self.routine_parameters.delete_batch(&parameter_keys)?;
        }

        let grant_keys = self
            .list_grants(routine_id)?
            .into_iter()
            .map(|grant| grant.grant_id)
            .collect::<Vec<_>>();
        if !grant_keys.is_empty() {
            self.routine_grants.delete_batch(&grant_keys)?;
        }

        self.routines.delete(routine_id)?;
        Ok(())
    }

    pub fn upsert_trigger(&self, trigger: CatalogTrigger) -> Result<(), SystemError> {
        put_model(&self.triggers, &trigger.trigger_id, &trigger)
    }

    pub fn get_trigger(
        &self,
        trigger_id: &TriggerId,
    ) -> Result<Option<CatalogTrigger>, SystemError> {
        get_model(&self.triggers, trigger_id)
    }

    pub fn list_triggers(&self) -> Result<Vec<CatalogTrigger>, SystemError> {
        list_models(&self.triggers)
    }

    pub fn drop_trigger(&self, trigger_id: &TriggerId) -> Result<(), SystemError> {
        if self.get_trigger(trigger_id)?.is_none() {
            return Err(SystemError::NotFound(format!("trigger not found: {trigger_id}")));
        }
        self.triggers.delete(trigger_id)?;
        Ok(())
    }

    pub fn upsert_trigger_attempt(
        &self,
        attempt: CatalogTriggerAttempt,
    ) -> Result<(), SystemError> {
        put_model(&self.trigger_attempts, &attempt.attempt_id, &attempt)
    }

    pub fn get_trigger_attempt(
        &self,
        attempt_id: &TriggerAttemptId,
    ) -> Result<Option<CatalogTriggerAttempt>, SystemError> {
        get_model(&self.trigger_attempts, attempt_id)
    }

    pub fn list_trigger_attempts(&self) -> Result<Vec<CatalogTriggerAttempt>, SystemError> {
        list_models(&self.trigger_attempts)
    }
}

fn put_model<K, T>(store: &IndexedEntityStore<K, T>, key: &K, model: &T) -> Result<(), SystemError>
where
    K: StorageKey,
    T: KSerializable,
{
    store.put(key, model)?;
    Ok(())
}

fn get_model<K, T>(store: &IndexedEntityStore<K, T>, key: &K) -> Result<Option<T>, SystemError>
where
    K: StorageKey,
    T: KSerializable,
{
    Ok(store.get(key)?)
}

fn list_models<K, T>(store: &IndexedEntityStore<K, T>) -> Result<Vec<T>, SystemError>
where
    K: StorageKey,
    T: KSerializable,
{
    Ok(store
        .scan_all_typed(None, None, None)?
        .into_iter()
        .map(|(_, model)| model)
        .collect())
}
