mod function_artifacts_provider;
mod function_modules_provider;
mod function_revisions_provider;
mod models;
mod routine_grants_provider;
mod routine_parameters_provider;
mod routines_provider;
mod scan;
mod stores;
mod trigger_attempts_provider;
mod triggers_provider;
mod type_fields_provider;
mod types_provider;

pub use function_artifacts_provider::FunctionArtifactsTableProvider;
pub use function_modules_provider::FunctionModulesTableProvider;
pub use function_revisions_provider::FunctionRevisionsTableProvider;
pub use models::{
    CatalogFunctionArtifact, CatalogFunctionModule, CatalogFunctionRevision, CatalogRoutine,
    CatalogRoutineGrant, CatalogRoutineParameter, CatalogTrigger, CatalogTriggerAttempt,
    CatalogType, CatalogTypeField,
};
pub use routine_grants_provider::RoutineGrantsTableProvider;
pub use routine_parameters_provider::RoutineParametersTableProvider;
pub use routines_provider::RoutinesTableProvider;
pub use stores::{ActivateFunctionOutcome, CatalogStores};
pub use trigger_attempts_provider::TriggerAttemptsTableProvider;
pub use triggers_provider::TriggersTableProvider;
pub use type_fields_provider::TypeFieldsTableProvider;
pub use types_provider::TypesTableProvider;

#[cfg(test)]
mod catalog_tests;
