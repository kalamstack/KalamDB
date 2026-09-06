//! SQL procedure CALL runtime (Wave 3 F7–F9).

mod acl;
mod call_types;
mod convert;
mod dispatcher;
mod executor;
mod host;
mod runtime_state;

pub use call_types::{FunctionCallOrigin, FunctionCallResult, HttpResponseOverrides};
pub use convert::{bind_call_arguments, json_to_routine_value};
pub use dispatcher::{dispatch_once, start_trigger_dispatcher, TriggerDispatcherRuntime};
pub(crate) use executor::{drop_staged_publishes, flush_staged_publishes};
pub use executor::{function_storage, FunctionService};
pub use kalamdb_functions::RoutineValue;
pub use runtime_state::{FunctionRuntimeState, StagedTopicPublish};
