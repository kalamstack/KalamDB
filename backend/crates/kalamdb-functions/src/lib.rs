//! Function runtime ABI, V8 adapter, and revision activation.

#[cfg(feature = "catalog")]
mod activation;
mod convert;
mod deadline;
mod engine;
mod engine_config;
mod error;
mod hash;
mod host;
mod invocation;
mod invocation_metadata;
mod limits;
mod revision;
mod v8_adapter;
mod v8_async;
#[cfg(feature = "wasm-runtime")]
mod wasm_adapter;
pub mod wasm_values;
pub const PROCEDURE_WIT: &str = include_str!("../wit/procedure.wit");
mod value;
mod wrap;

#[cfg(feature = "catalog")]
pub use activation::FunctionActivation;
pub use engine::FunctionEngine;
pub use engine_config::EngineConfig;
pub use error::{FunctionsError, Result};
pub use hash::hash_artifact_bytes;
pub use host::{FunctionHost, HostFuture, InvocationSource};
pub use invocation::{Invocation, InvocationScope};
pub use invocation_metadata::InvocationMetadata;
pub use limits::{RuntimeLimits, ABI_VERSION};
pub use revision::ModuleRevision;
pub use v8_adapter::{V8Session, FIXTURE_SOURCE};
pub use value::RoutineValue;
pub use wrap::wrap_procedure_source;
