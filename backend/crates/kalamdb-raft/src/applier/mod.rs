//! Applier traits for state machine persistence
//!
//! These traits define callbacks that state machines invoke when applying
//! committed commands. The actual implementation is provided by kalamdb-core
//! using its provider infrastructure, avoiding circular dependencies.
//!
//! ## Structure
//!
//! - **MetaApplier**: Unified applier for all metadata (namespaces, tables, storages, users, jobs)
//! - **UserDataApplier**: User table data + live queries
//! - **SharedDataApplier**: Shared table data

mod meta_applier;
mod shared_data_applier;
mod user_data_applier;

// Unified Meta applier
pub use meta_applier::{MetaApplier, NoOpMetaApplier};
// Data appliers (split into separate files for better organization)
pub use shared_data_applier::{NoOpSharedDataApplier, SharedDataApplier};
pub use user_data_applier::{NoOpUserDataApplier, UserDataApplier};
