//! Raft Applier Implementations
//!
//! These implementations connect the Raft state machines to the CommandExecutorImpl,
//! enabling replicated state across all nodes via dependency inversion.
//!
//! The traits are defined in kalamdb-raft:
//! - `MetaApplier`: For namespace, table, storage, user, job operations
//! - `UserDataApplier`: For user table data operations
//! - `SharedDataApplier`: For shared table data operations

mod provider_meta_applier;
mod provider_shared_data_applier;
mod provider_user_data_applier;

pub use provider_meta_applier::ProviderMetaApplier;
pub use provider_shared_data_applier::ProviderSharedDataApplier;
pub use provider_user_data_applier::ProviderUserDataApplier;
