//! User tables module - Store types only
//!
//! **Phase 13.6**: Provider moved to crate::utils::UserTableProvider
//! **Phase 13.6**: DML handlers deleted (logic in providers)
//! **Phase 13.7**: Flush logic moved to crate::utils::flush::UserTableFlushJob
//!
//! This module now contains ONLY:
//! - UserTableStore (EntityStore-based storage)
//! - UserTableRow (data structure)
//! - UserTablePkIndex (PK secondary index for efficient lookups)
//! - UserTableIndexedStore (IndexedEntityStore with PK index)

pub mod pk_index;
pub mod user_table_provider;
pub mod user_table_store;

// Re-export UserTableRowId and UserTableRow from commons for convenience
pub use kalamdb_commons::{ids::UserTableRowId, models::rows::UserTableRow};
pub use pk_index::{create_user_table_pk_index, UserTablePkIndex};
pub use user_table_provider::UserTableProvider;
pub use user_table_store::{
    new_indexed_user_table_store, new_user_table_store, UserTableIndexedStore, UserTableStore,
};
