//! Shared tables module - Store types only
//!
//! **Phase 13.6**: Provider moved to crate::utils::SharedTableProvider
//! **Phase 13.7**: Flush logic moved to crate::utils::flush::SharedTableFlushJob
//!
//! This module now contains ONLY:
//! - SharedTableStore (EntityStore-based storage)
//! - SharedTableRow (data structure)
//! - SharedTablePkIndex (primary key index for efficient lookups)
//! - SharedTableIndexedStore (store with PK index support)

pub mod pk_index;
pub mod shared_table_provider;
pub mod shared_table_store;

// Re-export SharedTableRowId from commons for convenience
pub use kalamdb_commons::ids::SharedTableRowId;
pub use pk_index::{create_shared_table_pk_index, SharedTablePkIndex};
pub use shared_table_provider::SharedTableProvider;
pub use shared_table_store::{
    new_indexed_shared_table_store, new_shared_table_store, SharedTableIndexedStore,
    SharedTableRow, SharedTableStore,
};
