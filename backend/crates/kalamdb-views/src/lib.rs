//! KalamDB Virtual Views
//!
//! Provides the VirtualView trait, ViewTableProvider wrapper, and all
//! system virtual view implementations (stats, settings, describe, etc.)
//! for DataFusion integration.
//!
//! This crate is extracted from `kalamdb-core` to enable parallel compilation.
//! The `SystemSchemaProvider` (DataFusion wiring) remains in `kalamdb-core`.
//! View providers now use the shared deferred execution substrate so batch
//! computation happens at execute time instead of inside `TableProvider::scan()`.

pub mod cluster;
pub mod cluster_groups;
pub mod columns_view;
pub mod datatypes;
pub mod describe;
pub mod error;
pub mod live;
pub mod server_logs;
pub mod sessions;
pub mod settings;
pub mod stats;
pub mod tables_view;
pub mod transactions;
pub mod view_base;

pub use cluster::*;
pub use cluster_groups::*;
pub use columns_view::*;
pub use datatypes::*;
pub use describe::*;
pub use error::*;
pub use live::*;
pub use server_logs::*;
pub use sessions::*;
pub use settings::*;
pub use stats::*;
pub use tables_view::*;
pub use transactions::*;
pub use view_base::*;
