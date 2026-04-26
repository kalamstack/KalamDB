//! Shared DataFusion source primitives for KalamDB.
//!
//! This crate centralizes the scaffolding that every KalamDB provider family
//! previously reimplemented: scan descriptors, capability reporting, execution
//! nodes, record-batch stream adapters, pruning, and statistics. Consumers
//! (`kalamdb-tables`, `kalamdb-system`, `kalamdb-views`, `kalamdb-vector`,
//! `kalamdb-transactions`) wire storage-specific behavior behind the narrow
//! traits defined here.
//!
//! ## Module boundaries
//!
//! - [`provider`]: scan descriptors, capability matrix, and thin `TableProvider`-adjacent traits.
//! - [`exec`]: shared [`ExecutionPlan`][datafusion::physical_plan::ExecutionPlan] scaffolding built
//!   on the DataFusion 53.x surface.
//! - [`stream`]: [`SendableRecordBatchStream`][datafusion::execution::SendableRecordBatchStream]
//!   adapters that preserve Arrow buffer sharing where possible.
//! - [`pruning`]: filter, projection, limit, and pruning descriptors reused by every source family.
//! - [`stats`]: partition statistics and `PlanProperties` builders.
//!
//! ## Library policy
//!
//! Prefer DataFusion, Arrow, Parquet, and `object_store` primitives. Do not
//! reintroduce custom row-buffer, pruning, or plan-metadata frameworks when
//! the upstream engine APIs already solve the problem.

pub mod exec;
pub mod provider;
pub mod pruning;
pub mod stats;
pub mod stream;
