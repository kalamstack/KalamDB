//! Shared transaction query types for KalamDB.
//!
//! This crate holds the query-facing transaction seam used by higher-level
//! transaction coordination code and table providers without introducing a
//! `kalamdb-tables -> kalamdb-core` dependency.

pub mod access;
pub mod commit_sequence;
pub mod overlay;
pub mod overlay_exec;
pub mod query_context;
pub mod query_extension;
pub mod staged_mutation;

pub use access::{TransactionAccessError, TransactionAccessValidator};
pub use commit_sequence::CommitSequenceSource;
pub use overlay::{TransactionOverlay, TransactionOverlayEntry};
pub use overlay_exec::TransactionOverlayExec;
pub use query_context::{TransactionMutationSink, TransactionOverlayView, TransactionQueryContext};
pub use query_extension::{extract_transaction_query_context, TransactionQueryExtension};
pub use staged_mutation::{build_insert_staged_mutations, StagedInsertBuildError, StagedMutation};
