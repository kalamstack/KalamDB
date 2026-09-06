//! Canonical SQL contract compiler: snapshot, Arrow resolution, hashing, and diff.

mod arrow;
mod compile;
mod diff;
mod hash;
mod snapshot;

use std::fmt;

pub use arrow::{is_builtin_type, resolve_arrow_type};
pub use compile::{compile_contract, compile_contract_sql, ContractSource};
pub use diff::{diff_contracts, ContractDiff};
pub use hash::canonical_contract_hash;
pub use snapshot::{
    ContractField, ContractRoutine, ContractSnapshot, ContractTable, ContractTableKind,
    ContractType, ContractTypeKind,
};

/// Error produced while compiling or diffing a SQL contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractError {
    pub message: String,
}

impl ContractError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ContractError {}
