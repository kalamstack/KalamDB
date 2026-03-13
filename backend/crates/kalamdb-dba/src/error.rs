use kalamdb_core::applier::ApplierError;
use kalamdb_core::error::KalamDbError;
use kalamdb_system::SystemError;
use kalamdb_tables::TableError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbaError {
    #[error("kalamdb error: {0}")]
    KalamDb(#[from] KalamDbError),

    #[error("applier error: {0}")]
    Applier(#[from] ApplierError),

    #[error("system error: {0}")]
    System(#[from] SystemError),

    #[error("table error: {0}")]
    Table(#[from] TableError),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("provider mismatch for table {0}")]
    ProviderMismatch(String),
}

pub type Result<T> = std::result::Result<T, DbaError>;
