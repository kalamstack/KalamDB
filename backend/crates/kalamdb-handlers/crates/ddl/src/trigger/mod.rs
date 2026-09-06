//! CREATE / DROP / ALTER TRIGGER handlers.

mod alter;
mod create;
mod drop;

pub use alter::AlterTriggerHandler;
pub use create::CreateTriggerHandler;
pub use drop::DropTriggerHandler;
