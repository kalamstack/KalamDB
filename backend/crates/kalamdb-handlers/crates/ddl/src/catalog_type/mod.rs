//! CREATE / ALTER / DROP TYPE handlers.

mod alter;
mod create;
mod drop;

pub use alter::AlterTypeHandler;
pub use create::{ensure_implicit_row_type, CreateTypeHandler};
pub use drop::DropTypeHandler;
