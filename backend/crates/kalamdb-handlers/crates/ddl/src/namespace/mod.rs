//! Namespace handlers module

pub mod alter;
pub mod create;
pub mod drop;
pub mod show;
pub mod use_namespace;

pub use alter::AlterNamespaceHandler;
pub use create::CreateNamespaceHandler;
pub use drop::DropNamespaceHandler;
pub use show::ShowNamespacesHandler;
pub use use_namespace::UseNamespaceHandler;
