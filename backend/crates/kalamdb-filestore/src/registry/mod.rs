//! Storage Registry module for filestore
//!
//! Provides centralized storage configuration management with lazy ObjectStore initialization.
//! This is the **single entry point** for all storage operations in KalamDB.
//!
//! # Architecture
//!
//! - `StorageCached` - Unified storage interface with all operations (list, get, put, delete, etc.)
//! - `StorageRegistry` - Maps storage IDs to StorageCached instances
//! - `operations` - Result types for storage operations
//!
//! # Design Principle
//!
//! **Nobody except kalamdb-filestore should depend on object_store crate.**
//! All storage operations go through `StorageCached.operation(...)` methods.

pub mod function_artifacts;
pub mod operations;
pub mod storage_cached;
pub mod storage_registry;

pub use function_artifacts::hash_function_artifact;
pub use operations::*;
pub use storage_cached::StorageCached;
pub use storage_registry::StorageRegistry;
