//! SQL module for DataFusion integration and query processing.
//!
//! This module provides the SQL execution layer for KalamDB, built on Apache DataFusion.
//!
//! # Architecture
//!
//! - [`SqlExecutor`]: Main entry point for executing SQL statements
//! - [`DataFusionSessionFactory`]: Creates configured DataFusion session contexts
//! - [`plan_cache`]: Query plan caching for improved performance
//! - [`functions`]: Custom SQL functions (e.g., `CURRENT_USER()`)
//!
//! # Handler Pattern
//!
//! SQL statements are routed through the `executor::handlers` module which provides
//! specialized handlers for:
//! - DDL (CREATE/DROP TABLE, ALTER TABLE)
//! - DML (INSERT, UPDATE, DELETE)
//! - Queries (SELECT)
//! - System operations (FLUSH, OPTIMIZE)
//!
//! # Example
//!
//! ```rust,ignore
//! use kalamdb_core::sql::SqlExecutor;
//!
//! let executor = SqlExecutor::new(app_context, handler_registry);
//! let result = executor.execute("SELECT * FROM users", None, None).await?;
//! ```

pub mod context;
pub mod datafusion_session;
pub mod executor;
pub mod functions;
pub mod impersonation;
pub mod plan_cache;
pub mod table_functions;

pub use context::{ExecutionContext, ExecutionMetadata, ScalarValue};
pub use datafusion_session::DataFusionSessionFactory; // KalamSessionState removed in v3 refactor
pub use executor::handlers::ExecutionResult;
pub use executor::SqlExecutor;
pub use functions::{
    CosineDistanceFunction, CurrentRoleFunction, CurrentUserFunction, CurrentUserIdFunction,
};
pub use impersonation::SqlImpersonationService;
pub use table_functions::VectorSearchTableFunction;

// Re-export permissions from kalamdb-session for backward compatibility
pub use kalamdb_session::{PermissionChecker, SessionError as TableAccessError};
