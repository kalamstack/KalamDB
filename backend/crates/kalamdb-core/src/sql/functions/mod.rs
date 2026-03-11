//! SQL functions module
//!
//! This module provides custom SQL functions for DataFusion, including:
//! - ID generation functions: SNOWFLAKE_ID(), UUID_V7(), ULID()
//! - Context functions: CURRENT_USER(), CURRENT_USER_ID(), CURRENT_ROLE()
//!
//! Note: Temporal functions NOW() and CURRENT_TIMESTAMP() are provided by
//! DataFusion's built-in function library and do not need custom implementations.
//!
//! All functions follow the DataFusion ScalarUDFImpl pattern and can be used
//! in DEFAULT clauses, SELECT projections, and WHERE predicates.

pub mod current_role;
pub mod current_user;
pub mod current_user_id;
pub mod snowflake_id;
pub mod ulid;
pub mod uuid_v7;

pub use current_role::CurrentRoleFunction;
pub use current_user::CurrentUserFunction;
pub use current_user_id::CurrentUserIdFunction;
pub use kalamdb_vector::CosineDistanceFunction;
pub use snowflake_id::SnowflakeIdFunction;
pub use ulid::UlidFunction;
pub use uuid_v7::UuidV7Function;
