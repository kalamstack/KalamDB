//! Generic secondary index support for entity stores.
//!
//! This module provides reusable index infrastructure that works with any
//! entity type stored via `EntityStore<T>`. Indexes are maintained automatically
//! when entities are created, updated, or deleted.
//!
//! ## Architecture
//!
//! ```text
//! EntityStore<T>              ← Typed entity CRUD (traits.rs)
//!     ↓
//! IndexManager<T>             ← Manages all indexes for entity type (this module)
//!     ↓
//! SecondaryIndex<T,K>         ← Individual index implementation
//!     ↓
//! StorageBackend              ← Storage layer (kalamdb_commons::storage)
//! ```
//!
//! ## Core Concepts
//!
//! - **Index Key Extraction**: Each index defines how to extract a key from an entity
//! - **Unique vs Non-Unique**: Unique indexes enforce one entity per key; non-unique allow multiple
//! - **Automatic Maintenance**: Indexes update when entities are put/deleted via EntityStore
//! - **Partition Isolation**: Each index lives in its own partition (e.g., `idx_users_username`)
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use kalamdb_store::{EntityStore, IndexManager, SecondaryIndex};
//! use kalamdb_commons::storage::StorageBackend;
//! use serde::{Serialize, Deserialize};
//! use std::sync::Arc;
//!
//! #[derive(Serialize, Deserialize, Clone)]
//! struct User {
//!     user_id: String,
//!     username: String,
//!     role: String,
//! }
//!
//! // Define index manager for User entity
//! struct UserIndexManager {
//!     backend: Arc<dyn StorageBackend>,
//!     username_idx: SecondaryIndex<User, String>,
//!     role_idx: SecondaryIndex<User, String>,
//! }
//!
//! impl UserIndexManager {
//!     fn new(backend: Arc<dyn StorageBackend>) -> Self {
//!         Self {
//!             username_idx: SecondaryIndex::unique(
//!                 backend.clone(),
//!                 "idx_users_username",
//!                 |user| user.username.clone(),
//!             ),
//!             role_idx: SecondaryIndex::non_unique(
//!                 backend.clone(),
//!                 "idx_users_role",
//!                 |user| user.role.clone(),
//!             ),
//!             backend,
//!         }
//!     }
//!
//!     // Find user by username (unique index)
//!     fn get_by_username(&self, username: &str) -> Result<Option<String>> {
//!         self.username_idx.get_primary_key(username)
//!     }
//!
//!     // Find all users with a role (non-unique index)
//!     fn get_by_role(&self, role: &str) -> Result<Vec<String>> {
//!         self.role_idx.get_primary_keys(role)
//!     }
//!
//!     // Maintain indexes when entity changes
//!     fn put(&self, user_id: &str, user: &User, old_user: Option<&User>) -> Result<()> {
//!         self.username_idx.put(user_id, user, old_user)?;
//!         self.role_idx.put(user_id, user, old_user)?;
//!         Ok(())
//!     }
//!
//!     fn delete(&self, user_id: &str, user: &User) -> Result<()> {
//!         self.username_idx.delete(user_id, user)?;
//!         self.role_idx.delete(user_id, user)?;
//!         Ok(())
//!     }
//! }
//!
//! // Use with EntityStore
//! store.put("u1", &user)?;
//! index_manager.put("u1", &user, None)?;
//!
//! let user_id = index_manager.get_by_username("alice")?.unwrap();
//! let admin_ids = index_manager.get_by_role("admin")?;
//! ```

pub mod extractor;
pub mod prefix;
pub mod secondary_index;

pub use extractor::{FunctionExtractor, IndexKeyExtractor};
pub use prefix::{PrefixIndex, PrefixIndexedKey, PrefixIndexedValue};
pub use secondary_index::SecondaryIndex;
