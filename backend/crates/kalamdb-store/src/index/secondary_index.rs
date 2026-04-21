use super::extractor::{FunctionExtractor, IndexKeyExtractor};
use crate::storage_trait::{Partition, Result, StorageBackend, StorageError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Secondary index implementation for entity stores.
///
/// Provides mapping from index keys to primary keys (entity IDs).
///
/// ## Index Types
/// - **Unique**: One entity per index key (e.g., username → user_id)
/// - **Non-Unique**: Multiple entities per index key (e.g., role → [user_id1, user_id2, ...])
///
/// ## Storage Format
/// - **Unique Index**: `partition:index_key` → `primary_key`
/// - **Non-Unique Index**: `partition:index_key` → `["pk1", "pk2", ...]` (JSON array)
///
/// ## Type Parameters
/// - `T`: Entity type
/// - `K`: Index key type (must implement AsRef<[u8]> for storage)
pub struct SecondaryIndex<T, K>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
    K: Clone + AsRef<[u8]> + Send + Sync,
{
    /// Storage backend
    backend: Arc<dyn StorageBackend>,
    /// Partition for this index (e.g., "idx_users_username")
    partition: Partition,
    /// Whether this is a unique index (one-to-one) or non-unique (one-to-many)
    unique: bool,
    /// Function to extract index key from entity
    key_extractor: Arc<dyn IndexKeyExtractor<T, K>>,
}

impl<T, K> SecondaryIndex<T, K>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    K: Clone + AsRef<[u8]> + Send + Sync + 'static,
{
    /// Creates a new unique secondary index.
    ///
    /// Unique indexes enforce a one-to-one mapping from index key to primary key.
    /// Attempting to insert a duplicate index key will return an error.
    ///
    /// # Arguments
    /// * `backend` - Storage backend
    /// * `partition_name` - Partition name (e.g., "idx_users_username")
    /// * `key_extractor` - Function to extract index key from entity
    ///
    /// # Example
    /// ```rust,ignore
    /// let idx = SecondaryIndex::unique(
    ///     backend,
    ///     "idx_users_username",
    ///     |user: &User| user.username.clone(),
    /// );
    /// ```
    pub fn unique<F>(
        backend: Arc<dyn StorageBackend>,
        partition_name: &str,
        key_extractor: F,
    ) -> Self
    where
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        // Create partition if it doesn't exist
        let partition = Partition::new(partition_name);
        let _ = backend.create_partition(&partition); // Ignore error if already exists

        Self {
            backend,
            partition,
            unique: true,
            key_extractor: Arc::new(FunctionExtractor::new(key_extractor)),
        }
    }

    /// Creates a new non-unique secondary index.
    ///
    /// Non-unique indexes support one-to-many mapping from index key to primary keys.
    /// Multiple entities can share the same index key.
    ///
    /// # Arguments
    /// * `backend` - Storage backend
    /// * `partition_name` - Partition name (e.g., "idx_users_role")
    /// * `key_extractor` - Function to extract index key from entity
    ///
    /// # Example
    /// ```rust,ignore
    /// let idx = SecondaryIndex::non_unique(
    ///     backend,
    ///     "idx_users_role",
    ///     |user: &User| user.role.clone(),
    /// );
    /// ```
    pub fn non_unique<F>(
        backend: Arc<dyn StorageBackend>,
        partition_name: &str,
        key_extractor: F,
    ) -> Self
    where
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        // Create partition if it doesn't exist
        let partition = Partition::new(partition_name);
        let _ = backend.create_partition(&partition); // Ignore error if already exists

        Self {
            backend,
            partition,
            unique: false,
            key_extractor: Arc::new(FunctionExtractor::new(key_extractor)),
        }
    }

    /// Updates the index when an entity is created or modified.
    ///
    /// For unique indexes, checks for duplicates before updating.
    /// For non-unique indexes, adds the primary key to the index key's list.
    ///
    /// # Arguments
    /// * `primary_key` - Primary key of the entity (e.g., "user_123")
    /// * `new_entity` - New entity value
    /// * `old_entity` - Optional old entity value (for updates)
    ///
    /// # Returns
    /// Ok(()) on success, error if unique constraint violated
    ///
    /// # Errors
    /// - `StorageError::UniqueConstraintViolation` if unique index key already exists
    pub fn put(&self, primary_key: &str, new_entity: &T, old_entity: Option<&T>) -> Result<()> {
        let new_key = self.key_extractor.extract(new_entity);

        // If updating, remove old index entry first
        if let Some(old) = old_entity {
            let old_key = self.key_extractor.extract(old);
            // Only delete if key changed
            if old_key.as_ref() != new_key.as_ref() {
                self.delete_index_entry(primary_key, &old_key)?;
            }
        }

        // Add new index entry
        if self.unique {
            // Unique index: check for duplicates
            if let Some(existing_pk) = self.backend.get(&self.partition, new_key.as_ref())? {
                let existing_pk_str = String::from_utf8_lossy(&existing_pk);
                // Allow update of same primary key (not a duplicate)
                if existing_pk_str != primary_key {
                    return Err(StorageError::UniqueConstraintViolation(format!(
                        "Index key already exists in {} for different entity",
                        self.partition
                    )));
                }
            }

            // Store: index_key → primary_key
            self.backend.put(&self.partition, new_key.as_ref(), primary_key.as_bytes())?;
        } else {
            // Non-unique index: append to list
            let mut primary_keys = match self.backend.get(&self.partition, new_key.as_ref())? {
                Some(bytes) => {
                    let existing: Vec<String> = serde_json::from_slice(&bytes)
                        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                    existing
                },
                None => Vec::new(),
            };

            // Add primary key if not already in list
            if !primary_keys.contains(&primary_key.to_string()) {
                primary_keys.push(primary_key.to_string());
            }

            let bytes = serde_json::to_vec(&primary_keys)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            self.backend.put(&self.partition, new_key.as_ref(), &bytes)?;
        }

        Ok(())
    }

    /// Removes an entity from the index.
    ///
    /// For unique indexes, deletes the index entry.
    /// For non-unique indexes, removes the primary key from the list.
    ///
    /// # Arguments
    /// * `primary_key` - Primary key of the entity being deleted
    /// * `entity` - The entity being deleted (to extract index key)
    pub fn delete(&self, primary_key: &str, entity: &T) -> Result<()> {
        let key = self.key_extractor.extract(entity);
        self.delete_index_entry(primary_key, &key)
    }

    /// Internal helper to delete an index entry.
    fn delete_index_entry(&self, primary_key: &str, index_key: &K) -> Result<()> {
        if self.unique {
            // Unique index: delete the mapping
            self.backend.delete(&self.partition, index_key.as_ref())?;
        } else {
            // Non-unique index: remove from list
            if let Some(bytes) = self.backend.get(&self.partition, index_key.as_ref())? {
                let mut primary_keys: Vec<String> = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::SerializationError(e.to_string()))?;

                primary_keys.retain(|pk| pk != primary_key);

                if primary_keys.is_empty() {
                    // No more entities with this index key, delete the entry
                    self.backend.delete(&self.partition, index_key.as_ref())?;
                } else {
                    // Update the list
                    let bytes = serde_json::to_vec(&primary_keys)
                        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                    self.backend.put(&self.partition, index_key.as_ref(), &bytes)?;
                }
            }
        }

        Ok(())
    }

    /// Retrieves the primary key for a unique index lookup.
    ///
    /// Only works for unique indexes. For non-unique indexes, use `get_primary_keys()`.
    ///
    /// # Arguments
    /// * `index_key` - The index key to look up
    ///
    /// # Returns
    /// Some(primary_key) if found, None if not found
    ///
    /// # Panics
    /// Panics if called on a non-unique index (use `get_primary_keys()` instead)
    pub fn get_primary_key<Q>(&self, index_key: &Q) -> Result<Option<String>>
    where
        Q: AsRef<[u8]> + ?Sized,
    {
        assert!(
            self.unique,
            "get_primary_key() only works for unique indexes. Use get_primary_keys() for non-unique indexes."
        );

        match self.backend.get(&self.partition, index_key.as_ref())? {
            Some(bytes) => {
                let pk = String::from_utf8(bytes).map_err(|e| {
                    StorageError::SerializationError(format!("Invalid UTF-8 in primary key: {}", e))
                })?;
                Ok(Some(pk))
            },
            None => Ok(None),
        }
    }

    /// Retrieves all primary keys for a non-unique index lookup.
    ///
    /// Works for both unique and non-unique indexes (unique returns Vec with 0-1 elements).
    ///
    /// # Arguments
    /// * `index_key` - The index key to look up
    ///
    /// # Returns
    /// Vector of primary keys (empty if not found)
    pub fn get_primary_keys<Q>(&self, index_key: &Q) -> Result<Vec<String>>
    where
        Q: AsRef<[u8]> + ?Sized,
    {
        match self.backend.get(&self.partition, index_key.as_ref())? {
            Some(bytes) => {
                if self.unique {
                    // Unique index: single primary key
                    let pk = String::from_utf8(bytes).map_err(|e| {
                        StorageError::SerializationError(format!(
                            "Invalid UTF-8 in primary key: {}",
                            e
                        ))
                    })?;
                    Ok(vec![pk])
                } else {
                    // Non-unique index: array of primary keys
                    let pks: Vec<String> = serde_json::from_slice(&bytes)
                        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                    Ok(pks)
                }
            },
            None => Ok(Vec::new()),
        }
    }

    /// Checks if an index key exists.
    ///
    /// # Arguments
    /// * `index_key` - The index key to check
    ///
    /// # Returns
    /// true if the index key exists, false otherwise
    pub fn exists<Q>(&self, index_key: &Q) -> Result<bool>
    where
        Q: AsRef<[u8]> + ?Sized,
    {
        Ok(self.backend.get(&self.partition, index_key.as_ref())?.is_some())
    }
}

#[cfg(all(test, feature = "rocksdb"))]
mod tests {
    use super::*;
    use crate::{RocksDBBackend, RocksDbInit};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct TestUser {
        user_id: String,
        username: String,
        role: String,
        email: String,
    }

    fn create_test_backend() -> (Arc<dyn StorageBackend>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let init = RocksDbInit::with_defaults(temp_dir.path().to_str().unwrap());
        let db = init.open().unwrap();
        let backend = Arc::new(RocksDBBackend::new(db));
        (backend, temp_dir)
    }

    #[test]
    fn test_unique_index_put_get() {
        let (backend, _temp_dir) = create_test_backend();
        let idx =
            SecondaryIndex::unique(backend.clone(), "idx_test_username", |user: &TestUser| {
                user.username.as_bytes().to_vec()
            });

        let user = TestUser {
            user_id: "u1".to_string(),
            username: "alice".to_string(),
            role: "admin".to_string(),
            email: "alice@example.com".to_string(),
        };

        // Put entry
        idx.put("u1", &user, None).unwrap();

        // Get primary key
        let pk = idx.get_primary_key(b"alice").unwrap();
        assert_eq!(pk, Some("u1".to_string()));

        // Check exists
        assert!(idx.exists(b"alice").unwrap());
        assert!(!idx.exists(b"bob").unwrap());
    }

    #[test]
    fn test_unique_index_duplicate_error() {
        let (backend, _temp_dir) = create_test_backend();
        let idx =
            SecondaryIndex::unique(backend.clone(), "idx_test_username", |user: &TestUser| {
                user.username.as_bytes().to_vec()
            });

        let user1 = TestUser {
            user_id: "u1".to_string(),
            username: "alice".to_string(),
            role: "admin".to_string(),
            email: "alice@example.com".to_string(),
        };

        let user2 = TestUser {
            user_id: "u2".to_string(),
            username: "alice".to_string(), // Same username
            role: "user".to_string(),
            email: "alice2@example.com".to_string(),
        };

        idx.put("u1", &user1, None).unwrap();

        // Attempting to add duplicate should fail
        let result = idx.put("u2", &user2, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::UniqueConstraintViolation(_)));
    }

    #[test]
    fn test_unique_index_update() {
        let (backend, _temp_dir) = create_test_backend();
        let idx =
            SecondaryIndex::unique(backend.clone(), "idx_test_username", |user: &TestUser| {
                user.username.as_bytes().to_vec()
            });

        let user_old = TestUser {
            user_id: "u1".to_string(),
            username: "alice".to_string(),
            role: "user".to_string(),
            email: "alice@example.com".to_string(),
        };

        let user_new = TestUser {
            user_id: "u1".to_string(),
            username: "alice_updated".to_string(), // Changed username
            role: "admin".to_string(),
            email: "alice@example.com".to_string(),
        };

        // Insert original
        idx.put("u1", &user_old, None).unwrap();
        assert!(idx.exists(b"alice").unwrap());

        // Update with new username
        idx.put("u1", &user_new, Some(&user_old)).unwrap();

        // Old username should be gone
        assert!(!idx.exists(b"alice").unwrap());
        // New username should exist
        assert!(idx.exists(b"alice_updated").unwrap());
        assert_eq!(idx.get_primary_key(b"alice_updated").unwrap(), Some("u1".to_string()));
    }

    #[test]
    fn test_unique_index_delete() {
        let (backend, _temp_dir) = create_test_backend();
        let idx =
            SecondaryIndex::unique(backend.clone(), "idx_test_username", |user: &TestUser| {
                user.username.as_bytes().to_vec()
            });

        let user = TestUser {
            user_id: "u1".to_string(),
            username: "alice".to_string(),
            role: "admin".to_string(),
            email: "alice@example.com".to_string(),
        };

        idx.put("u1", &user, None).unwrap();
        assert!(idx.exists(b"alice").unwrap());

        // Delete
        idx.delete("u1", &user).unwrap();
        assert!(!idx.exists(b"alice").unwrap());
        assert_eq!(idx.get_primary_key(b"alice").unwrap(), None);
    }

    #[test]
    fn test_non_unique_index_multiple_entries() {
        let (backend, _temp_dir) = create_test_backend();
        let idx =
            SecondaryIndex::non_unique(backend.clone(), "idx_test_role", |user: &TestUser| {
                user.role.as_bytes().to_vec()
            });

        let users = vec![
            TestUser {
                user_id: "u1".to_string(),
                username: "alice".to_string(),
                role: "admin".to_string(),
                email: "alice@example.com".to_string(),
            },
            TestUser {
                user_id: "u2".to_string(),
                username: "bob".to_string(),
                role: "admin".to_string(),
                email: "bob@example.com".to_string(),
            },
            TestUser {
                user_id: "u3".to_string(),
                username: "charlie".to_string(),
                role: "user".to_string(),
                email: "charlie@example.com".to_string(),
            },
        ];

        // Add all users
        for user in &users {
            idx.put(&user.user_id, user, None).unwrap();
        }

        // Get all admins
        let admin_pks = idx.get_primary_keys(b"admin").unwrap();
        assert_eq!(admin_pks.len(), 2);
        assert!(admin_pks.contains(&"u1".to_string()));
        assert!(admin_pks.contains(&"u2".to_string()));

        // Get all users with "user" role
        let user_pks = idx.get_primary_keys(b"user").unwrap();
        assert_eq!(user_pks.len(), 1);
        assert_eq!(user_pks[0], "u3");
    }

    #[test]
    fn test_non_unique_index_delete() {
        let (backend, _temp_dir) = create_test_backend();
        let idx =
            SecondaryIndex::non_unique(backend.clone(), "idx_test_role", |user: &TestUser| {
                user.role.as_bytes().to_vec()
            });

        let user1 = TestUser {
            user_id: "u1".to_string(),
            username: "alice".to_string(),
            role: "admin".to_string(),
            email: "alice@example.com".to_string(),
        };

        let user2 = TestUser {
            user_id: "u2".to_string(),
            username: "bob".to_string(),
            role: "admin".to_string(),
            email: "bob@example.com".to_string(),
        };

        idx.put("u1", &user1, None).unwrap();
        idx.put("u2", &user2, None).unwrap();

        let pks = idx.get_primary_keys(b"admin").unwrap();
        assert_eq!(pks.len(), 2);

        // Delete one admin
        idx.delete("u1", &user1).unwrap();

        let pks = idx.get_primary_keys(b"admin").unwrap();
        assert_eq!(pks.len(), 1);
        assert_eq!(pks[0], "u2");

        // Delete last admin
        idx.delete("u2", &user2).unwrap();

        // Index entry should be completely removed
        let pks = idx.get_primary_keys(b"admin").unwrap();
        assert_eq!(pks.len(), 0);
        assert!(!idx.exists(b"admin").unwrap());
    }
}
