use std::{sync::Arc, time::Duration};

use kalamdb_commons::UserId;
use kalamdb_system::{User, UsersTableProvider};
use moka::sync::Cache;

use crate::errors::error::AuthResult;

/// Abstraction over user persistence for authentication flows.
///
/// This allows kalamdb-auth to work with provider-based implementations
/// backed by system table providers without depending on transport crates.
#[async_trait::async_trait]
pub trait UserRepository: Send + Sync {
    async fn get_user_by_id(&self, user_id: &UserId) -> AuthResult<User>;

    /// Update a full user record. Implementations may persist only changed fields.
    async fn update_user(&self, user: &User) -> AuthResult<()>;

    /// Create a new user.
    async fn create_user(&self, user: User) -> AuthResult<()>;
}

const USER_CACHE_TTL_SECS: u64 = 5;
const USER_CACHE_MAX_CAPACITY: u64 = 1000;

pub struct CachedUsersRepo {
    inner: CoreUsersRepo,
    cache: Cache<UserId, User>,
}

impl CachedUsersRepo {
    pub fn new(provider: Arc<UsersTableProvider>) -> Self {
        let cache = Cache::builder()
            .max_capacity(USER_CACHE_MAX_CAPACITY)
            .time_to_live(Duration::from_secs(USER_CACHE_TTL_SECS))
            .build();

        Self {
            inner: CoreUsersRepo::new(provider),
            cache,
        }
    }

    pub fn invalidate_user(&self, user_id: &UserId) {
        self.cache.invalidate(user_id);
    }

    pub fn clear_cache(&self) {
        self.cache.invalidate_all();
    }
}

#[async_trait::async_trait]
impl UserRepository for CachedUsersRepo {
    async fn get_user_by_id(&self, user_id: &UserId) -> AuthResult<User> {
        if let Some(user) = self.cache.get(user_id) {
            return Ok(user);
        }

        let user = self.inner.get_user_by_id(user_id).await?;
        self.cache.insert(user_id.clone(), user.clone());

        Ok(user)
    }

    async fn update_user(&self, user: &User) -> AuthResult<()> {
        self.invalidate_user(&user.user_id);
        self.inner.update_user(user).await
    }

    async fn create_user(&self, user: User) -> AuthResult<()> {
        self.inner.create_user(user).await
    }
}

pub struct CoreUsersRepo {
    provider: Arc<UsersTableProvider>,
}

impl CoreUsersRepo {
    pub fn new(provider: Arc<UsersTableProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait::async_trait]
impl UserRepository for CoreUsersRepo {
    async fn get_user_by_id(&self, user_id: &UserId) -> AuthResult<User> {
        let user_id = user_id.clone();
        let provider = Arc::clone(&self.provider);
        tokio::task::spawn_blocking(move || {
            provider
                .get_user_by_id(&user_id)
                .map_err(|e| crate::AuthError::DatabaseError(e.to_string()))?
                .ok_or_else(|| {
                    crate::AuthError::UserNotFound(format!("User '{}' not found", user_id))
                })
        })
        .await
        .map_err(|e| crate::AuthError::DatabaseError(e.to_string()))?
    }

    async fn update_user(&self, user: &User) -> AuthResult<()> {
        let provider = Arc::clone(&self.provider);
        let user = user.clone();
        tokio::task::spawn_blocking(move || provider.update_user(user))
            .await
            .map_err(|e| crate::AuthError::DatabaseError(e.to_string()))?
            .map_err(|e| crate::AuthError::DatabaseError(e.to_string()))
    }

    async fn create_user(&self, user: User) -> AuthResult<()> {
        let provider = Arc::clone(&self.provider);
        tokio::task::spawn_blocking(move || provider.create_user(user))
            .await
            .map_err(|e| crate::AuthError::DatabaseError(e.to_string()))?
            .map_err(|e| crate::AuthError::DatabaseError(e.to_string()))
    }
}
