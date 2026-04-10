use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::UserId;
use serde::{Deserialize, Serialize};

/// Remote session state shared between the PostgreSQL backend connection and KalamDB.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteSessionContext {
    session_id: String,
    current_schema: Option<String>,
    transaction_id: Option<String>,
}

impl RemoteSessionContext {
    /// Create a validated remote session context.
    pub fn new(
        session_id: impl Into<String>,
        current_schema: Option<impl Into<String>>,
        transaction_id: Option<impl Into<String>>,
    ) -> Result<Self, KalamPgError> {
        let session_id = session_id.into().trim().to_string();
        if session_id.is_empty() {
            return Err(KalamPgError::Validation(
                "remote session_id must not be empty".to_string(),
            ));
        }

        let current_schema = match current_schema.map(Into::into) {
            Some(value) => {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    return Err(KalamPgError::Validation(
                        "remote current_schema must not be empty".to_string(),
                    ));
                }
                Some(trimmed)
            },
            None => None,
        };

        let transaction_id = match transaction_id.map(Into::into) {
            Some(value) => {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    return Err(KalamPgError::Validation(
                        "remote transaction_id must not be empty".to_string(),
                    ));
                }
                Some(trimmed)
            },
            None => None,
        };

        Ok(Self {
            session_id,
            current_schema,
            transaction_id,
        })
    }

    /// Validate a remote session context.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        if self.session_id.trim().is_empty() {
            return Err(KalamPgError::Validation(
                "remote session_id must not be empty".to_string(),
            ));
        }
        if let Some(current_schema) = &self.current_schema {
            if current_schema.trim().is_empty() {
                return Err(KalamPgError::Validation(
                    "remote current_schema must not be empty".to_string(),
                ));
            }
        }
        if let Some(transaction_id) = &self.transaction_id {
            if transaction_id.trim().is_empty() {
                return Err(KalamPgError::Validation(
                    "remote transaction_id must not be empty".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub fn current_schema(&self) -> Option<&str> {
        self.current_schema.as_deref()
    }

    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }
}

/// Session and tenant context extracted by the FDW before execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantContext {
    explicit_user_id: Option<UserId>,
    session_user_id: Option<UserId>,
}

impl TenantContext {
    /// Create an anonymous tenant context.
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// Create a tenant context with the same explicit and session user identity.
    pub fn with_user_id(user_id: UserId) -> Self {
        Self {
            explicit_user_id: Some(user_id.clone()),
            session_user_id: Some(user_id),
        }
    }

    /// Create a tenant context with independent explicit and session user identities.
    pub fn new(explicit_user_id: Option<UserId>, session_user_id: Option<UserId>) -> Self {
        Self {
            explicit_user_id,
            session_user_id,
        }
    }

    /// Returns the explicit `_userid` supplied by the query when present.
    pub fn explicit_user_id(&self) -> Option<&UserId> {
        self.explicit_user_id.as_ref()
    }

    /// Returns the user id supplied by the PostgreSQL session when present.
    pub fn session_user_id(&self) -> Option<&UserId> {
        self.session_user_id.as_ref()
    }

    /// Returns the effective user id after combining explicit and session identity.
    pub fn effective_user_id(&self) -> Option<&UserId> {
        self.explicit_user_id.as_ref().or(self.session_user_id.as_ref())
    }

    /// Validate the tenant context.
    ///
    /// When both explicit and session user ids are present, the explicit value
    /// takes precedence (override semantics). No conflict error is raised.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        Ok(())
    }
}
