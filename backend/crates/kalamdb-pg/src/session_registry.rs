use dashmap::DashMap;
use std::sync::Arc;

/// Mutable session state shared across PostgreSQL requests that belong to the same backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePgSession {
    session_id: String,
    current_schema: Option<String>,
    transaction_id: Option<String>,
}

impl RemotePgSession {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            current_schema: None,
            transaction_id: None,
        }
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

    pub fn with_current_schema(mut self, current_schema: Option<&str>) -> Self {
        self.current_schema = current_schema
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        self
    }

    pub fn with_transaction_id(mut self, transaction_id: Option<&str>) -> Self {
        self.transaction_id = transaction_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        self
    }
}

/// Concurrent registry for PostgreSQL backend sessions.
#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: Arc<DashMap<String, RemotePgSession>>,
}

impl SessionRegistry {
    /// Open a session if missing, or reuse the current one.
    pub fn open_or_get(&self, session_id: &str) -> RemotePgSession {
        let session_id = session_id.trim().to_string();
        self.sessions
            .entry(session_id.clone())
            .or_insert_with(|| RemotePgSession::new(session_id))
            .clone()
    }

    /// Update schema and transaction metadata for an existing session.
    pub fn update(
        &self,
        session_id: &str,
        current_schema: Option<&str>,
        transaction_id: Option<&str>,
    ) -> Option<RemotePgSession> {
        let mut session = self.sessions.get_mut(session_id)?;
        let updated = session
            .clone()
            .with_current_schema(current_schema)
            .with_transaction_id(transaction_id);
        *session = updated.clone();
        Some(updated)
    }

    /// Return the number of tracked sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Remove a session from the registry.
    pub fn remove(&self, session_id: &str) -> Option<RemotePgSession> {
        self.sessions.remove(session_id).map(|(_, session)| session)
    }
}
