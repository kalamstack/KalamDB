use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Current lifecycle state of a transaction handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and accepting operations.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been rolled back.
    RolledBack,
}

/// Mutable session state shared across PostgreSQL requests that belong to the same backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePgSession {
    session_id: String,
    current_schema: Option<String>,
    transaction_id: Option<String>,
    transaction_state: Option<TransactionState>,
    /// Whether the current transaction has performed writes.
    transaction_has_writes: bool,
}

impl RemotePgSession {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            current_schema: None,
            transaction_id: None,
            transaction_state: None,
            transaction_has_writes: false,
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

    pub fn transaction_state(&self) -> Option<TransactionState> {
        self.transaction_state
    }

    pub fn transaction_has_writes(&self) -> bool {
        self.transaction_has_writes
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
    /// Monotonic counter for generating unique transaction IDs.
    tx_counter: AtomicU64,
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

    /// Begin a new transaction for the given session. Returns the transaction ID.
    ///
    /// If a transaction is already active on this session, returns an error.
    pub fn begin_transaction(&self, session_id: &str) -> Result<String, String> {
        let mut session = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("session '{}' not found", session_id))?;

        if let Some(TransactionState::Active) = session.transaction_state {
            // Auto-rollback stale transaction left by a crashed/disconnected client.
            // This is a safety net — the FDW xact_callback should normally commit/rollback,
            // but network failures or panics can leave orphaned transactions.
            log::warn!(
                "PG session '{}': auto-rolling back stale transaction '{}' before starting new one",
                session_id,
                session.transaction_id.as_deref().unwrap_or("?")
            );
            session.transaction_id = None;
            session.transaction_state = None;
            session.transaction_has_writes = false;
        }

        let tx_id = format!(
            "tx-{}-{}",
            session_id,
            self.tx_counter.fetch_add(1, Ordering::Relaxed)
        );
        session.transaction_id = Some(tx_id.clone());
        session.transaction_state = Some(TransactionState::Active);
        session.transaction_has_writes = false;
        Ok(tx_id)
    }

    /// Commit the active transaction on the given session.
    ///
    /// Returns the transaction ID that was committed.
    pub fn commit_transaction(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, String> {
        let mut session = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("session '{}' not found", session_id))?;

        match session.transaction_state {
            Some(TransactionState::Active) => {},
            Some(TransactionState::Committed) => {
                return Err("transaction already committed".to_string());
            },
            Some(TransactionState::RolledBack) => {
                return Err("transaction already rolled back".to_string());
            },
            None => {
                return Err("no active transaction".to_string());
            },
        }

        let current_tx = session.transaction_id.as_deref().unwrap_or("");
        if current_tx != transaction_id {
            return Err(format!(
                "transaction ID mismatch: expected '{}', got '{}'",
                current_tx, transaction_id
            ));
        }

        session.transaction_state = Some(TransactionState::Committed);
        // Clear transaction state after commit
        let tx_id = session.transaction_id.take().unwrap_or_default();
        session.transaction_state = None;
        session.transaction_has_writes = false;
        Ok(tx_id)
    }

    /// Rollback the active transaction on the given session.
    ///
    /// Returns the transaction ID that was rolled back. Idempotent for
    /// already-rolled-back transactions.
    pub fn rollback_transaction(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, String> {
        let mut session = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("session '{}' not found", session_id))?;

        match session.transaction_state {
            Some(TransactionState::Active) => {},
            Some(TransactionState::RolledBack) => {
                // Idempotent rollback
                let tx_id = session.transaction_id.take().unwrap_or_default();
                session.transaction_state = None;
                session.transaction_has_writes = false;
                return Ok(tx_id);
            },
            Some(TransactionState::Committed) => {
                return Err("cannot rollback already-committed transaction".to_string());
            },
            None => {
                // No active transaction — idempotent no-op
                return Ok(String::new());
            },
        }

        let current_tx = session.transaction_id.as_deref().unwrap_or("");
        if current_tx != transaction_id {
            return Err(format!(
                "transaction ID mismatch: expected '{}', got '{}'",
                current_tx, transaction_id
            ));
        }

        session.transaction_state = Some(TransactionState::RolledBack);
        let tx_id = session.transaction_id.take().unwrap_or_default();
        session.transaction_state = None;
        session.transaction_has_writes = false;
        Ok(tx_id)
    }

    /// Mark the active transaction as having performed writes.
    pub fn mark_transaction_writes(&self, session_id: &str) {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            if session.transaction_state == Some(TransactionState::Active) {
                session.transaction_has_writes = true;
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_or_get_creates_session() {
        let registry = SessionRegistry::default();
        let session = registry.open_or_get("pg-1");
        assert_eq!(session.session_id(), "pg-1");
        assert!(session.transaction_id().is_none());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn begin_and_commit_transaction() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        let tx_id = registry.begin_transaction("pg-1").unwrap();
        assert!(tx_id.starts_with("tx-pg-1-"));

        let committed = registry.commit_transaction("pg-1", &tx_id).unwrap();
        assert_eq!(committed, tx_id);

        // After commit, can begin a new transaction
        let tx_id2 = registry.begin_transaction("pg-1").unwrap();
        assert_ne!(tx_id, tx_id2);
        registry.commit_transaction("pg-1", &tx_id2).unwrap();
    }

    #[test]
    fn begin_and_rollback_transaction() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        let tx_id = registry.begin_transaction("pg-1").unwrap();
        let rolled_back = registry.rollback_transaction("pg-1", &tx_id).unwrap();
        assert_eq!(rolled_back, tx_id);

        // After rollback, can begin a new transaction
        let tx_id2 = registry.begin_transaction("pg-1").unwrap();
        assert_ne!(tx_id, tx_id2);
    }

    #[test]
    fn stale_transaction_auto_rollback_on_begin() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        // Begin a transaction and "forget" to commit/rollback (simulates client crash)
        let tx_id1 = registry.begin_transaction("pg-1").unwrap();

        // Beginning a new transaction should auto-rollback the stale one
        let tx_id2 = registry.begin_transaction("pg-1").unwrap();
        assert_ne!(tx_id1, tx_id2);

        // The new transaction should be active and committable
        registry.commit_transaction("pg-1", &tx_id2).unwrap();
    }

    #[test]
    fn sequential_transactions_work() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        // Simulate 10 sequential transactions (like 10 sequential SELECTs)
        for _ in 0..10 {
            let tx_id = registry.begin_transaction("pg-1").unwrap();
            registry.commit_transaction("pg-1", &tx_id).unwrap();
        }
    }

    #[test]
    fn commit_wrong_tx_id_fails() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        let _tx_id = registry.begin_transaction("pg-1").unwrap();
        let result = registry.commit_transaction("pg-1", "wrong-tx-id");
        assert!(result.is_err());
    }

    #[test]
    fn begin_on_missing_session_fails() {
        let registry = SessionRegistry::default();
        let result = registry.begin_transaction("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn rollback_idempotent_no_active_tx() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        // Rollback with no active transaction should be idempotent
        let result = registry.rollback_transaction("pg-1", "any-tx-id");
        assert!(result.is_ok());
    }
}
