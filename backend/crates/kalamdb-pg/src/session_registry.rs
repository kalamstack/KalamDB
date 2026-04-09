use dashmap::DashMap;
use kalamdb_commons::models::TransactionState;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn normalize_optional(value: Option<&str>) -> Option<String> {
    value.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned)
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
    opened_at_ms: i64,
    last_seen_at_ms: i64,
    client_addr: Option<String>,
    last_method: Option<String>,
}

/// Live transaction state resolved from the core transaction coordinator for a pg session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePgTransaction {
    session_id: String,
    transaction_id: String,
    transaction_state: TransactionState,
    transaction_has_writes: bool,
}

impl LivePgTransaction {
    pub fn new(
        session_id: impl Into<String>,
        transaction_id: impl Into<String>,
        transaction_state: TransactionState,
        transaction_has_writes: bool,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            transaction_id: transaction_id.into(),
            transaction_state,
            transaction_has_writes,
        }
    }

    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub fn transaction_id(&self) -> &str {
        self.transaction_id.as_str()
    }

    pub fn transaction_state(&self) -> TransactionState {
        self.transaction_state
    }

    pub fn transaction_has_writes(&self) -> bool {
        self.transaction_has_writes
    }
}

impl RemotePgSession {
    pub fn new(session_id: impl Into<String>) -> Self {
        let now_ms = current_timestamp_ms();
        Self {
            session_id: session_id.into(),
            current_schema: None,
            transaction_id: None,
            transaction_state: None,
            transaction_has_writes: false,
            opened_at_ms: now_ms,
            last_seen_at_ms: now_ms,
            client_addr: None,
            last_method: None,
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

    pub fn opened_at_ms(&self) -> i64 {
        self.opened_at_ms
    }

    pub fn last_seen_at_ms(&self) -> i64 {
        self.last_seen_at_ms
    }

    pub fn client_addr(&self) -> Option<&str> {
        self.client_addr.as_deref()
    }

    pub fn last_method(&self) -> Option<&str> {
        self.last_method.as_deref()
    }

    pub fn with_current_schema(mut self, current_schema: Option<&str>) -> Self {
        self.current_schema = normalize_optional(current_schema);
        self
    }

    pub fn with_transaction_id(mut self, transaction_id: Option<&str>) -> Self {
        self.transaction_id = normalize_optional(transaction_id);
        self
    }

    fn with_live_transaction(mut self, live_transaction: Option<&LivePgTransaction>) -> Self {
        self.transaction_id = live_transaction.map(|transaction| transaction.transaction_id().to_owned());
        self.transaction_state = live_transaction.map(LivePgTransaction::transaction_state);
        self.transaction_has_writes = live_transaction
            .map(LivePgTransaction::transaction_has_writes)
            .unwrap_or(false);
        self
    }

    fn record_activity(
        &mut self,
        current_schema: Option<&str>,
        client_addr: Option<&str>,
        last_method: Option<&str>,
        touched_at_ms: i64,
    ) {
        if let Some(current_schema) = normalize_optional(current_schema) {
            self.current_schema = Some(current_schema);
        }

        if let Some(client_addr) = normalize_optional(client_addr) {
            self.client_addr = Some(client_addr);
        }

        if let Some(last_method) = normalize_optional(last_method) {
            self.last_method = Some(last_method);
        }

        self.last_seen_at_ms = touched_at_ms;
    }
}

fn compare_sessions_for_observability(
    left: &RemotePgSession,
    right: &RemotePgSession,
) -> std::cmp::Ordering {
    right
        .last_seen_at_ms
        .cmp(&left.last_seen_at_ms)
        .then_with(|| left.session_id.cmp(&right.session_id))
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

    /// Open a session if missing and record activity metadata.
    pub fn open_or_get_with_context(
        &self,
        session_id: &str,
        current_schema: Option<&str>,
        client_addr: Option<&str>,
        last_method: Option<&str>,
    ) -> RemotePgSession {
        let session_id = session_id.trim().to_string();
        let now_ms = current_timestamp_ms();

        let mut session = self
            .sessions
            .entry(session_id.clone())
            .or_insert_with(|| RemotePgSession::new(session_id));
        session.record_activity(current_schema, client_addr, last_method, now_ms);
        session.clone()
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
        session.last_seen_at_ms = current_timestamp_ms();
        Some(updated)
    }

    /// Begin a new transaction for the given session. Returns the transaction ID.
    ///
    /// If a transaction is already active on this session, returns an error.
    pub fn begin_transaction(&self, session_id: &str) -> Result<String, String> {
        let transaction_id = Uuid::now_v7().to_string();
        self.begin_transaction_with_id(session_id, transaction_id.as_str())
    }

    /// Begin a new transaction for the given session using an externally supplied ID.
    pub fn begin_transaction_with_id(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, String> {
        let mut session = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("session '{}' not found", session_id))?;

        if session.transaction_state.map(|state| state.is_open()).unwrap_or(false) {
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

        let tx_id = if transaction_id.trim().is_empty() {
            format!("{}-{}", Uuid::now_v7(), self.tx_counter.fetch_add(1, Ordering::Relaxed))
        } else {
            transaction_id.trim().to_string()
        };
        session.transaction_id = Some(tx_id.clone());
        session.transaction_state = Some(TransactionState::OpenRead);
        session.transaction_has_writes = false;
        session.last_seen_at_ms = current_timestamp_ms();
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
            Some(TransactionState::OpenRead | TransactionState::OpenWrite) => {},
            Some(TransactionState::Committed) => {
                return Err("transaction already committed".to_string());
            },
            Some(TransactionState::RolledBack) => {
                return Err("transaction already rolled back".to_string());
            },
            Some(TransactionState::Committing) => {
                return Err("transaction is already committing".to_string());
            },
            Some(TransactionState::RollingBack) => {
                return Err("transaction is already rolling back".to_string());
            },
            Some(TransactionState::TimedOut) => {
                return Err("transaction timed out".to_string());
            },
            Some(TransactionState::Aborted) => {
                return Err("transaction aborted".to_string());
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
        session.last_seen_at_ms = current_timestamp_ms();
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
            Some(TransactionState::OpenRead | TransactionState::OpenWrite) => {},
            Some(TransactionState::RolledBack) => {
                // Idempotent rollback
                let tx_id = session.transaction_id.take().unwrap_or_default();
                session.transaction_state = None;
                session.transaction_has_writes = false;
                session.last_seen_at_ms = current_timestamp_ms();
                return Ok(tx_id);
            },
            Some(TransactionState::Committed) => {
                return Err("cannot rollback already-committed transaction".to_string());
            },
            Some(TransactionState::Committing) => {
                return Err("cannot rollback transaction while it is committing".to_string());
            },
            Some(TransactionState::RollingBack) => {
                let tx_id = session.transaction_id.take().unwrap_or_default();
                session.transaction_state = None;
                session.transaction_has_writes = false;
                session.last_seen_at_ms = current_timestamp_ms();
                return Ok(tx_id);
            },
            Some(TransactionState::TimedOut | TransactionState::Aborted) => {
                let tx_id = session.transaction_id.take().unwrap_or_default();
                session.transaction_state = None;
                session.transaction_has_writes = false;
                session.last_seen_at_ms = current_timestamp_ms();
                return Ok(tx_id);
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
        session.last_seen_at_ms = current_timestamp_ms();
        Ok(tx_id)
    }

    /// Mark the active transaction as having performed writes.
    pub fn mark_transaction_writes(&self, session_id: &str) {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            if session.transaction_state == Some(TransactionState::OpenRead) {
                session.transaction_state = Some(TransactionState::OpenWrite);
            }
            if session.transaction_state.map(|state| state.is_open()).unwrap_or(false) {
                session.transaction_has_writes = true;
            }
            session.last_seen_at_ms = current_timestamp_ms();
        }
    }

    /// Return the number of tracked sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Return a point-in-time snapshot of all tracked sessions.
    pub fn snapshot(&self) -> Vec<RemotePgSession> {
        self.sessions.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Return a point-in-time snapshot of tracked sessions with transaction state
    /// reconciled against the live coordinator view for pg-owned transactions.
    pub fn snapshot_with_live_transactions<I>(
        &self,
        active_transactions: I,
    ) -> Vec<RemotePgSession>
    where
        I: IntoIterator<Item = LivePgTransaction>,
    {
        let active_transactions = active_transactions
            .into_iter()
            .map(|transaction| (transaction.session_id().to_owned(), transaction))
            .collect::<HashMap<_, _>>();

        let mut snapshot = self
            .snapshot()
            .into_iter()
            .map(|session| {
                let session_id = session.session_id().to_owned();
                session.with_live_transaction(active_transactions.get(session_id.as_str()))
            })
            .collect::<Vec<_>>();
        snapshot.sort_by(compare_sessions_for_observability);
        snapshot
    }

    /// Get a point-in-time snapshot of a tracked session.
    pub fn get(&self, session_id: &str) -> Option<RemotePgSession> {
        self.sessions.get(session_id).map(|session| session.clone())
    }

    /// Clear transaction metadata for an existing session without removing it.
    pub fn clear_transaction_state_if_matches(
        &self,
        session_id: &str,
        expected_transaction_id: Option<&str>,
    ) -> Option<RemotePgSession> {
        let mut session = self.sessions.get_mut(session_id)?;

        if let Some(expected_transaction_id) = expected_transaction_id {
            if session.transaction_id.as_deref() != Some(expected_transaction_id) {
                return Some(session.clone());
            }
        }

        session.transaction_id = None;
        session.transaction_state = None;
        session.transaction_has_writes = false;
        session.last_seen_at_ms = current_timestamp_ms();
        Some(session.clone())
    }

    /// Close a session and clear any tracked transaction metadata before removal.
    pub fn close_session(&self, session_id: &str) -> Option<RemotePgSession> {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.transaction_id = None;
            session.transaction_state = None;
            session.transaction_has_writes = false;
            session.last_seen_at_ms = current_timestamp_ms();
        }

        self.sessions.remove(session_id).map(|(_, session)| session)
    }

    /// Remove a session from the registry.
    pub fn remove(&self, session_id: &str) -> Option<RemotePgSession> {
        self.sessions.remove(session_id).map(|(_, session)| session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn open_or_get_creates_session() {
        let registry = SessionRegistry::default();
        let session = registry.open_or_get("pg-1");
        assert_eq!(session.session_id(), "pg-1");
        assert!(session.transaction_id().is_none());
        assert!(session.opened_at_ms() > 0);
        assert_eq!(session.last_method(), None);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn open_or_get_with_context_records_activity_metadata() {
        let registry = SessionRegistry::default();

        let session = registry.open_or_get_with_context(
            "pg-42",
            Some("tenant_a"),
            Some("127.0.0.1:54321"),
            Some("OpenSession"),
        );

        assert_eq!(session.current_schema(), Some("tenant_a"));
        assert_eq!(session.client_addr(), Some("127.0.0.1:54321"));
        assert_eq!(session.last_method(), Some("OpenSession"));
        assert!(session.last_seen_at_ms() >= session.opened_at_ms());
    }

    #[test]
    fn snapshot_returns_all_sessions() {
        let registry = SessionRegistry::default();
        registry.open_or_get_with_context("pg-1", None, None, Some("OpenSession"));
        registry.open_or_get_with_context("pg-2", Some("app"), None, Some("Scan"));

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.iter().any(|session| session.session_id() == "pg-1"));
        assert!(snapshot.iter().any(|session| session.session_id() == "pg-2"));
    }

    #[test]
    fn begin_and_commit_transaction() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");

        let tx_id = registry.begin_transaction("pg-1").unwrap();
        assert!(Uuid::parse_str(&tx_id).is_ok());

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

    #[test]
    fn mark_transaction_writes_promotes_open_read_to_open_write() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");
        let _tx_id = registry.begin_transaction("pg-1").unwrap();

        registry.mark_transaction_writes("pg-1");

        let session = registry.open_or_get("pg-1");
        assert_eq!(session.transaction_state(), Some(TransactionState::OpenWrite));
        assert!(session.transaction_has_writes());
    }

    #[test]
    fn snapshot_with_live_transactions_clears_stale_local_transaction_state() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");
        let _tx_id = registry.begin_transaction("pg-1").unwrap();
        registry.mark_transaction_writes("pg-1");

        let snapshot = registry.snapshot_with_live_transactions(Vec::<LivePgTransaction>::new());
        let session = snapshot
            .into_iter()
            .find(|session| session.session_id() == "pg-1")
            .unwrap();

        assert_eq!(session.transaction_id(), None);
        assert_eq!(session.transaction_state(), None);
        assert!(!session.transaction_has_writes());
    }

    #[test]
    fn snapshot_with_live_transactions_prefers_live_transaction_state() {
        let registry = SessionRegistry::default();
        registry.open_or_get("pg-1");
        registry.begin_transaction_with_id("pg-1", "stale-tx").unwrap();

        let snapshot = registry.snapshot_with_live_transactions(vec![LivePgTransaction::new(
            "pg-1",
            "live-tx",
            TransactionState::OpenWrite,
            true,
        )]);
        let session = snapshot
            .into_iter()
            .find(|session| session.session_id() == "pg-1")
            .unwrap();

        assert_eq!(session.transaction_id(), Some("live-tx"));
        assert_eq!(session.transaction_state(), Some(TransactionState::OpenWrite));
        assert!(session.transaction_has_writes());
    }
}
