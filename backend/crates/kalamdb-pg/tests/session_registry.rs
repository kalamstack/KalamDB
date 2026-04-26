use kalamdb_commons::models::TransactionId;
use kalamdb_pg::{SessionRegistry, TransactionState};

#[test]
fn session_registry_reuses_existing_session_keys() {
    let registry = SessionRegistry::default();

    let first = registry.open_or_get("pg-backend-1");
    let second = registry.open_or_get("pg-backend-1");

    assert_eq!(first.session_id(), second.session_id());
    assert_eq!(registry.len(), 1);
}

#[test]
fn session_registry_schema_update() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let updated = registry.update("s1", Some("tenant_a"), None).unwrap();
    assert_eq!(updated.current_schema(), Some("tenant_a"));

    let updated2 = registry.update("s1", Some("tenant_b"), None).unwrap();
    assert_eq!(updated2.current_schema(), Some("tenant_b"));
    assert_eq!(updated2.session_id(), "s1");
}

#[test]
fn session_registry_begin_commit_transaction() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id = registry.begin_transaction("s1").expect("begin");
    assert!(!tx_id.as_str().is_empty());

    let session = registry.open_or_get("s1");
    assert_eq!(session.transaction_id(), Some(tx_id.as_str()));
    assert_eq!(session.transaction_state(), Some(TransactionState::OpenRead));

    let committed = registry.commit_transaction("s1", &tx_id).expect("commit");
    assert_eq!(committed, tx_id);

    let session = registry.open_or_get("s1");
    assert_eq!(session.transaction_id(), None);
    assert_eq!(session.transaction_state(), None);
}

#[test]
fn session_registry_begin_rollback_transaction() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id = registry.begin_transaction("s1").expect("begin");

    let rolled_back = registry.rollback_transaction("s1", &tx_id).expect("rollback");
    assert_eq!(rolled_back, tx_id);

    let session = registry.open_or_get("s1");
    assert_eq!(session.transaction_id(), None);
    assert_eq!(session.transaction_state(), None);
}

#[test]
fn session_registry_double_begin_auto_rollbacks_stale() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id1 = registry.begin_transaction("s1").expect("begin");
    // A second begin should auto-rollback the stale transaction (safety net for
    // client crashes) instead of returning an error.
    let tx_id2 = registry.begin_transaction("s1").expect("double begin should auto-rollback");
    assert_ne!(tx_id1, tx_id2);

    // The new transaction should be committable
    registry.commit_transaction("s1", &tx_id2).expect("commit after auto-rollback");
}

#[test]
fn session_registry_commit_wrong_tx_id_fails() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let _tx_id = registry.begin_transaction("s1").expect("begin");
    let wrong_tx_id = TransactionId::new("01960f7b-3d16-7d6d-b26c-7e4db6f25f8d");
    let err = registry.commit_transaction("s1", &wrong_tx_id).expect_err("wrong tx id");
    assert!(err.contains("mismatch"));
}

#[test]
fn session_registry_rollback_without_transaction_is_noop() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id = TransactionId::new("01960f7b-3d17-7d6d-b26c-7e4db6f25f8d");
    let result = registry.rollback_transaction("s1", &tx_id);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), tx_id);
}

#[test]
fn session_registry_commit_already_committed_fails() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id = registry.begin_transaction("s1").expect("begin");
    registry.commit_transaction("s1", &tx_id).expect("commit");

    let err = registry.commit_transaction("s1", &tx_id).expect_err("no active tx");
    assert!(err.contains("no active transaction"));
}

#[test]
fn session_registry_mark_writes() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let _tx_id = registry.begin_transaction("s1").expect("begin");
    assert!(!registry.open_or_get("s1").transaction_has_writes());

    registry.mark_transaction_writes("s1");
    let session = registry.open_or_get("s1");
    assert_eq!(session.transaction_state(), Some(TransactionState::OpenWrite));
    assert!(session.transaction_has_writes());
}

#[test]
fn session_registry_multiple_sessions_independent() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");
    registry.open_or_get("s2");

    let tx1 = registry.begin_transaction("s1").expect("begin s1");
    let s2 = registry.open_or_get("s2");
    assert_eq!(s2.transaction_id(), None);

    let tx2 = registry.begin_transaction("s2").expect("begin s2");
    assert_ne!(tx1, tx2);

    registry.commit_transaction("s1", &tx1).expect("commit s1");
    let s2 = registry.open_or_get("s2");
    assert_eq!(s2.transaction_state(), Some(TransactionState::OpenRead));

    registry.rollback_transaction("s2", &tx2).expect("rollback s2");
}

#[test]
fn session_registry_remove_session() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");
    assert_eq!(registry.len(), 1);

    let removed = registry.remove("s1");
    assert!(removed.is_some());
    assert_eq!(registry.len(), 0);
}

#[test]
fn session_registry_close_session_clears_transaction_state() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id = registry.begin_transaction("s1").expect("begin");
    registry.mark_transaction_writes("s1");

    let closed = registry.close_session("s1").expect("close session");
    assert_eq!(closed.session_id(), "s1");
    assert_eq!(closed.transaction_id(), None);
    assert_eq!(closed.transaction_state(), None);
    assert!(!closed.transaction_has_writes());
    assert_eq!(registry.len(), 0);
    assert!(registry.rollback_transaction("s1", &tx_id).is_err());
}

#[test]
fn session_registry_clear_transaction_state_requires_matching_tx_id() {
    let registry = SessionRegistry::default();
    registry.open_or_get("s1");

    let tx_id = registry.begin_transaction("s1").expect("begin");
    let different_tx_id = TransactionId::new("01960f7b-3d18-7d6d-b26c-7e4db6f25f8d");
    registry.clear_transaction_state_if_matches("s1", Some(&different_tx_id));

    let session = registry.open_or_get("s1");
    assert_eq!(session.transaction_id(), Some(tx_id.as_str()));
    assert_eq!(session.transaction_state(), Some(TransactionState::OpenRead));

    registry.clear_transaction_state_if_matches("s1", Some(&tx_id));
    let cleared = registry.open_or_get("s1");
    assert_eq!(cleared.transaction_id(), None);
    assert_eq!(cleared.transaction_state(), None);
}

#[test]
fn session_registry_begin_on_nonexistent_session_fails() {
    let registry = SessionRegistry::default();
    let err = registry.begin_transaction("nonexistent").expect_err("no session");
    assert!(err.contains("not found"));
}
