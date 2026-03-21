use kalamdb_pg::SessionRegistry;

#[test]
fn session_registry_reuses_existing_session_keys() {
    let registry = SessionRegistry::default();

    let first = registry.open_or_get("pg-backend-1");
    let second = registry.open_or_get("pg-backend-1");

    assert_eq!(first.session_id(), second.session_id());
    assert_eq!(registry.len(), 1);
}
