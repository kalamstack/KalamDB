use kalam_pg_common::mode::BackendMode;

#[test]
fn detects_enabled_backend_mode() {
    assert_eq!(BackendMode::current(), BackendMode::Remote);
}
