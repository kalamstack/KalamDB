use pg_kalam::{kalam_compiled_mode, kalam_user_id_guc_name, kalam_version};

#[test]
fn extension_reports_version_and_guc_name() {
    assert_eq!(kalam_version(), env!("CARGO_PKG_VERSION"));
    assert_eq!(kalam_user_id_guc_name(), "kalam.user_id");
    assert_eq!(kalam_compiled_mode(), "remote");
}
