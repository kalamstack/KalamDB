use pg_kalam::SessionSettings;
use kalamdb_commons::models::UserId;

#[test]
fn parses_blank_guc_as_missing_user() {
    let settings =
        SessionSettings::from_guc_values(Some("   "), Some("   ")).expect("parse blank guc");
    assert_eq!(settings.session_user_id(), None);
    assert_eq!(settings.current_schema(), None);
}

#[test]
fn exposes_guc_name_and_user_id() {
    let settings =
        SessionSettings::from_guc_values(Some("u_session"), Some("tenant_app")).expect("parse guc");
    assert_eq!(SessionSettings::guc_name(), "kalam.user_id");
    assert_eq!(settings.session_user_id(), Some(&UserId::new("u_session")));
    assert_eq!(settings.current_schema(), Some("tenant_app"));
}

#[test]
fn tenant_context_rejects_mismatched_explicit_user() {
    let settings =
        SessionSettings::from_guc_values(Some("u_session"), Some("public")).expect("parse guc");
    let err = settings
        .tenant_context(Some(UserId::new("u_other")))
        .expect_err("mismatched user should fail");

    assert!(err.to_string().contains("does not match"));
}

#[test]
fn trims_blank_schema_to_missing() {
    let settings =
        SessionSettings::from_guc_values(Some("u_session"), Some("   ")).expect("parse schema");
    assert_eq!(settings.current_schema(), None);
}
