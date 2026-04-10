use kalamdb_commons::models::UserId;
use pg_kalam::SessionSettings;

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
fn tenant_context_allows_explicit_override() {
    let settings =
        SessionSettings::from_guc_values(Some("u_session"), Some("public")).expect("parse guc");
    let ctx = settings
        .tenant_context(Some(UserId::new("u_other")))
        .expect("explicit override should succeed");

    assert_eq!(
        ctx.effective_user_id(),
        Some(&UserId::new("u_other")),
        "explicit user_id should take precedence"
    );
}

#[test]
fn trims_blank_schema_to_missing() {
    let settings =
        SessionSettings::from_guc_values(Some("u_session"), Some("   ")).expect("parse schema");
    assert_eq!(settings.current_schema(), None);
}
