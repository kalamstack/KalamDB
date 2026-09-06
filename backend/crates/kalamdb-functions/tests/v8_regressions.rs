use datafusion_common::ScalarValue;
use kalamdb_commons::RoutineId;
use kalamdb_functions::{ModuleRevision, RoutineValue, RuntimeLimits, V8Session};
use tokio_util::sync::CancellationToken;

#[test]
fn settled_promise_returns_its_value() {
    let mut session = V8Session::load(
        ModuleRevision::typescript_fixture(
            "async function kalamInvoke(name, args) { return await Promise.resolve(args[0] + 1); }",
        ),
        RuntimeLimits::default(),
    )
    .unwrap();
    let result = session
        .invoke(
            &RoutineId::new("increment"),
            &[RoutineValue::new(ScalarValue::Int32(Some(6)))],
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(result.value, ScalarValue::Int32(Some(7)));
}

#[test]
fn reusable_session_does_not_retain_user_globals() {
    let mut session = V8Session::load(
        ModuleRevision::typescript_fixture(
            "function kalamInvoke() { globalThis.count = (globalThis.count || 0) + 1; return \
             count; }",
        ),
        RuntimeLimits::default(),
    )
    .unwrap();
    for _ in 0..3 {
        let value = session
            .invoke(&RoutineId::new("count"), &[], &CancellationToken::new())
            .unwrap();
        assert_eq!(value.value, ScalarValue::Int32(Some(1)));
    }
}
