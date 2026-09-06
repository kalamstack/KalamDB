//! Wave 3 Checkpoint C: CALL + nested SQL/topic + EXECUTE ACL.

use serde_json::json;

use crate::{common::*, kobj_helpers::*};

fn create_js_procedure(ns: &str, name: &str, params: &str, body: &str) {
    exec(&format!(
        "CREATE OR REPLACE PROCEDURE {ns}.{name}({params}) LANGUAGE JAVASCRIPT AS $$\n{body}\n$$"
    ));
}

#[ntest::timeout(300000)]
#[test]
fn kobj_functions_checkpoint_c_call_nested_db_and_topic() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_fn");
    let orders = format!("{ns}.fn_orders");
    exec(&format!(
        "CREATE TABLE {orders} (id INT PRIMARY KEY, status TEXT) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&orders);
    ready(&orders);
    let topic = format!("{ns}.fn_events");
    exec(&format!("CREATE TOPIC {topic}"));

    create_js_procedure(&ns, "echo", "msg TEXT", "return input;");
    create_js_procedure(&ns, "inc", "x INT", "return input + 1;");
    create_js_procedure(
        &ns,
        "plus_one",
        "x INT",
        &format!("return ctx.functions.call('{ns}.inc', [input]);"),
    );
    create_js_procedure(&ns, "boom", "", "throw new Error('boom');");
    create_js_procedure(&ns, "wrap_boom", "", &format!("return ctx.functions.call('{ns}.boom');"));
    create_js_procedure(
        &ns,
        "place_order",
        "p_id INT",
        &format!(
            "ctx.db.sql(\"INSERT INTO {orders} (id, status) VALUES (\" + input + \", \
             'ok')\");\nctx.topics.publish('{topic}', {{ id: input, status: 'ok' }});\nreturn {{ \
             id: input, status: 'ok' }};"
        ),
    );

    let echo_rows = query_rows(&format!("CALL {ns}.echo('hello')"));
    assert_eq!(echo_rows.len(), 1, "CALL echo should return one row: {echo_rows:?}");
    let echoed = cell(&echo_rows[0], "result");
    assert!(
        echoed.as_str() == Some("hello") || echoed.to_string().contains("hello"),
        "echoed value: {echoed}"
    );

    let nested = query_rows(&format!("CALL {ns}.plus_one(41)"));
    let nested_value = cell_i64(&nested[0], "result")
        .unwrap_or_else(|| cell(&nested[0], "result").as_i64().unwrap_or(-1));
    assert_eq!(nested_value, 42, "nested CALL should return 42: {nested:?}");

    let boom = exec_err(&format!("CALL {ns}.wrap_boom()"));
    let boom_lower = boom.to_ascii_lowercase();
    assert!(
        boom_lower.contains("wrap_boom") && boom_lower.contains("boom"),
        "nested error must include the call stack: {boom}"
    );

    exec(&format!("CALL {ns}.place_order(7)"));
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {orders} WHERE id = 7")), 1);
    let consumed = query_rows(&format!("CONSUME FROM {topic} FROM EARLIEST LIMIT 10"));
    assert!(
        !consumed.is_empty(),
        "typed topic publish should be visible after CALL commit: {consumed:?}"
    );

    exec(&format!("BEGIN; CALL {ns}.place_order(99); ROLLBACK;"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {orders} WHERE id = 99")),
        0,
        "rollback must drop nested INSERT"
    );
    let after_rollback = query_rows(&format!("CONSUME FROM {topic} FROM EARLIEST LIMIT 10"));
    assert_eq!(
        after_rollback.len(),
        consumed.len(),
        "rollback must drop staged topic publish: before={consumed:?} after={after_rollback:?}"
    );

    let (username, password) = create_login_user("fnuser");
    let denied =
        execute_sql_via_client_as(&username, &password, &format!("CALL {ns}.echo('nope')"));
    assert!(denied.is_err(), "user without EXECUTE must be denied");

    exec(&format!("GRANT EXECUTE ON PROCEDURE {ns}.echo TO user"));
    execute_sql_via_client_as(&username, &password, &format!("CALL {ns}.echo('ok')"))
        .unwrap_or_else(|err| panic!("granted user CALL should succeed: {err}"));
    exec(&format!("REVOKE EXECUTE ON PROCEDURE {ns}.echo FROM user"));
    let revoked =
        execute_sql_via_client_as(&username, &password, &format!("CALL {ns}.echo('later')"));
    assert!(revoked.is_err(), "revoked EXECUTE must deny CALL");

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let body = rt.block_on(async {
        let token = get_access_token(default_username(), default_password())
            .await
            .expect("root token");
        let response = shared_http_client()
            .post(format!("{}/v1/functions/{ns}/echo", server_url()))
            .bearer_auth(token)
            .json(&json!({ "msg": "rest" }))
            .send()
            .await
            .expect("REST CALL");
        let status = response.status();
        let body = response.text().await.expect("REST body");
        assert!(status.is_success(), "REST /v1/functions should succeed: {status} {body}");
        body
    });
    assert!(
        body.to_ascii_lowercase().contains("rest"),
        "REST result should echo the argument: {body}"
    );
}

#[ntest::timeout(300000)]
#[test]
fn kobj_functions_topic_trigger_delivers_and_acks() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_trig");
    let hits = format!("{ns}.trig_hits");
    exec(&format!(
        "CREATE TABLE {hits} (id INT PRIMARY KEY, note TEXT) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&hits);
    ready(&hits);
    let topic = format!("{ns}.trig_events");
    exec(&format!("CREATE TOPIC {topic}"));
    create_js_procedure(
        &ns,
        "on_trig",
        "payload TEXT",
        &format!(
            "var payload = input;\nif (typeof payload === 'string') {{ try {{ payload = \
             JSON.parse(payload); }} catch (e) {{}} }}\nvar id = (payload && payload.id != null) \
             ? payload.id : payload;\nctx.db.sql(\"INSERT INTO {hits} (id, note) VALUES (\" + id \
             + \", 'ok')\");\nreturn payload;"
        ),
    );
    create_js_procedure(
        &ns,
        "publish_trig",
        "p_id INT",
        &format!("ctx.topics.publish('{topic}', {{ id: input }});\nreturn input;"),
    );
    exec(&format!(
        "CREATE TRIGGER {ns}.on_trig_event ON TOPIC {topic} EXECUTE PROCEDURE \
         {ns}.on_trig(PAYLOAD) WITH (start = 'latest', retries = 3, retry_backoff = '100ms')"
    ));
    exec(&format!("CALL {ns}.publish_trig(7)"));

    let mut delivered = false;
    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(250));
        if count_sql(&format!("SELECT COUNT(*) FROM {hits} WHERE id = 7")) == 1 {
            delivered = true;
            break;
        }
    }
    assert!(delivered, "trigger should insert into {hits} after topic publish");
    let attempts = query_rows(&format!(
        "SELECT status FROM system.trigger_attempts WHERE event_id LIKE '%{topic}%'"
    ));
    assert!(
        attempts.iter().any(|row| cell(row, "status").as_str() == Some("succeeded")),
        "trigger attempt should be succeeded: {attempts:?}"
    );
}
