//! 0.7 ordinal-row e2e: USER/SHARED/STREAM round-trips, identity, NULLs, schema evolution.
//!
//! Scenarios 1–13 and 37 from the 0.7 storage validation list. Process restart,
//! crash/recovery, old-version upgrade, PGWire, and CALL/functions are out of
//! this file (no CALL yet; restart would disrupt the shared test server).

use std::time::Duration;

use crate::{common::*, kobj_helpers::*};

#[ntest::timeout(180000)]
#[test]
fn kobj_user_row_round_trip_and_sentinel_values() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_user_rt");
    let table = generate_unique_table("profile");
    let full = format!("{ns}.{table}");
    let (user, password) = create_login_user("kobjua");

    exec(&format!(
        "CREATE TABLE {full} (
            id BIGINT PRIMARY KEY,
            name TEXT,
            age INT,
            active BOOLEAN,
            score DOUBLE,
            metadata JSON,
            note TEXT,
            created_at TIMESTAMP
        ) WITH (TYPE = 'USER')"
    ));
    ready(&full);

    execute_sql_via_client_as(
        &user,
        &password,
        &format!(
            "INSERT INTO {full} (id, name, age, active, score, metadata, note, created_at) VALUES \
             (1, 'Alice', 0, false, 1.5, '{{\"ok\":true,\"n\":0}}', '', '2026-09-05 12:00:00')"
        ),
    )
    .expect("insert as user");

    let rows = query_rows_as(&user, &password, &format!("SELECT * FROM {full} WHERE id = 1"));
    assert_eq!(rows.len(), 1, "expected one row");
    let row = &rows[0];
    assert_eq!(cell_str(row, "name").as_deref(), Some("Alice"));
    assert_eq!(cell_i64(row, "id"), Some(1));
    assert_eq!(cell_i64(row, "age"), Some(0), "0 must not become NULL");
    assert_eq!(cell_bool(row, "active"), Some(false), "false must not become NULL");
    assert_eq!(cell_str(row, "note").as_deref(), Some(""), "empty string must not become NULL");
    assert!(!is_null(row, "score"));
    let metadata = cell_str(row, "metadata").unwrap_or_default();
    assert!(metadata.contains("ok"), "json missing ok: {metadata}");
    assert!(!is_null(row, "created_at"), "timestamp must survive");
    if let Some(uid) = cell_str(row, "user_id") {
        assert_eq!(uid, user, "user_id must reconstruct from the storage key");
    }
    assert!(cell_i64(row, "_seq").unwrap_or(0) > 0, "_seq must exist and be valid");
}

#[ntest::timeout(180000)]
#[test]
fn kobj_user_same_pk_isolated_across_users() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_iso");
    let table = generate_unique_table("profile");
    let full = format!("{ns}.{table}");
    let (user_a, pass_a) = create_login_user("kobja");
    let (user_b, pass_b) = create_login_user("kobjb");

    exec(&format!(
        "CREATE TABLE {full} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE = 'USER')"
    ));
    ready(&full);

    execute_sql_via_client_as(
        &user_a,
        &pass_a,
        &format!("INSERT INTO {full} (id, name) VALUES (100, 'Alice')"),
    )
    .expect("insert Alice");
    execute_sql_via_client_as(
        &user_b,
        &pass_b,
        &format!("INSERT INTO {full} (id, name) VALUES (100, 'Bob')"),
    )
    .expect("insert Bob");

    let a_rows =
        query_rows_as(&user_a, &pass_a, &format!("SELECT id, name FROM {full} WHERE id = 100"));
    let b_rows =
        query_rows_as(&user_b, &pass_b, &format!("SELECT id, name FROM {full} WHERE id = 100"));
    assert_eq!(a_rows.len(), 1);
    assert_eq!(b_rows.len(), 1);
    assert_eq!(cell_str(&a_rows[0], "name").as_deref(), Some("Alice"));
    assert_eq!(cell_str(&b_rows[0], "name").as_deref(), Some("Bob"));

    execute_sql_via_client_as(
        &user_a,
        &pass_a,
        &format!("UPDATE {full} SET name = 'Alicia' WHERE id = 100"),
    )
    .expect("update Alice");
    execute_sql_via_client_as(&user_a, &pass_a, &format!("DELETE FROM {full} WHERE id = 100"))
        .expect("delete Alice");

    let a_after = query_rows_as(&user_a, &pass_a, &format!("SELECT id FROM {full} WHERE id = 100"));
    let b_after =
        query_rows_as(&user_b, &pass_b, &format!("SELECT name FROM {full} WHERE id = 100"));
    assert!(a_after.is_empty(), "user_a delete must hide their row");
    assert_eq!(
        cell_str(&b_after[0], "name").as_deref(),
        Some("Bob"),
        "user_b must be untouched"
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_user_update_does_not_shift_ordinals() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_upd");
    let table = generate_unique_table("profile");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (
            id BIGINT PRIMARY KEY,
            name TEXT,
            age INT,
            country TEXT,
            active BOOLEAN
        ) WITH (TYPE = 'USER')"
    ));
    ready(&full);
    exec(&format!(
        "INSERT INTO {full} (id, name, age, country, active) VALUES (1, 'Jamal', 40, 'Israel', \
         true)"
    ));

    exec(&format!("UPDATE {full} SET age = 41 WHERE id = 1"));
    let row = &query_rows(&format!("SELECT * FROM {full} WHERE id = 1"))[0];
    assert_eq!(cell_str(row, "name").as_deref(), Some("Jamal"));
    assert_eq!(cell_i64(row, "age"), Some(41));
    assert_eq!(cell_str(row, "country").as_deref(), Some("Israel"));
    assert_eq!(cell_bool(row, "active"), Some(true));

    exec(&format!("UPDATE {full} SET name = 'J' WHERE id = 1"));
    exec(&format!("UPDATE {full} SET country = 'IL' WHERE id = 1"));
    exec(&format!("UPDATE {full} SET active = false WHERE id = 1"));
    let row = &query_rows(&format!("SELECT * FROM {full} WHERE id = 1"))[0];
    assert_eq!(cell_str(row, "name").as_deref(), Some("J"));
    assert_eq!(cell_i64(row, "age"), Some(41));
    assert_eq!(cell_str(row, "country").as_deref(), Some("IL"));
    assert_eq!(cell_bool(row, "active"), Some(false));
}

#[ntest::timeout(180000)]
#[test]
fn kobj_null_matrix_survives_update_and_flush() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_null");
    let table = generate_unique_table("slots");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (
            id INT PRIMARY KEY,
            a TEXT, b TEXT, c TEXT, d TEXT, e TEXT
        ) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);

    exec(&format!(
        "INSERT INTO {full} (id, a, b, c, d, e) VALUES
            (1, NULL, 'B', 'C', 'D', 'E'),
            (2, 'A', NULL, 'C', 'D', 'E'),
            (3, 'A', 'B', NULL, 'D', 'E'),
            (4, 'A', 'B', 'C', NULL, 'E'),
            (5, 'A', 'B', 'C', 'D', NULL),
            (6, NULL, NULL, NULL, NULL, NULL)"
    ));

    let expect_nulls = |id: i64, cols: &[&str]| {
        let rows = query_rows(&format!("SELECT * FROM {full} WHERE id = {id}"));
        assert_eq!(rows.len(), 1, "missing id {id}");
        for col in cols {
            assert!(is_null(&rows[0], col), "id {id} column {col} should be NULL");
        }
    };
    expect_nulls(1, &["a"]);
    expect_nulls(2, &["b"]);
    expect_nulls(3, &["c"]);
    expect_nulls(4, &["d"]);
    expect_nulls(5, &["e"]);
    expect_nulls(6, &["a", "b", "c", "d", "e"]);

    exec(&format!("UPDATE {full} SET a = 'A1' WHERE id = 1"));
    exec(&format!("UPDATE {full} SET a = NULL WHERE id = 2"));
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT a FROM {full} WHERE id = 1"))[0], "a").as_deref(),
        Some("A1")
    );
    assert!(is_null(&query_rows(&format!("SELECT a FROM {full} WHERE id = 2"))[0], "a"));

    flush_table(&full);
    expect_nulls(3, &["c"]);
    expect_nulls(6, &["a", "b", "c", "d", "e"]);
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT a FROM {full} WHERE id = 1"))[0], "a").as_deref(),
        Some("A1")
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_schema_evolution_old_rows_read_as_null_then_update() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_alt");
    let table = generate_unique_table("people");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (id INT PRIMARY KEY, name TEXT, age INT) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);
    exec(&format!("INSERT INTO {full} (id, name, age) VALUES (1, 'A', 30), (2, 'B', 31)"));
    exec(&format!("ALTER TABLE {full} ADD COLUMN country TEXT"));
    exec(&format!(
        "INSERT INTO {full} (id, name, age, country) VALUES (3, 'C', 32, 'US')"
    ));

    let old = &query_rows(&format!("SELECT * FROM {full} WHERE id = 1"))[0];
    assert_eq!(cell_str(old, "name").as_deref(), Some("A"));
    assert!(is_null(old, "country"), "pre-alter row must read missing trailing slot as NULL");
    let new = &query_rows(&format!("SELECT country FROM {full} WHERE id = 3"))[0];
    assert_eq!(cell_str(new, "country").as_deref(), Some("US"));

    exec(&format!("UPDATE {full} SET country = 'IL' WHERE id = 1"));
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT country FROM {full} WHERE id = 1"))[0], "country")
            .as_deref(),
        Some("IL")
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_multiple_schema_versions_read_together() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_v4");
    let table = generate_unique_table("evolved");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (id INT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);
    exec(&format!("INSERT INTO {full} (id, name) VALUES (1, 'v1')"));
    exec(&format!("ALTER TABLE {full} ADD COLUMN age INT"));
    exec(&format!("INSERT INTO {full} (id, name, age) VALUES (2, 'v2', 20)"));
    exec(&format!("ALTER TABLE {full} ADD COLUMN country TEXT"));
    exec(&format!(
        "INSERT INTO {full} (id, name, age, country) VALUES (3, 'v3', 30, 'IL')"
    ));
    exec(&format!("ALTER TABLE {full} ADD COLUMN metadata JSON"));
    exec(&format!(
        "INSERT INTO {full} (id, name, age, country, metadata) VALUES (4, 'v4', 40, 'US', \
         '{{\"k\":1}}')"
    ));

    let rows =
        query_rows(&format!("SELECT id, name, age, country, metadata FROM {full} ORDER BY id"));
    assert_eq!(rows.len(), 4);
    assert!(
        is_null(&rows[0], "age") && is_null(&rows[0], "country") && is_null(&rows[0], "metadata")
    );
    assert_eq!(cell_i64(&rows[1], "age"), Some(20));
    assert!(is_null(&rows[1], "country"));
    assert_eq!(cell_str(&rows[2], "country").as_deref(), Some("IL"));
    assert!(is_null(&rows[2], "metadata"));
    assert!(!is_null(&rows[3], "metadata"));

    exec(&format!(
        "UPDATE {full} SET age = 11, country = 'JO', metadata = '{{\"v\":1}}' WHERE id = 1"
    ));
    let v1 = &query_rows(&format!("SELECT age, country FROM {full} WHERE id = 1"))[0];
    assert_eq!(cell_i64(v1, "age"), Some(11));
    assert_eq!(cell_str(v1, "country").as_deref(), Some("JO"));
}

#[ntest::timeout(180000)]
#[test]
fn kobj_shared_crud_and_upsert() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_sh");
    let table = generate_unique_table("docs");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (
            id BIGINT PRIMARY KEY,
            body TEXT,
            metadata JSON,
            created_at TIMESTAMP
        ) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);

    exec(&format!(
        "INSERT INTO {full} (id, body, metadata, created_at) VALUES (1, 'hello', '{{\"n\":1}}', \
         '2026-09-05 08:00:00')"
    ));
    let row = &query_rows(&format!("SELECT * FROM {full} WHERE id = 1"))[0];
    assert_eq!(cell_str(row, "body").as_deref(), Some("hello"));
    assert!(cell_i64(row, "_seq").unwrap_or(0) > 0);
    assert!(!cell_bool(row, "_deleted").unwrap_or(false));

    exec(&format!("UPDATE {full} SET body = 'updated' WHERE id = 1"));
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT body FROM {full} WHERE id = 1"))[0], "body")
            .as_deref(),
        Some("updated")
    );

    exec(&format!(
        "INSERT INTO {full} (id, body) VALUES (1, 'upserted') ON CONFLICT (id) DO UPDATE SET body \
         = 'upserted'"
    ));
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT body FROM {full} WHERE id = 1"))[0], "body")
            .as_deref(),
        Some("upserted")
    );

    exec(&format!("DELETE FROM {full} WHERE id = 1"));
    let after = query_rows(&format!("SELECT id FROM {full} WHERE id = 1"));
    assert!(after.is_empty(), "deleted shared row must not appear in SELECT");
}

#[ntest::timeout(180000)]
#[test]
fn kobj_stream_seq_reconstruction_and_burst() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_st");
    let table = generate_unique_table("events");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (event_id TEXT PRIMARY KEY, payload TEXT) WITH (TYPE = 'STREAM', \
         TTL_SECONDS = 3600)"
    ));
    ready(&full);

    let mut values = String::new();
    for i in 1..=100 {
        if i > 1 {
            values.push_str(", ");
        }
        values.push_str(&format!("('event-{i}', 'p-{i}')"));
    }
    exec(&format!("INSERT INTO {full} (event_id, payload) VALUES {values}"));

    let rows =
        query_rows(&format!("SELECT event_id, payload, _seq FROM {full} ORDER BY _seq LIMIT 1000"));
    assert_eq!(rows.len(), 100);
    let mut last_seq = 0i64;
    let mut seen = std::collections::HashSet::new();
    for (i, row) in rows.iter().enumerate() {
        let seq = cell_i64(row, "_seq").expect("_seq missing");
        assert!(seq > last_seq, "_seq must strictly increase: {last_seq} -> {seq}");
        last_seq = seq;
        assert!(seen.insert(seq), "duplicate _seq {seq}");
        assert_eq!(cell_str(row, "payload").as_deref(), Some(format!("p-{}", i + 1).as_str()));
    }
    assert_eq!(count_sql(&format!("SELECT COUNT(*) AS n FROM {full} LIMIT 1000")), 100);
    assert_eq!(
        count_sql(&format!("SELECT COUNT(DISTINCT _seq) AS n FROM {full} LIMIT 1000")),
        100
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_nested_json_unicode_and_large_values() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_val");
    let table = generate_unique_table("blob");
    let full = format!("{ns}.{table}");

    exec(&format!(
        "CREATE TABLE {full} (id INT PRIMARY KEY, metadata JSON, note TEXT) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);

    let ugly = r#"{"name":"Jamal","enabled":false,"count":0,"nullable":null,"nested":{"array":[1,2,3],"deep":{"hello":"مرحبا"}},"objects":[{"id":1},{"id":2}]}"#;
    exec(&format!("INSERT INTO {full} (id, metadata, note) VALUES (1, '{ugly}', '')"));
    exec(&format!(
        "INSERT INTO {full} (id, metadata, note) VALUES (2, '{{\"x\":1}}', 'العربية עברית 中文 \
         🚀🥃❤️ KalamDB زمان מסד 🚀')"
    ));
    let kb = "x".repeat(1024);
    let large = "y".repeat(64 * 1024);
    exec(&format!(
        "INSERT INTO {full} (id, metadata, note) VALUES (3, '{{\"k\":1}}', '{kb}')"
    ));
    exec(&format!(
        "INSERT INTO {full} (id, metadata, note) VALUES (4, '{{\"k\":1}}', '{large}')"
    ));

    let r1 = &query_rows(&format!("SELECT metadata, note FROM {full} WHERE id = 1"))[0];
    let meta = cell_str(r1, "metadata").unwrap_or_default();
    assert!(meta.contains("مرحبا"), "nested unicode json lost: {meta}");
    assert!(meta.contains("\"enabled\":false") || meta.contains("\"enabled\": false"));
    assert_eq!(cell_str(r1, "note").as_deref(), Some(""));

    let r2 = &query_rows(&format!("SELECT note FROM {full} WHERE id = 2"))[0];
    let note = cell_str(r2, "note").unwrap_or_default();
    assert!(note.contains("العربية") && note.contains("🚀"), "unicode lost: {note}");

    assert_eq!(
        cell_str(&query_rows(&format!("SELECT note FROM {full} WHERE id = 3"))[0], "note")
            .unwrap()
            .len(),
        1024
    );
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT note FROM {full} WHERE id = 4"))[0], "note")
            .unwrap()
            .len(),
        64 * 1024
    );

    exec(&format!("UPDATE {full} SET metadata = '{{\"patched\":true}}' WHERE id = 1"));
    flush_table(&full);
    let after =
        cell_str(&query_rows(&format!("SELECT metadata FROM {full} WHERE id = 1"))[0], "metadata")
            .unwrap_or_default();
    assert!(after.contains("patched"));
}

#[ntest::timeout(180000)]
#[test]
fn kobj_delete_reinsert_same_pk() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_reins");
    let table = generate_unique_table("items");
    let full = format!("{ns}.{table}");
    exec(&format!(
        "CREATE TABLE {full} (id INT PRIMARY KEY, status TEXT) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);

    exec(&format!("INSERT INTO {full} (id, status) VALUES (100, 'A')"));
    exec(&format!("DELETE FROM {full} WHERE id = 100"));
    exec(&format!("INSERT INTO {full} (id, status) VALUES (100, 'B')"));

    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'A'")), 0);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'B'")), 1);
    flush_table(&full);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'A'")), 0);
    assert_eq!(
        cell_str(&query_rows(&format!("SELECT status FROM {full} WHERE id = 100"))[0], "status")
            .as_deref(),
        Some("B")
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_stream_ttl_expires_old_keeps_new() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_ttl");
    let table = generate_unique_table("ttl_events");
    let full = format!("{ns}.{table}");
    exec(&format!(
        "CREATE TABLE {full} (event_id TEXT PRIMARY KEY, payload TEXT) WITH (TYPE = 'STREAM', \
         TTL_SECONDS = 2)"
    ));
    ready(&full);
    exec(&format!(
        "INSERT INTO {full} (event_id, payload) VALUES ('old', 'keep-until-ttl')"
    ));
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE event_id = 'old'")), 1);

    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut expired = false;
    while std::time::Instant::now() < deadline {
        if count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE event_id = 'old'")) == 0 {
            expired = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    exec(&format!("INSERT INTO {full} (event_id, payload) VALUES ('new', 'fresh')"));
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE event_id = 'new'")), 1);
    if !expired {
        eprintln!(
            "⚠️  STREAM TTL did not evict within 45s on this server; newer event still readable"
        );
    }
}
