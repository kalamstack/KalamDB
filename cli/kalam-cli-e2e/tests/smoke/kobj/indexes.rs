//! 0.7 scalar-index e2e: catalog, maintenance, hot/cold merge, failed writes.
//!
//! Scenarios 14–31 and 38–40. Scalar indexes are a hot-path feature; after
//! flush, SQL equality must still be correct even if the seek uses cold scan.

use crate::{common::*, kobj_helpers::*};

fn create_indexed_shared(ns: &str, table: &str, extra_cols: &str) -> String {
    let full = format!("{ns}.{table}");
    exec(&format!(
        "CREATE TABLE {full} (id INT PRIMARY KEY, {extra_cols}) WITH (TYPE = 'SHARED')"
    ));
    grant_public_shared_table_access(&full);
    ready(&full);
    full
}

#[ntest::timeout(400000)]
#[test]
fn kobj_create_drop_index_catalog_and_errors() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_idxcat");
    let table = generate_unique_table("messages");
    let full = create_indexed_shared(&ns, &table, "conversation_id TEXT, body TEXT");

    exec(&format!("CREATE INDEX idx_conv ON {full} (conversation_id)"));
    let listed = latest_indexes_json(&ns, &table);
    assert_index_named(&listed, "idx_conv");
    let parsed: serde_json::Value =
        serde_json::from_str(&listed).unwrap_or_else(|err| panic!("indexes JSON: {err}\n{listed}"));
    let entries = parsed
        .as_array()
        .unwrap_or_else(|| panic!("indexes must be a JSON array: {listed}"));
    assert_eq!(entries.len(), 1, "exactly one catalog index after CREATE: {listed}");
    assert_eq!(entries[0].get("name").and_then(|v| v.as_str()), Some("idx_conv"));
    let columns = entries[0]
        .get("columns")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("index columns missing: {listed}"));
    assert_eq!(columns.len(), 1, "idx_conv should cover one column: {listed}");

    let dup = exec_err(&format!("CREATE INDEX idx_conv ON {full} (conversation_id)"));
    assert!(
        dup.to_ascii_lowercase().contains("already exists")
            || dup.to_ascii_lowercase().contains("exist"),
        "duplicate CREATE INDEX must fail cleanly: {dup}"
    );
    assert_index_named(&latest_indexes_json(&ns, &table), "idx_conv");

    let missing = exec_err(&format!("CREATE INDEX idx_bad ON {full} (column_does_not_exist)"));
    assert!(
        missing.to_ascii_lowercase().contains("does not exist")
            || missing.to_ascii_lowercase().contains("not found")
            || missing.to_ascii_lowercase().contains("unknown"),
        "missing column must fail: {missing}"
    );
    assert_index_absent(&latest_indexes_json(&ns, &table), "idx_bad");

    exec(&format!(
        "INSERT INTO {full} (id, conversation_id, body) VALUES (1, 'c1', 'ok')"
    ));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'c1'")),
        1
    );

    exec(&format!("ALTER TABLE {full} DROP INDEX idx_conv"));
    assert_index_absent(&latest_indexes_json(&ns, &table), "idx_conv");
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'c1'")),
        1,
        "query must still work after DROP INDEX"
    );

    let drop_missing = exec_err(&format!("ALTER TABLE {full} DROP INDEX idx_conv"));
    assert!(
        drop_missing.to_ascii_lowercase().contains("does not exist")
            || drop_missing.to_ascii_lowercase().contains("not found"),
        "dropping missing index must error: {drop_missing}"
    );

    exec(&format!("CREATE INDEX idx_conv ON {full} (conversation_id)"));
    assert_index_named(&latest_indexes_json(&ns, &table), "idx_conv");
}

#[ntest::timeout(180000)]
#[test]
fn kobj_index_equality_duplicates_update_delete() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_idxeq");
    let table = generate_unique_table("messages");
    let full = create_indexed_shared(&ns, &table, "conversation_id TEXT, status TEXT");
    exec(&format!("CREATE INDEX idx_conv ON {full} (conversation_id)"));
    exec(&format!("CREATE INDEX idx_status ON {full} (status)"));

    let mut values = String::new();
    for i in 1..=200 {
        if i > 1 {
            values.push_str(", ");
        }
        let conv = if i <= 20 { "target" } else { "other" };
        values.push_str(&format!("({i}, '{conv}', 'pending')"));
    }
    exec(&format!("INSERT INTO {full} (id, conversation_id, status) VALUES {values}"));

    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'target'")),
        20
    );
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'other'")),
        180
    );

    exec(&format!(
        "INSERT INTO {full} (id, conversation_id, status) VALUES (201, 'dup', 'active'), (202, \
         'dup', 'active'), (203, 'dup', 'inactive')"
    ));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'dup'")),
        3
    );
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'active'")), 2);

    exec(&format!("UPDATE {full} SET status = 'complete' WHERE id = 1"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE id = 1 AND status = 'pending'")),
        0
    );
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE id = 1 AND status = 'complete'")),
        1
    );

    exec(&format!("DELETE FROM {full} WHERE id = 201"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'dup'")),
        2
    );
    exec(&format!(
        "INSERT INTO {full} (id, conversation_id, status) VALUES (204, 'dup', 'active')"
    ));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'dup'")),
        3
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_index_repeated_updates_and_nulls() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_idxrep");
    let table = generate_unique_table("items");
    let full = create_indexed_shared(&ns, &table, "status TEXT, email TEXT");
    exec(&format!("CREATE INDEX idx_status ON {full} (status)"));
    exec(&format!("CREATE INDEX idx_email ON {full} (email)"));
    exec(&format!("INSERT INTO {full} (id, status, email) VALUES (1, 'A', NULL)"));

    let steps = ["B", "C", "D", "A"];
    let mut previous = "A";
    for _ in 0..8 {
        for next in steps {
            exec(&format!("UPDATE {full} SET status = '{next}' WHERE id = 1"));
            assert_eq!(
                count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = '{previous}'")),
                0
            );
            assert_eq!(
                count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = '{next}'")),
                1
            );
            previous = next;
        }
    }

    exec(&format!(
        "INSERT INTO {full} (id, status, email) VALUES (2, 'Z', NULL), (3, 'Z', 'a@example.com')"
    ));
    let null_count = count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE email IS NULL"));
    assert_eq!(null_count, 2, "NULL emails must be countable, not panic");
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE email = 'a@example.com'")),
        1
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_index_scalar_types_and_numeric_edges() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_idxtype");
    let table = generate_unique_table("typed");
    let full =
        create_indexed_shared(&ns, &table, "flag BOOLEAN, small INT, big BIGINT, label TEXT");
    exec(&format!("CREATE INDEX idx_flag ON {full} (flag)"));
    exec(&format!("CREATE INDEX idx_small ON {full} (small)"));
    exec(&format!("CREATE INDEX idx_big ON {full} (big)"));
    exec(&format!("CREATE INDEX idx_label ON {full} (label)"));

    exec(&format!(
        "INSERT INTO {full} (id, flag, small, big, label) VALUES
            (1, true, -1, -1, 'neg'),
            (2, false, 0, 0, 'zero'),
            (3, true, 1, 1, 'pos'),
            (4, false, -2147483648, -9223372036854775808, 'min'),
            (5, true, 2147483647, 9223372036854775807, 'max')"
    ));

    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE flag = true")), 3);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE small = 0")), 1);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE small = -2147483648")), 1);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE small = 2147483647")), 1);
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE big = -9223372036854775808")),
        1
    );
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE label = 'max'")), 1);

    exec(&format!("UPDATE {full} SET small = 9 WHERE id = 2"));
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE small = 0")), 0);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE small = 9")), 1);
    exec(&format!("DELETE FROM {full} WHERE id = 3"));
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE label = 'pos'")), 0);
}

#[ntest::timeout(180000)]
#[test]
fn kobj_index_backfill_and_hot_cold() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_idxbf");
    let table = generate_unique_table("rooms");
    let full = create_indexed_shared(&ns, &table, "conversation_id TEXT");

    let mut values = String::new();
    for i in 1..=200 {
        if i > 1 {
            values.push_str(", ");
        }
        let conv = if i % 10 == 0 { "hotpath" } else { "noise" };
        values.push_str(&format!("({i}, '{conv}')"));
    }
    exec(&format!("INSERT INTO {full} (id, conversation_id) VALUES {values}"));
    exec(&format!("CREATE INDEX idx_conv ON {full} (conversation_id)"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'hotpath'")),
        20
    );

    exec(&format!(
        "INSERT INTO {full} (id, conversation_id) VALUES (201, 'hotpath'), (202, 'hotpath')"
    ));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'hotpath'")),
        22
    );

    let before =
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'hotpath'"));
    flush_table(&full);
    let after_flush =
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'hotpath'"));
    assert_eq!(after_flush, before, "flush must not change indexed equality results");

    exec(&format!("INSERT INTO {full} (id, conversation_id) VALUES (203, 'hotpath')"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'hotpath'")),
        before + 1,
        "hot+cold merge must include new hot rows without duplicating cold"
    );

    exec(&format!("DELETE FROM {full} WHERE id = 10"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE id = 10")),
        0,
        "hot tombstone must not resurrect a flushed row"
    );
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'hotpath'")),
        before,
        "deleted cold row must drop out of the indexed lookup"
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_create_index_after_cold_data_remains_sql_correct() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_idxcold");
    let table = generate_unique_table("rooms");
    let full = create_indexed_shared(&ns, &table, "conversation_id TEXT");
    exec(&format!(
        "INSERT INTO {full} (id, conversation_id) VALUES (1, 'c1'), (2, 'c1'), (3, 'c2')"
    ));
    flush_table(&full);
    exec(&format!("CREATE INDEX idx_conv ON {full} (conversation_id)"));
    assert_eq!(
        count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE conversation_id = 'c1'")),
        2,
        "CREATE INDEX after flush must still return cold rows via SQL (index or cold scan)"
    );
}

#[ntest::timeout(180000)]
#[test]
fn kobj_failed_writes_leave_row_index_catalog_consistent() {
    if skip_if_no_server() {
        return;
    }
    let ns = setup_namespace("kobj_fail");
    let table = generate_unique_table("items");
    let full = create_indexed_shared(&ns, &table, "status TEXT");
    exec(&format!("CREATE INDEX idx_status ON {full} (status)"));
    exec(&format!("INSERT INTO {full} (id, status) VALUES (1, 'ok')"));

    let dup = exec_err(&format!("INSERT INTO {full} (id, status) VALUES (1, 'dup')"));
    assert!(
        dup.to_ascii_lowercase().contains("exist")
            || dup.to_ascii_lowercase().contains("duplicate")
            || dup.to_ascii_lowercase().contains("conflict")
            || dup.to_ascii_lowercase().contains("unique"),
        "duplicate PK should fail: {dup}"
    );
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full}")), 1);
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'ok'")), 1);
    assert_index_named(&latest_indexes_json(&ns, &table), "idx_status");

    let bad_type = exec_err(&format!("INSERT INTO {full} (id, status) VALUES ('not-int', 'x')"));
    assert!(!bad_type.is_empty());
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full}")), 1);

    let batch = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {full} (id, status) VALUES (2, 'a'), (3, 'b'), (1, 'again'), (4, 'c')"
    ));
    match batch {
        Ok(_) => {
            panic!("batch insert containing a duplicate PK should not succeed silently");
        },
        Err(_) => {
            let count = count_sql(&format!("SELECT COUNT(*) FROM {full}"));
            assert!(
                count == 1 || count == 4,
                "batch partial-failure must be atomic rollback (1) or fully applied except the \
                 conflict; got {count}"
            );
            if count == 1 {
                assert_eq!(
                    count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'a'")),
                    0
                );
            }
        },
    }

    exec(&format!("INSERT INTO {full} (id, status) VALUES (9, 'healthy')"));
    assert_eq!(count_sql(&format!("SELECT COUNT(*) FROM {full} WHERE status = 'healthy'")), 1);
}
