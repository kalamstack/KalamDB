use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{
        rows::Row,
        schemas::{
            ColumnDefinition, ScalarIndexDefinition, TableDefinition, TableOptions, TableType,
        },
        ColumnId,
    },
    BoundExprShape, NamespaceId, PolicyId, PolicyProgram, PrincipalExpr, Role, TableId, TableName,
    UserId,
};
use kalamdb_core::{
    sql::{
        context::ExecutionContext,
        executor::{handler_registry::HandlerRegistry, handlers::TypedStatementHandler},
        SqlExecutor,
    },
    test_helpers::{create_test_session_simple, test_app_context_simple},
};
use kalamdb_sql::ddl::{AlterPolicyStatement, CreatePolicyStatement, DropPolicyStatement};
use kalamdb_tables::{
    utils::{BaseTableProvider, KalamTableProvider},
    SharedTableProvider,
};

use super::{AlterPolicyHandler, CreatePolicyHandler, DropPolicyHandler};

fn with_user_id_index(mut table: TableDefinition) -> TableDefinition {
    table.scalar_indexes.push(ScalarIndexDefinition::new(
        format!("{}_user_id", table.table_name.as_str()),
        vec![ColumnId::new(2)],
        false,
    ));
    table
}

fn shared_table(name: &str) -> TableDefinition {
    TableDefinition::new(
        NamespaceId::new("chat"),
        TableName::new(name),
        TableType::Shared,
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "owner_id", 2, KalamDataType::Text),
        ],
        TableOptions::shared(),
        None,
    )
    .expect("valid test table")
}

fn execution_context(role: Role) -> ExecutionContext {
    ExecutionContext::new(UserId::new("policy_admin"), role, create_test_session_simple())
}

fn app_execution_context(
    app_context: &std::sync::Arc<kalamdb_core::app_context::AppContext>,
    user_id: &str,
    role: Role,
) -> ExecutionContext {
    ExecutionContext::new(UserId::new(user_id), role, app_context.base_session_context())
}

#[tokio::test]
async fn create_policy_compiles_and_persists_definition() {
    let app_context = test_app_context_simple();
    let definition = shared_table("documents");
    let table_id = TableId::new(definition.namespace_id.clone(), definition.table_name.clone());
    app_context
        .schema_registry()
        .register_table(definition)
        .expect("register table");

    let statement = CreatePolicyStatement::parse(
        "CREATE POLICY owner_read ON chat.documents FOR SELECT TO user USING (owner_id = \
         CURRENT_USER)",
        &NamespaceId::new("chat"),
    )
    .expect("parse policy");
    let handler = CreatePolicyHandler::new(app_context.clone());

    handler
        .execute(statement, Vec::new(), &execution_context(Role::Service))
        .await
        .expect("create policy");

    let policy_id = PolicyId::new(table_id, "owner_read").expect("valid id");
    let policy = app_context
        .system_tables()
        .table_policies()
        .get_policy(&policy_id)
        .await
        .expect("read policy")
        .expect("persisted policy");
    assert_eq!(policy.schema_generation, 1);
    assert_eq!(
        policy.using_program,
        Some(PolicyProgram::RowLocal {
            expr: BoundExprShape::ColumnEqualsPrincipal {
                column_id: 2,
                principal: PrincipalExpr::CurrentUser,
            },
        })
    );
}

#[tokio::test]
async fn alter_recompiles_and_drop_removes_policy() {
    let app_context = test_app_context_simple();
    let definition = shared_table("documents_mutation");
    let table_id = TableId::new(definition.namespace_id.clone(), definition.table_name.clone());
    app_context
        .schema_registry()
        .register_table(definition)
        .expect("register table");
    let context = execution_context(Role::Dba);

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_mutation FOR SELECT USING (true)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &context,
        )
        .await
        .expect("create policy");

    AlterPolicyHandler::new(app_context.clone())
        .execute(
            AlterPolicyStatement::parse(
                "ALTER POLICY owner_read ON chat.documents_mutation USING (owner_id = \
                 CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &context,
        )
        .await
        .expect("alter policy");

    let policy_id = PolicyId::new(table_id.clone(), "owner_read").unwrap();
    let altered = app_context
        .system_tables()
        .table_policies()
        .get_policy(&policy_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(altered.policy_generation, 2);
    assert!(matches!(
        altered.using_program,
        Some(PolicyProgram::RowLocal {
            expr: BoundExprShape::ColumnEqualsPrincipal { column_id: 2, .. },
        })
    ));

    DropPolicyHandler::new(app_context.clone())
        .execute(
            DropPolicyStatement::parse(
                "DROP POLICY owner_read ON chat.documents_mutation",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &context,
        )
        .await
        .expect("drop policy");
    assert!(app_context
        .system_tables()
        .table_policies()
        .get_policy(&policy_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn shared_scan_default_denies_and_filters_post_bind() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_scan");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .expect("add system columns");
    app_context
        .schema_registry()
        .register_table(definition)
        .expect("register table");
    let table_id = TableId::from_strings("chat", "documents_scan");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .expect("shared provider");
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-b".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("bob".to_string()))),
                ]),
            ],
        )
        .await
        .expect("seed rows");

    let alice = app_execution_context(&app_context, "alice", Role::User);
    let session = alice.create_session_with_user();
    let before = session
        .sql("SELECT id FROM chat.documents_scan")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(before.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_scan FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .expect("create policy");

    let after = session
        .sql("SELECT id FROM chat.documents_scan")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(after.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);

    let explain = session
        .sql("EXPLAIN SELECT id FROM chat.documents_scan")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let explain_text = format!("{explain:?}");
    assert!(
        explain_text.contains("RlsAuthorization strategy=RowLocal"),
        "EXPLAIN must show the row-local RLS strategy, got {explain_text}"
    );
    assert!(
        explain_text.contains("policies=[owner_read FOR SELECT USING (owner_id = CURRENT_USER)]"),
        "EXPLAIN must list the bound policy and USING qual like PostgreSQL security quals, got \
         {explain_text}"
    );
    assert!(
        !explain_text.contains("alice") && !explain_text.contains("doc-a"),
        "EXPLAIN must not list the bound principal or row keys, got {explain_text}"
    );
}

#[tokio::test]
async fn membership_rls_runs_after_mvcc_winner_selection() {
    let app_context = test_app_context_simple();
    let mut messages = TableDefinition::new(
        NamespaceId::new("chat"),
        TableName::new("mvcc_messages"),
        TableType::Shared,
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "group_id", 2, KalamDataType::Text),
        ],
        TableOptions::shared(),
        None,
    )
    .unwrap();
    let mut members = with_user_id_index(
        TableDefinition::new(
            NamespaceId::new("chat"),
            TableName::new("mvcc_members"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "user_id", 2, KalamDataType::Text),
                ColumnDefinition::simple(3, "group_id", 3, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .unwrap(),
    );
    app_context.system_columns_service().add_system_columns(&mut messages).unwrap();
    app_context.system_columns_service().add_system_columns(&mut members).unwrap();
    app_context.schema_registry().register_table(messages).unwrap();
    app_context.schema_registry().register_table(members).unwrap();

    let messages_id = TableId::from_strings("chat", "mvcc_messages");
    let members_id = TableId::from_strings("chat", "mvcc_members");
    let messages_provider = app_context.schema_registry().get_provider(&messages_id).unwrap();
    let messages_provider = (messages_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let members_provider = app_context.schema_registry().get_provider(&members_id).unwrap();
    let members_provider = (members_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let system = UserId::new("system");
    members_provider
        .insert_rows(
            &system,
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Utf8(Some("membership-1".to_string()))),
                ("user_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ("group_id".to_string(), ScalarValue::Utf8(Some("A".to_string()))),
            ])],
        )
        .await
        .unwrap();
    messages_provider
        .insert_rows(
            &system,
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Utf8(Some("message-1".to_string()))),
                ("group_id".to_string(), ScalarValue::Utf8(Some("A".to_string()))),
            ])],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY member_read ON chat.mvcc_messages FOR SELECT TO user USING \
                 (group_id IN (SELECT group_id FROM chat.mvcc_members WHERE user_id = \
                 CURRENT_USER))",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let alice = app_execution_context(&app_context, "alice", Role::User);
    let session = alice.create_session_with_user();
    let visible = session
        .sql("SELECT id FROM chat.mvcc_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(visible.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    let explain = session
        .sql("EXPLAIN SELECT id FROM chat.mvcc_messages WHERE group_id = 'A'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let explain_text = format!("{explain:?}");
    assert!(
        explain_text.contains("strategy=PointGuard"),
        "EXPLAIN must disclose the cached safe authorization strategy without listing keys, got \
         {explain_text}"
    );
    assert!(
        explain_text.contains("policies=[member_read FOR SELECT USING"),
        "EXPLAIN must name the bound policy, got {explain_text}"
    );
    assert!(
        explain_text.contains("USING (group_id IN") && explain_text.contains("CURRENT_USER"),
        "EXPLAIN must include the USING qual like PostgreSQL security quals, got {explain_text}"
    );
    assert!(
        !explain_text.contains("membership-1") && !explain_text.contains("alice"),
        "EXPLAIN must not list authorization keys or principals, got {explain_text}"
    );
    let broad_explain = session
        .sql("EXPLAIN SELECT id FROM chat.mvcc_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let broad_text = format!("{broad_explain:?}");
    assert!(
        broad_text.contains("strategy=CachedAuthorizationSet"),
        "EXPLAIN must show the cached authorization-set strategy for broad membership scans, got \
         {broad_text}"
    );
    assert!(
        broad_text.contains("policies=[member_read FOR SELECT USING"),
        "broad EXPLAIN must still list the bound policy, got {broad_text}"
    );
    assert!(
        !broad_text.contains("HashJoinExec"),
        "broad membership scans must not rely on a duplicate DataFusion semi-join, got \
         {broad_text}"
    );

    let metrics_after_build = messages_provider.authorization_cache_metrics();
    let visible_from_cache = session
        .sql("SELECT id FROM chat.mvcc_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(visible_from_cache.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert!(
        messages_provider.authorization_cache_metrics().hits > metrics_after_build.hits,
        "the second authorization should reuse the principal-scoped set"
    );

    let misses_before_revoke = messages_provider.authorization_cache_metrics().misses;
    members_provider
        .update_row_by_pk(
            &system,
            "membership-1",
            Row::from_vec(vec![(
                "user_id".to_string(),
                ScalarValue::Utf8(Some("bob".to_string())),
            )]),
        )
        .await
        .unwrap();
    assert!(members_provider
        .patch_latest_commit_seq_by_pk("membership-1", 9_000)
        .await
        .unwrap());
    let revoked = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM chat.mvcc_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(revoked.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);
    assert!(messages_provider.authorization_cache_metrics().misses > misses_before_revoke);

    members_provider
        .update_row_by_pk(
            &system,
            "membership-1",
            Row::from_vec(vec![(
                "user_id".to_string(),
                ScalarValue::Utf8(Some("alice".to_string())),
            )]),
        )
        .await
        .unwrap();
    assert!(members_provider
        .patch_latest_commit_seq_by_pk("membership-1", 9_500)
        .await
        .unwrap());
    let granted_again = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM chat.mvcc_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(granted_again.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);

    messages_provider
        .update_row_by_pk(
            &system,
            "message-1",
            Row::from_vec(vec![("group_id".to_string(), ScalarValue::Utf8(Some("B".to_string())))]),
        )
        .await
        .unwrap();
    assert!(messages_provider
        .patch_latest_commit_seq_by_pk("message-1", 10_000)
        .await
        .unwrap());
    let latest = messages_provider
        .find_by_pk(&ScalarValue::Utf8(Some("message-1".to_string())))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.1.fields.get("group_id"), Some(&ScalarValue::Utf8(Some("B".to_string()))));
    let resolved = messages_provider
        .scan_with_version_resolution_to_kvs_async(&system, None, None, None, false, None, None)
        .await
        .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(
        resolved[0].1.fields.get("group_id"),
        Some(&ScalarValue::Utf8(Some("B".to_string())))
    );

    let session_after =
        app_execution_context(&app_context, "alice", Role::User).create_session_with_user();
    let hidden = session_after
        .sql("SELECT id, group_id FROM chat.mvcc_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        hidden.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0,
        "unexpected visible batches: {hidden:?}"
    );
}

#[tokio::test]
async fn shared_dml_applies_using_and_with_check_atomically() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_dml");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .expect("add system columns");
    app_context
        .schema_registry()
        .register_table(definition)
        .expect("register table");

    let admin = app_execution_context(&app_context, "admin", Role::Dba);
    for sql in [
        "CREATE POLICY owner_select ON chat.documents_dml FOR SELECT TO user USING (owner_id = \
         CURRENT_USER)",
        "CREATE POLICY owner_insert ON chat.documents_dml FOR INSERT TO user WITH CHECK (owner_id \
         = CURRENT_USER)",
        "CREATE POLICY owner_update ON chat.documents_dml FOR UPDATE TO user USING (owner_id = \
         CURRENT_USER) WITH CHECK (owner_id = CURRENT_USER)",
        "CREATE POLICY owner_delete ON chat.documents_dml FOR DELETE TO user USING (owner_id = \
         CURRENT_USER)",
    ] {
        CreatePolicyHandler::new(app_context.clone())
            .execute(
                CreatePolicyStatement::parse(sql, &NamespaceId::new("chat")).unwrap(),
                Vec::new(),
                &admin,
            )
            .await
            .expect("create DML policy");
    }

    let alice = app_execution_context(&app_context, "alice", Role::User);
    let alice_session = alice.create_session_with_user();
    let batch_error = alice_session
        .sql(
            "INSERT INTO chat.documents_dml (id, owner_id) VALUES ('alice-ok', 'alice'), \
             ('bob-denied', 'bob')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("one denied row rejects the entire batch");
    assert!(batch_error.to_string().contains("WITH CHECK"));

    let admin_session = admin.create_session_with_user();
    let after_failed_batch = admin_session
        .sql("SELECT id FROM chat.documents_dml")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(after_failed_batch.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);

    alice_session
        .sql("INSERT INTO chat.documents_dml (id, owner_id) VALUES ('alice-ok', 'alice')")
        .await
        .unwrap()
        .collect()
        .await
        .expect("authorized insert");

    let update_error = alice_session
        .sql("UPDATE chat.documents_dml SET owner_id = 'bob' WHERE id = 'alice-ok'")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("new row must satisfy WITH CHECK");
    assert!(update_error.to_string().contains("WITH CHECK"));

    let still_visible = alice_session
        .sql("SELECT id FROM chat.documents_dml WHERE id = 'alice-ok'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(still_visible.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);

    let bob = app_execution_context(&app_context, "bob", Role::User);
    let bob_delete = bob
        .create_session_with_user()
        .sql("DELETE FROM chat.documents_dml WHERE id = 'alice-ok'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(bob_delete.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    let after_bob_delete = alice_session
        .sql("SELECT id FROM chat.documents_dml WHERE id = 'alice-ok'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(after_bob_delete.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);

    alice_session
        .sql("DELETE FROM chat.documents_dml WHERE id = 'alice-ok'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("authorized delete");
    let after_alice_delete = admin_session
        .sql("SELECT id FROM chat.documents_dml")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(after_alice_delete.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn omitted_policy_stays_default_deny_for_users() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_default_deny");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition.clone()).unwrap();

    let table_id = TableId::from_strings("chat", "documents_default_deny");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
            ])],
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM chat.documents_default_deny")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(visible.iter().map(|batch| batch.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn client_or_true_cannot_bypass_row_local_rls() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_bypass");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_bypass");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-b".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("bob".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_bypass FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM chat.documents_bypass WHERE owner_id = 'bob' OR true")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(visible.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn nested_query_cannot_bypass_row_local_rls() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_nested");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_nested");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-b".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("bob".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_nested FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM (SELECT * FROM chat.documents_nested) nested")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(visible.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}

fn row_count(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::test]
async fn plan_cache_binds_current_user_after_lookup() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_plan_cache");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_plan_cache");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-b".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("bob".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_plan_cache FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let sql = "SELECT id FROM chat.documents_plan_cache";
    let alice = app_execution_context(&app_context, "alice", Role::User).create_session_with_user();
    let bob = app_execution_context(&app_context, "bob", Role::User).create_session_with_user();

    let alice_first = alice.sql(sql).await.unwrap().collect().await.unwrap();
    let bob_rows = bob.sql(sql).await.unwrap().collect().await.unwrap();
    let alice_second = alice.sql(sql).await.unwrap().collect().await.unwrap();

    assert_eq!(row_count(&alice_first), 1);
    assert_eq!(row_count(&bob_rows), 1);
    assert_eq!(row_count(&alice_second), 1);
    assert_eq!(format!("{alice_first:?}"), format!("{alice_second:?}"));
    assert_ne!(format!("{alice_first:?}"), format!("{bob_rows:?}"));
}

#[tokio::test]
async fn user_on_conflict_on_shared_tables_is_rejected() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_on_conflict");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_write ON chat.documents_on_conflict FOR ALL TO user USING \
                 (owner_id = CURRENT_USER) WITH CHECK (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let executor =
        SqlExecutor::new(app_context.clone(), std::sync::Arc::new(HandlerRegistry::new()));
    let error = executor
        .execute(
            "INSERT INTO chat.documents_on_conflict (id, owner_id) VALUES ('doc-a', 'alice') ON \
             CONFLICT (id) DO UPDATE SET owner_id = EXCLUDED.owner_id",
            &app_execution_context(&app_context, "alice", Role::User),
            Vec::new(),
        )
        .await
        .expect_err("User ON CONFLICT must not bypass shared-table RLS");
    assert!(
        error.to_string().contains("ON CONFLICT"),
        "expected ON CONFLICT rejection, got {error}"
    );
}

#[tokio::test]
async fn live_authorization_fail_closes_when_policy_catalog_changes() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_live_revoke");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_live_revoke");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let alice_row = Row::from_vec(vec![
        ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
        ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
    ]);
    provider
        .insert_rows(&UserId::new("system"), vec![alice_row.clone()])
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_live_revoke FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let bound = provider
        .bind_live_authorization(&UserId::new("alice"), Role::User)
        .await
        .unwrap();
    assert!(bound.authorizes(&alice_row));

    DropPolicyHandler::new(app_context.clone())
        .execute(
            DropPolicyStatement::parse(
                "DROP POLICY owner_read ON chat.documents_live_revoke",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    assert!(
        !bound.authorizes(&alice_row),
        "DROP POLICY must fail closed for already-bound live subscriptions"
    );

    let rebound = provider
        .bind_live_authorization(&UserId::new("alice"), Role::User)
        .await
        .unwrap();
    assert!(!rebound.authorizes(&alice_row));
}

#[tokio::test]
async fn live_authorization_does_not_pick_up_grants_until_rebind() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_live_grant");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_live_grant");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let alice_row = Row::from_vec(vec![
        ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
        ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
    ]);
    provider
        .insert_rows(&UserId::new("system"), vec![alice_row.clone()])
        .await
        .unwrap();

    let bound = provider
        .bind_live_authorization(&UserId::new("alice"), Role::User)
        .await
        .unwrap();
    assert!(!bound.authorizes(&alice_row));

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_live_grant FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    assert!(
        !bound.authorizes(&alice_row),
        "CREATE POLICY must not grant already-bound live subscriptions"
    );

    let rebound = provider
        .bind_live_authorization(&UserId::new("alice"), Role::User)
        .await
        .unwrap();
    assert!(rebound.authorizes(&alice_row));
}

#[tokio::test]
async fn rejects_unbounded_not_owner_policy() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_null_owner");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_null_owner");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-null".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(None)),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-bob".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("bob".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    let error = CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY not_owner_read ON chat.documents_null_owner FOR SELECT TO user \
                 USING (NOT (owner_id = CURRENT_USER))",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .expect_err("unbounded NOT policy must be rejected");
    assert!(error.to_string().contains("indexed live routing"));
}

#[tokio::test]
async fn alter_rejects_unbounded_not_policy_for_indexed_live_routing() {
    let app_context = test_app_context_simple();
    let definition = shared_table("documents_alter_not");
    app_context
        .schema_registry()
        .register_table(definition)
        .expect("register table");
    let context = execution_context(Role::Dba);

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_alter_not FOR SELECT USING (owner_id \
                 = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &context,
        )
        .await
        .expect("create policy");

    let error = AlterPolicyHandler::new(app_context)
        .execute(
            AlterPolicyStatement::parse(
                "ALTER POLICY owner_read ON chat.documents_alter_not USING (NOT (owner_id = \
                 CURRENT_USER))",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &context,
        )
        .await
        .expect_err("ALTER POLICY must reject unbounded NOT");
    assert!(error.to_string().contains("indexed live routing"));
}

#[tokio::test]
async fn membership_policy_hides_rows_with_null_join_key() {
    let app_context = test_app_context_simple();
    let mut messages = TableDefinition::new(
        NamespaceId::new("chat"),
        TableName::new("null_key_messages"),
        TableType::Shared,
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "group_id", 2, KalamDataType::Text),
        ],
        TableOptions::shared(),
        None,
    )
    .unwrap();
    let mut members = with_user_id_index(
        TableDefinition::new(
            NamespaceId::new("chat"),
            TableName::new("null_key_members"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "user_id", 2, KalamDataType::Text),
                ColumnDefinition::simple(3, "group_id", 3, KalamDataType::Text),
                ColumnDefinition::simple(4, "status", 4, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .unwrap(),
    );
    app_context.system_columns_service().add_system_columns(&mut messages).unwrap();
    app_context.system_columns_service().add_system_columns(&mut members).unwrap();
    app_context.schema_registry().register_table(messages).unwrap();
    app_context.schema_registry().register_table(members).unwrap();

    let messages_id = TableId::from_strings("chat", "null_key_messages");
    let members_id = TableId::from_strings("chat", "null_key_members");
    let messages_provider = app_context.schema_registry().get_provider(&messages_id).unwrap();
    let messages_provider = (messages_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let members_provider = app_context.schema_registry().get_provider(&members_id).unwrap();
    let members_provider = (members_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let system = UserId::new("system");
    members_provider
        .insert_rows(
            &system,
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("m-null".to_string()))),
                    ("user_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(None)),
                    ("status".to_string(), ScalarValue::Utf8(Some("active".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("m-ok".to_string()))),
                    ("user_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(Some("group-a".to_string()))),
                    ("status".to_string(), ScalarValue::Utf8(Some("active".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();
    messages_provider
        .insert_rows(
            &system,
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("msg-null".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(None)),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("msg-ok".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(Some("group-a".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY member_read ON chat.null_key_messages FOR SELECT TO user USING \
                 (group_id IN (SELECT group_id FROM chat.null_key_members WHERE user_id = \
                 CURRENT_USER AND status <> 'blocked'))",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM chat.null_key_messages ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(row_count(&visible), 1);
}

#[tokio::test]
async fn union_cannot_bypass_row_local_rls() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_union");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_union");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("doc-b".to_string()))),
                    ("owner_id".to_string(), ScalarValue::Utf8(Some("bob".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_union FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql(
            "SELECT id FROM chat.documents_union WHERE owner_id = 'bob' UNION SELECT id FROM \
             chat.documents_union",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(row_count(&visible), 1);
}

#[tokio::test]
async fn live_authorization_fails_closed_when_membership_is_revoked() {
    let app_context = test_app_context_simple();
    let mut messages = TableDefinition::new(
        NamespaceId::new("chat"),
        TableName::new("live_membership_messages"),
        TableType::Shared,
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "group_id", 2, KalamDataType::Text),
        ],
        TableOptions::shared(),
        None,
    )
    .unwrap();
    let mut members = with_user_id_index(
        TableDefinition::new(
            NamespaceId::new("chat"),
            TableName::new("live_membership_members"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "user_id", 2, KalamDataType::Text),
                ColumnDefinition::simple(3, "group_id", 3, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .unwrap(),
    );
    app_context.system_columns_service().add_system_columns(&mut messages).unwrap();
    app_context.system_columns_service().add_system_columns(&mut members).unwrap();
    app_context.schema_registry().register_table(messages).unwrap();
    app_context.schema_registry().register_table(members).unwrap();

    let messages_id = TableId::from_strings("chat", "live_membership_messages");
    let members_id = TableId::from_strings("chat", "live_membership_members");
    let messages_provider = app_context.schema_registry().get_provider(&messages_id).unwrap();
    let messages_provider = (messages_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let members_provider = app_context.schema_registry().get_provider(&members_id).unwrap();
    let members_provider = (members_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let system = UserId::new("system");
    members_provider
        .insert_rows(
            &system,
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Utf8(Some("membership-1".to_string()))),
                ("user_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
                ("group_id".to_string(), ScalarValue::Utf8(Some("group-a".to_string()))),
            ])],
        )
        .await
        .unwrap();
    let message_row = Row::from_vec(vec![
        ("id".to_string(), ScalarValue::Utf8(Some("message-1".to_string()))),
        ("group_id".to_string(), ScalarValue::Utf8(Some("group-a".to_string()))),
    ]);
    messages_provider.insert_rows(&system, vec![message_row.clone()]).await.unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY member_read ON chat.live_membership_messages FOR SELECT TO user \
                 USING (group_id IN (SELECT group_id FROM chat.live_membership_members WHERE \
                 user_id = CURRENT_USER))",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let bound = messages_provider
        .bind_live_authorization(&UserId::new("alice"), Role::User)
        .await
        .unwrap();
    assert!(bound.authorizes(&message_row));

    members_provider.delete_row_by_pk(&system, "membership-1").await.unwrap();

    assert!(
        !bound.authorizes(&message_row),
        "membership revocation must fail closed for already-bound live subscriptions"
    );
}

#[tokio::test]
async fn service_role_does_not_inherit_user_targeted_policies() {
    let app_context = test_app_context_simple();
    let mut definition = shared_table("documents_service_role");
    app_context
        .system_columns_service()
        .add_system_columns(&mut definition)
        .unwrap();
    app_context.schema_registry().register_table(definition).unwrap();
    let table_id = TableId::from_strings("chat", "documents_service_role");
    let provider = app_context.schema_registry().get_provider(&table_id).unwrap();
    let provider = (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    provider
        .insert_rows(
            &UserId::new("system"),
            vec![Row::from_vec(vec![
                ("id".to_string(), ScalarValue::Utf8(Some("doc-a".to_string()))),
                ("owner_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
            ])],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY owner_read ON chat.documents_service_role FOR SELECT TO user USING \
                 (owner_id = CURRENT_USER)",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "svc", Role::Service)
        .create_session_with_user()
        .sql("SELECT id FROM chat.documents_service_role")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(row_count(&visible), 0);
}

#[tokio::test]
async fn membership_bind_uses_indexed_principal_among_many_rows() {
    let app_context = test_app_context_simple();
    let mut messages = TableDefinition::new(
        NamespaceId::new("chat"),
        TableName::new("scaled_messages"),
        TableType::Shared,
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
            ColumnDefinition::simple(2, "group_id", 2, KalamDataType::Text),
        ],
        TableOptions::shared(),
        None,
    )
    .unwrap();
    let mut members = with_user_id_index(
        TableDefinition::new(
            NamespaceId::new("chat"),
            TableName::new("scaled_members"),
            TableType::Shared,
            vec![
                ColumnDefinition::primary_key(1, "id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "user_id", 2, KalamDataType::Text),
                ColumnDefinition::simple(3, "group_id", 3, KalamDataType::Text),
            ],
            TableOptions::shared(),
            None,
        )
        .unwrap(),
    );
    app_context.system_columns_service().add_system_columns(&mut messages).unwrap();
    app_context.system_columns_service().add_system_columns(&mut members).unwrap();
    app_context.schema_registry().register_table(messages).unwrap();
    app_context.schema_registry().register_table(members).unwrap();

    let messages_id = TableId::from_strings("chat", "scaled_messages");
    let members_id = TableId::from_strings("chat", "scaled_members");
    let messages_provider = app_context.schema_registry().get_provider(&messages_id).unwrap();
    let messages_provider = (messages_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let members_provider = app_context.schema_registry().get_provider(&members_id).unwrap();
    let members_provider = (members_provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .unwrap();
    let system = UserId::new("system");

    let mut member_rows = Vec::with_capacity(1002);
    for i in 0..1000 {
        member_rows.push(Row::from_vec(vec![
            ("id".to_string(), ScalarValue::Utf8(Some(format!("other-{i}")))),
            ("user_id".to_string(), ScalarValue::Utf8(Some(format!("user-{i}")))),
            ("group_id".to_string(), ScalarValue::Utf8(Some(format!("G{i}")))),
        ]));
    }
    member_rows.push(Row::from_vec(vec![
        ("id".to_string(), ScalarValue::Utf8(Some("alice-1".to_string()))),
        ("user_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
        ("group_id".to_string(), ScalarValue::Utf8(Some("A".to_string()))),
    ]));
    member_rows.push(Row::from_vec(vec![
        ("id".to_string(), ScalarValue::Utf8(Some("alice-2".to_string()))),
        ("user_id".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
        ("group_id".to_string(), ScalarValue::Utf8(Some("B".to_string()))),
    ]));
    members_provider.insert_rows(&system, member_rows).await.unwrap();

    messages_provider
        .insert_rows(
            &system,
            vec![
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("msg-a".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(Some("A".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("msg-b".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(Some("B".to_string()))),
                ]),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("msg-other".to_string()))),
                    ("group_id".to_string(), ScalarValue::Utf8(Some("G0".to_string()))),
                ]),
            ],
        )
        .await
        .unwrap();

    CreatePolicyHandler::new(app_context.clone())
        .execute(
            CreatePolicyStatement::parse(
                "CREATE POLICY member_read ON chat.scaled_messages FOR SELECT TO user USING \
                 (group_id IN (SELECT group_id FROM chat.scaled_members WHERE user_id = \
                 CURRENT_USER))",
                &NamespaceId::new("chat"),
            )
            .unwrap(),
            Vec::new(),
            &app_execution_context(&app_context, "admin", Role::Dba),
        )
        .await
        .unwrap();

    let visible = app_execution_context(&app_context, "alice", Role::User)
        .create_session_with_user()
        .sql("SELECT id FROM chat.scaled_messages")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(row_count(&visible), 2);
}
