use super::common::{
    count_rows, create_shared_foreign_table_in_schema, create_user_foreign_table_in_schema,
    drop_foreign_tables, set_user_id, unique_name, TestEnv,
};

#[tokio::test]
#[ntest::timeout(45000)]
async fn e2e_scenario_chat_app_support_workspace_flow() {
    let env = TestEnv::global().await;
    let schema = unique_name("chat_app");
    let rooms = unique_name("rooms");
    let messages = unique_name("messages");
    let drafts = unique_name("drafts");

    let pg_admin = env.pg_connect().await;
    let pg_alice = env.pg_connect().await;
    let pg_bob = env.pg_connect().await;

    create_shared_foreign_table_in_schema(
        &pg_admin,
        &schema,
        &rooms,
        "id TEXT, workspace_id TEXT, title TEXT, channel_kind TEXT, owner_id TEXT",
    )
    .await;
    create_shared_foreign_table_in_schema(
        &pg_admin,
        &schema,
        &messages,
        "id TEXT, room_id TEXT, sender_id TEXT, body TEXT, sentiment TEXT, token_count INTEGER",
    )
    .await;
    create_user_foreign_table_in_schema(
        &pg_admin,
        &schema,
        &drafts,
        "id TEXT, room_id TEXT, body TEXT, last_model TEXT",
    )
    .await;

    set_user_id(&pg_alice, "chat-alice").await;
    set_user_id(&pg_bob, "chat-bob").await;

    pg_admin
        .batch_execute(&format!(
            "INSERT INTO {schema}.{rooms} (id, workspace_id, title, channel_kind, owner_id) VALUES
             ('room-billing', 'acme', 'billing-escalations', 'support', 'ops-1'),
             ('room-retain', 'acme', 'retention-playbooks', 'internal', 'ops-2');"
        ))
        .await
        .expect("seed chat rooms");

    pg_admin
        .batch_execute(&format!(
            "INSERT INTO {schema}.{messages} (id, room_id, sender_id, body, sentiment, token_count) VALUES
             ('m1', 'room-billing', 'cust-100', 'My invoice is wrong and I need a refund today', 'negative', 11),
             ('m2', 'room-billing', 'agent-7', 'I can review the invoice and open a credit ticket', 'neutral', 12),
             ('m3', 'room-billing', 'cust-100', 'Please escalate this before renewal', 'negative', 6),
             ('m4', 'room-retain', 'lead-4', 'Offer annual discount when renewal risk is high', 'positive', 9);"
        ))
        .await
        .expect("seed chat messages");

    pg_alice
        .execute(
            &format!("INSERT INTO {schema}.{drafts} (id, room_id, body, last_model) VALUES ($1, $2, $3, $4)"),
            &[&"draft-a1", &"room-billing", &"Prepared refund explanation with invoice steps", &"gpt-5.4"],
        )
        .await
        .expect("insert alice draft");
    pg_bob
        .execute(
            &format!("INSERT INTO {schema}.{drafts} (id, room_id, body, last_model) VALUES ($1, $2, $3, $4)"),
            &[&"draft-b1", &"room-retain", &"Drafted renewal outreach for risk account", &"gpt-5.4"],
        )
        .await
        .expect("insert bob draft");

    let analytics = pg_admin
        .query(
            &format!(
                "SELECT r.title, COUNT(m.id) AS message_count, SUM(m.token_count) AS total_tokens
                 FROM {schema}.{rooms} AS r
                 JOIN {schema}.{messages} AS m ON m.room_id = r.id
                 GROUP BY r.title
                 ORDER BY message_count DESC, r.title"
            ),
            &[],
        )
        .await
        .expect("chat analytics query");

    assert_eq!(analytics.len(), 2, "expected analytics for two chat rooms");
    let top_room: &str = analytics[0].get(0);
    let top_count: i64 = analytics[0].get(1);
    let top_tokens: i64 = analytics[0].get(2);
    assert_eq!(top_room, "billing-escalations");
    assert_eq!(top_count, 3);
    assert_eq!(top_tokens, 29);

    let alice_drafts = count_rows(&pg_alice, &format!("{schema}.{drafts}"), None).await;
    let bob_drafts = count_rows(&pg_bob, &format!("{schema}.{drafts}"), None).await;
    assert_eq!(alice_drafts, 1, "alice should see exactly one personal draft");
    assert_eq!(bob_drafts, 1, "bob should see exactly one personal draft");

    let escalation_feed = env
        .kalamdb_sql(&format!(
            "SELECT sender_id, body FROM {schema}.{messages} WHERE room_id = 'room-billing' ORDER BY id"
        ))
        .await;
    let escalation_feed_text = serde_json::to_string(&escalation_feed).unwrap_or_default();
    assert!(
        escalation_feed_text.contains("refund") && escalation_feed_text.contains("renewal"),
        "chat scenario should mirror escalation messages into KalamDB: {escalation_feed_text}"
    );

    drop_foreign_tables(&pg_admin, &schema, &[rooms, messages, drafts]).await;
}
