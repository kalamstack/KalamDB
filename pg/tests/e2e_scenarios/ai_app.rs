use super::common::{create_shared_kalam_table_in_schema, drop_kalam_tables, unique_name, TestEnv};

#[tokio::test]
#[ntest::timeout(45000)]
async fn e2e_scenario_ai_app_rag_and_agent_run_flow() {
    let env = TestEnv::global().await;
    let schema = unique_name("ai_app");
    let documents = unique_name("documents");
    let chunks = unique_name("chunks");
    let runs = unique_name("runs");
    let steps = unique_name("steps");

    let pg = env.pg_connect().await;

    create_shared_kalam_table_in_schema(
        &pg,
        &schema,
        &documents,
        "id TEXT, title TEXT, source_uri TEXT, owner_team TEXT",
    )
    .await;
    create_shared_kalam_table_in_schema(
        &pg,
        &schema,
        &chunks,
        "id TEXT, document_id TEXT, chunk_no INTEGER, token_count INTEGER, chunk_text TEXT, embedding_model TEXT",
    )
    .await;
    create_shared_kalam_table_in_schema(
        &pg,
        &schema,
        &runs,
        "id TEXT, user_id TEXT, prompt TEXT, status TEXT, retrieved_chunks INTEGER, answer_tokens INTEGER",
    )
    .await;
    create_shared_kalam_table_in_schema(
        &pg,
        &schema,
        &steps,
        "id TEXT, run_id TEXT, agent_name TEXT, tool_name TEXT, duration_ms INTEGER, outcome TEXT",
    )
    .await;

    pg.batch_execute(&format!(
        "INSERT INTO {schema}.{documents} (id, title, source_uri, owner_team) VALUES
         ('doc-1', 'Billing playbook', 'kb://billing/playbook', 'support-ai'),
         ('doc-2', 'Retention handbook', 'kb://retention/handbook', 'growth-ai');
         INSERT INTO {schema}.{chunks} (id, document_id, chunk_no, token_count, chunk_text, embedding_model) VALUES
         ('chunk-1', 'doc-1', 0, 120, 'Refund escalation requires invoice verification and account review.', 'text-embed-3-large'),
         ('chunk-2', 'doc-1', 1, 88, 'High-priority tickets must capture billing entity and renewal date.', 'text-embed-3-large'),
         ('chunk-3', 'doc-2', 0, 95, 'Retention outreach should include discount guardrails and contract term.', 'text-embed-3-large');
         INSERT INTO {schema}.{runs} (id, user_id, prompt, status, retrieved_chunks, answer_tokens) VALUES
         ('run-1', 'agent-user-42', 'How should I handle a refund request before renewal?', 'running', 0, 0);
         INSERT INTO {schema}.{steps} (id, run_id, agent_name, tool_name, duration_ms, outcome) VALUES
         ('step-1', 'run-1', 'retriever', 'vector_search', 42, 'matched billing playbook'),
         ('step-2', 'run-1', 'planner', 'policy_check', 18, 'passed retention guardrails');"
    ))
    .await
    .expect("seed ai app scenario data");

    pg.batch_execute(
        "CREATE TEMP TABLE retrieval_scores (
            document_id TEXT PRIMARY KEY,
            score INTEGER NOT NULL
        );",
    )
    .await
    .expect("create retrieval score temp table");
    pg.batch_execute(
        "INSERT INTO retrieval_scores (document_id, score) VALUES
         ('doc-1', 97),
         ('doc-2', 61);",
    )
    .await
    .expect("seed retrieval scores");

    let ranked_context = pg
        .query(
            &format!(
                "SELECT d.title, c.chunk_no, r.score, c.chunk_text
                 FROM retrieval_scores AS r
                 JOIN {schema}.{documents} AS d ON d.id = r.document_id
                 JOIN {schema}.{chunks} AS c ON c.document_id = d.id
                 WHERE r.score >= 80
                 ORDER BY r.score DESC, c.chunk_no"
            ),
            &[],
        )
        .await
        .expect("query ranked retrieval context");

    assert_eq!(ranked_context.len(), 2, "billing playbook should contribute two ranked chunks");
    let first_title: &str = ranked_context[0].get(0);
    let first_score: i32 = ranked_context[0].get(2);
    let first_text: &str = ranked_context[0].get(3);
    assert_eq!(first_title, "Billing playbook");
    assert_eq!(first_score, 97);
    assert!(first_text.contains("Refund escalation"));

    pg.batch_execute(&format!(
        "UPDATE {schema}.{runs}
         SET status = 'completed', retrieved_chunks = 2, answer_tokens = 186
         WHERE id = 'run-1';
         INSERT INTO {schema}.{steps} (id, run_id, agent_name, tool_name, duration_ms, outcome) VALUES
         ('step-3', 'run-1', 'writer', 'answer_synthesis', 64, 'generated final response');"
    ))
    .await
    .expect("complete ai run");

    let run_trace = pg
        .query(
            &format!(
                "SELECT r.status, r.retrieved_chunks, r.answer_tokens, COUNT(s.id) AS step_count
                 FROM {schema}.{runs} AS r
                 JOIN {schema}.{steps} AS s ON s.run_id = r.id
                 WHERE r.id = 'run-1'
                 GROUP BY r.status, r.retrieved_chunks, r.answer_tokens"
            ),
            &[],
        )
        .await
        .expect("query ai run trace");

    assert_eq!(run_trace.len(), 1, "expected a single aggregated run trace row");
    let status: &str = run_trace[0].get(0);
    let retrieved_chunks: i32 = run_trace[0].get(1);
    let answer_tokens: i32 = run_trace[0].get(2);
    let step_count: i64 = run_trace[0].get(3);
    assert_eq!(status, "completed");
    assert_eq!(retrieved_chunks, 2);
    assert_eq!(answer_tokens, 186);
    assert_eq!(step_count, 3);

    let remote_run = env
        .kalamdb_sql(&format!(
            "SELECT status, retrieved_chunks, answer_tokens FROM {schema}.{runs} WHERE id = 'run-1'"
        ))
        .await;
    let remote_run_text = serde_json::to_string(&remote_run).unwrap_or_default();
    assert!(
        remote_run_text.contains("completed") && remote_run_text.contains("186"),
        "ai run completion should be visible in KalamDB: {remote_run_text}"
    );

    drop_kalam_tables(&pg, &schema, &[documents, chunks, runs, steps]).await;
}
