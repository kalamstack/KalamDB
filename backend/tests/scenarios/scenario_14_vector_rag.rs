//! Scenario 14: RAG Documents With FILE + Vector Search
//!
//! Real-world RAG-like flow with:
//! - USER-scoped documents table containing two FILE attachments per row
//! - USER-scoped vectors table keyed by the same document ID
//! - Vector indexes + flush artifacts in cold storage
//! - Similarity queries joined back to document rows

use super::helpers::*;
use kalam_client::KalamCellValue;
use kalamdb_api::http::sql::models::{ResponseStatus as ApiResponseStatus, SqlResponse};
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::Role;
use kalamdb_system::FileRef;
use reqwest::multipart;
use serde_json::Value as JsonValue;

fn parse_file_ref(value: &JsonValue) -> anyhow::Result<FileRef> {
    if let Some(raw) = value.as_str() {
        return Ok(FileRef::from_json(raw)?);
    }
    Ok(serde_json::from_value(value.clone())?)
}

fn extract_id_list(rows: &[std::collections::HashMap<String, KalamCellValue>]) -> Vec<i64> {
    rows.iter()
        .filter_map(|row| {
            row.get("id")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
        })
        .collect()
}

async fn execute_sql_multipart(
    server: &crate::test_support::http_server::HttpTestServer,
    auth_header: &str,
    sql: &str,
    files: Vec<(&str, &str, Vec<u8>, &str)>,
) -> anyhow::Result<SqlResponse> {
    let url = format!("{}/v1/api/sql", server.base_url());
    let client = reqwest::Client::new();

    let mut form = multipart::Form::new().text("sql", sql.to_string());
    for (field, filename, data, mime) in files {
        let part = multipart::Part::bytes(data).file_name(filename.to_string()).mime_str(mime)?;
        form = form.part(format!("file:{}", field), part);
    }

    let response = client
        .post(url)
        .header("Authorization", auth_header)
        .multipart(form)
        .send()
        .await?;

    Ok(response.json::<SqlResponse>().await?)
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn test_scenario_14_rag_docs_with_files_and_vector_search() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("rag_vectors");
    let files_table = "documents";
    let vectors_table = "documents_vectors";
    let username = format!("{}_rag_user", ns);

    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE NAMESPACE");

    let create_files_table_sql = format!(
        "CREATE TABLE {}.{} (\
            id BIGINT PRIMARY KEY, \
            title TEXT NOT NULL, \
            body TEXT NOT NULL, \
            attachment_a FILE, \
            attachment_b FILE\
        ) WITH (TYPE='USER')",
        ns, files_table
    );
    let resp = server.execute_sql(&create_files_table_sql).await?;
    assert_success(&resp, "CREATE documents file table");

    let create_vectors_table_sql = format!(
        "CREATE TABLE {}.{} (\
            id BIGINT PRIMARY KEY, \
            doc_embedding EMBEDDING(3), \
            attachment_a_embedding EMBEDDING(3), \
            attachment_b_embedding EMBEDDING(3)\
        ) WITH (TYPE='USER')",
        ns, vectors_table
    );
    let resp = server.execute_sql(&create_vectors_table_sql).await?;
    assert_success(&resp, "CREATE documents vectors table");

    for column in [
        "doc_embedding",
        "attachment_a_embedding",
        "attachment_b_embedding",
    ] {
        let sql =
            format!("ALTER TABLE {}.{} CREATE INDEX {} USING COSINE", ns, vectors_table, column);
        let resp = server.execute_sql(&sql).await?;
        assert_success(&resp, &format!("CREATE INDEX {} on vectors table", column));
    }

    let user_id = ensure_user_exists(server, &username, "test123", &Role::User).await?;
    let user_auth = server.bearer_auth_header(&username)?;
    let user_client = create_user_and_client(server, &username, &Role::User).await?;
    let app_context = server.app_context();
    let manifest_user = UserId::new(user_id.clone());

    let docs = [
        (
            1_i64,
            "Architecture Notes",
            "KalamDB vector architecture and staging overview",
            "[1.0,0.0,0.0]",
            "[0.95,0.05,0.0]",
            "[0.90,0.10,0.0]",
            "doc1-appendix.txt",
            "doc1-policy.txt",
        ),
        (
            2_i64,
            "Operations Guide",
            "Flush operations and storage manifests",
            "[0.65,0.35,0.0]",
            "[0.60,0.40,0.0]",
            "[0.55,0.45,0.0]",
            "doc2-appendix.txt",
            "doc2-policy.txt",
        ),
        (
            3_i64,
            "Incident Runbook",
            "Recovery and incident response handbook",
            "[0.0,1.0,0.0]",
            "[0.10,0.90,0.0]",
            "[0.0,1.0,0.0]",
            "doc3-appendix.txt",
            "doc3-policy.txt",
        ),
    ];

    for (id, title, body, doc_vec, file_a_vec, file_b_vec, file_a_name, file_b_name) in docs {
        let insert_files_sql = format!(
            "INSERT INTO {}.{} (id, title, body, attachment_a, attachment_b) \
             VALUES ({}, '{}', '{}', FILE(\"file_a\"), FILE(\"file_b\"))",
            ns, files_table, id, title, body
        );
        let upload = execute_sql_multipart(
            server,
            &user_auth,
            &insert_files_sql,
            vec![
                (
                    "file_a",
                    file_a_name,
                    format!("file_a payload for row {}", id).into_bytes(),
                    "text/plain",
                ),
                (
                    "file_b",
                    file_b_name,
                    format!("file_b payload for row {}", id).into_bytes(),
                    "text/plain",
                ),
            ],
        )
        .await?;
        assert_eq!(
            upload.status,
            ApiResponseStatus::Success,
            "multipart INSERT failed for id={}: {:?}",
            id,
            upload.error
        );

        let insert_vectors_sql = format!(
            "INSERT INTO {}.{} (id, doc_embedding, attachment_a_embedding, attachment_b_embedding) \
             VALUES ({}, '{}', '{}', '{}')",
            ns, vectors_table, id, doc_vec, file_a_vec, file_b_vec
        );
        let insert_vectors_resp =
            user_client.execute_query(&insert_vectors_sql, None, None, None).await?;
        assert!(
            insert_vectors_resp.success(),
            "vector INSERT should succeed for id={}: {:?}",
            id,
            insert_vectors_resp.error
        );
    }

    let file_columns_resp = user_client
        .execute_query(
            &format!("SELECT attachment_a, attachment_b FROM {}.{} WHERE id = 1", ns, files_table),
            None,
            None,
            None,
        )
        .await?;
    assert!(file_columns_resp.success(), "SELECT file columns should succeed");
    let file_row = file_columns_resp
        .rows_as_maps()
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Expected one file row for id=1"))?;
    let file_a = parse_file_ref(
        file_row
            .get("attachment_a")
            .ok_or_else(|| anyhow::anyhow!("attachment_a missing"))?,
    )?;
    let file_b = parse_file_ref(
        file_row
            .get("attachment_b")
            .ok_or_else(|| anyhow::anyhow!("attachment_b missing"))?,
    )?;
    assert!(!file_a.stored_name().is_empty(), "attachment_a file reference should be valid");
    assert!(!file_b.stored_name().is_empty(), "attachment_b file reference should be valid");

    let resp = server
        .execute_sql(&format!("STORAGE FLUSH TABLE {}.{}", ns, vectors_table))
        .await?;
    assert_success(&resp, "STORAGE FLUSH vectors table");
    wait_for_flush_complete(server, &ns, vectors_table, std::time::Duration::from_secs(25)).await?;

    let vectors_table_id = TableId::from_strings(&ns, vectors_table);
    let manifest = app_context
        .manifest_service()
        .ensure_manifest_initialized(&vectors_table_id, Some(&manifest_user))
        .map_err(|e| anyhow::anyhow!("Failed to load vectors manifest: {}", e))?;
    let cached_table = app_context.schema_registry().get(&vectors_table_id).ok_or_else(|| {
        anyhow::anyhow!("Missing cached vectors table {}", vectors_table_id.full_name())
    })?;
    let storage_cached = cached_table
        .storage_cached(&app_context.storage_registry())
        .map_err(|e| anyhow::anyhow!("Failed to resolve vectors storage cache: {}", e))?;

    for column in [
        "doc_embedding",
        "attachment_a_embedding",
        "attachment_b_embedding",
    ] {
        let meta = manifest
            .vector_indexes
            .get(column)
            .ok_or_else(|| anyhow::anyhow!("Missing vector metadata for {}", column))?;
        let snapshot_path = meta
            .snapshot_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Missing snapshot_path for {}", column))?;
        let exists = storage_cached
            .exists(TableType::User, &vectors_table_id, Some(&manifest_user), snapshot_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed snapshot exists check for {}: {}", column, e))?;
        assert!(
            exists.exists,
            "Vector snapshot file should exist in cold storage for {}: {}",
            column, snapshot_path
        );
    }

    let seeded_rows_resp = user_client
        .execute_query(
            &format!("SELECT id FROM {}.{} ORDER BY id", ns, vectors_table),
            None,
            None,
            None,
        )
        .await?;
    assert!(seeded_rows_resp.success(), "seeded vector rows query should succeed");
    let seeded_ids = extract_id_list(&seeded_rows_resp.rows_as_maps());
    assert_eq!(seeded_ids, vec![1, 2, 3], "expected 3 seeded vector rows");

    let doc_query_resp = user_client
        .execute_query(
            &format!(
                "SELECT id FROM {}.{} ORDER BY COSINE_DISTANCE(doc_embedding, '[1.0,0.0,0.0]') LIMIT 2",
                ns, vectors_table
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(doc_query_resp.success(), "doc_embedding vector query should succeed");
    let doc_ids = extract_id_list(&doc_query_resp.rows_as_maps());
    assert!(
        doc_ids.contains(&1),
        "row 1 should be included in top-k for doc_embedding; ids={:?}",
        doc_ids
    );

    let attachment_query_resp = user_client
        .execute_query(
            &format!(
                "SELECT id FROM {}.{} ORDER BY COSINE_DISTANCE(attachment_b_embedding, '[0.0,1.0,0.0]') LIMIT 2",
                ns, vectors_table
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(
        attachment_query_resp.success(),
        "attachment_b_embedding vector query should succeed"
    );
    let attachment_ids = extract_id_list(&attachment_query_resp.rows_as_maps());
    assert!(
        attachment_ids.contains(&3),
        "row 3 should be included in top-k for attachment_b_embedding; ids={:?}",
        attachment_ids
    );

    let insert_files_hot_sql = format!(
        "INSERT INTO {}.{} (id, title, body, attachment_a, attachment_b) \
         VALUES (4, 'Realtime Notes', 'Hot row before flush', FILE(\"file_a\"), FILE(\"file_b\"))",
        ns, files_table
    );
    let upload_hot = execute_sql_multipart(
        server,
        &user_auth,
        &insert_files_hot_sql,
        vec![
            ("file_a", "doc4-appendix.txt", b"hot file_a payload".to_vec(), "text/plain"),
            ("file_b", "doc4-policy.txt", b"hot file_b payload".to_vec(), "text/plain"),
        ],
    )
    .await?;
    assert_eq!(
        upload_hot.status,
        ApiResponseStatus::Success,
        "multipart INSERT failed for id=4: {:?}",
        upload_hot.error
    );

    let insert_hot_vector_resp = user_client
        .execute_query(
            &format!(
                "INSERT INTO {}.{} (id, doc_embedding, attachment_a_embedding, attachment_b_embedding) \
                 VALUES (4, '[0.999,0.001,0.0]', '[0.98,0.02,0.0]', '[0.97,0.03,0.0]')",
                ns, vectors_table
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(
        insert_hot_vector_resp.success(),
        "vector INSERT should succeed for id=4: {:?}",
        insert_hot_vector_resp.error
    );

    let mixed_tier_resp = user_client
        .execute_query(
            &format!(
                "SELECT id FROM {}.{} ORDER BY COSINE_DISTANCE(doc_embedding, '[1.0,0.0,0.0]') LIMIT 3",
                ns, vectors_table
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(mixed_tier_resp.success(), "mixed hot+cold vector query should succeed");
    let mixed_ids = extract_id_list(&mixed_tier_resp.rows_as_maps());
    assert!(mixed_ids.contains(&1), "mixed vector results should include cold row id=1");
    assert!(mixed_ids.contains(&4), "mixed vector results should include hot row id=4");

    let delete_vector_resp = user_client
        .execute_query(
            &format!("DELETE FROM {}.{} WHERE id = 1", ns, vectors_table),
            None,
            None,
            None,
        )
        .await?;
    assert!(
        delete_vector_resp.success(),
        "vector DELETE should succeed for id=1: {:?}",
        delete_vector_resp.error
    );

    let after_delete_resp = user_client
        .execute_query(
            &format!(
                "SELECT id FROM {}.{} ORDER BY COSINE_DISTANCE(doc_embedding, '[1.0,0.0,0.0]') LIMIT 3",
                ns, vectors_table
            ),
            None,
            None,
            None,
        )
        .await?;
    assert!(after_delete_resp.success(), "vector query after delete should succeed");
    let after_delete_ids = extract_id_list(&after_delete_resp.rows_as_maps());
    assert!(
        !after_delete_ids.contains(&1),
        "deleted vector row should not appear in similarity results"
    );

    let resp = server
        .execute_sql(&format!("STORAGE FLUSH TABLE {}.{}", ns, vectors_table))
        .await?;
    assert_success(&resp, "STORAGE FLUSH vectors table (second cycle)");
    wait_for_flush_complete(server, &ns, vectors_table, std::time::Duration::from_secs(25)).await?;

    let manifest_after_second_flush = app_context
        .manifest_service()
        .ensure_manifest_initialized(&vectors_table_id, Some(&manifest_user))
        .map_err(|e| {
            anyhow::anyhow!("Failed to load vectors manifest after second flush: {}", e)
        })?;
    let doc_meta = manifest_after_second_flush
        .vector_indexes
        .get("doc_embedding")
        .ok_or_else(|| anyhow::anyhow!("Missing doc_embedding metadata after second flush"))?;
    assert!(
        doc_meta.snapshot_version >= 2,
        "second flush should rotate vector snapshot version for doc_embedding"
    );

    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
