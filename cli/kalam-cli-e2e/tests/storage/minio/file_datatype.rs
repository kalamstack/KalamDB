use super::common::{should_run_minio_storage_tests, *};
use crate::common::{
    default_password, default_username, execute_sql_as_root_via_cli, execute_sql_via_http_as,
    generate_unique_namespace, generate_unique_table, get_access_token, leader_or_server_url,
    shared_http_client,
};

#[test]
fn test_minio_file_datatype_roundtrip() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let runtime = tokio::runtime::Runtime::new().expect("file datatype runtime");
    let storage_id = generate_unique_namespace("minio_file");
    let namespace = generate_unique_namespace("minio_file_ns");
    let table = generate_unique_table("minio_file_table");
    let full_table = format!("{}.{}", namespace, table);

    setup_minio_storage(&storage_id, "MinIO File Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id TEXT PRIMARY KEY, name TEXT, attachment FILE) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:1')",
        full_table, storage_id
    ))
    .expect("file table creation");

    let base_url = leader_or_server_url();
    let token = runtime
        .block_on(async { get_access_token(default_username(), default_password()).await })
        .expect("access token");
    let client = shared_http_client();
    let sql = format!(
        "INSERT INTO {} (id, name, attachment) VALUES ('doc1', 'My Document', \
         FILE(\"myfile.txt\"))",
        full_table
    );
    let test_content = b"This is the file content for testing FILE datatype!".to_vec();
    let form = reqwest::multipart::Form::new().text("sql", sql).part(
        "file:myfile.txt",
        reqwest::multipart::Part::bytes(test_content.clone())
            .file_name("test-attachment.txt")
            .mime_str("text/plain")
            .expect("mime type"),
    );
    let response = runtime.block_on(async {
        client
            .post(format!("{}/v1/api/sql", base_url))
            .bearer_auth(&token)
            .multipart(form)
            .send()
            .await
    });
    let response = response.expect("multipart sql response");
    assert!(
        response.status().is_success(),
        "file upload should succeed: {}",
        response.status()
    );

    let query = runtime
        .block_on(async {
            execute_sql_via_http_as(
                default_username(),
                default_password(),
                &format!("SELECT id, name, attachment FROM {} WHERE id = 'doc1'", full_table),
            )
            .await
        })
        .expect("query inserted file row");
    let rows = query["results"][0]["rows"].as_array().expect("query rows");
    assert_eq!(rows.len(), 1, "expected one FILE row");

    let attachment_json: &str = rows[0][2].as_str().expect("attachment should be a string");
    let file_ref: serde_json::Value = serde_json::from_str(attachment_json).expect("FileRef JSON");
    assert!(file_ref["id"].is_string());
    assert!(file_ref["sub"].is_string());
    assert!(file_ref["name"].is_string());

    let subfolder = file_ref["sub"].as_str().unwrap();
    let stored_name = stored_filename_from_file_ref(&file_ref);
    let download_url =
        format!("{}/v1/files/{}/{}/{}/{}", base_url, namespace, table, subfolder, stored_name);
    let download = runtime
        .block_on(async { client.get(&download_url).bearer_auth(&token).send().await })
        .expect("download file");
    assert!(
        download.status().is_success(),
        "file download should succeed: {}",
        download.status()
    );
    let body = runtime.block_on(async { download.bytes().await }).expect("download bytes");
    assert_eq!(body.as_ref(), test_content.as_slice(), "downloaded file content should match");

    flush_table_and_wait(&full_table);
    let storage_meta = fetch_storage_metadata(&storage_id);
    let store = build_minio_store(&storage_meta.base_directory);
    let table_dir = resolve_template(&storage_meta.shared_template, &namespace, &table, None);
    assert_minio_files(&runtime, &store, &table_dir, "file datatype table");

    cleanup_minio_resources(&namespace, &table, &table, &storage_id);
}

fn stored_filename_from_file_ref(file_ref: &serde_json::Value) -> String {
    let file_id = file_ref["id"].as_str().unwrap_or_default();
    let name = file_ref["name"].as_str().unwrap_or_default();

    let ext = extract_extension(name);
    let sanitized = sanitize_filename(name);

    if sanitized.is_empty() {
        format!("{}.{}", file_id, ext)
    } else {
        format!("{}-{}.{}", file_id, sanitized, ext)
    }
}

fn sanitize_filename(name: &str) -> String {
    let name_without_ext = name.rsplit_once('.').map(|(n, _)| n).unwrap_or(name);

    let sanitized: String = name_without_ext
        .chars()
        .filter_map(|c| {
            if c.is_ascii_alphanumeric() {
                Some(c.to_ascii_lowercase())
            } else if c == ' ' || c == '_' || c == '-' {
                Some('-')
            } else {
                None
            }
        })
        .take(50)
        .collect();

    let mut result = String::with_capacity(sanitized.len());
    let mut last_was_dash = true;
    for c in sanitized.chars() {
        if c == '-' {
            if !last_was_dash {
                result.push(c);
            }
            last_was_dash = true;
        } else {
            result.push(c);
            last_was_dash = false;
        }
    }
    result.trim_end_matches('-').to_string()
}

fn extract_extension(name: &str) -> String {
    name.rsplit_once('.')
        .map(|(_, ext)| {
            let ext_lower = ext.to_ascii_lowercase();
            if ext_lower.len() <= 10 && ext_lower.chars().all(|c| c.is_ascii_alphanumeric()) {
                ext_lower
            } else {
                "bin".to_string()
            }
        })
        .unwrap_or_else(|| "bin".to_string())
}
