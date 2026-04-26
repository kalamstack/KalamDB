//! File permission tests over HTTP.

use std::path::{Path, PathBuf};

use kalam_client::models::ResponseStatus as LinkResponseStatus;
use kalamdb_api::http::sql::models::{ResponseStatus, SqlResponse};
use kalamdb_commons::Role;
use kalamdb_system::FileRef;
use reqwest::multipart;
use serde_json::Value as JsonValue;
use serial_test::serial;
use uuid::Uuid;

use super::test_support::{
    auth_helper::create_user_auth_header_with_id, http_server::start_http_test_server,
};

fn unique_suffix() -> String {
    Uuid::new_v4().simple().to_string()
}

fn table_path_for_user(
    storage_root: &Path,
    namespace: &str,
    table: &str,
    user_id: &str,
) -> PathBuf {
    storage_root.join(namespace).join(table).join(user_id)
}

fn table_path_shared(storage_root: &Path, namespace: &str, table: &str) -> PathBuf {
    storage_root.join(namespace).join(table)
}

fn find_files_in_subfolders(root: &Path, prefix: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();
    if !root.exists() {
        return results;
    }

    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return results,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(prefix) {
                    results.extend(find_files_recursive(&path));
                }
            }
        }
    }

    results
}

fn find_files_recursive(root: &Path) -> Vec<PathBuf> {
    let mut results = Vec::new();
    if !root.exists() {
        return results;
    }

    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return results,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            results.extend(find_files_recursive(&path));
        } else {
            results.push(path);
        }
    }

    results
}

fn parse_file_ref(value: &JsonValue) -> anyhow::Result<FileRef> {
    if let Some(raw) = value.as_str() {
        return Ok(FileRef::from_json(raw)?);
    }

    Ok(serde_json::from_value(value.clone())?)
}

async fn execute_sql_multipart(
    server: &super::test_support::http_server::HttpTestServer,
    auth_header: &str,
    sql: &str,
    files: Vec<(&str, &str, &'static [u8], &str)>,
) -> anyhow::Result<SqlResponse> {
    let url = format!("{}/v1/api/sql", server.base_url());
    let client = reqwest::Client::new();

    let mut form = multipart::Form::new().text("sql", sql.to_string());
    for (field, filename, data, mime) in files {
        let part = multipart::Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str(mime)?;
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
#[ntest::timeout(60000)]
#[serial]
async fn test_file_download_permissions_user_table() -> anyhow::Result<()> {
    let server = start_http_test_server().await?;
    let result = async {
        let namespace = format!("test_files_{}", unique_suffix());
        let table_name = format!("user_files_{}", unique_suffix());

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE NAMESPACE failed");

        let create_table_sql = format!(
            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT, doc FILE) WITH (TYPE='USER')",
            namespace, table_name
        );
        let resp = server.execute_sql(&create_table_sql).await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE TABLE failed");

        let (alice_auth, alice_id) =
            create_user_auth_header_with_id(&server, "alice", "test123", &Role::User).await?;
        let (bob_auth, _bob_id) =
            create_user_auth_header_with_id(&server, "bob", "test123", &Role::User).await?;
        let root_auth = server.bearer_auth_header("root")?;

        let insert_sql = format!(
            "INSERT INTO {}.{} (id, name, doc) VALUES (1, 'Alice', FILE(\"doc\"))",
            namespace, table_name
        );

        let upload = execute_sql_multipart(
            &server,
            &alice_auth,
            &insert_sql,
            vec![("doc", "hello.txt", b"hello world", "text/plain")],
        )
        .await?;
        assert_eq!(upload.status, ResponseStatus::Success);

        let select_sql = format!("SELECT doc FROM {}.{} WHERE id = 1", namespace, table_name);
        let resp = server.execute_sql_with_auth(&select_sql, &alice_auth).await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "SELECT failed");
        let rows = resp.rows_as_maps();
        let file_value = rows.get(0).and_then(|row| row.get("doc")).expect("doc should be present");
        let file_ref = parse_file_ref(file_value)?;
        let stored_name = file_ref.stored_name();

        let download_url = format!(
            "{}/v1/files/{}/{}/{}/{}?user_id={}",
            server.base_url(),
            namespace,
            table_name,
            file_ref.sub,
            stored_name,
            alice_id
        );
        let client = reqwest::Client::new();

        let bob_resp = client.get(&download_url).header("Authorization", bob_auth).send().await?;
        assert_eq!(bob_resp.status(), reqwest::StatusCode::FORBIDDEN);

        let root_resp = client.get(&download_url).header("Authorization", root_auth).send().await?;
        assert_eq!(root_resp.status(), reqwest::StatusCode::OK);

        let _ = server
            .execute_sql(&format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name))
            .await?;

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_insert_with_files_permission_denied() -> anyhow::Result<()> {
    let server = start_http_test_server().await?;
    let result = async {
        let namespace = format!("test_files_{}", unique_suffix());
        let table_name = format!("shared_files_{}", unique_suffix());

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE NAMESPACE failed");

        let create_table_sql = format!(
            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, doc FILE) WITH (TYPE='SHARED')",
            namespace, table_name
        );
        let resp = server.execute_sql(&create_table_sql).await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE TABLE failed");

        let (bob_auth, _bob_id) =
            create_user_auth_header_with_id(&server, "bob", "test123", &Role::User).await?;

        let insert_sql =
            format!("INSERT INTO {}.{} (id, doc) VALUES (1, FILE(\"doc\"))", namespace, table_name);

        let upload = execute_sql_multipart(
            &server,
            &bob_auth,
            &insert_sql,
            vec![("doc", "unauthorized.txt", b"nope", "text/plain")],
        )
        .await?;

        assert_eq!(upload.status, ResponseStatus::Error);
        let error_message =
            upload.error.as_ref().map(|err| err.message.to_lowercase()).unwrap_or_default();
        assert!(
            error_message.contains("access") || error_message.contains("permission"),
            "Expected permission denial, got: {}",
            error_message
        );

        let table_path = table_path_shared(&server.storage_root(), &namespace, &table_name);
        let leaked = find_files_in_subfolders(&table_path, "f");
        assert!(
            leaked.is_empty(),
            "Unauthorized insert should not leave files behind: {:?}",
            leaked
        );

        let _ = server
            .execute_sql(&format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name))
            .await?;

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_failed_insert_cleans_up_files() -> anyhow::Result<()> {
    let server = start_http_test_server().await?;
    let result = async {
        let namespace = format!("test_files_{}", unique_suffix());
        let table_name = format!("cleanup_files_{}", unique_suffix());

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE NAMESPACE failed");

        let create_table_sql = format!(
            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT NOT NULL, doc FILE) WITH \
             (TYPE='USER')",
            namespace, table_name
        );
        let resp = server.execute_sql(&create_table_sql).await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE TABLE failed");

        let (alice_auth, alice_id) =
            create_user_auth_header_with_id(&server, "alice", "test123", &Role::User).await?;

        let insert_sql = format!(
            "INSERT INTO {}.{} (id, name, doc) VALUES (1, NULL, FILE(\"doc\"))",
            namespace, table_name
        );

        let upload = execute_sql_multipart(
            &server,
            &alice_auth,
            &insert_sql,
            vec![("doc", "cleanup.txt", b"cleanup", "text/plain")],
        )
        .await?;

        assert_eq!(upload.status, ResponseStatus::Error);

        let table_path =
            table_path_for_user(&server.storage_root(), &namespace, &table_name, &alice_id);
        let leaked = find_files_in_subfolders(&table_path, "f");
        assert!(leaked.is_empty(), "Failed insert should cleanup staged files: {:?}", leaked);

        let _ = server
            .execute_sql(&format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name))
            .await?;

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}

#[tokio::test]
#[ntest::timeout(60000)]
#[serial]
async fn test_user_file_access_matrix() -> anyhow::Result<()> {
    let server = start_http_test_server().await?;
    let result = async {
        let namespace = format!("test_files_{}", unique_suffix());
        let table_name = format!("access_files_{}", unique_suffix());

        let resp = server
            .execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE NAMESPACE failed");

        let create_table_sql = format!(
            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT, doc FILE) WITH (TYPE='USER')",
            namespace, table_name
        );
        let resp = server.execute_sql(&create_table_sql).await?;
        assert_eq!(resp.status, LinkResponseStatus::Success, "CREATE TABLE failed");

        let (usera_auth, usera_id) =
            create_user_auth_header_with_id(&server, "usera", "test123", &Role::User).await?;
        let (userb_auth, userb_id) =
            create_user_auth_header_with_id(&server, "userb", "test123", &Role::User).await?;
        let (service_auth, _service_id) =
            create_user_auth_header_with_id(&server, "svc", "test123", &Role::Service).await?;
        let (dba_auth, _dba_id) =
            create_user_auth_header_with_id(&server, "dba", "test123", &Role::Dba).await?;
        let root_auth = server.bearer_auth_header("root")?;

        let insert_sql = format!(
            "INSERT INTO {}.{} (id, name, doc) VALUES (1, 'A', FILE(\"doc\"))",
            namespace, table_name
        );
        let upload_a = execute_sql_multipart(
            &server,
            &usera_auth,
            &insert_sql,
            vec![("doc", "usera.txt", b"usera", "text/plain")],
        )
        .await?;
        assert_eq!(upload_a.status, ResponseStatus::Success);

        let insert_sql = format!(
            "INSERT INTO {}.{} (id, name, doc) VALUES (1, 'B', FILE(\"doc\"))",
            namespace, table_name
        );
        let upload_b = execute_sql_multipart(
            &server,
            &userb_auth,
            &insert_sql,
            vec![("doc", "userb.txt", b"userb", "text/plain")],
        )
        .await?;
        assert_eq!(upload_b.status, ResponseStatus::Success);

        let select_sql = format!("SELECT doc FROM {}.{} WHERE id = 1", namespace, table_name);
        let resp_a = server.execute_sql_with_auth(&select_sql, &usera_auth).await?;
        assert_eq!(resp_a.status, LinkResponseStatus::Success, "SELECT usera failed");
        let rows_a = resp_a.rows_as_maps();
        let file_value_a = rows_a
            .get(0)
            .and_then(|row| row.get("doc"))
            .expect("usera doc should be present");
        let file_ref_a = parse_file_ref(file_value_a)?;
        let stored_name_a = file_ref_a.stored_name();

        let resp_b = server.execute_sql_with_auth(&select_sql, &userb_auth).await?;
        assert_eq!(resp_b.status, LinkResponseStatus::Success, "SELECT userb failed");
        let rows_b = resp_b.rows_as_maps();
        let file_value_b = rows_b
            .get(0)
            .and_then(|row| row.get("doc"))
            .expect("userb doc should be present");
        let file_ref_b = parse_file_ref(file_value_b)?;
        let stored_name_b = file_ref_b.stored_name();

        let client = reqwest::Client::new();
        let download_a = format!(
            "{}/v1/files/{}/{}/{}/{}?user_id={}",
            server.base_url(),
            namespace,
            table_name,
            file_ref_a.sub,
            stored_name_a,
            usera_id
        );
        let download_b = format!(
            "{}/v1/files/{}/{}/{}/{}?user_id={}",
            server.base_url(),
            namespace,
            table_name,
            file_ref_b.sub,
            stored_name_b,
            userb_id
        );

        let usera_on_b = client
            .get(&download_b)
            .header("Authorization", usera_auth.clone())
            .send()
            .await?;
        assert_eq!(usera_on_b.status(), reqwest::StatusCode::FORBIDDEN);

        let userb_on_a = client
            .get(&download_a)
            .header("Authorization", userb_auth.clone())
            .send()
            .await?;
        assert_eq!(userb_on_a.status(), reqwest::StatusCode::FORBIDDEN);

        for auth in [&service_auth, &dba_auth, &root_auth] {
            let resp_a =
                client.get(&download_a).header("Authorization", auth.clone()).send().await?;
            assert_eq!(resp_a.status(), reqwest::StatusCode::OK);
            let resp_b =
                client.get(&download_b).header("Authorization", auth.clone()).send().await?;
            assert_eq!(resp_b.status(), reqwest::StatusCode::OK);
        }

        let _ = server
            .execute_sql(&format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name))
            .await?;

        Ok(())
    }
    .await;

    server.shutdown().await;
    result
}
