//! Manifest persistence behavior over the real HTTP SQL API.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;
use tokio::time::{sleep, Duration, Instant};

use super::test_support::{
    auth_helper::create_user_auth_header,
    consolidated_helpers::{unique_namespace, unique_table},
    flush::flush_table_and_wait,
};

fn find_manifest_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn recurse(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                recurse(&path, out);
            } else if path.file_name().and_then(|n| n.to_str()) == Some("manifest.json") {
                out.push(path);
            }
        }
    }

    let mut out = Vec::new();
    recurse(root, &mut out);
    out
}

#[tokio::test]
async fn test_user_table_manifest_persistence_over_http() -> anyhow::Result<()> {
    let _guard = super::test_support::http_server::acquire_test_lock().await;
    let server = super::test_support::http_server::get_global_server().await;
    // Case 1: Manifest written only after flush
    {
        let ns = unique_namespace("test_manifest_persist");
        let table = "events";
        let user = unique_table("user1");
        let password = "UserPass123!";

        let user_auth = create_user_auth_header(server, &user, password, &Role::User).await?;

        let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
        assert_eq!(resp.status, ResponseStatus::Success, "resp.error={:?}", resp.error);

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "CREATE TABLE {}.{} (id TEXT PRIMARY KEY, event_type TEXT, ts INT) WITH (TYPE \
                     = 'USER', STORAGE_ID = 'local')",
                    ns, table
                ),
                &user_auth,
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "INSERT INTO {}.{} (id, event_type, ts) VALUES ('evt1', 'login', 1)",
                    ns, table
                ),
                &user_auth,
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);

        let storage_root = server.storage_root();
        let manifests_before = find_manifest_files(&storage_root)
            .into_iter()
            .filter(|p| {
                let s = p.to_string_lossy();
                s.contains(&ns) && s.contains(table)
            })
            .collect::<Vec<_>>();
        assert!(
            manifests_before.is_empty(),
            "Manifest should NOT exist on disk before flush for {}.{} (found: {:?})",
            ns,
            table,
            manifests_before
        );

        flush_table_and_wait(server, &ns, table).await?;

        // wait_for_flush_job_completed(server, &ns, table).await?;

        let deadline = Instant::now() + Duration::from_secs(20);
        let _manifest_path = loop {
            let candidates = find_manifest_files(&storage_root);
            if let Some(path) = candidates.iter().find(|p| {
                let s = p.to_string_lossy();
                s.contains(&ns) && s.contains(table)
            }) {
                break path.to_path_buf();
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "Expected manifest.json for {}.{} under {} (found: {:?})",
                    ns,
                    table,
                    storage_root.display(),
                    candidates
                );
            }
            sleep(Duration::from_millis(50)).await;
        };

        let resp = server
            .execute_sql_with_auth(
                &format!("SELECT id, event_type FROM {}.{} ORDER BY id", ns, table),
                &user_auth,
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);

        let rows = resp.rows_as_maps();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id").unwrap().as_str().unwrap(), "evt1");
    }

    // Case 2: Manifest exists after flush and query can read data
    {
        let ns = unique_namespace("test_manifest_reload");
        let table = "metrics";
        let user = unique_table("user2");
        let password = "UserPass123!";

        let user_auth = create_user_auth_header(server, &user, password, &Role::User).await?;

        let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
        assert_eq!(resp.status, ResponseStatus::Success, "resp.error={:?}", resp.error);

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "CREATE TABLE {}.{} (id TEXT PRIMARY KEY, metric_name TEXT, value DOUBLE) \
                     WITH (TYPE = 'USER', STORAGE_ID = 'local')",
                    ns, table
                ),
                &user_auth,
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success, "resp.error={:?}", resp.error);

        let resp = server
            .execute_sql_with_auth(
                &format!(
                    "INSERT INTO {}.{} (id, metric_name, value) VALUES ('m1', 'cpu_usage', 45.5)",
                    ns, table
                ),
                &user_auth,
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);

        flush_table_and_wait(server, &ns, table).await?;

        // wait_for_flush_job_completed(server, &ns, table).await?;

        let storage_root = server.storage_root();
        let deadline = Instant::now() + Duration::from_secs(20);
        let _manifest_path = loop {
            let candidates = find_manifest_files(&storage_root);
            if let Some(path) = candidates.iter().find(|p| {
                let s = p.to_string_lossy();
                s.contains(&ns) && s.contains(table)
            }) {
                break path.to_path_buf();
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "Expected manifest.json for {}.{} under {} (found: {:?})",
                    ns,
                    table,
                    storage_root.display(),
                    candidates
                );
            }
            sleep(Duration::from_millis(50)).await;
        };

        let resp = server
            .execute_sql_with_auth(
                &format!("SELECT id, metric_name, value FROM {}.{} WHERE id = 'm1'", ns, table),
                &user_auth,
            )
            .await?;
        assert_eq!(resp.status, ResponseStatus::Success);

        let rows = resp.rows_as_maps();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("metric_name").unwrap().as_str().unwrap(), "cpu_usage");
    }

    Ok(())
}
