//! Cluster-specific tests for KalamDB
//!
//! These tests are designed to be run against a multi-node cluster
//! and can be executed separately from the main smoke tests.
//!
//! To run cluster tests only:
//!   cargo test --test cluster
//!
//! Environment variables:
//!   KALAMDB_CLUSTER_URLS - Comma-separated list of cluster node URLs
//!     Default: http://127.0.0.1:2901,http://127.0.0.1:2902,http://127.0.0.1:2903
//!
//!   KALAMDB_ROOT_PASSWORD - Root password for authentication
//!     Required for authenticated cluster access

mod common;

/// Cluster-specific common utilities
mod cluster_common {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Mutex, OnceLock,
        },
        time::Duration,
    };

    use kalam_client::{KalamCellValue, KalamLinkTimeouts, QueryResponse};
    use serde_json::Value;

    use crate::common::*;

    struct ClusterHelperStats {
        calls:            AtomicU64,
        attempts:         AtomicU64,
        retryable_errors: AtomicU64,
        total_micros:     AtomicU64,
    }

    impl ClusterHelperStats {
        const fn new() -> Self {
            Self {
                calls:            AtomicU64::new(0),
                attempts:         AtomicU64::new(0),
                retryable_errors: AtomicU64::new(0),
                total_micros:     AtomicU64::new(0),
            }
        }
    }

    static CLUSTER_HELPER_STATS: ClusterHelperStats = ClusterHelperStats::new();

    fn cluster_helper_timing_enabled() -> bool {
        std::env::var("KALAMDB_TRACE_CLUSTER_HELPERS")
            .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
    }

    fn record_cluster_helper_timing(
        op: &str,
        sql: &str,
        elapsed: Duration,
        attempts: u64,
        retryable_errors: u64,
    ) {
        CLUSTER_HELPER_STATS.calls.fetch_add(1, Ordering::Relaxed);
        CLUSTER_HELPER_STATS.attempts.fetch_add(attempts, Ordering::Relaxed);
        CLUSTER_HELPER_STATS
            .retryable_errors
            .fetch_add(retryable_errors, Ordering::Relaxed);
        CLUSTER_HELPER_STATS
            .total_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);

        if cluster_helper_timing_enabled() || retryable_errors > 0 {
            let first_line = sql.lines().next().unwrap_or(sql).trim();
            eprintln!(
                "[cluster-helper] op={} attempts={} retryable={} took_ms={:.3} sql={}",
                op,
                attempts,
                retryable_errors,
                elapsed.as_secs_f64() * 1000.0,
                first_line
            );
        }
    }

    /// Get cluster node URLs from environment or use defaults
    pub fn cluster_urls() -> Vec<String> {
        get_available_server_urls()
    }

    /// Get cluster node URLs in configuration order (no leader reordering)
    pub fn cluster_urls_config_order() -> Vec<String> {
        crate::common::cluster_urls_config_order()
    }

    /// Shared tokio runtime for cluster tests
    pub fn cluster_runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("Failed to create cluster test runtime")
        })
    }

    /// Client cache for cluster helpers.
    ///
    /// Reusing a `KalamLinkClient` per URL avoids spawning a fresh reqwest connection pool
    /// on every `execute_on_node` call. Each `KalamLinkClient` owns an `Arc<reqwest::Client>`;
    /// cloning is cheap and all clones share the same pool.
    fn cached_cluster_client(base_url: &str, username: &str, password: &str) -> KalamLinkClient {
        static CLIENT_CACHE: OnceLock<Mutex<HashMap<String, KalamLinkClient>>> = OnceLock::new();
        let cache = CLIENT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
        let cache_key = format!("{}\n{}\n{}", base_url, username, password);
        if let Ok(mut guard) = cache.lock() {
            if let Some(client) = guard.get(&cache_key) {
                return client.clone();
            }
            let client = build_cluster_client_with_auth(base_url, username, password);
            guard.insert(cache_key, client.clone());
            return client;
        }
        // Fallback if lock poisoned
        build_cluster_client_with_auth(base_url, username, password)
    }

    fn build_cluster_client_with_auth(
        base_url: &str,
        username: &str,
        password: &str,
    ) -> KalamLinkClient {
        client_for_user_on_url_with_timeouts(
            base_url,
            username,
            password,
            KalamLinkTimeouts::builder()
                .connection_timeout_secs(5)
                .receive_timeout_secs(30)
                .send_timeout_secs(10)
                .subscribe_timeout_secs(10)
                .auth_timeout_secs(10)
                .initial_data_timeout(Duration::from_secs(30))
                .build(),
        )
        .expect("Failed to build cluster client")
    }

    /// Create a client connected to a specific cluster node
    pub fn create_cluster_client(base_url: &str) -> KalamLinkClient {
        cached_cluster_client(base_url, &default_username(), &default_password())
    }

    /// Create a client connected to a specific cluster node with custom credentials
    pub fn create_cluster_client_with_auth(
        base_url: &str,
        username: &str,
        password: &str,
    ) -> KalamLinkClient {
        cached_cluster_client(base_url, username, password)
    }

    /// Execute a query on a specific cluster node and return the count
    /// Note: With leader-only reads (Spec 021), this will automatically use the leader node for
    /// client reads
    pub fn query_count_on_url(base_url: &str, sql: &str) -> i64 {
        // Try the specified URL first, but if we get NOT_LEADER error, retry on leader
        let result = query_count_on_url_internal(base_url, sql);

        // If we got a NOT_LEADER error, retry on the leader (extracted from error or cached)
        if let Err(err_msg) = result {
            if is_leader_error(&err_msg) {
                // is_leader_error() will cache any leader URL found in the error message
                if let Some(leader) = leader_url() {
                    if leader != base_url {
                        return query_count_on_url_internal(&leader, sql).unwrap_or_else(|e| {
                            panic!("Cluster count query failed on leader {}: {}", leader, e);
                        });
                    }
                }
            }
            panic!("Cluster count query failed: {}", err_msg);
        }

        result.unwrap()
    }

    fn query_count_on_url_internal(base_url: &str, sql: &str) -> Result<i64, String> {
        let response = execute_on_node_response(base_url, sql)?;

        let result = response
            .results
            .first()
            .ok_or_else(|| "Missing query result for count".to_string())?;
        let rows = result
            .rows
            .as_ref()
            .and_then(|rows| rows.first())
            .ok_or_else(|| "Missing count row".to_string())?;
        let value = rows.first().ok_or_else(|| "Missing count column".to_string())?;
        let unwrapped = extract_typed_value(value);
        match unwrapped {
            serde_json::Value::String(s) => {
                s.parse::<i64>().map_err(|e| format!("Invalid count string: {}", e))
            },
            serde_json::Value::Number(n) => {
                n.as_i64().ok_or_else(|| "Invalid count number".to_string())
            },
            other => Err(format!("Unexpected count value: {}", other)),
        }
    }

    fn response_error_message(response: &QueryResponse) -> String {
        if let Some(error) = &response.error {
            if let Some(details) = &error.details {
                return format!("{} ({})", error.message, details);
            }
            return error.message.clone();
        }

        format!("Query failed: {:?}", response)
    }

    fn is_truncated_read_response(response: &QueryResponse, sql: &str) -> bool {
        if !is_read_only_sql(sql) {
            return false;
        }

        let Some(result) = response.results.first() else {
            return false;
        };

        if result.message.as_ref().is_some_and(|message| !message.is_empty()) {
            return false;
        }

        if result.rows.as_ref().is_some_and(|rows| !rows.is_empty()) {
            return false;
        }

        result.row_count > 0 || result.schema.is_empty()
    }

    fn is_read_only_sql(sql: &str) -> bool {
        let trimmed = sql.trim_start();
        let first_token = trimmed.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
        matches!(
            first_token.as_str(),
            "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "WITH"
        )
    }

    fn should_wait_for_cluster_after_sql(sql: &str) -> bool {
        let upper = sql.trim_start().to_ascii_uppercase();
        upper.starts_with("CREATE NAMESPACE")
            || upper.starts_with("CREATE TABLE")
            || upper.starts_with("CREATE SHARED TABLE")
            || upper.starts_with("CREATE USER TABLE")
            || upper.starts_with("CREATE STREAM TABLE")
            || upper.starts_with("DROP NAMESPACE")
            || upper.starts_with("DROP TABLE")
            || upper.starts_with("ALTER TABLE")
    }

    fn clean_identifier_token(token: &str) -> String {
        token
            .trim_end_matches(';')
            .trim_end_matches('(')
            .trim_matches('"')
            .trim_matches('`')
            .to_string()
    }

    fn split_full_table_name(token: &str) -> Option<(String, String)> {
        let cleaned = clean_identifier_token(token);
        let mut parts = cleaned.splitn(2, '.');
        let namespace = parts.next()?.to_string();
        let table = parts.next()?.to_string();
        if namespace.is_empty() || table.is_empty() {
            return None;
        }
        Some((namespace, table))
    }

    fn extract_created_namespace(sql: &str) -> Option<String> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 3
            && tokens[0].eq_ignore_ascii_case("CREATE")
            && tokens[1].eq_ignore_ascii_case("NAMESPACE")
        {
            return Some(clean_identifier_token(tokens[2]));
        }
        None
    }

    fn extract_created_table(sql: &str) -> Option<(String, String)> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 3
            && tokens[0].eq_ignore_ascii_case("CREATE")
            && tokens[1].eq_ignore_ascii_case("TABLE")
        {
            return split_full_table_name(tokens[2]);
        }
        if tokens.len() >= 4
            && tokens[0].eq_ignore_ascii_case("CREATE")
            && tokens[1].eq_ignore_ascii_case("SHARED")
            && tokens[2].eq_ignore_ascii_case("TABLE")
        {
            return split_full_table_name(tokens[3]);
        }
        if tokens.len() >= 4
            && tokens[0].eq_ignore_ascii_case("CREATE")
            && tokens[1].eq_ignore_ascii_case("USER")
            && tokens[2].eq_ignore_ascii_case("TABLE")
        {
            return split_full_table_name(tokens[3]);
        }
        if tokens.len() >= 4
            && tokens[0].eq_ignore_ascii_case("CREATE")
            && tokens[1].eq_ignore_ascii_case("STREAM")
            && tokens[2].eq_ignore_ascii_case("TABLE")
        {
            return split_full_table_name(tokens[3]);
        }
        None
    }

    fn wait_for_cluster_after_sql(sql: &str) {
        if !should_wait_for_cluster_after_sql(sql) {
            return;
        }

        if let Some(namespace) = extract_created_namespace(sql) {
            let _ = wait_for_namespace_on_all_nodes(&namespace, 12000);
            return;
        }

        if let Some((namespace, table)) = extract_created_table(sql) {
            let _ = wait_for_table_on_all_nodes(&namespace, &table, 15000);
            return;
        }

        std::thread::sleep(Duration::from_millis(600));
    }

    fn ordered_urls_for_query(base_url: &str, sql: &str, enforce_leader: bool) -> Vec<String> {
        let mut urls = cluster_urls();
        if urls.is_empty() {
            return vec![base_url.to_string()];
        }

        if enforce_leader && !is_read_only_sql(sql) {
            if let Some(leader) = leader_url() {
                urls.retain(|url| url != &leader);
                let mut ordered = Vec::with_capacity(urls.len() + 1);
                ordered.push(leader);
                ordered.extend(urls);
                return ordered;
            }
        }

        urls.retain(|url| url != base_url);
        let mut ordered = Vec::with_capacity(urls.len() + 1);
        ordered.push(base_url.to_string());
        ordered.extend(urls);
        ordered
    }

    /// Execute SQL on a specific cluster node
    pub fn execute_on_node(base_url: &str, sql: &str) -> Result<String, String> {
        execute_on_node_internal(base_url, sql, true)
    }

    /// Execute SQL on a specific cluster node without leader routing
    #[allow(dead_code)]
    pub fn execute_on_node_raw(base_url: &str, sql: &str) -> Result<String, String> {
        execute_on_node_internal(base_url, sql, false)
    }

    fn execute_on_node_internal(
        base_url: &str,
        sql: &str,
        enforce_leader: bool,
    ) -> Result<String, String> {
        let started_at = std::time::Instant::now();
        let sql = sql.to_string();
        let mut last_err: Option<String> = None;
        let mut attempts = 0u64;
        let mut retryable_errors = 0u64;

        for _ in 0..5 {
            let urls = ordered_urls_for_query(base_url, &sql, enforce_leader);
            for url in urls.iter().cloned() {
                attempts += 1;
                let client = create_cluster_client(&url);
                let sql_value = sql.clone();
                match cluster_runtime().block_on(async move {
                    client.execute_query(&sql_value, None, None, None).await
                }) {
                    Ok(response) => {
                        if !response.success() {
                            let err_msg = response_error_message(&response);
                            if is_retryable_cluster_error_for_sql(&sql, &err_msg) {
                                retryable_errors += 1;
                                last_err = Some(err_msg);
                                continue;
                            }
                            record_cluster_helper_timing(
                                "execute_on_node",
                                &sql,
                                started_at.elapsed(),
                                attempts,
                                retryable_errors,
                            );
                            return Err(err_msg);
                        }
                        if is_truncated_read_response(&response, &sql) {
                            if let Some(leader) = leader_url() {
                                if url != leader {
                                    retryable_errors += 1;
                                    last_err = Some(format!(
                                        "Truncated read response from follower {}",
                                        url
                                    ));
                                    continue;
                                }
                            }
                        }
                        wait_for_cluster_after_sql(&sql);
                        record_cluster_helper_timing(
                            "execute_on_node",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Ok(serde_json::to_string_pretty(&response)
                            .unwrap_or_else(|_| format!("{:?}", response)));
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        if is_retryable_cluster_error_for_sql(&sql, &msg) {
                            retryable_errors += 1;
                            last_err = Some(msg);
                            continue;
                        }
                        record_cluster_helper_timing(
                            "execute_on_node",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Err(msg);
                    },
                }
            }
        }

        record_cluster_helper_timing(
            "execute_on_node",
            &sql,
            started_at.elapsed(),
            attempts,
            retryable_errors,
        );
        Err(last_err.unwrap_or_else(|| "All cluster nodes failed".to_string()))
    }

    /// Execute SQL on a specific cluster node and return the structured response
    pub fn execute_on_node_response(base_url: &str, sql: &str) -> Result<QueryResponse, String> {
        execute_on_node_response_internal(base_url, sql, true)
    }

    /// Execute SQL on a specific cluster node and return the structured response without leader
    /// routing
    #[allow(dead_code)]
    pub fn execute_on_node_response_raw(
        base_url: &str,
        sql: &str,
    ) -> Result<QueryResponse, String> {
        execute_on_node_response_internal(base_url, sql, false)
    }

    fn execute_on_node_response_internal(
        base_url: &str,
        sql: &str,
        enforce_leader: bool,
    ) -> Result<QueryResponse, String> {
        let started_at = std::time::Instant::now();
        let sql = sql.to_string();
        let mut last_err: Option<String> = None;
        let mut attempts = 0u64;
        let mut retryable_errors = 0u64;

        for _ in 0..5 {
            let urls = ordered_urls_for_query(base_url, &sql, enforce_leader);
            for url in urls.iter().cloned() {
                attempts += 1;
                let client = create_cluster_client(&url);
                let sql_value = sql.clone();
                match cluster_runtime().block_on(async move {
                    client.execute_query(&sql_value, None, None, None).await
                }) {
                    Ok(response) => {
                        if !response.success() {
                            let err_msg = response_error_message(&response);
                            if is_retryable_cluster_error_for_sql(&sql, &err_msg) {
                                retryable_errors += 1;
                                last_err = Some(err_msg);
                                continue;
                            }
                            record_cluster_helper_timing(
                                "execute_on_node_response",
                                &sql,
                                started_at.elapsed(),
                                attempts,
                                retryable_errors,
                            );
                            return Err(err_msg);
                        }
                        if is_truncated_read_response(&response, &sql) {
                            if let Some(leader) = leader_url() {
                                if url != leader {
                                    retryable_errors += 1;
                                    last_err = Some(format!(
                                        "Truncated read response from follower {}",
                                        url
                                    ));
                                    continue;
                                }
                            }
                        }
                        wait_for_cluster_after_sql(&sql);
                        record_cluster_helper_timing(
                            "execute_on_node_response",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Ok(response);
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        if is_retryable_cluster_error_for_sql(&sql, &msg) {
                            retryable_errors += 1;
                            last_err = Some(msg);
                            continue;
                        }
                        record_cluster_helper_timing(
                            "execute_on_node_response",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Err(msg);
                    },
                }
            }
        }

        record_cluster_helper_timing(
            "execute_on_node_response",
            &sql,
            started_at.elapsed(),
            attempts,
            retryable_errors,
        );
        Err(last_err.unwrap_or_else(|| "All cluster nodes failed".to_string()))
    }

    /// Execute SQL on a specific cluster node as a custom user
    pub fn execute_on_node_as_user(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
    ) -> Result<String, String> {
        execute_on_node_as_user_internal(base_url, username, password, sql, true)
    }

    /// Execute SQL on a specific cluster node as a custom user without leader routing
    #[allow(dead_code)]
    pub fn execute_on_node_as_user_raw(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
    ) -> Result<String, String> {
        execute_on_node_as_user_internal(base_url, username, password, sql, false)
    }

    fn execute_on_node_as_user_internal(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
        enforce_leader: bool,
    ) -> Result<String, String> {
        let started_at = std::time::Instant::now();
        let sql = sql.to_string();
        let mut last_err: Option<String> = None;
        let mut attempts = 0u64;
        let mut retryable_errors = 0u64;

        for _ in 0..5 {
            let urls = ordered_urls_for_query(base_url, &sql, enforce_leader);
            for url in urls.iter().cloned() {
                attempts += 1;
                let client = create_cluster_client_with_auth(&url, username, password);
                let sql_value = sql.clone();
                match cluster_runtime().block_on(async move {
                    client.execute_query(&sql_value, None, None, None).await
                }) {
                    Ok(response) => {
                        if !response.success() {
                            let err_msg = response_error_message(&response);
                            if is_retryable_cluster_error_for_sql(&sql, &err_msg) {
                                retryable_errors += 1;
                                last_err = Some(err_msg);
                                continue;
                            }
                            record_cluster_helper_timing(
                                "execute_on_node_as_user",
                                &sql,
                                started_at.elapsed(),
                                attempts,
                                retryable_errors,
                            );
                            return Err(err_msg);
                        }
                        wait_for_cluster_after_sql(&sql);
                        record_cluster_helper_timing(
                            "execute_on_node_as_user",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Ok(serde_json::to_string_pretty(&response)
                            .unwrap_or_else(|_| format!("{:?}", response)));
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        if is_retryable_cluster_error_for_sql(&sql, &msg) {
                            retryable_errors += 1;
                            last_err = Some(msg);
                            continue;
                        }
                        record_cluster_helper_timing(
                            "execute_on_node_as_user",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Err(msg);
                    },
                }
            }
        }

        record_cluster_helper_timing(
            "execute_on_node_as_user",
            &sql,
            started_at.elapsed(),
            attempts,
            retryable_errors,
        );
        Err(last_err.unwrap_or_else(|| "All cluster nodes failed".to_string()))
    }

    /// Execute SQL on a specific cluster node as a custom user and return the response
    pub fn execute_on_node_as_user_response(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
    ) -> Result<QueryResponse, String> {
        execute_on_node_as_user_response_internal(base_url, username, password, sql, true)
    }

    /// Execute SQL on a specific cluster node as a custom user and return the response without
    /// leader routing
    #[allow(dead_code)]
    pub fn execute_on_node_as_user_response_raw(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
    ) -> Result<QueryResponse, String> {
        execute_on_node_as_user_response_internal(base_url, username, password, sql, false)
    }

    fn execute_on_node_as_user_response_internal(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
        enforce_leader: bool,
    ) -> Result<QueryResponse, String> {
        let started_at = std::time::Instant::now();
        let sql = sql.to_string();
        let mut last_err: Option<String> = None;
        let mut attempts = 0u64;
        let mut retryable_errors = 0u64;

        for _ in 0..5 {
            let urls = ordered_urls_for_query(base_url, &sql, enforce_leader);
            for url in urls.iter().cloned() {
                attempts += 1;
                let client = create_cluster_client_with_auth(&url, username, password);
                let sql_value = sql.clone();
                match cluster_runtime().block_on(async move {
                    client.execute_query(&sql_value, None, None, None).await
                }) {
                    Ok(response) => {
                        if !response.success() {
                            let err_msg = response_error_message(&response);
                            if is_retryable_cluster_error_for_sql(&sql, &err_msg) {
                                retryable_errors += 1;
                                last_err = Some(err_msg);
                                continue;
                            }
                            record_cluster_helper_timing(
                                "execute_on_node_as_user_response",
                                &sql,
                                started_at.elapsed(),
                                attempts,
                                retryable_errors,
                            );
                            return Err(err_msg);
                        }
                        wait_for_cluster_after_sql(&sql);
                        record_cluster_helper_timing(
                            "execute_on_node_as_user_response",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Ok(response);
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        if is_retryable_cluster_error_for_sql(&sql, &msg) {
                            retryable_errors += 1;
                            last_err = Some(msg);
                            continue;
                        }
                        record_cluster_helper_timing(
                            "execute_on_node_as_user_response",
                            &sql,
                            started_at.elapsed(),
                            attempts,
                            retryable_errors,
                        );
                        return Err(msg);
                    },
                }
            }
        }

        record_cluster_helper_timing(
            "execute_on_node_as_user_response",
            &sql,
            started_at.elapsed(),
            attempts,
            retryable_errors,
        );
        Err(last_err.unwrap_or_else(|| "All cluster nodes failed".to_string()))
    }

    fn normalize_rows(rows: &[Vec<KalamCellValue>]) -> Vec<String> {
        let mut normalized: Vec<String> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| {
                        let extracted = extract_typed_value(v.inner());
                        match extracted {
                            Value::Null => "NULL".to_string(),
                            Value::String(s) => s,
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            other => other.to_string(),
                        }
                    })
                    .collect::<Vec<String>>()
                    .join("|")
            })
            .collect();

        normalized.sort();
        normalized
    }

    /// Fetch normalized row strings from a root-authenticated query
    pub fn fetch_normalized_rows(base_url: &str, sql: &str) -> Result<Vec<String>, String> {
        let response = execute_on_node_response(base_url, sql)?;
        let result = response.results.first().ok_or_else(|| "Missing query result".to_string())?;
        let rows = result.rows.as_ref().ok_or_else(|| "Missing row data".to_string())?;

        Ok(normalize_rows(rows))
    }

    /// Fetch normalized row strings from a user-authenticated query
    pub fn fetch_normalized_rows_as_user(
        base_url: &str,
        username: &str,
        password: &str,
        sql: &str,
    ) -> Result<Vec<String>, String> {
        let response = execute_on_node_as_user_response(base_url, username, password, sql)?;
        let result = response.results.first().ok_or_else(|| "Missing query result".to_string())?;
        let rows = result.rows.as_ref().ok_or_else(|| "Missing row data".to_string())?;

        Ok(normalize_rows(rows))
    }

    /// Check if a cluster node is healthy
    pub fn is_node_healthy(base_url: &str) -> bool {
        crate::common::is_cluster_url_reachable(base_url)
    }

    /// Require cluster to be running (skip test if not available)
    pub fn require_cluster_running() -> bool {
        let server_type = std::env::var("KALAMDB_SERVER_TYPE")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase());
        let cluster_requested = server_type.as_deref() == Some("cluster");

        // Critical: do NOT call is_cluster_mode()/test_context() before this check.
        // Under KALAMDB_SERVER_TYPE=fresh (main CI), test_context() auto-starts the
        // shared single-node server and blocks on auth — which is exactly what cluster
        // skip paths must avoid. Real cluster coverage lives in cli-cluster-e2e.
        if !cluster_requested {
            if matches!(server_type.as_deref(), Some("fresh") | Some("running")) {
                println!(
                    "\n  Skipping: KALAMDB_SERVER_TYPE={} (cluster tests require \
                     KALAMDB_SERVER_TYPE=cluster)\n",
                    server_type.as_deref().unwrap_or("unknown")
                );
                return false;
            }

            let has_multi_node_urls = std::env::var("KALAMDB_CLUSTER_URLS")
                .ok()
                .map(|raw| raw.split(',').map(str::trim).filter(|url| !url.is_empty()).count() > 1)
                .unwrap_or(false);
            if !has_multi_node_urls {
                println!(
                    "\n  Skipping: single-node server detected (cluster tests require \
                     multi-node)\n"
                );
                return false;
            }
        }

        if !crate::common::is_cluster_mode() {
            if cluster_requested {
                panic!(
                    "Cluster tests were requested, but the harness resolved single-node mode. \
                     Check KALAMDB_CLUSTER_URLS and cluster reachability."
                );
            }
            println!(
                "\n  Skipping: single-node server detected (cluster tests require multi-node)\n"
            );
            return false;
        }

        let urls = cluster_urls_config_order();
        if urls.is_empty() {
            if cluster_requested {
                panic!("Cluster tests were requested, but no cluster URLs are configured.");
            }
            println!("\n  Skipping: no cluster URLs configured (set KALAMDB_CLUSTER_URLS)\n");
            return false;
        }

        static CLUSTER_REACHABILITY_CHECK: OnceLock<bool> = OnceLock::new();
        let reachable =
            *CLUSTER_REACHABILITY_CHECK.get_or_init(|| urls.iter().any(|url| is_node_healthy(url)));
        if !reachable {
            if cluster_requested {
                panic!(
                    "Cluster tests were requested, but no configured cluster node is reachable: \
                     {:?}",
                    urls
                );
            }
            println!(
                "\n  Skipping: no cluster nodes are reachable. Expected nodes at: {:?}\n",
                urls
            );
            return false;
        }

        true
    }

    /// Wait for a table to be visible on all cluster nodes
    /// Returns true if table is visible on all nodes within timeout, false otherwise
    pub fn wait_for_table_on_all_nodes(namespace: &str, table_name: &str, timeout_ms: u64) -> bool {
        let urls = cluster_urls();
        let query = format!(
            "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
            namespace, table_name
        );

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

        while start.elapsed() < timeout {
            let all_visible = urls.iter().all(|url| {
                matches!(execute_on_node(url, &query), Ok(result) if result.contains(table_name))
            });

            if all_visible {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        false
    }

    /// Wait for a namespace to be visible on all cluster nodes
    pub fn wait_for_namespace_on_all_nodes(namespace: &str, timeout_ms: u64) -> bool {
        let urls = cluster_urls();
        let query = format!(
            "SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'",
            namespace
        );

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

        while start.elapsed() < timeout {
            let all_visible = urls.iter().all(|url| {
                matches!(execute_on_node(url, &query), Ok(result) if result.contains(namespace))
            });

            if all_visible {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        false
    }

    /// Wait for row count to reach expected value on all nodes
    pub fn wait_for_row_count_on_all_nodes(
        full_table: &str,
        expected: i64,
        timeout_ms: u64,
    ) -> bool {
        let urls = cluster_urls();
        let query = format!("SELECT count(*) as count FROM {}", full_table);

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

        while start.elapsed() < timeout {
            let all_match = urls
                .iter()
                .map(|url| query_count_on_url(url, &query))
                .all(|count| count == expected);

            if all_match {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        false
    }

    /// Wait for the latest job id for a job type to appear.
    pub fn wait_for_latest_job_id_by_type(
        base_url: &str,
        job_type: &str,
        timeout: Duration,
    ) -> Option<String> {
        let start = std::time::Instant::now();
        let timeout = extend_job_timeout(timeout);
        let job_type = job_type.to_lowercase();
        let sql = format!(
            "SELECT job_id FROM system.jobs WHERE job_type = '{}' ORDER BY created_at DESC LIMIT 1",
            job_type
        );

        while start.elapsed() < timeout {
            if let Ok(response) = execute_on_node_response(base_url, &sql) {
                if let Some(result) = response.results.first() {
                    if let Some(rows) = &result.rows {
                        if let Some(row) = rows.first() {
                            if let Some(value) = row.first() {
                                let extracted = extract_typed_value(value);
                                if let Some(job_id) = extracted.as_str() {
                                    return Some(job_id.to_string());
                                }
                            }
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        None
    }

    /// Wait for a job to reach a specific status.
    pub fn wait_for_job_status(
        base_url: &str,
        job_id: &str,
        status: &str,
        timeout: Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        let timeout = extend_job_timeout(timeout);
        let sql = format!("SELECT status FROM system.jobs WHERE job_id = '{}' LIMIT 1", job_id);

        while start.elapsed() < timeout {
            if let Ok(response) = execute_on_node_response(base_url, &sql) {
                if let Some(result) = response.results.first() {
                    if let Some(rows) = &result.rows {
                        if let Some(row) = rows.first() {
                            if let Some(value) = row.first() {
                                if extract_typed_value(value)
                                    .as_str()
                                    .map(|s| s.eq_ignore_ascii_case(status))
                                    .unwrap_or(false)
                                {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        false
    }

    fn extend_job_timeout(timeout: Duration) -> Duration {
        if cluster_urls().len() > 1 {
            timeout + Duration::from_secs(12)
        } else {
            timeout
        }
    }
}

#[path = "cluster/cluster_test_cluster_list.rs"]
mod cluster_test_cluster_list;
#[path = "cluster/cluster_test_consistency.rs"]
mod cluster_test_consistency;
#[path = "cluster/cluster_test_data_digest.rs"]
mod cluster_test_data_digest;
#[path = "cluster/cluster_test_failover.rs"]
mod cluster_test_failover;
#[path = "cluster/cluster_test_final_consistency.rs"]
mod cluster_test_final_consistency;
#[path = "cluster/cluster_test_flush.rs"]
mod cluster_test_flush;
#[path = "cluster/cluster_test_leader_jobs.rs"]
mod cluster_test_leader_jobs;
#[path = "cluster/cluster_test_multi_node_smoke.rs"]
mod cluster_test_multi_node_smoke;
#[path = "cluster/cluster_test_node_rejoin.rs"]
mod cluster_test_node_rejoin;
#[path = "cluster/cluster_test_replication.rs"]
mod cluster_test_replication;
#[path = "cluster/cluster_test_snapshot.rs"]
mod cluster_test_snapshot;
#[path = "cluster/cluster_test_subscription_nodes.rs"]
mod cluster_test_subscription_nodes;
#[path = "cluster/cluster_test_system_tables_replication.rs"]
mod cluster_test_system_tables_replication;
#[path = "cluster/cluster_test_table_crud_consistency.rs"]
mod cluster_test_table_crud_consistency;
#[path = "cluster/cluster_test_table_identity.rs"]
mod cluster_test_table_identity;
#[path = "cluster/cluster_test_ws_follower.rs"]
mod cluster_test_ws_follower;
