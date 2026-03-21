//! SQL query execution via HTTP.

use crate::{
    auth::AuthProvider,
    error::{KalamLinkError, Result},
    models::{QueryRequest, QueryResponse, UploadProgress},
};
use bytes::Bytes;
use http_body::Frame;
use http_body_util::StreamBody;
use log::{debug, warn};
use reqwest::multipart::{Form, Part};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

/// Async callback that resolves fresh [`AuthProvider`] credentials.
///
/// Called by the executor when a query returns `TOKEN_EXPIRED`.
/// Implementations should obtain a fresh JWT (e.g. via login or dynamic
/// auth provider) and return it.
pub type AuthRefreshCallback = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<AuthProvider>> + Send>> + Send + Sync,
>;

/// Handles SQL query execution via HTTP.
#[derive(Clone)]
pub struct QueryExecutor {
    sql_url: String,
    http_client: reqwest::Client,
    auth: Arc<Mutex<AuthProvider>>,
    max_retries: u32,
    auth_refresher: Option<AuthRefreshCallback>,
}

/// Progress callback for multipart file uploads.
pub type UploadProgressCallback = Arc<dyn Fn(UploadProgress) + Send + Sync>;

fn build_progress_stream(
    data: Arc<Vec<u8>>,
    file_name: Arc<str>,
    file_index: usize,
    total_files: usize,
    progress_cb: UploadProgressCallback,
) -> impl futures_util::Stream<Item = std::result::Result<Frame<Bytes>, std::io::Error>> + Send + 'static
{
    let chunk_size = 64 * 1024;
    futures_util::stream::unfold(0usize, move |offset| {
        let data = Arc::clone(&data);
        let progress_cb = progress_cb.clone();
        let file_name = Arc::clone(&file_name);
        async move {
            if offset >= data.len() {
                return None;
            }

            let end = (offset + chunk_size).min(data.len());
            let chunk = Bytes::copy_from_slice(&data[offset..end]);
            let total_bytes = data.len() as u64;
            let bytes_sent = end as u64;
            let percent = if total_bytes == 0 {
                100.0
            } else {
                (bytes_sent as f64 / total_bytes as f64) * 100.0
            };

            (progress_cb)(UploadProgress {
                file_index,
                total_files,
                file_name: file_name.to_string(),
                bytes_sent,
                total_bytes,
                percent,
            });

            Some((Ok(Frame::data(chunk)), end))
        }
    })
}

impl QueryExecutor {
    pub(crate) fn new(
        base_url: String,
        http_client: reqwest::Client,
        auth: AuthProvider,
        max_retries: u32,
    ) -> Self {
        Self {
            sql_url: format!("{}/v1/api/sql", base_url),
            http_client,
            auth: Arc::new(Mutex::new(auth)),
            max_retries,
            auth_refresher: None,
        }
    }

    pub(crate) fn set_auth(&self, auth: AuthProvider) {
        *self.auth.lock().unwrap() = auth;
    }

    pub(crate) fn set_auth_refresher(&mut self, refresher: AuthRefreshCallback) {
        self.auth_refresher = Some(refresher);
    }

    fn is_retry_safe_sql(sql: &str) -> bool {
        matches!(
            Self::first_keyword(sql).as_deref(),
            Some("SELECT" | "SHOW" | "DESCRIBE" | "EXPLAIN")
        )
    }

    fn first_keyword(sql: &str) -> Option<String> {
        let bytes = sql.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            // Skip whitespace
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i >= bytes.len() {
                return None;
            }

            // Skip line comments: -- ...\n
            if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }

            // Skip block comments: /* ... */
            if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                i += 2;
                while i + 1 < bytes.len() {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            // Read keyword
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
                i += 1;
            }
            if start == i {
                return None;
            }
            return Some(sql[start..i].to_ascii_uppercase());
        }

        None
    }

    /// Execute a SQL query with optional parameters and namespace.
    pub async fn execute(
        &self,
        sql: &str,
        files: Option<Vec<(String, String, Vec<u8>, Option<String>)>>,
        params: Option<Vec<serde_json::Value>>,
        namespace_id: Option<String>,
    ) -> Result<QueryResponse> {
        self.execute_with_progress(sql, files, params, namespace_id, None).await
    }

    /// Execute a SQL query with optional parameters and namespace, with upload progress callback.
    pub async fn execute_with_progress(
        &self,
        sql: &str,
        files: Option<Vec<(String, String, Vec<u8>, Option<String>)>>,
        params: Option<Vec<serde_json::Value>>,
        namespace_id: Option<String>,
        progress: Option<UploadProgressCallback>,
    ) -> Result<QueryResponse> {
        let has_files = files.as_ref().map(|f| !f.is_empty()).unwrap_or(false);

        if has_files {
            let mut form = Form::new().text("sql", sql.to_string());

            if let Some(p) = &params {
                form = form.text("params", serde_json::to_string(p)?);
            }

            if let Some(ns) = &namespace_id {
                form = form.text("namespace_id", ns.clone());
            }

            if let Some(files) = files {
                let total_files = files.len();
                for (index, (placeholder_name, filename, data, mime_type)) in
                    files.into_iter().enumerate()
                {
                    let total_bytes = data.len() as u64;
                    let field_name = format!("file:{}", placeholder_name);

                    let part = if let Some(progress_cb) = progress.clone() {
                        let data = Arc::new(data);
                        let file_name = Arc::<str>::from(filename.clone());
                        let file_index = index + 1;

                        let stream = build_progress_stream(
                            Arc::clone(&data),
                            Arc::clone(&file_name),
                            file_index,
                            total_files,
                            progress_cb,
                        );

                        let body = reqwest::Body::wrap(StreamBody::new(stream));
                        Part::stream_with_length(body, total_bytes)
                    } else {
                        Part::bytes(data)
                    };

                    let part = part
                        .file_name(filename)
                        .mime_str(mime_type.as_deref().unwrap_or("application/octet-stream"))
                        .map_err(|e| {
                            KalamLinkError::ConfigurationError(format!("Invalid MIME type: {}", e))
                        })?;

                    form = form.part(field_name, part);
                }
            }

            let auth_snapshot = self.auth.lock().unwrap().clone();
            let mut req_builder = self.http_client.post(&self.sql_url).multipart(form);
            req_builder = auth_snapshot.apply_to_request(req_builder)?;

            let attempt_start = Instant::now();
            debug!("[LINK_HTTP] Sending multipart POST to {}", self.sql_url);

            let response = req_builder.send().await?;
            let http_duration_ms = attempt_start.elapsed().as_millis();
            debug!(
                "[LINK_HTTP] Response received: status={} duration_ms={}",
                response.status(),
                http_duration_ms
            );

            let result = Self::handle_response(response, sql).await?;

            // Auto-refresh on TOKEN_EXPIRED (multipart — no retry, just report).
            // Multipart uploads consume the body so we cannot replay them, but
            // we still refresh the token so the *next* request succeeds.
            if result.is_token_expired() {
                if let Some(refresher) = &self.auth_refresher {
                    warn!("[LINK_HTTP] TOKEN_EXPIRED on multipart request — refreshing auth for subsequent requests");
                    if let Ok(new_auth) = refresher().await {
                        *self.auth.lock().unwrap() = new_auth;
                    }
                }
            }

            return Ok(result);
        }

        let request = QueryRequest {
            sql: sql.to_string(),
            params,
            namespace_id,
        };

        let mut retries: u32 = 0;
        let max_retries = self.max_retries;
        let retry_safe_sql = Self::is_retry_safe_sql(sql);

        let sql_preview = if sql.len() > 80 {
            format!("{}...", &sql[..80])
        } else {
            sql.to_string()
        };
        debug!(
            "[LINK_QUERY] Starting query: \"{}\" (len={})",
            sql_preview.replace('\n', " "),
            sql.len()
        );

        let overall_start = Instant::now();

        loop {
            let auth_snapshot = self.auth.lock().unwrap().clone();
            let mut req_builder = self.http_client.post(&self.sql_url).json(&request);
            req_builder = auth_snapshot.apply_to_request(req_builder)?;

            let attempt_start = Instant::now();
            debug!(
                "[LINK_HTTP] Sending POST to {} (attempt {}/{})",
                self.sql_url,
                retries + 1,
                max_retries + 1
            );

            match req_builder.send().await {
                Ok(response) => {
                    let http_duration_ms = attempt_start.elapsed().as_millis();
                    debug!(
                        "[LINK_HTTP] Response received: status={} duration_ms={}",
                        response.status(),
                        http_duration_ms
                    );

                    let result = Self::handle_response(response, sql).await;

                    // Auto-refresh on TOKEN_EXPIRED: get fresh auth and retry once.
                    if let Ok(ref resp) = result {
                        if resp.is_token_expired() {
                            if let Some(refresher) = &self.auth_refresher {
                                warn!("[LINK_HTTP] TOKEN_EXPIRED — reauthenticating and retrying");
                                match refresher().await {
                                    Ok(new_auth) => {
                                        *self.auth.lock().unwrap() = new_auth.clone();
                                        // Retry exactly once with fresh credentials.
                                        let mut retry_builder = self.http_client.post(&self.sql_url).json(&request);
                                        retry_builder = new_auth.apply_to_request(retry_builder)?;
                                        match retry_builder.send().await {
                                            Ok(retry_resp) => return Self::handle_response(retry_resp, sql).await,
                                            Err(e) => return Err(e.into()),
                                        }
                                    },
                                    Err(e) => {
                                        warn!("[LINK_HTTP] Auth refresh failed: {}", e);
                                        // Return the original TOKEN_EXPIRED response.
                                    },
                                }
                            }
                        }
                    }

                    if result.is_ok() {
                        let total_duration_ms = overall_start.elapsed().as_millis();
                        debug!(
                            "[LINK_QUERY] Success: http_ms={} total_ms={}",
                            http_duration_ms, total_duration_ms
                        );
                    }

                    return result;
                },
                Err(e) if retry_safe_sql && retries < max_retries && Self::is_retriable(&e) => {
                    let http_duration_ms = attempt_start.elapsed().as_millis();
                    warn!(
                        "[LINK_HTTP] Retriable error (attempt {}/{}): {} duration_ms={}",
                        retries + 1,
                        max_retries + 1,
                        e,
                        http_duration_ms
                    );
                    retries += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * retries as u64))
                        .await;
                    continue;
                },
                Err(e) => {
                    let http_duration_ms = attempt_start.elapsed().as_millis();
                    warn!(
                        "[LINK_HTTP] Fatal error: {} duration_ms={} total_ms={}",
                        e,
                        http_duration_ms,
                        overall_start.elapsed().as_millis()
                    );
                    return Err(e.into());
                },
            }
        }
    }

    fn is_retriable(err: &reqwest::Error) -> bool {
        err.is_timeout() || err.is_connect()
    }

    async fn handle_response(response: reqwest::Response, _sql: &str) -> Result<QueryResponse> {
        let status = response.status();

        if status.is_success() {
            let parse_start = Instant::now();
            let query_response: QueryResponse = response.json().await?;
            let parse_duration_ms = parse_start.elapsed().as_millis();
            debug!("[LINK_QUERY] Parsed response in {}ms", parse_duration_ms);
            return Ok(query_response);
        }

        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());

        if status.is_client_error() {
            let status_code = status.as_u16();
            warn!(
                "[LINK_HTTP] Authentication/client error: status={} message=\"{}\"",
                status, error_text
            );
            return Err(KalamLinkError::ServerError {
                status_code,
                message: error_text,
            });
        }

        if let Ok(json_response) = serde_json::from_str::<QueryResponse>(&error_text) {
            return Ok(json_response);
        }

        warn!("[LINK_HTTP] Server error: status={} message=\"{}\"", status, error_text);

        Err(KalamLinkError::ServerError {
            status_code: status.as_u16(),
            message: error_text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{build_progress_stream, UploadProgress, UploadProgressCallback};
    use futures_util::StreamExt;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn progress_stream_reports_completion() {
        let data = Arc::new(vec![1u8; 128 * 1024]);
        let file_name = Arc::<str>::from("example.txt");
        let last_progress = Arc::new(Mutex::new(None::<UploadProgress>));

        let last_progress_clone = Arc::clone(&last_progress);
        let progress_cb: UploadProgressCallback = Arc::new(move |progress| {
            *last_progress_clone.lock().unwrap() = Some(progress);
        });

        let stream =
            build_progress_stream(Arc::clone(&data), Arc::clone(&file_name), 2, 3, progress_cb);

        futures_util::pin_mut!(stream);
        while let Some(frame) = stream.next().await {
            frame.unwrap();
        }

        let progress = last_progress.lock().unwrap().clone().expect("no progress reported");
        assert_eq!(progress.file_index, 2);
        assert_eq!(progress.total_files, 3);
        assert_eq!(progress.file_name, "example.txt");
        assert_eq!(progress.total_bytes, data.len() as u64);
        assert_eq!(progress.bytes_sent, data.len() as u64);
        assert!((progress.percent - 100.0).abs() < f64::EPSILON);
    }
}
