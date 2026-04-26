use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use log::{debug, warn};
use serde::{Deserialize, Serialize};
use serde_json;

use crate::{
    auth::AuthProvider,
    consumer::{
        models::{consumer_record::ConsumerRecordWire, AckResponse, AutoOffsetReset, CommitResult},
        utils::backoff::jittered_exponential_backoff,
    },
    error::{KalamLinkError, Result},
    models::LoginResponse,
};

#[derive(Clone)]
pub struct ConsumerPoller {
    consume_url: String,
    ack_url: String,
    login_url: String,
    http_client: reqwest::Client,
    auth: Arc<Mutex<AuthProvider>>,
    password_auth: Option<(String, String)>,
    request_timeout: Duration,
    retry_backoff: Duration,
    max_retries: u32,
}

impl ConsumerPoller {
    pub fn new(
        base_url: &str,
        http_client: reqwest::Client,
        auth: AuthProvider,
        request_timeout: Duration,
        retry_backoff: Duration,
    ) -> Self {
        let password_auth = match &auth {
            AuthProvider::BasicAuth(user, password) => Some((user.clone(), password.clone())),
            _ => None,
        };

        Self {
            consume_url: format!("{}/v1/api/topics/consume", base_url.trim_end_matches('/')),
            ack_url: format!("{}/v1/api/topics/ack", base_url.trim_end_matches('/')),
            login_url: format!("{}/v1/api/auth/login", base_url.trim_end_matches('/')),
            http_client,
            auth: Arc::new(Mutex::new(auth)),
            password_auth,
            request_timeout,
            retry_backoff,
            max_retries: 3,
        }
    }

    async fn login_with_password_auth(&self, user: &str, password: &str) -> Result<AuthProvider> {
        let response = self
            .http_client
            .post(&self.login_url)
            .timeout(self.request_timeout)
            .json(&serde_json::json!({
                "user": user,
                "password": password,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message: error_text,
            });
        }

        let login_response = response.json::<LoginResponse>().await?;
        let auth = AuthProvider::jwt_token(login_response.access_token);
        *self.auth.lock().unwrap() = auth.clone();
        Ok(auth)
    }

    async fn ensure_request_auth(&self) -> Result<AuthProvider> {
        let current_auth = self.auth.lock().unwrap().clone();
        match current_auth {
            AuthProvider::BasicAuth(user, password) => {
                self.login_with_password_auth(&user, &password).await
            },
            auth => Ok(auth),
        }
    }

    async fn refresh_auth_if_possible(&self) -> Result<bool> {
        let Some((user, password)) = self.password_auth.clone() else {
            return Ok(false);
        };

        self.login_with_password_auth(&user, &password).await?;
        Ok(true)
    }

    pub(crate) async fn consume(&self, request: ConsumeRequest) -> Result<ConsumeResponse> {
        let mut attempt: u32 = 0;
        let max_retries = self.max_retries;

        loop {
            let auth_snapshot = self.ensure_request_auth().await?;
            let mut req_builder = self.http_client.post(&self.consume_url).json(&request);
            req_builder = req_builder.timeout(self.request_timeout);
            req_builder = auth_snapshot.apply_to_request(req_builder)?;

            let attempt_start = std::time::Instant::now();
            debug!("[LINK_CONSUMER] consume request attempt {}/{}", attempt + 1, max_retries + 1);

            match req_builder.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let bytes = response.bytes().await?;
                        if bytes.is_empty() {
                            return Ok(ConsumeResponse {
                                messages: Vec::new(),
                                next_offset: 0,
                                has_more: false,
                            });
                        }
                        match serde_json::from_slice::<ConsumeResponse>(&bytes) {
                            Ok(result) => return Ok(result),
                            Err(_) => {
                                let body = String::from_utf8_lossy(&bytes);
                                return Err(KalamLinkError::ServerError {
                                    status_code: status.as_u16(),
                                    message: body.to_string(),
                                });
                            },
                        }
                    }

                    let error_text =
                        response.text().await.unwrap_or_else(|_| "Unknown error".to_string());

                    if status.is_client_error() {
                        if status.as_u16() == 401
                            && attempt < max_retries
                            && self.refresh_auth_if_possible().await?
                        {
                            attempt += 1;
                            continue;
                        }

                        return Err(KalamLinkError::ServerError {
                            status_code: status.as_u16(),
                            message: error_text,
                        });
                    }

                    if attempt < max_retries && is_retriable_status(status.as_u16()) {
                        let delay = jittered_exponential_backoff(
                            self.retry_backoff,
                            attempt,
                            Duration::from_secs(10),
                        );
                        warn!(
                            "[LINK_CONSUMER] Retriable consume error: status={} delay_ms={} \
                             duration_ms={}",
                            status,
                            delay.as_millis(),
                            attempt_start.elapsed().as_millis()
                        );
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    return Err(KalamLinkError::ServerError {
                        status_code: status.as_u16(),
                        message: error_text,
                    });
                },
                Err(err) if is_retriable_error(&err) && attempt < max_retries => {
                    let delay = jittered_exponential_backoff(
                        self.retry_backoff,
                        attempt,
                        Duration::from_secs(10),
                    );
                    warn!(
                        "[LINK_CONSUMER] Retriable consume error: {} delay_ms={} duration_ms={}",
                        err,
                        delay.as_millis(),
                        attempt_start.elapsed().as_millis()
                    );
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                },
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub async fn ack(&self, request: AckRequest) -> Result<CommitResult> {
        let auth_snapshot = self.ensure_request_auth().await?;
        let mut req_builder = self.http_client.post(&self.ack_url).json(&request);
        req_builder = req_builder.timeout(self.request_timeout);
        req_builder = auth_snapshot.apply_to_request(req_builder)?;

        let response = req_builder.send().await?;
        let status = response.status();

        if status.is_success() {
            let ack_response = response.json::<AckResponse>().await?;
            return Ok(CommitResult {
                acknowledged_offset: ack_response.acknowledged_offset,
                group_id: request.group_id,
                partition_id: request.partition_id,
            });
        }

        let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());

        if status.as_u16() == 401 && self.refresh_auth_if_possible().await? {
            let refreshed_auth = self.ensure_request_auth().await?;
            let mut retry_builder = self.http_client.post(&self.ack_url).json(&request);
            retry_builder = retry_builder.timeout(self.request_timeout);
            retry_builder = refreshed_auth.apply_to_request(retry_builder)?;

            let retry_response = retry_builder.send().await?;
            let retry_status = retry_response.status();
            if retry_status.is_success() {
                let ack_response = retry_response.json::<AckResponse>().await?;
                return Ok(CommitResult {
                    acknowledged_offset: ack_response.acknowledged_offset,
                    group_id: request.group_id,
                    partition_id: request.partition_id,
                });
            }

            let retry_error_text =
                retry_response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(KalamLinkError::ServerError {
                status_code: retry_status.as_u16(),
                message: retry_error_text,
            });
        }

        Err(KalamLinkError::ServerError {
            status_code: status.as_u16(),
            message: error_text,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumeRequest {
    pub topic_id: String,
    pub group_id: String,
    pub start: AutoOffsetReset,
    pub limit: u32,
    pub partition_id: u32,
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsumeResponse {
    pub messages: Vec<ConsumerRecordWire>,
    pub next_offset: u64,
    #[allow(dead_code)] // deserialized from JSON; reserved for pagination
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AckRequest {
    pub topic_id: String,
    pub group_id: String,
    pub partition_id: u32,
    pub upto_offset: u64,
}

fn is_retriable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

fn is_retriable_status(status_code: u16) -> bool {
    matches!(status_code, 500 | 502 | 503 | 504)
}
