#[cfg(feature = "healthcheck")]
use std::time::Instant;

use super::KalamLinkClient;
#[cfg(feature = "healthcheck")]
use super::HEALTH_CHECK_TTL;
use crate::error::{KalamLinkError, Result};
#[cfg(feature = "cluster")]
use crate::models::ClusterHealthResponse;
#[cfg(feature = "healthcheck")]
use crate::models::HealthCheckResponse;

impl KalamLinkClient {
    /// Check server health and get server information
    #[cfg(feature = "healthcheck")]
    pub async fn health_check(&self) -> Result<HealthCheckResponse> {
        {
            let cache = self.health_cache.lock().await;
            if let (Some(last_check), Some(response)) =
                (cache.last_check, cache.last_response.clone())
            {
                if last_check.elapsed() < HEALTH_CHECK_TTL {
                    log::debug!(
                        "[HEALTH_CHECK] Returning cached response (age: {:?})",
                        last_check.elapsed()
                    );
                    return Ok(response);
                }
            }
        }

        let url = format!("{}/v1/api/healthcheck", self.base_url);
        log::debug!("[HEALTH_CHECK] Fetching from url={}", url);
        let start = std::time::Instant::now();
        let response = self.http_client.get(&url).send().await?;
        log::debug!(
            "[HEALTH_CHECK] HTTP response received in {:?}, status={}",
            start.elapsed(),
            response.status()
        );
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = if status.as_u16() == 403 {
                format!("Health check endpoint is restricted to localhost connections ({})", body)
            } else {
                format!("HTTP {} — {}", status, body)
            };
            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message,
            });
        }
        let health_response = response.json::<HealthCheckResponse>().await?;
        log::debug!("[HEALTH_CHECK] JSON parsed in {:?}", start.elapsed());

        let mut cache = self.health_cache.lock().await;
        cache.last_check = Some(Instant::now());
        cache.last_response = Some(health_response.clone());

        Ok(health_response)
    }

    /// Fetch cluster-aware health information with per-node runtime metrics.
    #[cfg(feature = "cluster")]
    pub async fn cluster_health_check(&self) -> Result<ClusterHealthResponse> {
        let url = format!("{}/v1/api/cluster/health", self.base_url);
        let response = self.http_client.get(&url).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = if status.as_u16() == 403 {
                format!("Cluster health endpoint is restricted to localhost connections ({})", body)
            } else {
                format!("HTTP {} — {}", status, body)
            };
            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message,
            });
        }

        Ok(response.json::<ClusterHealthResponse>().await?)
    }

    /// Login with user and password to obtain a JWT token.
    #[cfg(feature = "auth-flows")]
    pub async fn login(&self, user: &str, password: &str) -> Result<crate::models::LoginResponse> {
        let url = format!("{}/v1/api/auth/login", self.base_url);
        log::debug!("[LOGIN] Authenticating user '{}' at url={}", user, url);

        let login_request = crate::models::LoginRequest {
            user: kalamdb_commons::UserId::from(user),
            password: password.to_string(),
        };

        let start = std::time::Instant::now();
        let response = self.http_client.post(&url).json(&login_request).send().await?;

        let status = response.status();
        log::debug!("[LOGIN] HTTP response received in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            log::debug!("[LOGIN] Login failed: {}", error_text);

            if status.as_u16() == 428 {
                let message = if let Ok(error_json) =
                    serde_json::from_str::<serde_json::Value>(&error_text)
                {
                    error_json
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Server requires initial setup")
                        .to_string()
                } else {
                    "Server requires initial setup".to_string()
                };
                return Err(KalamLinkError::SetupRequired(message));
            }

            return Err(KalamLinkError::AuthenticationError(format!(
                "Login failed ({}): {}",
                status, error_text
            )));
        }

        let login_response = response.json::<crate::models::LoginResponse>().await?;
        log::debug!("[LOGIN] Successfully authenticated user '{}' in {:?}", user, start.elapsed());

        Ok(login_response)
    }

    /// Refresh an access token using a refresh token.
    #[cfg(feature = "auth-flows")]
    pub async fn refresh_access_token(
        &self,
        refresh_token: &str,
    ) -> Result<crate::models::LoginResponse> {
        let url = format!("{}/v1/api/auth/refresh", self.base_url);
        log::debug!("[REFRESH] Refreshing access token at url={}", url);

        let start = std::time::Instant::now();
        let response = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", refresh_token))
            .send()
            .await?;

        let status = response.status();
        log::debug!("[REFRESH] HTTP response received in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            log::debug!("[REFRESH] Token refresh failed: {}", error_text);
            return Err(KalamLinkError::AuthenticationError(format!(
                "Token refresh failed ({}): {}",
                status, error_text
            )));
        }

        let login_response = response.json::<crate::models::LoginResponse>().await?;
        log::debug!("[REFRESH] Successfully refreshed token in {:?}", start.elapsed());

        Ok(login_response)
    }

    /// Check if the server requires initial setup.
    #[cfg(feature = "setup")]
    pub async fn check_setup_status(&self) -> Result<crate::models::SetupStatusResponse> {
        let url = format!("{}/v1/api/auth/status", self.base_url);
        log::debug!("[SETUP] Checking setup status at url={}", url);

        let start = std::time::Instant::now();
        let response = self.http_client.get(&url).send().await?;

        let status = response.status();
        log::debug!("[SETUP] Status check response in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message: error_text,
            });
        }

        Ok(response.json::<crate::models::SetupStatusResponse>().await?)
    }

    /// Perform initial server setup.
    #[cfg(feature = "setup")]
    pub async fn server_setup(
        &self,
        request: crate::models::ServerSetupRequest,
    ) -> Result<crate::models::ServerSetupResponse> {
        let url = format!("{}/v1/api/auth/setup", self.base_url);
        log::debug!("[SETUP] Performing server setup at url={}", url);

        let start = std::time::Instant::now();
        let response = self.http_client.post(&url).json(&request).send().await?;

        let status = response.status();
        log::debug!("[SETUP] Setup response in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());

            if status.as_u16() == 409 {
                return Err(KalamLinkError::ConfigurationError(
                    "Server is already configured".to_string(),
                ));
            }
            if status.as_u16() == 400 {
                if let Ok(error_json) = serde_json::from_str::<serde_json::Value>(&error_text) {
                    if let Some(message) = error_json.get("message").and_then(|m| m.as_str()) {
                        return Err(KalamLinkError::ConfigurationError(message.to_string()));
                    }
                }
            }

            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message: error_text,
            });
        }

        let setup_response = response.json::<crate::models::ServerSetupResponse>().await?;
        log::info!("[SETUP] Server setup complete: {}", setup_response.message);

        Ok(setup_response)
    }
}
