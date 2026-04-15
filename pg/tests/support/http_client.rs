use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;

#[derive(Clone)]
pub struct TestHttpClient {
    inner: Client<HttpConnector, Full<Bytes>>,
    timeout: Duration,
}

pub struct TestHttpResponse {
    pub status: StatusCode,
    pub body: String,
}

impl TestHttpClient {
    pub fn new(timeout: Duration) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(timeout));

        Self {
            inner: Client::builder(TokioExecutor::new()).build(connector),
            timeout,
        }
    }

    pub async fn get(&self, url: &str) -> Result<TestHttpResponse, String> {
        self.send(Method::GET, url, None, None).await
    }

    pub async fn post_json(
        &self,
        url: &str,
        payload: &Value,
        bearer_token: Option<&str>,
    ) -> Result<TestHttpResponse, String> {
        let body = serde_json::to_vec(payload)
            .map(Bytes::from)
            .map_err(|error| format!("serialize JSON body for {url}: {error}"))?;

        self.send(Method::POST, url, Some(body), bearer_token).await
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        body: Option<Bytes>,
        bearer_token: Option<&str>,
    ) -> Result<TestHttpResponse, String> {
        let mut request = Request::builder().method(method).uri(url);

        if body.is_some() {
            request = request.header(CONTENT_TYPE, "application/json");
        }

        if let Some(token) = bearer_token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }

        let request = request
            .body(Full::from(body.unwrap_or_default()))
            .map_err(|error| format!("build request for {url}: {error}"))?;

        let response = tokio::time::timeout(self.timeout, self.inner.request(request))
            .await
            .map_err(|_| format!("request to {url} timed out after {:?}", self.timeout))?
            .map_err(|error| format!("request to {url} failed: {error}"))?;

        let status = response.status();
        let body = tokio::time::timeout(self.timeout, response.into_body().collect())
            .await
            .map_err(|_| format!("reading response from {url} timed out after {:?}", self.timeout))?
            .map_err(|error| format!("reading response from {url} failed: {error}"))?
            .to_bytes();

        Ok(TestHttpResponse {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        })
    }
}
