use std::{collections::HashMap, sync::Arc};

use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use super::*;
use crate::error::KalamLinkError;

#[test]
fn test_builder_pattern() {
    let result = KalamLinkClient::builder()
        .base_url("http://localhost:3000")
        .timeout(Duration::from_secs(10))
        .jwt_token("test_token")
        .build();

    assert!(result.is_ok());
}

#[test]
fn test_builder_missing_url() {
    let result = KalamLinkClient::builder().build();
    assert!(result.is_err());
}

#[test]
fn test_builder_with_ws_lazy_connect() {
    let client = KalamLinkClient::builder()
        .base_url("http://localhost:3000")
        .jwt_token("test_token")
        .connection_options(ConnectionOptions::new().with_ws_lazy_connect(true))
        .build()
        .expect("build should succeed");

    assert!(client.connection_options.ws_lazy_connect);
}

#[test]
fn test_builder_default_ws_lazy_connect_is_true() {
    let client = KalamLinkClient::builder()
        .base_url("http://localhost:3000")
        .jwt_token("test_token")
        .build()
        .expect("build should succeed");

    assert!(client.connection_options.ws_lazy_connect);
}

#[derive(Debug, Default)]
struct QueryAuthState {
    headers: Vec<String>,
}

#[tokio::test]
async fn test_set_auth_updates_http_query_executor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let address = listener.local_addr().expect("read local addr");
    let state = Arc::new(Mutex::new(QueryAuthState::default()));
    let state_clone = Arc::clone(&state);

    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let state = Arc::clone(&state_clone);
            tokio::spawn(async move {
                let _ = handle_test_request(stream, state).await;
            });
        }
    });

    let mut client = KalamLinkClient::builder()
        .base_url(format!("http://{}", address))
        .jwt_token("expired-token")
        .build()
        .expect("build client");

    let first_error = client
        .execute_query("SELECT 1", None, None, None)
        .await
        .expect_err("expired token should fail");
    assert!(matches!(
        first_error,
        KalamLinkError::ServerError {
            status_code: 401,
            ..
        }
    ));

    client.set_auth(AuthProvider::jwt_token("fresh-token".to_string()));

    let response = client
        .execute_query("SELECT 1", None, None, None)
        .await
        .expect("fresh token should succeed");
    assert!(response.success());

    let recorded_headers = state.lock().await.headers.clone();
    assert_eq!(
        recorded_headers,
        vec![
            "Bearer expired-token".to_string(),
            "Bearer fresh-token".to_string()
        ]
    );

    server.abort();
}

async fn handle_test_request(
    mut stream: TcpStream,
    state: Arc<Mutex<QueryAuthState>>,
) -> std::io::Result<()> {
    let request = read_test_request(&mut stream).await?;
    let authorization = request.headers.get("authorization").cloned().unwrap_or_default();
    state.lock().await.headers.push(authorization.clone());

    let (status_line, body) = match authorization.as_str() {
        "Bearer fresh-token" => (
            "HTTP/1.1 200 OK",
            json!({
                "status": "success",
                "results": [{
                    "schema": [{
                        "name": "value",
                        "data_type": "BigInt",
                        "index": 0
                    }],
                    "rows": [["1"]],
                    "row_count": 1
                }],
                "took": 1.0
            })
            .to_string(),
        ),
        _ => ("HTTP/1.1 401 Unauthorized", "Token expired".to_string()),
    };

    let response = format!(
        "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: \
         close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

struct TestRequest {
    headers: HashMap<String, String>,
}

async fn read_test_request(stream: &mut TcpStream) -> std::io::Result<TestRequest> {
    let mut buffer = Vec::new();
    let mut temp = [0_u8; 1024];
    let mut header_end = None;
    let mut content_length = 0_usize;

    loop {
        let bytes_read = stream.read(&mut temp).await?;
        if bytes_read == 0 {
            break;
        }
        buffer.extend_from_slice(&temp[..bytes_read]);

        if header_end.is_none() {
            if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                header_end = Some(position + 4);
                let header_text = String::from_utf8_lossy(&buffer[..position]);
                for line in header_text.lines().skip(1) {
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            content_length = value.trim().parse().unwrap_or(0);
                        }
                    }
                }
            }
        }

        if let Some(end) = header_end {
            if buffer.len() >= end + content_length {
                break;
            }
        }
    }

    let header_end = header_end.expect("request should include headers");
    let header_text = String::from_utf8_lossy(&buffer[..header_end - 4]);
    let mut headers = HashMap::new();
    for line in header_text.lines().skip(1) {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    Ok(TestRequest { headers })
}
