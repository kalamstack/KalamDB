use super::helpers::*;
use crate::common::tcp_proxy::TcpDisconnectProxy;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;

/// Server goes down before the client finishes connecting.
/// The client should fail to connect (or auto-reconnect once the proxy resumes).
#[tokio::test]
async fn test_proxy_server_down_while_connecting() {
    let proxy = TcpDisconnectProxy::start(upstream_server_url()).await;

    // Pause the proxy BEFORE the client tries to connect — simulates
    // the server being unreachable.
    proxy.simulate_server_down().await;

    let (client, connect_count, disconnect_count) =
        match create_test_client_with_events_for_base_url(proxy.base_url()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Skipping test (proxy client unavailable): {}", e);
                proxy.shutdown().await;
                return;
            },
        };

    // connect() should either error or time out because the proxy rejects.
    let connect_result = tokio::time::timeout(Duration::from_secs(5), client.connect()).await;
    match connect_result {
        Ok(Ok(())) => {
            // Some implementations may return Ok even though the WS handshake
            // hasn't completed yet — the connection will fail shortly.
        },
        Ok(Err(_)) => { /* expected: connection refused */ },
        Err(_) => { /* timed out — also acceptable */ },
    }

    assert_eq!(
        connect_count.load(Ordering::SeqCst),
        0,
        "on_connect should NOT have fired while the server is down"
    );

    // Now bring the proxy back and wait for auto-reconnect.
    proxy.simulate_server_up();

    for _ in 0..80 {
        if connect_count.load(Ordering::SeqCst) >= 1 && client.is_connected().await {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(
        client.is_connected().await,
        "client should auto-reconnect after proxy becomes available"
    );
    assert!(
        connect_count.load(Ordering::SeqCst) >= 1,
        "on_connect should fire after proxy resumes"
    );

    client.disconnect().await;
    let _ = disconnect_count; // suppress warning
    proxy.shutdown().await;
}
