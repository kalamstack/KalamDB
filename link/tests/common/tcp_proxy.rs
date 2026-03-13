use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// A TCP proxy that sits between a client and a real server, allowing tests to
/// simulate network failures by pausing new connections and/or forcibly dropping
/// active ones.
///
/// Typical usage:
/// ```ignore
/// let proxy = TcpDisconnectProxy::start("http://127.0.0.1:8080").await;
/// // point your client at proxy.base_url()
/// proxy.pause();                           // reject new connections
/// proxy.drop_active_connections().await;   // kill existing sockets
/// // … verify client detects disconnect …
/// proxy.resume();                          // allow reconnects
/// proxy.shutdown().await;
/// ```
pub struct TcpDisconnectProxy {
    base_url: String,
    paused: Arc<AtomicBool>,
    active_connections: Arc<TokioMutex<HashMap<u64, JoinHandle<()>>>>,
    accept_task: JoinHandle<()>,
}

impl TcpDisconnectProxy {
    /// Start the proxy, forwarding all traffic to `target_base_url`.
    /// Binds to an ephemeral port on 127.0.0.1.
    pub async fn start(target_base_url: &str) -> Self {
        let target_addr = extract_host_port(target_base_url);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy should bind to an ephemeral port");
        let bind_addr = listener.local_addr().expect("proxy should have a local addr");
        let paused = Arc::new(AtomicBool::new(false));
        let active_connections = Arc::new(TokioMutex::new(HashMap::new()));
        let next_id = Arc::new(AtomicU64::new(1));

        let paused_clone = paused.clone();
        let active_clone = active_connections.clone();
        let next_id_clone = next_id.clone();
        let accept_task = tokio::spawn(async move {
            while let Ok((mut inbound, _peer)) = listener.accept().await {
                if paused_clone.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }

                let id = next_id_clone.fetch_add(1, Ordering::SeqCst);
                let target_addr = target_addr.clone();
                let active_for_task = active_clone.clone();
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&target_addr).await {
                        let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                    active_for_task.lock().await.remove(&id);
                });

                active_clone.lock().await.insert(id, task);
            }
        });

        Self {
            base_url: format!("http://{}", bind_addr),
            paused,
            active_connections,
            accept_task,
        }
    }

    /// The `http://host:port` URL that clients should connect to.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Stop accepting new connections (existing ones keep flowing).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Allow new connections again.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    /// Abort all in-flight proxy tasks, forcibly closing both sides of every
    /// active connection.
    pub async fn drop_active_connections(&self) {
        let mut active = self.active_connections.lock().await;
        for (_id, task) in active.drain() {
            task.abort();
        }
    }

    /// Returns the number of currently active proxy connections.
    pub async fn active_count(&self) -> usize {
        self.active_connections.lock().await.len()
    }

    /// Wait until at least `min_count` proxy connections are active, or until
    /// `timeout_dur` has elapsed. Returns `true` if the condition was met.
    pub async fn wait_for_active_connections(
        &self,
        min_count: usize,
        timeout_dur: Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        loop {
            if self.active_connections.lock().await.len() >= min_count {
                return true;
            }
            if start.elapsed() >= timeout_dur {
                return false;
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Simulate a full server outage: pause + drop all connections.
    pub async fn simulate_server_down(&self) {
        self.pause();
        self.drop_active_connections().await;
    }

    /// Simulate server coming back: resume accepting connections.
    pub fn simulate_server_up(&self) {
        self.resume();
    }

    /// Clean shutdown — abort the accept task and all active connections.
    pub async fn shutdown(self) {
        self.accept_task.abort();
        self.drop_active_connections().await;
    }
}

fn extract_host_port(base_url: &str) -> String {
    base_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:8080")
        .to_string()
}
