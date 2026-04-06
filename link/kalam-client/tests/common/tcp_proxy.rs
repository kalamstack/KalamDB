use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    impairments: Arc<ProxyImpairments>,
    active_connections: Arc<TokioMutex<HashMap<u64, JoinHandle<()>>>>,
    accept_task: JoinHandle<()>,
}

struct ProxyImpairments {
    blackholed: AtomicBool,
    latency_ms: AtomicU64,
    stall_every_n_chunks: AtomicU64,
    stall_duration_ms: AtomicU64,
}

impl ProxyImpairments {
    fn new() -> Self {
        Self {
            blackholed: AtomicBool::new(false),
            latency_ms: AtomicU64::new(0),
            stall_every_n_chunks: AtomicU64::new(0),
            stall_duration_ms: AtomicU64::new(0),
        }
    }

    async fn wait_before_forwarding(&self, chunk_index: u64) {
        while self.blackholed.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(25)).await;
        }

        let latency_ms = self.latency_ms.load(Ordering::SeqCst);
        if latency_ms > 0 {
            sleep(Duration::from_millis(latency_ms)).await;
        }

        let stall_every_n_chunks = self.stall_every_n_chunks.load(Ordering::SeqCst);
        let stall_duration_ms = self.stall_duration_ms.load(Ordering::SeqCst);
        if stall_every_n_chunks > 0
            && stall_duration_ms > 0
            && chunk_index % stall_every_n_chunks == 0
        {
            sleep(Duration::from_millis(stall_duration_ms)).await;
        }
    }
}

impl TcpDisconnectProxy {
    /// Start the proxy, forwarding all traffic to `target_base_url`.
    /// Binds to an ephemeral port on 127.0.0.1.
    pub async fn start(target_base_url: &str) -> Self {
        let target_addr = extract_host_port(target_base_url);
        let listener = bind_loopback_listener()
            .await
            .expect("proxy should bind to an ephemeral port");
        let bind_addr = listener.local_addr().expect("proxy should have a local addr");
        let paused = Arc::new(AtomicBool::new(false));
        let impairments = Arc::new(ProxyImpairments::new());
        let active_connections = Arc::new(TokioMutex::new(HashMap::new()));
        let next_id = Arc::new(AtomicU64::new(1));

        let paused_clone = paused.clone();
        let impairments_clone = impairments.clone();
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
                let impairments_for_task = impairments_clone.clone();
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&target_addr).await {
                        let (inbound_reader, inbound_writer) = inbound.split();
                        let (outbound_reader, outbound_writer) = outbound.split();
                        let _ = tokio::try_join!(
                            relay_with_impairments(
                                inbound_reader,
                                outbound_writer,
                                impairments_for_task.clone(),
                            ),
                            relay_with_impairments(
                                outbound_reader,
                                inbound_writer,
                                impairments_for_task,
                            ),
                        );
                    }
                    active_for_task.lock().await.remove(&id);
                });

                active_clone.lock().await.insert(id, task);
            }
        });

        Self {
            base_url: format!("http://{}", bind_addr),
            paused,
            impairments,
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

    /// Stall all traffic while keeping accepted TCP sockets open.
    pub fn blackhole(&self) {
        self.impairments.blackholed.store(true, Ordering::SeqCst);
    }

    /// Resume forwarding traffic after a `blackhole()` call.
    pub fn restore_traffic(&self) {
        self.impairments.blackholed.store(false, Ordering::SeqCst);
    }

    /// Add a fixed per-chunk forwarding delay in both directions.
    pub fn set_latency(&self, latency: Duration) {
        self.impairments.latency_ms.store(latency.as_millis() as u64, Ordering::SeqCst);
    }

    /// Clear any fixed forwarding latency.
    pub fn clear_latency(&self) {
        self.impairments.latency_ms.store(0, Ordering::SeqCst);
    }

    /// Stall every `every_n_chunks` forwarded chunks for `stall_duration`.
    ///
    /// This approximates packet loss and retransmission delays without dropping
    /// raw TCP bytes, which would corrupt WebSocket frames.
    pub fn set_chunk_stall_pattern(&self, every_n_chunks: u64, stall_duration: Duration) {
        self.impairments.stall_every_n_chunks.store(every_n_chunks, Ordering::SeqCst);
        self.impairments
            .stall_duration_ms
            .store(stall_duration.as_millis() as u64, Ordering::SeqCst);
    }

    /// Clear any intermittent stall pattern.
    pub fn clear_chunk_stall_pattern(&self) {
        self.impairments.stall_every_n_chunks.store(0, Ordering::SeqCst);
        self.impairments.stall_duration_ms.store(0, Ordering::SeqCst);
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

async fn bind_loopback_listener() -> std::io::Result<TcpListener> {
    let mut last_error = None;

    // Under a heavy nextest run on macOS, rapid ephemeral loopback allocation
    // can transiently return AddrNotAvailable. Retry briefly instead of
    // failing the whole reconnect suite on a one-off local port exhaustion.
    for _ in 0..20 {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => return Ok(listener),
            Err(err)
                if matches!(err.kind(), ErrorKind::AddrNotAvailable | ErrorKind::AddrInUse) =>
            {
                last_error = Some(err);
                sleep(Duration::from_millis(50)).await;
            },
            Err(err) => return Err(err),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            ErrorKind::AddrNotAvailable,
            "failed to bind loopback listener after retries",
        )
    }))
}

async fn relay_with_impairments(
    mut reader: tokio::net::tcp::ReadHalf<'_>,
    mut writer: tokio::net::tcp::WriteHalf<'_>,
    impairments: Arc<ProxyImpairments>,
) -> std::io::Result<()> {
    let mut buffer = [0_u8; 16 * 1024];
    let mut chunk_index = 0_u64;

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            writer.shutdown().await?;
            return Ok(());
        }

        chunk_index = chunk_index.saturating_add(1);
        impairments.wait_before_forwarding(chunk_index).await;
        writer.write_all(&buffer[..read]).await?;
        writer.flush().await?;
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
