use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

pub struct TcpDisconnectProxy {
    base_url: String,
    paused: Arc<AtomicBool>,
    impairments: Arc<ProxyImpairments>,
    active_connections: Arc<TokioMutex<HashMap<u64, JoinHandle<()>>>>,
    backend_client_addrs: Arc<TokioMutex<HashMap<u64, String>>>,
    accept_task: JoinHandle<()>,
}

#[derive(Default)]
struct ProxyImpairments {
    blackhole_traffic: AtomicBool,
    chunk_delay_ms: AtomicU64,
}

impl TcpDisconnectProxy {
    pub async fn start(target_base_url: &str) -> Self {
        let target_addr = extract_host_port(target_base_url);
        let listener =
            bind_loopback_listener().await.expect("proxy should bind to an ephemeral port");
        let bind_addr = listener.local_addr().expect("proxy should have a local addr");
        let paused = Arc::new(AtomicBool::new(false));
        let impairments = Arc::new(ProxyImpairments::default());
        let active_connections = Arc::new(TokioMutex::new(HashMap::new()));
        let backend_client_addrs = Arc::new(TokioMutex::new(HashMap::new()));
        let next_id = Arc::new(AtomicU64::new(1));

        let paused_clone = Arc::clone(&paused);
        let impairments_clone = Arc::clone(&impairments);
        let active_clone = Arc::clone(&active_connections);
        let backend_addrs_clone = Arc::clone(&backend_client_addrs);
        let next_id_clone = Arc::clone(&next_id);
        let accept_task = tokio::spawn(async move {
            while let Ok((mut inbound, _peer)) = listener.accept().await {
                if paused_clone.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }

                let id = next_id_clone.fetch_add(1, Ordering::SeqCst);
                let target_addr = target_addr.clone();
                let impairments_for_task = Arc::clone(&impairments_clone);
                let active_for_task = Arc::clone(&active_clone);
                let backend_addrs_for_task = Arc::clone(&backend_addrs_clone);
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(&target_addr).await {
                        if let Ok(local_addr) = outbound.local_addr() {
                            backend_addrs_for_task
                                .lock()
                                .await
                                .insert(id, local_addr.to_string());
                        }

                        let (inbound_reader, inbound_writer) = inbound.split();
                        let (outbound_reader, outbound_writer) = outbound.split();
                        let _ = tokio::try_join!(
                            relay(
                                inbound_reader,
                                outbound_writer,
                                Arc::clone(&impairments_for_task),
                            ),
                            relay(outbound_reader, inbound_writer, impairments_for_task),
                        );
                    }
                    active_for_task.lock().await.remove(&id);
                    backend_addrs_for_task.lock().await.remove(&id);
                });

                active_clone.lock().await.insert(id, task);
            }
        });

        Self {
            base_url: format!("http://{}", bind_addr),
            paused,
            impairments,
            active_connections,
            backend_client_addrs,
            accept_task,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn blackhole(&self) {
        self.impairments.blackhole_traffic.store(true, Ordering::SeqCst);
    }

    pub fn restore_traffic(&self) {
        self.impairments.blackhole_traffic.store(false, Ordering::SeqCst);
    }

    pub fn set_chunk_delay(&self, delay: Duration) {
        let delay_ms = delay.as_millis().min(u64::MAX as u128) as u64;
        self.impairments.chunk_delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    pub fn clear_chunk_delay(&self) {
        self.impairments.chunk_delay_ms.store(0, Ordering::SeqCst);
    }

    pub async fn drop_active_connections(&self) {
        let mut active = self.active_connections.lock().await;
        for (_id, task) in active.drain() {
            task.abort();
        }
        self.backend_client_addrs.lock().await.clear();
    }

    pub async fn active_count(&self) -> usize {
        self.active_connections.lock().await.len()
    }

    pub async fn backend_client_addrs(&self) -> Vec<String> {
        self.backend_client_addrs.lock().await.values().cloned().collect()
    }

    pub async fn wait_for_active_connections(
        &self,
        min_count: usize,
        timeout_dur: Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        loop {
            if self.active_count().await >= min_count {
                return true;
            }
            if start.elapsed() >= timeout_dur {
                return false;
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn wait_for_backend_client_addr(&self, timeout_dur: Duration) -> Option<String> {
        let start = std::time::Instant::now();
        loop {
            if let Some(client_addr) = self.backend_client_addrs().await.into_iter().next() {
                return Some(client_addr);
            }
            if start.elapsed() >= timeout_dur {
                return None;
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn simulate_server_down(&self) {
        self.pause();
        self.drop_active_connections().await;
    }

    pub fn simulate_server_up(&self) {
        self.restore_traffic();
        self.resume();
    }

    pub async fn shutdown(self) {
        self.accept_task.abort();
        self.drop_active_connections().await;
    }
}

async fn bind_loopback_listener() -> std::io::Result<TcpListener> {
    let mut last_error = None;

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

async fn relay(
    mut reader: tokio::net::tcp::ReadHalf<'_>,
    mut writer: tokio::net::tcp::WriteHalf<'_>,
    impairments: Arc<ProxyImpairments>,
) -> std::io::Result<()> {
    let mut buffer = [0_u8; 16 * 1024];

    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            writer.shutdown().await?;
            return Ok(());
        }

        while impairments.blackhole_traffic.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(25)).await;
        }

        let delay_ms = impairments.chunk_delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }

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
        .unwrap_or("127.0.0.1:9188")
        .to_string()
}
