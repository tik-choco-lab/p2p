use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify};
use tracing::debug;

use super::packet::{unwrap_packet, wrap_packet, StreamType};
use crate::rtc::RTCManager;

pub struct Bridge {
    manager: RTCManager,
    active_peer: Arc<Mutex<Option<String>>>,
    connected: Arc<Mutex<bool>>,
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    done: Arc<Notify>,
}

impl Bridge {
    pub fn new(manager: RTCManager) -> Self {
        Self {
            manager,
            active_peer: Arc::new(Mutex::new(None)),
            connected: Arc::new(Mutex::new(false)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            done: Arc::new(Notify::new()),
        }
    }

    pub async fn run(&self) {
        let active_peer = self.active_peer.clone();
        self.manager
            .on_stdio_message(move |peer_id, data| {
                let active_peer = active_peer.clone();
                tokio::spawn(async move {
                    *active_peer.lock().await = Some(peer_id);
                    let (stream_type, payload) = unwrap_packet(&data);
                    match stream_type {
                        StreamType::Stdout => {
                            let mut stdout = io::stdout();
                            let _ = stdout.write_all(payload).await;
                            let _ = stdout.flush().await;
                        }
                        StreamType::Stderr => {
                            let mut stderr = io::stderr();
                            let _ = stderr.write_all(payload).await;
                            let _ = stderr.flush().await;
                        }
                        _ => {}
                    }
                });
            })
            .await;

        let active_peer = self.active_peer.clone();
        let connected = self.connected.clone();
        let buffer = self.buffer.clone();
        let mgr = self.manager.clone();
        self.manager
            .on_stdio_open(move |peer_id| {
                let active_peer = active_peer.clone();
                let connected = connected.clone();
                let buffer = buffer.clone();
                let mgr = mgr.clone();
                tokio::spawn(async move {
                    let mut conn = connected.lock().await;
                    if !*conn {
                        *conn = true;
                        *active_peer.lock().await = Some(peer_id.clone());
                        debug!("stdio bridge connected to peer: {}", peer_id);

                        let mut buf = buffer.lock().await;
                        if !buf.is_empty() {
                            debug!("Flushing buffered stdin data...");
                            for data in buf.drain(..) {
                                let _ = mgr.send_stdio_to(&peer_id, data).await;
                            }
                        }
                    }
                });
            })
            .await;

        let active_peer = self.active_peer.clone();
        let connected = self.connected.clone();
        self.manager
            .on_stdio_close(move |peer_id| {
                let active_peer = active_peer.clone();
                let connected = connected.clone();
                tokio::spawn(async move {
                    let mut ap = active_peer.lock().await;
                    if ap.as_deref() == Some(peer_id.as_str()) {
                        *connected.lock().await = false;
                        *ap = None;
                        debug!("stdio bridge disconnected from peer: {}", peer_id);
                    }
                });
            })
            .await;

        let active_peer = self.active_peer.clone();
        let connected = self.connected.clone();
        let buffer = self.buffer.clone();
        let done = self.done.clone();
        let manager = self.manager.clone();
        tokio::spawn(async move {
            Self::read_stdin(active_peer, connected, buffer, manager, done).await;
        });

        self.done.notified().await;
    }

    async fn read_stdin(
        active_peer: Arc<Mutex<Option<String>>>,
        connected: Arc<Mutex<bool>>,
        buffer: Arc<Mutex<Vec<Vec<u8>>>>,
        manager: RTCManager,
        done: Arc<Notify>,
    ) {
        let mut stdin = io::stdin();
        let mut buf = vec![0u8; 32 * 1024];

        loop {
            match stdin.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let data = wrap_packet(StreamType::Stdin, &buf[..n]);
                    let conn = *connected.lock().await;
                    if conn {
                        let ap = active_peer.lock().await;
                        if let Some(ref peer_id) = *ap {
                            let _ = manager.send_stdio_to(peer_id, data).await;
                        }
                    } else {
                        buffer.lock().await.push(data);
                        debug!("Buffering stdin data (not connected yet)");
                    }
                }
                Err(e) => {
                    debug!("stdin read error: {}", e);
                    break;
                }
            }
        }
        done.notify_one();
    }

    pub fn close(&self) {
        self.done.notify_one();
    }
}
