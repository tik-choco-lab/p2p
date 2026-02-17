use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify};
use tracing::debug;

use super::packet::{unwrap_packet, wrap_packet, StreamType};
use crate::rtc::{RTCManager, RemotePeer};

pub struct Bridge {
    manager: RTCManager,
    active_peer: Arc<Mutex<Option<Arc<RemotePeer>>>>,
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
        let mgr = self.manager.clone();
        self.manager
            .on_stdio_message(move |peer_id, data| {
                let active_peer = active_peer.clone();
                let mgr = mgr.clone();
                tokio::spawn(async move {
                    {
                        let mut ap = active_peer.lock().await;
                        let needs_update = match &*ap {
                            Some(p) => p.peer_id() != peer_id,
                            None => true,
                        };
                        if needs_update {
                            *ap = mgr.get_peer(&peer_id).await;
                        }
                    }
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
                        *active_peer.lock().await = mgr.get_peer(&peer_id).await;
                        debug!("stdio bridge connected to peer: {}", peer_id);

                        let mut buf = buffer.lock().await;
                        if !buf.is_empty() {
                            debug!("Flushing buffered stdin data...");
                            let ap = active_peer.lock().await;
                            if let Some(ref peer) = *ap {
                                if let Some(dc) = peer.dc_stdio().await {
                                    for data in buf.drain(..) {
                                        let _ = dc.send(&bytes::Bytes::from(data)).await;
                                    }
                                }
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
                    if let Some(ref p) = *ap {
                        if p.peer_id() == peer_id {
                            *connected.lock().await = false;
                            *ap = None;
                            debug!("stdio bridge disconnected from peer: {}", peer_id);
                        }
                    }
                });
            })
            .await;

        let active_peer = self.active_peer.clone();
        let connected = self.connected.clone();
        let buffer = self.buffer.clone();
        let done = self.done.clone();
        tokio::spawn(async move {
            Self::read_stdin(active_peer, connected, buffer, done).await;
        });

        self.done.notified().await;
    }

    async fn read_stdin(
        active_peer: Arc<Mutex<Option<Arc<RemotePeer>>>>,
        connected: Arc<Mutex<bool>>,
        buffer: Arc<Mutex<Vec<Vec<u8>>>>,
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
                        if let Some(ref peer) = *ap {
                            if let Some(dc) = peer.dc_stdio().await {
                                let _ = dc.send(&bytes::Bytes::from(data)).await;
                            }
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
