#![allow(dead_code)]
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error};

use crate::rtc::{RTCManager, RemotePeer};
use crate::stdio::packet::{unwrap_packet, wrap_packet, StreamType};

pub struct Executor {
    manager: RTCManager,
    command: Vec<String>,
    active_peer: Arc<Mutex<Option<Arc<RemotePeer>>>>,
    stdin_tx: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
    done: Arc<Notify>,
}

impl Executor {
    pub fn new(manager: RTCManager, command: Vec<String>) -> Self {
        Self {
            manager,
            command,
            active_peer: Arc::new(Mutex::new(None)),
            stdin_tx: Arc::new(Mutex::new(None)),
            done: Arc::new(Notify::new()),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let active_peer = self.active_peer.clone();
        let stdin_tx = self.stdin_tx.clone();
        let mgr = self.manager.clone();
        self.manager
            .on_stdio_message(move |peer_id, data| {
                let active_peer = active_peer.clone();
                let stdin_tx = stdin_tx.clone();
                let mgr = mgr.clone();
                tokio::spawn(async move {
                    {
                        let mut ap = active_peer.lock().await;
                        let needs = match &*ap {
                            Some(p) => p.peer_id() != peer_id,
                            None => true,
                        };
                        if needs {
                            *ap = mgr.get_peer(&peer_id).await;
                        }
                    }
                    let (stream_type, payload) = unwrap_packet(&data);
                    if stream_type == StreamType::Stdin {
                        let mut tx = stdin_tx.lock().await;
                        if let Some(ref mut stdin) = *tx {
                            let _ = stdin.write_all(payload).await;
                        }
                    }
                });
            })
            .await;

        let active_peer = self.active_peer.clone();
        let stdin_tx = self.stdin_tx.clone();
        let mgr = self.manager.clone();
        let cmd = self.command.clone();
        self.manager
            .on_stdio_open(move |peer_id| {
                let active_peer = active_peer.clone();
                let stdin_tx = stdin_tx.clone();
                let mgr = mgr.clone();
                let cmd = cmd.clone();
                tokio::spawn(async move {
                    {
                        let existing = stdin_tx.lock().await;
                        if existing.is_some() {
                            return;
                        }
                    }
                    *active_peer.lock().await = mgr.get_peer(&peer_id).await;
                    debug!("Starting proxy command for peer: {}", peer_id);

                    if let Err(e) =
                        Self::start_command(&cmd, active_peer.clone(), stdin_tx.clone()).await
                    {
                        error!("Failed to start proxy command: {}", e);
                    }
                });
            })
            .await;

        let active_peer = self.active_peer.clone();
        let stdin_tx = self.stdin_tx.clone();
        self.manager
            .on_stdio_close(move |peer_id| {
                let active_peer = active_peer.clone();
                let stdin_tx = stdin_tx.clone();
                tokio::spawn(async move {
                    let ap = active_peer.lock().await;
                    if let Some(ref p) = *ap {
                        if p.peer_id() == peer_id {
                            debug!("Stdio closed, stopping proxy command");
                            *stdin_tx.lock().await = None;
                        }
                    }
                });
            })
            .await;

        self.done.notified().await;
        Ok(())
    }

    async fn start_command(
        cmd: &[String],
        active_peer: Arc<Mutex<Option<Arc<RemotePeer>>>>,
        stdin_holder: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
    ) -> Result<()> {
        if cmd.is_empty() {
            return Ok(());
        }

        let mut child = Command::new(&cmd[0])
            .args(&cmd[1..])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take().unwrap();
        *stdin_holder.lock().await = Some(stdin);

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        debug!("Proxy command started");

        let ap = active_peer.clone();
        tokio::spawn(async move {
            Self::forward_stream(stdout, StreamType::Stdout, ap).await;
        });

        let ap = active_peer.clone();
        tokio::spawn(async move {
            Self::forward_stream(stderr, StreamType::Stderr, ap).await;
        });

        let stdin_holder2 = stdin_holder.clone();
        tokio::spawn(async move {
            let status = child.wait().await;
            *stdin_holder2.lock().await = None;
            match status {
                Ok(s) => debug!("Proxy command exited: {}", s),
                Err(e) => debug!("Proxy command error: {}", e),
            }
        });

        Ok(())
    }

    async fn forward_stream<R: tokio::io::AsyncRead + Unpin>(
        mut reader: R,
        stream_type: StreamType,
        active_peer: Arc<Mutex<Option<Arc<RemotePeer>>>>,
    ) {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let data = wrap_packet(stream_type, &buf[..n]);
                    let ap = active_peer.lock().await;
                    if let Some(ref peer) = *ap {
                        if let Some(dc) = peer.dc_stdio().await {
                            let _ = dc.send(&bytes::Bytes::from(data)).await;
                        }
                    }
                }
                Err(e) => {
                    debug!("stream read error: {}", e);
                    break;
                }
            }
        }
    }

    pub fn close(&self) {
        self.done.notify_one();
    }
}
