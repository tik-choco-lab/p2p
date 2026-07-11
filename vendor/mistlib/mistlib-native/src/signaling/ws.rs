use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use mistlib_core::signaling::reconnect::random_reconnect_backoff_delay;
use mistlib_core::signaling::{MessageContent, Signaler, SignalingData};
use mistlib_core::stats::STATS;
use mistlib_core::types::{NodeId, SessionReestablishedHook};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_util::sync::CancellationToken;

/// Watch channel a caller of `reset_session` waits on when a reset is already
/// in flight (see `start_supervisor`): `None` while running, `Some(_)` once
/// the in-flight attempt has resolved.
type ResetInflightWatch = Arc<Mutex<Option<watch::Receiver<Option<Result<(), String>>>>>>;

pub struct WebSocketSignaler {
    pub url: String,
    sender: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    cancel: Arc<Mutex<Option<CancellationToken>>>,
    incoming_tx: Arc<Mutex<Option<mpsc::Sender<MessageContent>>>>,
    reset_inflight: ResetInflightWatch,
    on_session_reestablished: Arc<Mutex<Option<SessionReestablishedHook>>>,
}

impl WebSocketSignaler {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            sender: Arc::new(Mutex::new(None)),
            cancel: Arc::new(Mutex::new(None)),
            incoming_tx: Arc::new(Mutex::new(None)),
            reset_inflight: Arc::new(Mutex::new(None)),
            on_session_reestablished: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn connect(
        &self,
        incoming_tx: mpsc::Sender<MessageContent>,
    ) -> crate::error::Result<()> {
        *self.incoming_tx.lock().await = Some(incoming_tx.clone());
        self.start_supervisor(incoming_tx, false, false).await
    }

    async fn start_supervisor(
        &self,
        incoming_tx: mpsc::Sender<MessageContent>,
        notify_initial_reestablished: bool,
        continue_after_initial_failure: bool,
    ) -> crate::error::Result<()> {
        let cancel = CancellationToken::new();
        {
            let mut lock = self.cancel.lock().await;
            if let Some(previous) = lock.replace(cancel.clone()) {
                previous.cancel();
            }
        }
        *self.sender.lock().await = None;

        let (initial_tx, initial_rx) = oneshot::channel();
        let supervisor = WebSocketSupervisor {
            url: self.url.clone(),
            sender: self.sender.clone(),
            incoming_tx,
            cancel,
            on_session_reestablished: self.on_session_reestablished.clone(),
            notify_initial_reestablished,
            continue_after_initial_failure,
        };
        tokio::spawn(supervisor.run(initial_tx));

        initial_rx.await.map_err(|_| {
            crate::error::MistError::Network(
                "WebSocketSignaler: connection supervisor stopped before reporting status"
                    .to_string(),
            )
        })?
    }

    async fn reset_session_once(&self) -> mistlib_core::error::Result<()> {
        let Some(incoming_tx) = self.incoming_tx.lock().await.clone() else {
            return Ok(());
        };
        tracing::info!("WebSocketSignaler: resetting signaling session");
        self.start_supervisor(incoming_tx, true, true)
            .await
            .map_err(|err| mistlib_core::error::MistError::Network(err.to_string()))
    }

    async fn wait_for_reset_result(
        mut rx: watch::Receiver<Option<Result<(), String>>>,
    ) -> mistlib_core::error::Result<()> {
        loop {
            if let Some(result) = rx.borrow().clone() {
                return result.map_err(mistlib_core::error::MistError::Network);
            }
            if rx.changed().await.is_err() {
                return Err(mistlib_core::error::MistError::Network(
                    "WebSocketSignaler: reset task ended before reporting status".to_string(),
                ));
            }
        }
    }
}

struct WebSocketSupervisor {
    url: String,
    sender: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    incoming_tx: mpsc::Sender<MessageContent>,
    cancel: CancellationToken,
    on_session_reestablished: Arc<Mutex<Option<SessionReestablishedHook>>>,
    notify_initial_reestablished: bool,
    continue_after_initial_failure: bool,
}

impl WebSocketSupervisor {
    async fn run(self, initial_tx: oneshot::Sender<crate::error::Result<()>>) {
        let mut initial_tx = Some(initial_tx);
        let mut attempt = 0_u32;
        let mut reconnected = false;

        loop {
            tracing::info!("WebSocketSignaler: connecting to {}", self.url);
            let ws_stream = match connect_async(&self.url).await {
                Ok((stream, _)) => stream,
                Err(err) => {
                    let err = crate::error::MistError::Network(err.to_string());
                    if let Some(tx) = initial_tx.take() {
                        let should_continue = self.continue_after_initial_failure;
                        let _ = tx.send(Err(err));
                        if !should_continue {
                            return;
                        }
                    }
                    let delay = random_reconnect_backoff_delay(attempt);
                    tracing::warn!(
                        "WebSocketSignaler: reconnect to {} failed; retrying in {:?}",
                        self.url,
                        delay
                    );
                    attempt = attempt.saturating_add(1);
                    tokio::select! {
                        _ = self.cancel.cancelled() => return,
                        _ = tokio::time::sleep(delay) => continue,
                    }
                }
            };

            attempt = 0;
            let (mut write, mut read) = ws_stream.split();
            let (tx, mut rx) = mpsc::channel::<String>(1024);
            *self.sender.lock().await = Some(tx);

            if let Some(tx) = initial_tx.take() {
                let _ = tx.send(Ok(()));
                if self.notify_initial_reestablished {
                    self.call_reestablished_hook().await;
                }
            } else if reconnected {
                self.call_reestablished_hook().await;
            }
            reconnected = true;

            let incoming_tx = self.incoming_tx.clone();
            let mut writer = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let bytes = msg.len() as u64;
                    if let Err(err) = write.send(Message::Text(msg.into())).await {
                        tracing::warn!("WebSocketSignaler: send failed: {}", err);
                        break;
                    }
                    STATS.add_send(bytes);
                }
            });

            let mut reader = tokio::spawn(async move {
                while let Some(msg) = read.next().await {
                    let parse_result = match msg {
                        Ok(Message::Text(text)) => {
                            STATS.add_receive(text.len() as u64);
                            serde_json::from_slice::<SignalingData>(text.as_bytes())
                        }
                        Ok(Message::Binary(bin)) => {
                            STATS.add_receive(bin.len() as u64);
                            serde_json::from_slice::<SignalingData>(&bin)
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::info!("WebSocketSignaler: closed: {:?}", frame);
                            break;
                        }
                        Err(err) => {
                            tracing::warn!("WebSocketSignaler: read failed: {}", err);
                            break;
                        }
                        _ => continue,
                    };
                    match parse_result {
                        Ok(data) => {
                            if incoming_tx.send(MessageContent::Data(data)).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => tracing::warn!("WebSocketSignaler: decode failed: {}", err),
                    }
                }
            });

            tokio::select! {
                _ = self.cancel.cancelled() => {
                    writer.abort();
                    reader.abort();
                    *self.sender.lock().await = None;
                    return;
                }
                _ = &mut writer => {}
                _ = &mut reader => {}
            }

            writer.abort();
            reader.abort();
            *self.sender.lock().await = None;
            tracing::warn!(
                "WebSocketSignaler: disconnected from {}; reconnecting",
                self.url
            );
            let delay = random_reconnect_backoff_delay(attempt);
            attempt = attempt.saturating_add(1);
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn call_reestablished_hook(&self) {
        let hook = self.on_session_reestablished.lock().await.clone();
        if let Some(hook) = hook {
            tracing::info!("WebSocketSignaler: session reestablished");
            hook();
        }
    }
}

#[async_trait]
impl Signaler for WebSocketSignaler {
    async fn send_signaling(
        &self,
        _to: &NodeId,
        msg: MessageContent,
    ) -> mistlib_core::error::Result<()> {
        let MessageContent::Data(data) = msg else {
            return Err(mistlib_core::error::MistError::Signaling(
                "WebSocketSignaler: unsupported message type".to_string(),
            ));
        };
        let data_str = serde_json::to_string(&data)
            .map_err(|e| mistlib_core::error::MistError::Serialization(e.to_string()))?;

        let sender = self.sender.lock().await;
        let tx = sender.as_ref().ok_or_else(|| {
            mistlib_core::error::MistError::Signaling(format!(
                "WebSocketSignaler: not connected to {}",
                self.url
            ))
        })?;
        tx.send(data_str).await.map_err(|e| {
            mistlib_core::error::MistError::Signaling(format!(
                "WebSocketSignaler: channel closed: {}",
                e
            ))
        })
    }

    async fn reset_session(&self) -> mistlib_core::error::Result<()> {
        let existing = {
            let inflight = self.reset_inflight.lock().await;
            inflight.as_ref().cloned()
        };
        if let Some(rx) = existing {
            return Self::wait_for_reset_result(rx).await;
        }

        let (tx, rx) = watch::channel(None);
        {
            let mut inflight = self.reset_inflight.lock().await;
            if let Some(existing) = inflight.as_ref().cloned() {
                drop(inflight);
                return Self::wait_for_reset_result(existing).await;
            }
            *inflight = Some(rx);
        }

        let result = self.reset_session_once().await;
        let shared = result.as_ref().map(|_| ()).map_err(ToString::to_string);
        let _ = tx.send(Some(shared));
        *self.reset_inflight.lock().await = None;
        result
    }

    fn set_on_session_reestablished(&self, hook: SessionReestablishedHook) {
        if let Ok(mut lock) = self.on_session_reestablished.try_lock() {
            *lock = Some(hook);
            return;
        }
        let hooks = self.on_session_reestablished.clone();
        tokio::spawn(async move {
            *hooks.lock().await = Some(hook);
        });
    }

    async fn close(&self) -> mistlib_core::error::Result<()> {
        *self.incoming_tx.lock().await = None;
        if let Some(cancel) = self.cancel.lock().await.take() {
            cancel.cancel();
        }
        *self.sender.lock().await = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::WebSocketSignaler;
    use mistlib_core::signaling::{MessageContent, Signaler, SignalingData, SignalingType};
    use mistlib_core::types::NodeId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot, Notify};
    use tokio::time::{timeout, Duration};
    use tokio_tungstenite::accept_async;

    fn data(label: &str) -> MessageContent {
        MessageContent::Data(SignalingData {
            sender_id: NodeId("local".to_string()),
            receiver_id: NodeId::server(),
            room_id: "room-a".to_string(),
            data: label.to_string(),
            signaling_type: SignalingType::Request,
        })
    }

    async fn spawn_ws_capture_server() -> (String, mpsc::Receiver<String>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (frame_tx, frame_rx) = mpsc::channel(8);
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_task = accepted.clone();

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                accepted_for_task.fetch_add(1, Ordering::SeqCst);
                let frame_tx = frame_tx.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
                    if let Some(Ok(msg)) = futures_util::StreamExt::next(&mut ws).await {
                        if let Ok(text) = msg.into_text() {
                            let _ = frame_tx.send(text.to_string()).await;
                        }
                    }
                });
            }
        });

        (format!("ws://{addr}"), frame_rx, accepted)
    }

    #[tokio::test]
    async fn reconnects_after_server_disconnect_and_sends_again() {
        let (url, mut frames, accepted) = spawn_ws_capture_server().await;
        let signaler = WebSocketSignaler::new(&url);
        let (incoming_tx, _incoming_rx) = mpsc::channel(8);
        let (hook_tx, hook_rx) = oneshot::channel::<()>();
        let hook_tx = std::sync::Mutex::new(Some(hook_tx));
        signaler.set_on_session_reestablished(Arc::new(move || {
            if let Some(tx) = hook_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
        }));

        signaler.connect(incoming_tx).await.unwrap();
        signaler
            .send_signaling(&NodeId::server(), data("first"))
            .await
            .unwrap();
        let first = timeout(Duration::from_secs(2), frames.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(first.contains("first"));

        timeout(Duration::from_secs(3), hook_rx)
            .await
            .expect("reconnect hook should fire")
            .expect("hook sender should not be dropped");
        assert!(accepted.load(Ordering::SeqCst) >= 2);

        signaler
            .send_signaling(&NodeId::server(), data("second"))
            .await
            .unwrap();
        let second = timeout(Duration::from_secs(2), frames.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(second.contains("second"));
        signaler.close().await.unwrap();
    }

    #[tokio::test]
    async fn reset_session_reconnects_and_fires_hook_once() {
        let (url, _frames, accepted) = spawn_ws_capture_server().await;
        let signaler = WebSocketSignaler::new(&url);
        let (incoming_tx, _incoming_rx) = mpsc::channel(8);
        let hooks = Arc::new(AtomicUsize::new(0));
        let hook_notify = Arc::new(Notify::new());
        let hooks_for_hook = hooks.clone();
        let notify_for_hook = hook_notify.clone();
        signaler.set_on_session_reestablished(Arc::new(move || {
            hooks_for_hook.fetch_add(1, Ordering::SeqCst);
            notify_for_hook.notify_one();
        }));

        signaler.connect(incoming_tx).await.unwrap();
        assert_eq!(accepted.load(Ordering::SeqCst), 1);

        signaler.reset_session().await.unwrap();
        timeout(Duration::from_secs(2), hook_notify.notified())
            .await
            .expect("reset should fire the reestablished hook");
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
        signaler.close().await.unwrap();
    }

    #[tokio::test]
    async fn reset_session_after_close_is_noop() {
        let (url, _frames, accepted) = spawn_ws_capture_server().await;
        let signaler = WebSocketSignaler::new(&url);
        let (incoming_tx, _incoming_rx) = mpsc::channel(8);

        signaler.connect(incoming_tx).await.unwrap();
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        signaler.close().await.unwrap();

        signaler.reset_session().await.unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_reset_session_reuses_inflight_reconnect() {
        let (url, _frames, accepted) = spawn_ws_capture_server().await;
        let signaler = Arc::new(WebSocketSignaler::new(&url));
        let (incoming_tx, _incoming_rx) = mpsc::channel(8);
        let hooks = Arc::new(AtomicUsize::new(0));
        let hook_notify = Arc::new(Notify::new());
        let hooks_for_hook = hooks.clone();
        let notify_for_hook = hook_notify.clone();
        signaler.set_on_session_reestablished(Arc::new(move || {
            hooks_for_hook.fetch_add(1, Ordering::SeqCst);
            notify_for_hook.notify_one();
        }));

        signaler.connect(incoming_tx).await.unwrap();
        let first = signaler.clone();
        let second = signaler.clone();
        let (first_result, second_result) =
            tokio::join!(async move { first.reset_session().await }, async move {
                second.reset_session().await
            });

        first_result.unwrap();
        second_result.unwrap();
        timeout(Duration::from_secs(2), hook_notify.notified())
            .await
            .expect("reset should fire the reestablished hook");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
        signaler.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_stops_reconnect_attempts() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_task = accepted.clone();
        let first_closed = Arc::new(Notify::new());
        let first_closed_for_task = first_closed.clone();

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let connection_index = accepted_for_task.fetch_add(1, Ordering::SeqCst);
                if connection_index == 0 {
                    let first_closed = first_closed_for_task.clone();
                    tokio::spawn(async move {
                        let Ok(mut ws) = accept_async(stream).await else {
                            return;
                        };
                        let _ = futures_util::StreamExt::next(&mut ws).await;
                        first_closed.notify_waiters();
                    });
                }
            }
        });

        let signaler = WebSocketSignaler::new(&format!("ws://{addr}"));
        let (incoming_tx, _incoming_rx) = mpsc::channel(8);
        signaler.connect(incoming_tx).await.unwrap();
        signaler
            .send_signaling(&NodeId::server(), data("before-close"))
            .await
            .unwrap();
        timeout(Duration::from_secs(2), first_closed.notified())
            .await
            .expect("server should see the first frame");

        signaler.close().await.unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
    }
}
