use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error};

use super::message::SignalMessage;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = futures_util::stream::SplitSink<WsStream, WsMessage>;
type WsSource = futures_util::stream::SplitStream<WsStream>;

pub struct SignalClient {
    ws_url: String,
    self_id: String,
    room_id: String,

    write_tx: RwLock<mpsc::UnboundedSender<WsMessage>>,

    on_msg: Arc<RwLock<Option<Arc<dyn Fn(SignalMessage) + Send + Sync>>>>,
    on_reconnect: Arc<RwLock<Option<Arc<dyn Fn() + Send + Sync>>>>,

    closed: Arc<RwLock<bool>>,
    close_notify: Arc<Notify>,

    max_retries: usize,
    base_retry_delay: Duration,
    max_retry_delay: Duration,
    reconnecting: Mutex<bool>,
}

impl SignalClient {
    pub async fn new(ws_url: &str, self_id: &str, room_id: &str) -> Result<Arc<Self>> {
        let (write_tx, write_rx) = mpsc::unbounded_channel();

        let client = Arc::new(Self {
            ws_url: ws_url.to_string(),
            self_id: self_id.to_string(),
            room_id: room_id.to_string(),
            write_tx: RwLock::new(write_tx),
            on_msg: Arc::new(RwLock::new(None)),
            on_reconnect: Arc::new(RwLock::new(None)),
            closed: Arc::new(RwLock::new(false)),
            close_notify: Arc::new(Notify::new()),
            max_retries: 10,
            base_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(30),
            reconnecting: Mutex::new(false),
        });

        client.connect_ws(write_rx).await?;

        client
            .send(SignalMessage {
                msg_type: "Request".into(),
                sender_id: client.self_id.clone(),
                room_id: Some(client.room_id.clone()),
                ..Default::default()
            })
            .await?;

        debug!("WebSocket connected to {}", ws_url);
        Ok(client)
    }

    async fn connect_ws(
        self: &Arc<Self>,
        write_rx: mpsc::UnboundedReceiver<WsMessage>,
    ) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        let (ws_write, ws_read) = ws_stream.split();

        tokio::spawn(Self::write_loop(ws_write, write_rx));
        Self::spawn_read_loop(self.clone(), ws_read);

        Ok(())
    }

    fn spawn_read_loop(client: Arc<Self>, ws_read: WsSource) {
        tokio::spawn(run_read_loop(client, ws_read));
    }

    async fn write_loop(mut ws_write: WsSink, mut write_rx: mpsc::UnboundedReceiver<WsMessage>) {
        while let Some(msg) = write_rx.recv().await {
            if let Err(e) = ws_write.send(msg).await {
                error!("WebSocket write error: {}", e);
                break;
            }
        }
    }

    fn calculate_backoff(&self, retry_count: usize) -> Duration {
        let shift = (retry_count - 1).min(31) as u32;
        let delay = self.base_retry_delay * (1u32 << shift);
        delay.min(self.max_retry_delay)
    }

    pub async fn on_message<F>(&self, f: F)
    where
        F: Fn(SignalMessage) + Send + Sync + 'static,
    {
        let mut handler = self.on_msg.write().await;
        *handler = Some(Arc::new(f));
    }

    pub async fn on_reconnect_handler<F>(&self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut handler = self.on_reconnect.write().await;
        *handler = Some(Arc::new(f));
    }

    pub async fn send(&self, msg: SignalMessage) -> Result<()> {
        if *self.closed.read().await {
            anyhow::bail!("connection closed");
        }
        let data = serde_json::to_string(&msg)?;
        let tx = self.write_tx.read().await;
        tx.send(WsMessage::Text(data.into()))
            .map_err(|e| anyhow::anyhow!("send error: {}", e))?;
        Ok(())
    }

    pub async fn close(&self) {
        let mut closed = self.closed.write().await;
        *closed = true;
        self.close_notify.notify_waiters();
    }
}

async fn run_read_loop(client: Arc<SignalClient>, mut ws_read: WsSource) {
    loop {
        match ws_read.next().await {
            Some(Ok(WsMessage::Text(text))) => {
                if let Ok(msg) = serde_json::from_str::<SignalMessage>(&text) {
                    let handler = client.on_msg.read().await;
                    if let Some(ref h) = *handler {
                        h(msg);
                    }
                }
            }
            Some(Ok(WsMessage::Close(_))) | None => {
                debug!("WebSocket connection closed");
                let closed = *client.closed.read().await;
                if !closed {
                    tokio::spawn(run_reconnect(client));
                }
                return;
            }
            Some(Err(e)) => {
                debug!("WebSocket read error: {}", e);
                let closed = *client.closed.read().await;
                if !closed {
                    tokio::spawn(run_reconnect(client));
                }
                return;
            }
            _ => {}
        }
    }
}

async fn run_reconnect(client: Arc<SignalClient>) {
    {
        let mut flag = client.reconnecting.lock().await;
        if *flag {
            return;
        }
        *flag = true;
    }

    for attempt in 1..=client.max_retries {
        if *client.closed.read().await {
            break;
        }

        let delay = client.calculate_backoff(attempt);
        debug!(
            "Reconnecting in {:?} (attempt {}/{})...",
            delay, attempt, client.max_retries
        );

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = client.close_notify.notified() => { break; }
        }

        let (write_tx, write_rx) = mpsc::unbounded_channel();

        match connect_async(&client.ws_url).await {
            Ok((ws_stream, _)) => {
                let (ws_write, ws_read) = ws_stream.split();
                tokio::spawn(SignalClient::write_loop(ws_write, write_rx));

                {
                    let mut tx = client.write_tx.write().await;
                    *tx = write_tx;
                }

                SignalClient::spawn_read_loop(client.clone(), ws_read);

                debug!("Successfully reconnected");

                let _ = client
                    .send(SignalMessage {
                        msg_type: "Request".into(),
                        sender_id: client.self_id.clone(),
                        room_id: Some(client.room_id.clone()),
                        ..Default::default()
                    })
                    .await;

                let handler = client.on_reconnect.read().await;
                if let Some(ref h) = *handler {
                    h();
                }
                break;
            }
            Err(e) => {
                debug!("Reconnection failed: {}", e);
            }
        }
    }

    let mut flag = client.reconnecting.lock().await;
    *flag = false;
}
