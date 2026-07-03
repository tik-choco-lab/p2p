use bytes::Bytes;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::mpsc;
use tracing_subscriber::{prelude::*, EnvFilter};

use crate::engine::*;
use mistlib_core::action::OverlayAction;
use mistlib_core::config::Config;
use mistlib_core::layers::L0Engine;
use mistlib_core::transport::Transport;
use mistlib_core::types::{ConnectionState, DeliveryMethod, NodeId};

pub const DELIVERY_RELIABLE: u32 = 0;
pub const DELIVERY_UNRELIABLE_ORDERED: u32 = 1;
pub const DELIVERY_UNRELIABLE: u32 = 2;

#[derive(Clone)]
struct SendRequest {
    target_node: NodeId,
    bytes: Bytes,
    delivery: DeliveryMethod,
}

#[derive(Clone, Copy)]
struct PositionUpdate {
    x: f32,
    y: f32,
    z: f32,
}

static SEND_QUEUE: LazyLock<Mutex<Option<mpsc::Sender<SendRequest>>>> =
    LazyLock::new(|| Mutex::new(None));

static STATS_CACHE: LazyLock<Mutex<String>> = LazyLock::new(|| Mutex::new("{}".to_string()));
static STATS_WORKER_STARTED: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));
static POSITION_CACHE: LazyLock<Mutex<Option<PositionUpdate>>> = LazyLock::new(|| Mutex::new(None));
static POSITION_WORKER_STARTED: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

pub static INIT_LOG: LazyLock<()> = LazyLock::new(|| {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("off"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(ExternalLogLayer)
        .try_init();
});

pub fn join_room(room_id: String) {
    ENGINE.l0.join_room(room_id);
}

pub fn init_and_join(id: String, signaling_url: String, room_id: String) {
    init(id, signaling_url);
    join_room(room_id);
}

pub fn register_log_callback(cb: LogCallback) {
    let mut callback = ENGINE.log_callback.lock().unwrap();
    *callback = Some(cb);
}

pub fn register_event_callback(cb: EventCallback) {
    let mut callback = ENGINE.global_callback.lock().unwrap();
    *callback = Some(cb);
}

pub fn register_raw_handler<F>(handler: F)
where
    F: Fn(u32, String, Vec<u8>) + Send + Sync + 'static,
{
    let mut callback = ENGINE.rust_event_callback.lock().unwrap();
    *callback = Some(Arc::new(handler));
}

pub fn clear_raw_handler() {
    let mut callback = ENGINE.rust_event_callback.lock().unwrap();
    *callback = None;
}

pub fn init(id: String, signaling_url: String) {
    *INIT_LOG;
    tracing::info!("mistlib::init called");

    let local_id = NodeId(id);
    ENGINE.l0.initialize(local_id, signaling_url);
    ensure_send_worker();
    ensure_stats_worker();
}

fn config_from_json(data: &str) -> Option<Config> {
    let mut config = if let Ok(config) = serde_json::from_str::<Config>(data) {
        config
    } else {
        let mut config = ENGINE.l0.get_config();
        if !config.update_from_json(data) {
            return None;
        }
        config
    };

    if let Ok(value) = serde_json::from_str::<serde_json::Value>(data) {
        if let Some(url) = value.get("signalingUrl").and_then(|v| v.as_str()) {
            config.signaling_url = url.to_string();
        }
    }

    config.validate_signaling().then_some(config)
}

pub fn init_with_config(id: String, data: &[u8]) -> bool {
    *INIT_LOG;
    tracing::info!("mistlib::init_with_config called");

    let Ok(json_str) = std::str::from_utf8(data) else {
        tracing::warn!("init_with_config ignored non-UTF-8 config JSON");
        return false;
    };
    let Some(config) = config_from_json(json_str) else {
        tracing::warn!("init_with_config ignored invalid config JSON");
        return false;
    };
    let signaling_url = config.signaling_url.clone();

    ENGINE.l0.set_config(config);
    ENGINE.l0.initialize(NodeId(id), signaling_url);
    ensure_send_worker();
    ensure_stats_worker();
    true
}

pub fn leave_room() {
    ENGINE.l0.leave_room();
}

pub fn shutdown() {
    leave_room();
}

pub fn update_position(x: f32, y: f32, z: f32) {
    {
        let mut cache = POSITION_CACHE.lock().unwrap();
        *cache = Some(PositionUpdate { x, y, z });
    }

    ensure_position_worker();
}

pub fn on_connected(node_id: NodeId) {
    on_connected_internal(node_id);
}

pub fn on_disconnected(node_id: NodeId) {
    on_disconnected_internal(node_id);
}

pub fn set_config(data: &[u8]) {
    if let Ok(json_str) = std::str::from_utf8(data) {
        if let Some(config) = config_from_json(json_str) {
            ENGINE.l0.set_config(config);
        }
    }
}

pub fn send_message(target_id: String, data: &[u8], method: u32) {
    if let Err(err) = try_send_message(target_id, data, method) {
        tracing::warn!("send_message dropped: {}", err);
    }
}

pub fn try_send_message(target_id: String, data: &[u8], method: u32) -> crate::error::Result<()> {
    let Some(sender) = SEND_QUEUE.lock().unwrap().clone() else {
        return Err(crate::error::MistError::Internal(
            "send worker is not initialized".to_string(),
        ));
    };

    sender
        .try_send(SendRequest {
            target_node: NodeId(target_id),
            bytes: Bytes::copy_from_slice(data),
            delivery: delivery_method(method),
        })
        .map_err(|err| crate::error::MistError::Internal(err.to_string()))
}

pub async fn send_message_direct(
    target_id: String,
    data: Vec<u8>,
    method: u32,
) -> crate::error::Result<()> {
    let Some(ctx) = ENGINE.get_context().await else {
        return Err(crate::error::MistError::Internal(
            "engine is not initialized".to_string(),
        ));
    };
    let Some(overlay) = ctx.overlay.as_ref() else {
        return Err(crate::error::MistError::Internal(
            "overlay router is not available".to_string(),
        ));
    };
    let Some(transport) = ctx.webrtc_transport.as_ref() else {
        return Err(crate::error::MistError::Internal(
            "WebRTC transport is not available".to_string(),
        ));
    };

    let target_node = NodeId(target_id);
    let action = overlay.wrap_data(&target_node, Bytes::from(data), delivery_method(method));
    let OverlayAction::SendMessage { to, data, method } = action else {
        return Err(crate::error::MistError::Internal(
            "overlay did not produce a send action".to_string(),
        ));
    };

    if to.is_broadcast() {
        transport.broadcast(data, method).await?;
    } else {
        transport.send(&to, data, method).await?;
    }
    Ok(())
}

fn ensure_send_worker() {
    let mut queue_lock = SEND_QUEUE.lock().unwrap();
    if queue_lock.is_some() {
        return;
    }

    let (tx, rx) = mpsc::channel::<SendRequest>(8192);
    *queue_lock = Some(tx);

    ENGINE.runtime.spawn(async move {
        send_worker(rx).await;
    });
}

fn ensure_stats_worker() {
    let mut started = STATS_WORKER_STARTED.lock().unwrap();
    if *started {
        return;
    }
    *started = true;

    ENGINE.runtime.spawn(async move {
        stats_worker().await;
    });
}

fn ensure_position_worker() {
    let mut started = POSITION_WORKER_STARTED.lock().unwrap();
    if *started {
        return;
    }
    *started = true;

    ENGINE.runtime.spawn(async move {
        position_worker().await;
    });
}

fn delivery_method(method: u32) -> DeliveryMethod {
    match method {
        DELIVERY_RELIABLE => DeliveryMethod::ReliableOrdered,
        DELIVERY_UNRELIABLE_ORDERED => DeliveryMethod::UnreliableOrdered,
        _ => DeliveryMethod::Unreliable,
    }
}

pub fn get_connected_nodes() -> Vec<String> {
    ENGINE.runtime.block_on(get_connected_nodes_async())
}

pub async fn get_connected_nodes_async() -> Vec<String> {
    let Some(ctx) = ENGINE.get_context().await else {
        return vec![];
    };

    let nodes = ctx
        .webrtc_transport
        .as_ref()
        .map(|transport| transport.get_connected_nodes())
        .unwrap_or_else(|| ctx.transport.get_connected_nodes());

    nodes.into_iter().map(|node| node.0).collect()
}

pub fn get_connection_state(node_id: &str) -> String {
    get_connection_state_value(node_id).to_string()
}

pub async fn get_connection_state_async(node_id: &str) -> String {
    get_connection_state_value_async(node_id).await.to_string()
}

pub fn get_connection_state_value(node_id: &str) -> ConnectionState {
    ENGINE
        .runtime
        .block_on(get_connection_state_value_async(node_id))
}

pub async fn get_connection_state_value_async(node_id: &str) -> ConnectionState {
    let Some(ctx) = ENGINE.get_context().await else {
        return ConnectionState::Disconnected;
    };
    let node = NodeId(node_id.to_string());

    ctx.webrtc_transport
        .as_ref()
        .map(|transport| transport.get_connection_state(&node))
        .unwrap_or_else(|| ctx.transport.get_connection_state(&node))
}

async fn send_worker(mut rx: mpsc::Receiver<SendRequest>) {
    let mut cached_generation = u64::MAX;
    let mut cached_ctx: Option<std::sync::Arc<RunningContext>> = None;

    while let Some(req) = rx.recv().await {
        let current_generation = ENGINE
            .context_generation
            .load(std::sync::atomic::Ordering::Relaxed);
        if cached_ctx.is_none() || cached_generation != current_generation {
            cached_ctx = ENGINE.get_context().await;
            cached_generation = current_generation;
        }

        if let Some(ctx) = cached_ctx.as_ref() {
            if let Some(l1) = ctx.l1_transport.as_ref() {
                if req.target_node.0.is_empty() {
                    let _ = l1.broadcast(req.bytes, req.delivery).await;
                } else {
                    let _ = l1
                        .send_message(&req.target_node, req.bytes, req.delivery)
                        .await;
                }
            }
        }
    }
}

async fn stats_worker() {
    // Prime the cache immediately so the first caller doesn't see stale data.
    update_stats_cache().await;

    let mut interval = tokio::time::interval(web_time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        update_stats_cache().await;
    }
}

async fn position_worker() {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
    let mut cached_generation = u64::MAX;
    let mut cached_ctx: Option<std::sync::Arc<RunningContext>> = None;

    loop {
        interval.tick().await;

        let update = {
            let mut cache = POSITION_CACHE.lock().unwrap();
            cache.take()
        };

        let Some(update) = update else {
            continue;
        };

        let current_generation = ENGINE
            .context_generation
            .load(std::sync::atomic::Ordering::Relaxed);
        if cached_ctx.is_none() || cached_generation != current_generation {
            cached_ctx = ENGINE.get_context().await;
            cached_generation = current_generation;
        }

        if let Some(ctx) = cached_ctx.as_ref() {
            if let Some(l1) = ctx.l1_transport.as_ref() {
                l1.update_position(update.x, update.y, update.z);
            }
        }
    }
}

async fn update_stats_cache() {
    let stats_json = ENGINE.get_stats_json().await;
    let mut cache = STATS_CACHE.lock().unwrap();
    *cache = stats_json;
}

pub fn get_stats() -> String {
    STATS_CACHE.lock().unwrap().clone()
}

pub fn get_config() -> String {
    ENGINE.l0.get_config().to_json_string()
}

pub fn storage_add(name: &str, data: &[u8]) -> mistlib_core::error::Result<String> {
    ENGINE.runtime.block_on(ENGINE.l0.storage_add(name, data))
}

pub fn storage_get(cid: &str) -> mistlib_core::error::Result<Vec<u8>> {
    ENGINE.runtime.block_on(ENGINE.l0.storage_get(cid))
}
