#![allow(dead_code)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::RwLock;
use tracing::debug;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::signaling_state::RTCSignalingState;
use webrtc::peer_connection::RTCPeerConnection;

use crate::signal::SignalMessage;

use super::manager::{PeerRole, RTCManagerHandle};

static MSG_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn next_msg_id(self_id: &str) -> String {
    let seq = MSG_SEQ.fetch_add(1, Ordering::Relaxed);
    let prefix = if self_id.len() > 4 {
        &self_id[..4]
    } else {
        self_id
    };
    let now = chrono::Local::now().format("%H%M%S%.3f");
    format!("{}_{}_{}", now, prefix, seq)
}

pub struct RemotePeer {
    peer_id: String,
    role: RwLock<PeerRole>,
    pc: RwLock<Option<Arc<RTCPeerConnection>>>,
    dc_tunnel: RwLock<Option<Arc<RTCDataChannel>>>,
    dc_chat: RwLock<Option<Arc<RTCDataChannel>>>,
    dc_signal: RwLock<Option<Arc<RTCDataChannel>>>,
    dc_stdio: RwLock<Option<Arc<RTCDataChannel>>>,
    reconnecting: RwLock<bool>,
}

impl RemotePeer {
    pub fn new(peer_id: String) -> Arc<Self> {
        Arc::new(Self {
            peer_id,
            role: RwLock::new(PeerRole::Client),
            pc: RwLock::new(None),
            dc_tunnel: RwLock::new(None),
            dc_chat: RwLock::new(None),
            dc_signal: RwLock::new(None),
            dc_stdio: RwLock::new(None),
            reconnecting: RwLock::new(false),
        })
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub async fn role(&self) -> PeerRole {
        self.role.read().await.clone()
    }

    pub async fn is_server(&self) -> bool {
        *self.role.read().await == PeerRole::Server
    }

    pub async fn set_role(&self, role: PeerRole) {
        *self.role.write().await = role;
    }

    pub async fn dc_tunnel(&self) -> Option<Arc<RTCDataChannel>> {
        self.dc_tunnel.read().await.clone()
    }

    pub async fn dc_chat(&self) -> Option<Arc<RTCDataChannel>> {
        self.dc_chat.read().await.clone()
    }

    pub async fn dc_stdio(&self) -> Option<Arc<RTCDataChannel>> {
        self.dc_stdio.read().await.clone()
    }

    pub async fn dc_signal(&self) -> Option<Arc<RTCDataChannel>> {
        self.dc_signal.read().await.clone()
    }

    pub async fn pc(&self) -> Option<Arc<RTCPeerConnection>> {
        self.pc.read().await.clone()
    }

    pub async fn signaling_stable(&self) -> bool {
        let pc = self.pc.read().await;
        pc.as_ref()
            .map(|p| p.signaling_state() == RTCSignalingState::Stable)
            .unwrap_or(false)
    }

    pub async fn conn_active(&self) -> bool {
        let pc = self.pc.read().await;
        pc.as_ref()
            .map(|p| {
                let s = p.connection_state();
                s == RTCPeerConnectionState::Connected || s == RTCPeerConnectionState::Connecting
            })
            .unwrap_or(false)
    }

    pub async fn has_pc(&self) -> bool {
        self.pc.read().await.is_some()
    }

    async fn new_peer_connection(
        self: &Arc<Self>,
        manager: &RTCManagerHandle,
    ) -> Result<Arc<RTCPeerConnection>> {
        let api = APIBuilder::new().build();
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".into()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let pc = Arc::new(api.new_peer_connection(config).await?);

        let mgr = manager.clone();
        let peer_id = self.peer_id.clone();
        pc.on_ice_candidate(Box::new(move |candidate| {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            Box::pin(async move {
                if let Some(c) = candidate {
                    if let Ok(json) = c.to_json() {
                        let data = serde_json::to_string(&json).unwrap_or_default();
                        mgr.send_signal(SignalMessage {
                            msg_type: "candidate".into(),
                            data: Some(data),
                            sender_id: mgr.self_id().to_string(),
                            receiver_id: Some(peer_id),
                            room_id: Some(mgr.room_id().to_string()),
                            ..Default::default()
                        })
                        .await;
                    }
                }
            })
        }));

        let mgr = manager.clone();
        let peer_id = self.peer_id.clone();
        let self_clone = self.clone();
        pc.on_peer_connection_state_change(Box::new(move |s| {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            let self_clone = self_clone.clone();
            Box::pin(async move {
                debug!("[{}] PeerConn: {:?}", peer_id, s);
                if s == RTCPeerConnectionState::Failed {
                    let mgr2 = mgr.clone();
                    let self_clone2 = self_clone.clone();
                    tokio::spawn(async move {
                        self_clone2.handle_reconnect(&mgr2).await;
                    });
                }
            })
        }));

        Ok(pc)
    }

    pub async fn start_offer(self: &Arc<Self>, manager: &RTCManagerHandle) -> Result<()> {
        self.teardown().await;

        let pc = self.new_peer_connection(manager).await?;
        *self.pc.write().await = Some(pc.clone());

        for label in &["tunnel", "chat", "signal", "stdio"] {
            let dc = pc.create_data_channel(label, None).await?;
            match *label {
                "tunnel" => self.init_tunnel_dc(dc, manager).await,
                "chat" => self.init_chat_dc(dc, manager).await,
                "signal" => self.init_signal_dc(dc, manager).await,
                "stdio" => self.init_stdio_dc(dc, manager).await,
                _ => {}
            }
        }

        let offer = pc.create_offer(None).await?;
        pc.set_local_description(offer.clone()).await?;

        let data = serde_json::to_string(&offer)?;
        manager
            .send_signal(SignalMessage {
                msg_type: "offer".into(),
                data: Some(data),
                sender_id: manager.self_id().to_string(),
                receiver_id: Some(self.peer_id.clone()),
                room_id: Some(manager.room_id().to_string()),
                role: Some(manager.self_role().await),
                ..Default::default()
            })
            .await;

        Ok(())
    }

    pub async fn handle_offer(
        self: &Arc<Self>,
        manager: &RTCManagerHandle,
        data: &str,
    ) -> Result<()> {
        self.teardown().await;

        let pc = self.new_peer_connection(manager).await?;
        *self.pc.write().await = Some(pc.clone());

        let mgr = manager.clone();
        let self_clone = self.clone();
        pc.on_data_channel(Box::new(move |dc| {
            let mgr = mgr.clone();
            let self_clone = self_clone.clone();
            Box::pin(async move {
                let label = dc.label().to_string();
                match label.as_str() {
                    "tunnel" => self_clone.init_tunnel_dc(dc, &mgr).await,
                    "chat" => self_clone.init_chat_dc(dc, &mgr).await,
                    "signal" => self_clone.init_signal_dc(dc, &mgr).await,
                    "stdio" => self_clone.init_stdio_dc(dc, &mgr).await,
                    _ => {}
                }
            })
        }));

        let offer: RTCSessionDescription = serde_json::from_str(data)?;
        pc.set_remote_description(offer).await?;

        let answer = pc.create_answer(None).await?;
        pc.set_local_description(answer.clone()).await?;

        let ans_data = serde_json::to_string(&answer)?;
        manager
            .send_signal(SignalMessage {
                msg_type: "answer".into(),
                data: Some(ans_data),
                sender_id: manager.self_id().to_string(),
                receiver_id: Some(self.peer_id.clone()),
                room_id: Some(manager.room_id().to_string()),
                role: Some(manager.self_role().await),
                ..Default::default()
            })
            .await;

        Ok(())
    }

    pub async fn handle_answer(&self, data: &str) -> Result<()> {
        let pc = self.pc.read().await;
        if let Some(ref pc) = *pc {
            let answer: RTCSessionDescription = serde_json::from_str(data)?;
            pc.set_remote_description(answer).await?;
        }
        Ok(())
    }

    pub async fn handle_candidate(&self, data: &str) -> Result<()> {
        let pc = self.pc.read().await;
        if let Some(ref pc) = *pc {
            let cand: RTCIceCandidateInit = serde_json::from_str(data)?;
            pc.add_ice_candidate(cand).await?;
        }
        Ok(())
    }

    async fn init_chat_dc(self: &Arc<Self>, dc: Arc<RTCDataChannel>, manager: &RTCManagerHandle) {
        *self.dc_chat.write().await = Some(dc.clone());
        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_message(Box::new(move |msg: DataChannelMessage| {
            let peer_id = peer_id.clone();
            let mgr = mgr.clone();
            Box::pin(async move {
                let text = String::from_utf8_lossy(&msg.data);
                mgr.notify_chat(&peer_id, &text).await;
            })
        }));
    }

    async fn init_tunnel_dc(self: &Arc<Self>, dc: Arc<RTCDataChannel>, manager: &RTCManagerHandle) {
        *self.dc_tunnel.write().await = Some(dc.clone());

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_open(Box::new(move || {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            Box::pin(async move { mgr.notify_tunnel_open(&peer_id).await })
        }));

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_close(Box::new(move || {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            Box::pin(async move { mgr.notify_tunnel_close(&peer_id).await })
        }));

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_message(Box::new(move |msg: DataChannelMessage| {
            let peer_id = peer_id.clone();
            let mgr = mgr.clone();
            Box::pin(async move {
                mgr.notify_tunnel_msg(&peer_id, &msg.data).await;
            })
        }));
    }

    async fn init_signal_dc(self: &Arc<Self>, dc: Arc<RTCDataChannel>, manager: &RTCManagerHandle) {
        *self.dc_signal.write().await = Some(dc.clone());

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_open(Box::new(move || {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            Box::pin(async move {
                mgr.broadcast_peer_list(&peer_id).await;
                mgr.notify_peer_connected(&peer_id).await;
            })
        }));

        let mgr = manager.clone();
        dc.on_message(Box::new(move |msg: DataChannelMessage| {
            let mgr = mgr.clone();
            Box::pin(async move {
                mgr.handle_raw_relay(&msg.data).await;
            })
        }));
    }

    async fn init_stdio_dc(self: &Arc<Self>, dc: Arc<RTCDataChannel>, manager: &RTCManagerHandle) {
        *self.dc_stdio.write().await = Some(dc.clone());

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_open(Box::new(move || {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            Box::pin(async move { mgr.notify_stdio_open(&peer_id).await })
        }));

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_close(Box::new(move || {
            let mgr = mgr.clone();
            let peer_id = peer_id.clone();
            Box::pin(async move { mgr.notify_stdio_close(&peer_id).await })
        }));

        let peer_id = self.peer_id.clone();
        let mgr = manager.clone();
        dc.on_message(Box::new(move |msg: DataChannelMessage| {
            let peer_id = peer_id.clone();
            let mgr = mgr.clone();
            Box::pin(async move {
                mgr.notify_stdio_msg(&peer_id, &msg.data).await;
            })
        }));
    }

    pub async fn teardown(&self) {
        let pc = {
            let mut w = self.pc.write().await;
            let pc = w.take();
            *self.dc_tunnel.write().await = None;
            *self.dc_chat.write().await = None;
            *self.dc_signal.write().await = None;
            *self.dc_stdio.write().await = None;
            pc
        };
        if let Some(pc) = pc {
            debug!("[{}] Tearing down connection", self.peer_id);
            let _ = pc.close().await;
        }
    }

    async fn handle_reconnect(self: &Arc<Self>, manager: &RTCManagerHandle) {
        {
            let mut flag = self.reconnecting.write().await;
            if *flag {
                return;
            }
            *flag = true;
        }

        self.teardown().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        manager.request_peer_connection(&self.peer_id).await;

        *self.reconnecting.write().await = false;
    }

    pub async fn close(&self) {
        self.teardown().await;
    }

    pub async fn send_chat(&self, msg: &str) -> Result<()> {
        let dc = self.dc_chat.read().await;
        if let Some(ref dc) = *dc {
            dc.send_text(msg.to_string()).await?;
        }
        Ok(())
    }

    pub async fn send_signal_relay(&self, data: &[u8]) -> Result<()> {
        let dc = self.dc_signal.read().await;
        match *dc {
            Some(ref dc) => {
                dc.send(&bytes::Bytes::copy_from_slice(data)).await?;
                Ok(())
            }
            None => anyhow::bail!("signal data channel not ready"),
        }
    }
}
