use async_trait::async_trait;
use bytes::Bytes;
use mistlib_core::signaling::{MessageContent, Signaler, SignalingData, SignalingType};
use mistlib_core::transport::{NetworkEventHandler, Transport};
use mistlib_core::types::{ConnectionState, DeliveryMethod, NodeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock as StdRwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264, MIME_TYPE_OPUS};
use webrtc::api::APIBuilder;
use webrtc::api::API;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::signaling_state::RTCSignalingState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::{
    RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType,
};
use webrtc::rtp_transceiver::rtp_sender::RTCRtpSender;
use webrtc::rtp_transceiver::RTCPFeedback;
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;

/// Maps `mistlib_core::config::WebRtcConfig::ice_servers` (CONFIG.md-documented,
/// user-settable) into the `webrtc`-rs crate's `RTCIceServer` shape consumed by
/// `RTCConfiguration`. Kept as a pure function -- independent of any
/// `WebRtcTransport`/`RTCPeerConnection` state -- so the mapping is unit
/// testable on its own. An empty `configured` slice maps to an empty `Vec`,
/// i.e. a user who explicitly configures zero ICE servers is honored as-is;
/// the "default has one Google STUN entry" behavior comes from
/// `Config::new_default`, not from this function.
///
/// Unusable entries (no URLs, or turn/turns without credentials -- see
/// `IceServer::is_usable`) are dropped with a warning rather than forwarded:
/// webrtc-rs re-validates every entry inside `API::new_peer_connection`, so a
/// single bad entry would otherwise fail *every* `create_pc` call -- and with
/// it every connection attempt -- for the rest of the session.
pub(crate) fn map_ice_servers(configured: &[mistlib_core::config::IceServer]) -> Vec<RTCIceServer> {
    configured
        .iter()
        .filter(|server| {
            if server.is_usable() {
                true
            } else {
                tracing::warn!(
                    "ignoring unusable ICE server entry {:?}: turn/turns URLs require a \
                     non-empty username and credential",
                    server.urls
                );
                false
            }
        })
        .map(|server| RTCIceServer {
            urls: server.urls.clone(),
            username: server.username.clone().unwrap_or_default(),
            credential: server.credential.clone().unwrap_or_default(),
        })
        .collect()
}

/// Upper bound on buffered-but-not-yet-appliable ICE candidates per node
/// (candidates that arrive before the remote description is set). Mirrors
/// `mistlib_wasm::transport::webrtc::pending_candidates::MAX_PENDING_CANDIDATES_PER_NODE`
/// so native and wasm apply the same bounded-memory behavior instead of
/// letting a slow/stuck handshake accumulate candidates unboundedly.
pub(crate) const MAX_PENDING_CANDIDATES_PER_NODE: usize = 64;

/// Pushes `candidate` onto `list`, evicting the oldest entry once the count
/// exceeds [`MAX_PENDING_CANDIDATES_PER_NODE`]. Returns `true` if an entry was
/// evicted. Kept pure (no transport/lock state) so the eviction behavior is
/// unit-testable directly.
pub(crate) fn push_pending_candidate(list: &mut Vec<String>, candidate: String) -> bool {
    list.push(candidate);
    if list.len() > MAX_PENDING_CANDIDATES_PER_NODE {
        list.remove(0);
        true
    } else {
        false
    }
}

/// SPEC-13: whether `size` bytes crosses the "approaching the limit" warn
/// threshold (80% of `limit`). Factored out as a pure predicate, independent
/// of any transport state, so the boundary itself is unit-testable without
/// capturing `tracing` output. Cross-multiplied instead of using floating
/// point; safe from overflow because callers only reach this after already
/// confirming `size <= limit` (a `u32`).
pub(crate) fn exceeds_warn_threshold(size: usize, limit: u32) -> bool {
    size * 100 > limit as usize * 80
}

/// SPEC-13: same "lower ID wins" direction as the existing offer-glare rule
/// in `signaling::handle_offer` (`local_node_id.0 < remote_id.0`). Only the
/// initiator side attempts a one-shot ICE restart when a peer's disconnect
/// grace period begins; the other side waits for the restart offer to
/// arrive instead of racing its own. Kept pure so the direction is
/// unit-testable without a real `WebRtcTransport`/`RTCPeerConnection`.
pub(crate) fn is_ice_restart_initiator(local_id: &NodeId, remote_id: &NodeId) -> bool {
    local_id.0 < remote_id.0
}

/// Best-effort recovery for a failed negotiation step on `pc`: if the failure
/// left signaling state anywhere other than `Stable`, issues a `rollback` on
/// whichever side actually has a pending description, clearing it back to
/// `Stable`. Mirrors `mistlib-wasm`'s `rollback_to_stable_on_failure`
/// (`transport/webrtc.rs`) -- native never had an equivalent, so a rejected
/// `set_local_description`/`set_remote_description`, or a `send_signaling`
/// that fails *after* a local offer/answer was already applied (e.g. the
/// `RoutedSignaler` returning `RouteNotFound` for a peer whose overlay route
/// hasn't caught up with a just-established connection yet), left the peer
/// wedged in `HaveLocalOffer`/`HaveRemoteOffer` forever: every later
/// negotiation attempt for that peer (a renegotiation, an ICE restart, the
/// remote's own offer) would keep failing the "signaling state is not
/// stable"/glare precondition, since nothing else ever moved it back to
/// `Stable`. Call this from every fallible step in `send_offer`/`apply_offer`/
/// `handle_answer` so a transient failure self-heals instead of requiring the
/// whole peer to be torn down and reconnected from scratch.
///
/// Best-effort: the rollback call itself can fail too (e.g. the connection is
/// already closing) -- that's logged and swallowed, since the original error
/// is what the caller should act on.
pub(crate) async fn rollback_to_stable_on_failure(pc: &Arc<RTCPeerConnection>, remote_id: &NodeId) {
    let signaling_state = pc.signaling_state();
    match signaling_state {
        RTCSignalingState::HaveLocalOffer | RTCSignalingState::HaveLocalPranswer => {
            if let Err(err) = pc.set_local_description(rollback_description()).await {
                tracing::warn!(
                    "Rollback to stable (local) failed for {} after a negotiation error: {:?}",
                    remote_id,
                    err
                );
            }
        }
        RTCSignalingState::HaveRemoteOffer | RTCSignalingState::HaveRemotePranswer => {
            if let Err(err) = pc.set_remote_description(rollback_description()).await {
                tracing::warn!(
                    "Rollback to stable (remote) failed for {} after a negotiation error: {:?}",
                    remote_id,
                    err
                );
            }
        }
        RTCSignalingState::Stable | RTCSignalingState::Closed | RTCSignalingState::Unspecified => {}
    }
}

/// Builds a `Rollback`-typed `RTCSessionDescription`. `RTCSessionDescription`
/// has no public constructor for this variant (only `offer`/`answer`/
/// `pranswer`) and its `parsed` field is `pub(crate)` to the `webrtc` crate,
/// so a struct literal (even via `..Default::default()`) doesn't compile from
/// here -- go through its `Deserialize` impl instead, the same way
/// `parse_offer_payload`/`parse_answer_payload` already parse inbound SDP
/// payloads via `serde_json::from_str::<RTCSessionDescription>`. The literal
/// is fixed and always valid, so a parse failure would be a `webrtc`-crate
/// shape change this code needs to know about immediately.
fn rollback_description() -> RTCSessionDescription {
    serde_json::from_str(r#"{"type":"rollback","sdp":""}"#)
        .expect("rollback session description literal must always parse")
}

const SERVER_ID: &str = "server";
const CONNECTION_TIMEOUT_MS: u64 = 6000;
const DATA_CHANNEL_OPEN_TIMEOUT_MS: u64 = 5000;
const RECONNECT_COOLDOWN_MS: u64 = 3000;
const LAST_DISCONNECT_TTL_MS: u64 = RECONNECT_COOLDOWN_MS;
const DEFAULT_MAX_CONCURRENT_HANDSHAKES: usize = 6;
const CONNECT_REQUEST_RETRY_INTERVAL_MS: u64 = 1000;
const DEFAULT_CONNECT_REQUEST_RETRIES: u32 = 30;
#[cfg(test)]
pub(crate) const DISCONNECTED_GRACE_MS: u64 = 50;
#[cfg(not(test))]
pub(crate) const DISCONNECTED_GRACE_MS: u64 = 5000;

/// What triggered a peer's current reconnect-grace period. `ClearSuspect` may
/// only cancel a `LivenessSuspect`-origin grace: an `Ice`-origin one is left
/// alone for ICE's own recovery signal to end (see `PeerSharedHandles::clear_suspect`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraceOrigin {
    Ice,
    LivenessSuspect,
}

/// A single entry in `disconnected_since`: when the grace period started, and
/// what triggered it.
#[derive(Debug, Clone, Copy)]
pub struct DisconnectGrace {
    pub started_at: Instant,
    pub origin: GraceOrigin,
}

pub mod connection;
pub mod peer;
pub mod publish;
pub mod signaling;
pub mod stats;
pub mod sweeper;

pub use peer::{MediaTrackEvent, Peer};
pub use stats::SctpPeerStats;

use peer::PeerSharedHandles;

/// Registers exactly Opus (audio) + H264 (video) on `engine`, in place of
/// `MediaEngine::register_default_codecs()` (which also registers
/// VP8/VP9/AV1/H265/G722/PCMU/PCMA). See the comment at the `new()` call
/// site for why the answer side must be pinned to this set.
///
/// The parameters (fmtp lines, payload types, RTCP feedback) below are
/// copied verbatim from `register_default_codecs()` in webrtc-rs 0.13.0
/// (src/api/media_engine/mod.rs) so the negotiated codec profiles are
/// identical to what pion/webrtc-rs peers already expect -- only the
/// VP8/VP9/AV1/H265/G722/PCMU/PCMA entries from that function are omitted.
fn register_h264_opus_codecs(engine: &mut MediaEngine) -> webrtc::error::Result<()> {
    engine.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        RTPCodecType::Audio,
    )?;

    let video_rtcp_feedback = vec![
        RTCPFeedback {
            typ: "goog-remb".to_owned(),
            parameter: "".to_owned(),
        },
        RTCPFeedback {
            typ: "ccm".to_owned(),
            parameter: "fir".to_owned(),
        },
        RTCPFeedback {
            typ: "nack".to_owned(),
            parameter: "".to_owned(),
        },
        RTCPFeedback {
            typ: "nack".to_owned(),
            parameter: "pli".to_owned(),
        },
    ];
    for codec in [
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"
                        .to_owned(),
                rtcp_feedback: video_rtcp_feedback.clone(),
            },
            payload_type: 102,
            ..Default::default()
        },
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f"
                        .to_owned(),
                rtcp_feedback: video_rtcp_feedback.clone(),
            },
            payload_type: 127,
            ..Default::default()
        },
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                        .to_owned(),
                rtcp_feedback: video_rtcp_feedback.clone(),
            },
            payload_type: 125,
            ..Default::default()
        },
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f"
                        .to_owned(),
                rtcp_feedback: video_rtcp_feedback.clone(),
            },
            payload_type: 108,
            ..Default::default()
        },
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032"
                        .to_owned(),
                rtcp_feedback: video_rtcp_feedback.clone(),
            },
            payload_type: 123,
            ..Default::default()
        },
    ] {
        engine.register_codec(codec, RTPCodecType::Video)?;
    }

    Ok(())
}

/// Process-wide media-track handler. With multi-room sessions there is one
/// [`WebRtcTransport`] per session, created whenever a room is joined -- a
/// consumer registering before (or between) joins can't reach those future
/// transports, so registration stashes the sender here and every new
/// transport inherits it at construction. `app::register_media_track_handler`
/// also applies it to already-running sessions.
pub(crate) static GLOBAL_MEDIA_TX: Mutex<Option<mpsc::UnboundedSender<MediaTrackEvent>>> =
    Mutex::new(None);

pub struct WebRtcTransport {
    pub signaler: Arc<dyn Signaler>,
    pub local_node_id: NodeId,
    pub api: API,
    pub peers: Arc<tokio::sync::RwLock<HashMap<NodeId, Arc<Peer>>>>,
    pub event_handler: Mutex<Option<Arc<dyn NetworkEventHandler>>>,
    /// Sender for remote media track (audio/video) arrival events. `None` by
    /// default (data-channel-only mode); set via `set_media_track_handler`
    /// before connecting peers that are expected to carry media, e.g. by a
    /// WHIP/broadcast-facing consumer such as mistlib-media.
    pub(crate) media_tx: Mutex<Option<mpsc::UnboundedSender<MediaTrackEvent>>>,
    pub connection_states: Arc<StdRwLock<HashMap<NodeId, ConnectionState>>>,
    pub room_id: Arc<StdRwLock<String>>,
    pub pending_candidates: Arc<tokio::sync::RwLock<HashMap<NodeId, Vec<String>>>>,
    /// STUN/TURN servers used for every new `RTCPeerConnection` (see
    /// `connection::create_pc`). Defaults to a single Google STUN entry
    /// (matching the pre-config-wiring hardcoded behavior) and is overridden
    /// via `set_ice_servers` from `Config::webrtc.ice_servers` at session
    /// construction (`layers::native_l0::init::build_webrtc_transport`).
    pub ice_servers: Arc<StdRwLock<Vec<RTCIceServer>>>,
    pub max_connections: AtomicU32,
    /// SPEC-13: upper bound (bytes, post-envelope/pre-wire) enforced by
    /// `Transport::send` before a payload ever reaches a DataChannel.
    /// Defaults to 64KiB, matching `Config::limits.max_message_bytes`'s
    /// default, and is overridden via `set_max_message_bytes` from config at
    /// session construction (`layers::native_l0::init::build_webrtc_transport`).
    pub max_message_bytes: AtomicU32,
    pub connection_attempt_ids: Arc<StdRwLock<HashMap<NodeId, u32>>>,
    pub connect_request_attempt_ids: Arc<StdRwLock<HashMap<NodeId, u32>>>,
    pub pc_connected_at: Arc<StdRwLock<HashMap<NodeId, Instant>>>,
    pub handshake_semaphore: Arc<Semaphore>,
    pub handshake_permits: Arc<StdRwLock<HashMap<NodeId, OwnedSemaphorePermit>>>,
    pub last_disconnect_at: Arc<StdRwLock<HashMap<NodeId, Instant>>>,
    pub disconnected_since: Arc<StdRwLock<HashMap<NodeId, DisconnectGrace>>>,
    pub next_connection_attempt_id: AtomicU32,
    pub isolation_recovery_epoch: Arc<std::sync::atomic::AtomicU64>,
    pub sweeper_started: AtomicBool,
    pub(crate) sweeper_cancel: Mutex<Option<CancellationToken>>,
    /// Local media tracks marked "published" via `publish_local_track`
    /// (`transports::webrtc::publish`): every currently-connected peer gets
    /// them attached (with renegotiation), and every future peer gets them
    /// automatically at connection setup (`connection::create_pc`), before
    /// its first offer/answer. Keyed by `TrackLocal::id()` so publishing the
    /// same id again just replaces the stored track. Mirrors mistlib-wasm's
    /// `WasmWebRtcTransport::local_tracks` (`published` bookkeeping) --
    /// see `mistlib-wasm/src/transport/webrtc.rs`.
    pub(crate) published_tracks: Arc<StdRwLock<HashMap<String, Arc<TrackLocalStaticRTP>>>>,
    /// Per-peer RTP senders created for published tracks, so
    /// `attach_published_tracks_to_peer` can skip a track already attached
    /// to a given peer and `unpublish_local_track` can find the sender to
    /// remove. Mirrors mistlib-wasm's `peer_senders`.
    pub(crate) published_senders: Arc<tokio::sync::RwLock<PublishedSenders>>,
}

/// Per-peer map of published-track RTP senders, keyed by peer then track id.
pub(crate) type PublishedSenders = HashMap<NodeId, HashMap<String, Arc<RTCRtpSender>>>;

impl WebRtcTransport {
    pub fn new(signaler: Arc<dyn Signaler>, local_node_id: NodeId) -> Self {
        let mut m = MediaEngine::default();
        // Answer-side codec pinning: register ONLY H264 (video) + Opus
        // (audio) instead of `register_default_codecs()`. That default set
        // also includes VP8/VP9/AV1/H265/G722/PCMU/PCMA, and browsers
        // (tc-chat via mistlib-wasm's `publish_local_track`) typically list
        // VP8 first in their offer's codec preference order. Since we are
        // the answerer, the codecs WE list in the answer are what the
        // browser is constrained to send -- if VP8 were present in our
        // answer, the browser could pick it, and this native peer only ever
        // relays H264 onward (to RTSP/AVPro on the VRChat side, which has no
        // VP8 decode path). Restricting the engine to H264 + Opus forces the
        // answer to advertise only codecs we can actually consume, so the
        // browser is left with H264 as its only viable video choice.
        // Data channels are unaffected by codec registration -- SCTP doesn't
        // negotiate through the MediaEngine.
        register_h264_opus_codecs(&mut m)
            .expect("registering a fixed H264+Opus codec set on a fresh MediaEngine cannot fail");

        // Wire up the interceptor pipeline (NACK generator/responder, RTCP
        // sender/receiver reports, receive-side TWCC) so RTCP feedback the
        // codecs above already advertise (`nack`/`nack pli` in
        // `register_h264_opus_codecs`) actually does something. Without a
        // registered interceptor chain -- the default before this change --
        // `APIBuilder` falls back to an empty `Registry` (see
        // `APIBuilder::build` in webrtc-rs), so lost inbound packets were
        // never NACKed and no retransmissions were ever served for outbound
        // ones; the relay could only mitigate loss by dropping until the
        // next IDR. `register_default_interceptors` must run after codec
        // registration (it registers additional feedback/header-extension
        // capabilities against the already-populated `MediaEngine`, mirroring
        // pion/webrtc-rs's own `NewAPI` example ordering).
        let mut registry = webrtc::interceptor::registry::Registry::new();
        registry =
            webrtc::api::interceptor_registry::register_default_interceptors(registry, &mut m)
                .expect(
                    "registering the default interceptor set (NACK, RTCP reports, TWCC) against a \
                 MediaEngine with only H264+Opus registered cannot fail",
                );

        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_interceptor_registry(registry)
            .build();

        let max_concurrent_handshakes = std::env::var("MIST_WEBRTC_MAX_CONCURRENT_HANDSHAKES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_CONCURRENT_HANDSHAKES);

        Self {
            signaler,
            local_node_id,
            api,
            peers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            event_handler: Mutex::new(None),
            media_tx: Mutex::new(GLOBAL_MEDIA_TX.lock().unwrap().clone()),
            connection_states: Arc::new(StdRwLock::new(HashMap::new())),
            room_id: Arc::new(StdRwLock::new("lobby".to_string())),
            pending_candidates: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            ice_servers: Arc::new(StdRwLock::new(vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_string()],
                ..Default::default()
            }])),
            max_connections: AtomicU32::new(30),
            max_message_bytes: AtomicU32::new(65536),
            connection_attempt_ids: Arc::new(StdRwLock::new(HashMap::new())),
            connect_request_attempt_ids: Arc::new(StdRwLock::new(HashMap::new())),
            pc_connected_at: Arc::new(StdRwLock::new(HashMap::new())),
            handshake_semaphore: Arc::new(Semaphore::new(max_concurrent_handshakes)),
            handshake_permits: Arc::new(StdRwLock::new(HashMap::new())),
            last_disconnect_at: Arc::new(StdRwLock::new(HashMap::new())),
            disconnected_since: Arc::new(StdRwLock::new(HashMap::new())),
            next_connection_attempt_id: AtomicU32::new(1),
            isolation_recovery_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sweeper_started: AtomicBool::new(false),
            sweeper_cancel: Mutex::new(None),
            published_tracks: Arc::new(StdRwLock::new(HashMap::new())),
            published_senders: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub fn set_room_id(&self, room_id: String) {
        let mut room = self.room_id.write().unwrap();
        *room = room_id;
    }

    pub fn set_max_connections(&self, max: u32) {
        self.max_connections.store(max, Ordering::Relaxed);
    }

    /// SPEC-13: overrides the max payload size accepted by `Transport::send`
    /// (and, transitively, `broadcast`). A `set_config` call applies this
    /// best-effort -- it only affects sends issued after the store, not any
    /// already in flight.
    pub fn set_max_message_bytes(&self, max: u32) {
        self.max_message_bytes.store(max, Ordering::Relaxed);
    }

    /// SPEC-13: rejects a payload larger than `max_message_bytes` before it
    /// ever reaches a DataChannel. Checked first thing in `Transport::send`,
    /// ahead of the peer/channel lookup, so it applies even to a target with
    /// no live connection (see `transports/webrtc/tests/message_size.rs`) and
    /// so `broadcast` (which forwards to `send` per target) inherits it for
    /// free. Does not apply to the signaling (offer/answer/candidate) path,
    /// which never calls this.
    fn check_message_size(&self, size: usize) -> mistlib_core::error::Result<()> {
        let limit = self.max_message_bytes.load(Ordering::Relaxed);
        if size > limit as usize {
            return Err(mistlib_core::error::MistError::MessageTooLarge { size, limit });
        }
        if exceeds_warn_threshold(size, limit) {
            tracing::warn!("message size {size} bytes exceeds 80% of max_message_bytes ({limit})");
        }
        Ok(())
    }

    /// Overrides the STUN/TURN servers used by every subsequently-created
    /// `RTCPeerConnection`. Does not affect peers already connected.
    pub fn set_ice_servers(&self, servers: Vec<RTCIceServer>) {
        *self.ice_servers.write().unwrap() = servers;
    }

    /// Registers a channel to receive remote media track (audio/video) arrival
    /// events for peers connected from this point forward. Peers created before
    /// this call was made do not retroactively get the handler wired up.
    pub fn set_media_track_handler(&self, tx: mpsc::UnboundedSender<MediaTrackEvent>) {
        let mut media_tx = self.media_tx.lock().unwrap();
        *media_tx = Some(tx);
    }

    pub(crate) fn get_room_id(&self) -> String {
        self.room_id.read().unwrap().clone()
    }

    pub(crate) fn peer_handles(&self) -> PeerSharedHandles {
        PeerSharedHandles {
            connection_states: self.connection_states.clone(),
            peers: self.peers.clone(),
            pending_candidates: self.pending_candidates.clone(),
            connection_attempt_ids: self.connection_attempt_ids.clone(),
            connect_request_attempt_ids: self.connect_request_attempt_ids.clone(),
            pc_connected_at: self.pc_connected_at.clone(),
            handshake_permits: self.handshake_permits.clone(),
            last_disconnect_at: self.last_disconnect_at.clone(),
            disconnected_since: self.disconnected_since.clone(),
            signaler: self.signaler.clone(),
            isolation_recovery_epoch: self.isolation_recovery_epoch.clone(),
            room_id: self.get_room_id(),
            local_node_id: self.local_node_id.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) async fn cleanup_session(&self, node: &NodeId, force_failed: bool) {
        self.cleanup_session_with_reason(node, force_failed, "cleanup_session")
            .await;
    }

    pub(crate) async fn cleanup_session_with_reason(
        &self,
        node: &NodeId,
        force_failed: bool,
        reason: &'static str,
    ) {
        self.peer_handles()
            .cleanup_session_with_reason(node, force_failed, reason)
            .await;
        // Drop stale published-track sender bookkeeping for this node -- the
        // peer connection it referred to is gone, and `connection::create_pc`
        // also clears this defensively on the next reconnect, but doing it
        // here too keeps `published_senders` from accumulating dead entries
        // across many connect/disconnect cycles for nodes that never
        // reconnect.
        self.published_senders.write().await.remove(node);
    }

    /// Same as `cleanup_session_with_reason`, but only tears down `node` if
    /// `self.peers` still maps it to `expected` -- see
    /// `PeerSharedHandles::cleanup_session_if_current` for the race this
    /// closes. Used by the connect-timeout watchdog and the periodic
    /// sweeper, both of which act on an earlier `Peer` snapshot.
    pub(crate) async fn cleanup_session_if_current(
        &self,
        node: &NodeId,
        expected: &std::sync::Weak<Peer>,
        force_failed: bool,
        reason: &'static str,
    ) {
        let cleaned = self
            .peer_handles()
            .cleanup_session_if_current(node, expected, force_failed, reason)
            .await;
        if cleaned {
            // Only drop `published_senders` bookkeeping when this call
            // actually superseded the registration it was aimed at -- if
            // `node` now belongs to a newer, live peer, that peer's sender
            // bookkeeping must be left alone.
            self.published_senders.write().await.remove(node);
        }
    }

    pub async fn close_all_peer_connections(&self) {
        let peers = {
            let mut lock = self.peers.write().await;
            std::mem::take(&mut *lock)
        };

        for (node, peer) in peers {
            tracing::warn!("[WebRTC Close] reason=room_close_all node={}", node);
            peer.close_all().await;
            crate::mem::record_peer_cleaned();
        }

        self.pending_candidates.write().await.clear();
        self.connection_attempt_ids.write().unwrap().clear();
        self.connect_request_attempt_ids.write().unwrap().clear();
        self.pc_connected_at.write().unwrap().clear();
        self.handshake_permits.write().unwrap().clear();
        self.connection_states.write().unwrap().clear();
        self.last_disconnect_at.write().unwrap().clear();
        self.disconnected_since.write().unwrap().clear();
    }

    /// Pushes `data` onto `peer`'s ordered send queue (`Peer::send_tx`,
    /// drained by `Peer::spawn_send_queue`). Shared by the async
    /// `Transport::send` (which resolves `peer` via an awaited read of
    /// `self.peers`) and `try_enqueue_send` (which resolves it synchronously)
    /// -- both ultimately just need to hand the message to the same
    /// single-writer-per-peer queue.
    fn enqueue_on_peer(
        node: &NodeId,
        peer: &Peer,
        data: Bytes,
        method: DeliveryMethod,
    ) -> mistlib_core::error::Result<()> {
        peer.send_tx
            .try_send(peer::QueuedSend { data, method })
            .map_err(|_| {
                mistlib_core::error::MistError::Internal(format!(
                    "Send queue full for {:?} method {:?} (peer not keeping up or stuck disconnected)",
                    node, method
                ))
            })
    }

    /// Fully synchronous version of `Transport::send` -- no `.await`
    /// anywhere, so it can be called inline from `MistEngine::handle_action_for`
    /// (`engine/action.rs`) without spawning a task for it. This is what
    /// makes the fix in `Peer::spawn_send_queue` actually hold end to end:
    /// that queue only preserves the order messages are *enqueued* in, so
    /// the enqueue call itself must happen synchronously, in the exact order
    /// `OverlayAction::SendMessage` actions were produced (overlay seq
    /// numbers are stamped synchronously too, in `OverlayRouter::wrap_data`)
    /// -- if this were spawned instead, N concurrently spawned enqueue calls
    /// could still run in a different order than they were spawned in
    /// (tokio's scheduler makes no such guarantee), which would silently
    /// reintroduce the exact reordering bug this queue exists to fix.
    ///
    /// Uses `try_read()` (never blocks) rather than `.await` on `self.peers`:
    /// that lock is only ever write-locked for brief, non-blocking swaps
    /// (`replace_peer_and_close_old`, `cleanup_session_impl`, ...), so a
    /// `try_read()` failure here is an exceedingly rare, transient race --
    /// treated the same as any other momentarily-undeliverable case (message
    /// dropped, caller logs the error), not worse than the loss this whole
    /// fix is meant to reduce.
    pub(crate) fn try_enqueue_send(
        &self,
        node: &NodeId,
        data: Bytes,
        method: DeliveryMethod,
    ) -> mistlib_core::error::Result<()> {
        self.check_message_size(data.len())?;

        let peers = self.peers.try_read().map_err(|_| {
            mistlib_core::error::MistError::Internal(format!(
                "peers map momentarily locked while enqueueing send to {:?}",
                node
            ))
        })?;
        let peer = peers.get(node).ok_or_else(|| {
            mistlib_core::error::MistError::Internal(format!("Node not found: {:?}", node))
        })?;
        Self::enqueue_on_peer(node, peer, data, method)
    }

    /// Synchronous broadcast built on `try_enqueue_send` -- see its doc
    /// comment. Best-effort per target, exactly like the async `broadcast`.
    pub(crate) fn try_enqueue_broadcast(&self, data: Bytes, method: DeliveryMethod) {
        for target in self.get_connected_nodes() {
            let _ = self.try_enqueue_send(&target, data.clone(), method);
        }
    }
}

impl WebRtcTransport {
    /// シグナリングサーバーへ参加通知を送る。
    /// `start()` の後、ルームに参加する準備が整った時点で明示的に呼ぶこと。
    pub async fn announce_to_room(&self) -> mistlib_core::error::Result<()> {
        let room_id = self.get_room_id();
        self.signaler
            .send_signaling(
                &NodeId(SERVER_ID.to_string()),
                MessageContent::Data(SignalingData {
                    sender_id: self.local_node_id.clone(),
                    receiver_id: NodeId("".to_string()),
                    room_id,
                    data: "".to_string(),
                    signaling_type: SignalingType::Request,
                }),
            )
            .await
    }
}

#[async_trait]
impl Transport for WebRtcTransport {
    async fn start(
        &self,
        handler: Arc<dyn NetworkEventHandler>,
    ) -> mistlib_core::error::Result<()> {
        self.ensure_session_sweeper();
        let mut h = self.event_handler.lock().unwrap();
        *h = Some(handler);
        Ok(())
    }

    async fn send(
        &self,
        node: &NodeId,
        data: Bytes,
        method: DeliveryMethod,
    ) -> mistlib_core::error::Result<()> {
        self.check_message_size(data.len())?;

        let peer = {
            let peers = self.peers.read().await;
            peers.get(node).cloned()
        }
        .ok_or_else(|| {
            mistlib_core::error::MistError::Internal(format!("Node not found: {:?}", node))
        })?;

        Self::enqueue_on_peer(node, &peer, data, method)
    }

    async fn broadcast(
        &self,
        data: Bytes,
        method: DeliveryMethod,
    ) -> mistlib_core::error::Result<()> {
        let targets = self.get_connected_nodes();
        for target in targets {
            let _ = self.send(&target, data.clone(), method).await;
        }
        Ok(())
    }

    fn get_connection_state(&self, node: &NodeId) -> ConnectionState {
        let states = self.connection_states.read().unwrap();
        states
            .get(node)
            .cloned()
            .unwrap_or(ConnectionState::Disconnected)
    }

    async fn connect(&self, node: &NodeId) -> mistlib_core::error::Result<()> {
        {
            let peers = self.peers.read().await;
            if peers.contains_key(node) {
                return Ok(());
            }
        }

        if self.local_node_id.0 > node.0 {
            tracing::debug!(
                "[Glare] requesting lower-id peer {} to initiate WebRTC offer",
                node
            );
            return self.request_lower_id_offer(node).await;
        }

        let wait_duration = {
            let last_disconnect = self.last_disconnect_at.read().unwrap();
            last_disconnect.get(node).copied().and_then(|at| {
                let elapsed = at.elapsed();
                if elapsed < Duration::from_millis(RECONNECT_COOLDOWN_MS) {
                    Some(Duration::from_millis(RECONNECT_COOLDOWN_MS) - elapsed)
                } else {
                    None
                }
            })
        };

        if let Some(wait_duration) = wait_duration {
            tracing::warn!(
                "[Reconnect] waiting {:?} before retrying connection to {}",
                wait_duration,
                node
            );
            tokio::time::sleep(wait_duration).await;
        }

        {
            let mut states = self.connection_states.write().unwrap();
            if states.contains_key(node) {
                return Ok(());
            }
            let max = self.max_connections.load(Ordering::Relaxed) as usize;
            let count = states
                .values()
                .filter(|s| {
                    matches!(
                        **s,
                        ConnectionState::Connected
                            | ConnectionState::Connecting
                            | ConnectionState::Reconnecting
                    )
                })
                .count();
            if count >= max {
                return Ok(());
            }
            states.insert(node.clone(), ConnectionState::Connecting);
            tracing::debug!("[CS] INSERT connect: {} total={}", node, states.len());
        }

        let result = self.connect_inner(node).await;
        if result.is_err() {
            let states = self.connection_states.read().unwrap();
            tracing::debug!("[CS] FAILED connect_err: {} total={}", node, states.len());
        }
        result
    }

    async fn disconnect(&self, node: &NodeId) -> mistlib_core::error::Result<()> {
        self.cleanup_session_with_reason(node, false, "explicit_disconnect")
            .await;
        Ok(())
    }

    async fn suspect_disconnected(&self, node: &NodeId) -> mistlib_core::error::Result<()> {
        if !self.peer_handles().mark_suspect_disconnected(node) {
            tracing::debug!(
                "[CS] ignored suspect-disconnected for {} (not Connected, or already in grace)",
                node
            );
        }
        Ok(())
    }

    async fn clear_suspect(&self, node: &NodeId) -> mistlib_core::error::Result<()> {
        if !self.peer_handles().clear_suspect(node) {
            tracing::debug!(
                "[CS] ignored clear-suspect for {} (no liveness-suspect grace active)",
                node
            );
        }
        Ok(())
    }

    fn get_connected_nodes(&self) -> Vec<NodeId> {
        let states = self.connection_states.read().unwrap();
        states
            .iter()
            .filter(|(_, &s)| s == ConnectionState::Connected)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

impl WebRtcTransport {
    pub fn get_active_connection_states(&self) -> Vec<(NodeId, ConnectionState)> {
        let states = self.connection_states.read().unwrap();
        states
            .iter()
            .filter(|(_, s)| {
                matches!(
                    **s,
                    ConnectionState::Connected
                        | ConnectionState::Connecting
                        | ConnectionState::Reconnecting
                )
            })
            .map(|(id, s)| (id.clone(), *s))
            .collect()
    }
}

#[cfg(test)]
pub(crate) mod tests;
