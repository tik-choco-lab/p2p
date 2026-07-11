use std::sync::Arc;

use mistlib_core::action::OverlayAction;
use mistlib_core::transport::Transport;
use mistlib_core::types::NodeId;

use super::{SessionCtx, ENGINE};

impl super::MistEngine {
    /// Applies a single overlay-generated action against `ctx`'s session
    /// stack (WebRTC transport, signaling dispatch). Spawned fire-and-forget
    /// so a slow send never blocks the caller: the per-session background
    /// tick loop, the network event pump, or `notify_peer_disconnected`.
    pub(crate) fn handle_action_for(&self, ctx: Arc<SessionCtx>, action: OverlayAction) {
        let handle = self.runtime.handle().clone();
        handle.spawn(async move {
            match action {
                OverlayAction::SendMessage { to, data, method } => {
                    if let Some(wt) = &ctx.webrtc_transport {
                        let result = if to.is_broadcast() {
                            wt.broadcast(data, method).await
                        } else {
                            wt.send(&to, data, method).await
                        };
                        if let Err(err) = result {
                            let target = if to.is_broadcast() {
                                NodeId::BROADCAST
                            } else {
                                &to.0
                            };
                            tracing::warn!("Failed to send overlay action to {target}: {err:?}");
                        }
                    }
                }
                OverlayAction::Connect { to } => {
                    if let Some(wt) = &ctx.webrtc_transport {
                        if let Err(err) = wt.connect(&to).await {
                            tracing::warn!("Failed to connect overlay action to {}: {err:?}", to.0);
                        }
                    }
                }
                OverlayAction::Disconnect { to } => {
                    if let Some(wt) = &ctx.webrtc_transport {
                        if let Err(err) = wt.disconnect(&to).await {
                            tracing::warn!(
                                "Failed to disconnect overlay action to {}: {err:?}",
                                to.0
                            );
                        }
                    }
                }
                OverlayAction::SuspectDisconnected { to } => {
                    if let Some(wt) = &ctx.webrtc_transport {
                        if let Err(err) = wt.suspect_disconnected(&to).await {
                            tracing::warn!(
                                "Failed to mark suspect-disconnected overlay action to {}: {err:?}",
                                to.0
                            );
                        }
                    }
                }
                OverlayAction::ClearSuspect { to } => {
                    if let Some(wt) = &ctx.webrtc_transport {
                        if let Err(err) = wt.clear_suspect(&to).await {
                            tracing::warn!(
                                "Failed to clear-suspect overlay action to {}: {err:?}",
                                to.0
                            );
                        }
                    }
                }
                OverlayAction::SendSignaling { to, envelope } => {
                    if let Some(sig) = &ctx.signaling_dispatch {
                        if let Err(err) = sig.send_signaling(&to, envelope.content).await {
                            tracing::warn!(
                                "Failed to send signaling overlay action to {}: {err:?}",
                                to.0
                            );
                        }
                    }
                }
            }
        });
    }

    /// Same as `handle_action_for`, but resolves `room_id` to its session
    /// first. This is what each session's `ActionHandler` (wired into its
    /// own `OverlayRouter` at construction time -- see
    /// `layers/native_l0/init.rs`) actually calls: mistlib-core's
    /// `ActionHandler` trait has no room parameter, so the handler instance
    /// itself is what carries the room_id.
    pub(crate) fn handle_action_in_room(&self, room_id: String, action: OverlayAction) {
        let handle = self.runtime.handle().clone();
        handle.spawn(async move {
            if let Some(ctx) = ENGINE.get_session(&room_id).await {
                ENGINE.handle_action_for(ctx, action);
            }
        });
    }
}
