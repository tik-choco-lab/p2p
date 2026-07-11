use std::sync::Arc;

use mistlib_core::signaling::MessageContent;
use mistlib_core::transport::NetworkEvent;
use mistlib_core::types::NodeId;
use tokio::sync::mpsc;

use super::SessionCtx;

impl super::MistEngine {
    /// Pumps `ctx`'s network events until the session is torn down
    /// (`ctx.cancel`) or its transport's event channel closes. Unlike the
    /// single-session original, this no longer re-reads shared engine state
    /// per event: `ctx` *is* the running session for the lifetime of this loop.
    pub(super) async fn process_network_events(
        &self,
        mut rx: mpsc::UnboundedReceiver<NetworkEvent>,
        ctx: Arc<SessionCtx>,
    ) {
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                event = rx.recv() => {
                    let Some(event) = event else { break };
                    let Some(ov) = ctx.overlay.as_ref() else { continue };

                    let from_origin = event.from.clone();
                    match mistlib_core::overlay::wire::deserialize::<mistlib_core::overlay::OverlayEnvelope>(
                        &event.data,
                    ) {
                        Ok(envelope) => {
                            self.handle_overlay_envelope(envelope, from_origin, &ctx, ov).await;
                        }
                        Err(e) => {
                            tracing::trace!("process_network_events: bincode deserialize failed ({e}), trying storage protocol");
                            self.handle_storage_message(&event.data, from_origin, &ctx);
                        }
                    }
                }
            }
        }
    }

    async fn handle_overlay_envelope(
        &self,
        envelope: mistlib_core::overlay::OverlayEnvelope,
        from_origin: NodeId,
        ctx: &Arc<SessionCtx>,
        ov: &Arc<mistlib_core::overlay::OverlayRouter>,
    ) {
        ov.learn_route(&envelope.from, &from_origin);
        ctx.touch_node(&from_origin);
        ctx.touch_node(&envelope.from);

        let to_self = envelope.to == *self.self_id.lock().unwrap() || envelope.to.0.is_empty();
        let content = envelope.content.clone();

        let result = ov.handle_envelope(envelope, from_origin.clone());
        for action in result.actions {
            self.handle_action_for(ctx.clone(), action);
        }

        if to_self && result.should_deliver {
            self.dispatch_local_message(content, from_origin, ctx);
        }
    }

    fn dispatch_local_message(
        &self,
        content: MessageContent,
        from_origin: NodeId,
        ctx: &Arc<SessionCtx>,
    ) {
        match content {
            MessageContent::Raw(payload) => {
                let _ = self.handle_storage_message(&payload, from_origin.clone(), ctx);
                super::dispatch_event(super::EVENT_RAW, &ctx.room_id, &from_origin, &payload);
            }
            MessageContent::Overlay(overlay_msg) => {
                if overlay_msg.is_internal_control() {
                    return;
                }
                super::dispatch_event(
                    super::EVENT_OVERLAY,
                    &ctx.room_id,
                    &from_origin,
                    &overlay_msg.payload,
                );
            }
            MessageContent::Data(signaling_data) => {
                ctx.ensure_node_registered(&signaling_data.sender_id);
                if let Some(handler) = ctx.p2p_signaling_handler.clone() {
                    self.runtime.handle().spawn(async move {
                        if let Err(err) = handler
                            .handle_message(MessageContent::Data(signaling_data))
                            .await
                        {
                            tracing::warn!("NativeEngine: p2p signaling handler failed: {:?}", err);
                        }
                    });
                } else {
                    tracing::debug!(
                        "NativeEngine: p2p signaling relay not configured, dropping message"
                    );
                }
            }
        }
    }

    /// Handles the storage control protocol (WANT/QUERY/HAVE/...). `ctx` is
    /// the session the message arrived on, threaded through to
    /// `storage::handle_want`/`handle_query` so their HAVE/HAVE_STATUS
    /// replies go back out via the same session's transport (SPEC-15 rule 8).
    fn handle_storage_message(
        &self,
        data: &[u8],
        from_origin: NodeId,
        ctx: &Arc<SessionCtx>,
    ) -> bool {
        use crate::storage::resolver;

        if let Some(cid) = resolver::parse_want_message(data) {
            let from = from_origin.clone();
            let ctx = ctx.clone();
            self.runtime.handle().spawn(async move {
                crate::storage::handle_want(ctx, from, cid).await;
            });
            true
        } else if let Some(cid) = resolver::parse_query_message(data) {
            let from = from_origin.clone();
            let ctx = ctx.clone();
            self.runtime.handle().spawn(async move {
                crate::storage::handle_query(ctx, from, cid).await;
            });
            true
        } else if let Some(cid) = resolver::parse_have_status_message(data) {
            crate::storage::handle_have_status(from_origin, cid);
            true
        } else if let Some((cid, data)) = resolver::parse_have_message(data) {
            crate::storage::handle_have(cid, data);
            true
        } else if let Some((cid, chunk_index, chunk_total, data)) =
            resolver::parse_have_chunk_message(data)
        {
            crate::storage::handle_have_chunk(cid, chunk_index, chunk_total, data);
            true
        } else {
            false
        }
    }
}
