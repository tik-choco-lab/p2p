use super::NostrSignaler;
use mistlib_core::error::MistError;
use mistlib_core::signaling::nostr::{
    accept_message_order as accept_nostr_message_order,
    accept_sender_for_payload as accept_nostr_sender_for_payload, decode_discovery_event,
    decode_message_event, is_room_mailbox_message, record_discovery_and_should_request,
    MessageOrderAcceptance, DEFAULT_MAX_DISCOVERY_RESPONDERS_PER_PEER,
};
use mistlib_core::signaling::{MessageContent, SignalingType};
use tokio::sync::mpsc;

impl NostrSignaler {
    async fn accept_sender_for_payload(
        &self,
        sender_pubkey: &str,
        data: &mistlib_core::signaling::SignalingData,
    ) -> bool {
        let sender_was_requested = self.requested_pubkeys.lock().await.contains(sender_pubkey);
        let accepted = {
            let mut table = self.discovery_table.lock().await;
            accept_nostr_sender_for_payload(&mut table, sender_was_requested, sender_pubkey, data)
        };
        if !accepted {
            tracing::warn!(
                "NostrSignaler: dropping unexpected signaling payload from {} node={} type={:?}",
                sender_pubkey,
                data.sender_id.0,
                data.signaling_type
            );
        }
        accepted
    }

    async fn accept_message_order(
        &self,
        sender_pubkey: &str,
        message_id: Option<&str>,
        sequence: Option<u64>,
    ) -> bool {
        let outcome = {
            let mut dedupe = self.message_dedupe.lock().await;
            let mut sequences = self.incoming_sequences.lock().await;
            accept_nostr_message_order(
                &mut dedupe,
                &mut sequences,
                sender_pubkey,
                message_id,
                sequence,
            )
        };
        if let MessageOrderAcceptance::Gap { last, sequence } = outcome {
            tracing::warn!(
                "NostrSignaler: message sequence gap from {}: last={} next={}",
                sender_pubkey,
                last,
                sequence
            );
        }
        outcome.is_accepted()
    }

    pub(super) async fn process_event(
        &self,
        event: mistlib_core::signaling::nostr::NostrEvent,
        incoming_tx: mpsc::Sender<MessageContent>,
    ) -> mistlib_core::error::Result<()> {
        let session_epoch = self.session_epoch();
        let identity = self.current_identity().await;
        if event.pubkey == identity.public_key {
            return Ok(());
        }

        if event.kind == self.codec_config.discovery_kind {
            let Some(room_id) = self.current_room_id().await else {
                return Ok(());
            };
            let decoded =
                decode_discovery_event(&self.codec_config, &self.crypto, &event, &room_id)?;
            if self.current_room_id().await.as_deref() != Some(room_id.as_str())
                || !self.session_is_current(session_epoch)
            {
                return Ok(());
            }
            {
                let mut dedupe = self.dedupe.lock().await;
                if !dedupe.check_and_insert(&event.id) {
                    return Ok(());
                }
            }
            // A peer that disconnected and rejoined advertises a newer joined_at.
            // Forget our prior request for it so the surviving side re-requests
            // the fresh session instead of silently suppressing the duplicate.
            if let Some(joined_at) = decoded.joined_at {
                let rejoined = {
                    let mut sessions = self.peer_sessions.lock().await;
                    match sessions.get(&decoded.signaling_pubkey) {
                        Some(&previous) if joined_at > previous => {
                            sessions.insert(decoded.signaling_pubkey.clone(), joined_at);
                            true
                        }
                        Some(_) => false,
                        None => {
                            sessions.insert(decoded.signaling_pubkey.clone(), joined_at);
                            false
                        }
                    }
                };
                if rejoined {
                    self.requested_pubkeys
                        .lock()
                        .await
                        .remove(&decoded.signaling_pubkey);
                    self.incoming_sequences
                        .lock()
                        .await
                        .remove(&decoded.signaling_pubkey);
                    self.outgoing_sequences
                        .lock()
                        .await
                        .remove(&decoded.signaling_pubkey);
                }
            }
            let should_request = {
                let mut table = self.discovery_table.lock().await;
                if !self.session_is_current(session_epoch) {
                    return Ok(());
                }
                record_discovery_and_should_request(
                    &self.codec_config,
                    &mut table,
                    &decoded,
                    &room_id,
                    &identity.public_key,
                    DEFAULT_MAX_DISCOVERY_RESPONDERS_PER_PEER,
                )
            };
            if !self.session_is_current(session_epoch) {
                return Ok(());
            }
            let request_pubkeys = {
                let mut requested = self.requested_pubkeys.lock().await;
                if !self.session_is_current(session_epoch) {
                    return Ok(());
                }
                if should_request && requested.insert(decoded.signaling_pubkey.clone()) {
                    vec![decoded.signaling_pubkey.clone()]
                } else {
                    Vec::new()
                }
            };
            for pubkey in request_pubkeys {
                if self.current_room_id().await.as_deref() != Some(room_id.as_str())
                    || !self.session_is_current(session_epoch)
                {
                    return Ok(());
                }
                self.send_request_to_pubkey(&pubkey, &room_id).await?;
            }
            return Ok(());
        }

        if event.kind == self.codec_config.message_kind {
            let Some(room_id) = self.current_room_id().await else {
                return Ok(());
            };
            let decoded = match decode_message_event(
                &self.codec_config,
                &self.crypto,
                &identity,
                &self.local_node_id,
                &event,
                &room_id,
            ) {
                Ok(decoded) => decoded,
                Err(MistError::Signaling(err))
                    if err == "invalid encrypted Nostr payload"
                        && is_room_mailbox_message(&self.codec_config, &event, &room_id) =>
                {
                    let mut dedupe = self.dedupe.lock().await;
                    dedupe.check_and_insert(&event.id);
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            if self.current_room_id().await.as_deref() != Some(room_id.as_str())
                || !self.session_is_current(session_epoch)
            {
                return Ok(());
            }
            {
                let mut dedupe = self.dedupe.lock().await;
                if !dedupe.check_and_insert(&event.id) {
                    return Ok(());
                }
            }
            let mut incoming = decoded.data;
            let sender_was_requested = self
                .requested_pubkeys
                .lock()
                .await
                .contains(&decoded.sender_pubkey);
            if !self
                .accept_sender_for_payload(&decoded.sender_pubkey, &incoming)
                .await
            {
                return Ok(());
            }
            if !self
                .accept_message_order(
                    &decoded.sender_pubkey,
                    decoded.message_id.as_deref(),
                    decoded.sequence,
                )
                .await
            {
                return Ok(());
            }
            if !self.session_is_current(session_epoch) {
                return Ok(());
            }
            let reply_pubkey = {
                let sender_rank = self
                    .codec_config
                    .topology_rank(&room_id, &decoded.sender_pubkey);
                let mut table = self.discovery_table.lock().await;
                if !self.session_is_current(session_epoch) {
                    return Ok(());
                }
                let known_sender = table.bind_node_checked_with_rank_and_rebind(
                    incoming.sender_id.clone(),
                    decoded.sender_pubkey.clone(),
                    decoded.expires_at,
                    sender_rank,
                    sender_was_requested && incoming.signaling_type == SignalingType::Request,
                )?;
                // A signaling message from this sender just passed validation,
                // dedupe, and ordering checks, so it is proof of life right now.
                // Renew its discovery entry using our own clock rather than
                // relying solely on the sender-declared `decoded.expires_at`,
                // so an active peer never lapses out of `node_to_pubkey` due to
                // a missed discovery re-announce cycle mid-exchange.
                table.touch_node(&incoming.sender_id, self.codec_config.ttl_seconds);
                if !known_sender
                    && incoming.signaling_type == SignalingType::Request
                    && incoming.receiver_id.is_broadcast()
                {
                    Some(decoded.sender_pubkey.clone())
                } else {
                    None
                }
            };
            if incoming.receiver_id.is_broadcast() {
                incoming.receiver_id = self.local_node_id.clone();
            }
            if self.current_room_id().await.as_deref() != Some(room_id.as_str())
                || !self.session_is_current(session_epoch)
            {
                return Ok(());
            }
            incoming_tx
                .send(MessageContent::Data(incoming))
                .await
                .map_err(|e| {
                    mistlib_core::error::MistError::Signaling(format!(
                        "NostrSignaler: incoming channel closed: {e}"
                    ))
                })?;
            if let Some(pubkey) = reply_pubkey {
                let first_request = { self.requested_pubkeys.lock().await.insert(pubkey.clone()) };
                if !first_request {
                    return Ok(());
                }
                if self.current_room_id().await.as_deref() != Some(room_id.as_str())
                    || !self.session_is_current(session_epoch)
                {
                    return Ok(());
                }
                self.send_request_to_pubkey(&pubkey, &room_id).await?;
            }
        }
        Ok(())
    }
}
