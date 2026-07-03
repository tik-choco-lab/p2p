use super::NostrSignaler;
use mistlib_core::signaling::nostr::{
    build_discovery_event_with_joined_at, build_message_event_with_sequence, event_frame_json,
    next_outgoing_sequence,
};
use mistlib_core::signaling::{SignalingData, SignalingType};
use mistlib_core::stats::STATS;
use mistlib_core::types::NodeId;
#[cfg(not(test))]
use tokio::time::Duration;

#[cfg(not(test))]
fn discovery_request_jitter() -> Duration {
    use rand::{rngs::OsRng, Rng};
    Duration::from_millis(OsRng.gen_range(100..=900))
}

impl NostrSignaler {
    pub(super) async fn publish_frame(&self, frame: String) -> mistlib_core::error::Result<()> {
        let mut senders = self.senders.lock().await;
        if senders.is_empty() {
            return Err(mistlib_core::error::MistError::Signaling(
                "NostrSignaler: no relay connection is open".to_string(),
            ));
        }

        let mut delivered = 0_usize;
        let mut alive = Vec::with_capacity(senders.len());
        for tx in senders.drain(..) {
            if tx.is_closed() {
                tracing::warn!("NostrSignaler: dropping closed relay sender");
                continue;
            }
            match tx.send(frame.clone()).await {
                Ok(()) => {
                    delivered += 1;
                    alive.push(tx);
                }
                Err(err) => {
                    tracing::warn!("NostrSignaler: dropping dead relay sender: {}", err);
                }
            }
        }
        *senders = alive;

        if delivered == 0 {
            return Err(mistlib_core::error::MistError::Signaling(
                "NostrSignaler: no relay connection is open".to_string(),
            ));
        }
        STATS.add_send(frame.len() as u64);
        Ok(())
    }

    async fn publish_event(
        &self,
        event: &mistlib_core::signaling::nostr::NostrEvent,
    ) -> mistlib_core::error::Result<()> {
        let frame = event_frame_json(event)?;
        self.publish_frame(frame).await
    }

    pub(super) async fn publish_discovery(&self, room_id: &str) -> mistlib_core::error::Result<()> {
        let identity = self.current_identity().await;
        let joined_at = *self.local_joined_at.lock().await;
        let event = build_discovery_event_with_joined_at(
            &self.codec_config,
            &self.crypto,
            &identity,
            room_id,
            joined_at,
        )?;
        self.publish_event(&event).await
    }

    pub(super) async fn publish_message_to_pubkey(
        &self,
        receiver_pubkey: &str,
        data: &SignalingData,
    ) -> mistlib_core::error::Result<()> {
        let sequence = self.next_outgoing_sequence(receiver_pubkey).await;
        let identity = self.current_identity().await;
        let event = build_message_event_with_sequence(
            &self.codec_config,
            &self.crypto,
            &identity,
            receiver_pubkey,
            data,
            sequence,
        )?;
        self.publish_event(&event).await
    }

    async fn next_outgoing_sequence(&self, receiver_pubkey: &str) -> u64 {
        let mut sequences = self.outgoing_sequences.lock().await;
        next_outgoing_sequence(&mut sequences, receiver_pubkey)
    }

    pub(super) async fn send_request_to_pubkey(
        &self,
        receiver_pubkey: &str,
        room_id: &str,
    ) -> mistlib_core::error::Result<()> {
        #[cfg(not(test))]
        tokio::time::sleep(discovery_request_jitter()).await;

        let request = SignalingData {
            sender_id: self.local_node_id.clone(),
            receiver_id: NodeId::broadcast(),
            room_id: room_id.to_string(),
            data: String::new(),
            signaling_type: SignalingType::Request,
        };
        self.publish_message_to_pubkey(receiver_pubkey, &request)
            .await
    }
}
