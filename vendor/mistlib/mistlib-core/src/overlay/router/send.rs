use super::OverlayRouter;
use crate::action::OverlayAction;
use crate::overlay::OverlayEnvelope;
use crate::signaling::MessageContent;
use crate::types::{DeliveryMethod, NodeId};
use bytes::Bytes;

impl OverlayRouter {
    fn create_send_action(
        &self,
        node: &NodeId,
        data: Bytes,
        method: DeliveryMethod,
    ) -> OverlayAction {
        let rt = self
            .routing_table
            .lock()
            .expect("routing_table lock poisoned");
        let next_hop = rt.get_next_hop(node).unwrap_or_else(|| node.clone());
        OverlayAction::SendMessage {
            to: next_hop,
            data,
            method,
        }
    }

    pub fn wrap_data(&self, to: &NodeId, data: Bytes, method: DeliveryMethod) -> OverlayAction {
        let envelope = OverlayEnvelope::new(
            self.local_node_id.clone(),
            to.clone(),
            self.hop_count,
            MessageContent::Raw(data),
        );
        self.remember_outgoing(&envelope);
        let enveloped_data =
            bincode::serialize(&envelope).expect("OverlayEnvelope serialization must not fail");
        self.create_send_action(to, Bytes::from(enveloped_data), method)
    }
}
