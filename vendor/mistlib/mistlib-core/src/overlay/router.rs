use self::dedupe::{OverlaySeenCache, OVERLAY_SEEN_MAX_ENTRIES, OVERLAY_SEEN_TTL};
use crate::config::Config;
use crate::overlay::node_store::NodeStore;
use crate::overlay::routing_table::RoutingTable;
use crate::overlay::TopologyStrategy;
use crate::types::{ConnectionState, NodeId};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

mod dedupe;
mod envelope;
mod send;
mod strategies;

pub use envelope::HandleEnvelopeResult;

pub struct OverlayRouter {
    pub node_store: Arc<Mutex<NodeStore>>,
    pub routing_table: Arc<Mutex<RoutingTable>>,
    pub strategies: Vec<Arc<dyn TopologyStrategy>>,
    pub local_node_id: NodeId,
    pub hop_count: u32,
    seen_envelopes: Mutex<OverlaySeenCache>,
}

impl OverlayRouter {
    pub fn new(config: &Config, node_store: Arc<Mutex<NodeStore>>, local_node_id: NodeId) -> Self {
        let routing_table = Arc::new(Mutex::new(RoutingTable::new()));

        Self {
            node_store,
            routing_table,
            strategies: Vec::new(),
            local_node_id,
            hop_count: config.limits.hop_count,
            seen_envelopes: Mutex::new(OverlaySeenCache::new(
                OVERLAY_SEEN_TTL,
                OVERLAY_SEEN_MAX_ENTRIES,
            )),
        }
    }

    /// Synchronises the routing table's direct connected set with a transport snapshot.
    pub fn sync_connection_states(
        &self,
        connected_node_states: &[(NodeId, ConnectionState)],
    ) -> HashSet<NodeId> {
        let connected = connected_node_states
            .iter()
            .filter(|(_, state)| *state == ConnectionState::Connected)
            .map(|(id, _)| id.clone())
            .collect::<HashSet<_>>();
        self.sync_connected_nodes(&connected);
        connected
    }

    pub fn sync_connected_nodes(&self, connected: &HashSet<NodeId>) {
        let mut rt = self
            .routing_table
            .lock()
            .expect("routing_table lock poisoned");
        let previous = rt.connected_nodes.clone();
        for id in connected {
            rt.on_connected(id.clone());
        }
        for id in previous {
            if !connected.contains(&id) {
                rt.on_disconnected(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> OverlayRouter {
        OverlayRouter::new(
            &Config::new_default(),
            Arc::new(Mutex::new(NodeStore::new())),
            NodeId("local".to_string()),
        )
    }

    #[test]
    fn sync_connection_states_keeps_only_connected_nodes() {
        let router = router();
        let connected = router.sync_connection_states(&[
            (NodeId("connected".to_string()), ConnectionState::Connected),
            (
                NodeId("connecting".to_string()),
                ConnectionState::Connecting,
            ),
            (
                NodeId("disconnected".to_string()),
                ConnectionState::Disconnected,
            ),
        ]);

        assert!(connected.contains(&NodeId("connected".to_string())));
        assert!(!connected.contains(&NodeId("connecting".to_string())));

        let rt = router.routing_table.lock().unwrap();
        assert!(rt
            .connected_nodes
            .contains(&NodeId("connected".to_string())));
        assert!(!rt
            .connected_nodes
            .contains(&NodeId("connecting".to_string())));
    }

    #[test]
    fn sync_connection_states_removes_routes_via_disconnected_nodes() {
        let router = router();
        let relay = NodeId("relay".to_string());
        let target = NodeId("target".to_string());
        router.sync_connection_states(&[(relay.clone(), ConnectionState::Connected)]);
        router
            .routing_table
            .lock()
            .unwrap()
            .add_route(target.clone(), relay.clone());

        router.sync_connection_states(&[]);

        assert_eq!(
            router.routing_table.lock().unwrap().get_next_hop(&target),
            None
        );
    }

    fn envelope(
        from: &str,
        to: NodeId,
        msg_id: u64,
        hop_count: u32,
    ) -> crate::overlay::OverlayEnvelope {
        crate::overlay::OverlayEnvelope {
            from: NodeId(from.to_string()),
            to,
            msg_id,
            hop_count,
            content: crate::signaling::MessageContent::Raw(bytes::Bytes::from_static(b"payload")),
        }
    }

    #[test]
    fn handle_envelope_drops_duplicate_msg_id() {
        let router = router();
        let peer = NodeId("peer-a".to_string());
        let other = NodeId("peer-b".to_string());
        router.sync_connection_states(&[
            (peer.clone(), ConnectionState::Connected),
            (other.clone(), ConnectionState::Connected),
        ]);
        let envelope = envelope("peer-a", NodeId::broadcast(), 42, 2);

        let first = router.handle_envelope(envelope.clone(), peer.clone());
        assert!(first.should_deliver);
        assert_eq!(first.actions.len(), 1);

        let second = router.handle_envelope(envelope, other);
        assert!(!second.should_deliver);
        assert!(second.actions.is_empty());
    }

    #[test]
    fn handle_envelope_zero_msg_id_is_not_deduped() {
        let router = router();
        let peer = NodeId("peer-a".to_string());
        let other = NodeId("peer-b".to_string());
        router.sync_connection_states(&[
            (peer.clone(), ConnectionState::Connected),
            (other.clone(), ConnectionState::Connected),
        ]);
        let envelope = envelope("peer-a", NodeId::broadcast(), 0, 2);

        let first = router.handle_envelope(envelope.clone(), peer.clone());
        let second = router.handle_envelope(envelope, peer);

        assert!(first.should_deliver);
        assert!(second.should_deliver);
        assert_eq!(first.actions.len(), 1);
        assert_eq!(second.actions.len(), 1);
    }

    #[test]
    fn handle_envelope_drops_local_echo() {
        let router = router();
        let envelope = envelope("local", NodeId::broadcast(), 99, 2);

        let result = router.handle_envelope(envelope, NodeId("peer-a".to_string()));

        assert!(!result.should_deliver);
        assert!(result.actions.is_empty());
    }

    #[test]
    fn broadcast_direct_and_relayed_duplicate_delivers_once() {
        let router_b = router();
        let a = NodeId("node-a".to_string());
        let c = NodeId("node-c".to_string());
        router_b.sync_connection_states(&[
            (a.clone(), ConnectionState::Connected),
            (c.clone(), ConnectionState::Connected),
        ]);
        let direct = envelope("node-a", NodeId::broadcast(), 7, 2);
        let mut relayed = direct.clone();
        relayed.hop_count = 1;

        let direct_result = router_b.handle_envelope(direct, a.clone());
        let relayed_result = router_b.handle_envelope(relayed, c);

        assert!(direct_result.should_deliver);
        assert_eq!(
            direct_result.actions.len(),
            1,
            "B should forward direct A broadcast to C"
        );
        assert!(!relayed_result.should_deliver);
        assert!(relayed_result.actions.is_empty());
    }

    #[test]
    fn outgoing_envelopes_use_nonzero_msg_id_and_are_remembered() {
        let router = router();
        let action = router.wrap_data(
            &NodeId::broadcast(),
            bytes::Bytes::from_static(b"payload"),
            crate::types::DeliveryMethod::ReliableOrdered,
        );
        let crate::action::OverlayAction::SendMessage { data, .. } = action else {
            panic!("wrap_data should produce SendMessage");
        };
        let env: crate::overlay::OverlayEnvelope = bincode::deserialize(&data).unwrap();

        assert_ne!(env.msg_id, 0);
        let echo = router.handle_envelope(env, NodeId("peer-a".to_string()));
        assert!(!echo.should_deliver);
        assert!(echo.actions.is_empty());
    }
}
