use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// An incoming forward proposal from a peer, awaiting a local approve/deny in
/// the TUI pending pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingForward {
    /// Local, monotonically increasing id used for display and resolution.
    pub id: u64,
    /// Protocol-level request id echoed back in the response.
    pub req_id: String,
    pub peer_id: String,
    pub proto: String,
    /// Address the peer wants to reach on this node (ip:port).
    pub remote_addr: String,
    pub target: String,
}

/// State the requester remembers between sending a `ForwardRequest` and
/// receiving the matching `ForwardResponse`.
#[derive(Debug, Clone)]
pub struct OutgoingForward {
    pub peer_id: String,
    pub proto: String,
    pub listen_port: i32,
    pub local_addr: String,
    pub remote_addr: String,
    pub target: String,
}

/// The requester-side outcome of a forward proposal, drained by the TUI loop.
#[derive(Debug, Clone)]
pub struct ForwardOutcome {
    pub outgoing: OutgoingForward,
    pub accepted: bool,
}

/// Coordinates the forward-negotiation handshake between the network handlers
/// (which only push data) and the TUI loop (which performs async side-effects).
#[derive(Debug, Clone, Default)]
pub struct ForwardNegotiator {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    next_id: u64,
    incoming: BTreeMap<u64, IncomingForward>,
    outgoing: BTreeMap<String, OutgoingForward>,
    outcomes: Vec<ForwardOutcome>,
}

impl ForwardNegotiator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an incoming proposal and returns its local id.
    pub async fn record_incoming(
        &self,
        req_id: String,
        peer_id: String,
        proto: String,
        remote_addr: String,
        target: String,
    ) -> u64 {
        let mut inner = self.inner.lock().await;
        inner.next_id += 1;
        let id = inner.next_id;
        inner.incoming.insert(
            id,
            IncomingForward {
                id,
                req_id,
                peer_id,
                proto,
                remote_addr,
                target,
            },
        );
        id
    }

    pub async fn list_incoming(&self) -> Vec<IncomingForward> {
        self.inner.lock().await.incoming.values().cloned().collect()
    }

    /// Removes and returns the incoming proposal with the given local id.
    pub async fn take_incoming(&self, id: u64) -> Option<IncomingForward> {
        self.inner.lock().await.incoming.remove(&id)
    }

    /// Remembers a request the local node just sent, keyed by protocol req id.
    pub async fn record_outgoing(&self, req_id: String, outgoing: OutgoingForward) {
        self.inner.lock().await.outgoing.insert(req_id, outgoing);
    }

    /// Matches a received response to a pending outgoing request and queues the
    /// outcome for the TUI loop. Unknown req ids are ignored.
    pub async fn record_response(&self, req_id: &str, accepted: bool) {
        let mut inner = self.inner.lock().await;
        if let Some(outgoing) = inner.outgoing.remove(req_id) {
            inner.outcomes.push(ForwardOutcome { outgoing, accepted });
        }
    }

    /// Drains queued outcomes for the requester side to act on.
    pub async fn drain_outcomes(&self) -> Vec<ForwardOutcome> {
        std::mem::take(&mut self.inner.lock().await.outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_outgoing() -> OutgoingForward {
        OutgoingForward {
            peer_id: "peer-1".into(),
            proto: "tcp".into(),
            listen_port: 8080,
            local_addr: "127.0.0.1:8080".into(),
            remote_addr: "127.0.0.1:80".into(),
            target: "tcp:127.0.0.1:80".into(),
        }
    }

    #[tokio::test]
    async fn incoming_requests_are_listed_and_taken() {
        let neg = ForwardNegotiator::new();
        let id = neg
            .record_incoming(
                "r1".into(),
                "peer-1".into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "tcp:127.0.0.1:80".into(),
            )
            .await;

        assert_eq!(neg.list_incoming().await.len(), 1);
        let taken = neg.take_incoming(id).await.unwrap();
        assert_eq!(taken.req_id, "r1");
        assert!(neg.list_incoming().await.is_empty());
        assert!(neg.take_incoming(id).await.is_none());
    }

    #[tokio::test]
    async fn responses_match_outgoing_and_produce_outcomes() {
        let neg = ForwardNegotiator::new();
        neg.record_outgoing("r1".into(), sample_outgoing()).await;

        // Unknown req id is ignored.
        neg.record_response("nope", true).await;
        assert!(neg.drain_outcomes().await.is_empty());

        neg.record_response("r1", true).await;
        let outcomes = neg.drain_outcomes().await;
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].accepted);
        assert_eq!(outcomes[0].outgoing.listen_port, 8080);

        // Drained once only; a second response for the same id no longer matches.
        neg.record_response("r1", true).await;
        assert!(neg.drain_outcomes().await.is_empty());
    }
}
