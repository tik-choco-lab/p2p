use super::*;
use mistlib_core::types::{ConnectionState, NodeId};

#[test]
fn push_pending_candidate_does_not_evict_under_the_cap() {
    let mut list = Vec::new();
    for i in 0..MAX_PENDING_CANDIDATES_PER_NODE {
        let dropped = push_pending_candidate(&mut list, format!("cand-{i}"));
        assert!(!dropped, "must not evict while at or under the cap");
    }
    assert_eq!(list.len(), MAX_PENDING_CANDIDATES_PER_NODE);
}

#[test]
fn push_pending_candidate_evicts_oldest_past_the_cap() {
    let mut list = Vec::new();
    for i in 0..MAX_PENDING_CANDIDATES_PER_NODE {
        push_pending_candidate(&mut list, format!("cand-{i}"));
    }

    let dropped = push_pending_candidate(&mut list, "cand-overflow".to_string());

    assert!(dropped, "pushing past the cap must report an eviction");
    assert_eq!(
        list.len(),
        MAX_PENDING_CANDIDATES_PER_NODE,
        "list must stay bounded at the cap"
    );
    assert_eq!(
        list.first().map(String::as_str),
        Some("cand-1"),
        "the oldest entry (cand-0) must be the one evicted"
    );
    assert_eq!(
        list.last().map(String::as_str),
        Some("cand-overflow"),
        "the newest entry must be kept"
    );
}

#[tokio::test]
async fn handle_candidate_buffers_are_bounded_for_an_active_node() {
    let t = make_transport();
    let node = NodeId("peer".to_string());
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connecting);

    for i in 0..(MAX_PENDING_CANDIDATES_PER_NODE + 10) {
        t.handle_candidate(node.clone(), format!("cand-{i}"))
            .await
            .expect("buffering a candidate for an active node must not error");
    }

    let pending = t.pending_candidates.read().await;
    let list = pending
        .get(&node)
        .expect("node must have buffered candidates");
    assert_eq!(
        list.len(),
        MAX_PENDING_CANDIDATES_PER_NODE,
        "buffered candidates for a single node must stay bounded at the cap"
    );
}
