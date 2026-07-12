use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::event::{dispatch_event, run_payload_worker};
use super::super::payload::P2pPayload;
use super::super::state::PeerRole;
use super::{encode, test_manager};

/// Regression test for the ordering bug fixed by routing `EVENT_RAW`/
/// `EVENT_OVERLAY` through a single FIFO worker instead of a per-event
/// `tokio::spawn`.
///
/// mistlib delivers events to the raw handler in strict order from a single
/// dispatch thread (see `mistlib-native/src/engine.rs::spawn_event_dispatch_thread`),
/// but `tokio::spawn`ing a task per event does not preserve execution order
/// across tasks on a multi-thread runtime: a later-spawned task can finish
/// before an earlier one if the earlier one's handler takes longer. That
/// reordering is exactly what corrupted `TunnelMessage` sequencing in
/// production (a later `seq` got processed -- and forwarded to the TCP
/// tunnel -- before an earlier one, so the receiver's gap detection closed
/// the connection).
///
/// This drives events through the same `dispatch_event` + `run_payload_worker`
/// pair that `RTCManagerHandle::new` wires together in production, so it
/// exercises the real dispatch path rather than only `handle_payload`
/// directly. The first event's handler is made deliberately slow (a
/// synchronous sleep, blocking whichever task/thread executes it) so that,
/// under the old "spawn per event" design, later events would very likely
/// finish first; run on a multi-thread runtime so there are free worker
/// threads for a wrongly-spawned task to race ahead on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_of_raw_events_is_processed_in_order() {
    let manager = test_manager("self", PeerRole::Client);
    let order = Arc::new(Mutex::new(Vec::new()));

    {
        let order = order.clone();
        manager
            .on_tunnel_message(move |_peer, data| {
                let seq = String::from_utf8(data).unwrap();
                if seq == "0" {
                    // Deliberately slow: if events are still independently
                    // spawned, later ones have every opportunity to
                    // complete first.
                    std::thread::sleep(Duration::from_millis(75));
                }
                order.lock().unwrap().push(seq);
            })
            .await;
    }

    let weak = Arc::downgrade(&manager.inner);
    let runtime = tokio::runtime::Handle::current();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run_payload_worker(weak.clone(), rx));

    const N: u32 = 10;
    for seq in 0..N {
        let payload = encode(P2pPayload::Tunnel {
            data: seq.to_string().into_bytes(),
        });
        dispatch_event(
            &runtime,
            &weak,
            &tx,
            mistlib::EVENT_RAW,
            "peer-1".to_string(),
            payload,
        );
    }

    // Give the FIFO worker time to drain the queue (well over the 75ms
    // artificial delay plus (N - 1) fast iterations).
    tokio::time::sleep(Duration::from_millis(500)).await;

    let expected: Vec<String> = (0..N).map(|n| n.to_string()).collect();
    assert_eq!(*order.lock().unwrap(), expected);
}
