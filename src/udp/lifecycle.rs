use crate::forward_runtime::ForwardRuntime;
use crate::rtc::RTCManager;

pub(super) fn forward_key(proto: &str, addr: &str) -> String {
    let port = addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(-1);
    format!("{}:{}", proto, port)
}

pub(super) fn spawn_handler_cleanup(
    rtc_manager: RTCManager,
    runtime: ForwardRuntime,
    handler_id: u64,
) {
    tokio::spawn(async move {
        let mut shutdown = runtime.subscribe();
        loop {
            if *shutdown.borrow() {
                break;
            }
            if shutdown.changed().await.is_err() {
                break;
            }
        }
        rtc_manager.remove_tunnel_message_handler(handler_id).await;
    });
}
