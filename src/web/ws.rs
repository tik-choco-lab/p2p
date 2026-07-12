//! `GET /api/ws`: pushes `{"type":"state", ...same shape as /api/state}` on
//! connect, then again whenever the broadcast loop (see `web::run`) or a
//! mutating API call detects the snapshot changed. Coalesced to at most one
//! push per detected change via a `watch` channel.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use serde::Serialize;

use super::state::{AppState, StateDto};

#[derive(Serialize)]
struct StateMessage<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(flatten)]
    state: &'a StateDto,
}

pub(crate) async fn handler(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run(socket, state))
}

async fn run(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.state_tx.subscribe();

    let initial = rx.borrow_and_update().clone();
    if send_state(&mut socket, &initial).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    // Sender dropped (server shutting down).
                    break;
                }
                let dto = rx.borrow_and_update().clone();
                if send_state(&mut socket, &dto).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {
                        // Client pings / stray messages: nothing to act on,
                        // axum/tungstenite already answer protocol-level
                        // pings for us.
                    }
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

async fn send_state(socket: &mut WebSocket, dto: &Arc<StateDto>) -> Result<(), axum::Error> {
    let payload = StateMessage {
        kind: "state",
        state: dto.as_ref(),
    };
    let text = serde_json::to_string(&payload).unwrap_or_default();
    socket.send(Message::Text(text.into())).await
}
