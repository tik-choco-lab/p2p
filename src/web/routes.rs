//! HTTP routes: the JSON control API plus the embedded frontend assets.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};

use crate::app::generate_room_id;
use crate::app::session::{parse_addr_port, SessionError};
use crate::forward_args::node_scoped_target;
use crate::negotiation::OutgoingForward;

use super::state::{
    build_state_dto, parse_auth_decision, ActionResult, AddForwardBody, AppState, AuthDecisionBody,
    ChatSendBody, ForwardAcceptBody, RemoveTrustBody, RoomSwitchResult, RoomsResponse,
    SwitchRoomBody,
};
use super::ws;

pub(crate) fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/api/state", get(get_state))
        .route("/api/pending/auth/{id}", post(post_pending_auth))
        .route("/api/pending/forward/{id}", post(post_pending_forward))
        .route("/api/forwards", post(post_forwards))
        .route("/api/forwards/{id}", delete(delete_forward))
        .route("/api/trust", delete(delete_trust))
        .route("/api/rooms", get(get_rooms))
        .route("/api/room", post(post_room))
        .route("/api/chat", post(post_chat))
        .route("/api/ws", get(ws::handler))
        .with_state(state)
}

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("assets/index.html"),
    )
}

async fn app_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("assets/app.js"),
    )
}

async fn style_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("assets/style.css"),
    )
}

async fn get_state(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(build_state_dto(&state.ctx).await)
}

async fn post_pending_auth(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    Json(body): Json<AuthDecisionBody>,
) -> Response {
    let Some(decision) = parse_auth_decision(&body.decision) else {
        return bad_request(format!("unknown decision: {}", body.decision));
    };
    let resolved = state.ctx.resolve_pending_auth(id, decision).await;
    state.broadcast_now().await;
    if resolved {
        success()
    } else {
        not_found(format!("pending auth {} not found", id))
    }
}

async fn post_pending_forward(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
    Json(body): Json<ForwardAcceptBody>,
) -> Response {
    let result = state.ctx.resolve_forward(id, body.accept).await;
    state.broadcast_now().await;
    session_result(result)
}

async fn post_forwards(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddForwardBody>,
) -> Response {
    let proto = body.proto.trim().to_lowercase();
    if crate::controller::Proto::from_name(&proto).is_err() {
        return bad_request(format!("unsupported protocol: {}", proto));
    }
    let local = body.local.trim();
    let remote = body.remote.trim();
    if local.is_empty() || remote.is_empty() {
        return bad_request("local and remote are required".to_string());
    }
    let Some(listen_port) = parse_addr_port(local) else {
        return bad_request("local must be host:port".to_string());
    };

    let peer_id = match body
        .peer_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(p) => p.to_string(),
        None => {
            let peers = state.ctx.manager.connected_peers().await;
            match peers.len() {
                0 => return bad_request("no peers connected".to_string()),
                1 => peers[0].clone(),
                _ => {
                    return bad_request("multiple peers connected; peer_id is required".to_string())
                }
            }
        }
    };

    let target = node_scoped_target(&format!("{}:{}", proto, remote), &peer_id);
    let draft = OutgoingForward {
        peer_id,
        proto,
        listen_port,
        local_addr: local.to_string(),
        remote_addr: remote.to_string(),
        target,
    };

    let result = state.ctx.send_outgoing_forward(draft).await;
    state.broadcast_now().await;
    match result {
        Ok(_) => success(),
        Err(e) => bad_request(e.to_string()),
    }
}

async fn delete_forward(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    let result = state.ctx.remove_forward(&id).await;
    state.broadcast_now().await;
    match result {
        Ok(()) => success(),
        // The controller's only failure mode for remove is "not found";
        // treat any failure here as 404 rather than 400.
        Err(e) => not_found(e.to_string()),
    }
}

async fn delete_trust(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RemoveTrustBody>,
) -> Response {
    let result = state
        .ctx
        .remove_trust(&body.peer_id, &body.forward_key)
        .await;
    state.broadcast_now().await;
    match result {
        Ok(true) => success(),
        Ok(false) => not_found("trust entry not found".to_string()),
        Err(e) => bad_request(e.to_string()),
    }
}

async fn get_rooms(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(RoomsResponse {
        rooms: state.room_store.list().await,
    })
}

async fn post_room(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SwitchRoomBody>,
) -> Response {
    let room_id = match body
        .room_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(r) => r.to_string(),
        None => generate_room_id(),
    };
    state.ctx.switch_room(room_id.clone()).await;
    let _ = state.room_store.record_use(&room_id).await;
    state.broadcast_now().await;
    (
        StatusCode::OK,
        Json(RoomSwitchResult {
            ok: true,
            room_id: Some(room_id),
            error: None,
        }),
    )
        .into_response()
}

async fn post_chat(State(state): State<Arc<AppState>>, Json(body): Json<ChatSendBody>) -> Response {
    let result = state.ctx.send_chat(&body.text).await;
    state.broadcast_now().await;
    match result {
        Ok(()) => success(),
        Err(e) => bad_request(e.to_string()),
    }
}

fn session_result(result: Result<String, SessionError>) -> Response {
    match result {
        Ok(_) => success(),
        Err(SessionError::NotFound(msg)) => not_found(msg),
        Err(SessionError::Invalid(msg)) => bad_request(msg),
    }
}

fn success() -> Response {
    (
        StatusCode::OK,
        Json(ActionResult {
            ok: true,
            error: None,
        }),
    )
        .into_response()
}

fn not_found(error: String) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ActionResult {
            ok: false,
            error: Some(error),
        }),
    )
        .into_response()
}

fn bad_request(error: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ActionResult {
            ok: false,
            error: Some(error),
        }),
    )
        .into_response()
}
