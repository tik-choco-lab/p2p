//! Web UI backend: an HTTP + WebSocket control API plus an embedded
//! single-page frontend, driving the same session core the TUI uses (see
//! `crate::app::session`).
//!
//! Binds to `127.0.0.1` only (the "localhost trust model"): anything that
//! can reach the loopback interface can drive the whole session, and there
//! is currently no auth token guarding the API. TODO(follow-up): add a
//! bearer token (e.g. generated and printed to stderr at startup, or
//! required as a query param / header) before this is ever exposed beyond
//! localhost — e.g. via a reverse proxy or port forward.

mod routes;
mod state;
mod ws;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::app::session::SessionContext;
use crate::app::{generate_room_id, load_or_create_node_id};

pub(crate) use state::AppState;

/// How often the background loop recomputes the state snapshot and (if it
/// changed) pushes it to WebSocket subscribers, independent of the
/// immediate push each mutating API call also triggers.
const BROADCAST_INTERVAL: Duration = Duration::from_millis(500);

/// Port first attempted when the Web UI is started from inside the TUI
/// (`w` key). Matches the `p2p web` CLI default.
pub(crate) const DEFAULT_PORT: u16 = 8787;

/// Runs `p2p web`: builds the shared session context, spawns the
/// outcome-drain / state-broadcast background loop, and serves the HTTP +
/// WebSocket control API plus the embedded frontend on `127.0.0.1:port`.
/// Shuts down cleanly on Ctrl-C.
pub(crate) async fn run(room_id: Option<&str>, port: u16, open: bool) -> Result<()> {
    let room = match room_id {
        Some(r) => r.to_string(),
        None => {
            let id = generate_room_id();
            eprintln!("Room ID: {}", id);
            id
        }
    };

    let self_id = load_or_create_node_id().await?;
    let ctx = SessionContext::build(self_id, room, true).await?;
    let manager = ctx.manager.clone();

    let initial = state::build_state_dto(&ctx).await;
    let (state_tx, _state_rx) = watch::channel(Arc::new(initial));
    // `_state_rx` is kept alive for the lifetime of `run()` purely so
    // `state_tx.send()` always has at least one receiver; every real
    // subscriber comes from `AppState::state_tx.subscribe()` in the WS
    // handler.
    let app_state = Arc::new(AppState { ctx, state_tx });

    // Background loop: apply requester-side forward outcomes as they
    // arrive (this is the Web UI's equivalent of the TUI's per-tick
    // `refresh()`), then recompute/broadcast state if it changed.
    {
        let app_state = app_state.clone();
        tokio::spawn(async move {
            loop {
                app_state.ctx.drain_and_apply_outcomes().await;
                app_state.broadcast_now().await;
                tokio::time::sleep(BROADCAST_INTERVAL).await;
            }
        });
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let url = format!("http://{}", addr);
    info!("web UI listening on {}", url);
    eprintln!("Web UI: {}", url);

    if open {
        best_effort_open(&url);
    }

    let router = routes::build_router(app_state);
    let server = axum::serve(listener, router).with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    });
    let result = server.await.map_err(anyhow::Error::from);

    manager.close().await;
    result
}

/// Starts the Web UI inside an already-running front end (the TUI's `w`
/// key), sharing its live `SessionContext` so both drive the same session.
/// Binds `127.0.0.1:preferred_port`, falling back to an ephemeral port when
/// taken. Unlike [`run`], this does NOT drain forward outcomes — the host
/// front end already does, and a second drain loop would steal them — so
/// the background loop here only recomputes/broadcasts state snapshots.
/// The server lives until the process exits. Returns the reachable URL.
pub(crate) async fn start_in_process(ctx: SessionContext, preferred_port: u16) -> Result<String> {
    let listener = match tokio::net::TcpListener::bind(("127.0.0.1", preferred_port)).await {
        Ok(l) => l,
        Err(e) => {
            warn!(
                "web UI port {} unavailable ({}); falling back to an ephemeral port",
                preferred_port, e
            );
            tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?
        }
    };
    let url = format!("http://{}", listener.local_addr()?);

    let initial = state::build_state_dto(&ctx).await;
    let (state_tx, _state_rx) = watch::channel(Arc::new(initial));
    let app_state = Arc::new(AppState { ctx, state_tx });

    {
        let app_state = app_state.clone();
        tokio::spawn(async move {
            // Keeps `state_tx.send()` from erroring while no WS client is
            // subscribed; real subscribers come from the WS handler.
            let _state_rx = _state_rx;
            loop {
                app_state.broadcast_now().await;
                tokio::time::sleep(BROADCAST_INTERVAL).await;
            }
        });
    }

    let router = routes::build_router(app_state);
    info!("web UI listening on {}", url);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            warn!("web UI server exited: {}", e);
        }
    });
    Ok(url)
}

/// Best-effort browser launch on the state URL; failures are logged and
/// otherwise ignored (matches the CLI flag's documented behavior). Returns
/// whether the launcher process could be spawned.
pub(crate) fn best_effort_open(url: &str) -> bool {
    let spawned = if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()
    } else if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).spawn()
    } else {
        std::process::Command::new("xdg-open").arg(url).spawn()
    };
    match spawned {
        Ok(_) => true,
        Err(e) => {
            warn!("failed to open browser for {}: {}", url, e);
            false
        }
    }
}

/// Best-effort copy of `text` to the OS clipboard via the platform's CLI
/// tool (`clip` / `pbcopy` / `xclip` or `wl-copy`). Returns whether it
/// succeeded.
pub(crate) fn best_effort_copy_clipboard(text: &str) -> bool {
    let candidates: &[(&str, &[&str])] = if cfg!(target_os = "windows") {
        &[("clip", &[])]
    } else if cfg!(target_os = "macos") {
        &[("pbcopy", &[])]
    } else {
        &[("xclip", &["-selection", "clipboard"]), ("wl-copy", &[])]
    };
    for (cmd, args) in candidates {
        if copy_via(cmd, args, text) {
            return true;
        }
    }
    warn!("no clipboard tool accepted the web UI url");
    false
}

fn copy_via(cmd: &str, args: &[&str], text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let child = Command::new(cmd)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    let Ok(mut child) = child else { return false };
    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.kill();
        return false;
    };
    if stdin.write_all(text.as_bytes()).is_err() {
        let _ = child.kill();
        return false;
    }
    drop(stdin);
    matches!(child.wait(), Ok(status) if status.success())
}
