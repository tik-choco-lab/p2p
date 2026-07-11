# p2p (webrtc-p2p-tunnel-rs)

A high-performance P2P tunnel application written in Rust, utilizing mistlib for secure, NAT-traversing connectivity.

This project is a functional port of the original Go implementation [webrtc-p2p-tunnel](https://github.com/tik-choco-lab/webrtc-p2p-tunnel), rewritten in Rust using the `tokio` runtime.

## Features

- **P2P Connectivity**: Establishes direct peer-to-peer connections through mistlib, allowing connectivity even behind restrictive NATs.
- **TCP/UDP Forwarding**: Tunnel any TCP or UDP traffic through the P2P connection.
- **Multiple Forwards**: Publish or connect to several ports at once, multiplexed over a single room via per-forward target keys.
- **Interactive TUI**: A ratatui-based terminal UI (the default `p2p` mode) to add/remove forwards at runtime, approve incoming connections, and watch live metrics.
- **Connection Authorization**: Serve side approves inbound connections (who connects to which target), with a persisted trust store and an audit log.
- **Stdio Bridging**: Bridge remote standard input/output to your local terminal, similar to SSH execution.
- **Multi-Channel Architecture**: Routes tunnel, chat, and stdio payloads over mistlib messages.
- **Embedded Chat**: Simple built-in P2P chat mode for coordination.
- **Signaling**: Uses mistlib's default Nostr signaling configuration.

## Installation

Ensure you have the Rust toolchain installed.

```bash
cargo build --release
```

The binary will be located at `target/release/p2p`.

### mistlib dependency

The `mistlib` (mistlib-native) source tree is **vendored** into `vendor/mistlib/` and referenced as a path dependency, so the project builds without access to the private mistlib repository. The vendored ref and commit are recorded in `vendor/mistlib/VENDORED_FROM`. Tests, examples, and benches are pruned during vendoring, so only the sources needed to build the project are included.

To update the vendored copy (requires access to the mistlib repository): copy `.env.example` to `.env`, set

- `MISTLIB_REPO` — git URL of the mistlib repository
- `MISTLIB_REF` — branch name, or a full 40-char commit hash to pin a revision (defaults to `develop`)

then run `just vendor-mistlib` and commit the resulting diff.

`MISTLIB_REPO` and `MISTLIB_REF` may also be set as environment variables, which take precedence over `.env`.

Two additional helpers automate this process:

- `just vendor-mistlib-check` — detects drift between the vendored copy and upstream; exits 0 if up to date, 1 if upstream has moved on.
- `just vendor-mistlib-update` — a one-shot that detects drift, re-vendors, and auto-commits the result if any is found.

`just release` runs this update automatically before building when `MISTLIB_REPO` is configured; without configuration (or when upstream is unreachable) it builds with the existing vendored copy.

CI runs `.github/workflows/update-mistlib.yml` daily to check for drift and open an update PR automatically; it requires a `MISTLIB_DEV_TOKEN` repository secret (a fine-grained PAT with read access to mistlib-dev).

## Usage

The launch mode is selected by whether a subcommand is given:

| Command | Behavior |
|---------|----------|
| `p2p` (no args) | Starts the **interactive TUI**. Add/remove serve and connect forwards at runtime and approve incoming connections. |
| `p2p serve ...` / `p2p connect ...` | Static, non-interactive mode for scripts/daemons. Forwards are fixed by the arguments; no TUI. |
| `p2p chat [room-id]` | Built-in P2P chat mode. |

If no room ID is provided, one is generated and printed to stderr as `Room ID: <id>`.

### 1. Interactive TUI (Default)

```bash
# Start a new room with a generated ID
p2p

# Open the TUI on an existing room
p2p [room-id]
```

Inside the TUI:

- `a` — add a forward. A small form opens: pick **Proto** (TCP/UDP), type your **Local** `ip:port` (what you listen on) and the peer's **Remote** `ip:port` (the target on their side). There is no serve/connect choice — you always listen locally and reach the peer's remote address.
- `d` — delete the selected forward
- `t` — manage trust entries
- `Tab` — switch focus between the forwards table and the Pending pane
- `Enter` — expand the selected forward to show per-peer connection details
- `y` / `n` — approve / deny the selected pending item (`Y` / `N` to also remember it)
- `q` — quit

When you add a forward, a request is sent to the connected peer (a picker appears if several are connected). The peer sees it in the Pending pane as a `[FWD]` row and approves or denies it. On approval both sides show the established forward, the peer is trusted for that target, and the forward is **saved** — it is re-established automatically on the next launch (`forwards.json`).

The forwards table shows direction, target key, protocol, endpoint, state, active connections, and in/out byte counters.

### 2. Serve Mode (Server side)
Publish one or more local ports (or a command) to a room. Multiple forwards can be given.

```bash
# Serve a single local TCP port (:80) to a room
p2p serve my-room :80

# Publish HTTP and a database at once
p2p serve my-room tcp://127.0.0.1:80 tcp://127.0.0.1:5432

# Mix TCP and UDP
p2p serve my-room :8080 udp://127.0.0.1:9000

# Execute a command and bridge its stdio to the room
p2p serve my-room -- python3 -m http.server
```

Authorization policy flags (serve only):

- `--auto-accept` — accept all inbound connections automatically.
- `--allow-peer <peer-id>` — accept connections from the given peer(s); repeatable.
- (default) — unknown peers are denied. Use the TUI to approve interactively, or pre-populate the trust store.

### 3. Connect Mode (Client side)
Connect to a room to access served ports or interact with a remote command via stdio. Multiple forwards can be given.

```bash
# Join a room and bridge remote stdio to your local terminal
p2p connect my-room

# Map local 8080 to the remote served port 80
p2p connect my-room 8080:80

# Multiple forwards: local 8080->remote 80, local 15432->remote 5432
p2p connect my-room 8080:80 15432:5432
```

Forward notation for `connect` is `[proto://]<listen-port>:<remote-port>`. If `remote-port` is omitted it defaults to `listen-port`. The matching peer is selected automatically from the forward keys it advertises.

### 4. Chat Mode

```bash
p2p chat            # start a new room
p2p chat [room-id]  # join an existing room
```

## Global Options

- `-v`: Enable Info level logging.
- `-vv`: Enable Debug level logging.

## Live Tests

Nostr signaling requires relay access, so the E2E test is ignored by default.

```bash
P2P_NOSTR_E2E=1 cargo test --test nostr_signaling -- --ignored --nocapture
```

```powershell
$env:P2P_NOSTR_E2E = "1"
cargo test --test nostr_signaling -- --ignored --nocapture
```

With `just`, `test-nostr` targets a relay on `127.0.0.1:7777` by default. Start your own relay there, or pass a different port:

```powershell
just test-nostr 7778
```

## Architecture

This Rust implementation follows the same internal logic as the Go version:
- **mistlib**: Provides the P2P transport, room membership, and Nostr signaling. mistlib is a single-room-per-process singleton, so one `p2p` process operates within one room.
- **RTC Manager**: Adapts mistlib events and raw payloads to the tunnel, chat, and stdio handlers, and routes tunnel messages by per-forward target key.
- **Forward Controller**: A UI-independent layer (`add_forward` / `remove_forward` / `list_forwards`) shared by the CLI subcommands, the legacy text shell, and the TUI. Each forward carries a direction (serve/connect), protocol, address/listen port, and target key.
- **Bridges**: Dedicated logic for mapping TCP, UDP, and stdio to mistlib messages.
- **Authorization**: A `ConnectionAuthorizer` checked before serve-side connections are dialed, backed by a persisted trust store and an audit log.

A single room can host both serve and connect forwards simultaneously (a "virtual client/server" model); the direction is chosen per forward rather than per process.

