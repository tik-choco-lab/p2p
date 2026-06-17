# p2p (webrtc-p2p-tunnel-rs)

A high-performance P2P tunnel application written in Rust, utilizing mistlib for secure, NAT-traversing connectivity.

This project is a functional port of the original Go implementation [webrtc-p2p-tunnel](https://github.com/tik-choco-lab/webrtc-p2p-tunnel), rewritten in Rust using the `tokio` runtime.

## Features

- **P2P Connectivity**: Establishes direct peer-to-peer connections through mistlib, allowing connectivity even behind restrictive NATs.
- **TCP/UDP Forwarding**: Tunnel any TCP or UDP traffic through the P2P connection.
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

## Usage

### 1. Chat Mode (Default)
Start p2p in chat mode. If no room ID is provided, one will be generated for you.

```bash
# Start a new room
p2p

# Join an existing room
p2p [room-id]
```

### 2. Serve Mode (Server side)
Publish a local port or a command to a room.

```bash
# Serve a local TCP port (:80) to a room
p2p serve my-room :80

# Serve a local UDP port
p2p serve my-room udp://127.0.0.1:9000

# Execute a command and bridge its stdio to the room
p2p serve my-room -- python3 -m http.server
```

### 3. Connect Mode (Client side)
Connect to a room to access a served port or interact with a remote command via stdio.

```bash
# Join a room and bridge remote stdio to your local terminal
p2p connect my-room

# Join a room and map a local port to the remote served port
p2p connect my-room :8080
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

With `just`:

```powershell
# terminal 1
just nostr-relay

# terminal 2
just test-nostr
```

## Architecture

This Rust implementation follows the same internal logic as the Go version:
- **mistlib**: Provides the P2P transport, room membership, and Nostr signaling.
- **RTC Manager**: Adapts mistlib events and raw payloads to the tunnel, chat, and stdio handlers.
- **Bridges**: Dedicated logic for mapping TCP, UDP, and stdio to mistlib messages.

