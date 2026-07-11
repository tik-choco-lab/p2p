set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

default:
    just --list

# Re-vendors mistlib into vendor/mistlib from .env (MISTLIB_REPO / MISTLIB_REF).
# Requires access to the mistlib repository; commit the resulting diff.
[windows]
vendor-mistlib:
    & .\scripts\vendor-mistlib.ps1

[unix]
vendor-mistlib:
    sh ./scripts/vendor-mistlib.sh

# Checks whether the vendored mistlib copy is up to date; exits 1 on drift.
[windows]
vendor-mistlib-check:
    & .\scripts\check-mistlib-drift.ps1

[unix]
vendor-mistlib-check:
    sh ./scripts/check-mistlib-drift.sh

# Detects drift, re-vendors, and auto-commits the result in one shot.
[windows]
vendor-mistlib-update:
    & .\scripts\update-mistlib.ps1

[unix]
vendor-mistlib-update:
    sh ./scripts/update-mistlib.sh

# Best-effort freshness gate: updates vendored mistlib when configured and
# stale; skips when unconfigured, warns and continues when upstream is
# unreachable.
[windows]
ensure-mistlib:
    & .\scripts\ensure-mistlib.ps1

[unix]
ensure-mistlib:
    sh ./scripts/ensure-mistlib.sh

build:
    cargo build

release: ensure-mistlib
    cargo build --release

test:
    cargo test

# Runs the ignored live E2E test against a Nostr relay on localhost.
[windows]
test-nostr port="7777":
    $env:P2P_NOSTR_E2E = '1'; $env:P2P_MISTLIB_CONFIG_JSON = '{"signaling":{"mode":"nostr","nostr":{"relays":["ws://127.0.0.1:{{port}}"],"discoveryKind":25049,"messageKind":25050,"ttlSeconds":60,"inviteSalt":"nostr-sig-test-local-salt","inviteCode":"dev-invite-001"}}}'; cargo test --test nostr_signaling -- --ignored --nocapture

[unix]
test-nostr port="7777":
    P2P_NOSTR_E2E=1 P2P_MISTLIB_CONFIG_JSON='{"signaling":{"mode":"nostr","nostr":{"relays":["ws://127.0.0.1:{{port}}"],"discoveryKind":25049,"messageKind":25050,"ttlSeconds":60,"inviteSalt":"nostr-sig-test-local-salt","inviteCode":"dev-invite-001"}}}' cargo test --test nostr_signaling -- --ignored --nocapture

# Runs only the TCP-forward E2E test against a Nostr relay on localhost.
[windows]
test-forward port="7777":
    $env:P2P_NOSTR_E2E = '1'; $env:P2P_MISTLIB_CONFIG_JSON = '{"signaling":{"mode":"nostr","nostr":{"relays":["ws://127.0.0.1:{{port}}"],"discoveryKind":25049,"messageKind":25050,"ttlSeconds":60,"inviteSalt":"nostr-sig-test-local-salt","inviteCode":"dev-invite-001"}}}'; cargo test --test nostr_signaling tcp_forward -- --ignored --nocapture

[unix]
test-forward port="7777":
    P2P_NOSTR_E2E=1 P2P_MISTLIB_CONFIG_JSON='{"signaling":{"mode":"nostr","nostr":{"relays":["ws://127.0.0.1:{{port}}"],"discoveryKind":25049,"messageKind":25050,"ttlSeconds":60,"inviteSalt":"nostr-sig-test-local-salt","inviteCode":"dev-invite-001"}}}' cargo test --test nostr_signaling tcp_forward -- --ignored --nocapture

# Runs the ignored live E2E test against mistlib's default Nostr signaling config.
[windows]
test-nostr-default:
    $env:P2P_NOSTR_E2E = '1'; Remove-Item Env:\P2P_MISTLIB_CONFIG_JSON -ErrorAction SilentlyContinue; cargo test --test nostr_signaling -- --ignored --nocapture

[unix]
test-nostr-default:
    P2P_NOSTR_E2E=1 cargo test --test nostr_signaling -- --ignored --nocapture
