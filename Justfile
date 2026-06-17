set shell := ["powershell.exe", "-NoLogo", "-Command"]

default:
    just --list

test:
    cargo test

# Runs the ignored live E2E test against a local Nostr relay.
# Start `just nostr-relay` in another terminal first.
test-nostr port="7777":
    $env:P2P_NOSTR_E2E = '1'; $env:P2P_MISTLIB_CONFIG_JSON = '{"signaling":{"mode":"nostr","nostr":{"relays":["ws://127.0.0.1:{{port}}"],"discoveryKind":25049,"messageKind":25050,"ttlSeconds":60,"inviteSalt":"nostr-sig-test-local-salt","inviteCode":"dev-invite-001"}}}'; cargo test --test nostr_signaling -- --ignored --nocapture

# Runs the ignored live E2E test against mistlib's default Nostr signaling config.
test-nostr-default:
    $env:P2P_NOSTR_E2E = '1'; Remove-Item Env:\P2P_MISTLIB_CONFIG_JSON -ErrorAction SilentlyContinue; cargo test --test nostr_signaling -- --ignored --nocapture

nostr-relay host="127.0.0.1" port="7777":
    $env:GOPATH = Join-Path (Get-Location) '.just-go'; $env:GOCACHE = Join-Path (Get-Location) '.just-go-cache'; Set-Location ..\mistlib-dev\tests\go; go run ./cmd/nostr-relay --host {{host}} --port {{port}}

nostr-relay-raw host="127.0.0.1" port="7777":
    $env:GOPATH = Join-Path (Get-Location) '.just-go'; $env:GOCACHE = Join-Path (Get-Location) '.just-go-cache'; Set-Location ..\mistlib-dev\tests\go; go run ./cmd/nostr-relay --host {{host}} --port {{port}} --verbose
