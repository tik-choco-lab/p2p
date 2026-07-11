#!/bin/sh
# Compares the vendored mistlib commit (recorded in vendor/mistlib/VENDORED_FROM)
# against the latest upstream commit for the configured ref, so CI can flag
# when vendor/mistlib has drifted out of date.
#
# Configuration: same as vendor-mistlib.sh -- the MISTLIB_REPO / MISTLIB_REF
# environment variables take priority, falling back to .env for whichever of
# those is not already set in the environment.
#
# Exit codes: 0 = up to date, 1 = drift detected, 2 = configuration, network,
# or parse error.
# POSIX-sh counterpart of check-mistlib-drift.ps1 for Linux/macOS.
set -eu

fail() {
    echo "check-mistlib-drift: $1" >&2
    exit 2
}

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
env_file="$root/.env"

env_repo=${MISTLIB_REPO:-}
env_ref=${MISTLIB_REF:-}

repo=""
ref=""
if [ -f "$env_file" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        line=$(printf '%s' "$line" | tr -d '\r')
        case $line in
            MISTLIB_REPO=*) repo=${line#MISTLIB_REPO=} ;;
            MISTLIB_REF=*) ref=${line#MISTLIB_REF=} ;;
        esac
    done < "$env_file"
fi

# Environment variables take priority; .env only fills in what's not already set.
[ -n "$env_repo" ] && repo=$env_repo
[ -n "$env_ref" ] && ref=$env_ref

if [ -z "$repo" ]; then
    fail "MISTLIB_REPO is not set (set the env var or add it to .env)"
fi
[ -n "$ref" ] || ref=develop

vendored_from="$root/vendor/mistlib/VENDORED_FROM"
if [ ! -f "$vendored_from" ]; then
    fail "vendor/mistlib/VENDORED_FROM not found"
fi

local_sha=""
while IFS= read -r line || [ -n "$line" ]; do
    line=$(printf '%s' "$line" | tr -d '\r')
    case $line in
        commit:*)
            candidate=$(printf '%s' "$line" | sed -n 's/^commit:[[:space:]]*//p')
            if printf '%s' "$candidate" | grep -Eq '^[0-9a-fA-F]{40}$'; then
                local_sha=$candidate
            fi
            ;;
    esac
done < "$vendored_from"

if [ -z "$local_sha" ]; then
    fail "could not parse 'commit:' line from vendor/mistlib/VENDORED_FROM"
fi

if printf '%s' "$ref" | grep -Eq '^[0-9a-fA-F]{40}$'; then
    # Full commit hash: it is its own upstream SHA, no network needed.
    upstream_sha=$(printf '%s' "$ref" | tr 'A-F' 'a-f')
else
    ls_remote_output=$(git ls-remote "$repo" "$ref") || fail "git ls-remote of $repo @ $ref failed"

    branch_sha=$(printf '%s\n' "$ls_remote_output" | awk -v r="refs/heads/$ref" -F'\t' '$2 == r { print $1; exit }')
    tag_sha=$(printf '%s\n' "$ls_remote_output" | awk -v r="refs/tags/$ref" -F'\t' '$2 == r { print $1; exit }')

    if [ -n "$branch_sha" ]; then
        upstream_sha=$branch_sha
    elif [ -n "$tag_sha" ]; then
        upstream_sha=$tag_sha
    else
        fail "ref '$ref' not found as refs/heads/$ref or refs/tags/$ref on $repo"
    fi
fi

echo "check-mistlib-drift: vendored: $local_sha (ref: $ref)"
echo "check-mistlib-drift: upstream: $upstream_sha"

local_sha_lc=$(printf '%s' "$local_sha" | tr 'A-F' 'a-f')
upstream_sha_lc=$(printf '%s' "$upstream_sha" | tr 'A-F' 'a-f')

if [ "$local_sha_lc" = "$upstream_sha_lc" ]; then
    echo "check-mistlib-drift: up-to-date"
    exit 0
else
    echo "check-mistlib-drift: drift detected"
    exit 1
fi
