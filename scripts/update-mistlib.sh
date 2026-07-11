#!/bin/sh
# One-shot mistlib updater: detect -> re-vendor -> commit.
#
# Delegates drift detection to check-mistlib-drift.sh (expected alongside
# this script) and re-vendoring to vendor-mistlib.sh. check-mistlib-drift.sh
# is expected to exit 0 (up to date), 1 (drift, upstream moved), or anything
# else (error). Run this instead of the individual scripts when you just want
# mistlib brought up to date and committed without extra steps.
# POSIX-sh counterpart of update-mistlib.ps1 for Linux/macOS.
#
# Usage: scripts/update-mistlib.sh [--no-commit]
#   --no-commit   Re-vendor but leave the diff uncommitted for review.
set -eu

no_commit=0
for arg in "$@"; do
    case $arg in
        --no-commit) no_commit=1 ;;
        *)
            echo "update-mistlib: unknown argument: $arg" >&2
            exit 2
            ;;
    esac
done

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/.." && pwd)
vendor_pathspec="vendor/mistlib"
target="$root/vendor/mistlib"

# Guard: refuse to run if vendor/mistlib already carries uncommitted changes,
# so the auto-commit below doesn't sweep up unrelated edits.
pre_status=$(git -C "$root" status --porcelain -- "$vendor_pathspec")
if [ -n "$pre_status" ]; then
    echo "update-mistlib: vendor/mistlib has uncommitted changes; commit or stash them first" >&2
    exit 2
fi

# Detect drift before doing any work.
drift_script="$script_dir/check-mistlib-drift.sh"
if [ ! -f "$drift_script" ]; then
    echo "update-mistlib: check-mistlib-drift.sh not found at $drift_script" >&2
    exit 2
fi
if sh "$drift_script"; then
    drift_code=0
else
    drift_code=$?
fi
if [ "$drift_code" -eq 0 ]; then
    echo "update-mistlib: already up to date"
    exit 0
elif [ "$drift_code" -ne 1 ]; then
    echo "update-mistlib: check-mistlib-drift.sh failed (exit $drift_code)" >&2
    exit 2
fi

# Drift detected (exit 1): re-vendor.
vendor_script="$script_dir/vendor-mistlib.sh"
if [ ! -f "$vendor_script" ]; then
    echo "update-mistlib: vendor-mistlib.sh not found at $vendor_script" >&2
    exit 2
fi
if ! sh "$vendor_script"; then
    echo "update-mistlib: vendor-mistlib.sh failed" >&2
    exit 2
fi

if [ "$no_commit" -eq 1 ]; then
    echo "update-mistlib: vendored; review the diff and commit it."
    exit 0
fi

post_status=$(git -C "$root" status --porcelain -- "$vendor_pathspec")
if [ -z "$post_status" ]; then
    echo "update-mistlib: no changes after vendoring"
    exit 0
fi

vendored_from="$target/VENDORED_FROM"
if [ ! -f "$vendored_from" ]; then
    echo "update-mistlib: VENDORED_FROM not found at $vendored_from" >&2
    exit 2
fi

sha=""
ref=""
while IFS= read -r line || [ -n "$line" ]; do
    line=$(printf '%s' "$line" | tr -d '\r')
    case $line in
        commit:*) sha=$(printf '%s' "${line#commit:}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//') ;;
        ref:*) ref=$(printf '%s' "${line#ref:}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//') ;;
    esac
done < "$vendored_from"

if [ -z "$sha" ] || [ -z "$ref" ]; then
    echo "update-mistlib: could not parse commit/ref from $vendored_from" >&2
    exit 2
fi
sha_short=$(printf '%s' "$sha" | cut -c1-12)

git -C "$root" add -- "$vendor_pathspec"
git -C "$root" commit -m "chore: vendor mistlib $ref @ $sha_short"

echo "update-mistlib: committed $sha_short ($ref)"
