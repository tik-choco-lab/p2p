#!/bin/sh
# Best-effort mistlib freshness gate for build recipes: if MISTLIB_REPO is
# configured and upstream has moved, re-vendor and commit via
# update-mistlib.sh before building. If nothing is configured (public
# checkout without mistlib access) or upstream cannot be reached, build
# with the existing vendored copy instead of failing.
# POSIX-sh counterpart of ensure-mistlib.ps1 for Linux/macOS.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/.." && pwd)

if [ -z "${MISTLIB_REPO:-}" ] && [ ! -f "$root/.env" ]; then
    echo "ensure-mistlib: MISTLIB_REPO not configured; skipping freshness check"
    exit 0
fi

if sh "$script_dir/check-mistlib-drift.sh"; then
    drift_code=0
else
    drift_code=$?
fi
case $drift_code in
    0)
        exit 0
        ;;
    1)
        echo "ensure-mistlib: vendored mistlib is stale; updating"
        sh "$script_dir/update-mistlib.sh"
        ;;
    *)
        echo "ensure-mistlib: drift check failed (exit $drift_code); building with the existing vendored copy" >&2
        exit 0
        ;;
esac
