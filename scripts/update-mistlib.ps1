# One-shot mistlib updater: detect -> re-vendor -> commit.
#
# Delegates drift detection to check-mistlib-drift.ps1 (expected alongside
# this script) and re-vendoring to vendor-mistlib.ps1. check-mistlib-drift.ps1
# is expected to exit 0 (up to date), 1 (drift, upstream moved), or anything
# else (error). Run this instead of the individual scripts when you just want
# mistlib brought up to date and committed without extra steps.
#
# Usage: scripts/update-mistlib.ps1 [-NoCommit]
#   -NoCommit   Re-vendor but leave the diff uncommitted for review.
param(
    [switch]$NoCommit
)
$ErrorActionPreference = 'Stop'

function Fail([string]$message) {
    # -ErrorAction Continue: avoid Write-Error becoming a terminating error
    # under $ErrorActionPreference = 'Stop', which would make PowerShell
    # exit with code 1 instead of the explicit exit 2 below.
    Write-Error "update-mistlib: $message" -ErrorAction Continue
    exit 2
}

$scriptDir = $PSScriptRoot
$root = Split-Path -Parent $scriptDir
$vendorDir = Join-Path $root 'vendor'
$target = Join-Path $vendorDir 'mistlib'
$vendorPathspec = 'vendor/mistlib'

try {
    # Guard: refuse to run if vendor/mistlib already carries uncommitted
    # changes, so the auto-commit below doesn't sweep up unrelated edits.
    $preStatus = git -C $root status --porcelain -- $vendorPathspec
    if ($LASTEXITCODE -ne 0) { Fail 'git status failed' }
    if ($preStatus) { Fail 'vendor/mistlib has uncommitted changes; commit or stash them first' }

    # Detect drift before doing any work.
    $driftScript = Join-Path $scriptDir 'check-mistlib-drift.ps1'
    if (-not (Test-Path $driftScript)) { Fail "check-mistlib-drift.ps1 not found at $driftScript" }
    & $driftScript
    $driftCode = $LASTEXITCODE
    if ($driftCode -eq 0) {
        Write-Host 'update-mistlib: already up to date'
        exit 0
    } elseif ($driftCode -ne 1) {
        Fail "check-mistlib-drift.ps1 failed (exit $driftCode)"
    }

    # Drift detected (exit 1): re-vendor.
    # vendor-mistlib.ps1 signals failure by throwing (it has no exit-code
    # contract), so its errors are caught by the try/catch around this whole
    # script rather than checked via $LASTEXITCODE, which could otherwise be
    # left stale from an unrelated earlier native command (e.g. the drift
    # check above).
    $vendorScript = Join-Path $scriptDir 'vendor-mistlib.ps1'
    if (-not (Test-Path $vendorScript)) { Fail "vendor-mistlib.ps1 not found at $vendorScript" }
    & $vendorScript

    if ($NoCommit) {
        Write-Host 'update-mistlib: vendored; review the diff and commit it.'
        exit 0
    }

    $postStatus = git -C $root status --porcelain -- $vendorPathspec
    if ($LASTEXITCODE -ne 0) { Fail 'git status failed' }
    if (-not $postStatus) {
        Write-Host 'update-mistlib: no changes after vendoring'
        exit 0
    }

    $vendoredFrom = Join-Path $target 'VENDORED_FROM'
    if (-not (Test-Path $vendoredFrom)) { Fail "VENDORED_FROM not found at $vendoredFrom" }
    $sha = $null
    $ref = $null
    foreach ($line in Get-Content $vendoredFrom) {
        if ($line -match '^commit:\s*(.+)$') { $sha = $Matches[1].Trim() }
        elseif ($line -match '^ref:\s*(.+)$') { $ref = $Matches[1].Trim() }
    }
    if (-not $sha -or -not $ref) { Fail "could not parse commit/ref from $vendoredFrom" }
    $shaShort = $sha.Substring(0, [Math]::Min(12, $sha.Length))

    git -C $root add -- $vendorPathspec
    if ($LASTEXITCODE -ne 0) { Fail 'git add failed' }

    git -C $root commit -m "chore: vendor mistlib $ref @ $shaShort"
    if ($LASTEXITCODE -ne 0) { Fail 'git commit failed' }

    Write-Host "update-mistlib: committed $shaShort ($ref)"
    exit 0
} catch {
    Fail $_.Exception.Message
}
