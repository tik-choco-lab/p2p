# Best-effort mistlib freshness gate for build recipes: if MISTLIB_REPO is
# configured and upstream has moved, re-vendor and commit via
# update-mistlib.ps1 before building. If nothing is configured (public
# checkout without mistlib access) or upstream cannot be reached, build
# with the existing vendored copy instead of failing.
$ErrorActionPreference = 'Stop'

$scriptDir = $PSScriptRoot
$root = Split-Path -Parent $scriptDir

if (-not $env:MISTLIB_REPO -and -not (Test-Path (Join-Path $root '.env'))) {
    Write-Host 'ensure-mistlib: MISTLIB_REPO not configured; skipping freshness check'
    exit 0
}

& (Join-Path $scriptDir 'check-mistlib-drift.ps1')
$driftCode = $LASTEXITCODE
if ($driftCode -eq 0) {
    exit 0
}
if ($driftCode -eq 1) {
    Write-Host 'ensure-mistlib: vendored mistlib is stale; updating'
    & (Join-Path $scriptDir 'update-mistlib.ps1')
    exit $LASTEXITCODE
}
Write-Host "ensure-mistlib: drift check failed (exit $driftCode); building with the existing vendored copy"
exit 0
