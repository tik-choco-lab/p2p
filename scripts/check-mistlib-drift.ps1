# Compares the vendored mistlib commit (recorded in vendor/mistlib/VENDORED_FROM)
# against the latest upstream commit for the configured ref, so CI can flag
# when vendor/mistlib has drifted out of date.
#
# Configuration: same as vendor-mistlib.ps1 — the MISTLIB_REPO / MISTLIB_REF
# environment variables take priority, falling back to .env for whichever of
# those is not already set in the environment.
#
# Exit codes: 0 = up to date, 1 = drift detected, 2 = configuration, network,
# or parse error.
$ErrorActionPreference = 'Stop'

function Fail([string]$message) {
    # -ErrorAction Continue: avoid Write-Error becoming a terminating error
    # under $ErrorActionPreference = 'Stop', which would make PowerShell
    # exit with code 1 instead of the explicit exit 2 below.
    Write-Error "check-mistlib-drift: $message" -ErrorAction Continue
    exit 2
}

$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'

$vars = @{}
if (Test-Path $envFile) {
    foreach ($line in Get-Content $envFile) {
        $line = $line.Trim()
        if ($line -eq '' -or $line.StartsWith('#')) { continue }
        $idx = $line.IndexOf('=')
        if ($idx -lt 1) { continue }
        $vars[$line.Substring(0, $idx).Trim()] = $line.Substring($idx + 1).Trim()
    }
}

# Environment variables take priority; .env only fills in what's not already set.
$repo = $env:MISTLIB_REPO
if (-not $repo) { $repo = $vars['MISTLIB_REPO'] }
$ref = $env:MISTLIB_REF
if (-not $ref) { $ref = $vars['MISTLIB_REF'] }
if (-not $repo) { Fail 'MISTLIB_REPO is not set (set the env var or add it to .env)' }
if (-not $ref) { $ref = 'develop' }

$vendoredFrom = Join-Path $root 'vendor/mistlib/VENDORED_FROM'
if (-not (Test-Path $vendoredFrom)) { Fail "vendor/mistlib/VENDORED_FROM not found" }

$localSha = $null
foreach ($line in Get-Content $vendoredFrom) {
    if ($line -match '^commit:\s*([0-9a-fA-F]{40})\s*$') {
        $localSha = $Matches[1]
        break
    }
}
if (-not $localSha) { Fail "could not parse 'commit:' line from vendor/mistlib/VENDORED_FROM" }

if ($ref -match '^[0-9a-fA-F]{40}$') {
    # Full commit hash: it is its own upstream SHA, no network needed.
    $upstreamSha = $ref.ToLower()
} else {
    $lsRemoteOutput = git ls-remote $repo $ref
    if ($LASTEXITCODE -ne 0) { Fail "git ls-remote of $repo @ $ref failed" }

    $branchSha = $null
    $tagSha = $null
    foreach ($outLine in $lsRemoteOutput) {
        $parts = $outLine -split '\t'
        if ($parts.Length -lt 2) { continue }
        $sha = $parts[0].Trim()
        $refName = $parts[1].Trim()
        if ($refName -eq "refs/heads/$ref") { $branchSha = $sha }
        elseif ($refName -eq "refs/tags/$ref") { $tagSha = $sha }
    }
    if ($branchSha) {
        $upstreamSha = $branchSha
    } elseif ($tagSha) {
        $upstreamSha = $tagSha
    } else {
        Fail "ref '$ref' not found as refs/heads/$ref or refs/tags/$ref on $repo"
    }
}

Write-Host "check-mistlib-drift: vendored: $localSha (ref: $ref)"
Write-Host "check-mistlib-drift: upstream: $upstreamSha"

if ($localSha.ToLower() -eq $upstreamSha.ToLower()) {
    Write-Host 'check-mistlib-drift: up-to-date'
    exit 0
} else {
    Write-Host 'check-mistlib-drift: drift detected'
    exit 1
}
