<#
.SYNOPSIS
    Fetch the pinned DuckDB CLI into tools/duckdb/.

.DESCRIPTION
    The engine shells out to the DuckDB CLI rather than linking it, so the
    binary is a build input like any other. It is vendored into tools/ (which
    git ignores) rather than installed system-wide, so the version this
    project runs against is the version it was tested against, and nothing
    outside the project changes.

    Point ETL_DUCKDB_BIN at a different binary to override.

.EXAMPLE
    ./scripts/fetch-duckdb.ps1
    ./scripts/fetch-duckdb.ps1 -Version v1.5.5 -Force
#>
[CmdletBinding()]
param(
    # Pinned deliberately: extension binaries are tied to the engine version,
    # so this moves only when someone decides it moves.
    [string] $Version = 'v1.5.5',
    [switch] $Force
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$destination = Join-Path $repoRoot 'tools\duckdb'
$binary = Join-Path $destination 'duckdb.exe'

if ((Test-Path $binary) -and -not $Force) {
    $installed = (& $binary --version) -join ' '
    if ($installed -like "*$Version*") {
        Write-Host "DuckDB $Version already present: $binary"
        exit 0
    }
    Write-Host "Replacing $installed with $Version"
}

$arch = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'arm64' } else { 'amd64' }
$asset = "duckdb_cli-windows-$arch.zip"
$url = "https://github.com/duckdb/duckdb/releases/download/$Version/$asset"
$temp = Join-Path ([System.IO.Path]::GetTempPath()) $asset

Write-Host "Downloading $url"
Invoke-WebRequest -Uri $url -OutFile $temp

Write-Host ("SHA256: {0}" -f (Get-FileHash -Algorithm SHA256 $temp).Hash)

New-Item -ItemType Directory -Force $destination | Out-Null
Expand-Archive -Path $temp -DestinationPath $destination -Force
Remove-Item $temp

$reported = (& $binary --version) -join ' '
if ($reported -notlike "*$Version*") {
    throw "Expected $Version but the downloaded binary reports '$reported'"
}

Write-Host "Installed $reported at $binary"
