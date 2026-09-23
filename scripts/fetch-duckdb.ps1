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

    Another platform's CLI can be fetched with -Platform, for Phase 9c's
    cross-builds. Those land under tools/duckdb/targets/<platform>/ rather than
    beside the host's copy, deliberately: the executor finds its engine by
    searching upward for tools/duckdb/, and a Linux binary sitting where it
    looks would be found and then fail to run.

.EXAMPLE
    ./scripts/fetch-duckdb.ps1
    ./scripts/fetch-duckdb.ps1 -Version v1.5.5 -Force
    ./scripts/fetch-duckdb.ps1 -Platform linux_amd64
#>
[CmdletBinding()]
param(
    # Pinned deliberately: extension binaries are tied to the engine version,
    # so this moves only when someone decides it moves.
    [string] $Version = 'v1.5.5',

    # DuckDB's own platform vocabulary: windows_amd64, linux_amd64, osx_arm64.
    # Empty means this machine, which is the ordinary case and lands in the
    # place the executor already looks.
    [string] $Platform = '',

    [switch] $Force
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

# `$IsWindows` and friends are PowerShell Core automatic variables and are
# absent in Windows PowerShell 5.1, where the answer is always Windows. Written
# this way because CI runs this script on Linux, where the old
# `$env:PROCESSOR_ARCHITECTURE` test silently reported windows_amd64 and
# downloaded the wrong engine.
$hostOs = if ($null -eq $IsWindows -or $IsWindows) {
    'windows'
} elseif ($IsLinux) {
    'linux'
} elseif ($IsMacOS) {
    'osx'
} else {
    throw 'Cannot tell what operating system this is; pass -Platform explicitly.'
}

$hostArch = switch -Regex ("$env:PROCESSOR_ARCHITECTURE$(& uname -m 2>$null)") {
    'ARM64|aarch64|arm64' { 'arm64'; break }
    default               { 'amd64' }
}

$hostPlatform = "${hostOs}_${hostArch}"

if (-not $Platform) { $Platform = $hostPlatform }

$isHost = $Platform -eq $hostPlatform
$exeSuffix = if ($Platform -like 'windows*') { '.exe' } else { '' }

# The host's engine keeps the place the executor searches for. Everything else
# is a build input for `etl build --target`, and is kept out of that path.
$destination = if ($isHost) {
    Join-Path $repoRoot 'tools\duckdb'
} else {
    Join-Path $repoRoot "tools\duckdb\targets\$Platform"
}

$binary = Join-Path $destination "duckdb$exeSuffix"

if ((Test-Path $binary) -and -not $Force) {
    if ($isHost) {
        $installed = (& $binary --version) -join ' '
        if ($installed -like "*$Version*") {
            Write-Host "DuckDB $Version already present: $binary"
            exit 0
        }
        Write-Host "Replacing $installed with $Version"
    } else {
        # A Linux binary cannot be asked its version from here, so presence is
        # the only check available. -Force is how it gets replaced.
        Write-Host "DuckDB for $Platform already present: $binary"
        exit 0
    }
}

# DuckDB's release assets spell the platform with a hyphen where the extension
# repository uses an underscore. One character, and the only difference.
$asset = "duckdb_cli-$($Platform -replace '_', '-').zip"
$url = "https://github.com/duckdb/duckdb/releases/download/$Version/$asset"
$temp = Join-Path ([System.IO.Path]::GetTempPath()) $asset

Write-Host "Downloading $url"
Invoke-WebRequest -Uri $url -OutFile $temp

Write-Host ("SHA256: {0}" -f (Get-FileHash -Algorithm SHA256 $temp).Hash)

New-Item -ItemType Directory -Force $destination | Out-Null
Expand-Archive -Path $temp -DestinationPath $destination -Force
Remove-Item $temp

if (-not (Test-Path $binary)) {
    throw "Downloaded $asset but found no duckdb$exeSuffix in $destination"
}

if ($isHost) {
    $reported = (& $binary --version) -join ' '
    if ($reported -notlike "*$Version*") {
        throw "Expected $Version but the downloaded binary reports '$reported'"
    }
    Write-Host "Installed $reported at $binary"
} else {
    # Nothing here can run it, so the version is taken on the strength of the
    # URL it was pinned to rather than asked for and verified.
    $size = [math]::Round((Get-Item $binary).Length / 1MB, 1)
    Write-Host "Installed DuckDB $Version for $Platform at $binary ($size MB, unverified)"
}
