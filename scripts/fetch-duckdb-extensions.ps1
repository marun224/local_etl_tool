<#
.SYNOPSIS
    Fetch the DuckDB extensions this project's components need, into
    tools/duckdb/extensions/.

.DESCRIPTION
    Components declare the extensions they need on their spec
    (`.requires_extension("postgres")`), and a compiled plan emits a LOAD
    prelude for the union of them. LOAD, never INSTALL: a run must not depend
    on reaching the internet, which is this script's whole reason to exist —
    it is the one place that does.

    The files land inside the project rather than in the user's DuckDB home,
    for the same reason the CLI itself is vendored: the version the project
    runs against is the version it was tested against, and nothing outside the
    project changes. It is also Phase 9's air-gapped packaging path, exercised
    from the start rather than discovered at the end.

    DuckDB lays the directory out as
    <extension_directory>/<version>/<platform>/<name>.duckdb_extension, which
    is what `SET extension_directory` expects, so the layout is DuckDB's rather
    than ours. Installing one extension may pull in others it depends on —
    iceberg brings avro, for instance — so expect more files than names below.

    Roughly 250 MB when complete. tools/ is git-ignored; this script is how it
    is reproduced.

.EXAMPLE
    ./scripts/fetch-duckdb-extensions.ps1
    ./scripts/fetch-duckdb-extensions.ps1 -Force
#>
[CmdletBinding()]
param(
    # Must match the CLI fetched by fetch-duckdb.ps1: an extension binary is
    # tied to the engine version that loads it.
    [string] $Version = 'v1.5.5',

    # The names components pass to `.requires_extension(...)`. Add to this list
    # when a new component needs something that is not here yet.
    [string[]] $Extensions = @(
        'excel',
        'httpfs',
        'postgres',
        'mysql',
        'sqlite',
        'iceberg',
        'delta',
        'ducklake'
    ),

    [switch] $Force
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$binary = Join-Path $repoRoot 'tools\duckdb\duckdb.exe'
$destination = Join-Path $repoRoot 'tools\duckdb\extensions'

if (-not (Test-Path $binary)) {
    throw "No DuckDB CLI at $binary. Run ./scripts/fetch-duckdb.ps1 first."
}

$reported = (& $binary --version) -join ' '
if ($reported -notlike "*$Version*") {
    throw "The CLI at $binary reports '$reported', not $Version. Extensions must match the engine that loads them."
}

New-Item -ItemType Directory -Force $destination | Out-Null

# Ask DuckDB for its own platform triple rather than guessing it, so this keeps
# working on arm64 and on whatever comes next.
$platform = (& $binary -noheader -list -c 'PRAGMA platform;') -join ''
$platform = $platform.Trim()
$installed = Join-Path $destination "$Version\$platform"

Write-Host "DuckDB $Version ($platform)"
Write-Host "Extension directory: $destination"

foreach ($extension in $Extensions) {
    # INSTALL is idempotent, but it still goes to the network to check; skip it
    # when the file is already here so a re-run is cheap and works offline.
    $present = @(Get-ChildItem -Path $installed -Filter '*.duckdb_extension' -ErrorAction SilentlyContinue |
        Where-Object { $_.BaseName -eq $extension -or $_.BaseName -eq "${extension}_scanner" })

    if ($present.Count -gt 0 -and -not $Force) {
        Write-Host "  $extension already present"
        continue
    }

    Write-Host "  installing $extension"

    # `SET extension_directory` before INSTALL is what keeps this inside the
    # project; without it DuckDB writes to ~/.duckdb/extensions.
    $sql = "SET extension_directory='$($destination -replace '\\', '/')'; INSTALL $extension;"
    & $binary -c $sql | Out-Null

    if ($LASTEXITCODE -ne 0) {
        throw "Could not install the '$extension' extension."
    }
}

# Loading each one is the check that matters: a file that downloaded but cannot
# load is worse than one that is missing, because the failure moves to run time.
Write-Host "Verifying each extension loads"

foreach ($extension in $Extensions) {
    $sql = "SET extension_directory='$($destination -replace '\\', '/')'; LOAD $extension;"
    & $binary -c $sql | Out-Null

    if ($LASTEXITCODE -ne 0) {
        throw "The '$extension' extension is present but will not load."
    }

    Write-Host "  $extension loads"
}

$files = @(Get-ChildItem -Path $installed -Filter '*.duckdb_extension')
$size = [math]::Round((($files | Measure-Object -Property Length -Sum).Sum / 1MB), 1)

Write-Host ""
Write-Host ("{0} extension file(s), {1} MB, at {2}" -f $files.Count, $size, $installed)
