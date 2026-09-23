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

    Another platform's extensions can be fetched with -Platform, for Phase 9c's
    cross-builds. Those cannot be INSTALLed, because a Windows DuckDB installs
    Windows binaries and nothing here can run a Linux one; they are downloaded
    from DuckDB's extension repository directly and gunzipped into the same
    layout. The consequence is that they cannot be *verified* by loading them,
    which the host's are -- see the note where they are written.

.EXAMPLE
    ./scripts/fetch-duckdb-extensions.ps1
    ./scripts/fetch-duckdb-extensions.ps1 -Force
    ./scripts/fetch-duckdb-extensions.ps1 -Platform linux_amd64
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

    # DuckDB's platform vocabulary: windows_amd64, linux_amd64, osx_arm64.
    # Empty means this machine, which is the only one that can be installed
    # through DuckDB itself and the only one that gets verified.
    [string] $Platform = '',

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
$hostPlatform = (& $binary -noheader -list -c 'PRAGMA platform;') -join ''
$hostPlatform = $hostPlatform.Trim()

if (-not $Platform) { $Platform = $hostPlatform }

$isHost = $Platform -eq $hostPlatform
$installed = Join-Path $destination "$Version\$Platform"

if (-not $isHost) {
    # Downloaded rather than installed. DuckDB will only install binaries for
    # the platform it is itself, so a cross-target's extensions have to come
    # from the repository by hand -- the same files INSTALL would have fetched,
    # from the same place, gzipped.
    Write-Host "DuckDB $Version ($Platform, cross-target)"
    Write-Host "Extension directory: $destination"

    New-Item -ItemType Directory -Force $installed | Out-Null

    foreach ($extension in $Extensions) {
        $candidates = @($extension, "${extension}_scanner")
        $already = @(Get-ChildItem -Path $installed -Filter '*.duckdb_extension' -ErrorAction SilentlyContinue |
            Where-Object { $candidates -contains $_.BaseName })

        if ($already.Count -gt 0 -and -not $Force) {
            Write-Host "  $extension already present"
            continue
        }

        # The repository knows one of the two spellings; try the bare name and
        # fall back to the _scanner form, the same pair resolved everywhere else.
        $fetched = $false

        foreach ($name in $candidates) {
            $url = "http://extensions.duckdb.org/$Version/$Platform/$name.duckdb_extension.gz"
            $temp = Join-Path ([System.IO.Path]::GetTempPath()) "$name.duckdb_extension.gz"

            try {
                Invoke-WebRequest -Uri $url -OutFile $temp -ErrorAction Stop
            } catch {
                continue
            }

            $target = Join-Path $installed "$name.duckdb_extension"

            $source = [System.IO.File]::OpenRead($temp)
            $output = [System.IO.File]::Create($target)
            $gzip = New-Object System.IO.Compression.GZipStream($source, [System.IO.Compression.CompressionMode]::Decompress)
            try {
                $gzip.CopyTo($output)
            } finally {
                $gzip.Dispose(); $output.Dispose(); $source.Dispose()
            }

            Remove-Item $temp
            $size = [math]::Round((Get-Item $target).Length / 1MB, 1)
            Write-Host "  downloaded $name ($size MB)"
            $fetched = $true
            break
        }

        if (-not $fetched) {
            throw "Could not download the '$extension' extension for $Platform from extensions.duckdb.org"
        }
    }

    # No verification step, and it is worth being explicit about why: loading
    # these would need a DuckDB of that platform, and this machine has none. A
    # cross-target's extensions are trusted on the strength of the URL they came
    # from. The artifact that embeds them is what finally proves they work, which
    # is why Phase 9c's acceptance is running one rather than building one.
    Write-Host ""
    Write-Host "Downloaded, not verified: nothing here can load a $Platform extension."
    Write-Host "Build an artifact and run it on that platform to find out."
    exit 0
}

Write-Host "DuckDB $Version ($Platform)"
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
