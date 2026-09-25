<#
.SYNOPSIS
    Fetch the assistant's local model and llama.cpp's server into tools/.

.DESCRIPTION
    `etl assist` runs a small coding model on this machine through llama.cpp's
    `llama-server` (Settled decision 95). Both are build inputs like DuckDB:
    vendored into tools/ (which git ignores), pinned, and nothing outside the
    project changes.

        tools/llama/llama-server[.exe]   and the libraries beside it
        tools/models/<model>.gguf        about 1 GB

    The model is checked against its pinned SHA256, since a truncated or
    swapped 1 GB file would otherwise fail later and more confusingly.

    Point ETL_LLAMA_SERVER and ETL_ASSIST_MODEL (or `etl assist --llama-server
    / --model`) elsewhere to use other copies; any GGUF chat model will load.

.EXAMPLE
    ./scripts/fetch-model.ps1
    ./scripts/fetch-model.ps1 -Force
#>
[CmdletBinding()]
param(
    # llama.cpp's build number. Pinned so the grammar support `etl assist`
    # was tested against is the one it runs with.
    [string] $LlamaBuild = 'b11173',

    [string] $ModelRepo = 'Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF',
    [string] $ModelRevision = 'f86cb2c1fa58255f8052cc32aeede1b7482d4361',
    [string] $ModelFile = 'qwen2.5-coder-1.5b-instruct-q4_k_m.gguf',
    [string] $ModelSha256 = 'cc324af070c2ecbfd324a30884d2f951a7ff756aba85cb811a6ec436933bb046',

    [switch] $Force
)

$ErrorActionPreference = 'Stop'
# Invoke-WebRequest's progress bar slows a 1 GB download to a crawl in 5.1.
$ProgressPreference = 'SilentlyContinue'

$repoRoot = Split-Path -Parent $PSScriptRoot
$tools = Join-Path $repoRoot 'tools'

# As fetch-duckdb.ps1: `$IsWindows` is absent in Windows PowerShell 5.1.
$onWindows = $null -eq $IsWindows -or $IsWindows
$machine = if ($onWindows) { $env:PROCESSOR_ARCHITECTURE } else { & uname -m }
$arm = $machine -match 'ARM64|aarch64|arm64'

$asset = if ($onWindows) {
    if ($arm) { "llama-$LlamaBuild-bin-win-cpu-arm64.zip" } else { "llama-$LlamaBuild-bin-win-cpu-x64.zip" }
} elseif ($IsMacOS) {
    if ($arm) { "llama-$LlamaBuild-bin-macos-arm64.tar.gz" } else { "llama-$LlamaBuild-bin-macos-x64.tar.gz" }
} elseif ($IsLinux) {
    if ($arm) { "llama-$LlamaBuild-bin-ubuntu-arm64.tar.gz" } else { "llama-$LlamaBuild-bin-ubuntu-x64.tar.gz" }
} else {
    throw 'Cannot tell what operating system this is.'
}

# --- llama-server -----------------------------------------------------------

$serverDir = Join-Path $tools 'llama'
$serverName = if ($onWindows) { 'llama-server.exe' } else { 'llama-server' }
$server = Join-Path $serverDir $serverName
$stamp = Join-Path $serverDir 'BUILD'

$present = (Test-Path $server) -and (Test-Path $stamp) -and ((Get-Content $stamp -Raw).Trim() -eq $LlamaBuild)

if ($present -and -not $Force) {
    Write-Host "llama-server $LlamaBuild already present: $server"
} else {
    $url = "https://github.com/ggml-org/llama.cpp/releases/download/$LlamaBuild/$asset"
    $temp = Join-Path ([System.IO.Path]::GetTempPath()) $asset
    $unpacked = Join-Path ([System.IO.Path]::GetTempPath()) "llama-$LlamaBuild"

    Write-Host "Downloading $url"
    Invoke-WebRequest -Uri $url -OutFile $temp
    Write-Host ("SHA256: {0}" -f (Get-FileHash -Algorithm SHA256 $temp).Hash)

    if (Test-Path $unpacked) { Remove-Item -Recurse -Force $unpacked }
    New-Item -ItemType Directory -Force $unpacked | Out-Null
    if ($asset.EndsWith('.zip')) {
        Expand-Archive -Path $temp -DestinationPath $unpacked -Force
    } else {
        & tar -xzf $temp -C $unpacked
        if ($LASTEXITCODE -ne 0) { throw "tar could not unpack $asset" }
    }

    # The archives have moved their binaries between the root and build/bin
    # over time; the server's own directory is what is kept, libraries and all.
    $found = Get-ChildItem -Path $unpacked -Recurse -Filter $serverName | Select-Object -First 1
    if (-not $found) { throw "Downloaded $asset but found no $serverName in it" }

    if (Test-Path $serverDir) { Remove-Item -Recurse -Force $serverDir }
    New-Item -ItemType Directory -Force $serverDir | Out-Null
    Copy-Item -Path (Join-Path $found.DirectoryName '*') -Destination $serverDir -Recurse -Force
    Set-Content -Path $stamp -Value $LlamaBuild -Encoding ascii

    Remove-Item $temp
    Remove-Item -Recurse -Force $unpacked

    # It reports on stderr, which 5.1 turns into a terminating error under Stop.
    $ErrorActionPreference = 'Continue'
    $reported = (& $server --version 2>&1 | Select-String 'version') -join ' '
    $ErrorActionPreference = 'Stop'
    Write-Host "Installed llama-server at $server ($reported)"
}

# --- the model ---------------------------------------------------------------

$modelDir = Join-Path $tools 'models'
$model = Join-Path $modelDir $ModelFile

function Test-Model {
    (Test-Path $model) -and ((Get-FileHash -Algorithm SHA256 $model).Hash -eq $ModelSha256.ToUpper())
}

if ((Test-Path $model) -and -not $Force) {
    Write-Host "Checking $ModelFile against its pinned SHA256..."
    if (Test-Model) {
        Write-Host "Model already present: $model"
        exit 0
    }
    Write-Host 'It does not match; downloading it again.'
}

New-Item -ItemType Directory -Force $modelDir | Out-Null
$url = "https://huggingface.co/$ModelRepo/resolve/$ModelRevision/$ModelFile"
$partial = "$model.partial"

Write-Host "Downloading $url (about 1 GB)"
Invoke-WebRequest -Uri $url -OutFile $partial
Move-Item -Force $partial $model

if (-not (Test-Model)) {
    $actual = (Get-FileHash -Algorithm SHA256 $model).Hash
    Remove-Item $model
    throw "The model's SHA256 is $actual, not the pinned $ModelSha256"
}

$size = [math]::Round((Get-Item $model).Length / 1GB, 2)
Write-Host "Installed $ModelFile at $model ($size GB, SHA256 verified)"
