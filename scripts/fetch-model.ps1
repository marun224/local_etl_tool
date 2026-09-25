<#
.SYNOPSIS
    Fetch llama.cpp's server and the local models into tools/.

.DESCRIPTION
    `etl assist` runs a small coding model on this machine through llama.cpp's
    `llama-server` (Settled decision 95). Both are build inputs like DuckDB:
    vendored into tools/ (which git ignores), pinned, and nothing outside the
    project changes.

        tools/llama/llama-server[.exe]   and the libraries beside it
        tools/models/<model>.gguf        about 1 GB, for etl assist
        tools/models/<embedder>.gguf     about 37 MB, for xf.ai.embed

    Each model is checked against its pinned SHA256, since a truncated or
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

    # xf.ai.embed's model (Settled decision 109): llama.cpp's own conversion.
    [string] $EmbedRepo = 'ggml-org/bge-small-en-v1.5-Q8_0-GGUF',
    [string] $EmbedRevision = 'f2068edd9b54f2a369549ccc71f70ed273a2a801',
    [string] $EmbedFile = 'bge-small-en-v1.5-q8_0.gguf',
    [string] $EmbedSha256 = 'f046db1dc724cf4f6f0a0c5917e922823b73eb1d27b8f9a9c2797f7866974804',

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

# --- the models --------------------------------------------------------------

$modelDir = Join-Path $tools 'models'
New-Item -ItemType Directory -Force $modelDir | Out-Null

function Get-Model([string] $Repo, [string] $Revision, [string] $File, [string] $Sha256) {
    $path = Join-Path $modelDir $File
    $verified = { (Test-Path $path) -and ((Get-FileHash -Algorithm SHA256 $path).Hash -eq $Sha256.ToUpper()) }

    if ((Test-Path $path) -and -not $Force) {
        Write-Host "Checking $File against its pinned SHA256..."
        if (& $verified) {
            Write-Host "Model already present: $path"
            return
        }
        Write-Host 'It does not match; downloading it again.'
    }

    $url = "https://huggingface.co/$Repo/resolve/$Revision/$File"
    $partial = "$path.partial"
    Write-Host "Downloading $url"
    Invoke-WebRequest -Uri $url -OutFile $partial
    Move-Item -Force $partial $path

    if (-not (& $verified)) {
        $actual = (Get-FileHash -Algorithm SHA256 $path).Hash
        Remove-Item $path
        throw "$File's SHA256 is $actual, not the pinned $Sha256"
    }

    $size = [math]::Round((Get-Item $path).Length / 1MB, 1)
    Write-Host "Installed $File at $path ($size MB, SHA256 verified)"
}

Get-Model $ModelRepo $ModelRevision $ModelFile $ModelSha256
Get-Model $EmbedRepo $EmbedRevision $EmbedFile $EmbedSha256
