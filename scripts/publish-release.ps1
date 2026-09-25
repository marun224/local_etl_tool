<#
.SYNOPSIS
    Package the Windows installer for a release, and publish it when asked.

.DESCRIPTION
    Installers are published on GitHub Releases in a public repository that holds
    only installers (Settled decision 116); the engine's source stays private.

    Without -Publish this touches nothing outside the repo: it copies the NSIS
    installer `tauri build` made into target/release-out/v<version>/, writes
    SHA256SUMS.txt and RELEASE_NOTES.md beside it, and prints the values the
    website's src/config/release.ts needs.

    With -Publish it also creates the releases repository if it does not exist
    (public, empty but for a README), and a GitHub release v<version>, marked
    as a pre-release, with the installer and SHA256SUMS.txt attached. Publishing
    is public and immediate: only run it when you mean it.

    Build first, from apps/desktop:
        ../../frontend/node_modules/.bin/tauri build

.EXAMPLE
    ./scripts/publish-release.ps1
    ./scripts/publish-release.ps1 -Publish
#>
[CmdletBinding()]
param(
    [string] $Repo = 'marun224/headrace-releases',
    [switch] $Publish
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

$config = Get-Content (Join-Path $repoRoot 'apps\desktop\tauri.conf.json') -Raw | ConvertFrom-Json
$version = $config.version
$product = $config.productName

$bundle = Join-Path $repoRoot 'target\release\bundle\nsis'
$installer = Get-ChildItem -Path $bundle -Filter "*_$($version)_x64-setup.exe" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $installer) {
    throw "No installer for $version in $bundle. Build it first: ../../frontend/node_modules/.bin/tauri build (from apps/desktop)."
}

$out = Join-Path $repoRoot "target\release-out\v$version"
New-Item -ItemType Directory -Force $out | Out-Null
$file = "$product-$version-windows-x64-setup.exe"
$copy = Join-Path $out $file
Copy-Item -Force $installer.FullName $copy

$sha = (Get-FileHash -Algorithm SHA256 $copy).Hash.ToLower()
$sizeMb = [math]::Round((Get-Item $copy).Length / 1MB, 1)
Set-Content -Path (Join-Path $out 'SHA256SUMS.txt') -Value "$sha  $file" -Encoding ascii

$notes = @"
# $product $version (preview)

A preview of the $product desktop app for **Windows x64**. It builds, validates and runs
pipelines on your machine; it is not finished, so expect rough edges.

## Install

1. Download ``$file``.
2. Check it: ``Get-FileHash .\$file -Algorithm SHA256`` must print
   ``$sha`` (also in SHA256SUMS.txt).
3. Run it. **The preview is not code-signed yet**, so Windows SmartScreen will say it does not
   recognise the app: choose *More info*, then *Run anyway*.

It installs for your user only, without administrator rights, and keeps its work (run
history, the pipelines you save there) in ``Documents\$product``. Uninstall it from Windows
Settings, Apps.

## What is in it

- The canvas, validation, plan, preview and run, with 88 components.
- DuckDB 1.5.5 and the extensions the components use (PostgreSQL, MySQL, SQLite, Excel,
  S3/HTTP, Iceberg, Delta), so nothing else needs installing to run a pipeline.

## What is not

- **The AI assistant and the local-model components** (``xf.ai.embed``, and ``xf.ai.prompt``,
  ``classify`` and ``extract`` without a ``base_url``) need llama.cpp and the models (about
  1.2 GB), which this installer does not carry. With a ``base_url``, the three endpoint
  components work.
- Linux, macOS, the command-line runner and the agent quickstart are not released yet.
"@
Set-Content -Path (Join-Path $out 'RELEASE_NOTES.md') -Value $notes -Encoding utf8

Write-Host "Packaged $file ($sizeMb MB) in $out"
Write-Host "SHA-256: $sha"
Write-Host ''
Write-Host 'For the website, src/config/release.ts:'
Write-Host @"
export const WINDOWS: DesktopRelease | null = {
  version: '$version',
  file: '$file',
  sizeMb: $sizeMb,
  sha256: '$sha',
  signed: false,
  published: false, // true once the release is live
};
"@

if (-not $Publish) {
    Write-Host ''
    Write-Host 'Nothing was published. Run again with -Publish to create the release on GitHub.'
    exit 0
}

# --- publishing: public and immediate -----------------------------------------

$ErrorActionPreference = 'Continue'
gh repo view $Repo *> $null
$exists = $LASTEXITCODE -eq 0
$ErrorActionPreference = 'Stop'
if (-not $exists) {
    Write-Host "Creating the public repository $Repo"
    gh repo create $Repo --public --description "$product desktop installers. The source is not public yet." --add-readme
    if ($LASTEXITCODE -ne 0) { throw "gh repo create failed" }
}

Write-Host "Creating release v$version on $Repo"
gh release create "v$version" --repo $Repo --prerelease --title "$product $version (preview)" `
    --notes-file (Join-Path $out 'RELEASE_NOTES.md') $copy (Join-Path $out 'SHA256SUMS.txt')
if ($LASTEXITCODE -ne 0) { throw "gh release create failed" }

Write-Host "Published: https://github.com/$Repo/releases/tag/v$version"
Write-Host 'Now set published: true in the website''s src/config/release.ts and redeploy it.'
