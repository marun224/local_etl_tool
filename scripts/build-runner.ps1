<#
.SYNOPSIS
    Build an etl-runner for another operating system, into tools/runners/.

.DESCRIPTION
    `etl build --target <platform>` produces a cross-OS artifact by copying a
    runner that was already compiled for that platform and appending the
    pipeline to the copy. This script is what produces the runner.

    It does so by building *natively inside a Linux container* rather than by
    cross-compiling on Windows. That is the whole trick, and it is why this
    needs no cross toolchain, no linker configuration and nothing installed
    globally: inside the container the target is the host, so it is an ordinary
    `cargo build`. Docker is the only requirement, and Docker was already here.

    The alternatives were `cross` (a global cargo install that drives this same
    Docker) and `cargo-zigbuild` (two global installs). Both were declined in
    favour of the thing that adds nothing to the machine — the same call
    Settled decision 3 made for DuckDB itself.

    The cargo registry is kept in a named Docker volume, so the second run does
    not re-download every crate. The build tree is kept out of ./target to stop
    a Linux build and a Windows build fighting over the same fingerprints.

    tools/ is git-ignored; this script is how its contents are reproduced.

.EXAMPLE
    ./scripts/build-runner.ps1
    ./scripts/build-runner.ps1 -Platform linux_amd64 -Force
#>
[CmdletBinding()]
param(
    # DuckDB's platform vocabulary, matching `etl build --target` and the
    # extension directory layout.
    [string] $Platform = 'linux_amd64',

    # Must be able to build this workspace: rust-toolchain.toml pins 1.96.0, so
    # an older image would download a toolchain on every run.
    #
    # Pinned to **bookworm** deliberately, and this is the one choice here worth
    # understanding. A Rust binary links against the glibc of the image that
    # built it and will not start on anything older, so the base image sets the
    # oldest Linux the artifact can run on. The default `rust:1.96-slim` is
    # trixie (glibc 2.39), which produced a runner that would not start on
    # Debian 12 -- found by running one, not by reading about it.
    #
    # Bookworm is glibc 2.36, which is also where DuckDB's own published Linux
    # CLI runs. Matching it means the artifact's floor is DuckDB's floor rather
    # than one we imposed on top, and moving this to a newer image silently
    # raises that floor for everybody.
    [string] $Image = 'rust:1.96-slim-bookworm',

    [switch] $Force
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$destination = Join-Path $repoRoot "tools\runners\$Platform"
$binary = Join-Path $destination 'etl-runner'

if ((Test-Path $binary) -and -not $Force) {
    $size = [math]::Round((Get-Item $binary).Length / 1MB, 1)
    Write-Host "Runner for $Platform already present: $binary ($size MB)"
    Write-Host "Pass -Force to rebuild."
    exit 0
}

if ($Platform -ne 'linux_amd64') {
    throw @"
Only linux_amd64 is supported here.

Building inside a container works because the container's own platform is the
target. macOS cannot be built this way -- Apple's SDK is not redistributable and
there is no licensed image to run -- and a second Windows platform would need a
Windows container. Both need a different approach than this script.
"@
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw @"
Docker is not responding.

The CLI is installed but the daemon is not running -- start Docker Desktop and
try again. This script needs it: building a Linux binary is the one part of a
cross-build that cannot be done with what Windows has on its own.
"@
}

# Inside the container the repo is at /w. The build tree goes to a directory of
# its own: sharing ./target with the Windows build means two toolchains
# invalidating each other's fingerprints on every alternating run.
#
# Keyed by image, because a build tree is only reusable by the image that made
# it. Cargo caches compiled *build scripts* and runs them on the next build --
# so a tree left by a trixie image makes a bookworm build die with
# "GLIBC_2.39 not found" while compiling proc-macro2, which is a confusing way
# to say "wrong leftovers". One directory per image, and each keeps its cache.
$imageKey = $Image -replace '[^A-Za-z0-9._-]', '-'
$containerTarget = "/w/target/docker/$imageKey"

Write-Host "Building etl-runner for $Platform in $Image"
Write-Host "(first run pulls the image and downloads crates; later runs reuse both)"

docker run --rm `
    -v "${repoRoot}:/w" `
    -v etl-cargo-registry:/usr/local/cargo/registry `
    -w /w `
    $Image `
    cargo build --release -p etl-runner --target-dir $containerTarget

if ($LASTEXITCODE -ne 0) {
    throw "The container build failed with exit code $LASTEXITCODE"
}

$built = Join-Path $repoRoot "target\docker\$imageKey\release\etl-runner"

if (-not (Test-Path $built)) {
    throw "The build reported success but produced no binary at $built"
}

New-Item -ItemType Directory -Force $destination | Out-Null
Copy-Item $built $binary -Force

$size = [math]::Round((Get-Item $binary).Length / 1MB, 1)
Write-Host ("SHA256: {0}" -f (Get-FileHash -Algorithm SHA256 $binary).Hash)
Write-Host "Installed etl-runner for $Platform at $binary ($size MB)"
Write-Host ""
Write-Host "It still needs a DuckDB for this platform before an artifact can embed one:"
Write-Host "  ./scripts/fetch-duckdb.ps1 -Platform $Platform"
