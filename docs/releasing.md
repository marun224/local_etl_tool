# Releasing the desktop app

Added 2026-09-25 (Phase R1; Settled decisions 116–124). Windows x64 only so far.

## What a release is

- **An NSIS installer** made by `tauri build`: `Headrace-<version>-windows-x64-setup.exe`, about
  66 MB. It installs for the current user, without administrator rights.
- **It carries DuckDB 1.5.5 and the seven extensions the components use** (delta, excel, httpfs,
  iceberg, mysql, postgres, sqlite), from `tools/duckdb/`, listed in
  `apps/desktop/tauri.windows.conf.json`. The app finds them in its resources at start
  (`bundled_in` in `apps/desktop/src/main.rs`) and passes them to every run, so nothing else
  needs installing. avro and ducklake are vendored but used by no component, so are left out.
- **It does not carry** `llama-server` or the models (about 1.2 GB): the assistant and the
  local-model `xf.ai.*` components need `scripts/fetch-model.ps1`, which an installed app has
  no checkout to run. A later release can offer them as a separate download.
- **An installed app works in `Documents\Headrace`**: a release build that found its bundled
  DuckDB makes that folder and works there, rather than in the install folder, which the
  uninstaller removes.
- **Unsigned** (decision 119): SmartScreen asks before it runs. The download page and the
  release notes say so, and give the SHA-256 to check first.
- **Published on GitHub Releases in `marun224/headrace-releases`**, a public repository holding
  only installers; the engine's source stays private.

## Cutting one

```powershell
# 1. The version, in apps/desktop/tauri.conf.json ("version"), e.g. 0.1.0-preview.1.
# 2. Build (about 20 minutes from clean; it builds the frontend first):
cd apps/desktop
../../frontend/node_modules/.bin/tauri build
cd ../..
# 3. Package: installer, SHA256SUMS.txt and RELEASE_NOTES.md in target/release-out/v<version>/,
#    and the values for the website printed. Publishes nothing.
./scripts/publish-release.ps1
# 4. Publish: creates the releases repository if needed and a pre-release with the files.
#    Public and immediate.
./scripts/publish-release.ps1 -Publish
```

5. **Then the website** (`ETL_Local_WebApp`): paste the printed values into
   `src/config/release.ts`, set `published: true`, build, and redeploy. In that order: the
   site's link must not go live before the file it points at.

## Checked for 0.1.0-preview.1

- The bundled DuckDB loads all seven bundled extensions, and `etl run
  samples/pipelines/orders_enriched.json` with `ETL_DUCKDB_BIN` and `ETL_DUCKDB_EXTENSIONS`
  pointed at the bundled copies runs its five stages.
- The download page, built with `published: true`, shows the Windows card with version, size,
  SHA-256, the SmartScreen steps and both links, and keeps Linux and macOS "Not yet released".
- **Not checked**: installing it and clicking through the installed app, which writes outside
  the project (assignment A81).
