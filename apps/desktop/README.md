# Tokn Desktop

A macOS-first local control panel built with Tauri 2, React and TypeScript.

## Run locally

Install Rust, Xcode command-line tools, Node.js 22+ and pnpm. From this directory:

```sh
pnpm install --frozen-lockfile
pnpm desktop
```

`pnpm desktop` builds the repository's gateway, prepares the Tauri sidecar, then
launches the app. `pnpm bundle` builds a release gateway and an unsigned `.app`
under `src-tauri/target/release/bundle/macos`. Signing/notarization and distribution
are not configured yet. Builds currently target the host's macOS architecture.

The desktop Rust crate has its own workspace and lockfile so headless gateway
builds do not require WebKit/GTK or other desktop libraries.

## What the app manages

- **Overview:** check the first loopback `llm_api` listener (sorted by listener
  ID), start the bundled gateway, stop a gateway owned by this app, and view
  recorded usage for the past 24 hours.
- **Accounts:** search a full-width list grouped by provider; open an account in
  a floating panel with Settings, Credentials, and Usage tabs. Edit labels and shared
  active/fallback/disabled state; filter by provider and activation; remove accounts; check authentication and live
  provider quota separately from recorded local usage. Add accounts in
  a floating modal with device-code login or supported credential imports (paste, environment, file,
  and provider-specific sources). Device login shows progress and can be cancelled.
  Stored credentials are never returned to the frontend; pasted credentials are
  cleared from the form when submitted. Account IDs are optional. When omitted, the app uses the provider username,
  upstream account ID, or provider name, adding a numeric suffix if needed.
  Explicit IDs must be unique.
  Changes preserve auth-store shards and attempt to reload a running gateway;
  failures clearly distinguish saved credentials from unapplied runtime changes.
  Token refreshes are persisted before quota probes, including when quota fails.
  Quota checks are on demand and report unsupported/unavailable separately from zero.
- **Routing:** edit the `defaults`, `profiles`, `routes` and `model_scores`
  sections as TOML. Save validates the complete candidate with the existing configuration
  validator (including legacy fragments) and uses a locked, atomic replacement with a revision check.
  Unrelated sections and their comments are retained. Apply separately calls
  the local admin reload endpoint; reload failure leaves the saved file in
  place and reports that the running configuration has not changed. Runtime
  linking can reject a configuration accepted by the structural compiler.
- **Activity:** the request/session inspector. Browse UTC request days,
  paginate and filter requests, inspect messages and tool definitions, lazily
  load headers and bodies, and browse semantic session trees and usage. The
  existing Lit components run inside the desktop shell with isolated styles;
  native Tauri commands replace the old HTTP API. No inspector listener runs.
  Missing, unavailable and older databases retain their existing error states;
  opening Activity never creates or migrates a database.

Pages load on their first visit and retain their state when switching screens.
Account drafts survive closing the panel, account selection, and filtering; each
unsaved account is marked in the list. Closing with Escape or the backdrop returns
to the list. Dismissal is disabled during account operations; successful removal
returns to the list. Provider quota checks remain on demand. Routing keeps its editor draft until saved or explicitly
reloaded. Use Command/Ctrl + 1–4 to switch between Overview, Activity, Accounts,
and Routing when focus is outside an editor and no dialog is open.

The app uses `~/.tokn/router/config.toml` and the normal auth store. It supports both
legacy and v2 configuration with a fixed loopback API port. It does not migrate
configuration on launch. Legacy fragments are included in validation and
revision checks; the editor lists them because their profiles/scores can override
primary settings. Fragment editing remains in the CLI. Use the CLI to initialize
a workspace first. Config errors appear in the app.

A gateway already running outside the app can be viewed and reloaded but cannot
be stopped by the app. Closing/quitting gracefully stops the child owned by the
app, allowing up to 40 seconds for shutdown before terminating it. Unexpected
app termination uses child kill-on-drop when Rust cleanup runs; force-killing
the desktop process can still leave a child, which will be treated as external
on the next launch. Gateway logs use the configuration's existing logging setup; a bounded 8 KiB
stderr tail is shown when the managed child exits.
The webview has no shell or filesystem plugin permissions.

## Isolated local testing

Set `TOKN_DESKTOP_CONFIG` to an absolute config path before launching to use
an isolated gateway configuration. Use temporary persistence paths and a free
loopback port. Account metadata still comes from the normal auth store; the app
never changes credentials. Merely launching the app does not start a gateway.

```sh
pnpm build
pnpm test
pnpm prepare:gateway
cargo fmt --manifest-path src-tauri/Cargo.toml --all
cargo test --locked --manifest-path src-tauri/Cargo.toml
cargo clippy --locked --manifest-path src-tauri/Cargo.toml --all-targets --all-features -- -D warnings
```

Also run repository formatting and Clippy as described in `AGENTS.md`. Native
checks need permission to bind loopback sockets for process ownership tests.
Manually check start/stop, external ownership, invalid/stale routing edits,
reload failure, request filters, lazy payloads, session trees and empty/unavailable databases before release.

To exercise the real bundled gateway on a temporary listener (no inference calls):

```sh
TOKN_DESKTOP_TEST_GATEWAY="$(pwd)/../../target/debug/tokn-gateway" \
  cargo test --locked --manifest-path src-tauri/Cargo.toml -- --include-ignored
```

`tokn-gateway inspect` and the standalone `tokn-router-inspect` crate have been
removed. Use the desktop **Activity** screen instead. The inspector's existing
frontend checks now run from this package; native tests are still run locally.
UI navigation discards cancelled native query results; a database read already
in progress finishes on a blocking worker rather than interrupting SQLite.

Appearance is available at the bottom of the sidebar: System follows the operating system, while Light and Dark override it. The preference is saved locally and applies to every page, including Activity.
