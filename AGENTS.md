# AGENTS.md

## Verify First

- After any work, always run `cargo fmt --all` and `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`.
- CI also runs `cargo test --locked --workspace --all-features`, `cargo build --locked --workspace --all-features`, `cargo llvm-cov --locked --workspace --all-features --codecov --output-path codecov.json`, and the Docker build. Run the smallest relevant subset locally, but match CI flags when you verify.

## Workspace Shape

- This is a Rust workspace, not a single crate. Top-level orchestration lives in `crates/gateway-cli` and `crates/router`.
- `crates/gateway-cli` is the shipped CLI/runtime package: Cargo package `tokn-gateway-cli`, binary target `tokn-gateway`.
- The user-facing clap command name is still `tokn-router` in `crates/gateway-cli/src/cli/mod.rs`. Do not assume README command names match the built binary name.
- `crates/router` owns the HTTP API/router/proxy wiring. `crates/tokn` is a re-export wrapper crate, not the runtime entrypoint.

## Crate Map

- `crates/gateway-cli`: CLI commands, process startup, logging, serve/proxy runtime.
- `crates/router`: axum router, `/v1/*` endpoints, proxy interception, shared request pipeline wiring.
- `crates/accounts`: account pool, provider registry, route resolution.
- `crates/auth`: auth provider interfaces and credential storage/import.
- `crates/policy`: provider/account policy types shared by config and accounts.
- `crates/access`: access-control checks over persisted data.
- `crates/catalogue`: provider/model catalogue loading and cache handling.
- `crates/config`: config loading, validation, path resolution, TOML edits.
- `crates/legacy-config`: legacy config reading/import for migration paths.
- `crates/convert`: cross-endpoint request/response conversion, including SSE translation.
- `crates/requests`: composable request pipeline stages and event emission.
- `crates/service`: config-free HTTP/Tower service contracts used by the pipeline.
- `crates/sdk`: embedded/serverless facade over router and pipeline crates.
- `crates/agent-migration`: agent binding (Codex/OpenCode) reconcile/import/unlink engine.
- `crates/persistence`: SQLite persistence, migrations, archival, schema snapshots.
- `crates/core`: shared types, provider contracts, utilities.
- `crates/headers`: header parsing/schema/persona utilities.
- `crates/endpoint-*`: typed endpoint schemas and endpoint proc macros; usually touched together when request/response shapes change.
- `crates/provider-*`: upstream-specific provider implementations (`copilot`, `openai`, `deepseek`, `zai`, `llama-cpp`, `opencode`).
- `crates/mock-server`: shared test fixture server for provider/integration tests.
- `crates/tokn`: re-export crate for library consumers, not where runtime behavior is implemented.
- `bindings/python` / `bindings/typescript`: SDK bindings published as `tokn-requests` (PyPI) and `@tokn-ai/requests` (npm).
- `apps/desktop`: Tauri 2 desktop app with its own Cargo workspace under `src-tauri` (not a member of the root workspace).
- `scripts/`: Bun-based Docker agent-test harness and release tooling.
- `docs/`: supporting documentation, not the executable source of truth.
- `tmp/`: scratch/output area; avoid treating it as checked-in source.

## Repo-Specific Gotchas

- Schema snapshots under `crates/persistence/schemas/snapshot/**` must stay aligned with the active release line from `VERSION`: extend the highest existing `vX.Y.Z.sql` snapshot files (currently `v0.2.1.sql`) instead of inventing new snapshot names for an unreleased version.
- Treat files under `crates/persistence/schemas/migrations/**` as forward-only after release; do not rewrite old migrations.
- `crates/convert/tests/golden_chat_to_responses.rs` is a folder-driven golden test. Refresh expected output with `UPDATE_GOLDEN=1 cargo test -p tokn-convert --test golden_chat_to_responses`.

## Focused Commands

- Full workspace test: `cargo test --locked --workspace --all-features`
- Full workspace build: `cargo build --locked --workspace --all-features`
- Run the CLI from source: `cargo run -p tokn-gateway-cli --bin tokn-gateway -- <args>`
- Build the release artifact used by Docker/release CI: `cargo build --locked --release --package tokn-gateway-cli --bin tokn-gateway`

## Style Signals

- `rustfmt.toml` sets `tab_spaces = 2` and `max_width = 120`; expect 2-space Rust indentation after formatting.
