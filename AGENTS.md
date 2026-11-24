# AGENTS.md

## Verify First

- After any work, always run `cargo fmt --all` and `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`.
- CI also runs `cargo test --locked --workspace --all-features`, `cargo build --locked --workspace --all-features`, `cargo llvm-cov --locked --workspace --all-features --codecov --output-path codecov.json`, and the Docker build. Run the smallest relevant subset locally, but match CI flags when you verify.

## Workspace Shape

- This is a Rust workspace, not a single crate. Top-level orchestration lives in `apps/gateway-cli` and `services/router`.
- `apps/gateway-cli` is the shipped CLI/runtime package: Cargo package `tokn-gateway-cli`, binary target `tokn-gateway`.
- The user-facing clap command name is still `tokn-router` in `apps/gateway-cli/src/cli/mod.rs`. Do not assume README command names match the built binary name.
- `services/router` owns the HTTP API/router/proxy wiring. `apps/tokn` is a re-export wrapper crate, not the runtime entrypoint.

## Crate Map

- `apps/gateway-cli`: CLI commands, process startup, logging, serve/proxy runtime.
- `services/router`: axum router, `/v1/*` endpoints, proxy interception, shared request pipeline wiring.
- `services/accounts`: account pool, provider registry, route resolution.
- `core/auth`: auth provider interfaces and credential storage/import.
- `core/catalogue`: provider/model catalogue loading and cache handling.
- `core/config`: config loading, validation, path resolution, TOML edits.
- `endpoints/convert`: cross-endpoint request/response conversion, including SSE translation.
- `services/requests`: composable request pipeline stages and event emission.
- `services/persistence`: SQLite persistence, migrations, archival, schema snapshots.
- `core/core`: shared types, provider contracts, utilities.
- `core/headers`: header parsing/schema/persona utilities.
- `endpoints/*`: typed endpoint schemas and endpoint proc macros; usually touched together when request/response shapes change.
- `providers/*`: upstream-specific provider implementations (`copilot`, `deepseek`, `llama-cpp`, `openai`, `opencode`, `zai`).
- `services/mock-server`: shared test fixture server for provider/integration tests.
- `apps/tokn`: re-export crate for library consumers, not where runtime behavior is implemented.
- `docs/`: supporting documentation, not the executable source of truth.
- `tmp/`: scratch/output area; avoid treating it as checked-in source.

## Repo-Specific Gotchas

- Schema snapshots under `services/persistence/schemas/snapshot/**` must stay aligned with the active release line from `VERSION`. For the `v0.2.x` release line, keep new snapshot updates on the existing `v0.2.0.sql` files.
- Treat files under `services/persistence/schemas/migrations/**` as forward-only after release; do not rewrite old migrations.
- `endpoints/convert/tests/golden_chat_to_responses.rs` is a folder-driven golden test. Refresh expected output with `UPDATE_GOLDEN=1 cargo test -p tokn-convert --test golden_chat_to_responses`.

## Focused Commands

- Full workspace test: `cargo test --locked --workspace --all-features`
- Full workspace build: `cargo build --locked --workspace --all-features`
- Run the CLI from source: `cargo run -p tokn-gateway-cli --bin tokn-gateway -- <args>`
- Build the release artifact used by Docker/release CI: `cargo build --locked --release --package tokn-gateway-cli --bin tokn-gateway`

## Style Signals

- `rustfmt.toml` sets `tab_spaces = 2` and `max_width = 120`; expect 2-space Rust indentation after formatting.
