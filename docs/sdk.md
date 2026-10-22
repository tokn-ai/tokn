# Embedded SDKs

With their default features, the SDKs execute requests in-process using the
standard configuration, credentials, account pool, routing, conversion, retry,
and provider runtime. They do not require a gateway process.
Rust also supports configured requests with caller-supplied networking in
`no_std + alloc` environments.

## Rust

The `tokn-sdk` crate is the stable façade. It provides:

- default and explicit `config.toml` / `auth.yaml` loading, including
  `config.d` and `auth.d`;
- atomic `reload()` of configuration and credentials;
- default and per-request profile selection;
- a provider-neutral generation builder with friendly text, reasoning, tool
  call, usage, and streaming outputs;
- owned, serializable requests that can be transformed or bound to a client
  later;
- typed clients for Responses, Chat Completions, and Messages;
- a raw JSON request escape hatch;
- buffered typed responses and live byte streams.

The client-bound builder is the shortest path for a one-off request:

```rust
let response = client
  .generate("smart")
  .system("You are a Rust expert.")
  .prompt("Explain this function.")
  .send()
  .await?;

println!("{}", response.text);
```

Generation controls use provider-neutral names and are mapped after the route
selects an upstream endpoint:

```rust
use tokn_sdk::{
  GenerateRequest, ReasoningEffort, ReasoningMode, ReasoningSummary,
};

let openai_request = GenerateRequest::builder("gpt-5")
  .prompt("Solve this step by step.")
  .max_tokens(2048)
  .reasoning_effort(ReasoningEffort::High)
  .reasoning_summary(ReasoningSummary::Auto)
  .build()?;

let llama_request = GenerateRequest::builder("local-llama")
  .prompt("Compare these implementations.")
  .top_p(0.9)
  .top_k(40)
  .max_tokens(2048)
  .build()?;

let claude_request = GenerateRequest::builder("claude-sonnet-4.6")
  .prompt("Plan this migration.")
  .max_tokens(2048)
  .reasoning_mode(ReasoningMode::Adaptive)
  .reasoning_effort(ReasoningEffort::High)
  .build()?;
```

`max_tokens()` is an alias for the neutral `max_output_tokens()` control.
Managed routes serialize that limit as `max_output_tokens` for Responses,
`max_completion_tokens` for OpenAI Chat Completions, and `max_tokens` for
other Chat Completions or Messages routes. Codex account
routes reject an explicit limit because that backend does not preserve it.
The examples assume those selectors route to OpenAI Responses, llama.cpp Chat
Completions, and Copilot's Claude Chat Completions fallback; use selectors from
your own configuration.
`top_p()` remains portable across compatible routes and is validated in the
inclusive range 0 through 1. Responses supports reasoning effort and summary
but not `top_k` or an enabled/adaptive mode. Typed `top_k` is currently
supported on llama.cpp Chat Completions; llama.cpp has no portable reasoning
control. Known non-reasoning models reject typed reasoning locally.

Model discovery exposes `x_tokn_router.capabilities.reasoning_efforts` as an
array of supported strings. `null` means unknown; `[]` means no effort control
is advertised. The same metadata validates typed SDK requests after routing.
Cached upstream capabilities (Copilot, Codex, or Anthropic-compatible model
records) take precedence over the provider-specific models.dev catalogue.
Unknown effort support does not reject a request; a known list rejects values
outside that list, including custom strings. Raw endpoint clients preserve
provider wire controls without this SDK validation.

The bundled catalogue includes effort metadata. The serving gateway refreshes
upstream discovery every five minutes and models.dev daily by default, updating
model IDs and metadata in memory while retaining the last successful data on
refresh failure. `tokn-router update` also refreshes the disk cache for subsequent
processes. Embedded servers can opt into the same lifecycle with
`LiveRuntime::start_model_refresh` and keep its guard until shutdown.

DeepSeek V4 Flash advertises `low`, `high`, and `max`; V4 Pro advertises `high`
and `max`. Typed requests follow those model-specific lists. DeepSeek thinking
also rejects `temperature` or `top_p`, which that backend would ignore. Claude
supports adaptive reasoning on 4.6 and newer models but not a reasoning
summary. Manual Claude reasoning requires `ReasoningMode::Enabled`, an
explicit `max_tokens()` limit, a budget of at least 1024 tokens, and
`budget_tokens < max_tokens`; manual mode is rejected on 4.7 and newer models,
while adaptive mode is rejected on 4.5 and older models. Claude effort levels
use the same discovery metadata. Incompatible sampling values are
rejected while Claude thinking is enabled, as are explicit sampling controls
on Claude generations that do not accept them.

Unsupported explicit controls fail clearly after routing rather than being
silently dropped or reinterpreted. Raw endpoint clients remain the escape
hatch for an exact provider wire shape.

`passthrough` and `switch` profiles preserve the generated Responses payload,
except detected compaction requests sent to Codex always use
`service_tier: "priority"`, overriding any incoming tier. Their outgoing
`x-codex-routing-hint` also uses the outgoing model and `tier=priority`,
preserving unrelated routing directives. These profiles
reject typed `top_k` and reasoning controls that would
require post-route lowering. Use an `exact`, `route`, or `fuzzy` profile for
the provider-neutral control API.

Build a request without a client when it needs to be serialized, transformed,
queued, or reused:

```rust
let request = GenerateRequest::builder("smart")
  .prompt("Explain this function.")
  .temperature(0.2)
  .build()?;

let serialized = serde_json::to_string(&request)?;
let request: GenerateRequest = serde_json::from_str(&serialized)?;
let response = client.send(&request).await?;
```

The same owned request can instead be rebound for fluent execution:

```rust
let response = request.bind(&client).send().await?;
```

The façade deliberately hides router `AppState`, account handles, and request
pipeline stages. Those remain implementation details and can evolve without
breaking SDK consumers. Typed endpoint clients and raw JSON remain available
when an exact endpoint wire shape is required.

### Shared standard and embedded execution

`Client<P>` delegates execution to its platform. Both platforms use the shared
stage driver and managed request interpretation in `tokn-requests`. The default
`StdPlatform` loads configuration and credentials from disk and preserves
reloads, lifecycle subscriptions, and concurrent requests. The SDK does not
depend on the HTTP gateway crate.

Disable default features to use `no_std + alloc`. Compile the same in-memory
`config::v2::RawConfig` with `config::v2::compile_execution`, then create a
platform using `Injected::new(plan, accounts, transport, runtime)` and pass it
to `Client::from_platform`. Accounts use the shared `account::AccountConfig`.
No listener, configuration file, credential file, or gateway process is required.

```rust
use tokn_sdk::{Client, Injected};

let plan = tokn_sdk::config::v2::compile_execution(&raw_config)?;
let platform = Injected::new(plan, accounts, transport, runtime)?;
let client = Client::from_platform(platform);
let response = client.chat_completions().create(&request).await?;
```

Managed request interpretation has one implementation in
`tokn_requests::interpretation::RequestInterpretation`:

- SDK option fields become canonical inbound headers; explicit headers override
  matching fields, then the shared header alias rules apply.
- A JSON `stream` boolean takes precedence over `Accept: text/event-stream`.
- Missing, empty, or non-string models become `unknown` for route resolution.
- Session/project identity, explicit and inferred initiator, request purpose,
  and route hints are normalized before routing and provider conversion.

The injected executor uses the normalized session for account affinity and the
normalized stream/initiator for provider context. Request-purpose classification
is portable metadata; this does not yet port standard Codex compaction priority
or routing-header policy. Standard passthrough keeps its distinct typed metadata
peek, header-only initiator, and original body bytes.

The standard and injected sessions still implement their stages separately.
They reuse provider preparation helpers, JSON conversion, and SSE primitives,
but complete routing, retry, response, and event behavior is not yet unified.
The default standard SDK still loads legacy configuration and uses its older
account pool; gateway v2 and injected execution share `tokn-accounts::PoolState`
and selection helpers, with separate linking and route traversal.
`tokn-policy` owns execution-plan data and `tokn-convert` owns protocol conversion.
Further unification belongs in these existing crates, with platform services for
transport, time, credentials, state access, and event delivery. There is no
separate direct client or engine crate.

Implement `transport::Transport` on the application's HTTP/TLS session:

- `send` starts an exchange and returns its status and headers;
- `read` reads decoded HTTP entity bytes, returning zero at EOF;
- `accept_encoding` advertises the adapter’s supported content decoders and
  defaults to `identity`;
- `recoverable` classifies errors that can be replayed;
- `close_response` aborts/releases the active exchange on completion or drop.

Implement `Runtime` for monotonic time, wall-clock timestamps, asynchronous
sleep, and optional lifecycle observation. Networking, TLS validation, an
allocator, and an async executor are supplied by firmware. Explicit API keys
and access tokens are supplied in memory; desktop login and refresh adapters
remain standard features.

The injected platform supports managed profiles and fixed-provider relays,
including caller-supplied credentials. Origin-based proxy routing remains a
gateway concern. A response stream retains a
borrow of its HTTP session until completion, cancellation, or drop; another
call using that session returns an error while it is busy. Selection state is
borrowed only for short transactions and is not held over network awaits.
Model metadata can be supplied through `Injected::with_models` for family and
capability-based routing. `Limits` bounds buffered responses and SSE records.
The friendly generation builder and gateway relay/proxy adapters currently
remain standard-only.

Shared `tokn_core::types::{Bytes, SmolStr}` are concrete types selected at build
time. Standard builds re-export the original upstream types; embedded builds
use local static/inline/`Rc` storage. The local backend supports single-threaded,
non-atomic targets and is intentionally neither `Send` nor `Sync`.

`crates/sdk/tests/interpretation_parity.rs` compares real standard and injected
clients using controlled upstream responses: stream mode, initiator headers,
session aliases, option precedence, and account affinity. Extraction-stage
comparisons also cover request purpose across all three APIs and missing-model
behavior. These checks establish interpretation parity, not full stage parity.

Verification also includes an isolated default-feature SDK consumer and an isolated
alloc-only consumer that exercises configured requests, injected transports,
and suspended stream reads. The latter is compiled for both targets below:

```sh
cargo test --locked --manifest-path crates/sdk/tests/std/Cargo.toml
cargo test --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml
cargo build --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml --target thumbv6m-none-eabi
cargo build --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml --target thumbv7em-none-eabihf --release
```
