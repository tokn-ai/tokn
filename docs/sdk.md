# Embedded SDKs

With their default features, the SDKs execute requests in-process using the
v2 configuration, credentials, account pool, routing, conversion, retry,
and provider runtime. They do not require a gateway process.
Rust also supports configured requests with caller-supplied networking in
`no_std + alloc` environments.

## Rust

The `tokn-sdk` crate is the stable façade. It provides:

- default and explicit legacy or v2 `config.toml` loading, with `auth.yaml`
  and `auth.d` credential fragments;
- atomic `reload()` of configuration and credentials;
- default and per-request profile selection;
- a provider-neutral generation builder with friendly text, reasoning, tool
  call, usage, and streaming outputs;
- owned, serializable requests that can be transformed or bound to a client
  later;
- typed clients for Responses, Chat Completions, and Messages;
- a raw JSON request escape hatch;
- buffered typed responses and live byte streams.

The standard client accepts native v2 documents with `schema_version = 2`
and legacy documents with no schema marker. Legacy configuration and its sorted
`config.d` overlays are projected into v2 in memory, then enter the same compiler
and request runtime as native v2 input.
A missing config file, malformed schema marker, or unsupported schema version
is an error. A minimal native v2 config needs no listener:

```toml
schema_version = 2

[defaults]
provider = { kind = "fixed", provider = "openai" }
model = { kind = "qualified", namespace = "provider" }

[providers.openai]
driver = "openai"
base_url = "https://api.openai.com"
```

With an OpenAI account in `auth.yaml`, select a model such as `openai/gpt-5`.
`Client::builder()` selects profile `default`, created by `[defaults]` or an
explicit `[profiles.default]`. Use `.profile("name")` for another default;
`RequestOptions.profile` overrides it for an individual request. Unknown
profiles fail explicitly, including a missing `default` profile at build time.

The SDK accepts gateway v2 documents too, but only builds their request runtime.
It does not start listeners, persistence, or proxy certificate management.
Managed routes and fixed-provider relays are available; original-destination
proxy routes require gateway context. `service.outbound` configures SDK HTTP
clients. A failed `reload()` preserves the previous configuration and credentials;
successful reloads keep event subscriptions valid and let in-flight requests
finish with their original snapshot.

Legacy loading does not migrate or rewrite configuration or credentials. It
loads accounts from `auth.yaml` and `auth.d`, then adds inline legacy accounts
whose IDs are absent from that store. Store accounts take precedence when an
inline account has the same ID. Account-level destinations are promoted to v2
provider destinations; conflicting destinations within a provider are rejected.
Native v2 input uses `[providers.<id>].base_url`, while account files supply
credentials and do not override that destination.

Projection follows v2 routing and selection semantics. Unsupported legacy
recipes fail during loading. Inspect `client.config_warnings()` for behavior
changes, promoted account destinations, and renamed profile resources. Builder
and request profile options accept original legacy names. Generated resource
IDs are also accepted unless an original name shadows them; original names
take precedence in a projected legacy snapshot. Legacy API/proxy listener
settings are outside SDK execution; the client neither compiles nor starts those
listeners. A successful reload
replaces warnings with the new snapshot, including an empty list for native
v2 input; a failed reload preserves the previous warnings and runtime.

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

V2 relay profiles preserve the generated Responses payload,
except detected compaction requests sent to Codex always use
`service_tier: "priority"`, overriding any incoming tier. Their outgoing
`x-codex-routing-hint` also uses the outgoing model and `tier=priority`,
preserving unrelated routing directives. These profiles
reject typed `top_k` and reasoning controls that would
require post-route lowering. Use a managed route for
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

`Client<P>` delegates configuration and service ownership to its platform. Both
SDK platforms execute through one `tokn-requests` session, `Session<T, C, E>`:
transport exchange `T`, clock/timer `C`, and event sink `E` supply the platform
services. Interpretation, profile and route selection, provider preparation,
credential refresh, retry decisions, and response processing belong to the
shared session. The SDK does not depend on the HTTP gateway crate.

The default `StdPlatform` loads configuration and credentials from disk and
preserves reloads, lifecycle subscriptions, and concurrent requests. Each std
request owns an independent exchange backed by the shared HTTP connection pool;
keeping one streaming response alive does not block another request. Injected
execution borrows an application's exchange until its response completes or is
cancelled. These ownership adapters do not choose routing or provider policy.

Disable default features to use `no_std + alloc`. Compile the same in-memory
`config::v2::RawConfig` with `config::v2::compile_execution`, then create a
platform using `Injected::new(plan, accounts, transport, runtime)` and pass it
to `Client::from_platform`. Accounts use the shared `account::AccountConfig`.
No listener, configuration file, credential file, or gateway process is required.
The implicit profile is always `default`; a configuration with only named
profiles requires `RequestOptions::with_profile`. The injected client checks
this when executing a request, since an explicit profile can be supplied there.

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

The session uses the normalized session identity for account affinity and the
normalized stream/initiator for provider context. Codex compaction priority and
routing-header policy apply through the same provider preparation on both SDK
platforms. Gateway original-destination proxy admission remains separate from
SDK execution.

The standard SDK, v2 gateway, and injected executor share provider/account linking and route
selection in `tokn-accounts`:

- `link::portable` validates provider destinations, account identities, and pool
  membership from the same `ExecutionRules`. Disabled accounts remain valid pool
  references but cannot be selected; provider URLs are owned by v2 configuration.
- `selection::select_route` handles provider/driver qualification, model families,
  capability evidence, operation fallback, provider scores, and access filtering.
  Both adapters pass only the selected pool's bindings to this algorithm.
- `PoolState` owns round-robin, cooldown, fallback tiers, and session affinity.
  Standard execution locks its pool for each state transaction; injected execution
  borrows local state and uses the supplied monotonic clock.

Native provider transformations and credential policy are shared. Model
discovery and cache refresh happen outside request execution; routing parity
depends on matching execution rules, accounts, capability inventories, and pool state.
The injected platform accepts in-memory v2 configuration; legacy filesystem
loading and projection belong to `StdPlatform`. Standard SDK and gateway API
profiles are built by `tokn_requests::v2` from the same `ExecutionRules`;
gateway admission and original-destination proxy wiring remain in `tokn-router`.

Retry configuration and replay decisions share `tokn_requests::retry::RetryPolicy`.
The standard SDK, v2 gateway, and injected executor resolve named policies through `from_route`;
standard pipelines keep the existing `RetryPolicy::new` and `safe_methods` APIs.
The shared session supplies error facts and the transport method to `next_delay`:

- Only recoverable send failures can replay; deliberate stops and later response
  or stream failures do not replay a request.
- The retry limit counts additional attempts. Backoff doubles from the configured
  initial delay; a zero delay skips the timer.
- Safe-method policies allow only GET, HEAD, OPTIONS, and TRACE. Injected LLM API
  calls use POST, so account relays require an explicit buffered policy to replay.
- The shared driver resolves an account again for every retry. Account cooldown
  still applies to recoverable send failures even when retries are disabled.

Timers and concrete transport-error classification remain platform services.
HTTP status handling and account feedback share `tokn_requests::send_policy`.
Response handling and pool feedback are separate decisions:

| Request path | Received status handling | Account feedback |
| --- | --- | --- |
| Managed or account-credential API | Below 400 proceeds; 4xx fails permanently; 500 and above fails recoverably | Successful send records success; recoverable failure starts cooldown; permanent failure leaves state unchanged |
| Client-credential v2 relay | Every status is forwarded | No account feedback |
| Account-credential v2 forward proxy | Every status is forwarded | 401, 403, 408, 425, 429, and 500–599 start cooldown; other received statuses record success |
| Legacy standard proxy | Below 500 is forwarded; 500 and above fails recoverably | No v2 account feedback |

Forwarding an HTTP error does not itself trigger replay. A recoverable send
error cools the selected v2 account even if retries are disabled or exhausted.
Successful send feedback commits session affinity before response decoding or
stream consumption; a later body failure does not replay or cool that account.
Shared execution state uses brief synchronized transactions under std and local
interior mutability under `no_std`. No state guard spans transport I/O.

Managed buffered responses share `tokn_convert::buffered::convert_response`:

- SSE is detected from `Content-Type` or a body starting with `event:` or `data:`
  after whitespace. Accumulated SSE renders directly to the requested endpoint.
- JSON responses keep their original bytes when the upstream and requested
  endpoints match. Cross-endpoint responses use the shared converters.
- Empty non-SSE bodies remain empty. Typed SDK decoding can still reject an empty body
  when the requested response type requires JSON.
- Invalid JSON, incomplete SSE, and conversion errors fail the response stage
  without retrying the request or changing account feedback.

Injected buffered execution now follows these standard behaviors, including
content-based SSE detection when an upstream omits or mislabels its MIME type.
Its decoded input, converted output, and individual SSE records remain bounded
by `Limits`. Opaque buffered relays preserve their bytes without normalization.
Opaque relays follow the upstream content type: `text/event-stream` produces a
live byte stream even when the request's `stream` field is false.
Accumulated SSE receives `application/json`; other buffered responses retain
their upstream content type in the converter and injected adapter. The HTTP
gateway sets its managed response content type when constructing the HTTP reply.
Live injected streams still require `text/event-stream`.

Managed live streams share `tokn_convert::sse::StreamProcessor` for terminal
validation, transformation, queued output, and completion:

- The upstream protocol determines completion: Chat requires `[DONE]`, Responses
  requires `response.completed`, and Messages requires `message_stop`. A finish
  reason alone does not make an EOF successful.
- A valid terminal stops upstream reads without waiting for transport EOF.
  Success is reported only after all converted events have been delivered.
- Upstream failure events, malformed translated payloads, framing errors, and
  transport errors fail once. Dropping or cancelling a response with output still
  pending reports cancellation instead of success. Neither case retries the request.
- Dropping an injected `next_event` future leaves its response resumable; dropping
  the response itself releases the exchange and cancels it.

The standard recorder consumes the processor's outcome for managed streams,
instead of reparsing upstream bytes as the downstream protocol. Standard opaque
relays still forward original bytes and retain best-effort protocol telemetry.
Generic `SsePipeline::from_stream` retains transport-EOF completion unless a
known upstream endpoint is supplied with `with_endpoint`.

The std and injected SDK paths use the same session implementation and share
provider preparation, buffered response normalization, stream processing, and
pool feedback. Platform services supply transport I/O, time, and event delivery;
the response owns the exchange and completion state until delivery, failure, or
cancellation. Gateway original-destination and legacy compatibility wiring
remain outside this SDK session contract.
`tokn-policy` owns execution rules and `tokn-convert` owns protocol conversion.
The implementation stays in the existing crates; there is no separate direct
client or engine crate.

Implement `transport::Transport` on the application's HTTP/TLS session:

- `send` starts an exchange and returns its status and headers;
- `read` reads HTTP entity bytes, returning zero at EOF; managed responses are
  decoded, while an opaque exchange may preserve encoding and report that through
  `response_is_decoded`;
- `accept_encoding` advertises the adapter’s supported content decoders and
  defaults to `identity`;
- `recoverable` classifies errors that can be replayed;
- `close_response` aborts/releases the active exchange on completion or drop.

Implement `Runtime` for monotonic time, wall-clock timestamps, asynchronous
sleep, and optional event delivery. Networking, TLS validation, an
allocator, and an async executor are supplied by firmware. Explicit API keys
and access tokens are supplied in memory. Copilot accounts with a refresh
credential exchange it through the supplied transport when their cached token
needs renewal; the shared state reuses the replacement token. Interactive login,
credential-file loading, and persistence remain outside the session.

Request events use the same `RequestEvent`, `StageEvent`, `RecordEvent`, and
`usage::Usage` types in both builds. Attach a `RequestEventSink` or a local
`FnMut(RequestEvent)` callback to the injected platform:

```rust
use tokn_sdk::{Injected, RequestEvent, RequestEventPayload, RecordEvent};

let platform = Injected::new(plan, accounts, transport, runtime)?
  .with_event_sink(|event: RequestEvent| {
    if let RequestEventPayload::Record(RecordEvent::Usage(usage)) = event.payload {
      // Store or forward this observation using application-owned services.
      observe_usage(event.request_id, event.attempt, usage);
    }
  });
```

The sink may borrow local state and does not require `Send` or `'static`.
Delivery is synchronous; callbacks should return promptly. Re-entering the
same client from a callback returns a busy error. Applications can instead
expose a sink through `Runtime::event_sink`; `Runtime::observe` remains available
for existing lifecycle observers and is called alongside the typed sink.
`Runtime::unix_millis` supplies event timestamps, defaulting to `unix_seconds`
converted to milliseconds when the application does not override it.

The injected adapter emits stage summaries, errors, completion, and usage.
Each attempt has its own Started/Completed pair; `attempt` is zero-based and
Completed's `attempts` is one-based. Stream completion follows the shared
terminal rules above. Cancellation and response failures emit Error followed
by Completed with `success: false`. Summary payloads and usage observations
are constructed only when a typed sink is installed. Installing a sink opts
into body/header copies and upstream/converted body records. Each live-stream
recording is capped at `Limits.max_response_bytes`; reaching that recording cap
does not stop delivery, and the corresponding record reports truncation.
Injected ingress summaries serialize the supplied JSON value because this
adapter has no original HTTP wire body or agent-detection context.

Usage observations preserve the existing parser's zero, partial, and repeated
figures; they are snapshots, not additive token deltas. Managed buffered JSON
is observed before conversion, buffered SSE after rendering, and live SSE
before translation. Opaque buffered responses use a best-effort JSON parse.
Both adapters receive the same transport/body capture records. The standard
event bus retains Tokio delivery and persistence subscribers. No database or file I/O is introduced into the alloc-only path.

The injected platform supports managed profiles and fixed-provider relays,
including caller-supplied credentials. Origin-based proxy routing remains a
gateway concern. A response stream retains a
borrow of its HTTP session until completion, cancellation, or drop; another
call using that session returns an error while it is busy. Selection state is
borrowed only for short transactions and is not held over network awaits.
Model identifiers can be supplied through `Injected::with_models` for family
and capability-based routing. `Injected::with_model_metadata` accepts
`ModelInventory` values with endpoint and reasoning capabilities, so embedded
applications can provide the same evidence as std discovery. `Limits` bounds
buffered responses and SSE records.
The friendly generation builder and gateway relay/proxy adapters currently
remain standard-only.

Shared `tokn_core::types::{Bytes, SmolStr, Shared}` are concrete types selected at build
time. Standard builds re-export the original upstream types; embedded builds
use local static/inline/`Rc` storage. The local backend supports single-threaded,
non-atomic targets and is intentionally neither `Send` nor `Sync`.

`crates/sdk/tests/interpretation_parity.rs` compares real standard and injected
clients using controlled upstream responses: stream mode, initiator headers,
session aliases, option precedence, and account affinity. Extraction-stage
comparisons also cover request purpose across all three APIs and missing-model
behavior. These checks establish interpretation parity, not full stage parity.
`crates/requests/tests/injected_event_parity.rs` compares usage and terminal
events across all nine upstream/requested API pairs for buffered JSON,
buffered SSE, and live SSE, including conversion failure and cancellation.

Verification also includes an isolated default-feature SDK consumer and an isolated
alloc-only consumer that exercises configured requests, injected transports,
and suspended stream reads. The latter is compiled for both targets below:

```sh
cargo test --locked --manifest-path crates/sdk/tests/std/Cargo.toml
cargo test --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml
cargo build --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml --target thumbv6m-none-eabi
cargo build --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml --target thumbv7em-none-eabihf --release
```
