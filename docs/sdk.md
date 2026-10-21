# Embedded SDKs

With their default features, the SDKs execute requests in-process through the same configuration,
credentials, account pool, routing, conversion, retry, and provider
implementations as the gateway. They do not require a gateway process.
Rust also supports direct Responses, Chat Completions, and Messages requests
with caller-supplied networking in `no_std + alloc` environments.

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

### Direct provider APIs (`no_std + alloc`)

Disable the default `std` feature to use `DirectClient` without the gateway,
Tokio, Reqwest, filesystem configuration, or credential stores:

```toml
[dependencies]
tokn-sdk = { version = "0.2.4", default-features = false }
serde_json = { version = "1", default-features = false, features = ["alloc"] }
```

`DirectClient` also works with default features enabled, alongside the existing
`Client`. It accepts the same typed `ResponsesRequest`, `ChatRequest`, and
`MessagesRequest` wire schemas and returns `direct::BufferedResponse<T>` with
`status`, `headers`, and typed `data`. Each call takes `&mut self`, allowing the
transport to own a device driver or a single connection.

```rust
use tokn_sdk::{
  direct::{BufferedResponse, Error, OpenAi, Transport},
  responses::{ResponsesRequest, ResponsesResponse},
  DirectClient,
};

async fn ask<T: Transport>(
  transport: T,
  provider: OpenAi,
) -> Result<BufferedResponse<ResponsesResponse>, Error<T::Error>> {
  let mut client = DirectClient::new(transport, provider);
  let request: ResponsesRequest = serde_json::from_value(serde_json::json!({
    "model": "gpt-5",
    "input": "Say hello.",
    "max_output_tokens": 64
  }))
  .expect("valid request");

  client.responses(&request).await
}
```

Create the provider with `OpenAi::new(api_key)?`. The default base URL is
`https://api.openai.com/v1`. For an explicit OpenAI-compatible service, use
`OpenAi::new(api_key)?.with_base_url("https://example.com/api/v1")?`.
This convenience configuration enables Responses and Chat Completions.

For Anthropic Messages, use `Anthropic`:

```rust
use tokn_sdk::{direct::Anthropic, messages::MessagesRequest, DirectClient};

let provider = Anthropic::new(api_key)?;
let mut client = DirectClient::new(transport, provider);
let request: MessagesRequest = serde_json::from_value(serde_json::json!({
  "model": "claude-sonnet-4-6",
  "max_tokens": 64,
  "messages": [{ "role": "user", "content": "Say hello." }]
}))?;
let response = client.messages(&request).await?;
```

`Anthropic` enables only Messages at `https://api.anthropic.com/v1/messages`,
using `x-api-key` and `anthropic-version: 2023-06-01`. Override the API base URL
with `with_base_url()` and the version header with `with_version()`. See
[Anthropic API versioning](https://platform.claude.com/docs/en/api/versioning).

#### Explicit endpoint routes and credentials

Use `ProviderConfig` and `EndpointConfig` for other providers or proxies. Each
endpoint has its own complete URL and headers, so paths and authentication can
differ between formats. For example, OpenCode Go uses Bearer authentication for
Chat Completions and Responses, and an API-key header for Messages:

```rust
use tokn_sdk::direct::{ConfigError, Endpoint, EndpointConfig, ProviderConfig};

fn opencode_routes(api_key: &str) -> Result<ProviderConfig, ConfigError> {
  Ok(ProviderConfig::new()
    .with_endpoint(
      Endpoint::ChatCompletions,
      EndpointConfig::new("https://opencode.ai/zen/go/v1/chat/completions")?
        .with_bearer_auth(api_key)?,
    )
    .with_endpoint(
      Endpoint::Responses,
      EndpointConfig::new("https://opencode.ai/zen/go/v1/responses")?
        .with_bearer_auth(api_key)?,
    )
    .with_endpoint(
      Endpoint::Messages,
      EndpointConfig::new("https://opencode.ai/zen/go/v1/messages")?
        .with_header("x-api-key", api_key)?
        .with_header("anthropic-version", "2023-06-01")?,
    ))
}
```

Pass the result to `DirectClient::new(transport, opencode_routes(api_key)?)`.
Only configured endpoints are callable; other operations return
`UnsupportedEndpoint` before transport I/O. An endpoint's headers are sent only
to that endpoint. Configuring all three formats does not mean every provider
model accepts all three; choose a model and native request supported by the
selected endpoint.

`EndpointConfig::new()` requires HTTPS and preserves the complete URL exactly,
without appending a path. The `OpenAi` and `Anthropic` convenience configurations
instead append operation paths to their base URL and remove trailing slashes.
The portable URL validator accepts ASCII hosts, optional ports, and unescaped
paths, and rejects userinfo, queries, fragments, percent escapes, and dot
segments. A local llama.cpp server can explicitly opt into cleartext HTTP:

```rust
let provider = ProviderConfig::new().with_endpoint(
  Endpoint::ChatCompletions,
  EndpointConfig::new_insecure_http("http://127.0.0.1:8080/v1/chat/completions")?,
);
```

Authentication is optional on explicit routes. Add `with_bearer_auth()` when
the server requires a key or an already acquired access token. Header names
and values are validated; repeated names replace the previous value without
regard to case. SDK-owned Accept/Content-Type headers and HTTP framing or
hop-by-hop headers cannot be overridden. Credentials and other configured
header values are redacted from debug output.

The caller owns credential acquisition and expiry handling. Use
`client.set_provider(updated_routes)` to replace credentials or routes while
retaining the transport. Native protocol support does not include the default
client's provider authentication and body transformations: Copilot needs a
caller-supplied exchanged API token and its required headers; Codex needs an
access token, appropriate headers, and an already adapted native streaming
Responses body. `DirectClient` does not perform their login, refresh, body
rewrites, or model routing.

#### Injected transport

Implement `direct::Transport` on the application's HTTP/TLS adapter. Its contract is:

```rust
pub trait Transport {
  type Error;

  async fn send(&mut self, request: Request) -> Result<Response, Self::Error>;
}
```

`Request` contains a method, complete URL, ordered header entries, and JSON body
bytes. The SDK supplies the configured endpoint headers, content type, and
accept headers.
Return a `Response` containing status, headers, and buffered body bytes, including
for non-2xx responses. The SDK checks HTTP status before decoding JSON, and
`Error::HttpStatus` retains the original response for inspecting provider errors
and retry headers.

The transport owns DNS, TCP, TLS certificate verification, timeouts, and body
size limits while receiving data. It must decode HTTP transfer/content encodings.
Prefer returning redirects as responses; redirects must never forward
credentials to another origin or downgrade HTTPS. No executor, boxed future,
`Send` bound, or pointer atomics are required by this client. Firmware supplies
an allocator and its own executor.

The buffered methods `responses()`, `chat_completions()`, and `messages()`
reject `stream: true`. Use native endpoint fields such as `max_output_tokens`
for Responses, `max_completion_tokens` for OpenAI Chat Completions, or
`max_tokens` for Messages.
The neutral `generate()` builder, routing profiles, automatic retries, OAuth
refresh, and lifecycle subscriptions belong to the default `std` client.
Endpoint schema crates also support `no_std + alloc` independently.

#### Injected streaming

Implement `direct::StreamingTransport` to receive typed SSE events without
buffering a complete response. This trait is independent of `Transport`, so a
device can implement only the request modes it uses:

```rust
pub trait StreamBody {
  type Error;
  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error>;
}

pub trait StreamingTransport {
  type Error;
  type Body<'a>: StreamBody<Error = Self::Error>
  where
    Self: 'a;

  async fn send_stream(
    &mut self,
    request: Request,
  ) -> Result<StreamingResponse<Self::Body<'_>>, Self::Error>;
}
```

`StreamingResponse` contains `status`, `headers`, and an incremental `body`.
The body may borrow the transport's socket or driver and need not implement
`Send`. Return decoded HTTP body bytes from `read`; zero means permanent EOF.
Reads must be cancellation safe, and dropping the body must abort or release
the response without draining it. The adapter retains responsibility for TLS,
timeouts, header limits, and any transport buffers.

Use `responses_stream(&request, options)`,
`chat_completions_stream(&request, options)`, or
`messages_stream(&request, options)`. All set `stream: true` in the outgoing
JSON, overriding any conflicting typed field or flattened extra, and request
`text/event-stream`.

```rust
use tokn_sdk::{
  direct::{StreamError, StreamOptions, StreamingTransport},
  responses::{ResponsesEvent, ResponsesRequest},
  DirectClient,
};

async fn receive<T: StreamingTransport>(
  client: &mut DirectClient<T>,
  request: &ResponsesRequest,
  mut on_event: impl FnMut(ResponsesEvent),
) -> Result<(), StreamError<T::Error>> {
  let options = StreamOptions {
    max_event_bytes: 32 * 1024,
    max_error_body_bytes: 4 * 1024,
  };
  let mut stream = client.responses_stream(request, options).await?;
  while let Some(event) = stream.next_event().await? {
    // Inspect failure/incomplete events as well as text deltas and completion.
    on_event(event);
  }
  Ok(())
}
```

`EventStream` exposes HTTP `status` and `headers`. `next_event()` reads on
demand, returning native `ResponsesEvent`, `ChatEvent`, or `MessagesEvent`
values. It handles fragmented UTF-8, LF/CR/CRLF, comments, and multiline SSE
data. Unknown Responses and Messages event types remain available as their
respective `Other` variants; malformed payloads for known event types are
rejected.

Chat streams yield `ChatEvent::Done` for `[DONE]`; a chunk's `finish_reason`
does not terminate the stream because usage can follow it. Responses streams
yield their terminal event, including failed, incomplete, or error outcomes.
Messages streams yield `MessagesEvent::MessageStop` or `MessagesEvent::Error`
as their terminal event; `Ping` keeps the stream open.
These outcomes must be inspected by the application; reaching the end alone
does not mean generation succeeded. See the official
[Responses streaming event definitions](https://developers.openai.com/api/reference/resources/responses/streaming-events)
and [Chat Completions stream options](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create),
plus [Anthropic's streaming event lifecycle](https://platform.claude.com/docs/en/build-with-claude/streaming).

EOF before an endpoint terminal event returns `StreamError::UnexpectedEof`.
Malformed JSON, invalid SSE UTF-8, oversized records, and body-read failures
also end the stream. A successful HTTP response must have a
`text/event-stream` content type. Non-2xx responses return
`StreamError::HttpStatus` with status, headers, a bounded body prefix, and a
`truncated` flag.

Defaults allow 64 KiB per SSE record and retain 8 KiB of an HTTP error body.
The event limit includes fields, comments, and delimiters, counting CRLF as one
newline; it is enforced before growing the record buffer. The reader uses a
fixed 1 KiB buffer and never accumulates the entire generated response. These
are wire-data limits, not a total heap budget: JSON decoding, transport buffers,
and events retained by the application consume additional memory.

Dropping the stream or calling `cancel()` immediately drops its body. Terminal
events and errors also drop it, and later `next_event()` calls return `None`.
Dropping a pending `next_event()` future preserves parser state so reading can
resume, provided the transport follows the cancellation-safe read contract.
Drop the stream itself to release its borrow of the client. Local cancellation
does not issue a provider cancellation API request.

#### Consumer verification

Bare-metal checks build both the SDK and an isolated consumer that instantiates
Responses, Chat Completions, and Messages in both buffered and streaming modes
with a transport that does not implement `Send`. Streaming bodies borrow that
transport and exercise pending reads:

```sh
rustup target add thumbv6m-none-eabi thumbv7em-none-eabihf
cargo build --locked -p tokn-sdk --lib --no-default-features --target thumbv6m-none-eabi
cargo build --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml --target thumbv6m-none-eabi
cargo build --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml --target thumbv7em-none-eabihf --release
cargo test --locked --manifest-path crates/sdk/tests/no_std/Cargo.toml
cargo test --locked --manifest-path crates/sdk/tests/std/Cargo.toml
```

CI covers both targets in debug and release, including a target without pointer
atomics. The isolated consumer avoids the SDK's native test dependencies enabling
`std` through Cargo feature unification. These checks verify compilation and a
host round trip through an in-memory transport; they do not exercise a device's
network stack or make paid provider calls.

The separate `std` consumer resolves the SDK's default features outside the
workspace. It loads configuration and verifies the existing `Client` buffered
and streaming behavior against a local HTTP server, without workspace test
dependencies enabling missing features.

## Python

The `bindings/python` package is a mixed Python/Rust package built with
Maturin and PyO3. Its native module owns an `Arc<tokn_sdk::Client>`, while the
public generation models are dependency-free Python dataclasses.

The PyPI distribution is `tokn-requests`; its import name is `tokn_requests`.
Install it with:

```sh
python -m pip install tokn-requests
```

Release preparation and publisher setup are documented in
[SDK releases](sdk-release.md).

The native extension uses CPython's Python 3.10 stable ABI (`abi3-py310`). Each
platform wheel supports regular CPython 3.10 and newer; CI tests the same wheel
on CPython 3.10–3.14. Free-threaded Python requires a different ABI.

The client-bound builder mirrors the Rust API:

```python
from tokn_requests import Client

client = Client()

response = await (
  client.generate("smart")
  .system("You are a Python expert.")
  .prompt("Explain this function.")
  .temperature(0.2)
  .send()
)

print(response.text)
```

Python exposes the same neutral generation controls:

```python
from tokn_requests import (
  GenerateRequest,
  ReasoningEffort,
  ReasoningMode,
  ReasoningSummary,
)

openai_request = (
  GenerateRequest.builder("gpt-5")
  .prompt("Solve this step by step.")
  .max_tokens(2048)
  .reasoning_effort(ReasoningEffort.HIGH)
  .reasoning_summary(ReasoningSummary.AUTO)
  .build()
)

llama_request = (
  GenerateRequest.builder("local-llama")
  .prompt("Compare these implementations.")
  .top_p(0.9)
  .top_k(40)
  .max_tokens(2048)
  .build()
)

claude_request = (
  GenerateRequest.builder("claude-sonnet-4.6")
  .prompt("Plan this migration.")
  .max_tokens(2048)
  .reasoning_mode(ReasoningMode.ADAPTIVE)
  .reasoning_effort(ReasoningEffort.HIGH)
  .build()
)
```

As in Rust, `max_tokens()` aliases `max_output_tokens()`. Reasoning modes,
summaries, and `top_k` remain provider- and model-dependent; the three examples
deliberately keep each request to controls its route can represent.
Unsupported explicit controls fail clearly after routing. Use the raw endpoint
clients when the application needs to control the exact wire representation.

`GenerateRequest` is owned and independent from a client, so it can be
serialized, transformed, queued, and later sent or bound:

```python
from tokn_requests import Client, GenerateRequest, Message

client = Client()
request = GenerateRequest(
  model="smart",
  messages=[Message.user("Explain this function.")],
)

serialized = request.to_json()
request = GenerateRequest.from_json(serialized).with_changes(
  max_output_tokens=128,
)
response = await client.send(request)
```

`client.stream(request)` yields typed semantic events and
`client.stream_text(request)` yields only text deltas. The endpoint clients
remain available as raw mapping and byte-stream escape hatches. All calls are
`async`; Python never reads or interprets credential files itself.

## Node.js and Bun

`bindings/typescript` is the ESM `@tokn-ai/requests` package for Node.js 22 and newer
and Bun. Its TypeScript façade exposes plain JSON-compatible objects while a
private N-API binding runs `tokn-sdk` in-process. TypeScript does not load
configuration or credentials itself.

Create and close the client asynchronously:

```ts
import { Client } from "@tokn-ai/requests";

const client = await Client.create();

try {
  const response = await client
    .generate("smart")
    .system("You are a TypeScript expert.")
    .prompt("Explain this function.")
    .temperature(0.2)
    .send();

  console.log(response.text);
} finally {
  await client.close();
}
```

The client-bound builder and detached request builder expose the same neutral
controls as Rust and Python. Builder methods use normal TypeScript casing;
every serializable field stays `snake_case`:

```ts
import { request } from "@tokn-ai/requests";

const value = request("smart")
  .prompt("Plan this migration.")
  .topP(0.9)
  .topK(40)
  .maxTokens(2048)
  .reasoningMode("adaptive")
  .reasoningEffort("high")
  .build();

const serialized = JSON.stringify(value);
const response = await client.send(JSON.parse(serialized));
```

Object input is also a first-class API:

```ts
const response = await client.send({
  model: "smart",
  prompt: "Explain this function.",
  top_p: 0.9,
  max_output_tokens: 256,
  reasoning: {
    effort: "high",
    summary: "auto",
  },
});
```

`client.generateStream()` yields typed semantic events,
`client.textStream()` yields text deltas, and raw endpoint streams yield
`Uint8Array`. Streams are pull-based async iterables with explicit `close()`;
breaking out of `for await` also closes them. Buffered calls and stream
startup accept an `AbortSignal`, which cancels the Rust operation rather than
only abandoning its JavaScript promise.

The raw endpoint namespaces remain available for exact provider wire shapes:

```ts
const response = await client.chat.completions.create(
  {
    model: "smart",
    messages: [{ role: "user", content: "Hello" }],
  },
  {
    profile: "work",
    request_id: crypto.randomUUID(),
  },
);
```

`Client.create()`, `reload()`, and `close()` are async so configuration I/O,
credential loading, and shutdown never block the JavaScript event loop.
Native failures are mapped to stable `ToknError` subclasses and codes;
provider status failures also retain their HTTP status and response body.

Build the package from the repository with:

```sh
cd bindings/typescript
pnpm install --frozen-lockfile
pnpm build
pnpm test
```

The generated `_native.cjs` loader is internal. The public `@tokn-ai/requests`
package uses exact-version optional native packages for Linux x64 glibc, macOS
arm64, and Windows x64 MSVC. Install it with:

```sh
npm install @tokn-ai/requests
```

CI builds the native binding and runs the façade against it with Node.js 22
and Bun 1.3.13 on all three platforms, plus Node.js 24 on Linux. The separate
npm release workflow verifies the Linux glibc floor and tests installed root
and platform tarballs with both runtimes before publication. The macOS artifact
uses an 11.0 deployment target; runtime CI runs on a newer macOS version.
See [SDK releases](sdk-release.md) for platform requirements, build rehearsals,
and registry publisher setup.
