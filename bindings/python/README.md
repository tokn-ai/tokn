# tokn-requests Python SDK

The Python package embeds the same Rust routing engine as `tokn-sdk`. It uses
the existing `config.toml`, `config.d`, `auth.yaml`, and `auth.d` sources and
does not require a gateway process.

## Installation

Install the PyPI distribution `tokn-requests`, then import `tokn_requests`:

```sh
python -m pip install tokn-requests==0.2.3
```

Release wheels use CPython's Python 3.10 stable ABI (`abi3-py310`): one
`cp310-abi3` wheel per platform supports regular CPython 3.10 and newer.
CI tests the same wheels on CPython 3.10 through 3.14 on Linux x86-64
(glibc 2.17 or newer), macOS 11 or newer on Apple Silicon, and Windows x86-64.
Free-threaded CPython builds require a different ABI and are not covered by
these wheels.
Other platforms can build the source distribution with a current stable Rust
toolchain and a C/C++ compiler. Type annotations and native extension stubs
are included in the installed package.

## Friendly generation API

For a one-off request, start with the client-bound builder:

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

Responses normalize text, reasoning, tool calls, token usage, and finish
reasons across supported providers.

Common generation controls are available directly on both the client-bound and
detached builders:

```python
from tokn_requests import (
  ReasoningEffort,
  ReasoningMode,
  ReasoningSummary,
)

openai_call = (
  client.generate("gpt-5")
  .prompt("Solve this step by step.")
  .max_tokens(2048)
  .reasoning_effort(ReasoningEffort.HIGH)
  .reasoning_summary(ReasoningSummary.AUTO)
)

llama_call = (
  client.generate("local-llama")
  .prompt("Compare these implementations.")
  .top_p(0.9)
  .top_k(40)
  .max_tokens(2048)
)

claude_call = (
  client.generate("claude-sonnet-4.6")
  .prompt("Plan this migration.")
  .max_tokens(2048)
  .reasoning_mode(ReasoningMode.ADAPTIVE)
  .reasoning_effort(ReasoningEffort.HIGH)
)
```

`max_tokens()` is a convenience alias for the provider-neutral
`max_output_tokens()` control. Managed routes serialize that limit as
`max_output_tokens` for Responses, `max_completion_tokens` for OpenAI Chat
Completions, and `max_tokens` for other Chat Completions or Messages routes.
Codex account routes reject an explicit limit because that backend does not
preserve it.

These examples assume the model selectors route to OpenAI Responses,
llama.cpp Chat Completions, and Copilot's Claude Chat Completions fallback,
respectively; use selectors from your own configuration. `top_p()` is
portable across compatible routes and accepts values from 0 through 1.
Responses supports reasoning effort and summary but not `top_k` or an
enabled/adaptive mode. Typed `top_k` is currently supported on llama.cpp Chat
Completions; llama.cpp has no portable reasoning control. Known non-reasoning
models reject typed reasoning locally.

Effort levels are validated against cached upstream model metadata, falling
back to the provider-specific models.dev catalogue. Discovery exposes
`x_tokn_router.capabilities.reasoning_efforts`: `null` means unknown and `[]`
means no effort control. Unknown support does not reject an effort value.
DeepSeek V4 Flash advertises `low`, `high`, and `max`; V4 Pro advertises `high`
and `max`. DeepSeek thinking also rejects
`temperature` and `top_p`, which that backend would ignore. Claude supports
adaptive reasoning on 4.6 and newer models but not a reasoning summary. Manual
Claude reasoning requires `ReasoningMode.ENABLED`, an explicit `max_tokens()`
limit, a budget of at least 1024 tokens, and
`budget_tokens < max_tokens`; manual mode is rejected on 4.7 and newer models,
while adaptive mode is rejected on 4.5 and older models. Claude effort levels
use discovery metadata; sampling compatibility uses the selected model generation.
Explicit controls unsupported by the selected route fail clearly after routing
instead of being silently dropped or reinterpreted.

`passthrough` and `switch` profiles preserve the generated Responses payload
verbatim, so they reject typed `top_k` and reasoning controls that would
require post-route lowering. Use an `exact`, `route`, or `fuzzy` profile for
the provider-neutral control API.

The snippets below continue using the `client` created above. Snippets after
the detached-request example also reuse its `request`.

Build an owned request when it needs to be serialized, transformed, queued, or
reused independently of a client:

```python
from tokn_requests import GenerateRequest

request = (
  GenerateRequest.builder("smart")
  .prompt("Explain this function.")
  .temperature(0.2)
  .build()
)

serialized = request.to_json()
request = GenerateRequest.from_json(serialized)
request = request.with_changes(max_output_tokens=128)

response = await client.send(request)
```

As an alternative to `client.send(request)`, use
`await request.bind(client).send()` when fluent binding is more convenient.

Semantic streaming returns typed events:

```python
from tokn_requests import Completed, TextDelta

stream = await client.generate("smart").prompt("Write a haiku.").stream()
async with stream:
  async for event in stream:
    if isinstance(event, TextDelta):
      print(event.text, end="")
    elif isinstance(event, Completed):
      print(f"\nfinish reason: {event.finish_reason}")
```

Use `stream_text()` when only generated text is needed:

```python
stream = await client.stream_text(request)
async with stream:
  async for text in stream:
    print(text, end="")
```

Local request-model validation raises `ValueError` before native execution.
Execution failures derive from `ToknError` (and remain compatible with
`RuntimeError`). Catch a specific subtype when recovery depends on the cause:

```python
from tokn_requests import APIStatusError, ToknError

try:
  response = await client.send(request)
except APIStatusError as error:
  print(error.status, error.body)
except ToknError as error:
  print(f"request failed: {error}")
```

## Raw endpoint escape hatches

Raw endpoint clients are the exact-wire escape hatch for endpoint- or
provider-specific fields. Unlike the friendly generation API, the mapping is
sent in the selected endpoint's native shape:

```python
raw = await client.responses.create({
  "model": "gpt-5",
  "input": "Explain this function.",
})

stream = await client.chat.completions.stream({
  "model": "claude-sonnet-4",
  "messages": [{"role": "user", "content": "Hello"}],
})
async with stream:
  payload = b"".join([chunk async for chunk in stream])
```

Raw stream chunks are transport bytes and do not necessarily align with SSE
event or UTF-8 boundaries.

Pass `config_path`, `auth_path`, or `profile` to `Client` to override the same
defaults used by the gateway.

## Preparing a PyPI release

Pushing `v0.2.3-sdk` runs the `Python release` workflow in
`.github/workflows/release-python.yml`. It builds three stable-ABI wheels,
audits their Python symbols, installs the same artifacts on CPython 3.10–3.14,
rebuilds the source distribution with locked Cargo dependencies, and uploads
the distributions as workflow artifacts. Branch runs build only. Use the
successful CI artifacts for the manual publication steps in the
[SDK release guide](../../docs/sdk-release.md). `VERSION`, the Cargo workspace
version, and `pyproject.toml` must agree.

For optional automated publication in future releases, create the GitHub
environment `pypi` and register a PyPI trusted publisher for:

- Project: `tokn-requests`
- Owner: `tokn-ai`
- Repository: `tokn`
- Workflow: `release-python.yml`
- Environment: `pypi`

Use a pending publisher in PyPI's account publishing settings if the project
does not exist yet. Once workflow dispatch is available, `publish=false`
rehearses a release and `publish=true` enables the publishing job. This job
uses GitHub OIDC. Configure any desired release approval rules on the `pypi`
environment before enabling publishing. See the
[PyPI trusted publishing setup](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
and [publishing documentation](https://docs.pypi.org/trusted-publishers/using-a-publisher/).

Use Python 3.12 or newer to run the release packaging helper. To build and
test a local wheel from the repository root:

```sh
python -m venv tmp/release-python/venv
tmp/release-python/venv/bin/python -m pip install 'maturin==1.14.1' twine
tmp/release-python/venv/bin/maturin build --release --locked \
  --manifest-path bindings/python/Cargo.toml --out tmp/release-python/dist
cargo fetch --locked --manifest-path bindings/python/Cargo.toml
tmp/release-python/venv/bin/python bindings/python/scripts/build_sdist.py \
  --out tmp/release-python/dist
tmp/release-python/venv/bin/python -m twine check --strict tmp/release-python/dist/*
tmp/release-python/venv/bin/python -m pip install tmp/release-python/dist/*.whl
tmp/release-python/venv/bin/python -m unittest discover -s bindings/python/tests
```

The local wheel targets the current operating system and CPython's Python 3.10
stable ABI. The release workflow builds the portable Linux wheel inside a
manylinux2014 container. Check a wheel's package metadata and stable ABI with:

```sh
tmp/release-python/venv/bin/python bindings/python/scripts/check_wheel.py \
  tmp/release-python/dist/*.whl
tmp/release-python/venv/bin/python -m pip install abi3audit==0.0.26
tmp/release-python/venv/bin/python -m abi3audit --strict --summary \
  tmp/release-python/dist/*.whl
```

Use the source-distribution helper above when preparing releases. Maturin
removes unrelated Cargo workspace members from an sdist but currently leaves
their lockfile entries behind ([upstream issue](https://github.com/PyO3/maturin/issues/2609)).
The helper reconciles the archive's lockfile offline, verifies that every
remaining dependency retains its original version and checksum, and checks
that Cargo accepts it with `--locked`. The repository's lockfile is preserved.
