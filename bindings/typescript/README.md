# @tokn/sdk

Embedded TypeScript SDK for routing LLM requests through the providers,
profiles, configuration, and credentials already managed by tokn.

The package is an ESM package for Node.js 22+ and Bun. Its public API is
TypeScript, while routing and provider execution run in-process through the
same Rust engine as `tokn-gateway`.

Install it with npm or Bun:

```sh
npm install @tokn/sdk
# or
bun add @tokn/sdk
```

The package installs a prebuilt native addon through an exact-version optional
dependency. Supported native platforms are macOS arm64 (addon deployment target
11.0), Linux x64 with glibc 2.28+, and Windows x64 MSVC. The selected Node.js or
Bun runtime's own OS requirements also apply. Keep optional dependencies enabled. Other
architectures and musl-based Linux distributions currently require a source
build. Consumer installation does not compile Rust or run an install script.

## Usage

```ts
import { Client } from "@tokn/sdk";

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

Requests can also be plain, owned objects that are easy to serialize, queue,
or transform:

```ts
const request = {
  model: "smart",
  prompt: "Plan this migration.",
  top_p: 0.9,
  max_output_tokens: 2048,
  reasoning: {
    effort: "high",
    summary: "auto",
  },
} as const;

const response = await client.send(request);
```

Builder methods use camelCase. Fields that cross the serialization boundary
use snake_case.

## Streaming and cancellation

Generation streams are pull-based async iterables:

```ts
const controller = new AbortController();
const stream = await client.textStream(
  {
    model: "smart",
    prompt: "Write a short explanation.",
  },
  { signal: controller.signal },
);

try {
  for await (const text of stream) {
    process.stdout.write(text);
  }
} finally {
  await stream.close();
}
```

Breaking from a `for await` loop closes the stream automatically. Passing an
`AbortSignal` cancels the underlying Rust operation, including an idle stream
read.

## Raw endpoints

The provider-neutral API is the default. Raw endpoint namespaces remain
available when an application needs an exact wire shape:

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

## Development

From this directory:

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm test
```

`pnpm build` compiles both the host N-API addon and the TypeScript façade.
An application can then depend on this directory with a local `link:` or
workspace dependency.

For a host-only packaging rehearsal, use Node.js 24 for the release scripts:

```sh
pnpm build
pnpm release:pack --target darwin-arm64
pnpm release:test darwin-arm64
```

Use `linux-x64-gnu` or `win32-x64-msvc` for the other supported hosts. The
install test serves the packed archives from a temporary local npm registry,
installs the facade with npm and Bun into fresh directories, and completes a
native provider request. This verifies dependency selection and loading without
falling back to a checkout binary.

## Registry releases

Run `.github/workflows/release-npm.yml` manually on the intended source ref,
with `version` matching `VERSION` and `publish` left false for rehearsal. The
workflow builds every native target, checks the Linux artifact's glibc symbol
requirements against the 2.28 floor, packs one facade and three native packages,
and install-tests the same archives with Node.js 22/24 and Bun 1.3.13. Linux
uses the locked NAPI cross toolchain's glibc 2.17 sysroot. The resulting support
floor also accounts for the Node.js runtime.

First publication requires the npm account to own the `@tokn` scope. Publish
the three native archives before the facade, using the tested `npm-packages`
workflow artifact and an authenticated npm account. This completes the first
0.2.3 release. Do not create placeholder versions or run a second publication
of the same version. See the [SDK release guide](../../docs/sdk-release.md)
for the publication commands and recovery steps.

npm trusted publishing requires each package to already exist. After the first
release, register a GitHub trusted publisher for **each** of `@tokn/sdk`,
`@tokn/sdk-darwin-arm64`, `@tokn/sdk-linux-x64-gnu`, and
`@tokn/sdk-win32-x64-msvc`, with repository `tokn-ai/tokn`, workflow filename
`release-npm.yml`, environment `npm`, and direct `npm publish` allowed. Configure
the `npm` GitHub environment's release protections. Future versions can then
set `publish` true; publication runs only after every install test passes.

The workflow uses Node.js 24 and checks npm 11.5.1+ for OIDC authentication.
It publishes the verified archives with provenance and disabled lifecycle
scripts, uploading native packages first. A partial-publication retry accepts
an existing version only when the registry archive's integrity matches exactly.
Different bytes require a new version because npm versions are immutable.

References: [NAPI-RS release model](https://napi.rs/docs/deep-dive/release),
[cross compilation](https://napi.rs/docs/cross-build),
[npm trusted publishing](https://docs.npmjs.com/trusted-publishers/), and
[npm trust prerequisites](https://docs.npmjs.com/cli/v11/commands/npm-trust/).
