# Request-level version dispatch

The frontend owns the fixed API and forward-proxy listeners. Each worker runs
its gateway version and accepts requests through one private Unix socket.
Worker selection happens for each API, plain proxy, or intercepted HTTPS
request. Existing CONNECT/TLS connections stay open across worker switches.
This mode supports macOS and Linux.

## Start and upgrade

Start the frontend once, in its own terminal or process manager:

```sh
tokn-gateway frontend --with-proxy
```

Start a worker in another terminal:

```sh
tokn-gateway worker start
```

The worker discovers the frontend, inherits its listener projection settings,
opens its IPC socket, and registers automatically. No socket paths or worker
IDs are needed. Start the replacement using its new binary:

```sh
/path/to/new/tokn-gateway worker start
```

Once the new worker passes compatibility checks, it takes all new requests.
The old worker stops receiving new requests and exits automatically after its
pending requests and response streams finish. Idle client connections do not
prevent retirement. Each request executes once; transport failures are never
replayed on another worker.

For a single-command startup, use:

```sh
tokn-gateway serve --with-proxy
```

`serve` starts an independent frontend process if none exists, then runs an
attached worker in the foreground. If a frontend already exists, it reuses it
and starts a replacement worker. The frontend survives worker retirement and
worker Ctrl-C. To stop the whole service, signal the frontend process; its PID
is logged when `serve` starts or reuses it. An automatically started frontend
writes output to `frontend.log` beside its control socket, whose directory is
also logged during startup.

Native v2 configs declare their listeners directly. Use `frontend` or `serve`
without `--with-proxy`; `worker start` needs no listener flags for either schema.
Frontend policy changes require restarting the frontend. An existing frontend
started without the legacy proxy cannot gain it through a later
`serve --with-proxy` invocation.

## Configuration and discovery

All commands use the usual global `--config` option. Discovery is scoped to the
canonical configuration file path, so different binary versions using the same
config find the same frontend. The private runtime directory is created
with mode `0700`; sockets use `0600`. Workers never bind the public TCP ports.
A stale socket is reported instead of unlinking another process's listener.

To run a worker with a separate execution config, identify the frontend config:

```sh
/path/to/new/tokn-gateway --config candidate.toml worker start \
  --frontend-config frontend.toml
```

Worker configs must preserve listener IDs, kinds, authentication modes, API
mounts, enabled endpoints, and API credential ownership. Provider destinations,
models, and execution policies may differ. Admission and IPC compatibility are
checked before the new assignment is published. A rejected worker leaves the
current worker serving traffic.

The frontend owns client authentication, request limits, CONNECT policy, and
the interception CA. Workers own routing, provider pipelines, conversion, and
persistence. For comparisons between versions with different database schemas,
configure separate persistence paths in their configs, for example:

```toml
[service.persistence]
usage_db_path = "/path/to/candidate/usage.db"
sessions_db_path = "/path/to/candidate/sessions.db"
requests_dir = "/path/to/candidate/requests"
```

Changing the config filename alone does not isolate the default database paths.
For temporary experiments, persistence may instead be disabled explicitly.

## A/B experiments

Register a candidate without replacing or retiring the current worker:

```sh
/path/to/candidate/tokn-gateway --config candidate.toml worker start \
  --frontend-config frontend.toml --candidate
```

The candidate starts with weight zero. A loopback API listener exposes
`/admin/workers`, guarded by the explicit `x-tokn-admin: workers` header.
Inspect worker IDs, versions, weights, and active requests:

```sh
curl -H 'x-tokn-admin: workers' http://127.0.0.1:4141/admin/workers
```

Use the returned IDs to assign relative request weights:

```sh
curl -X POST http://127.0.0.1:4141/admin/workers \
  -H 'x-tokn-admin: workers' -H 'Content-Type: application/json' \
  -d '{"weights":{"CURRENT_ID":9,"CANDIDATE_ID":1}}'
```

This deterministic cycle gives the candidate one in every ten new assignments.
Every update must include every connected worker and keep at least one positive
weight. Positive-weight workers are checked before publishing an update.
Weight changes preserve existing streams. Workers registered for an experiment
remain available at zero weight, allowing rollback by reversing the weights.
To finish the experiment with automatic retirement, start the chosen binary
with ordinary `worker start` or `serve`; it becomes the only current worker
and drains all earlier workers. You can also stop an experimental worker yourself.

Status includes assigned, completed, and cancelled requests, transport and HTTP
errors, time to response headers, total request duration, and active requests.
Counters live for that registration; disconnected workers disappear from status.
Use separate worker request/usage records for token and pipeline comparisons.
`RUST_LOG=debug` includes worker and request IDs in dispatch logs.

Responses, including SSE, stream over IPC with backpressure and cancellation.
Request bodies retain bounded ingress buffering. IPC preserves the original
URI, headers, proxy authority, authentication scope, request ID, and connection
addresses. Client-supplied `x-tokn-ipc-*` headers are replaced with trusted
frontend metadata. Only processes with access to the private directory may
submit IPC requests directly.

Both gateway versions must implement this IPC protocol. Existing releases
without worker support need an initial upgrade before they can participate.
Opaque passthrough CONNECT tunnels remain frontend-owned; they have no decoded
requests and do not enter worker assignment or counters. If every worker exits,
the frontend remains listening and dispatch requests fail until a new worker
registers. Frontend shutdown or control disconnection also stops attached workers.
