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

## Worker states and Ctrl-C

The frontend tracks `current`, `stale`, and `exiting` workers. Starting a normal
worker makes it current and makes earlier workers stale. Stale workers exit
automatically once their requests finish; until then, they can take over again.
Zero-weight A/B candidates stay available until replaced or explicitly stopped.

The first Ctrl-C (or SIGTERM) on `serve` or `worker start` marks that worker
exiting and stops assigning it requests. The newest eligible stale worker becomes
current, preferring one already receiving traffic. Existing requests on both
workers continue uninterrupted. The exiting worker waits for all its requests
and streams to finish, with no drain deadline, then flushes persistence and exits.
Exiting workers cannot be promoted or given a positive traffic weight.
A second shutdown signal exits immediately with status 130, cancelling remaining
work. The frontend and its client connections remain running.

`GET /admin/workers` includes `main_worker_id` and each worker's `state`; exiting
workers remain listed while draining. If no eligible worker remains, new requests
fail until another worker registers.

This lifecycle uses frontend control protocol version 2. Restart a frontend
running control version 1 before attaching these workers. Request IPC remains
version 1.

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

### Automatic rollout policies

With a frontend and one baseline worker already running, start the new version:

```sh
/path/to/new/tokn-gateway worker start --ab-test
# Or reuse the existing frontend through serve:
/path/to/new/tokn-gateway serve --with-proxy --ab-test
```

By default, the frontend sends the new worker 10% of new requests initially and increases
its share linearly toward 90% over 24 hours. Weights advance in one percentage
point increments every 18 minutes: 30% at six hours, 50% at twelve hours, and
70% at eighteen hours. At 24 hours, the new worker takes 100% and the baseline
exits automatically after its existing requests and streams finish. Idle client
connections do not delay retirement. The new worker is `current` during the
experiment; the baseline is `stale` and continues receiving its share.

The frontend owns the monotonic clock and updates weights independently of
request arrivals. This rollout is driven by elapsed time, without an error-rate
or latency threshold. The `ab_test` object in `/admin/workers` shows the worker
IDs, elapsed and total seconds, the initialized `rollout_policy`, and the new
worker's traffic percentage; it is
removed after completion or cancellation. The worker's terminal footer also shows
`ab=10%/90% elapsed=00:01/24:00`: the split is new/baseline and times are hours
and minutes. It refreshes every five seconds even without requests, and clears
when the experiment ends. The frontend must support banner status queries;
restart it with this version to enable them.

A manual weight update cancels the automatic ramp. Starting an ordinary worker
also cancels it and replaces both versions. Stopping either participating worker
cancels the ramp and leaves the surviving worker handling traffic. Existing
requests retain their original worker throughout all changes. Only one automatic
experiment may run at a time, and starting it requires exactly one worker with a
positive weight. `--ab-test` conflicts with `--candidate`.

The frontend must advertise `rollout_policy_v1` for custom policies. Upgrade an
older frontend once, then attach the baseline and new worker. Future changes to
initial policies do not require restarting that frontend. The default 24-hour
policy also works with a frontend that advertises the earlier automatic A/B
capability; custom policies fail clearly on that version. Control protocol v2 and request IPC v1 are unchanged, so earlier v2
worker binaries can still act as the baseline. An experiment is not persisted
across frontend shutdown; attached workers stop when the frontend stops.

Set the duration when starting the worker:

```sh
tokn-gateway worker start --ab-test --ab-test-duration 72h
tokn-gateway serve --with-proxy --ab-test --ab-test-duration 36h
```

For custom percentages and stages, initialize from a TOML file:

```sh
tokn-gateway worker start --ab-test --ab-test-policy rollout.toml
```

```toml
initial_percent = 5
completion_percent = 100

# Hold at 5% for six hours.
[[stages]]
duration_seconds = 21600
traffic_percent = 5

# Ramp from 5% to 90% over the next 66 hours.
[[stages]]
duration_seconds = 237600
traffic_percent = 90
```

Each stage interpolates linearly from the previous percentage to its target.
Durations must be positive whole seconds. Initial and stage percentages must be
between 1 and 99 so both workers remain available throughout the experiment.
At completion, `completion_percent` applies immediately: 100 drains the baseline,
0 returns traffic to the baseline and drains the new worker, and an intermediate
percentage retains that split.

The CLI reads the policy once and sends it to the frontend during registration.
An active policy cannot be edited; changing its file does not change the running
experiment. `--ab-test-duration` and `--ab-test-policy` require `--ab-test` and
cannot be combined. The frontend executes the supplied stages and owns their
clock; duration and traffic defaults belong to the CLI.

For a separate new-version config, use `worker start --ab-test` with the global
`--config candidate.toml` and `--frontend-config frontend.toml` options. The
separate persistence paths described above still apply.

### Manual traffic weights

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
