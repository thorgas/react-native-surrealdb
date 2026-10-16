# Experimental sync telemetry v1

`ExperimentalSyncTelemetry` is a React-free metadata channel shared by the HTTP
adapter, scheduler and observed client. Inject the same instance as `telemetry`
into each. Subscribe exporters before starting the scheduler. Events have local
monotone sequence/span ordinals; HTTP children carry a cycle parent. These are
process-local correlations, not globally unique user or commit identifiers.
Typed commit receipts and trusted session events retain the application's commit
identity. The metadata channel deliberately excludes raw commit IDs.

```ts
const telemetry = new ExperimentalSyncTelemetry({ capacity: 64 });
const unsubscribe = telemetry.subscribe(event => queueForUpload(event));
// Supply telemetry to the scheduler, HTTP adapter and observed client.
const support = await observed.supportSnapshot();
const flushed = await telemetry.close(500);
unsubscribe();
```

Each exporter has its own bounded FIFO, drops newest on overflow, and permits
only one outstanding callback. A hung promise cannot hold persistence, scheduler
progress or another exporter; flush returns false at its deadline. No retries
spawn replacement callbacks for a hung exporter. Delivery is asynchronous on timer
tasks so a reentrant exporter cannot create an infinite microtask chain. Exporters
must yield: JavaScript cannot preempt a callback with an infinite synchronous loop.
Use a worker/process for untrusted executable exporters. The legacy `logger` and
`subscribeEvents` remain compatibility APIs for trusted session consumers; use
this channel for bounded external export.

`close` seals ingestion first, drains to the deadline, then discards remaining
queued events with drop accounting. `unsubscribe` cancels queued delivery; an
already running callback cannot be forcibly canceled. Events are advisory and
session-only. Sequence gaps and drops mean a trace is incomplete. Snapshots
include queue/in-flight/delivery/drop/error counters, never error text.

Boundary meanings:

- `native_persistence` measures the awaited native call from JavaScript; it includes
  bridge/lock/encoding/storage time. It does not claim an internal Rust breakdown.
- `local_durable` follows successful native enqueue, `authority_*` follows persisted
  push outcome, and `locally_visible` follows successful atomic pull apply.
- `projection_checkpoint` includes the native projection/state transaction; a
  status read is **not** an atomic application-row snapshot.
- `transport` is request/response roundtrip including response-body read; request
  bytes are encoded body bytes. `encode`/`decode` measure codec calls. Neither
  transport nor client clocks imply server processing duration.
- Scheduler states identify offline, retry/backoff, authentication and failure
  blocks without exporting exception text. Explicit scope/recovery reasons are
  available to owners of those transitions.

Only enumerated phases/reasons and finite numeric fields cross the exporter
boundary; unknown properties are reconstructed away. No bodies, credentials,
principal identifiers, URLs, checkpoint tokens or arbitrary errors are exported.
`supportSnapshot` is JSON-safe, versioned and limited to native status counters,
revision/cursor metadata and channel accounting. Rust/native and server spans use
their own measured clocks and scope; they are not inferred from these client spans.

Native Rust `diagnostics()` adds fixed-size cumulative runtime-lock wait/hold,
normalized state encoding and storage transition counters/nanoseconds. Reads use
independent atomics, never take the runtime or database lock, and are deliberately
not a cross-field atomic snapshot. Lock hold includes storage; storage includes
encoding/validation, so these durations overlap and must not be summed. A latest-completed operation span carries a handle-local ordinal and measured
phase durations. Publishing uses try-lock with counted metadata drops; readers
cannot block runtime work. Only one completed span is retained, so intermediate
operations may be overwritten. Counters restart when a native handle is reopened. Use a new native build for this API.

The private Haus authority returns a server-generated `x-sync-request-id: sync-N`.
Client transport events retain only the bounded numeric N as `serverRequest`.
Server spans carry that same request ordinal with authority/encoding/total
microseconds and request/response bytes. Joins require the owning process session;
ordinals restart across process restarts. No client supplied ID is trusted.
Session commit ordinals are retained for up to 1024 identities; snapshots count
correlation evictions, so evicted long-lived commits cannot be silently joined.
