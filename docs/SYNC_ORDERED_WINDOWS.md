# Private ordered subscription experiment

This source experiment requires matching rebuilt native bindings and binaries.
It has its own `surrealdb-subscription/1` CBOR protocol and
`POST /v1/sync/subscriptions` route. The existing v1 full-scope read capability
continues to report `predicateSubscriptions: false`.

An application selects a trusted server definition ID, named typed equality
parameters, a generation and a bounded limit. The server fixes the fields,
ordering and authorization; clients cannot send SQL, field paths, arbitrary
predicates or sort expressions. Authorization is applied before filtering,
ordering and limiting. A changed boundary row can require a complete window
refill. A membership leave does not mean record deletion.

## Explicit lifecycle

Create a window client with `sync.createWindowClient()`. Call `start(request)`
to persist its identity and generation before making an HTTP request. Structural
u64 counters are canonical decimal strings, and signed query integers use
`{kind: "int", value: "-9223372036854775808"}`. Boolean and text query values
keep their normal types. Record payloads retain the SDK's existing lossless
SurrealDB value bridge; query metadata is never interpreted as a user payload.

`request(id)` returns the durable Start, fresh Resume or Cancel request. Pass
it to `experimentalCanonicalSubscriptionHttpTransport({...}).exchange(request)`.
For normal applications, `createExperimentalCanonicalSubscriptionHttpTransport`
supplies those native functions. The lower-level transport uses the generated native `encodeSubscriptionRequestJson` and
`decodeSubscriptionResponse` functions supplied in its `native` option. Supply
an access-token callback; use HTTPS, or explicitly permit a local test endpoint.
It performs one exchange, with a deadline and 256-KiB request/response limits.
It neither retries failures nor installs a response automatically. Pass successful
Start/Resume responses to `applyResponse(responseJson)`, which persists and
validates owner, generation and authorization before installing any visible view.

A local `cancel(id, generation)` first persists a cancellation barrier and
withdraws membership. Its durable Cancel request can be sent explicitly; a server
cancellation acknowledgement does not need to be installed into the already
cancelled local view. Reuse requires a higher generation. Local cancellation
remains available after scope quarantine; it cannot restore reads or transmit
retained edits. Server 410 requires an explicit restart of an active generation;
a locally cancelled generation cannot be restarted.

## Committed observations

`snapshot(id)` returns one atomic frame: installed ordered rows and metadata,
pending record IDs outside the window (null when their full-scope enumeration
is not known), the durable transport request,
cancellation barrier and cursor. An absent snapshot with a Start request is
awaiting coverage. An absent snapshot with a Cancel request is cancelled.
The cursor's process identity is per native handle; its revision is the durable
root revision. Metadata revision matches that cursor, while `viewRevision` and
`frontier` identify the authorized server window. A matching checkpoint alone does not establish full-scope reset coverage;
`pendingOutsideWindow: null` must never be displayed as zero pending edits.
This is as-of coverage, not a
claim about the current server while offline.

`createStore(id)` exposes `subscribe(listener)` and `getSnapshot()`. It polls
committed native frames, performs no HTTP and owns no durable outbox. Metadata,
ordered rows and pending references publish together. `loading` withdraws a
previous frame while a local mutation is re-read; ordinary read errors retain
the dated frame. Scope rejection and close clear it, stop polling and discard
late completions. Observer exceptions are isolated and counted. Explicit
`refresh()` restarts a stopped read pump without retrying any network mutation.
Close the store when its consumer leaves; closing it does not cancel the durable
subscription or close the shared engine.

Native replay is bounded to128 batches/1MiB per query and16MiB aggregate. Evicted
or cross-handle cursors return an atomic fresh frame. Read output is bounded to
16MiB. Definitions have at most8 parameters and32 rows per window; native durable
membership and cancellation histories are independently bounded. Pending intent,
conflict outcomes and v1 full-scope coverage remain engine-owned and separate.
Overlapping query windows share durable record versions internally; those cache
keys are not part of the public read view. Cancellation never purges records still
referenced by another window, v1 state, pending work or conflict audit.

## Verification boundaries

`test/sync-window.test.ts` covers lossless counters and payloads, cancellation
barriers, complete-batch preflight, cursor identity, HTTP bounds/status failures,
late completion and reentrant observation withdrawal. The SDK's full suite also
runs its existing loopback transport test. Canonical authority and native storage
fixtures separately prove authorization, window refill, durable installation,
cancellation races and recovery. Host checks do not establish mobile ABI matching,
preserved-container upgrades, two-client app behavior or production readiness.
This private source has not been published or enabled in production.

## Native storage compatibility

Ordinary journals without subscription state keep normalized storage format2.
Once a durable subscription or cancellation exists, native persistence uses
format3 for metadata and components. The new reader accepts both versions; old
format2-only readers refuse format3 before loading or saving it. An attempted
save that drops existing subscription state is rejected. This protects query
cancellation and remembered authorization during rollback: install an older binary
only with a compatible separate journal, or retain the newer binary to recover
its journal. Never delete the existing database to make an older binary open it.
This storage guard does not change the existing full-scope wire protocol.
