# react-native-surrealdb

Native SurrealDB for React Native, backed by the official Rust SDK and exposed
through generated UniFFI/Hermes JSI bindings.

> This package is an early alpha. Pin the exact version and read the limitations
> before using it in an application.

## Install

```sh
pnpm add react-native-surrealdb
cd ios && pod install
```

Requirements:

- React Native 0.82 or newer with the New Architecture and Hermes enabled;
- iOS 15.1 or newer (arm64 devices and Apple Silicon simulators);
- Android API 24 or newer (arm64-v8a devices and x86_64 emulators).

Intel Mac iOS simulators do not work with this alpha because the x86_64 iOS
slice is not included. That slice and the 32-bit Android ABIs were removed to
keep the prebuilt native distribution below npm's effective upload-size
boundary. Android x86_64 emulators remain supported.
Release binaries also use abort-on-panic optimization: an unexpected Rust panic
terminates the application process instead of unwinding across the native
boundary. Expected database and validation failures continue to be returned as
typed JavaScript errors.

## Connect and query

```ts
import { SurrealRecordId, connect } from "react-native-surrealdb";

const db = await connect({
  endpoint: "memory",
  namespace: "app",
  database: "app",
});

const [result] = await db.query("RETURN $person", {
  person: new SurrealRecordId("person:ada"),
});

await db.transaction(async (transaction) => {
  await transaction.query("CREATE person:ada SET name = $name", {
    name: "Ada",
  });
  await transaction.query("CREATE person:lin SET name = $name", {
    name: "Lin",
  });
});

await db.close();
```

`transaction()` begins one native SurrealDB transaction, passes a JavaScript
handle to the callback, and commits once the callback resolves. Each
`transaction.query()` is executed immediately inside that transaction. If the
callback throws, the transaction is cancelled and the original error is
re-thrown. For manual lifecycle control, use `beginTransaction()`, then call
`commit()` or `cancel()` on the returned handle.

```ts
const transaction = await db.beginTransaction();
try {
  await transaction.query("UPDATE account:one SET balance -= $amount", {
    amount: 10,
  });
  await transaction.query("UPDATE account:two SET balance += $amount", {
    amount: 10,
  });
  await transaction.commit();
} catch (error) {
  await transaction.cancel();
  throw error;
}
```

`transaction.query()` calls are sent individually and share the native
transaction ID. For bulk work, `transaction.queryBatch()` executes independently
parameterized queries in one asynchronous native call and preserves every
result. `transaction.executeBatch()` is the lower-overhead write variant: it
repeats one parameterized query, discards `RETURN NONE` results, and returns the
executed count.

```ts
await db.transaction((transaction) =>
  transaction.executeBatch(
    "CREATE person CONTENT { name: $name } RETURN NONE",
    [{ name: "Ada" }, { name: "Lin" }],
  ),
);
```

Await transaction operations in sequence, and do not reuse a handle after
`commit()` or `cancel()`.

The alpha supports embedded `memory` and experimental `surrealkv://...`
endpoints, plus remote `ws://` and `wss://` endpoints. Remote connections can
select a namespace/database and authenticate with root or database credentials.

## Live queries

The low-level API is a pull-based async iterator with natural backpressure:

```ts
const events = await db.live<{ text: string }>(
  "LIVE SELECT * FROM message WHERE room = $room",
  { room: "engineering" },
);

for await (const notification of events) {
  console.log(notification.action, notification.value);
}
```

Use a multicast subscription when multiple consumers need the same native
stream or when integrating with an observable UI:

```ts
const messages = await db.subscribe<{ text: string }>(
  "LIVE SELECT * FROM message WHERE room = $room",
  { room: "engineering" },
);

const stopListening = messages.onNotification((notification) => {
  console.log(notification.action, notification.value);
});

stopListening();
await messages.close();
```

`LiveSubscription.subscribe()` and `getSnapshot()` implement the external-store
contract used by React. The optional React entry point wraps that contract in a
TanStack Query-style result:

```tsx
import { useLiveQuery } from "react-native-surrealdb/react";

function LatestMessage({ db, room }: Props) {
  const message = useLiveQuery<{ text: string }>({
    client: db,
    queryKey: ["messages", room],
    surql: "LIVE SELECT * FROM message WHERE room = $room",
    variables: { room },
  });

  if (message.isPending) return <ActivityIndicator />;
  if (message.isError) return <Text>{String(message.error)}</Text>;
  return <Text>{message.data?.text ?? "Waiting for a message"}</Text>;
}
```

`data` is the latest notification value; `notification` also exposes its
`action` and query ID. The hook closes its server-side live query on unmount or
when its query key changes. `queryKey` follows TanStack Query semantics: include
every variable used by the query. If omitted, the query and encoded variables
form the key automatically.

Closing is idempotent and cancels the server-side live query. Automatic
reconnection, re-subscription, and duplicate-event suppression are not yet
implemented; applications using remote live queries must currently handle a
dropped connection.

Closing the database also closes its live queries and cancels its open
transactions.

## Cancellation and limitations

Async operations accept `{ signal }` as their final options argument. The
package requires Hermes and React Native's New Architecture and cannot run in
Expo Go. Persistent SurrealKV support is experimental. Transaction callbacks
must finish before the database is closed, and remote connection recovery is
currently application-managed.

### Experimental sync prototype (alpha.2)

There is no destructive local-versus-sync database mode. `connect()` opens the
same embedded database either way. For a **sync-managed local-first** feature,
open `openExperimentalSync()` once per durable client identity and enqueue its
writes even while offline; local optimistic data and the outbox persist. At the
application composition root, add the injected HTTP adapter and scheduler only
when authenticated sync is enabled. Stopping the scheduler pauses transport,
not local reads or durable intent; restarting it resumes from the saved outbox
and checkpoint. Keep the database/sync handle scoped to the same account,
partition, and client ID, and close them on account switch or sign-out without
deleting unresolved data.

This is **not** an automatic upgrade path from local-only direct SurrealQL
writes. Those writes do not create protocol commits or enter the server log, so
later attaching a sync adapter will not upload them. An application must
bootstrap or migrate that existing data explicitly before enabling sync, and
must define how authorization, derived rows, and conflicts work. Likewise,
pausing transport does not turn sync-managed writes into direct database
writes. The package intentionally has no global `syncEnabled` switch that
could hide these semantics.

### Typed application commands and diagnostics

`ExperimentalSyncApplicationAdapter<Command>` maps domain commands to the existing
protocol with an injected `encodeCommand`, durable `sync` port, and optional
scheduler wakeup. It does not create another mutation queue. `commit(command)`
returns `{ clientCommitId, milestone: "local_durable", status }` only after native
enqueue succeeds. This receipt does **not** mean the authority accepted the edit
or another device received it. Keep validation, identity generation and domain
side effects in the application mapping.

```ts
const observed = new ExperimentalSyncObservedClient(sync, {
  logger: (metadata) => diagnostics.write(metadata),
});
const commands = new ExperimentalSyncApplicationAdapter<RemoveItem>({
  sync: observed,
  encodeCommand: (command) => ({
    identity: {
      clientCommitId: command.commitId,
      fingerprint: "computed-by-native",
    },
    operations: [{
      kind: "delete",
      record_id: command.recordId,
      base_version: command.version,
    }],
  }),
  requestSync: () => scheduler.requestSync("local_mutation"),
});
const receipt = await commands.commit(command);
```

Route persisted push responses through the **same** observed port's
`recordPushResponseProtocolJson()` to receive `authority_accepted`,
`authority_conflict`, or `authority_rejected` events via `subscribeEvents()`.
These events carry commit identity and metadata, never row contents. They are
session-only observations, not a durable receipt registry: reopening does not
replay old events, and bypassing the decorator does not emit them. Native
persistence remains the source of truth. Unsubscribe when disposing the scope.

The injected logger receives only milestone, revision and queue/outcome/conflict
counts; it receives no commit IDs, records, tokens, checkpoints or error messages.
It runs asynchronously, isolates thrown/rejected failures, and allows at most one
in-flight invocation. A hung logger drops later diagnostics without delaying
writes. Event listeners are synchronous advisory callbacks; keep them short.
`diagnosticSnapshot()` returns a defensive metadata copy from one native status
read, plus logger `inFlight`, `droppedCount` and `errorCount` counters for this
observation scope. Counters are advisory and reset when the decorator is recreated. It does not
promise a consistent batch of application rows plus revision; that requires a
separate durable native boundary and is intentionally not claimed here.

These adapters call existing alpha.3 facade methods only and introduce no native
ABI, database migration, provider, transport, or authentication changes.

This alpha exposes `openExperimentalSync()` only to exercise the native protocol
workflow. The returned transport-free client can enqueue an atomic local commit, inspect its
durable pending/conflict queues, and apply HTTP push/pull responses supplied by the application.
Optimistic records and normalized sync metadata/outbox/outcome rows commit together in embedded
SurrealDB. Existing v1 blob state migrates atomically on open and fails closed on downgrade. The optional
`ExperimentalSyncHttpAdapter` adds serialized, explicit `push()`, `pull()`, and `syncOnce()` calls.
The protocol's `record_id` is the projected SurrealDB row identity. When an application also puts
a logical string `id` inside an upsert value, SurrealDB's ordinary row `id` becomes a
`SurrealRecordId` (for example, `item:stable-1`), not that string. Do not deserialize queried
projection rows directly into the original payload type. Keep logical-ID conversion and domain
validation in the application adapter; the generic sync runtime does not know an app's ID policy.
Applications must inject their access-token provider, wire codec, and `fetch`. The adapter reads the
last complete checkpoint from native durable client state; applying a pull atomically persists its
records, cursor, scope snapshot, and opaque checkpoint before the next request can observe it.
Requests and responses are bounded to 4 MiB by default, the complete fetch/body/decode operation is
timed out (including token acquisition and codec work), content types are checked, and bodies
without a stream or declared length fail closed. HTTPS is required; development HTTP is accepted
only for an explicitly enabled loopback URL (including Android emulator host alias `10.0.2.2`). The token provider receives the bounded call's abort
signal and should stop its own work when aborted; the adapter still releases its serialized queue if
the provider ignores that signal.

The native codec constructor is the only complete wire codec on this branch:

```ts
const sync = await database.openExperimentalSync({
  partitionId: "workspace",
  clientId: "device-1",
  requestedScope: "all",
  subscriptionRevision: 1n,
});

// Durable conflicts stay recoverable until the application makes an explicit choice.
await sync.resolveConflictKeepServer("conflicted-commit");
await sync.resolveConflictKeepLocal("conflicted-commit", "fresh-retry-commit");
await sync.resolveConflictMerge("conflicted-commit", {
  identity: { clientCommitId: "fresh-merge-commit", fingerprint: "computed-by-native" },
  operations: [{
    kind: "upsert",
    record_id: "recipe:42",
    base_version: "absent", // Must match the authoritative state in the conflict.
    value: { title: "Explicitly merged recipe" },
    reference: null,
  }],
});

const transport = new ExperimentalSyncHttpAdapter({
  sync,
  baseUrl: "https://sync.example.invalid",
  partitionId: "workspace",
  clientId: "device-1",
  requestedScope: "all",
  subscriptionRevision: 1n,
  accessToken: async () => applicationToken,
  fetch,
  codec: createExperimentalCanonicalCborSyncHttpCodec(),
});

const scheduler = new ExperimentalSyncScheduler({
  adapter: transport,
  connectivity: applicationConnectivity,
  invalidations: new ExperimentalSyncWebSocketHints({
    url: applicationShortLivedHintUrl,
  }),
});

const lifecycle = new ExperimentalSyncLifecycleCoordinator({
  scheduler,
  lifecycle: {
    current: () =>
      AppState.currentState === "active" ||
      AppState.currentState === "inactive" ||
      AppState.currentState === "background"
        ? AppState.currentState
        : "unknown",
    subscribe: (listener) => {
      const subscription = AppState.addEventListener("change", listener);
      return () => subscription.remove();
    },
  },
  refreshAuthentication: async (_status, { signal }) => {
    applicationToken = await refreshApplicationToken({ signal });
    return true; // Return false when credentials did not change.
  },
});

lifecycle.start();
// Stop on application teardown; background transitions already stop scheduling.
lifecycle.stop();
```

The example URL is intentionally non-routable: this package does not supply or deploy the authority.

Keep-server durably accepts the authority value. Keep-local replays the complete retained atomic
batch under a fresh identity after rebasing its conflicting operation. Merge queues an explicit
application-provided batch under the normal compare-and-set path. The original conflict and local
intent remain in durable history; only unresolved conflicts appear in `conflicts()` and
`conflictCount`. This mechanism deliberately does not choose an application merge policy.

An experimental, opt-in `preferServerV1` disposition is available for applications that want
one narrow automatic choice. Pass `conflictPolicy: "preferServerV1"` to
`ExperimentalSyncHttpAdapter`, or call `sync.recordPushResponseWithPolicy(response,
"preferServerV1")` when supplying responses yourself. The default methods remain manual.
Native Rust records the policy decision atomically with a durable conflict only for a single
stale upsert against a present canonical record. Deletes, tombstones, absent records, and
multi-operation commits stay unresolved. The original local commit remains in durable history;
no stale write is accepted by the authority. This is not general last-write-wins or a safe
default for purchases and other cross-record application effects. Select policy at the app
composition root; changing it cannot rewrite an already-durable outcome. Pull after a policy
disposition to materialize the canonical winner: closing the conflict itself does not replace
confirmed rows.

The payload API uses this package's tagged lossless value bridge for JavaScript `bigint`, bytes,
`NONE`, record links, and finite fractional JavaScript numbers. Native Rust ignores any caller-supplied fingerprint, validates record
values against the bounded canonical protocol safe subset, and emits the content-bound SHA-256
fingerprint in the durable pending commit. NaN, infinities, decimals, UUID/range record keys, dates,
sets, and other undecided protocol kinds fail closed. Integers and fractions are different typed
values, and this binary64 support does not provide decimal money arithmetic.
`createExperimentalCanonicalCborSyncHttpCodec()`
uses the copied protocol crate's bounded, deterministic `surrealdb-sync/1` request/response codec;
its finite-value profile matches private branch commit
`d26e07b` (`feat/application-shaped-runtime-v1`). The exported
`experimentalJsonSyncHttpCodec` remains test/prototype-only and throws when a pending `bigint` lies
outside JavaScript's safe integer range. The optional scheduler coalesces triggers, performs only one
cycle at a time, pauses offline, and applies bounded full-jitter retry to transient failures. Its
timers are advisory and non-durable: the native outbox and checkpoint remain the recovery source.
The lifecycle coordinator fails closed until the application is active, stops and aborts scheduling
in background/inactive states, and permits one application-owned refresh for each `401`/`403`
challenge. A failed or declined refresh stays halted until `retryAuthentication()` or a later
lifecycle restart; stale refresh completion cannot resume a backgrounded scheduler. The package
does not own login state or import React Native `AppState`, so applications inject that small
adapter explicitly.
WebSocket hints only wake a pull, use a fresh application-supplied URL/ticket for every connection,
and never define durability or ordering; a 60-second periodic pull is the default fallback.
Production hints require WSS. The client closes an oversized frame after receipt, but the authority
or proxy must enforce its own pre-allocation frame limit. Authority deployment is absent. Do not
use this API for production data yet; see the repository
[sync handoff](../../docs/SYNC_RUNTIME_HANDOFF.md) for the remaining gates.

The private candidate also exposes [atomic local read snapshots and revision batches](../../docs/SYNC_READ_CONTRACT.md), with explicit coverage, bounded replay and scope quarantine. These additive native methods require matching rebuilt binaries; they are not an OTA overlay for older runtimes.

The conflict-resolution facade is covered by native SurrealKV reopen tests, package tests, and the
opt-in two-replica local-authority Harness flow on iOS and Android. These are local prototype tests,
not deployed authentication or production-retention evidence.

Maintainers can measure the local durable path without a server using
`pnpm --filter surrealdb-harness-rn86 run benchmark:ios:sync` or
`benchmark:android:sync` from the repository root. This is a Debug Harness
diagnostic against a minimal persistent SQLite outbox lower bound, not a
production or end-to-end replication benchmark. See
[`PERFORMANCE.md`](../../PERFORMANCE.md) for methodology and current results.

Native conformance tests consume exact accepted, pull-batch, and reset CBOR emitted by the private
SurrealDB authority adapter. The pull/reset test applies the messages, closes and drops the embedded
database handle, reopens the same SurrealKV path, and verifies the checkpoint, confirmed record,
pending outbox, and optimistic replay. This is a local storage/codec proof, not a deployed service.

## Value transport

The JavaScript/Rust boundary preserves 64-bit integers, decimals, record IDs,
UUIDs, bytes, sets, `NONE`, and other SurrealQL-only values rather than losing
them through ordinary JSON conversion.

## Benchmark diagnostics

`queryProfiled()` is an opt-in benchmark variant available on database and
transaction handles. It returns the ordinary query results plus timing for
JavaScript input encoding, embedded SDK execution, Rust output encoding,
binding/scheduling residual, and JavaScript output decoding.
`benchmarkNativeBoundary()` measures an async UniFFI/JSI no-op round trip.
These APIs are intended for controlled benchmarks rather than production
telemetry; ordinary `query()` calls contain no diagnostic clocks or additional
return fields. Normal result transport streams the lossless wire format
directly from SurrealDB values and decodes parsed arrays/objects in place,
avoiding the previous pair of full intermediate value trees.

## Native development

The generated TypeScript/C++/platform glue is checked in. Release archives are
generated locally and ignored by Git because they are large.

```sh
pnpm install
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter react-native-surrealdb run release:check
pnpm --filter react-native-surrealdb pack
```

Android generation requires Android NDK `27.1.12297006`, API level 24, and
`cargo-ndk 4.1.2`. See [RELEASING.md](./RELEASING.md) for the complete release
procedure.

## Acknowledgements

The native bridge is generated by
[`uniffi-bindgen-react-native`](https://github.com/jhugman/uniffi-bindgen-react-native),
led by James Hugman and built on
[Mozilla UniFFI](https://github.com/mozilla/uniffi-rs). See
[THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md) for project, contributor, and
license details included with the published package.

### Opaque bootstrap v1 (additive source ABI)

`ExperimentalSyncClient.bootstrapCapabilities()` and
`bindBootstrapSession(context, options)` require the matching regenerated native
ABI. Missing capabilities fail before storage, with no v1 fallback. Context must
come independently from authenticated transport, including server authorization
revision; neither the SDK nor an unsigned manifest authenticates that input.
Factory binding is read-only and process-local. A successful `begin(start,
manifest)` establishes the durable context floor. A terminal stage may advance
its authorization revision under the same immutable owner/generation; active
stages remain exact. Native completion retains canonical history/scope guards.

Session methods `begin`, `page`, `progress`, `cancel` and `complete` accept opaque
canonical bytes. `ArrayBuffer` and typed views are copied using their exact byte
range, with the 4 MiB frame limit checked before native dispatch. Counters remain
`bigint`. Stage objects are opaque and scoped to their JS session; reacquire them
with read-only `progress(manifest)` after reopen. Missing is explicit; it does
not create a journal. Completion distinguishes `applied` and
`historical_success`. Closing a JS session never cancels durable data.

UTC and the 30-second verification deadline are native-owned. The app must obtain
fresh authenticated completion after restart. `AbortSignal` is forwarded, but a
failed/cancelled mutating call must not be treated as a known rollback. Native
owned-worker admission retains mutation settlement/publication after a caller or
UniFFI waiter is cancelled; returned unknown storage outcomes poison the database
wrapper lifetime and ordinary sync reopen cannot clear that poison. See
[Native mutation settlement](../../docs/NATIVE_MUTATION_SETTLEMENT.md) for the
exact recovery boundary and host FFI proof. This is not generated JSI/mobile JS
acceptance. Do not activate this surface until matching iOS/Android builds,
device acceptance and performance gates pass. UniFFI allocates received buffers
before Rust can check them; the JS check protects normal callers, not hostile JS.

Binding provenance for the initial source generation: Rust `498ecc8`, existing
`uniffi-bindgen-react-native` `0.31.0-3`, default host features. Reproduce from the
workspace root with `CARGO_BUILD_JOBS=2 cargo rustc -p surrealdb-rn-core --lib
--crate-type cdylib`, then
`packages/react-native-surrealdb/node_modules/.bin/ubrn generate jsi bindings
--library target/debug/libsurrealdb_rn_core.dylib --ts-dir
packages/react-native-surrealdb/src/generated --cpp-dir
packages/react-native-surrealdb/cpp/generated`. From the package directory run
`node_modules/.bin/ubrn generate jsi turbo-module --config ubrn.config.yaml
surrealdb_rn_core` and the existing `format:generated` script. Generation includes
all current Rust metadata, including the previously dormant inspector ABI. Host
export/checksum consistency is not JSI method or device acceptance.

Compatibility boundary: a whole old native binary may fail the generated module's
initial checksum/contract checks during import, before a typed bootstrap facade
can exist. That gate performs no journal/storage operation and remains unchanged.
`UnsupportedAbi` is the method-level error once the binding module initializes
but bootstrap methods/capabilities are absent or incompatible. It is not a promise
about the error type from importing a mismatched whole binary. Native runtime
fingerprints and matching rebuilt binaries must prevent shipping this bundle to
an older binary; no checksum bypass or fallback is provided.

The bindings were regenerated again from local Rust commit
`9f7880ffa6d0f41b7e757a9aacdf486e62aee1e9` after native mutation settlement.
Arc receiver changes preserve method parameters; generated checksums and a host
symbol handshake are checked separately from runtime/device execution. The
existing whole-binary generated initialization gate remains unchanged.

Bootstrap failures expose only fixed error codes/messages. Classification accepts
allowlisted own data fields; it never evaluates an error's tag getter or inherited
tag, and catches proxy inspection failures. Unknown mutating failures remain
`RecoveryRequired`, unknown read failures remain `Storage`, and raw error causes
or messages are not retained.
