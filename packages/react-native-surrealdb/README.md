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

### Unreleased sync prototype

This development branch exposes `openExperimentalSync()` only to exercise the native protocol
workflow. The returned transport-free client can enqueue an atomic local commit, inspect its
durable pending/conflict queues, and apply HTTP push/pull responses supplied by the application.
Optimistic records and sync metadata commit together in embedded SurrealDB. The optional
`ExperimentalSyncHttpAdapter` adds serialized, explicit `push()`, `pull()`, and `syncOnce()` calls.
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

scheduler.start();
// Stop on application teardown; durable outbox/checkpoint state remains native.
scheduler.stop();
```

The example URL is intentionally non-routable: this package does not supply or deploy the authority.

The payload API uses this package's tagged lossless value bridge for JavaScript `bigint`, bytes,
`NONE`, and record links. Native Rust ignores any caller-supplied fingerprint, validates record
values against the bounded canonical protocol safe subset, and emits the content-bound SHA-256
fingerprint in the durable pending commit. Floats, decimals, UUID/range record keys, dates, sets,
and other undecided protocol kinds fail closed. `createExperimentalCanonicalCborSyncHttpCodec()`
uses the copied protocol crate's bounded, deterministic `surrealdb-sync/1` request/response codec;
its golden messages match private commit `2032066722ccb0202f2f8481f30fd5c70f4d681e`. The exported
`experimentalJsonSyncHttpCodec` remains test/prototype-only and throws when a pending `bigint` lies
outside JavaScript's safe integer range. The optional scheduler coalesces triggers, performs only one
cycle at a time, pauses offline, and applies bounded full-jitter retry to transient failures. Its
timers are advisory and non-durable: the native outbox and checkpoint remain the recovery source.
WebSocket hints only wake a pull, use a fresh application-supplied URL/ticket for every connection,
and never define durability or ordering; a 60-second periodic pull is the default fallback.
Production hints require WSS. The client closes an oversized frame after receipt, but the authority
or proxy must enforce its own pre-allocation frame limit. Authority deployment is absent. Do not
ship or advertise this API; see the repository
[sync handoff](../../docs/SYNC_RUNTIME_HANDOFF.md) for the remaining gates.

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
