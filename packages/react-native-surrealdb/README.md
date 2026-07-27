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
- iOS 15.1 or newer (arm64 devices and arm64/x86_64 simulators);
- Android API 24 or newer (arm64-v8a, armeabi-v7a, x86_64, and x86).

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

```ts
const subscription = await db.live<{ text: string }>(
  "LIVE SELECT * FROM message",
);

const notification = await subscription.next();
if (notification) {
  console.log(notification.action, notification.value);
}

await subscription.close();
```

`LiveQuery` is also an `AsyncIterable`, so it can be consumed with `for await`.
Closing is idempotent and cancels the server-side live query. Closing the
database closes its live queries and cancels its open transactions. Automatic
reconnection, re-subscription, and duplicate-event suppression are not yet
implemented; applications using remote live queries must currently handle a
dropped connection.

## Cancellation and limitations

Async operations accept `{ signal }` as their final options argument. The
package requires Hermes and React Native's New Architecture and cannot run in
Expo Go. Persistent SurrealKV support is experimental. Transaction callbacks
must finish before the database is closed, and remote connection recovery is
currently application-managed.

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
