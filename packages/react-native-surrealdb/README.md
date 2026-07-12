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
import { SurrealRecordId, connect } from 'react-native-surrealdb';

const db = await connect({
  endpoint: 'memory',
  namespace: 'app',
  database: 'app',
});

const [result] = await db.query('RETURN $person', {
  person: new SurrealRecordId('person:ada'),
});

await db.close();
```

The alpha supports embedded `memory` and experimental `surrealkv://...`
endpoints, plus remote `ws://` and `wss://` endpoints. Remote connections can
select a namespace/database and authenticate with root or database credentials.

## Live queries

```ts
const subscription = await db.live(
  'LIVE SELECT * FROM message',
  (notification) => {
    console.log(notification.action, notification.result);
  },
);

await subscription.unsubscribe();
```

Unsubscribing is idempotent and cancels the server-side live query. Automatic
reconnection, re-subscription, and duplicate-event suppression are not yet
implemented; applications using remote live queries must currently handle a
dropped connection.

## Value transport

The JavaScript/Rust boundary preserves 64-bit integers, decimals, record IDs,
UUIDs, bytes, sets, `NONE`, and other SurrealQL-only values rather than losing
them through ordinary JSON conversion.

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
