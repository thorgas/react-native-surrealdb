# react-native-surrealdb

Native SurrealDB for React Native, backed by the official Rust SDK and exposed
through generated UniFFI/Hermes JSI bindings.

The current alpha implements:

- embedded in-memory and persistent SurrealKV endpoints;
- remote `ws://` and `wss://` endpoints;
- namespace/database selection and root/database authentication;
- multi-statement SurrealQL queries;
- lossless JavaScript transport for 64-bit integers, decimals, record IDs,
  UUIDs, bytes, sets, `NONE`, and SurrealQL-only values.

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

## Native development

The generated TypeScript/C++/platform glue is checked in. Prebuilt Rust
archives are intentionally ignored because the current unstripped alpha
artifacts are hundreds of megabytes.

```sh
pnpm install
pnpm --filter react-native-surrealdb run ubrn:ios
pnpm --filter react-native-surrealdb run ubrn:android
pnpm --filter react-native-surrealdb run typecheck
pnpm --filter react-native-surrealdb run test
pnpm --filter react-native-surrealdb run build
```

Android generation requires Android NDK `27.1.12297006`, API level 24, and
`cargo-ndk 4.1.2`. Release packaging will generate the complete architecture
matrix and attach the prebuilt artifacts instead of publishing source-only.
