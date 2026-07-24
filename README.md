# React Native SurrealDB

Embedded and remote SurrealDB for React Native, powered by the official
[SurrealDB Rust SDK](https://surrealdb.com/docs/languages/rust) through generated
UniFFI/Hermes JSI bindings.

> [!WARNING]
> This project is an early alpha. Pin exact versions, read the known limitations,
> and test your own persistence and upgrade paths before shipping it.

## Why this exists

SurrealDB's official [Expo guide](https://surrealdb.com/docs/reference/javascript/frameworks/expo)
documents that its JavaScript embedded engines cannot run on React Native:
Hermes does not provide the WebAssembly runtime they need, and the Node.js engine
is a native Node addon. Expo and React Native apps therefore normally connect to
a remote SurrealDB instance.

This project fills that mobile embedding gap differently. It packages the
official Rust SDK as a native React Native module, so SurrealDB runs inside the
iOS or Android app process. The same API can also connect to a remote SurrealDB
server over WebSocket.

I see it as another option in the space normally occupied by SQLite on mobile:
an embedded, local database for React Native apps, but with SurrealQL plus
document, graph, relational, and live-query capabilities. It is an architectural
alternative, not a drop-in replacement—the query language, data model, runtime,
binary footprint, and maturity are different, and each app should benchmark its
own workload.

## What works today

- Embedded in-memory databases and experimental persistent SurrealKV databases
- Remote `ws://` and `wss://` connections
- SurrealQL queries, variables, namespace/database selection, authentication,
  invalidation, and close
- Callback and manually managed native transaction handles with commit,
  cancellation, and automatic rollback when a callback throws
- Cancellable live queries as pull-based async iterators
- Multicast live-query subscriptions and an optional TanStack Query-style React
  hook
- Lossless transport for 64-bit integers, decimals, bytes, UUIDs, record IDs,
  `NONE`, sets, and other SurrealDB-specific values
- React Native's New Architecture and Hermes on iOS and Android
- Prebuilt native artifacts for supported release architectures
- Device-side correctness, persistence, lifecycle, size, and performance testing
- Paired SurrealDB/OP-SQLite workloads adapted from `sqlite-bench`

The current package targets React Native 0.82 or newer, iOS 15.1 or newer, and
Android API 24 or newer. It contains custom native code, so it requires a native
development build and does not run in Expo Go.

## Quick example

The first npm alpha is being prepared. Once it is published:

```sh
pnpm add react-native-surrealdb
cd ios && pod install
```

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

await db.close();
```

See the [package README](./packages/react-native-surrealdb/README.md) for live
queries, the React hook, supported platforms, native development, and the
release process.

## Current limitations

- SurrealKV support remains experimental.
- Automatic WebSocket reconnection, live-query re-subscription, and duplicate
  suppression across reconnects are not implemented yet.
- This is a database binding, not yet a local-first synchronization engine.
  An embedded database and a remote database do not automatically replicate.
- The compatibility and durability matrix is still growing; this is not a
  stable API promise yet.

## Outlook: local-first sync

The longer-term goal is to explore an optional sync layer on top of the native
database binding. That work would remain separate from the core package and
would need to provide a real replication protocol: durable mutation IDs and an
outbox, server checkpoints, initial snapshots and incremental catch-up,
idempotency, conflict and tombstone semantics, schema negotiation, retry and
authentication recovery, and crash-safe resume.

[Syncular](https://syncular.dev/) is the closest current reference: it already
offers an offline-first SQL sync design and a React Native integration. The
first step is to evaluate whether an adapter or collaboration makes more sense
than building another engine.

[Electric's durable transport work](https://github.com/electric-sql/transport)
is also useful research for resumable, addressable streams. Its current packages
target AI SDK transports and durable collaborative sessions, however, so it is
an architectural reference rather than a drop-in database sync engine for this
project.

No sync capability will be advertised until it has explicit authorization,
conflict, offline recovery, migration, and adversarial failure tests.

## Maintenance

This is not a one-off experiment. I use the package in a production app every
day and intend to maintain it for the long term. The app's public App Store
launch is still awaiting approval, but the package is already exercised by a
real product rather than only by the example harness.

## Acknowledgements and AI disclosure

The implementation and research were informed by the
[SurrealDB Rust SDK](https://surrealdb.com/docs/languages/rust),
[`uniffi-bindgen-react-native`](https://github.com/jhugman/uniffi-bindgen-react-native),
[Mozilla UniFFI](https://github.com/mozilla/uniffi-rs),
[Jazz's React Native Rust crate](https://github.com/garden-co/jazz/tree/main/crates/jazz-rn),
[Turso's React Native binding](https://github.com/tursodatabase/turso/tree/main/bindings/react-native),
and [React Native Harness](https://github.com/callstackincubator/react-native-harness).
Thank you to the people maintaining those projects and publishing their work.

In particular, `uniffi-bindgen-react-native` is led by James Hugman, builds on
Mozilla's original UniFFI bindings-generator ecosystem, and credits Filament,
Mozilla, and LiveKit for collaboration or funding. Without that foundation this
project's native bridge would not exist.

The test and benchmark work deserves its own explicit credit:

- [OP-SQLite](https://github.com/OP-Engineering/op-sqlite) informed the mobile
  benchmark workload categories, Release-app execution pattern, cooldowns, and
  the need to measure both query completion and full JSI value materialization.
- [Oscar Franco's `sqlite-bench`](https://github.com/ospfranco/sqlite-bench) was
  the source for the historical paired SQLite/SurrealDB benchmark adaptation.
  The repository history pins the studied revision and records what was adapted
  or excluded; that paired code lives in benchmark branches rather than this
  current checkout.
- [SurrealDB's `crud-bench`](https://github.com/surrealdb/crud-bench) is the
  source for the workload matrix adapted by the current mobile benchmark suite,
  with pinned source links beside the translated cases.
- [React Native Harness](https://github.com/callstackincubator/react-native-harness)
  and [React Native Test App](https://github.com/microsoft/react-native-test-app)
  provide the real React Native runtime, device orchestration, and maintainable
  native hosts used for integration and benchmark runs.

The adapted code and the upstream projects remain under their respective
licenses. See [THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md) for consolidated
project and license attribution, and [PERFORMANCE.md](./PERFORMANCE.md) for
methodology, provenance, limitations, and pinned workload references.

The project was built entirely with AI under my direction, review, and device
validation, mostly using Codex with GPT-5.6 Sol at medium reasoning effort. This
disclosure is about how the code was produced, not a claim that generated code
is automatically correct; the native test matrix and release checks remain the
standard for accepting changes.

## Development

```sh
pnpm install
./scripts/verify-core.sh
pnpm --filter react-native-surrealdb test
pnpm --filter react-native-surrealdb typecheck
```

To exercise the opt-in authenticated WebSocket integration test, start a local
server and run:

```sh
surreal start --no-banner --bind 127.0.0.1:18080 --user root --pass root memory
SURREAL_TEST_WS_ENDPOINT=ws://127.0.0.1:18080 \
  cargo test -p surrealdb-rn-core authenticated_websocket_live_query -- --ignored
```

The Rust toolchain and dependency graph are pinned through
`rust-toolchain.toml` and `Cargo.lock`.

## Design documents

- [Architecture research](./RESEARCH.md)
- [Performance and device-test strategy](./PERFORMANCE.md)
- [Native size and Rust implementation decisions](./NATIVE_SIZE.md)

## License and project status

The original code in this repository is available under the [MIT License](./LICENSE).
Bundled dependencies and generated artifacts remain subject to their respective
licenses and are documented in [third-party notices](./THIRD_PARTY_NOTICES.md).
SurrealDB is a trademark of SurrealDB Ltd.; this independent project is not
affiliated with or endorsed by SurrealDB Ltd.
