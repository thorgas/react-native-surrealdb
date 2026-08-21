# `react-native-surrealdb` architecture research

Research date: 2026-07-12
Implementation update: 2026-08-21

## Implementation status

The native-alpha core described by this research is now implemented. The
current package supports embedded memory and experimental SurrealKV, remote
WebSocket connections, authentication, lossless values, pull-based live-query
handles, multicast subscriptions, an optional React live-query hook, and native
transaction handles exposed to JavaScript. Transactions execute each `query()`
immediately under one Rust SDK transaction ID and then commit or cancel once;
callback transactions automatically cancel when the callback throws.

Prebuilt release artifacts now cover iOS arm64 devices, arm64/x86_64
simulators, and Android arm64-v8a, armeabi-v7a, x86_64, and x86. Local native
build validation passes across the isolated React Native 0.82.1, 0.83.10,
0.84.1, 0.85.3, and 0.86.0 hosts. The package remains New-Architecture/Hermes
only. Its first npm alpha passed package verification and a dry run, but npm
rejected the 284.5 MB upload with HTTP 413. The release pipeline now strips
non-runtime symbols after binding generation while preserving all four Android
ABIs and both iOS simulator architectures. The same candidate measures
approximately 258.9 MB compressed and 801.3 MB unpacked, with a 260,000,000-byte
automated ceiling. Native artifact size therefore remains a release and
consumer-experience concern.

The full reactive `surreal-store`, automatic reconnect/re-subscription, a true
local-first synchronization engine, and production durability/migration gates
remain future work.

The recommendation and project comparisons below retain their original
research-date context. Current package behavior is documented in
`packages/react-native-surrealdb/README.md`.

## Executive recommendation

Build this as two packages, in this order:

1. `react-native-surrealdb`: a small, typed React Native binding around the SurrealDB Rust SDK. Version 1 should support embedded `mem://` and `surrealkv://` connections plus remote `ws://`/`wss://` connections with authentication and live queries.
2. `surreal-store`: an optional reactive query/cache package built on top of a narrow database adapter. It should not be part of the native binding and should not attempt to clone all of Legend-State in its first release.

Use **UniFFI + `uniffi-bindgen-react-native` for the first production implementation**. It is available now, handles async Rust, callbacks, owned objects, records, enums, errors, JSI and TurboModule installation, and is already used by Jazz's React Native Rust crate. Keep the Rust interface deliberately small so that the bridge can later move to Nitro's Rust generator or a hand-written C ABI/JSI layer without changing the public TypeScript API.

Do not base the first release on Nitro PR [#1229](https://github.com/mrousavy/nitro/pull/1229). As of the research date, the PR is open, conflicts with `main`, has no approving review, and its end-to-end Jazz validation is still unchecked. It is promising as a future bridge: it generates Rust HybridObject traits, FFI type conversions, Android/iOS build integration, promises, callbacks, errors, and autolinking.

Treat persistent mobile embedding as a technical spike before committing to the product. SurrealDB describes SurrealKV as the storage engine aimed at embedded/local-first workloads, but it remains beta. The spike must prove iOS and Android compilation, lifecycle correctness, durability after forced termination, acceptable binary size/startup/RSS, and migration across SurrealDB versions.

## What the linked projects teach us

### Jazz (`crates/cojson-core-rn`, now `crates/jazz-rn`)

The linked directory has since been renamed to `crates/jazz-rn`. Its current design is the closest template:

- Rust crate outputs `cdylib`, `staticlib`, and `lib`.
- UniFFI proc macros define exported objects, async methods, errors, records, and callback interfaces.
- `uniffi-bindgen-react-native` generates the JSI C++, TurboModule installer, TypeScript bindings, Android CMake/Gradle glue, and iOS Objective-C++/podspec glue.
- iOS ships an XCFramework; Android ships per-ABI native libraries.
- The package uses explicit panic boundaries, owned `Arc` objects, callback interfaces, shutdown state, and a worker/scheduler rather than calling JavaScript from arbitrary Rust threads.
- Complex values are deliberately serialized at the boundary instead of exposing a large Rust domain model directly.

The lesson is not to copy its entire API. Copy its ownership, callback, panic, packaging, and lifecycle patterns.

Source: [Jazz React Native crate](https://github.com/garden-co/jazz/tree/main/crates/jazz-rn) and the originally supplied [historical path](https://github.com/garden-co/jazz/tree/main/crates/cojson-core-rn).

### Nitro Rust PR #1229

The PR adds `{ ios: 'rust', android: 'rust' }`-style Rust targets to Nitrogen's code-generation pipeline and covers primitives, arrays, maps, optionals, records, variants, buffers, callbacks, promises, errors, and HybridObjects. This would be an attractive lower-level, statically generated JSI route once merged and released.

Current blockers:

- open and conflicting with `main`;
- not present in the current stable Nitro platform target type, which still exposes C++, Swift, and Kotlin;
- downstream end-to-end validation is incomplete;
- adopting an unmerged generator fork would make this package responsible for tracking Nitro internals.

Recommendation: keep a small `NativeDatabase` interface that could be regenerated with Nitro later; do not fork Nitro for v1.

Sources: [PR #1229](https://github.com/mrousavy/nitro/pull/1229), [Nitro repository](https://github.com/mrousavy/nitro).

### Turso: thin JSI, smart TypeScript

Turso uses a different but valuable pattern:

```text
React API and async orchestration (TypeScript)
                  ↓
thin C++ JSI HostObjects
                  ↓
stable Rust C API / native database engine
```

Its JSI objects mostly own opaque native pointers and map methods one-for-one. Query orchestration, path normalization, asynchronous I/O, and the ergonomic SDK live in TypeScript. iOS vendors an XCFramework and Android packages ABI-specific shared libraries.

Apply that separation here even with UniFFI: Rust should own the database, runtime, typed value codec, and stream cancellation; TypeScript should own the friendly API, validation, query cache, React hooks, retry policy, and developer diagnostics.

Sources: [Turso article](https://turso.tech/blog/react-native-bindings-for-turso), [React Native binding source](https://github.com/tursodatabase/turso/tree/main/bindings/react-native).

### `uniffi-bindgen-react-native`

The generator currently supports same-thread and async Rust calls, callbacks in both directions, objects by reference, records by value, enums/tagged unions, JSI installation, and a TurboModule. It also supports Node and WebAssembly targets from the same UniFFI annotations, which is useful for unit tests and a future cross-platform API.

Costs to accept:

- generated C++/TS/native project files become part of release engineering;
- Rust/UniFFI/generator versions must remain pinned together;
- callback threading and runtime teardown still require careful application code;
- broad, highly dynamic types are awkward across FFI, so a versioned wire codec is preferable.

Source: [`uniffi-bindgen-react-native`](https://github.com/jhugman/uniffi-bindgen-react-native).

## Proposed architecture

```text
@scope/surreal-store (optional, pure TypeScript)
  query cache • selectors • React hooks • optimistic mutations • sync state
                              │
                              ▼
@scope/react-native-surrealdb (public TypeScript API)
  Database • Query • Transaction • LiveQuery • codecs • errors • lifecycle
                              │
                              ▼
generated UniFFI JSI/TurboModule bindings
                              │
                              ▼
surrealdb-rn-core (Rust)
  Tokio runtime • connection registry • SurrealDB SDK • cancellation • codec
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
       embedded Mem/SurrealKV       remote WS/WSS
```

Implemented monorepo:

```text
crates/
  surrealdb-rn-core/
packages/
  react-native-surrealdb/
    src/
    ios/
    android/
    cpp/
apps/
  harness-shared/
  harness-rn82/
  harness-rn83/
  harness-rn84/
  harness-rn85/
  harness-rn86/
```

The optional `surreal-store` and dedicated Expo example have not been created.
An Expo config plugin can automate native configuration, but the library cannot
run in Expo Go because it contains custom native code. Validate Expo development
builds separately from the bare React Native Harness app.

## Native API: keep it smaller than the SurrealDB Rust SDK

Do not expose every generic Rust SDK builder through UniFFI. A narrow object API is easier to version and test:

```ts
export type ConnectOptions = {
  endpoint: string;
  namespace?: string;
  database?: string;
};

export interface SurrealClient {
  query<T = unknown>(
    surql: string,
    vars?: SurrealVars,
  ): Promise<QueryStatement<T>[]>;
  beginTransaction(): Promise<SurrealTransaction>;
  transaction<T>(
    run: (transaction: SurrealTransaction) => Promise<T>,
  ): Promise<T>;
  live<T = unknown>(surql: string, vars?: SurrealVars): Promise<LiveQuery<T>>;
  use(namespace: string, database: string): Promise<void>;
  close(): Promise<void>;
}

export interface SurrealTransaction {
  query<T = unknown>(
    surql: string,
    vars?: SurrealVars,
  ): Promise<QueryStatement<T>[]>;
  commit(): Promise<void>;
  cancel(): Promise<void>;
}

export interface LiveQuery<T> extends AsyncIterable<LiveNotification<T>> {
  next(): Promise<LiveNotification<T> | undefined>;
  close(): Promise<void>;
}
```

Add convenience CRUD methods in TypeScript by compiling them to parameterized SurrealQL. This prevents duplicating the Rust SDK's generic builder surface and makes remote and embedded adapters behave the same.

### Value transport

Plain JSON is acceptable for a proof of concept but not as the permanent contract. SurrealDB's value system includes `None`, decimal, bytes, datetime, duration, geometry, table, record ID, file, range, regex, set, and UUID in addition to JSON values. A JSON-only boundary silently loses identity or precision.

Define a versioned tagged wire format in Rust and TypeScript, for example:

```ts
type SurrealValue =
  | null
  | boolean
  | string
  | number
  | { $surreal: "none" }
  | { $surreal: "int"; value: string }
  | { $surreal: "decimal"; value: string }
  | { $surreal: "record"; table: string; key: SurrealValue }
  | { $surreal: "datetime"; value: string }
  | { $surreal: "duration"; value: string }
  | { $surreal: "uuid"; value: string }
  | { $surreal: "bytes"; base64: string }
  | SurrealValue[]
  | { [key: string]: SurrealValue };
```

Start with a UTF-8 JSON encoding of that algebra for correctness. Benchmark moving the same schema to a binary buffer only after profiling proves bridge serialization is material. The public API may decode familiar types into wrapper classes (`RecordId`, `Decimal`, etc.) while retaining a lossless raw mode.

### Rust runtime and ownership

- One long-lived Tokio runtime per native module, not one runtime per call.
- One `Arc`-owned database handle per connection.
- One `Arc`-owned transaction handle around the Rust SDK transaction; serialize
  queries on that handle, commit or cancel idempotently, and cancel registered
  open transactions when the database closes.
- No database mutex held while invoking a JavaScript callback.
- Every stream has an explicit cancellation token and is cancelled during connection close/module invalidation.
- Catch Rust panics at every exported sync and async boundary and convert them to typed errors.
- Dispatch callbacks through the generated callback/runtime mechanism; never touch Hermes from an arbitrary Rust task.
- Make `close()` idempotent. Reject new work once closing starts and await/cancel outstanding work with a bounded policy.
- Resolve relative database paths in the native platform layer to app-private writable storage; never accept accidental shared/external Android paths by default.

## Storage scope

### v0 proof of concept

- `kv-mem` only.
- query, variables, namespace/database selection, close.
- purpose: prove Rust → UniFFI → RN builds on iOS simulator/device and Android arm64/x86_64.

### v1 candidate

- `kv-surrealkv` for persistence, behind an explicit `surrealkv://` endpoint.
- SurrealKV is the better mobile candidate than RocksDB because SurrealDB positions it for embedded/local-first workloads with smaller resident memory and simpler in-process behavior. It is still beta, so label the persistent adapter experimental until durability testing passes.
- Do not enable every SurrealDB feature. Disable defaults and opt into the minimum engine/runtime features to control binary size and attack surface.
- `ws://` and `wss://` remote transport using the Rust SDK's WebSocket engine.
- namespace/database selection, token authentication, record and root sign-in/sign-up where supported by the Rust SDK, session invalidation, and reconnect/connection events.
- managed live-query subscriptions with explicit cancellation, automatic re-subscription after reconnect, and deduplication of events around reconnect boundaries.
- feature detection so embedded connections reject unsupported remote-only operations with typed errors.

### later

- RocksDB only if a measured workload needs it and mobile builds/resources are acceptable;
- optional engine/plugin packages if persistent engines make the base binary too large.

Official docs distinguish embedded limitations by SDK/engine and note that live queries generally require WebSockets in client SDKs. Do not promise embedded live queries until the exact Rust local engine behavior is verified by integration tests.

Sources: [Rust embedding](https://surrealdb.com/docs/reference/rust/embedding), [deployment/storage guidance](https://surrealdb.com/docs/build/deployment), [Rust live queries](https://surrealdb.com/docs/reference/rust/concepts/live), [JavaScript live queries](https://surrealdb.com/docs/languages/javascript/concepts/live-queries).

## `surreal-store`: useful, but make it a reactive query layer

The compelling product is not another general-purpose observable implementation. It is a SurrealDB-aware reactive cache with fine-grained React subscriptions.

Recommended v1 concepts:

```ts
const todos = store.query({
  key: ["todos", projectId],
  surql: "SELECT * FROM todo WHERE project = $project ORDER BY created_at",
  vars: { project: projectId },
});

function TodoCount() {
  return useSurrealSelector(todos, (state) => state.data.length);
}

await store.mutate({
  surql: "UPDATE $id MERGE $patch RETURN AFTER",
  vars: { id, patch },
  optimistic: (cache) => cache.patchRecord(id, patch),
});
```

Core behaviors:

- external-store semantics (`subscribe` + `getSnapshot`) compatible with `useSyncExternalStore`;
- selectors with equality functions so components only re-render for selected changes;
- stable query keys and deduplication;
- query states: `idle | loading | ready | refreshing | error`;
- mutation states and optimistic updates with rollback;
- normalized record identity using a lossless `RecordId`, while preserving ordered query results;
- reference-counted activation: fetch/subscribe on first consumer, tear down after the last consumer plus a cache timeout;
- transaction/batch notifications so one database event causes one coherent React update;
- explicit stale/invalidate/refetch operations;
- a driver interface so embedded, remote, mocked, and future sync engines are interchangeable.

Do not implement deep JavaScript proxies, two-way persistence of arbitrary object graphs, computed observables, or a custom React renderer in the first version. Legend-State already solves that general problem. If users want Legend-State, publish a small adapter that exposes a Surreal query as a Legend `synced` source.

Legend-State ideas worth borrowing are lazy activation, observable sync status, pending changes, persistence/sync separation, and fine-grained consumption—not necessarily its proxy API.

Sources: [Legend-State getting started](https://legendapp.com/open-source/state/v3/intro/getting-started/), [Legend-State persist and sync](https://legendapp.com/open-source/state/v3/sync/persist-sync/).

## Remote sync is not automatic

An embedded SurrealKV database plus a remote SurrealDB server is not, by itself, an offline synchronization system. Live queries are notifications, not a durable bidirectional replication protocol. A correct sync layer needs at least:

- durable mutation/outbox IDs and idempotency;
- server cursors/changefeed checkpoints;
- conflict semantics per field or record;
- delete/tombstone policy;
- schema migration/version negotiation;
- retry/backoff and authentication renewal;
- initial snapshot plus incremental catch-up;
- crash recovery during every phase.

Therefore `surreal-store` v1 should support either a local database or a remote database and provide optimistic cache behavior. Market true offline sync only after a separate protocol is designed and tested. Turso can advertise push/pull because the database and cloud protocol provide those primitives; this package does not get them merely by embedding SurrealDB.

## Build and release requirements

Minimum release matrix:

- React Native New Architecture with Hermes;
- iOS device arm64, iOS simulator arm64 and x86_64;
- Android arm64-v8a, armeabi-v7a, x86_64, and x86;
- bare RN example and Expo development-build example;
- Debug and Release builds;
- app restart, Fast Refresh/native module invalidation, background/foreground, forced termination, and low-storage tests.

CI should generate bindings, fail on generated diffs, build all native targets, run Rust unit tests, TypeScript codec parity tests, RN integration tests, and publish size reports. Releases should contain prebuilt native artifacts so consumers do not need a Rust toolchain. Pin the Rust toolchain, SurrealDB crate, UniFFI, generator, NDK, CMake, Xcode, and minimum platform versions.

The implemented compatibility workflow builds shared Android and iOS native
artifacts and exercises React Native 0.82 through 0.86 in separate RNTA hosts.
The native-size workflow builds the Rust library, stock RNTA baseline, and
SurrealDB candidate on separate runners, then compares the downloaded baseline
and candidate APKs. Physical-device lifecycle, forced-termination, migration,
and low-storage coverage are still required before a stable release.

Use [React Native Harness](https://github.com/callstackincubator/react-native-harness) for native-module correctness, lifecycle, cancellation, crash, persistence, and JavaScript/native integration tests on iOS and Android. Harness executes Jest-style tests serially inside the real React Native runtime, supports simulators/emulators and physical devices, detects native crashes, and has an official CI action. Its test bundle requires a Debug application, so Harness timings are useful as a controlled regression signal but must not be presented as production performance. Canonical performance gates should come from a dedicated Release benchmark app on pinned physical devices.

The complete benchmark methodology and proposed CI lanes are in [PERFORMANCE.md](./PERFORMANCE.md).

## Required spikes and decision gates

### Spike A — cross-platform native build (go/no-go)

Build the smallest Rust crate using SurrealDB `kv-mem` through UniFFI and execute `RETURN { ok: true }` on:

- iOS simulator and physical device;
- Android emulator and arm64 physical device.

Record stripped binary contribution, cold initialization time, first-query latency, idle RSS, and peak RSS. Fail the approach if unsupported transitive dependencies or package size cannot be reduced to acceptable product limits.

### Spike B — SurrealKV durability (go/no-go for persistence)

Create/update/query data, kill the app during and after writes, relaunch, and validate recovery. Test app upgrades across two pinned SurrealDB versions and corrupt/truncated files. Run at least a sustained write loop, compaction scenario, and low-disk scenario.

### Spike C — value fidelity

Round-trip every SurrealDB value variant through Rust → wire format → TypeScript → wire format → Rust. Include 64-bit integer boundaries, decimals, nested/composite record IDs, bytes, datetime, duration, geometry, `NONE` vs `NULL`, set, range, regex, and UUID.

### Spike D — lifecycle and subscriptions

Prove cancellation and absence of callbacks after close/invalidate. Repeatedly mount/unmount subscribers, Fast Refresh, background/foreground the app, disconnect/reconnect remote WebSockets, and assert no leaked Rust tasks or native handles.

Implement these native integration cases as Harness suites, with platform-specific files where necessary. Include forced native errors/panics, timeout recovery, repeated database deletion/recreation, concurrent queries, close during an in-flight query, app relaunch, and validation that no callback arrives after cancellation.

## Licensing and naming

SurrealDB core uses the Business Source License, while some SDKs and related crates use Apache-2.0 or MIT. SurrealDB's current FAQ explicitly permits embedding and shipping SurrealDB in customer applications; the restriction is offering a commercial hosted database-as-a-service. Preserve all required notices and obtain a license review before publishing native binaries, especially if the package or a downstream product could be positioned as hosted SurrealDB.

Sources: [SurrealDB license FAQ](https://surrealdb.com/license), [official licensing FAQ](https://support.surrealdb.com/en/articles/11541883-frequently-asked-questions), [SurrealDB repository licensing summary](https://github.com/surrealdb/surrealdb#license).

Search did not reveal an established public package named `react-native-surrealdb` or a Legend-like `surreal-store`, but registry ownership must be checked directly immediately before naming or publishing. Prefer an organization scope (`@your-scope/react-native-surrealdb`, `@your-scope/surreal-store`) until official SurrealDB branding permission is clear.

## Phased delivery

1. **Proof of concept:** `kv-mem`, query, variables, lossless codec, close; one example app; collect size/performance data.
2. **Native alpha:** SurrealKV experimental persistence, remote WebSocket transport, authentication, live-query subscriptions, typed errors, lifecycle hardening, prebuilt artifacts, bare + Expo examples.
3. **SDK beta:** ergonomic TypeScript CRUD/query API, reconnect and live-query recovery, connection events, feature detection, and the full local/remote integration matrix.
4. **Store alpha:** query cache, selectors/hooks, optimistic mutation, local or remote driver; no replication claims.
5. **Production candidate:** durability/migration suite, observability, security review, compatibility policy, benchmark budgets, documented recovery and backup behavior.

## Bottom line

The native binding approach is proven across the supported iOS and Android build
targets, and Jazz's UniFFI-based architecture remains the right bridge model for
this alpha. The largest remaining unknown is not JSI or basic mobile build
compatibility; it is whether experimental SurrealKV has acceptable long-term
resource use, forced-termination durability, recovery, and upgrade behavior in
production. Prove those properties before calling the package stable or
investing in a broad state-management or synchronization API.
