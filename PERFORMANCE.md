# Performance monitoring and native-device test strategy

Research date: 2026-07-12

## Recommendation

Use two complementary systems:

1. **React Native Test App + React Native Harness** for a low-maintenance native host, repeatable native integration tests, and a lightweight performance smoke signal in Debug builds.
2. **A dedicated Release benchmark app** for canonical latency, throughput, memory, startup, durability, and binary-size results.

Do not use screenshots or a single elapsed-time measurement as the performance history. Each run should emit structured JSON containing raw samples and environment metadata, upload it as a CI artifact, compare it with a compatible baseline, and publish a human-readable summary.

## What OP-SQLite currently does

The current [`OP-Engineering/op-sqlite`](https://github.com/OP-Engineering/op-sqlite) repository has three distinct performance-related ideas.

### 1. An example-app microbenchmark

`example/src/performance_test.ts` uses `performance.now()` around fixed loops. Its workloads include:

- 1,000 synchronous inserts;
- 100,000 repeated point selects against a 1,000-row, 14-column table;
- 1,000 full-table selects after inserting 1,000 rows;
- comparison of individual inserts with inserts inside a transaction;
- result access/materialization comparisons shown in the repository benchmark images.

The public images compare OP-SQLite, Nitro SQLite, and Expo SQLite on iOS and Android, with 1,000 rows and a stated 2.5-second cooldown between tests. They explicitly highlight that HostObject/HybridObject results can defer JSI conversion cost until properties are accessed. This is an important lesson for our codec: measure both `query()` completion and full traversal of every returned value.

### 2. Release app testing on simulators/emulators

OP-SQLite's CI launches Release builds, runs its JavaScript test suite inside the app, watches iOS/Android logs for pass/fail markers, and captures diagnostics when Android progress stalls. That is a sound pattern for native correctness and more representative than Node-based tests.

### 3. Compile-time performance mode

Its optional `performanceMode` applies SQLite compile flags that trade features or diagnostics for speed. This is not a benchmark framework, but it demonstrates why benchmark records must include the exact native feature/build configuration. A result from one feature set must never be compared silently with another.

## Limits of the OP-SQLite benchmark as a monitor

The current repository does **not** provide an automated performance regression system:

- benchmark results are screenshots rather than raw machine-readable samples;
- the comparison implementation for all pictured libraries is not present in the current benchmark source;
- the performance function is launched manually from the example UI after a timeout;
- a single elapsed duration is reported, without warm-up samples, distribution, uncertainty, or outlier handling;
- setup and measured work are mixed in at least one workload;
- current PR CI runs correctness tests but does not upload performance history or fail against budgets;
- simulator/emulator and physical-device results are not identified in the screenshots with enough hardware/build metadata to reproduce them.

We should reuse the workload categories, Release-app execution, cooldown, and conversion-cost awareness—not the reporting methodology.

## React Native Harness: where it fits

[`callstackincubator/react-native-harness`](https://github.com/callstackincubator/react-native-harness) replaces the app's Metro entry bundle with a test runner while leaving the native binary intact. The Node CLI controls a simulator, emulator, or physical device; tests execute serially in the real React Native JavaScript runtime and communicate results over a WebSocket.

Use [`microsoft/react-native-test-app`](https://github.com/microsoft/react-native-test-app) for that native binary. RNTA keeps the Android application and Xcode project package-owned and generates their integration from `app.json`, while Harness remains responsible for device orchestration and on-device tests.

Useful capabilities for this package:

- Jest-style `describe`, `it`, hooks, and async expectations;
- real TurboModule/JSI/native-library access;
- iOS simulator/device and Android emulator/device runners;
- platform-specific `*.ios.harness.ts` and `*.android.harness.ts` suites;
- per-test timeouts, environment reset between test files, native crash detection, automatic relaunch, logs, and crash artifacts;
- official GitHub Action and cached app/emulator/Metro artifacts;
- experimental iOS-simulator native coverage for pod targets.

Important limitation: Harness loads tests through Metro into a **Debug app** and does not run a normal bundled Release app. Debug JavaScript, assertions, Metro, instrumentation, and device orchestration make it unsuitable as the sole source of publishable performance claims. Use it for correctness and relative smoke tests; use the Release benchmark app for performance gates.

## Benchmark architecture

```text
benchmarks/specs/*.ts
        │ shared workload definitions
        ├─────────────────────────────┐
        ▼                             ▼
Harness Debug suites            Release benchmark app
PR correctness + smoke          canonical measurements
emulator/simulator              pinned physical devices
        │                             │
        └──────────────┬──────────────┘
                       ▼
             benchmark-result.v1.json
                       │
          compare → artifact → trend store
```

Keep workload definitions independent of the UI and expose two runners. A benchmark must accept a driver so the same work can compare:

- direct Rust core benchmark where applicable;
- React Native SurrealDB over UniFFI/JSI;
- v1 remote WebSocket adapter;
- another React Native database only for carefully equivalent workloads.

## Workload matrix

### Bridge and codec

- no-op/health call latency;
- `RETURN 1` sequential latency;
- N concurrent `RETURN 1` calls at concurrency 1, 4, and 16;
- variables crossing JS → Rust at 1 B, 1 KiB, 64 KiB, and 1 MiB;
- result crossing Rust → JS at 1, 10, 100, 1,000, and 10,000 records;
- narrow scalar records versus deeply nested objects, arrays, bytes, record IDs, decimals, datetime, geometry, and mixed values;
- query completion without traversal, then complete traversal/decoding of every property;
- tagged JSON codec versus any future binary codec.

The split between completion and full traversal is mandatory. Lazy native objects can make `query()` appear fast while charging conversion cost during UI access.

### Database operations

- cold open and warm open;
- first query after open and steady-state query;
- individual inserts outside a transaction;
- 100, 1,000, and 10,000 inserts in one transaction/batch;
- point lookup by record ID;
- indexed predicate lookup;
- full scan returning 10, 100, 1,000, and 10,000 records;
- update and delete by ID;
- relation creation and one-/two-hop graph traversal;
- transaction commit and rollback;
- export/import or backup when exposed;
- SurrealKV reopen and recovery after clean and forced termination.

Do not imply SQLite equivalence for graph, document, or SurrealQL workloads. Cross-library comparisons are valid only when schema, durability, returned data, transaction boundaries, and materialization work are equivalent.

### Reactive store

- initial query activation;
- cache hit;
- invalidation and refetch;
- application of 1, 100, and 1,000 live updates;
- end-to-end mutation-to-subscriber latency;
- selector notification count and React render count;
- optimistic update and rollback;
- mount/unmount churn and subscription teardown;
- retained cache memory after all consumers unsubscribe.

### Resource and lifecycle

- stripped native binary contribution per ABI/slice;
- application download/install size delta;
- cold startup and native module initialization;
- idle RSS after open;
- peak RSS during a large result and after release/GC;
- CPU time and energy/thermal state for sustained workloads;
- database file growth, compaction, and write amplification where measurable;
- task/thread/handle count after repeated open/query/subscribe/close cycles;
- cancellation latency and close-while-querying latency.

## Measurement protocol

Every timing benchmark should follow the same protocol:

1. Create a uniquely named database and deterministic seed data outside the timed region.
2. Open once unless measuring open time; OP-SQLite correctly notes that repeatedly opening a connection adds unrelated latency.
3. Run at least three untimed warm-ups until JIT/cache/native initialization effects settle.
4. Run multiple independent samples, not one giant opaque timer. Use enough operations inside a sample to exceed timer noise without making thermal throttling dominate.
5. Alternate competitor order or randomize workload order when comparing implementations.
6. Cool down between sustained workloads and record thermal/power state when the platform exposes it.
7. Delete or restore the database between samples when prior writes affect later results.
8. Fully consume results inside the timed region for end-to-end measurements.
9. Report median, p95, minimum, maximum, median absolute deviation, and raw samples. Do not use only the mean.
10. Repeat the entire suite at least three times before updating a public benchmark.

Use `performance.now()` for JavaScript elapsed time. Add native Rust spans based on a monotonic clock so the result can separate:

```text
JS request/queue → FFI decode → database execution → value encode → JS decode/traversal
```

Never force `global.gc()` as a required part of a benchmark; it is not consistently available or representative. For memory suites, explicitly define whether the result is measured before release, after dropping handles, or after an optional diagnostic GC.

## Structured result format

```json
{
  "schemaVersion": 1,
  "commit": "git-sha",
  "timestamp": "2026-07-12T12:00:00Z",
  "platform": "android",
  "device": {
    "model": "Pixel 8",
    "os": "Android 16",
    "abi": "arm64-v8a",
    "physical": true
  },
  "build": {
    "appVariant": "release",
    "hermes": true,
    "reactNative": "exact-version",
    "surrealdb": "exact-version",
    "engine": "surrealkv",
    "rustProfile": "release",
    "features": ["kv-surrealkv"]
  },
  "benchmark": "query.point-lookup.materialized",
  "unit": "ms",
  "warmups": 3,
  "samples": [1.42, 1.38, 1.47],
  "summary": {
    "median": 1.42,
    "p95": 1.47,
    "mad": 0.04
  }
}
```

Store one JSON Lines file per run plus a Markdown summary. Artifacts must include device logs, crash reports, and build-size reports. A future dashboard can ingest the same schema without changing the runner.

## Regression policy

Use two thresholds together:

- **relative threshold:** percentage regression from a compatible baseline;
- **absolute threshold:** user-visible or resource budget that must never be exceeded.

Only compare results when platform, device model, OS major version, build variant, Hermes, native features, SurrealDB version, storage engine, dataset, and benchmark schema match. Otherwise establish a new baseline.

Suggested starting policy, to be calibrated after collecting variance:

- PR emulator/simulator smoke: warn above 15%; do not block on noisy microsecond-scale tests;
- nightly pinned physical device: fail when median regresses more than 10% and the change exceeds three baseline MADs;
- release candidate: run three full suites and require both median and p95 budgets;
- binary size: block on an unexplained per-ABI increase above the agreed absolute budget or 5%;
- memory: block on retained-memory growth after lifecycle loops, even if peak latency is unchanged.

Avoid blindly copying these numbers. First collect 20–30 baseline runs and derive thresholds from observed device variance.

## Harness suites to add

Harness should cover behavior that Node/Rust unit tests cannot:

- connect, query with every value variant, close, and idempotent close;
- 64-bit integer/decimal/record-ID fidelity through Hermes;
- simultaneous queries and separate database handles;
- transaction rollback after a thrown JavaScript error;
- cancel a long-running query and verify rollback/no late resolution;
- close during an in-flight query with a bounded timeout;
- subscribe, unsubscribe, close, and assert no callback after teardown;
- delete/recreate persistent databases;
- app relaunch persistence and migration fixtures;
- invalid paths, low-space errors, corrupt database, wrong schema/version;
- repeated open/query/close loops to surface native leaks or crashes;
- `surreal-store` selector and render-count tests in the actual React renderer.

Use platform-specific suites only where behavior genuinely differs. Most database contract suites should be shared and run on both platforms.

## CI lanes

### Pull requests

- Rust unit/criterion benchmarks on the host, saved for diagnostic comparison;
- TypeScript codec/store unit tests;
- Harness correctness suite on Android emulator and iOS simulator;
- short Harness performance smoke suite with generous, noise-aware warnings;
- build and stripped-size report for every native target.

### Nightly

- full Harness stress/lifecycle suite;
- Release benchmark app on pinned physical Android and iOS devices through a device lab or self-hosted runners;
- SurrealKV durability/relaunch cases;
- raw result upload and comparison with the last accepted compatible baseline;
- trend publication to CI artifacts initially, then a dashboard when history is sufficient.

### Release candidate

- repeat the physical-device Release suite three times;
- compare against the previous published package and the release branch baseline;
- run cold install/start, migration, forced termination, low-storage, memory, and binary-size gates;
- publish the exact benchmark commit, devices, build settings, raw JSON, and limitations with any public performance claims.

## Practical first implementation

Before the native package exists, create these shared files during scaffolding:

```text
benchmarks/
  schema/benchmark-result.v1.json
  specs/bridge.ts
  specs/query.ts
  specs/codec.ts
  specs/lifecycle.ts
  runner.ts
packages/example/
  src/__tests__/database.harness.ts
  src/__tests__/lifecycle.harness.ts
  src/benchmarks/ReleaseBenchmarkScreen.tsx
scripts/
  compare-benchmarks.ts
  collect-ios-metrics.sh
  collect-android-metrics.sh
```

The first performance milestone should produce a reproducible `kv-mem` baseline for `RETURN 1`, a 1,000-record fully materialized query, 1,000 transactional inserts, open/close latency, peak RSS, and stripped binary size on one pinned Android and one pinned iOS device. Add SurrealKV only after that baseline is stable.

## Sources

- [OP-SQLite repository](https://github.com/OP-Engineering/op-sqlite)
- [OP-SQLite installation/performance mode](https://op-engineering.github.io/op-sqlite/docs/installation)
- [React Native Harness repository](https://github.com/callstackincubator/react-native-harness)
- [Harness architecture](https://react-native-harness.dev/docs/getting-started/architecture)
- [Harness configuration](https://react-native-harness.dev/docs/getting-started/configuration)
- [Harness CI/CD guide](https://react-native-harness.dev/docs/guides/ci-cd)
- [Harness native coverage](https://react-native-harness.dev/docs/guides/native-coverage)
