# SurrealDB React Native test app

This package uses [react-native-test-app](https://github.com/microsoft/react-native-test-app) for its Android and iOS hosts and [React Native Harness](https://github.com/callstackincubator/react-native-harness) for device integration tests.

Install workspace dependencies from the repository root:

```sh
pnpm install
```

The iOS command prepares the ignored Rust XCFramework and RNTA bundle resources
before launching the configured iPhone 17 Pro (iOS 26.1) simulator:

```sh
pnpm --filter SurrealDbHarness run ios
```

The first run builds all iOS Rust slices and can take several minutes. Later
runs reuse them. To select another installed simulator, override the default:

```sh
SURREALDB_IOS_SIMULATOR="iPhone 16 Pro (18.5)" \
  pnpm --filter SurrealDbHarness run ios
```

After changing iOS native dependencies, regenerate the RNTA workspace with:

```sh
cd apps/harness/ios
bundle exec pod install
```

Run the app or tests from `apps/harness`:

```sh
pnpm start
pnpm run android
pnpm run ios
pnpm run test:harness:android
pnpm run test:harness:ios
```

Native host configuration belongs in `app.json`, the RNTA manifest. Do not add application code under the generated native projects.

RNTA 5.4.4 currently resolves `@rnx-kit/react-native-host` 0.5.20, which is missing one RN 0.86 header import. The pnpm patch in `../../patches` mirrors Microsoft rnx-kit's upstream fix (`361155b`) and can be removed after that fix is published.

## Release size regression checks

The Android size check compares this harness against a measured stock React
Native Test App baseline using RNTA 5.4.4, React Native 0.86.0, Hermes, the New
Architecture, and an arm64-v8a Release APK. The committed baseline is 18.59 MiB.
The first static-linked SurrealDB release measured 80.46 MiB, an incremental
61.88 MiB. Size-oriented Rust LTO and a separate Android `cdylib` reduced the
candidate to 42.90 MiB and the increment to 24.31 MiB on 2026-07-13.

Generate the required arm64 artifact and run a platform comparison from the
repository root:

```sh
pnpm --filter react-native-surrealdb run ubrn:android:size
pnpm --filter SurrealDbHarness run size:android:benchmark
```

To reproduce only the stock RNTA number, run
`pnpm --filter SurrealDbHarness run size:android:baseline`. The script records
the exact configuration, command, timestamp, and source in
`size-results/android/baseline-report.json`. The paired benchmark uses the
freshly built baseline APK; `size-budget.json` also retains the original
2026-07-12 reference measurement and its provenance for historical comparison.

The release artifact and JSON report are written below `size-results/`. The
budget in `size-budget.json` is a hard ceiling for the candidate-minus-baseline
delta. Re-measure and review the stock baseline whenever RNTA or React Native is
upgraded. CI uploads the report even when the budget fails.

The report also lists every packaged native library. Both the app increment and
combined SurrealDB `.so` files have a 28 MiB ceiling, with the exact 2026-07-13
optimized measurement and provenance retained in `size-budget.json`. This
catches native growth directly even when APK compression happens to hide it.

## Mobile performance regression benchmarks

The opt-in performance suite adapts the workload families from SurrealDB's
open-source [`crud-bench`](https://github.com/surrealdb/crud-bench) at revision
`18eb1fc8d8edcfd3d6ba8328149789ffa7866659`. The shared runner implements every
case enabled by that revision's default `config/bench.toml`: single-record and
100/1,000-record batch CRUD; count, ID-only, full, limit, and offset scans; all
seven filter/order families as count and full projections; heap and indexed
reads; 15% and 50% mixed-write runs; index build/removal; and all three BM25
queries. It adds one bridge baseline and two graph traversals for this package,
for 141 measured variants in total. Each measurement includes the complete
Hermes → JSI → UniFFI → Rust → SurrealDB round trip and result decoding.

Run the short profile on the configured emulator/simulator:

```sh
pnpm --filter SurrealDbHarness run benchmark:android
pnpm --filter SurrealDbHarness run benchmark:ios
```

The canonical profile uses 2,000 records and more repetitions for a steadier
mobile regression signal. The upstream profile uses 10,000 records, preserves
the exact `START 5000 LIMIT 100` shape, and uses the largest repetition counts:

```sh
pnpm --filter SurrealDbHarness run benchmark:android:canonical
pnpm --filter SurrealDbHarness run benchmark:ios:canonical
pnpm --filter SurrealDbHarness run benchmark:android:upstream
pnpm --filter SurrealDbHarness run benchmark:ios:upstream
```

To run the benchmark manually, start the RNTA example app with `pnpm start` and
`pnpm run android` or `pnpm run ios`. Its **Mobile benchmark lab** screen lets
you choose any profile, start or cancel it, follow per-workload progress, inspect
median/p95/operations-per-second results, and share the complete JSON report.
The app and Harness tests call the same benchmark implementation.

The example app also runs a **10k startup load** automatically when its React
tree mounts. This opens an in-memory embedded database, creates 10,000
deterministic entries in 250-record transactions, queries and decodes every
entry, touches every selected field, and mounts all 10,000 rows in an invisible
React Native render probe. The result card separates database open, seed,
query/decode, materialization, React render, render-to-native-layout, and total
ready time. Use its rerun button to repeat the same workload without restarting
the native app. Completed runs also emit `SURREALDB_STARTUP_TIMING=` JSON to the
device log for collection on Android and iOS.

The app and performance Harness also include an opt-in adaptation of Oscar
Franco's open-source
[`ospfranco/sqlite-bench`](https://github.com/ospfranco/sqlite-bench), pinned to
revision `4c022c9a38294b66af2cd79fae64f0e91f25353b`. It preserves the upstream
1,000 awaited async inserts, 1,000 transaction inserts, and 1,000 full-table
selects of 1,000 rows with every selected property read, plus the 2,500 ms
cooldown between workloads. The performance Harness runs both SurrealDB and the
upstream benchmark's exact op-sqlite `17.1.1` dependency in the same native app
on the same device. Both use in-memory databases for engine parity; this is the
only intentional change to op-sqlite's comparable workloads from the upstream
named-file configuration. It collects two samples in opposite library orders
to balance first-run and thermal effects. Both transaction legs make 1,000
awaited JavaScript calls through a transaction handle before committing once.
For SurrealDB, those calls execute against one native SDK transaction ID; the
workload no longer substitutes a single concatenated 1,000-statement query.
The schema-version-4 paired report includes a `comparisons` entry for each
comparable workload. Each entry records both median durations, the faster
library, the unrounded speed factor, and a direction-aware statement such as
“op-sqlite is 8.17× faster than SurrealDB.” Factors use median duration with
lower treated as faster. Each SurrealDB metric also attributes time to the
embedded SDK query future (`engineMs`), the package path (`packagePathMs`), and
work outside individually profiled queries (`unattributedMs`). The report
retains both opposite-order attribution samples, an async UniFFI/JSI no-op
baseline, and a 1/10/100/1,000-statement transaction batch-size sweep.
Attribution is collected in two dedicated SurrealDB runs after the paired
comparison, so extra clocks and diagnostic return fields do not alter the
durations used for the SurrealDB/op-sqlite speed factors.
The same report also includes a separate glue-optimization section. Its batch
case gives both libraries one parameterized template, 1,000 parameter sets,
one asynchronous batch payload, and a full transaction lifecycle in opposite
orders. Its codec case runs the same 100 materialized 1,000-row reads with a
2×2 legacy/optimized serializer and decoder matrix. Checksums must match across
both databases and all codec variants. These diagnostics never replace or
modify the original per-call comparison.
The synchronous insert and HostObject select are retained as op-sqlite-only
measurements because the SurrealDB client API is asynchronous and eagerly
decodes results.

Run that profile with:

```sh
pnpm --filter SurrealDbHarness run benchmark:android:sqlite
pnpm --filter SurrealDbHarness run benchmark:ios:sqlite
```

The comparison card retains the values from the
[result image supplied by the benchmark author](https://pbs.twimg.com/media/HLwBJGpWcAAV_Fl?format=jpg&name=4096x4096):

| Library      | Async insert 1k | Transaction insert 1k | Select 1k × 1k |
| ------------ | --------------: | --------------------: | -------------: |
| op-sqlite    |      1,457.3 ms |              152.8 ms |       534.2 ms |
| nitro-sqlite |      1,448.6 ms |              124.1 ms |     1,843.5 ms |
| expo-sqlite  |      2,046.3 ms |              551.3 ms |     3,344.3 ms |

The image does not state its device, OS, or build configuration. Those published
numbers are therefore shown as external context, not treated as a directly
comparable baseline or used by the regression gate.

Reports and raw Harness logs are written below `performance-results/`. To gate
a run, supply a report from the same device and exact configuration:

```sh
SURREALDB_PERFORMANCE_BASELINE=/absolute/path/to/report.json \
  pnpm --filter SurrealDbHarness run benchmark:android
```

The initial gate fails only when median latency grows by both more than 15% and
more than 0.1 ms. Keep baselines device-specific; simulator, OS, React Native,
SurrealDB, workload, and build-profile mismatches are rejected instead of being
silently compared. The mobile results are regression signals and must not be
presented as directly comparable to SurrealDB's server benchmark hardware. The
separate upstream `config/vector.toml`, multi-client/server orchestration,
resource monitoring, and cross-database drivers are explicitly out of scope for
this embedded single-client mobile runner.
