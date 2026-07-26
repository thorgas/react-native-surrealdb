# SurrealDB React Native compatibility apps

The repository keeps one static
[react-native-test-app](https://github.com/microsoft/react-native-test-app)
host per supported React Native version. They all import the application,
integration tests, and benchmark code from `../harness-shared`.

| Workspace package | React Native |
| --- | --- |
| `surrealdb-harness-rn82` | 0.82.1 |
| `surrealdb-harness-rn83` | 0.83.10 |
| `surrealdb-harness-rn84` | 0.84.1 |
| `surrealdb-harness-rn85` | 0.85.3 |
| `surrealdb-harness-rn86` | 0.86.0 |

The versions are declared with named pnpm catalogs in
`../../pnpm-workspace.yaml`; no script rewrites a `package.json`. Each host also
has a Rock configuration backed by GitHub Actions artifacts. Rock includes the
host's native files, resolved dependencies, and the shared Rust sources in its
fingerprint, so caches cannot cross React Native versions accidentally.

Install workspace dependencies from the repository root:

```sh
pnpm install
```

The iOS command prepares the ignored Rust XCFramework and RNTA bundle resources
before launching the configured iPhone 17 Pro (iOS 26.1) simulator:

```sh
pnpm --filter surrealdb-harness-rn86 run ios
```

The first run builds all iOS Rust slices and can take several minutes. Later
runs reuse them. To select another installed simulator, override the default:

```sh
SURREALDB_IOS_SIMULATOR="iPhone 16 Pro (18.5)" \
  pnpm --filter surrealdb-harness-rn86 run ios
```

After changing iOS native dependencies, regenerate the RNTA workspace with:

```sh
cd apps/harness-rn86/ios
bundle exec pod install
```

Run a particular host from the repository root:

```sh
pnpm --filter surrealdb-harness-rn82 run rock:android
pnpm --filter surrealdb-harness-rn86 run rock:ios
pnpm --filter surrealdb-harness-rn84 run test:harness:android
pnpm typecheck:react-native-matrix
```

Locally, Rock reads a token from `GITHUB_TOKEN` or the authenticated GitHub CLI.
CI passes `GITHUB_TOKEN` to the pinned Rock Android and iOS actions and grants
only `contents: read` and `actions: write`. A cache miss builds and uploads the
binary; the next job with the same native fingerprint downloads it.

Native host configuration belongs in each host's `app.json`, the RNTA manifest.
Application code belongs in `harness-shared`, not in generated native projects.

## Release size regression checks

The Android size check compares this harness against a measured stock React
Native Test App baseline using RNTA 5.4.5, React Native 0.86.0, Hermes, the New
Architecture, and an arm64-v8a Release APK. The committed baseline is 18.59 MiB.
The first static-linked SurrealDB release measured 80.46 MiB, an incremental
61.88 MiB. Size-oriented Rust LTO and a separate Android `cdylib` reduced the
candidate to 42.90 MiB and the increment to 24.31 MiB on 2026-07-13.

Generate the required arm64 artifact and run a platform comparison from the
repository root:

```sh
pnpm --filter react-native-surrealdb run ubrn:android:size
pnpm --filter surrealdb-harness-rn86 run size:android:benchmark
```

To reproduce only the stock RNTA number, run
`pnpm --filter surrealdb-harness-rn86 run size:android:baseline`. The script records
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
pnpm --filter surrealdb-harness-rn86 run benchmark:android
pnpm --filter surrealdb-harness-rn86 run benchmark:ios
```

The canonical profile uses 2,000 records and more repetitions for a steadier
mobile regression signal. The upstream profile uses 10,000 records, preserves
the exact `START 5000 LIMIT 100` shape, and uses the largest repetition counts:

```sh
pnpm --filter surrealdb-harness-rn86 run benchmark:android:canonical
pnpm --filter surrealdb-harness-rn86 run benchmark:ios:canonical
pnpm --filter surrealdb-harness-rn86 run benchmark:android:upstream
pnpm --filter surrealdb-harness-rn86 run benchmark:ios:upstream
```

To run the benchmark manually, start the RNTA example app with `pnpm start` and
`pnpm run android` or `pnpm run ios`. Its **Mobile benchmark lab** screen lets
you choose any profile, start or cancel it, follow per-workload progress, inspect
median/p95/operations-per-second results, and share the complete JSON report.
The app and Harness tests call the same benchmark implementation.

Reports and raw Harness logs are written below `performance-results/`. To gate
a run, supply a report from the same device and exact configuration:

```sh
SURREALDB_PERFORMANCE_BASELINE=/absolute/path/to/report.json \
  pnpm --filter surrealdb-harness-rn86 run benchmark:android
```

The initial gate fails only when median latency grows by both more than 15% and
more than 0.1 ms. Keep baselines device-specific; simulator, OS, React Native,
SurrealDB, workload, and build-profile mismatches are rejected instead of being
silently compared. The mobile results are regression signals and must not be
presented as directly comparable to SurrealDB's server benchmark hardware. The
separate upstream `config/vector.toml`, multi-client/server orchestration,
resource monitoring, and cross-database drivers are explicitly out of scope for
this embedded single-client mobile runner.
