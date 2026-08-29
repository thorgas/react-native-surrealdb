# SurrealDB React Native compatibility apps

The repository keeps one static
[react-native-test-app](https://github.com/microsoft/react-native-test-app)
host per supported React Native version. They all import the application,
integration tests, and benchmark code from `../harness-shared`.

| Workspace package        | React Native |
| ------------------------ | ------------ |
| `surrealdb-harness-rn82` | 0.82.1       |
| `surrealdb-harness-rn83` | 0.83.10      |
| `surrealdb-harness-rn84` | 0.84.1       |
| `surrealdb-harness-rn85` | 0.85.3       |
| `surrealdb-harness-rn86` | 0.86.0       |

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

The RN 0.86 host also has a two-phase sync recovery check. Each command builds
and launches the host, seeds an app-private SurrealKV database, crosses a real
Harness process-termination boundary, and verifies the durable outbox and
optimistic record after reopening. The seed phase also calls the public native
canonical-CBOR codec, checks a real pending push envelope, and decodes the
private pull-response golden message through Hermes:

```sh
pnpm --filter surrealdb-harness-rn86 run e2e:sync-restart:ios
pnpm --filter surrealdb-harness-rn86 run e2e:sync-restart:android
```

The permanent cross-version fixture builds the historical `e0c200b` revision
with its exact SurrealDB `3.2.1` pin, seeds the app-private database, replaces
only the native runtime with the current exact `3.2.4` build, reinstalls without
clearing application data, and verifies the existing restart assertions:

```sh
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-migration:ios
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-migration:android
```

The first run needs a frozen install and full historical Rust builds in a
temporary detached worktree. The runner removes that worktree and restores the
current native artifact even after a failure. It does not remove this active
checkout's `node_modules` or Cargo target cache.

The lifecycle resource proof repeatedly opens, writes, reads, and closes the
same SurrealKV database 64 times while sampling the application process RSS:

```sh
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-churn:ios
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-churn:android
```

Reports are written below the ignored
`performance-results/<platform>/surrealkv-churn/` directory. They contain every
raw sample and min/median/p95/max summaries. The commands require at least five
valid samples but deliberately have no memory regression threshold: Debug,
Metro, Harness, simulator/emulator, relaunch, and allocator behavior make these
diagnostic baselines rather than production memory claims.

The ordinary shared Hermes suite also covers the application-owned HTTP adapter
with a redacted mocked authority and a real embedded native sync client. Run it
without rebuilding an already-installed host with:

```sh
pnpm --filter surrealdb-harness-rn86 run test:harness:ios \
  -- --testPathPatterns shared.harness.ts
pnpm --filter surrealdb-harness-rn86 run test:harness:android \
  -- --testPathPatterns shared.harness.ts
```

To exercise a real local authority rather than the redacted mock, first start the private
development stack and then run the dedicated opt-in trace. The normal harness suites never require
its token:

```sh
/absolute/path/to/surrealdb-sync-engine.dev/scripts/local-dev.sh up
SYNC_ENGINE_DEV_REPO=/absolute/path/to/surrealdb-sync-engine.dev \
  pnpm --filter surrealdb-harness-rn86 run e2e:local-authority:ios
SYNC_ENGINE_DEV_REPO=/absolute/path/to/surrealdb-sync-engine.dev \
  pnpm --filter surrealdb-harness-rn86 run e2e:local-authority:android
```

The environment override is optional for the normal sibling checkout layout. The runner reads the
ignored mode-`600` local credentials without sourcing or printing them and removes its generated
token module on exit. The trace proves initial pull, concurrent optimistic writes,
accepted/conflict outcomes, facade reopen, and final convergence. Its second scenario keeps a
durable mutation queued while offline, recovers one real `401` by swapping the injected token,
stops the scheduler in background, catches an authority write on foreground, and recovers another
write through the 250 ms test-only periodic pull without a WebSocket hint. Lifecycle events are
injected: physically backgrounding the host would suspend Hermes and prevent in-process assertions.

Use Node 22.22.0 from the repository `.node-version`. The Android runner may
stop and restart its configured `Pixel_9` AVD between the seed and verification
files; do not uninstall the app or pass `HARNESS_APP_PATH` between those phases,
because either action can erase the app-private database being verified.
The migration runner removes an earlier harness installation once, before installing the historical
seed binary, so a database last opened by a newer SurrealKV cannot contaminate the baseline. It
never clears app data between the historical seed and current-runtime reopen phases. The runner
fails before building when the active Node major is outside the repository's supported 20–22 range.

Locally, Rock reads a token from `GITHUB_TOKEN` or the authenticated GitHub CLI.
CI passes `GITHUB_TOKEN` to the pinned Rock Android and iOS actions and grants
only `contents: read` and `actions: write`. A cache miss builds and uploads the
binary; the next job with the same native fingerprint downloads it.

Each static host exposes Rock's remote-cache command through `rock:cache`. List
the cache records for an exact host and native build configuration from the
repository root:

```sh
pnpm --filter surrealdb-harness-rn86 run rock:cache list \
  --platform android --traits debug
pnpm --filter surrealdb-harness-rn86 run rock:cache list \
  --platform ios --traits simulator,Debug
```

Rock normally uploads a successful local build automatically. To upload an
already-built binary explicitly, pass the artifact produced by that same host,
platform, and configuration:

```sh
pnpm --filter surrealdb-harness-rn86 run rock:cache upload \
  --platform android --traits debug \
  --binary-path android/app/build/outputs/apk/debug/app-debug.apk
pnpm --filter surrealdb-harness-rn86 run rock:cache upload \
  --platform ios --traits simulator,Debug \
  --binary-path /absolute/path/to/ReactTestApp.app
```

The iOS `.app` location depends on the selected Xcode build folder, so resolve
it from the completed local build rather than copying the placeholder. A cache
record is usable only when its Rock fingerprint and uploaded artifact both
exist; do not upload an artifact built by another React Native host.

After a compatibility workflow has completed, list its run IDs and rerun only
the failed jobs with the GitHub CLI:

```sh
gh run list --repo thorgas/react-native-surrealdb \
  --workflow "React Native compatibility"
gh run rerun <run-id> --repo thorgas/react-native-surrealdb --failed
```

Failures that happen before Rock performs its cache lookup, such as an explicit
`pod install` failure, must be fixed or retried independently of the cache.

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

The local sync durability profile requires no authority or cloud account. It
compares persistent sync enqueue, pending-state materialization, and reopen
recovery with a file-backed OP-SQLite lower bound in the same native app:

```sh
pnpm --filter surrealdb-harness-rn86 run benchmark:android:sync
pnpm --filter surrealdb-harness-rn86 run benchmark:ios:sync
```

The benchmark resets logical tables before every enqueue sample, retains both
physical stores between samples, uses WAL and `synchronous=FULL` for SQLite,
and rejects semantically different materialized records or outboxes. It does
not measure HTTP, an authority, authentication, conflicts, or changefeeds. See
`../../PERFORMANCE.md` for current results and interpretation limits.

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
