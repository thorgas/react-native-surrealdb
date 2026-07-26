# React Native SurrealDB

An experimental React Native binding for SurrealDB, built from the SurrealDB Rust SDK through UniFFI/JSI.

## Current status

The native core and first React Native integration milestone are implemented:

- SurrealDB 3.2.1 with `mem://`, `surrealkv://`, `ws://`, and `wss://` engine features;
- async UniFFI API for connect, query, namespace/database selection, token authentication, root/database sign-in, invalidation, and close;
- pull-based live-query handles with backpressure, async iteration, explicit cancellation, and automatic cancellation when the database closes;
- versioned JSON wire values that preserve 64-bit integers, decimals, bytes, UUIDs, record IDs, `NONE`, sets, and other non-JSON SurrealDB values;
- checked-in UniFFI/JSI generated bindings and a hand-written TypeScript facade for React Native's New Architecture;
- host tests for queries, live notifications, Hermes-safe integer transport, idempotent close, and SurrealKV persistence/reopen;
- a `react-native-test-app` host with React Native Harness integration tests passing on React Native 0.86/Hermes V1 with an iPhone 17 Pro simulator and an Android API 36 arm64 Pixel 9 emulator;
- authenticated remote WebSocket live-query integration tested against a real SurrealDB server;
- size-oriented Rust LTO plus a separately packaged Android Rust `cdylib`, reducing the measured arm64 RNTA app increment from 61.88 MiB to 24.31 MiB while preserving 16 KB page-size support (NDK 27, API 24 minimum);
- the complete default crud-bench workload matrix as 141 device-side metrics, with smoke/canonical/upstream profiles, an RNTA manual benchmark lab, raw samples, and compatible-baseline regression checks.

Automatic WebSocket re-subscription and event deduplication across reconnects remain future work; the current subscription terminates when its SDK stream terminates.

## Development

This repository is a pnpm workspace containing the Rust core, the published
React Native package, and five static compatibility apps:

| Path                                      | Purpose                                                                 |
| ----------------------------------------- | ----------------------------------------------------------------------- |
| `crates/surrealdb-rn-core`                | Rust API exposed through UniFFI                                         |
| `packages/react-native-surrealdb`         | Published TypeScript, JSI/C++, iOS, and Android package                 |
| `apps/harness-shared`                     | Application, integration tests, and benchmarks shared by every test app |
| `apps/harness-rn82` … `apps/harness-rn86` | One React Native Test App host per supported React Native version       |

The React Native versions are pinned as named catalogs in
[`pnpm-workspace.yaml`](./pnpm-workspace.yaml). No maintenance script rewrites a
host's `package.json`.

### Where the command names come from

Commands run through `pnpm run` use executables from the relevant workspace
package's `node_modules/.bin`; they are not expected to be installed globally.

| Command                               | Provided by                                                                                                                                                 | What it does here                                                                        |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `ubrn`                                | [`uniffi-bindgen-react-native`](https://jhugman.github.io/uniffi-bindgen-react-native/reference/commandline.html), a dependency of `react-native-surrealdb` | Builds the Rust libraries and generates the UniFFI TypeScript/C++/TurboModule bindings   |
| `bob`                                 | [`react-native-builder-bob`](https://oss.callstack.com/react-native-builder-bob/build), a dev dependency of `react-native-surrealdb`                        | Builds the publishable ESM JavaScript and TypeScript declarations into `lib/`            |
| `configure-test-app`                  | [`react-native-test-app`](https://github.com/microsoft/react-native-test-app), a dependency of each harness                                                 | Regenerates an RNTA native host from its `app.json` manifest                             |
| `rock`                                | [`rock`](https://github.com/callstack/rock), a dependency of each harness                                                                                   | Starts Metro or builds/runs a native app, using the configured GitHub remote build cache |
| `react-native-harness`                | [`react-native-harness`](https://react-native-harness.dev/docs/), a dependency of each harness                                                              | Installs/runs the app and executes device-side integration or benchmark tests            |
| `react-native`                        | React Native and its community CLI packages in each harness                                                                                                 | Bundles JavaScript or builds/runs a host without Rock                                    |
| `vitest`, `tsc`, `eslint`, `prettier` | Package-local development dependencies                                                                                                                      | Run unit tests and static checks                                                         |
| `cargo`, `rustup`                     | The Rust toolchain pinned by `rust-toolchain.toml`                                                                                                          | Build, test, lint, format, and install target standard libraries for the Rust core       |
| `bundle`, `pod`                       | Ruby Bundler and CocoaPods                                                                                                                                  | Resolve the iOS host's native dependencies                                               |
| `surreal`                             | The optional [SurrealDB CLI](https://surrealdb.com/docs/surrealdb/installation)                                                                             | Starts a real server for the ignored WebSocket integration test                          |

For example, this selects the package workspace and then runs its `ubrn:ios`
script. The `ubrn` inside that script resolves from
`packages/react-native-surrealdb/node_modules/.bin`:

```sh
pnpm --filter react-native-surrealdb run ubrn:ios
```

If a local executable cannot be found, install the pinned workspace
dependencies instead of installing the command globally:

```sh
pnpm install --frozen-lockfile
```

The repository requires Node.js 20 or newer and pnpm 11 or newer; CI currently
uses Node.js 22 and pnpm 11.5.0. Platform builds additionally require Xcode and
CocoaPods on macOS, or an Android SDK, NDK 27, Java, and `cargo-ndk` for
Android. The complete Rust target list is recorded in
[`packages/react-native-surrealdb/RELEASING.md`](./packages/react-native-surrealdb/RELEASING.md).

### Everyday checks

Run the narrow checks while developing:

```sh
./scripts/verify-core.sh
pnpm --filter react-native-surrealdb test
pnpm --filter react-native-surrealdb typecheck
```

The root `package.json` provides these repository-wide commands:

| Command                              | What it runs                                                                                                                                                                           |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pnpm build`                         | Runs every workspace's `build` script. Currently this is the package's Bob build; the harness bundle scripts are named `build:android` and `build:ios` and are therefore not included. |
| `pnpm format`                        | Checks the package's TypeScript/JSON/YAML with Prettier, then checks all Rust code with `cargo fmt`. It reports differences but does not rewrite normal source files.                  |
| `pnpm lint`                          | Runs the package TypeScript check and ESLint for all five compatibility hosts.                                                                                                         |
| `pnpm test`                          | Runs workspace scripts named exactly `test`. Currently this is the package Vitest suite; Harness tests and benchmark-tool tests have explicit names and are not included.              |
| `pnpm typecheck:react-native-matrix` | Type-checks RN 0.82 through 0.86 sequentially so failures identify a particular host.                                                                                                  |
| `./scripts/verify-core.sh`           | Runs Rust formatting, Clippy with warnings denied, Rust unit tests, and cross-compiles the core for iOS Simulator and Android arm64.                                                   |

`verify-core.sh` expects the corresponding Rust targets to be installed. Its
Android check uses `ANDROID_NDK_HOME` when set, otherwise it looks below
`~/Library/Android/sdk/ndk`.

### Native artifacts and generated bindings

The `ubrn:*` scripts live in
[`packages/react-native-surrealdb/package.json`](./packages/react-native-surrealdb/package.json):

| Command                                                      | Result                                                                                                                                                                                    |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pnpm --filter react-native-surrealdb run ubrn:ios`          | Builds release libraries for iOS device plus arm64/x86_64 simulators, creates `SurrealDbRnFramework.xcframework`, and regenerates the UniFFI bindings. The deployment target is iOS 15.1. |
| `pnpm --filter react-native-surrealdb run ubrn:android`      | Builds release `.so` files for arm64-v8a, armeabi-v7a, x86_64, and x86 under `android/src/main/jniLibs`, and regenerates the bindings.                                                    |
| `pnpm --filter react-native-surrealdb run ubrn:android:size` | Builds only arm64 Android, which is sufficient for the controlled release-size benchmark. Do not use this reduced artifact set for publishing.                                            |
| `pnpm --filter react-native-surrealdb run release:artifacts` | Runs the full iOS and Android `ubrn` builds.                                                                                                                                              |
| `pnpm --filter react-native-surrealdb run format:generated`  | Rewrites the generated TurboModule entry files with Prettier. The `ubrn:*` scripts run it automatically.                                                                                  |

`--and-generate` in these scripts means “build Rust and regenerate bindings”;
`--release` selects optimized Rust artifacts; `--targets` lists the Rust target
triples to include. Their paths and output names are configured in
[`ubrn.config.yaml`](./packages/react-native-surrealdb/ubrn.config.yaml).

The XCFramework and Android `jniLibs` outputs are intentionally ignored because
they are large and platform-generated. The binding source in `src/generated/`,
`cpp/generated/`, `src/native.tsx`, and `src/NativeSurrealdb.ts` is checked in.
After changing the UniFFI surface, regenerate all artifacts, review the
generated source diff, and commit that source diff.

### Package build and release commands

All commands below target the published package:

| Command                                          | What it means                                                                                                                |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `pnpm --filter react-native-surrealdb run build` | Runs Bob and recreates publishable ESM and declaration output in `lib/`.                                                     |
| `... run test`                                   | Runs the Vitest host/unit suite once.                                                                                        |
| `... run typecheck`                              | Runs `tsc --noEmit`.                                                                                                         |
| `... run lint`                                   | Also runs `tsc --noEmit`; this alias lets the root recursive lint command include the package.                               |
| `... run format`                                 | Checks package source, tests, and top-level JSON/YAML with Prettier.                                                         |
| `... run verify:package`                         | Checks built output, generated bindings, all expected iOS slices, all four Android ABIs, and package metadata.               |
| `... run release:check`                          | Runs build, type-check, tests, and package verification. It does **not** create missing native artifacts.                    |
| `pnpm --filter react-native-surrealdb pack`      | Runs `prepack` (therefore `release:check`) and creates the npm tarball for consumer testing.                                 |
| `pnpm --filter react-native-surrealdb publish …` | Runs `prepublishOnly` (also `release:check`) before publishing. Follow the release guide rather than invoking this casually. |

Prepare native artifacts before a release check:

```sh
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter react-native-surrealdb run release:check
pnpm --filter react-native-surrealdb pack
```

See [`packages/react-native-surrealdb/RELEASING.md`](./packages/react-native-surrealdb/RELEASING.md)
for target installation, package-size review, and the manual publish step.

### React Native compatibility apps

Each static RNTA host imports the same code from `apps/harness-shared`:

| Workspace filter         | React Native |
| ------------------------ | ------------ |
| `surrealdb-harness-rn82` | 0.82.1       |
| `surrealdb-harness-rn83` | 0.83.10      |
| `surrealdb-harness-rn84` | 0.84.1       |
| `surrealdb-harness-rn85` | 0.85.3       |
| `surrealdb-harness-rn86` | 0.86.0       |

Replace the filter in these examples to test another supported version:

| Command                                                     | Purpose                                                                                                                              |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `pnpm --filter surrealdb-harness-rn86 run start`            | Starts Metro through Rock.                                                                                                           |
| `... run rock:android`                                      | Builds or restores the Android app through Rock, then runs it.                                                                       |
| `... run rock:ios`                                          | Prepares iOS artifacts and Pods, then builds or restores and runs through Rock.                                                      |
| `... run android`                                           | Uses the React Native CLI directly instead of Rock.                                                                                  |
| `... run ios`                                               | Prepares iOS, then uses the React Native CLI directly. Set `SURREALDB_IOS_SIMULATOR` to override the default simulator.              |
| `... run prepare:ios`                                       | Creates the missing XCFramework and JS bundle, and runs `bundle exec pod install` when the Pods project does not link the framework. |
| `... run build:android` / `... run build:ios`               | Produces a development JS bundle and assets in `dist/`; these do not compile a native app.                                           |
| `... run test:harness:android` / `... run test:harness:ios` | Runs device integration tests with React Native Harness. A compatible emulator/simulator or `HARNESS_APP_PATH` must be available.    |
| `... run lint`                                              | Lints shared app/test code using that React Native version's ESLint configuration.                                                   |
| `... run typecheck`                                         | Type-checks shared code against that host's React Native and React versions.                                                         |
| `... run configure`                                         | Runs RNTA's `configure-test-app` generator. Use only after intentionally changing `app.json` or RNTA/native configuration.           |

`configure` is not a routine prerequisite. It can rewrite `android/`, `ios/`,
and package metadata based on RNTA defaults. Always run it in only the intended
host and review `git diff` before keeping its changes. Application and tests
belong in `apps/harness-shared`; host-specific native configuration belongs in
the host's `app.json`.

For example:

```sh
pnpm --filter surrealdb-harness-rn82 run rock:android
pnpm --filter surrealdb-harness-rn84 run test:harness:android
pnpm typecheck:react-native-matrix
```

Rock fingerprints each host's native files, resolved dependencies, React Native
version, and the shared Rust/package sources. Locally, its GitHub provider reads
`GITHUB_TOKEN` or the authenticated GitHub CLI session. A cache miss performs a
native build and uploads the result; a matching later invocation restores it.
CI builds the Rust native artifact once per platform before running the
five-version Rock matrix. Details live in
[`apps/harness-rn86/README.md`](./apps/harness-rn86/README.md) and
[`.github/workflows/react-native-compatibility.yml`](./.github/workflows/react-native-compatibility.yml).

### RN 0.86-only maintenance commands

RN 0.86 is the reference host for size and device performance measurements.
These scripts are intentionally not duplicated in older hosts:

| Command                                                                   | Purpose                                                                                                              |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `pnpm --filter surrealdb-harness-rn86 run size:android`                   | Builds an arm64 release candidate and compares it with the committed reference or `BASELINE_APK`.                    |
| `... run size:android:baseline`                                           | Builds and records a stock RNTA app without this package.                                                            |
| `... run size:android:benchmark`                                          | Builds a fresh stock baseline and the candidate, then performs the paired comparison. Run `ubrn:android:size` first. |
| `... run benchmark:android` / `... run benchmark:ios`                     | Runs the short `smoke` device benchmark.                                                                             |
| `... run benchmark:android:canonical` / `... run benchmark:ios:canonical` | Runs the 2,000-record regression profile.                                                                            |
| `... run benchmark:android:upstream` / `... run benchmark:ios:upstream`   | Runs the 10,000-record upstream-shaped profile.                                                                      |
| `... run test:benchmark-tools`                                            | Tests the Node report extraction and comparison utilities without a device.                                          |

Size reports are written below `apps/harness-rn86/size-results/`; benchmark
reports and captured device logs go below
`apps/harness-rn86/performance-results/`. Both output directories are ignored.
Set `SURREALDB_PERFORMANCE_BASELINE` to an absolute report path to enable the
performance regression gate. See [`PERFORMANCE.md`](./PERFORMANCE.md) and the
harness README for the measurement rules and baseline compatibility checks.

### Common maintenance flows

For a TypeScript facade change:

```sh
pnpm --filter react-native-surrealdb run test
pnpm --filter react-native-surrealdb run typecheck
pnpm --filter react-native-surrealdb run build
pnpm typecheck:react-native-matrix
```

For a Rust or UniFFI API change:

```sh
./scripts/verify-core.sh
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter react-native-surrealdb run release:check
pnpm typecheck:react-native-matrix
```

Then inspect the generated binding changes and exercise at least one iOS and
one Android compatibility host. The five-version native matrix itself runs in
GitHub Actions.

When adding or upgrading a supported React Native version, update its named
catalog in `pnpm-workspace.yaml`, create or update a dedicated static host,
review any deliberate RNTA regeneration, update the compatibility workflow
matrix, reinstall with the frozen lockfile, and run the TypeScript matrix.
Keeping independent manifests and native directories prevents one version's
generator output or native cache from leaking into another.

### Optional WebSocket integration test

To exercise the opt-in authenticated WebSocket integration test, start a local server and run:

```sh
surreal start --no-banner --bind 127.0.0.1:18080 --user root --pass root memory
SURREAL_TEST_WS_ENDPOINT=ws://127.0.0.1:18080 \
  cargo test -p surrealdb-rn-core authenticated_websocket_live_query -- --ignored
```

The Rust toolchain and dependency graph are pinned through `rust-toolchain.toml` and `Cargo.lock`.

## Design documents

- [Architecture research](./RESEARCH.md)
- [Performance and device-test strategy](./PERFORMANCE.md)
- [Native size and Rust implementation decisions](./NATIVE_SIZE.md)
