# SurrealDB React Native test app

This package uses [react-native-test-app](https://github.com/microsoft/react-native-test-app) for its Android and iOS hosts and [React Native Harness](https://github.com/callstackincubator/react-native-harness) for device integration tests.

Install workspace dependencies from the repository root:

```sh
pnpm install
```

After changing iOS native dependencies, generate the RNTA workspace with:

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
The first SurrealDB-enabled release measured 80.46 MiB, an incremental 61.88
MiB, against an 80 MiB incremental CI budget.

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
