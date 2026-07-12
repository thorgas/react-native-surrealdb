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
