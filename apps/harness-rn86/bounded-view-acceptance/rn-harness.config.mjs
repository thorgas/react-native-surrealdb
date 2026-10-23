import {
  androidPlatform,
  physicalAndroidDevice,
} from '@react-native-harness/platform-android';
export default {
  entryPoint: './index.js',
  appRegistryComponentName: 'SurrealDbHarness',
  runners: [
    androidPlatform({
      name: 'bounded-android',
      device: physicalAndroidDevice('Google', 'Pixel 6a'),
      bundleId: 'com.surrealdbharness.boundedviews20261023',
      activityName: 'com.microsoft.reacttestapp.MainActivity',
    }),
  ],
  defaultRunner: 'bounded-android',
  metroPort: 19061,
  platformReadyTimeout: 120000,
  bridgeTimeout: 120000,
  bundleStartTimeout: 120000,
  maxAppRestarts: 0,
  resetEnvironmentBetweenTestFiles: false,
  detectNativeCrashes: true,
  forwardClientLogs: true,
  permissions: false,
};
