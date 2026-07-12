import {
  androidEmulator,
  androidPlatform,
} from '@react-native-harness/platform-android';
import {
  applePlatform,
  appleSimulator,
} from '@react-native-harness/platform-apple';

export default {
  entryPoint: './index.js',
  appRegistryComponentName: 'SurrealDbHarness',
  runners: [
    androidPlatform({
      name: 'android',
      device: androidEmulator('Pixel_9'),
      bundleId: 'com.surrealdbharness',
      activityName: 'com.microsoft.reacttestapp.MainActivity',
    }),
    applePlatform({
      name: 'ios',
      device: appleSimulator('iPhone 17 Pro', '26.1'),
      bundleId: 'org.reactjs.native.example.SurrealDbHarness',
    }),
  ],
  defaultRunner: 'ios',
  platformReadyTimeout: 300_000,
  bridgeTimeout: 120_000,
  detectNativeCrashes: true,
  resetEnvironmentBetweenTestFiles: true,
  forwardClientLogs: true,
};
