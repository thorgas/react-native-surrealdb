import {
  androidEmulator,
  androidPlatform,
} from '@react-native-harness/platform-android';
import {
  applePlatform,
  appleSimulator,
} from '@react-native-harness/platform-apple';
import { createHarnessConfig } from '../harness-shared/rn-harness-config.mjs';

export default createHarnessConfig({
  androidEmulator,
  androidPlatform,
  applePlatform,
  appleSimulator,
  androidBundleId: 'com.surrealdbharness.rn84',
  iosBundleId: 'org.reactjs.native.example.SurrealDbHarnessRN84',
});
