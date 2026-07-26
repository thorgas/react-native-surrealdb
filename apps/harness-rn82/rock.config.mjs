import { platformAndroid } from '@rock-js/platform-android';
import { platformIOS } from '@rock-js/platform-ios';
import { pluginMetro } from '@rock-js/plugin-metro';
import { providerGitHub } from '@rock-js/provider-github';
import { createRockConfig } from '../harness-shared/rock-config.mjs';

export default createRockConfig({
  platformAndroid,
  platformIOS,
  pluginMetro,
  providerGitHub,
});
