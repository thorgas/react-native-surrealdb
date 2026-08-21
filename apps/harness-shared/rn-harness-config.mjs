export function createHarnessConfig({
  androidEmulator,
  androidPlatform,
  applePlatform,
  appleSimulator,
  androidBundleId,
  iosBundleId,
}) {
  return {
    entryPoint: './index.js',
    appRegistryComponentName: 'SurrealDbHarness',
    runners: [
      androidPlatform({
        name: 'android',
        device: androidEmulator('Pixel_9', {
          apiLevel: 36,
          profile: 'pixel_9',
          diskSize: '8G',
          heapSize: '2G',
        }),
        bundleId: androidBundleId,
        activityName: 'com.microsoft.reacttestapp.MainActivity',
      }),
      applePlatform({
        name: 'ios',
        device: appleSimulator('iPhone 17 Pro', '26.1'),
        bundleId: iosBundleId,
      }),
    ],
    defaultRunner: 'ios',
    platformReadyTimeout: 300_000,
    bridgeTimeout: 120_000,
    detectNativeCrashes: true,
    resetEnvironmentBetweenTestFiles: true,
    forwardClientLogs: true,
  };
}
