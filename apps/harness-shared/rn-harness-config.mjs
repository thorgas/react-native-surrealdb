export function createHarnessConfig({
  androidEmulator,
  androidPlatform,
  physicalAndroidDevice,
  applePlatform,
  appleSimulator,
  androidBundleId,
  iosBundleId,
}) {
  return {
    entryPoint: "./index.js",
    appRegistryComponentName: "SurrealDbHarness",
    runners: [
      androidPlatform({
        name: "android",
        device: androidEmulator(
          process.env.SURREALDB_ANDROID_EMULATOR_NAME ?? "Pixel_9",
          {
            apiLevel: 36,
            profile: "pixel_9",
            diskSize: "8G",
            heapSize: "2G",
          },
        ),
        bundleId: androidBundleId,
        activityName: "com.microsoft.reacttestapp.MainActivity",
      }),
      // A USB-connected phone. Select it with --harnessRunner android-device;
      // override the match with SURREALDB_ANDROID_DEVICE_MANUFACTURER/_MODEL.
      androidPlatform({
        name: "android-device",
        device: physicalAndroidDevice(
          process.env.SURREALDB_ANDROID_DEVICE_MANUFACTURER ?? "Google",
          process.env.SURREALDB_ANDROID_DEVICE_MODEL ?? "Pixel 6a",
        ),
        bundleId: androidBundleId,
        activityName: "com.microsoft.reacttestapp.MainActivity",
      }),
      applePlatform({
        name: "ios",
        device: appleSimulator(
          process.env.SURREALDB_IOS_SIMULATOR_NAME ?? "iPhone 17 Pro",
          "26.1",
        ),
        bundleId: iosBundleId,
      }),
    ],
    defaultRunner: "ios",
    platformReadyTimeout: 300_000,
    bridgeTimeout: 120_000,
    detectNativeCrashes: true,
    resetEnvironmentBetweenTestFiles: true,
    forwardClientLogs: true,
  };
}
