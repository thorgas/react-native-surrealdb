export function createRockConfig({
  platformAndroid,
  platformIOS,
  pluginMetro,
  providerGitHub,
}) {
  return {
    bundler: pluginMetro(),
    platforms: {
      // RNTA supplies its app module from node_modules at Gradle evaluation
      // time. Point Rock's Community CLI project discovery at that manifest.
      android: platformAndroid({
        sourceDir: 'android',
        manifestPath:
          '../node_modules/react-native-test-app/android/app/src/main/AndroidManifest.xml',
      }),
      ios: platformIOS(),
    },
    remoteCacheProvider: providerGitHub({
      owner: 'thorgas',
      repository: 'react-native-surrealdb',
      token: process.env.GITHUB_TOKEN,
    }),
    fingerprint: {
      extraSources: [
        '../../crates/surrealdb-rn-core',
        '../../Cargo.toml',
        '../../Cargo.lock',
        '../../rust-toolchain.toml',
        '../../packages/react-native-surrealdb/android',
        '../../packages/react-native-surrealdb/cpp',
        '../../packages/react-native-surrealdb/ios',
        '../../packages/react-native-surrealdb/src',
        '../../packages/react-native-surrealdb/package.json',
        '../../packages/react-native-surrealdb/Surrealdb.podspec',
        '../../packages/react-native-surrealdb/ubrn.config.yaml',
      ],
      ignorePaths: [
        '../../packages/react-native-surrealdb/SurrealDbRnFramework.xcframework',
        '../../packages/react-native-surrealdb/android/.cxx',
        '../../packages/react-native-surrealdb/android/build',
        '../../packages/react-native-surrealdb/android/src/main/jniLibs',
      ],
      env: [],
    },
  };
}
