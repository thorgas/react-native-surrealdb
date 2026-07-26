export default {
  preset: 'react-native-harness',
  testMatch: [
    '<rootDir>/../harness-shared/__tests__/**/*.harness.[jt]s?(x)',
  ],
  testTimeout: 30_000,
  watchman: false,
};
