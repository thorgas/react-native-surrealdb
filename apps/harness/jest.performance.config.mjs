export default {
  preset: 'react-native-harness',
  testMatch: ['<rootDir>/__benchmarks__/**/*.performance.harness.[jt]s?(x)'],
  testTimeout: 300_000,
  watchman: false,
};
