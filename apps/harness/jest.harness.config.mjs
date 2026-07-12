export default {
  preset: 'react-native-harness',
  testMatch: ['<rootDir>/__tests__/**/*.harness.[jt]s?(x)'],
  testTimeout: 30_000,
  watchman: false,
};
