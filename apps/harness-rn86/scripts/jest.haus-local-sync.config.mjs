export default {
  rootDir: '..',
  preset: 'react-native-harness',
  roots: ['<rootDir>/e2e'],
  testMatch: ['<rootDir>/e2e/haus-local-sync.harness.[jt]s?(x)'],
  testTimeout: 120_000,
  watchman: false,
};
