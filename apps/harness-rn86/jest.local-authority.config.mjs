export default {
  preset: 'react-native-harness',
  roots: ['<rootDir>/e2e'],
  testMatch: ['<rootDir>/e2e/local-authority.harness.[jt]s?(x)'],
  testTimeout: 120_000,
  watchman: false,
};
