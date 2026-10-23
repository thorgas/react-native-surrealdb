export default {
  preset: 'react-native-harness',
  roots: ['<rootDir>/__tests__'],
  testMatch: ['<rootDir>/__tests__/locale-sort.harness.ts'],
  testTimeout: 180000,
  watchman: false,
};
