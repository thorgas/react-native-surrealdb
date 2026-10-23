export default {
  preset: 'react-native-harness',
  roots: ['<rootDir>/__tests__'],
  testMatch: [
    process.env.BOUNDED_PHASE === 'locale'
      ? '<rootDir>/__tests__/locale-sort.harness.ts'
      : '<rootDir>/__tests__/bounded-view.harness.ts',
  ],
  testTimeout: 180000,
  watchman: false,
};
