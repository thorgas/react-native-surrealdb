const project = (() => {
  try {
    const { configureProjects } = require('react-native-test-app');
    return configureProjects({
      android: {
        sourceDir: 'android',
      },
      ios: {
        sourceDir: 'ios',
      },
      macos: {
        sourceDir: 'macos',
      },
      windows: {
        sourceDir: 'windows',
        solutionFile: 'windows/SurrealDbHarness.sln',
      },
    });
  } catch (_) {
    return undefined;
  }
})();

module.exports = {
  ...(project ? { project } : undefined),
  // Used only by scripts/measure-android-baseline.sh. This produces the stock
  // RNTA measurement without changing package.json or the pnpm dependency graph.
  ...(process.env.SURREALDB_SIZE_BASELINE === '1'
    ? {
        dependencies: {
          'react-native-surrealdb': {
            platforms: { android: null, ios: null },
          },
        },
      }
    : undefined),
};
