function createReactNativeConfig({ configureProjects }) {
  const project = configureProjects({
    android: {
      sourceDir: 'android',
    },
    ios: {
      sourceDir: 'ios',
    },
  });

  return {
    project,
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
}

module.exports = { createReactNativeConfig };
