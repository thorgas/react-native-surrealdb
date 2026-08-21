const path = require('node:path');

function createMetroConfig({
  exclusionList,
  hostRoot,
  makeMetroConfig,
}) {
  const workspaceRoot = path.resolve(hostRoot, '../..');
  const blockList = exclusionList([], hostRoot).map(
    pattern =>
      new RegExp(
        pattern.source.replace('|__tests__', ''),
        pattern.flags,
      ),
  );

  return makeMetroConfig({
    projectRoot: hostRoot,
    watchFolders: [
      workspaceRoot,
      path.resolve(hostRoot, '../harness-shared'),
    ],
    resolver: {
      blockList,
      extraNodeModules: {
        '@op-engineering/op-sqlite': path.resolve(
          hostRoot,
          'node_modules/@op-engineering/op-sqlite',
        ),
        react: path.resolve(hostRoot, 'node_modules/react'),
        'react-native': path.resolve(
          hostRoot,
          'node_modules/react-native',
        ),
        'react-native-safe-area-context': path.resolve(
          hostRoot,
          'node_modules/react-native-safe-area-context',
        ),
      },
      nodeModulesPaths: [
        path.resolve(hostRoot, 'node_modules'),
        path.resolve(workspaceRoot, 'node_modules'),
      ],
    },
    transformer: {
      getTransformOptions: async () => ({
        transform: {
          experimentalImportSupport: false,
          inlineRequires: false,
        },
      }),
    },
  });
}

module.exports = { createMetroConfig };
