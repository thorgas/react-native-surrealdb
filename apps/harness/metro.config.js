const { exclusionList, makeMetroConfig } = require("@rnx-kit/metro-config");
const path = require("node:path");

const workspaceRoot = path.resolve(__dirname, "../..");
// RNX Kit excludes __tests__ by default. Harness bundles its selected test
// file into the device runtime, so preserve every other default exclusion but
// keep that directory visible to Metro.
const blockList = exclusionList([], __dirname).map(
  (pattern) => new RegExp(pattern.source.replace("|__tests__", ""), pattern.flags)
);

module.exports = makeMetroConfig({
  projectRoot: __dirname,
  watchFolders: [workspaceRoot],
  resolver: {
    blockList,
    nodeModulesPaths: [
      path.resolve(__dirname, "node_modules"),
      path.resolve(workspaceRoot, "node_modules"),
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
