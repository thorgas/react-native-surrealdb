const {
  exclusionList,
  makeMetroConfig,
} = require('@rnx-kit/metro-config');
const {
  createMetroConfig,
} = require('../harness-shared/metro-config');

module.exports = createMetroConfig({
  exclusionList,
  hostRoot: __dirname,
  makeMetroConfig,
});
