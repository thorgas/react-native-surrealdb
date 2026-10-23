const base = require('../metro.config');
const blockList = Array.isArray(base.resolver.blockList)
  ? base.resolver.blockList
  : [base.resolver.blockList].filter(Boolean);
module.exports = {
  ...base,
  projectRoot: __dirname,
  server: {
    ...base.server,
    enhanceMiddleware: middleware =>
      require('./capture-metro.cjs')(
        base.server?.enhanceMiddleware
          ? base.server.enhanceMiddleware(middleware)
          : middleware,
      ),
  },
  resetCache: true,
  resolver: {
    ...base.resolver,
    useWatchman: false,
    blockList: [...blockList, /[/\\]roadmap-next-runtime[/\\]output[/\\]/],
  },
};
