# Changelog

All notable changes to this package are documented here.

## 0.1.0-alpha.1

- Optimize release binaries with abort-on-panic behavior and retain post-build
  symbol stripping.
- Distribute iOS arm64 device/simulator and Android arm64-v8a/x86_64 artifacts.
- Add a conservative 180 MB compressed npm-package ceiling after the first
  full-architecture candidate exceeded npm's effective upload boundary.

## 0.1.0-alpha.0

- Add embedded in-memory and experimental persistent SurrealKV connections.
- Expose `transaction()` and `beginTransaction()` JavaScript handles backed by
  native SurrealDB transaction IDs, including automatic callback rollback,
  explicit commit/cancel, query variables, and database-close cancellation.
- Add remote WebSocket connections, authentication, and cancellable live queries.
- Add multicast live-query subscriptions and the optional `useLiveQuery` React hook.
- Add generated Hermes JSI bindings for the official SurrealDB Rust SDK.
- Preserve SurrealDB-specific values across the JavaScript/Rust boundary.
- Add opt-in benchmark diagnostics that separate embedded SDK execution,
  package codecs/bindings, and JavaScript result decoding without adding
  timing overhead to normal queries.
- Add transaction `queryBatch()` and repeated-template `executeBatch()` APIs
  that send bulk work through one asynchronous native call.
- Stream lossless result JSON directly from SurrealDB values and decode parsed
  containers in place, with controlled legacy/optimized benchmark variants.
- Ship prebuilt native archives for supported iOS and Android architectures.
