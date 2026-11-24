## Unreleased

- **Breaking native runtime:** embedded SurrealDB is now `4.0.1` (was `3.2.4`).
  Rebuild the native libraries.
- SurrealDB 4 refuses to open SurrealKV stores written by 3.x. The default
  `migrate-from-3x` Rust feature migrates such a store on the first `connect`:
  it exports every database with a bundled 3.2.4 engine, imports it into a new
  4.x store at the same path, checks row counts and removes the old copy. An
  interrupted migration is rolled back and retried on the next open. Stores
  with root, namespace or database users or accesses are not migrated; `connect`
  fails and leaves the 3.x data unchanged.
- New tagged wire values `date`, `time`, `timestamp` and `interval` arrive as
  `SurrealSqlValue`. Integers and floats of the new sized numeric types keep
  the existing `int` / `float` / `decimal` wire shapes.
- Harness hosts for React Native 0.87.1 and 0.88.1.

## 0.1.0-alpha.13.haus.20261116.1 — private artifact

## 0.1.0-alpha.14.haus.20261116.1

- Add typed metadata-only rejected command pages and idempotent keep-server resolution.
- Atomically close refused commands, rebase eligible aggregate successors and promote FIFO without losing later intent. Existing rejected journals require no reset or migration.
- Keep receipt gates, queue bounds, v1/v2 wire compatibility and linkable iOS archives. Matching native binaries are required; see `docs/REJECTED_COMMAND_RESOLUTION.md` in the SDK repository.

- Materialize tombstones safely when a fresh local application table does not
  exist. Guard projection deletion with pinned 3.2.4 record::exists inside
  the same transaction; keep confirmed tombstone versions and checkpoint,
  pending, outcome and ownership metadata. Do not create unused application
  tables or suppress other database errors.
- Add fresh memory/SurrealKV bootstrap cases without pre-created application
  tables, including a tombstone before the first live row of the same family
  and a family containing only a tombstone. Native ABI and wire protocol stay
  unchanged. Rebuild native libraries; embedded SurrealDB remains 3.2.4.

## 0.1.0-alpha.12.haus.20261116.1 — private artifact

- Keep original iOS release archives without an additional strip pass.
  Xcode 27's strip (including debug-only `-S`) corrupts AWS-LC assembly unwind relocations, causing
  consumer app links to fail. Packaging now partially links every object in
  both iOS archives before packing. Rust/queue code and embedded 3.2.4 stay
  unchanged from alpha.11. Alpha.11 is rejected for iOS consumption.

## 0.1.0-alpha.11.haus.20261116.1 — rejected for iOS

- Fingerprint multi-record commands and retain whole overlapping aggregates in
  the durable successor queue. Promote after exact receipt/pull coverage and
  rebase every parent/child version atomically. Preserve new-child absence,
  tombstones, conflict intent, FIFO, count/byte limits and ID mappings.
- Advertise the optional `atomic-aggregate-successors-v1` queue profile through
  the existing read-capabilities ABI. Keep embedded SurrealDB pinned to 3.2.4
  and the HTTP protocol unchanged. Requires matching native app builds.
- Private artifact and reproducible source patch in the private consuming app;
  no npm publication or public SDK push.

## 0.1.0-alpha.10.haus.20261115.1 — private artifact

- Synchronize private engine ADR 0017: one record or commit may use 65,536
  canonical items (was 4,096), while envelopes that pack several records or
  commits keep the 4,096 bound. Rebuild all four native libraries; a consuming
  app needs this build to receive a household record over 4,096 items.
- Private tarball only; no npm publication or public release tag.

## 0.1.0-alpha.8.haus.20261114.1 — private artifact

- Rebuild all four mobile native libraries from merged main, including bounded pending queue admission/read APIs and exact bigint CAS handling.
- Private tarball only; no npm publication or public release tag.

## 0.1.0-alpha.7.haus.20261023.1 — private wrapper artifact

- Package bounded native local-view count/page reads with matching generated
  bindings and Android arm64/x86_64 plus iOS device/simulator arm64 libraries.
- Retain the exact Rust inputs from `fb1164ebf98469a64e41f0422938cb2bb23a5111`
  and the existing Rust crate version. This identifies an immutable private
  wrapper package, not a new Rust source release or public SDK publication.
- Android arm64 synthetic acceptance and cross-platform static ABI checks do not
  establish consuming-app startup, iOS runtime acceptance or production readiness.

## 0.1.0-alpha.6.haus.20261020.1 — private artifact candidate

- Add read-only native bootstrap request builders preserving independently bound
  context and opaque stage ownership. Matching regenerated native artifacts and
  rebuilt apps are required; no new public release is authorized by this entry.

# Changelog

All notable changes to this package are documented here.

## 0.1.0-alpha.4.haus.20261019.1

- Private Hauswirtschaft native candidate; no public tag or npm publication.
- Add bounded paged bootstrap staging and whole-storage ownership inspection.
- Keep admitted native mutations owned through cancellation and database settlement;
  fail closed after ambiguous or unexpectedly dropped writes.
- Requires matching rebuilt iOS/Android binaries; host verification alone does not
  establish mobile acceptance or production sync readiness.

## 0.1.0-alpha.3

- Register the iOS `Surrealdb` TurboModule provider explicitly so a clean Expo
  prebuild can load the native module. Verified in a local shopping sync device
  build; this is still an experimental prerelease, not a production-ready sync
  integration.

## 0.1.0-alpha.2

- Add an experimental crash-safe native sync client with optimistic records,
  durable pending commits, checkpoints, and explicit keep-server, keep-local,
  and application-supplied merge conflict resolution.
- Add bounded canonical CBOR HTTP push/pull codecs and a transport adapter;
  the application still supplies its authenticated authority and access token.
- Preserve complete pull batches, accepted outcomes, ID mappings, and unresolved
  conflicts across SurrealKV reopen. Reject malformed or oversized sync values.
- Add local two-client authority harness coverage for concurrent writes,
  durable conflicts, recovery, and periodic visibility. This does not enable
  synchronization in a consuming app automatically.

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
