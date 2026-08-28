# Sync runtime adapter handoff

Updated: 2026-08-28

## Branch goal

Stage temporary copies of the client-side Rust protocol/runtime crates and prove the complete local
prototype path from React Native through UniFFI to crash-safe embedded state. The branch exposes an
explicitly experimental API but is not usable as a synchronization engine.

- Runtime-copy commit: `9667aec2adc1a1b739d3742df8fa4439237501ec`.
- Persistence-adapter commit: `0f06fd777e1dd4e4aae06f86a16ab293f54c7b44`.
- Bounded-decoder commit: `b0b7b5a`.
- Combined domain/state transaction commit: `524500f`.
- Native/TypeScript facade commit: `1909d60`.
- Recovery/error contract commit: `c42afbb`.
- Harness reliability commit: `fdf7beb`.
- Review: [draft PR #12](https://github.com/thorgas/react-native-surrealdb/pull/12).
- Published branch: `origin/feat/sync-runtime-crates`.

## Source boundary

- Branch base: `react-native-surrealdb` `origin/main` at
  `b1e6a01dc4f544fc5d647d85ca2cf5732e1cd35c`.
- Copied source: private canonical `surrealdb-sync-engine.dev` `main` at
  `056374ac5430e1e09ee73ab30b4d8c20247a2f68`.
- Included: `surrealdb-sync-protocol`, `surrealdb-sync-client`, and the client's public transition
  regressions.
- Excluded: Quint specifications, authority implementation, authority-cycle/conformance/adversarial
  suites, wiki, codec experiments, deployment code, and commercial work.
- Licensing: copied crates remain Apache-2.0; the existing repository remains MIT.

The source files are intentionally unchanged from the canonical commit. Only Cargo manifests,
public documentation, and the reduced public-test boundary differ. Until a proper export/dependency
mechanism exists, protocol changes must originate in the private canonical repository.

## Implemented local prototype

`crates/surrealdb-rn-core/src/sync_state.rs` adds a Rust-only adapter:

- `_sync_client_state` is a private schemafull local table.
- Each record uses the structured SurrealDB ID
  `_sync_client_state:[partition_id, client_id]`; identities are never concatenated into SurrealQL.
- The record stores format version, envelope identity, revision, and a bounded byte payload.
- `DurableClientState<serde_json::Value>` is serialized as JSON with a 4 MiB maximum.
- Initial writes require an absent row at revision zero. Later writes compare the durable prior
  revision and require the next revision.
- A byte-identical retry of an already committed revision is idempotent.
- Loads verify the format, envelope, revision, payload bound, JSON, and all client-state invariants.
  Corruption is an error and is never treated as an empty replica.
- Operation failures explicitly cancel the transaction. Commit errors are reported as an unknown
  outcome; the native facade then requires reopen/reconciliation before another mutation.
- Optimistic domain-record changes and the durable state replacement commit in the same embedded
  SurrealDB transaction. Rejected transitions leave both unchanged.
- Simple `table:key` record IDs, nested objects, arrays, tagged record links, references, deletes,
  conflicts, and pull application are mapped to native values. The private sync-state table cannot
  be targeted by protocol operations.
- `open_sync_client` and `NativeSyncClient` expose named enqueue, push-outcome, pull, pending,
  conflict, status, close, and closed-state operations over UniFFI.
- `ExperimentalSyncClient` is the hand-written TypeScript facade. `SurrealClient` exposes it through
  `openExperimentalSync()` only on embedded databases.

The JSON value/payload choice is a persistence prototype, not the canonical sync wire codec.
The facade deliberately performs no network, authentication, authority, retry scheduling, or
background work.

## Verification

From the repository root:

```bash
cargo fmt --all --check
cargo clippy -p surrealdb-rn-core --all-targets -- -D warnings
cargo test -p surrealdb-rn-core sync_state
cargo test -p surrealdb-rn-core sync_client
pnpm --filter react-native-surrealdb run test
pnpm --filter react-native-surrealdb run typecheck
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter surrealdb-harness-rn86 run e2e:ios
pnpm --filter surrealdb-harness-rn86 run e2e:android
./scripts/verify-core.sh
```

The focused adapter tests cover revision conflicts, idempotent retry, explicit rollback, committed
replacement, malformed and semantically corrupt payloads, size limits, structured identity keys,
SurrealKV close/reopen with an uncommitted transaction, atomic optimistic create/delete, facade
reopen, durable conflict retention, scope drift, and malformed input. The shared native harness
executes enqueue, optimistic visibility, facade reopen, and conflict reconciliation through Hermes.

All documented commands passed on 2026-08-28. `./scripts/verify-core.sh` ran formatting,
warning-denied workspace Clippy, all workspace tests, and Rust checks for the iOS arm64 simulator
and Android arm64 targets. Package build, type checking, Vitest, package verification, and the
five-host React Native TypeScript matrix also passed. The RN 0.86 host ran all seven shared Hermes
tests on both an iPhone 17 Pro simulator and a Pixel 9 Android emulator; the new sync case proved
optimistic visibility, facade reopen, pending recovery, conflict durability, and canonical rollback.

The E2E run also repaired pre-existing harness setup gaps: each host now has a tiny local test proxy
that imports the shared suites, host app IDs and the iOS scheme are explicit, `e2e:ios` and
`e2e:android` perform build/install/test in one command, and `.node-version` selects Node 22.22.0.
No UI changed, so screenshots are not applicable.

## Next implementation slices

1. Select the canonical protocol value codec and fingerprint rules, then replace the prototype JSON
   boundary in `crates/surrealdb-sync-protocol`, `crates/surrealdb-rn-core`, and
   `packages/react-native-surrealdb/src/sync.ts`.
2. Add an app-private SurrealKV path to the harness and prove full process/database restart on iOS
   and Android in `apps/harness-shared`.
3. Add an application-owned HTTP push/pull adapter around the facade; keep WebSockets as pull hints.
4. Integrate the reviewed authority endpoint and authorization/checkpoint semantics separately.
5. Replace the copied crates with the agreed single-source/public-export mechanism before release.

## Hard blockers

- The long-term single-source/export mechanism is unresolved.
- GitHub Actions may remain unavailable until account billing permits runner allocation.
- The canonical native-value storage/wire codec is not selected yet.
- Device-level full-process SurrealKV restart needs a cross-platform app-private path mechanism.
- The HTTP authority deployment shape and authentication integration remain separate work.
- This branch must remain a draft and must not be released or advertised as sync support.

Only GitHub runner/billing availability is a user-side operational blocker. The codec, path,
transport, and authority items are engineering/architecture follow-ups, not account setup tasks.
