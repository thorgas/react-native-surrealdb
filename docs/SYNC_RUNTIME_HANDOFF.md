# Sync runtime adapter handoff

Updated: 2026-08-29

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
- Process-restart E2E commit: `106305f13c6d66829da17f4ec656e9d37cccaee9`.
- Reverted checkpoint-binding experiment: `8bcf29e288111d8e2c1a8f89877264be96cac559`,
  restored by `8f9265ba74cea68a1b7d4866d09f9e47d9d1a9e1`.
- Review: [draft PR #12](https://github.com/thorgas/react-native-surrealdb/pull/12).
- Published branch: `origin/feat/sync-runtime-crates`.
- Canonical codec source: private `main` (also `feat/canonical-codec-v1`) at
  `2032066722ccb0202f2f8481f30fd5c70f4d681e`.
- Native canonical HTTP codec commit: `c383a11`.
- Accepted authority outcome persistence commit: `be8be3b`.
- Authority pull/reset persistence commit: `4aa43a1`.

## Source boundary

- Branch base: `react-native-surrealdb` `origin/main` at
  `b1e6a01dc4f544fc5d647d85ca2cf5732e1cd35c`.
- Copied source: private canonical `surrealdb-sync-engine.dev` `main` at
  `056374ac5430e1e09ee73ab30b4d8c20247a2f68`.
- Canonical codec update: private `main` at
  `2032066722ccb0202f2f8481f30fd5c70f4d681e`.
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

Durable state still stores the existing tagged JSON representation, but native enqueue now ignores
the caller's prototype fingerprint and recomputes it from the protocol-owned bounded
`CanonicalValue`/CBOR profile. Enqueue, push conflicts, pull/reset records, loaded state, and every
prepared replacement fail closed when a value is unsupported, oversized, or over-nested. Pending
commit fingerprints are revalidated against their content. An accepted ID mapping deliberately
remaps the resolved recoverable intent while retaining the authority identity bound to the original
pre-remap commit; remapped content is bounds-validated but cannot be re-fingerprinted as the same
identity. The TypeScript facade uses the same
lossless bridge as query variables, preserving `bigint`, bytes, `NONE`, and structured record links.
The safe subset deliberately excludes floats, decimals, UUID/range record keys, dates, sets, and
other undecided kinds.

`packages/react-native-surrealdb/src/sync-http.ts` adds an application-owned HTTP adapter around
the facade. It serializes calls, submits each pending commit to `POST /v1/sync/push`, pulls from
`POST /v1/sync/pull`, forwards the application token, and accepts only HTTP 200 protocol outcomes.
The application must inject the codec and fetch implementation. The native client owns the opaque
checkpoint with its records, cursor, scope snapshot, and outbox, so applying a complete pull is one
embedded transaction.

`crates/surrealdb-sync-protocol/src/http_codec.rs` is synchronized byte-for-byte from private commit
`2032066`. Its definite-length CBOR grammar covers all push/pull requests and responses under a
4 MiB/4,096-item aggregate budget, applies smaller identifier/container/checkpoint limits, rejects
noncanonical or hostile messages before allocation, and binds commit fingerprints to partition and
client. `crates/surrealdb-rn-core/src/sync_http_codec.rs` exposes four async UniFFI functions:
`encode_sync_push_request`, `decode_sync_push_response`, `encode_sync_pull_request`, and
`decode_sync_pull_response`. The public `createExperimentalCanonicalCborSyncHttpCodec()` constructor
wires these bindings without sending protocol `u64` values through JavaScript numbers. The injected
JSON codec remains a lossy test fallback and rejects unsafe `bigint` values.
It deliberately provides no authority, background scheduler, implicit retry/backoff, or WebSocket
ordering. WebSockets remain notification hints that cause the application to pull.

The HTTP adapter now reads the opaque checkpoint from the same native durable state that owns the
confirmed records, cursor, scope snapshot, and outbox. A complete pull is therefore one embedded
transaction instead of a native state write followed by a second application checkpoint write.
The generated async `checkpointToken()` method is valid on both platforms when the Rust shared
library and generated C++/TypeScript bindings are rebuilt together. The earlier reverted experiment
paired a changed method table with a plausibly stale ignored Android `.so`; exact historical logs
were not retained, so that cause remains an evidence-backed inference rather than a proven fact.

## Verification

From the repository root:

```bash
cargo fmt --all --check
cargo clippy -p surrealdb-rn-core --all-targets -- -D warnings
cargo test -p surrealdb-rn-core sync_state
cargo test -p surrealdb-rn-core sync_client
cargo test -p surrealdb-rn-core sync_codec
cargo test -p surrealdb-rn-core sync_http_codec
cargo test -p surrealdb-rn-core authority_adapter_response_persists_across_surrealkv_reopen
cargo test -p surrealdb-rn-core adapter_pull_cbor_reopens_with_durable_batch_and_reset_state
cargo test -p surrealdb-sync-protocol http_codec
pnpm --filter react-native-surrealdb run test
pnpm --filter react-native-surrealdb run typecheck
pnpm --filter react-native-surrealdb run build
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter surrealdb-harness-rn86 run e2e:ios
pnpm --filter surrealdb-harness-rn86 run e2e:android
pnpm --filter surrealdb-harness-rn86 run e2e:sync-restart:ios
pnpm --filter surrealdb-harness-rn86 run e2e:sync-restart:android
./scripts/verify-core.sh
```

The focused adapter tests cover native-computed fingerprints, lossless tagged canonical values,
hostile depth, unsupported enqueue/push/pull values without mutation, revision conflicts,
idempotent retry, explicit rollback, committed
replacement, malformed and semantically corrupt payloads, size limits, structured identity keys,
SurrealKV close/reopen with an uncommitted transaction, atomic optimistic create/delete, facade
reopen, durable conflict retention, scope drift, malformed input, exact HTTP request shape,
authorization failures, lost responses, explicit retry, call serialization, cancellation, injected
codec use, incomplete-pull rejection, and atomic native checkpoint persistence. The shared native
harness executes enqueue, optimistic visibility, facade reopen, and conflict reconciliation through
Hermes.

The HTTP codec tests replay all four private golden messages, round-trip every protocol message and
canonical value variant, and reject malformed, noncanonical, oversized, deeply nested, type-confused,
and ambiguous tagged-object inputs. The package suite also runs the adapter against a real localhost
HTTP server. No production data or credentials are used.

All documented commands passed on 2026-08-28. `./scripts/verify-core.sh` ran formatting,
warning-denied workspace Clippy, all workspace tests, and Rust checks for the iOS arm64 simulator
and Android arm64 targets. Package build, type checking, Vitest, package verification, and the
five-host React Native TypeScript matrix also passed. The RN 0.86 host runs ten shared Hermes tests;
the sync cases prove native-computed fingerprints, optimistic visibility, facade reopen, pending
recovery, accepted temporary-ID mapping, conflict durability, and canonical rollback.

The E2E run also repaired pre-existing harness setup gaps: each host now has a tiny local test proxy
that imports the shared suites, host app IDs and the iOS scheme are explicit, `e2e:ios` and
`e2e:android` perform build/install/test in one command, and `.node-version` selects Node 22.22.0.
No UI changed, so screenshots are not applicable.

The dedicated restart suites derive an app-private SurrealKV endpoint from the
already-installed op-sqlite native path constants. The seed phase writes a
database marker and a pending optimistic sync commit, deliberately leaves both
native handles open, and lets Harness terminate the process. A separately
loaded verification file reopens the same database, proves the marker, outbox,
and optimistic row survived, records a durable conflict, then cleans up. This
passed with the canonical-fingerprint changes on an iPhone 17 Pro simulator and
a Pixel 9 Android API 36 emulator. The test changes no UI.

The monorepo also carries a narrow pnpm patch for React Native Harness 1.3.0.
Its Metro resolver previously selected the hoisted RN 0.85 Harness runtime for
the RN 0.86 entry bundle while shared tests imported the RN 0.86 runtime. The
patch resolves the runtime relative to the active host package, and the shared
Metro config likewise pins `react-native-harness` to that host. Without it,
Harness either attempted to define its immutable Jest guard twice or collected
tests into a different runtime context.

The RN 0.86 shared harness also exercises the HTTP adapter through Hermes against a redacted mocked
authority while the embedded native client holds real pending state. It verifies accepted push,
reset pull, pending removal, cursor advancement, and native checkpoint persistence. The process-
restart seed additionally encodes a real pending commit and decodes the private pull golden message
through the public canonical codec on both iOS and Android. This is a native-boundary E2E, not a
deployed-server test.

Authority coverage is currently a split conformance chain. Private branch
`feat/surrealdb-authority-pull` replays all nine redacted fixtures through the transactional
SurrealDB 3.2.4 adapter on SurrealKV and RocksDB and pins accepted, pull-batch, and reset canonical
CBOR responses. Native tests decode those exact responses, apply them in the embedded client, fully
close/drop/reopen SurrealKV, and prove durable outcome, checkpoint/cursor, confirmed records, reset
replacement, pending outbox, and optimistic replay. The package separately proves a real TCP client
boundary, while the device tests prove the same codec through Hermes. A single deployed
client-to-authority HTTP E2E is still not claimed: HTTP routing, authentication integration, the raw
changefeed scanner, token/retention policy, and production listener remain separate boundaries.

## Next implementation slices

1. Expose the reviewed private push/pull adapter through an authenticated deployment boundary and
   run one deployed client-to-authority E2E.
2. Add scheduling, retry/backoff, connectivity policy, and WebSocket-only invalidation hints around
   the explicit HTTP-correctness path.
3. Replace the copied crates with the agreed single-source/public-export mechanism before release.
4. Extend the canonical value profile only through private protocol decisions and golden vectors.

## Hard blockers

- The long-term single-source/export mechanism is unresolved.
- GitHub Actions may remain unavailable until account billing permits runner allocation.
- The HTTP adapter is only application-owned orchestration; authority deployment and authentication
  integration remain separate work.
- The authority still needs a privileged raw changefeed scanner plus explicit retention and opaque
  checkpoint-token generation/signing before production deployment.
- This branch must remain a draft and must not be released or advertised as sync support.

Only GitHub runner/billing availability is a user-side operational blocker. The codec, transport,
and authority items are engineering/architecture follow-ups, not account setup tasks.
