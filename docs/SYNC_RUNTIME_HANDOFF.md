# Sync runtime adapter handoff

Updated: 2026-08-30

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
- Local durable benchmark implementation: `1a2d738`.
- Finite binary64 source: private `feat/application-shaped-runtime-v1` at `d26e07b`.
- Canonical codec source: private `main` (also `feat/canonical-codec-v1`) at
  `2032066722ccb0202f2f8481f30fd5c70f4d681e`.
- Native canonical HTTP codec commit: `c383a11`.
- Accepted authority outcome persistence commit: `be8be3b`.
- Authority pull/reset persistence commit: `4aa43a1`.
- Local-authority host resolver commit: `6a9c921`.
- Live local-authority device E2E commit: `2e86ddc`.
- SurrealDB 3.2.4 alignment branch: `origin/feat/surrealdb-3.2.4-alignment`.
- Exact engine/benchmark metadata alignment commit: `8b02b2d`.
- Cross-version SurrealKV reopen commit: `f69a285`.
- SurrealKV lifecycle/RSS commit: `24b0818`.
- Bounded HTTP transport commit: `fbb86d7`.
- Durable retry scheduler commit: `5cfaebd`.
- Pull-only WebSocket hints commit: `d03e322`.
- Scheduler documentation commit: `cc2a255`.
- Cross-platform Hermes scheduler E2E commit: `394b269`.
- Scheduler hardening commit and integrated draft head:
  `9dbf2befad234b6d66332092c313e5dea9810040`.
- Application lifecycle/authentication coordinator commit: `df9b6c4`.
- Cross-platform local-authority lifecycle E2E and documentation commit: `0412b00`.
- Finite binary64 application-value commit: `5d368a6`.
- Normalized durable sync-storage commit: `a7b9f9e`.
- Published application-shaped branch: `origin/feat/normalized-sync-storage-v1`.

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

The native runtime now pins SurrealDB `3.2.4`, matching the private authority and local Docker
stack. The lockfile changes only the six SurrealDB-family packages; the storage engine remains
`surrealkv 0.21.2`, exactly as under the previous SurrealDB `3.2.1` pin. This removes the known
cross-repository engine-version mismatch but is not evidence for every upstream behavioral change.

## Implemented local prototype

`crates/surrealdb-rn-core/src/sync_state.rs` adds a Rust-only adapter. Its v2 format replaces the
single aggregate state blob with normalized private rows:

- `_sync_client_meta`, `_sync_client_confirmed`, `_sync_client_outbox`, `_sync_client_outcome`, and
  `_sync_client_id_map` are schemafull private tables with structured composite IDs.
- Tagged JSON rows preserve integer types as canonical decimal strings. Each row is bounded to
  4 MiB; a client is bounded to 16,384 components and 64 MiB aggregate decoded bytes.
- The metadata row owns revision, scope, checkpoint, and stable ordered component keys. State
  transitions write only changed component rows plus metadata in the optimistic-domain transaction.
- A validated v1 `_sync_client_state` blob migrates atomically to v2. The old row becomes a small
  invalid sentinel so an older binary fails closed instead of replaying stale outbox/checkpoint data.
- Initial writes require an absent row at revision zero. Later writes compare the durable prior
  revision and require the next revision.
- A byte-identical retry of an already committed normalized revision is idempotent.
- Loads verify formats, row identities, stable keys, payload/aggregate bounds, tagged JSON, and all
  client-state invariants.
  Corruption is an error and is never treated as an empty replica.
- Operation failures explicitly cancel the transaction. Commit errors are reported as an unknown
  outcome; the native facade then requires reopen/reconciliation before another mutation.
- Optimistic domain-record changes and the durable state replacement commit in the same embedded
  SurrealDB transaction. Rejected transitions leave both unchanged.
- Simple `table:key` record IDs, nested objects, arrays, tagged record links, references, deletes,
  conflicts, and pull application are mapped to native values. The private sync-state tables cannot
  be targeted by protocol operations.
- `open_sync_client` and `NativeSyncClient` expose named enqueue, push-outcome, pull, pending,
  conflict, status, close, and closed-state operations over UniFFI.
- `ExperimentalSyncClient` is the hand-written TypeScript facade. `SurrealClient` exposes it through
  `openExperimentalSync()` only on embedded databases.

Normalized persistence removes the former 4 MiB whole-replica ceiling and reduces write
amplification, but `ClientRuntime` still clones the complete in-memory durable state while preparing
a transition. A storage-aware transition interface remains necessary for very large replicas.

The normalized loader also inventories every component table for the client and fails closed on
missing metadata, orphan or extra rows, duplicate identities, oversized keys, noncanonical tagged
JSON, and same-revision state divergence. Migration is deliberately one-way: a v2 sentinel replaces
the legacy aggregate row so an older binary cannot resume from stale durable state.

Durable state still stores the existing tagged JSON representation, but native enqueue now ignores
the caller's prototype fingerprint and recomputes it from the protocol-owned bounded
`CanonicalValue`/CBOR profile. Enqueue, push conflicts, pull/reset records, loaded state, and every
prepared replacement fail closed when a value is unsupported, oversized, or over-nested. Pending
commit fingerprints are revalidated against their content. An accepted ID mapping deliberately
remaps the resolved recoverable intent while retaining the authority identity bound to the original
pre-remap commit; remapped content is bounds-validated but cannot be re-fingerprinted as the same
identity. Finite binary64 values now cover recipe-shaped fractional quantities and measurements;
signed zero normalizes, integers remain a distinct protocol type, and decimals remain unsupported.
The TypeScript facade uses the same lossless bridge as query variables, preserving `bigint`, bytes,
`NONE`, and structured record links. The safe subset deliberately excludes decimals, UUID/range
record keys, dates, sets, and other undecided kinds.

`packages/react-native-surrealdb/src/sync-http.ts` adds an application-owned HTTP adapter around
the facade. It serializes calls, submits each pending commit to `POST /v1/sync/push`, pulls from
`POST /v1/sync/pull`, forwards the application token, and accepts only HTTP 200 protocol outcomes.
The application must inject the codec and fetch implementation. The native client owns the opaque
checkpoint with its records, cursor, scope snapshot, and outbox, so applying a complete pull is one
embedded transaction.

The adapter bounds request and response bodies, validates response content type, requires HTTPS
outside an explicit loopback-only development escape hatch, and times out token acquisition, codec
work, fetch, body read, and decode. `ExperimentalSyncScheduler` adds one active cycle at a time,
coalesced triggers, injected connectivity, bounded full-jitter transient retry, auth/terminal halt,
60-second default periodic pull, and lifecycle-generation-safe stop/restart. Terminal failures remain
halted until explicit manual/local-mutation recovery or an explicit scheduler restart. Scheduler timers are advisory and non-durable;
restart reads the native outbox and checkpoint. `ExperimentalSyncWebSocketHints` requires WSS outside
an explicit loopback-only development escape hatch, obtains a fresh application-owned URL/ticket for
each connection, throttles messages, and can only wake a pull. Its size check closes an oversized
frame after the runtime has received it; the authority/proxy must enforce a pre-allocation frame
limit. Hints cannot advance a cursor or establish ordering, and periodic HTTP pull remains the
correctness fallback.

`crates/surrealdb-sync-protocol/src/http_codec.rs` is synchronized byte-for-byte from private commit
`2032066`. Its definite-length CBOR grammar covers all push/pull requests and responses under a
4 MiB/4,096-item aggregate budget, applies smaller identifier/container/checkpoint limits, rejects
noncanonical or hostile messages before allocation, and binds commit fingerprints to partition and
client. `crates/surrealdb-rn-core/src/sync_http_codec.rs` exposes four async UniFFI functions:
`encode_sync_push_request`, `decode_sync_push_response`, `encode_sync_pull_request`, and
`decode_sync_pull_response`. The public `createExperimentalCanonicalCborSyncHttpCodec()` constructor
wires these bindings without sending protocol `u64` values through JavaScript numbers. The injected
JSON codec remains a lossy test fallback and rejects unsafe `bigint` values.
It deliberately provides no authority or WebSocket ordering/durability.

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
pnpm --filter surrealdb-harness-rn86 run e2e:local-authority:ios
pnpm --filter surrealdb-harness-rn86 run e2e:local-authority:android
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-migration:ios
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-migration:android
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-churn:ios
pnpm --filter surrealdb-harness-rn86 run e2e:surrealkv-churn:android
./scripts/verify-core.sh
```

The local-authority commands require the private development checkout and its running stack:

```bash
/absolute/path/to/surrealdb-sync-engine.dev/scripts/local-dev.sh up
SYNC_ENGINE_DEV_REPO=/absolute/path/to/surrealdb-sync-engine.dev \
  pnpm --filter surrealdb-harness-rn86 run e2e:local-authority:ios
```

Omit `SYNC_ENGINE_DEV_REPO` when the private and React Native repositories use their normal sibling
locations. The runner requires `.local-dev/env` to have mode `600`, validates but never prints the
token, writes it to one ignored mode-`600` module for the device test, and removes that module on
exit. Normal unit and harness suites do not depend on the private checkout or live stack.

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

The SurrealDB 3.2.4 alignment passed again on 2026-08-29: workspace formatting, warning-denied
Clippy, 53 Rust tests with one existing ignored test, 21 package tests, type checking, package build,
`verify-core.sh`, regenerated iOS device/simulator plus Android arm64/x86_64 release artifacts, and
`release:check`. The live local-authority trace passed on iOS and Android against the separate
scanner process. The SurrealKV seed/process-termination/reopen suites also passed on both platforms.
The default iPhone 17 Pro launch stalled once; rebuilding on the existing Hauswirtschaft E2E
simulator succeeded without deleting either simulator. Node 26 remains outside the documented
`>=20 <23` range and emits non-fatal type-stripping warnings; the cached exact pnpm 11.5.0 runner
completed every test. No UI changed, so screenshots are not applicable.

The scheduler slice passed package build/typecheck and 33 Vitest cases, `verify-core.sh`, Release
artifact generation, and `release:check`. The standard RN 0.86 Harness suite passed 13 tests on an
iPhone 17 Pro/iOS 26.1 simulator and 13 tests on a Pixel 9/Android 16 emulator. The mocked-authority
trace starts offline, resumes a single push/pull cycle on connectivity, persists the mapped record
and checkpoint through the native facade, and handles a WebSocket-style hint as a pull-only wakeup.
The standard scripts exclude the dedicated process-restart seed/verify fixtures; those retain their
own explicit two-phase commands. Harness runs are Debug/Metro functional evidence, not bundled
Release-app evidence.

The final adversarial audit raised the package suite to 39 tests. It proved terminal errors cannot
be retriggered by hints/timers, a lost hint is recovered by the default periodic pull, a hung token
provider or codec cannot wedge the serialized queue, stale work cannot mutate a restarted lifecycle,
and insecure remote HTTP/WS endpoints fail closed. The hardened standard Harness suites again passed
13/13 on iOS and Android, and the live local-authority trace passed on both platforms through the
explicit simulator/emulator loopback escape hatch.

The permanent cross-version fixture also passed on iOS and Android on
2026-08-29. It built historical revision `e0c200b` with SurrealDB 3.2.1,
seeded app-private SurrealKV, replaced the native artifact and app in place,
then reopened the same data with SurrealDB 3.2.4. The 64-cycle lifecycle test
passed on both platforms and emitted raw RSS reports. Android recorded 47
samples (107,324–417,976 KiB; median 261,540; p95 416,436); iOS recorded 25
samples (195,040–783,552 KiB; median 641,136; p95 733,280). These Debug/Harness
measurements have no regression gate and are not release-memory claims.

The local durable sync benchmark passed on the same RN 0.86 iOS and Android
hosts. It times isolated empty-state single and ten-record atomic enqueues,
fully materialized reads of a fixed 25-commit outbox, and reopen recovery. The
file-backed SQLite control uses WAL plus `synchronous=FULL` and is explicitly
labeled a minimal lower bound because it omits protocol validation,
fingerprinting, optimistic reconstruction, and conflict bookkeeping. Matching
semantic checksums prove both sides materialized the same logical records and
outbox operations. The first Debug runs recorded 12.38/20.35 ms sync enqueue
medians on the iOS simulator and 4.61/8.30 ms on the Android emulator for
single/ten-record commits respectively. These are diagnostic values, not
release budgets.

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
client-to-authority HTTP E2E is now covered locally by an opt-in RN 0.86 trace on both an iPhone 17
Pro simulator and Pixel 9 Android emulator. Two memory-backed replicas perform initial pulls,
concurrent absent-base writes, accepted/conflict pushes, facade reopen, and final convergence through
the canonical codec. A second device scenario proves durable offline enqueue, `401` token recovery,
background stop, foreground catch-up, and periodic recovery without a hint. The coordinator owns
only lifecycle and scheduling; the application still owns its token and injected `AppState` mapping.
The private gateway uses a local development principal/scope. Its separate
bounded raw changefeed worker renews the durable frontier while clients are idle and every pull
retains a fail-closed catch-up gate. This is not a deployed-production claim: production
authentication, certified retention/rebootstrap operations, and checkpoint issuance remain
separate boundaries.

The application-shaped storage slice passed on 2026-08-30: workspace formatting, warning-denied
Clippy, all workspace tests (41 RN-core tests plus one ignored Docker-only case, nine client
transition tests, and 14 protocol tests), package Vitest under the pinned Node 22.22.0 runtime,
package type checking, and RN 0.86 harness type checking. The fixtures use synthetic recipe-shaped
nested arrays, record links, and fractional quantities; no intended consumer repository or private
application data was read or changed.

## Next implementation slices

1. Replace the fixed local identity and development checkpoint digest with the intended
   authenticated deployment boundary and production checkpoint policy; certify scanner retention,
   alerting, and administrator rebootstrap on that deployment.
2. Integrate the proven injected lifecycle/connectivity/token-refresh interfaces into the first
   application prototype and validate them against a deployed authenticated authority; retain
   periodic HTTP pull as the correctness fallback.
3. Replace the copied crates with the agreed single-source/public-export mechanism before release.
4. Extend the canonical value profile only through private protocol decisions and golden vectors.
5. Add a normal bundled Release functional runner and establish repeated pinned physical-device RSS
   baselines before defining a memory budget. Existing Harness E2Es are Debug/Metro runs;
   `release:artifacts` only proves native artifact generation.
6. Extend `apps/harness-shared/benchmarks/` with separately attributed
   canonical-codec, local HTTP transport, authority transaction, conflict,
   retry/idempotency, and catch-up profiles. Do not fold those layers into the
   local persistence ratio.

## Hard blockers

- The long-term single-source/export mechanism is unresolved.
- GitHub Actions may remain unavailable until account billing permits runner allocation.
- The local gateway proves HTTP orchestration but not production deployment or authentication.
- The private authority now has bounded checkpoint collection, durable scanner health, fail-closed
  single-partition rebootstrap, and durable random checkpoint issuance. Production deployment,
  row-volume/alerting thresholds, and revocation exercises remain unresolved.
- This branch must remain a draft and must not be released or advertised as sync support.

Only GitHub runner/billing availability is a user-side operational blocker. The codec, transport,
and authority items are engineering/architecture follow-ups, not account setup tasks.
