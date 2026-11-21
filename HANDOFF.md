# React Native conflict-policy handoff

2026-10-06 — Branch `feat/opt-in-conflict-policy-rn` starts from the existing
`feat/conflict-resolution-v1` worktree at `56a7b04`, which contains the unreleased sync runtime.
The private protocol policy is on `surrealdb-sync-engine.dev` branch `feat/conflict-policy` at
`323a7b1`. The RN crate copy, UniFFI method, generated bindings, typed TypeScript opt-in, and
tests live on this branch. The default manual path is unchanged; no authority code or production
build was changed. The unrelated untracked benchmark
`apps/harness-shared/benchmarks/sync-household-workload.ts` predates this task and is untouched.

Run from this checkout: `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`,
`/opt/homebrew/bin/pnpm --filter react-native-surrealdb run typecheck`, and
`/opt/homebrew/bin/pnpm --filter react-native-surrealdb run test`. Native binding regeneration
uses the pinned `ubrn` CLI from `packages/react-native-surrealdb/node_modules/.bin`, a freshly
built host `target/debug/libsurrealdb_rn_core.dylib`, and `ubrn generate jsi bindings` plus
`ubrn generate jsi turbo-module`. A fresh checkout needs the documented pnpm/Cargo setup.
The host has Node 26 while this package declares Node 20–22; commands warn but have run.

The checked-in generated binding source changed. On 2026-10-06,
`pnpm --filter react-native-surrealdb run ubrn:ios`, `release:strip:ios`, and `build`
passed; the ignored `SurrealDbRnFramework.xcframework` now contains this policy branch.
The local test tarball packed to 144,076,650 bytes, then was removed after the
Hauswirtschaft native build was stopped for low disk. The two task-created Rust
iOS `target/` subdirectories were also removed; regenerate them if the source
changes. Android artifacts were not rebuilt. A device claiming the opt-in path
still needs a fresh app development build and runtime check. The existing
released alpha package remains manual-only. No package was published.

## 2026-10-16 local roadmap observability

Task9 on feat/sync-observability-dst-20261016 extends candidate3a1105a. See
[ROADMAP_9_7_8.md](docs/ROADMAP_9_7_8.md) and
[SYNC_TELEMETRY.md](docs/SYNC_TELEMETRY.md) for verification, APIs and native ABI
rebuild boundary. Public merge/package publication and production changes remain
separate. Native status/support reads do not provide atomic application-row snapshots.

## Local-only native bootstrap staging foundation (2026-10-19)

Branch `private/bootstrap-staging-20261019` is intentionally local with no remote
tracking. Public SDK publication is paused pending the user's decision about the
three previously pushed commits. Do not push this branch or export these sources.

Private Rust-only begin/page/progress/cancel stores exact bounded manifest/pages
outside confirmed journal data. Per-key rotating incarnation/fence prevents ABA;
combined stage/current/final reservations are checked before payload decoding.
Journal and barrier writes participate in the same fence. First-begin barrier
write intent has real both-order Memory/SurrealKV transaction regressions. Native
512-byte UTF-8 key limits apply before DB access at all four entrypoints.

Frozen foundation: `/private/tmp/sdk-bootstrap-storage-bench/review-v3-keyguard`.
Core tests: 88 passed, 1 existing ignored; strict Clippy and formatting pass.
No ABI/bindings/native artifacts, completion receipt, final install or activation
are added. Reads fully validate staged/current data; host debug measurements show
material overhead requiring optimization and later matching-build device proof.
Evidence: `/private/tmp/sdk-bootstrap-storage-bench/`. Final atomic install must
verify complete root/context/receipt and commit journal, projection, cursor and
consumed identity together while preserving newer local pending/accepted intent.

### Local query consolidation follow-up

Batch scalar inventory statements with unchanged per-table bounds, then reuse
inspection only within its borrowed transaction. Full payload/canonical/orphan
checks and normalized budget validation remain. The preflight loader has exactly
one caller, `InspectedStage::current`; do not bypass this transaction-local proof.
Final restored source: 88 core passed / 1 existing ignored; strict Clippy, format
and diff checks pass. Source hashes: `review-v4-opt/sha256.json` in the evidence.

Six alternating source-verified pairs (144 samples/variant/case) in the repeat
show active progress 25.10 to 17.79 ms and active barrier update 26.43 to 19.23 ms
medians. Ordinary save 19.24 to 20.14 ms and barrier update 1.39 to 1.47 ms show
observed slowdowns; their cause remains unresolved. Peer workloads were paused,
but host load still ranged 66.17 to 125.66. These are host debug memory-engine
results, not an idle-host causal finding, mobile startup, Release or fsync proof.
No overall performance win or activation readiness is claimed. Retain all runs
and uncertainty: `/private/tmp/sdk-bootstrap-storage-opt-bench/quiet-replay/`.

### Local-only durable bootstrap completion

Commit `281a1c8` is private Rust store-only completion: exact trusted context and
receipt, full ordered root and latest durable state are verified in one transaction;
journal/projection/cursor/terminal identity and page deletion commit together. UTC
and a maximum 30-second monotonic window are rechecked before commit. Historical
exact success never reapplies state or clears a newer barrier. Genuine prior
header schemas receive optional marker fields only inside explicit completion.
Zero/malformed consumed revisions fail closed.

Final full core: 99 passed / 1 existing ignored; strict Clippy, formatting and diff
checks passed. Memory/SurrealKV both-order enqueue/barrier/cancel races, rollback/
reopen, discarded successful ACK, accepted-intent/epoch and capacity/corruption
cases pass. The inspector comparison excludes only volatile CPU/memory counters
and still detects actual namespace mutation. Final commit blobs and live source
match `/private/tmp/sdk-bootstrap-final-install-review-v2/sha256.json`; the old
f195 review snapshot is superseded. Root initially attempted verification against
that stale snapshot before receiving the refreshed freeze; the local commit has
since been independently reconciled against the exact 99-test source.

No NativeSyncClient publication, cross-handle ownership/reader-generation proof,
FFI/ABI, bindings, platform builds or activation is included. Read-only next design:
`/private/tmp/haus-native-bootstrap-runtime-publication-design.md`. Core evidence:
`/private/tmp/sdk-bootstrap-final-install-core.log`. Public SDK publication remains
paused; do not push this local branch.

### Local-only shared native runtime and lifecycle ownership

Reviewed six-file snapshot `/private/tmp/sdk-shared-runtime-review/sha256.json`
on c9cb1ec. Runtime and both read journals now share ownership per embedded DB,
session generation and journal key. Sibling wrappers observe the same state;
closing one wrapper leaves siblings usable. Typed session mutation invalidates
at intent and again under the serialized lifecycle write lock, preventing a
queued open from adopting another mutation's pending generation ticket. Registry
locks never span await and helper calls never retain the connection read lock.

Private bootstrap completion holds shared ownership through commit/readback and
publishes once. Every ambiguous or failed postcommit publication invalidates the
shared cache. Both reader epochs rotate; exact historical receipt retry publishes
nothing and preserves later durable changes/barriers. Permanent regressions cover
queued mutation/open ordering, postcommit account/close intent, mismatched durable
readback, lost ACK recovery, physical same-path/alias/process exclusion, sibling
close and independent keys. Arbitrary trusted raw SQL session/private-table writes
remain outside supported ownership semantics.

Full core: 109 passed / 1 existing ignored; strict all-target Clippy, format and
diff checks passed. Logs `/private/tmp/sdk-shared-runtime-{core,clippy}.log`;
participation catalogue `/private/tmp/sdk-shared-runtime-review/participation.md`.
No ABI/TypeScript/options/dependency/platform artifact changes or activation.
Host timing uncertainty and before/after device measurements remain open.
This local branch has no tracking remote; public publication remains paused.

Final shared-runtime freeze supersedes the 109-test snapshot above:
`/private/tmp/sdk-shared-runtime-review-v2/sha256.json`. Full core111 passed /
1 existing ignored; strict Clippy/fmt/diff checks green. Both journals retain
an immutable random cell/process prefix plus checked monotonic epoch counter;
recovery preserves/increments them rather than constructing fresh Default journals.
Repeated recovery and counter-exhaustion tests prove old cursor rejection within
the shared cell. Cross-process prefix uniqueness remains probabilistic. No public
push, ABI activation or device/performance acceptance is implied.

### Local-only additive bootstrap UniFFI facade

Reviewed final five-file snapshot `/private/tmp/sdk-bootstrap-facade-review-v2`:
actual Rust UniFFI session/stage objects, independent expected context, native clock,
capability and typed redacted errors. Begin/Page/Cancel use shared lifecycle/runtime
ownership; read-only Progress reopens exact durable incarnation without creating
missing schema. Unknown mutations invalidate every sibling until durable recovery.
No JavaScript clock or fake native authentication assertion was introduced.

Review exposed and reproduced an accidental authorization freeze in the first
candidate. Final code separates immutable owner/origin/generation from changing
authorization revision. Exact active stages remain exact; terminal replacement may
advance authorization while checked process tickets invalidate older session
objects. Successful Begin persists the new floor; read-only factory promotion
alone remains process-local. Complete delegates unchanged canonical pending,
accepted/conflict and subscription quarantine/new-journal guards.

Final full core125 passed /1 existing ignored; strict all-target Clippy, format
and diff checks pass. Old frozen candidate's red log:
`/private/tmp/sdk-bootstrap-auth-transition-red.log`. Final logs:
`/private/tmp/sdk-bootstrap-facade-auth-{core,clippy}.log`. Generated bindings,
TypeScript facade, matching platform artifacts/device acceptance and startup
measurements remain pending. Native clocks assume OS UTC accuracy; authenticated
transport enforces server expiry. UniFFI incoming allocations precede Rust limits.
This commit stays local with no public SDK publication or app activation.

## 2026-10-19 additive bootstrap generated bindings and TypeScript facade

Source slice based on local Rust facade commit `498ecc8` (no public push).
Generated TS/C++ comes from a default-feature host dylib and pinned installed
`uniffi-bindgen-react-native` `0.31.0-3`; the unchanged TurboModule entry was also
regenerated. A second run reproduced all six generated/entry files byte for byte.
The complete current metadata includes the dormant inspector ABI. No generated
checksums were hand-edited, no existing exports removed, and no iOS/Android native
artifacts were rebuilt or distributed. Host dylib SHA256:
`ba461e6496c26e5c32989543f2ca28565d5cfd32a69b0b70c8a0325b74ecc105`.
All 66 generated exported checksums (including 7 bootstrap) matched actual host
symbols; this does not establish JSI method execution or device acceptance.

`ExperimentalSyncClient` now exposes bootstrap capability/binding with independent
context, exact copied byte-view slices, pre-dispatch 4 MiB limits, opaque per-session
stage handles, lossless bigint metadata, explicit Missing/Staging/Cancelled/Consumed
progress, applied/historical completion, typed redacted errors and closure guards.
Native UTC/deadline remain authoritative. Whole-old-binary imports can fail the
existing generated initialization gate before any facade or storage; the typed
UnsupportedAbi guarantee applies only once binding-module initialization succeeds.
The generated gate stays unchanged. Matching native runtime fingerprints/builds
must prevent a new JS bundle from reaching an old binary.

Verification: package TypeScript full suite **124/124 tests**, typecheck, complete
source/test Prettier check, Bob module+declaration build and git diff whitespace
check passed. Initial sandbox-only full run could not listen on loopback; the
approved disposable-loopback rerun passed all tests. Logs/receipts:
`/private/tmp/sdk-bootstrap-ts-full-approved.log`,
`/private/tmp/sdk-bootstrap-ts-typecheck.log`,
`/private/tmp/sdk-bootstrap-ts-format.log`,
`/private/tmp/sdk-bootstrap-ts-build.log`,
`/private/tmp/sdk-bootstrap-native-handshake.json`,
`/private/tmp/sdk-bootstrap-bindings-sha256.json`,
`/private/tmp/sdk-bootstrap-generated-review.json`.

At the initial498ecc8 generation, native future-drop/cancellation settlement
remained open; the later9f7880f source milestone addresses that host-native gate
as documented in [Native mutation settlement](docs/NATIVE_MUTATION_SETTLEMENT.md).
JS mock AbortSignal tests do not prove native settlement or device execution.
Matching iOS+Android builds/device acceptance and previously recorded storage
performance uncertainty remain separate activation gates. No app activation,
package publish or public SDK push occurred.

### Post-settlement binding recheck on9f7880f

Regenerated from exact local Rust
`9f7880ffa6d0f41b7e757a9aacdf486e62aee1e9`. Its Arc receiver changes emit no
TS/C++/TurboModule differences from the prior498ecc8 binding generation. Two
fresh runs reproduced all six generated/entry files exactly. The newly linked
host dylib SHA256 is
`b32f075a1bba3922cd890316a1843b045f467d608c530a1b16d0d2bfdd9f554f`;
all66 actual exported checksums (7bootstrap) match generated TS. Parent independently
accepted the Rust132-pass/1-existing-ignored settlement gate; this binding slice
rechecked host generation/checksums and TypeScript, not mobile JS execution.
See [Native mutation settlement](docs/NATIVE_MUTATION_SETTLEMENT.md) for owned
worker cancellation, unknown-outcome poison and recovery limits.

Final TS recheck:124/124 tests across15files, no skips; typecheck, full Prettier,
Bob module+declarations build and whitespace checks green. Receipts and exact
source snapshots: `/private/tmp/sdk-bootstrap-settlement-binding-review/`.
The generated whole-old-binary import failure boundary remains unchanged;
method-level UnsupportedAbi does not promise the error type from importing an
incompatible binary. Matching iOS/Android builds, generated argument-lifting and
device acceptance, plus recorded performance gates, remain outstanding. No
platform build/distribution, activation, package publication or SDK push occurred.

### Bootstrap error inspection hardening

Root review found tag getter/proxy inspection could throw a private error out of
the redactor. Two permanent regressions failed against the original code, then
passed after own-data descriptor inspection was enclosed in a protective catch.
`instanceof`/prototype traps are protected too. Local wrapper errors are rebuilt
from allowlisted own code fields instead of retaining potentially modified
messages/causes. Actual generated native error classes and safe own-data-tag
proxies still preserve their allowlisted codes. Unknown mutation/read failures
remain RecoveryRequired/Storage with no raw cause/private text.

Final gate:126/126 TS tests across15files,0skips; typecheck, fullsource/test
Prettier, Bobmodule+declarations and whitespace checks passed. Actual red/green
logs and source snapshot are at `/private/tmp/sdk-bootstrap-redaction-review/`.
Only facade/test and this documentation changed since the postsettlement freeze;
generated binding/Rust bytes are unchanged, and no Cargo work was needed.

## Internal whole-storage ownership inspector (2026-10-19)

Accepted six-file frozen inspector patch d80bc0db4724db7bc4a1cfceb64c1048f3e5aff75ab9f17a07752feccf4eb3ff.
Rust-only no-init observation now distinguishes pristine unselected metadata from
selected app occupancy and exact native journal state, rejecting foreign/alias/
orphan/malformed layouts across all ten fixed native tables. Lifecycle WRITE,
session and sticky settlement-poison checks prevent unsafe concurrent observation.
No exported ABI, marker policy, repair, deletion or app activation is included.
See READ_STORAGE_INSPECTION.md for the ROOT postdecode metadata exception and
physical-work limits. The observation is not a future-write lease.

Canonical exact frozen hashes were verified and Rust source mtimes refreshed;
CargoJSON fresh:false and canonical manifest were verified before immutable
executable copy/hash and listing13 inspector tests plus2 bootstrap regressions.
146 core tests passed,2 existing explicit cases ignored; explicit SurrealKV reopen
passed1/0skip. All-target warning-denied Clippy, format and diff checks passed.
Logs/provenance: /private/tmp/haus-inspector-canonical-{build.log,artifacts.jsonl,
provenance.json,tests.log,persistent.log,clippy.log}. Persistent fixture retained at
/private/tmp/haus-inspector-canonical-persistent. Matching rebuilt iOS/Android
acceptance remains required; no SDK public push is authorized pending user choice.

## 2026-10-19 typed whole-storage inspection bridge

Based on local e6b0d746 plus the accepted six-file internal inspector patch
(subsequently committed by the coordinator as 216fc40). This additive source slice
exports typed no-init inspectStorage options/result/error through UniFFI and
SurrealClient.inspectExperimentalStorage. Existing APIs and all 66 previous native
checksums remain unchanged. All 67 generated checksums match the actual immutable
host dylib; two generator runs reproduce all six generated/entry files exactly.
No generated code/checksum was edited by hand. Host dylib SHA256:
2243845f79e58fcb77cb4f0b0e1013ea669fa0d0c6a3f03e666fa398d8719dab.

The facade snapshots bounded own data properties, rejects hostile accessors/proxy
traps with fixed redacted classifications, validates/copies/freezes bounded native
output, preserves bigint revisions and forwards AbortSignal for this read only.
Namespaced storage names accept the actual household-local profile name; they are
bound/compared only, while SQL table identifiers remain strict. Permanent embedded
coverage proves pristine and selected no-init behavior with hyphenated names.

Host proof: 147 core tests pass (2 explicit fixture/server tests ignored by default),
explicit SurrealKV inspector reopen 1 pass; all-target warning-denied clippy, fmt and
diff checks pass. TypeScript 134 tests pass (focused new 8), typecheck, full source/test
formatting and Bob module/declaration build pass. Generation/checksum receipt and
frozen source/executable identity live under
/private/tmp/haus-storage-inspection-abi-SOtM1l. This is source/host evidence only:
matching rebuilt iOS/Android binaries, runtime fingerprints and device execution
remain mandatory before app activation. Existing generated import gating can
reject an old native binary before the method-level typed UnsupportedAbi path.
No physical owner-marker policy, journal creation/migration, app activation,
mobile artifact build, publication, push or commit belongs to this worker slice.

Canonical ABI acceptance: all13 frozen source/generated hashes matched before
source refresh and fresh:false exact-manifest host compilation. Immutable canonical
test executable/listing/provenance saved at /private/tmp/haus-inspector-abi-canonical-*;
147 core passed/2explicit ignored plus explicit SurrealKV reopen1pass/0skip.
Full TypeScript134passed/0skip, typecheck and strict all-target Clippy/fmt/diff pass.
The initial sandboxed HTTP test failed on listen EPERM; the identical suite passed
with loopback permission. No pnpm install/module purge was accepted; installed
binaries were used directly after pnpm's automatic preflight attempted a network
metadata check. No SDK remote push, platform build or app activation is claimed.

## Private native candidate identity (2026-10-19)

Package/crate/Cargo.lock own version is0.1.0-alpha.4.haus.20261019.1, distinct from
published alpha.3. This is local private distribution only: no public tag, npm
publication, SDK remote push, visibility change or release creation. Existing
package allowlist and full-artifact size gate remain required before packing.
No dependency versions changed. Exact pnpm11.5 root lockfile-only policy check
passed1287entries; pnpm-lock unchanged. Locked Cargo metadata confirms matching
package/crate version. An offline attempt could not satisfy registry policy
metadata and was stopped; registry-enabled lockfile-only retry passed without
module install/scripts. Native platform artifacts will be rebuilt from this
candidate before consuming it in Hauswirtschaft. Host functional147/134 gates
precede this metadata-only version change; mobile acceptance remains pending.

## Private native candidate and owner-metadata conformance (2026-10-20)

Runtime candidate e804746ca6a0075aa72231335989306579fd0b0e, version
0.1.0-alpha.4.haus.20261019.1, built iOS arm64 device+simulator and Android
arm64+x86_64 release libraries. Existing strip scripts passed; generated tracked
bindings matched committed source after formatting. Package build/types,134JS
tests and package verification passed. Private tarball152534206bytes (existing
180000000 ceiling), SHA256
1d2d8baf7dff3bf512886618303801527e1bf7ffe642e9cd224b4d9f589e9b23.
Artifact proof /private/tmp/haus-private-sdk-complete-artifact-proof.json.
NO SDK publication, tag or remote push. New app consumption/build/device gates
remain open. Completed iOS compiler target intermediates were removed only after
packaging/hash verification; packaged libraries, source and evidence were retained.

New test-only conformance validates pristine ROOT/no native initialization,
same-request USE NS+INFO NS, typed use only after exact database admission, foreign
metadata refusal and persistent SurrealKV reopen. Actual raw USE selection does
not persist across later queries; the app must use the proved same-request INFO
then explicit typed use. Public query encoding confirms tagged NONE and exact
statement indices. USE's result is a namespace/database object, not NONE; the
initial test expectation failed and was corrected from actual output, without
changing runtime admission. Host151passed/3explicitignored; persistent test1pass
with0skip; all-target Clippy/fmt passed. Logs /private/tmp/haus-sdk-owner-proof-*.

To keep host proof cache small without changing debug assertions/optimization:
CARGO_TARGET_DIR=/private/tmp/haus-sdk-owner-proof-target
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
CARGO_BUILD_JOBS=1 cargo test --locked --offline -p surrealdb-rn-core.
This is a host correctness configuration, not mobile performance evidence.
No API/ABI/runtime change is included in this test-only slice.

## 2026-10-23 Private alpha7 wrapper packaging

The matching bounded read-view binaries and generated bindings are packaged as
`0.1.0-alpha.7.haus.20261023.1`. This private wrapper version preserves exact
Rust/native inputs fb1164ebf98469a64e41f0422938cb2bb23a5111 and the existing
Rust crate version; no native artifacts were regenerated by packaging. Canonical
source/lib/all four native slice hashes were verified unchanged. Android strip
operated only on independent staged copies; unchanged archives were hardlinked
read-only. All mutable JS build outputs were detached before Bob ran.

Exact `pnpm run release:check` passed in the owned stage with Node22.22.0 and
repository-pinned existing pnpm11.5.0: Bob0.40.18, TypeScript5.9.3, 148 tests
without skips, and package verification. All 670 reachable dependency lock
snapshots matched the reused installed closure. All 102 generated JS/type files
were byte-identical after the build. No installation or native generation ran.

Archive: 152,993,844 bytes, 155 safe regular files, SHA256
408016b9840e750aeea33a562430af5f328c7c5fa72af9b464eae4afb7ce5b5f.
Audits cover exact file inventory/content, 72 UniFFI checksums/contract30 on all
four slices, Android16KiB alignment and license attribution. Apache terms are
bundled for copied sync-client/protocol code with verified canonical attribution;
wrapper MIT and generator notices stay intact. Peak newly allocated packaging
space remained below400MiB; owned npm caches were removed, final below250MiB.

Evidence: output/private-sdk-alpha7-20261023/. Pin the reviewed wrapper commit
in the consuming application's private artifact manifest before publication.
Never replace alpha6 or push this private branch wholesale to the public origin.
App integration/builds, physical Android/iOS execution and startup acceptance
remain separate unfinished gates. Root SDK dependencies were previously removed
under explicit permission; ordinary SDK development requires their restoration.

## Offline intent adapter format migration (source WIP)

The experimental normalized sync-state adapter now pins both engine crates to
`surrealdb-sync-engine.dev` revision `c5c8c7b36b14210ef8a8cb6f42cc8c26abdf2541`
and defines adapter format 4 for engine `state_version`, `local_intents`,
`lanes`, and `last_reset_cursor`. Loading v2/v3 normalized rows remains
read-only. Migration is lazy and occurs only on the next successful durable CAS
save, which writes component rows and metadata in one transaction. Legacy
accepted outcomes remain preserved; they do not gain causal proof from an ACK.

The populated v2/v3 fixture now checks read-only load, wrong-revision CAS
preservation of metadata and every component row, transaction cancellation
after a component write, successful retry, and reopen after format-4 rewrite.
The v2 fixture also omits `state_version` to exercise its conservative default.
The fixture opens a legacy accepted receipt in `ClientRuntime`, checks that
a dependent recipe intent is blocked with `LegacyHandoffPending`, then applies
an authoritative reset and checks that the lane unblocks. The format-4
roundtrip fixture includes a blocked lane and resolution revision. These tests
remain source-only while the app owns the heavy compile slot; they need a
focused compile and run before any commit. The Rust `NativeSyncClient` facade
now exposes JSON edit observation, enqueue intent, materialize-next, blocked
lane snapshot, and explicit resolution through the existing atomic transition
path. UniFFI derives these declarations from the exported Rust impl; there is
no UDL to hand edit. The blocked-lane roundtrip carries two pending intents
and checks the snapshot's covered prefix after reopening. TypeScript adds
optional lane ABI shapes and fail-closed wrappers without casting or editing
generated interfaces. HTTP pull has explicit `pullSourceProof` opt-in: the v1
request CBOR goes to `/v1/sync/pull/source-v2` with exact source-v2 Accept and
response content type. Rust exports a dedicated source-v2 response decoder.
Default pull stays v1. Opt-in requires a matching native decoder and fails at
construction if absent. TypeScript check, focused tests, generated bindings,
native binaries, and source-proof transport tests remain pending. No native
build, app activation, or release artifact is claimed.

The source-proof transport test now covers the exact route, v1 request type,
source-v2 Accept and response type, and rejection of absent, v1, generic-v2,
or JSON response types before native apply. Lane observation and conflict
snapshots are opaque JSON strings in TypeScript so a JS parse cannot round
large version numbers or discard covered-prefix fields. Lane enqueue and
materialization require explicit source-proof adapter opt-in. The TS test run
was attempted with the workspace `pnpm` command, but Corepack tried to fetch
the pinned pnpm 11.5.0 distribution; it was interrupted to avoid installs.
The existing top-down-todos Vitest 4.1.12 binary starts this test but cannot
resolve SDK's `@ubjs/core`; its `tsc` cannot find SDK React/React Native types.
The SDK's declared Vitest 3.2.4 and package links are absent here, so
these borrowed binaries do not validate the source. The rollback fixture uses
a real SurrealKV transaction, changes a component row, cancels before metadata,
checks both row and metadata bytes, reloads legacy state, then retries CAS.
The migration test now also runs `replace_v2_in_transaction` to Applied and
cancels it, and uses a cfg(test)-only `save_atomic` hook to fail after its first
component write. Both paths assert original metadata/component payloads and
legacy reload before the normal CAS retry. Both v2 and v3 fixtures omit
`state_version` and require conservative default zero.
The hook returns a cfg(test)-only `InjectedAfterComponentWrite` error and the
test asserts that exact variant, so a pre-write CAS conflict cannot satisfy
the rollback test. Production error variants and generated ABI are unchanged.
The save path now detects a pre-v4 metadata row inside its transaction and
forces every component row rewrite even when its decoded payload is unchanged;
the successful migration fixture checks every component's format version.
CAS validation and `component_record_id(key, item)` remain the existing paths;
the migration flag changes only whether equal decoded component payloads are
skipped. Removed component keys still pass through the prior diff deletion.

Pinned JS setup: `.node-version` is 22.22.0 (current Node matches), root
`package.json` pins pnpm 11.5.0, and README/CI use `pnpm install
--frozen-lockfile`. Existing Corepack pnpm executable is
`/Users/timhorgas/.cache/node/corepack/v1/pnpm/11.5.0/dist/pnpm.mjs`;
reproducible command from repository root is `node <that-path> install
--frozen-lockfile --offline`. This has not been run. Read-only measurements:
the local pnpm store is 8,215,872 KiB, this worktree's incomplete
`node_modules/.pnpm` is 2,205,476 KiB with no `.bin` links, and free disk is
7,807,872 KiB. The 8 GiB floor is already unmet, so no install headroom is
available under that gate: at least 580,736 KiB must be reclaimed merely to
reach the floor, plus the as-yet-unmeasured completion delta. Earlier `pnpm exec` unexpectedly began a partial
workspace dependency link before interruption; preserve it for later review.

Update: root authorized the bounded offline install. Ran cached pnpm 11.5.0
with `install --frozen-lockfile --offline --ignore-scripts`; it completed in
6.9 seconds with 1,291 packages added, 1,212 reused, zero downloads, and no
manifest/lockfile change. Free disk moved from 7,755,796 KiB before install
to 7,692,176 KiB after, a 63,620 KiB change below the 512 MiB cap; no native
build ran. Package-local Vitest 3.2.7 passed `test/sync-http.test.ts` (23/23),
and package-local `tsc --noEmit` passed. Typecheck sees optional source-only
lane declarations; it does not verify generated UniFFI/native ABI alignment.
The lane opt-in now checks all native methods plus a native capability document
(`schemaVersion: 1`, `storageFormat: 4`, `pullSourceProfile: pull-source-v2`)
before enabling queue mutation. Old native artifacts, partial ABI, malformed
capability JSON, and mismatched storage/profile fail with the same explicit
matching-native error; the legacy v1 transport remains available. Focused
`sync-http.test.ts` and `sync.test.ts` pass together (30/30), and package
`tsc --noEmit` passes. This is source-level verification only: generated
bindings and platform binaries still need rebuilding after Rust compile.
The lock lists 1,287 package entries; the incomplete virtual store has 1,291
package directories, including TypeScript 5.9.3 and `@ubjs/core`, but no
virtual-store lock, root `.bin`, or package links. That suggests fetching is
mostly cached, not that offline install is guaranteed: pnpm must validate the
content-addressed store. No exact completion delta can be measured without
performing the frozen install. Rust registry sources for SurrealDB core/types
3.2.4 and the pinned engine checkout are present. First native commands once
the sole slot opens: `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<existing-engine-target>
cargo +1.96.1 check --locked --offline -p surrealdb-rn-core`, then the same
environment with `cargo +1.96.1 test --locked --offline -p surrealdb-rn-core
--lib sync_state::tests::v2_v3_normalized_rows_migrate_populated_state_and_reopen_losslessly`.
These should avoid downloads; they may still fail if another locked registry
source is absent, and compilation may allocate substantial target output.

Dependency lock audit (2026-11-20): SDK `Cargo.lock` already resolves
`surrealdb`, `surrealdb-core`, `surrealdb-collections`, `surrealdb-types`, and
`surrealdb-types-derive` at 3.2.4. The current lock diff only disambiguates
the pinned engine git client/protocol from local workspace crates; no registry
package or checksum changed. `surrealdb-protocol` is a separate 0.10.2 crate,
not a SurrealDB 3.2.x package. Do not refresh this lock broadly: the app's
fresh lock picked `surrealdb-core` 3.2.6 with `surrealdb` 3.2.4 and hit a
`changefeed_process`/`CancellationToken` API mismatch. SDK compilation stays
deferred while the app owns the existing engine target and 8 GiB floor.

Original SDK engine lineage at d9c51e7: there was no Git dependency pin.
`surrealdb-rn-core` depended on workspace paths
`../surrealdb-sync-client` and `../surrealdb-sync-protocol`, last changed at
SDK commit fb1164ebf98469a64e41f0422938cb2bb23a5111. The local crates
began at 9667aec2adc1a1b739d3742df8fa4439237501ec and subsequently
received SDK-owned read, subscription, bootstrap, conflict-policy, and
canonical codec commits. The current c5c8c7b engine Git source descends from
752e25a587d1a5d4ea4e239de21ad1144f96c537 (lanes), then
b603331e1ece580195588fec6d8ea19ec31889f5 (pull scope),
9d1fb14d04a8e31ec5b928baa30762530a5647ef (source profile),
8a84bcf3e9b717994deea2f69d900097fb44e3bc (3.2.4 pins), and
c5c8c7b36b14210ef8a8cb6f42cc8c26abdf2541 (type fix). These repositories
have separate commit ancestry; the SDK baseline must be ported as source/API
work into an engine integration branch, not cherry-picked across repositories.
Minimal graph: SDK vendor baseline through fb1164e -> engine lane transition
series (752e25a through b0a64b9) on the reviewed authority policy base
(4e0d2a6) -> pull-scope/source-profile commits
(b603331, 9d1fb14, c5c8c7b) with 3.2.4 authority pins (8a84bcf) ->
compatibility tests for bootstrap/read/subscription/lanes together -> new
engine integration SHA -> SDK pin/lock update. Preserve the SDK baseline
`reset_coverage`, `subscriptions`, `covered_by_reset`, conflict policy and
bootstrap modules while reconciling lane accepted-epoch/proof fields. Do not
repin SDK to an arbitrary upstream SHA or delete these existing surfaces.
`crates/surrealdb-rn-core/tests/engine_api_compat.rs` is the short compile-only
contract for the combined engine API. The SDK library check remains the
exhaustive gate. Engine-worker baseline cases to retain include bootstrap
`completion_surrealkv_restart_and_lost_ack_never_reapplies_over_later_writes`,
`complete_conflicts_with_enqueue_barrier_and_cancel_in_both_orders_memory_and_kv`,
`complete_retains_same_epoch_accepted_and_conflict_but_rejects_unknown_or_other_epoch`,
the subscription window tests, read metadata/snapshot tests, and the new SDK
v2/v3 lazy migration/rollback and format-4 lane roundtrip. These require
atomic state replacement, preserved reset coverage and subscription membership,
no backward checkpoint or inferred ACK/pull proof, and source-v2 opt-in without
altering canonical v1 bytes. The fixture is source-only until the integrated
engine SHA is pinned and the SDK compile slot reopens.

## Receipt integration staging (source only)

Engine compatibility source commit
dfa0374a9e88fcb2a823a0ebd5b2c3c16d2bf022 is uncompiled; receipt work in
its worktree is not yet a validated commit. SDK pin stays at c5c8c7b. The
planned SDK FFI resolver now accepts a caller-stable `resolution_id` and
fails with Protocol before mutation under the old pin; it must never accept an
ID and lose its durable association. The read-only receipt method currently
returns opaque `unknown`. TS forwards the ID and opaque receipt JSON and
requires capability schema version 2 plus the receipt method. Current Rust
advertises version 1, so old binaries cannot enable the staged API. Once the
engine receipt shape and validated full SHA arrive, wire the resolver to
`prepare_resolve_lane_conflict(resolution_id, snapshot, request)`, project the
engine receipt after reopen, add `resolution_receipts` to normalized storage
with lazy migration and rollback coverage, then advance native capability.
No generated binding edit or inferred pulled proof is allowed.

## Matching native artifact plan (prepared, no build)

Hold `package.json` and Rust crate versions until the integrated engine SHA,
focused Rust tests, generated UniFFI, and both chosen devices agree. Current
SDK source declares lane methods that are absent from checked-in generated
bindings and old native artifacts; package typecheck and optional TS ports do
not claim native ABI compatibility. The runtime capability guard refuses old
artifacts. Never hand-edit `src/generated`, `cpp/generated`, or native slices.

The repository scripts are `packages/react-native-surrealdb/package.json`:
`ubrn:ios` builds iOS device+arm64 simulator and generates bindings;
`ubrn:android` builds arm64+x86_64 and generates bindings;
`ubrn:android:size` is arm64 only; `release:artifacts` runs both full scripts
and strips output. `verify:package` requires both Android ABIs and iOS
device+simulator slices, so a two-device smoke (Android arm64 and iOS arm64
simulator) is deliberately not a publishable package. The scripts use
`uniffi-bindgen-react-native` 0.31.0-3 and Rust UniFFI 0.31; iOS deployment
target is 15.1. All four Rust targets are installed. This worktree has no
matching native output yet. The shared engine target measured 12,848,136 KiB;
free disk was 5,901,268 KiB at plan time, leaving about 0.9 GiB before the
5 GiB stop. Do not start cross-target release builds at that headroom.

After compatible engine pin and focused Rust checks pass, use the existing
package-local executable from the package directory, one target at a time under the sole
native slot (replace `<ENGINE_TARGET>` with the existing shared target):

```sh
export CARGO_TARGET_DIR=<ENGINE_TARGET>
export CARGO_BUILD_JOBS=1
export ANDROID_NDK_HOME=<installed-ndk-27-path>
cd packages/react-native-surrealdb
IPHONEOS_DEPLOYMENT_TARGET=15.1 ./node_modules/.bin/ubrn build ios --and-generate --release --targets aarch64-apple-ios-sim
./node_modules/.bin/ubrn build android --and-generate --release --targets aarch64-linux-android
node /Users/timhorgas/.cache/node/corepack/v1/pnpm/11.5.0/dist/pnpm.mjs run format:generated
```

The two one-target commands are a bounded device smoke, not the repository's
full release scripts. Before using `verify:package` or `release:pack`, complete
the actual script-supported full set via `ubrn:ios`, `release:strip:ios`,
`ubrn:android`, and `release:strip:android` in package.json, then run
`release:check`. Use the exact cached pnpm executable above to invoke scripts;
never let Corepack choose a new version. `release:pack` calls `npm pack
--ignore-scripts`, writes `npm-pack.json`, and enforces 180,000,000 bytes.

Bind one provenance manifest to the SDK wrapper HEAD, Rust source HEAD,
integrated engine Git SHA, Cargo.lock SHA256, generated TS/C++ SHA256, each
raw native input SHA256, each stripped staged slice SHA256, and final tarball
SHA256. Record `git rev-parse HEAD`, `shasum -a 256` on those exact files,
and `shasum -a 256 <tarball> > SHA256SUMS` only after packing. Verify generated
UniFFI checksum table/contract version against every built slice, 16 KiB
Android ELF alignment, iOS arm64 simulator/device entries, and package file
inventory. No package signing is performed by local scripts; public release
signing uses `git tag -s` and trusted-publishing provenance, which are outside
this private source-preparation task. Do not reuse an old signature or claim
that an unsigned local tarball is signed. No publishing is authorized here.

## Locked check retry and resolution observation (source WIP)

Root authorized a locked online retry for declared crates. Cargo downloaded
locked archives, then began compiling a broad cold dependency graph in the
shared target, so the run was interrupted at the unexpected-expansion stop.
Target grew from 11,940,040 to 12,015,700 KiB; free disk stayed at 7,433,404
KiB, above the 5 GiB stop. No SDK compile diagnostic was reached.

The SDK now has source-only `laneResolutionObservationJson`. A durable
resolution head with the supplied replacement identity and exact snapshot
covered prefix yields queued, accepted, or conflict, with decimal-string
sequence and resolution revision. Any missing binding yields `unknown`.
After completed pull, the engine clears the head; retained `ready_proof` has
identity and sequence but no association to the original snapshot prefix.
Thus this query does not claim a pulled proof. True pulled reentry requires an
engine-owned durable snapshot-attempt association before a supplemental state
design. TS focused tests passed 30/30 and package typecheck passed; Rust source
remains uncompiled.

First authorized shared-target Rust check (2026-11-20): with one job, `--locked
--offline`, and the existing engine authority target, Cargo stopped before
compilation because the `affinitypool 0.7.0` registry archive is absent. Its
unpacked source exists in the local registry source cache, but the `.crate`
archive was not found in the Cargo registry cache. Target size stayed
11,940,040 KiB; free disk moved from 7,527,124 to 7,524,492 KiB, well above
the 5 GiB stop. No second build or test was started. The focused migration
test remains queued until the exact locked crate cache is available.

Resolution observation contract: pinned engine lanes durably retain
`resolution_revision`, pending intent IDs, head identity and role, accepted
epoch/sequence, pull proof, ready proof, blocked state, and outcomes. The app
conflict core expects opaque commit ID, sequence, and resolution revision for
reentry. SDK `resolveLaneConflict` currently returns only status; no durable
per-record phase/proof query exists. A future read-only SDK projection must
derive queued/accepted/pulled/conflict from those engine fields after reopen,
and keep all u64 values as decimal strings. The engine snapshot binds blocking
identity, remote/observed state, reason, local latest, and exact covered prefix;
it does not include resolution revision itself. Do not expose an app-owned
queue or treat ACK alone as pull proof.

Proposed smallest engine change: retain a bounded resolution receipt in
`RecordIntentLane` when `prepare_resolve_lane_conflict` accepts a snapshot.
It binds the original blocking identity and exact covered intent prefix (plus
the snapshot's observed/remote state or canonical fingerprint) to the
replacement `CommitIdentity` and incremented `resolution_revision`. Advance
the same receipt with accepted epoch/sequence from the durable outcome, then
with exact `LanePullProof` from a complete source-aware pull. Retain it after
`head` clears until safely superseded/consumed. An engine read API can return
queued/accepted/pulled/conflict only for a matching snapshot and replacement,
otherwise unknown. This avoids SDK or app ownership of a second queue.

The resumed locked Cargo check reached `surrealdb-rn-core` and failed with 64
source errors. The pinned engine c5c8c7b lacks existing SDK dependencies:
protocol `bootstrap`, `bootstrap_codec`, `subscription`, and
`subscription_codec`; client `AutomaticConflictPolicy`, read metadata/batch
APIs, subscription APIs, reset coverage/subscriptions state, and the old
`covered_by_reset` field. These are baseline SDK/engine API incompatibilities,
not a SurrealDB 3.2.4 graph failure. The complete short diagnostic list is
`/tmp/sdk-check-errors-f6b5.txt` (local temporary, not committed). Cargo.lock
remained locked. Free disk after the run was 7,038,396 KiB, above the 5 GiB
stop. The new lane observation query produced no isolated diagnostic in this
run, but its compilation is unproven until the pinned engine also supplies
the SDK's preexisting bootstrap/subscription/read contract. Do not remove
existing SDK capabilities or switch pins just to satisfy this check; align the
engine source contract first.

## Integrated engine receipt slice (2026-11-20, in progress)

The SDK manifests and lockfile now pin validated engine commit
`383cb224c51382b591deb9079c0a48190f367047` from
`integration/sdk-compatible-client`; the existing SurrealDB, core, types,
and protocol registry versions remain `3.2.4`. The first cached check reached
SDK code and reported only the missing `resolution_receipts` field in normalized
reconstruction. That check has ended; the app temporarily owns the Rust slot
for its cached binary rebuild. No subsequent SDK Rust check or migration test
has run yet.

The SDK source now uses the engine's caller-generated `LaneResolutionId` for
`prepare_resolve_lane_conflict`, reads the retained `LaneResolutionReceipt`
from durable state, and exposes the engine's nondeleting receipt acknowledgment
transition. Unknown is returned only when no receipt exists for that ID.
Native lane capability schema 2 requires normalized storage format 5 and
`pull-source-v2`; the TS opt-in checks every method before lane queue mutation.
Format 5 adds a required `resolution_receipts` metadata map. Formats 2/3/4
read without mutation and rewrite all component rows plus metadata on the
next successful CAS save. A missing receipt map in a format 5 row or an
unexpected map in an older format is corruption. Tests cover retained
keep-server receipt and format 4 lazy migration; Rust execution is pending.

After the app releases the slot, run this exact first command from this SDK
worktree, checking `df -k .` before and during the run and stopping at 5 GiB
free:

```sh
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/.t3/worktrees/roadmap-engine/feat-laptop-workspace-a-8bf94ffcab161fd7/crates/surrealdb-sync-surrealdb-authority/target cargo +1.96.1 check --locked --offline -p surrealdb-rn-core --tests
```

Then run focused migration and compatibility tests in the same target:

```sh
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/.t3/worktrees/roadmap-engine/feat-laptop-workspace-a-8bf94ffcab161fd7/crates/surrealdb-sync-surrealdb-authority/target cargo +1.96.1 test --locked --offline -p surrealdb-rn-core receiptless_format_four_migrates_only_on_successful_cas_save
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/.t3/worktrees/roadmap-engine/feat-laptop-workspace-a-8bf94ffcab161fd7/crates/surrealdb-sync-surrealdb-authority/target cargo +1.96.1 test --locked --offline -p surrealdb-rn-core v2_v3_normalized_rows_migrate_populated_state_and_reopen_losslessly
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/.t3/worktrees/roadmap-engine/feat-laptop-workspace-a-8bf94ffcab161fd7/crates/surrealdb-sync-surrealdb-authority/target cargo +1.96.1 test --locked --offline -p surrealdb-rn-core lane_format_roundtrips_all_engine_lane_fields_and_rejects_bad_versions
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/.t3/worktrees/roadmap-engine/feat-laptop-workspace-a-8bf94ffcab161fd7/crates/surrealdb-sync-surrealdb-authority/target cargo +1.96.1 test --locked --offline -p surrealdb-rn-core sdk_engine_api_surface_compiles
```

Focused TS `vitest` passed 30/30 and package `tsc --noEmit` passed after the
schema 2/format 5 guard and acknowledgment wrapper changes. Matching UniFFI
bindings and platform binaries must be regenerated from this one verified
source tree after Rust is green; existing packaged native artifacts do not
claim these methods yet. No package publication is authorized.

## Enrolled-note intent read contract (source WIP)

`ExperimentalSyncClient.readIntentRecord(recordId)` returns one native-locked
`ExperimentalIntentRecord`:

- `record: SyncReadRecord` from the engine `read_snapshot()` projection,
  including `metadata.revision`, `row.optimistic` (or `null` for a local delete),
  confirmed state and the existing wire pending/accepted/conflict lists;
- `lane.pendingIntentIds: string[]`, the durable local intent prefix and suffix
  even before any wire commit is materialized;
- `lane.blockedReason: string | null` and `blockedSnapshotJson: string | null`
  for conflict editing; `lane.headIdentityJson: string | null` for an in-flight
  materialized head; `lane.resolutionRevision: string` as an exact decimal.

The native implementation takes one runtime lock, obtains an engine read
snapshot, narrows rows to the requested record, and reads lane fields from the
same installed state. It reuses the v1 read decoder and sends the view as an
opaque JSON string inside the lane envelope, avoiding JSON-number conversion
in the wrapper. A record can have `record.row.optimistic === null` and still
have pending intent IDs: that means a local delete of an absent or formerly
present record, not a proven server absence. The app should use this API as
its enrolled-note save/read authority, not write SQL first and enqueue later.
No app-owned intent queue or projection rules are added.

A Rust contract test covers a new-record intent before materialization, a
suffix intent while the head is pending, a conflict and local delete, and
reopen persistence. It passed on the validated engine pin; see the current
checkpoint below. The native binary and generated bindings remain pending.

## Historical Rust gate and engine read regression (2026-11-20)

`cargo +1.96.1 check --locked --offline -p surrealdb-rn-core --tests` passed
with `CARGO_BUILD_JOBS=1` and the shared engine target after downloading only
the already locked `temp-dir 0.1.16` crate during a locked online retry.
Focused tests passed: `v2_v3_normalized_rows_migrate_populated_state_and_reopen_losslessly`,
`receiptless_format_four_migrates_only_on_successful_cas_save`,
`lane_format_roundtrips_all_engine_lane_fields_and_rejects_bad_versions`,
`receipt_format_rejects_missing_map_and_mixed_component_versions`, and
`sdk_engine_api_surface_compiles`. The v2/v3 reset fixture now uses a complete
absence proof; a tombstone for a previously absent observation correctly left
a stale-observation block. Test binaries were built with one job; the final
focused command ended and the SDK released the Rust slot for engine tests.

`intent_read_projects_unmaterialized_suffix_delete_conflict_and_reopen`
currently fails at its first optimistic value assertion on engine commit
`383cb224`: the SDK baseline local engine declares `OptimisticRecord` with
`#[serde(tag = "state", rename_all = "snake_case", rename_all_fields =
"camelCase")]`, while the integrated engine omitted that attribute. Engine
`read_snapshot()` therefore emits an externally tagged `Present` object where
the existing SDK read wire/TS decoder expects `state: "present"` and
`baseVersion`. This is an engine integration regression, sent to the root for
engine ownership and golden tests. Keep the SDK test; repin the exact corrected
engine SHA after validation, then rerun the native intent/resolve/reopen test.

## Measured mobile cache and artifact gate

The package's `ubrn:android` and `ubrn:ios` scripts build four release targets:
Android arm64 and x86_64 plus iOS device arm64 and simulator arm64. The
package verifier requires all four and generated TS/C++ files. A two-target
Android arm64/iOS simulator build is only current-device evidence, not a
package release. `ubrn.config.yaml` writes to `src/generated`, `cpp/generated`,
`android/src/main/jniLibs`, and `SurrealDbRnFramework.xcframework`.

The current shared authority target contains host artifacts only (about 15GiB)
and no mobile triple directories. The clean older SDK checkout at
`/Users/timhorgas/git/worktrees/sync-receipt-sdk` has a 6.0GiB release target
with SurrealDB 3.2.4: `target/aarch64-linux-android` is 1.6GiB and
`target/aarch64-apple-ios-sim` is 1.4GiB. Its older 43MiB Android `.so` and
223MiB simulator `.a` are source-incompatible and cannot be packaged. The
other two target caches are 1.5GiB Android x86_64 and 1.4GiB iOS device.
Reusing that target for a bounded mobile build would retain cached registry
objects but the integrated Git engine and SDK source must rebuild; estimate
another 1-3GiB plus transient link space. Measure `df -k .` before and during
each target, stop below 5GiB free, and avoid simultaneous targets. Do not
remove this worktree or its cache without confirming ownership and preserving
its branch/ref. No mobile build has started in this SDK checkout.

A read-only `ubrn build --help` unexpectedly began bootstrapping the local Rust
command binary and was interrupted. It created about 536MiB under the package
manager's `uniffi-bindgen-react-native/target` cache; no generated bindings or
native libraries resulted. Do not use the CLI just to inspect help. After the
corrected engine SHA and host tests pass, use the repository's explicit
`--and-generate --release --targets` commands and record exact source SHA,
engine SHA, generated checksums, native binary checksums, and package inventory.

## Historical validated engine serde repin before SDK test slot

The SDK manifests and lockfile now point to validated/pushed engine commit
`5f104c16e0d367b686fba6905550b38ef57653f4`, which restores the baseline
optimistic read JSON shape and adds engine golden tests. This was an exact SHA
replacement only; the SurrealDB 3.2.4 registry graph remains unchanged. Cargo
has not run against the new pin yet because the Android verifier owns the
brief build slot and free space temporarily fell below the 5GiB stop. Wait for
its cleanup/readback and explicit slot return. Then run the locked `--tests`
check and `intent_read_projects_unmaterialized_suffix_delete_conflict_and_reopen`
first, followed by the previously passing migration/receipt tests. Do not
start UniFFI/native artifacts until those tests pass and mobile disk headroom
is remeasured.

## Current source and verification checkpoint (2026-11-20)

The SDK pin is the validated engine `5f104c16e0d367b686fba6905550b38ef57653f4`.
On that pin, `cargo +1.96.1 check --locked --offline -p surrealdb-rn-core
--tests` passed using the shared authority target and one Cargo job. Focused
Rust tests passed:

- `intent_read_projects_unmaterialized_suffix_delete_conflict_and_reopen`
- `prepared_merge_preserves_large_numbers_across_replay_and_receipt`

The latter covers native merge preparation with versions and replacement
values above JavaScript's safe integer range, exact persisted request replay
after reopen, retained receipt identity, changed-request rejection, and an
accepted sequence above the safe integer range in the retained receipt. The
test passed with the final assertion. Four focused format 5 migration/receipt
tests and `--test engine_api_compat` also passed. The full cached Rust unit
suite passed with 175 passed, 3 intentional ignored, 0 failed. No generated
ABI or mobile binary has been produced for this source.

The public source facade in `src/sync.ts` now exposes
`prepareLaneResolution(snapshotJson, choice)` returning a native-generated
`{ resolutionId, requestJson }`, `submitPreparedLaneResolution` for exact opaque
replay, `inspectLaneResolution(snapshotJson, attempt)` for validated phase and
opaque receipt, and `projectLaneConflictSnapshot` for domain-only editor
values. The source-profile adapter must establish schema 2, format 5 and
pull-source-v2 capability with matching native methods before any lane
mutation. Supported exact lookup returns `absent` only while the original
blocked snapshot remains current; unsupported bindings or transport fail, and
stale missing evidence is `unknown`. `docs/SYNC_INTENT_LANES.md` documents
the prepare/persist/replay contract. The conflict adapter worker typechecked
`ExperimentalSyncClient` against its `ConflictSdkFacade` and reported 24
adapter tests plus 88 core/storage tests passing; that is source compatibility,
not a matching native artifact or device result. SDK source-profile Vitest
passed 27/27; the full package Vitest suite passed 158/158 across 18 files;
package `tsc --noEmit` passed after the facade changes.

The SDK released the Cargo slot after host tests. To reclaim only audited
old SDK host outputs, six paths under the shared authority target were removed
after a no-compiler and no-open-handle recheck; the exact path/size manifest is
`/tmp/sdk-obsolete-host-output-cleanup-20261120.json`. Current passing test
binary, current incrementals, dependencies, authority binary and native caches
were preserved. Free space after cleanup was 5,451,476 KiB, still short of
the Android diagnostic forecast. No more SDK build may start until the root
explicitly returns the sole slot and the agreed 5 GiB stop floor is viable.

The storage worker subsequently removed the separately verified inactive
`engine-authority-4` worktree with its branch and remote refs preserved;
the root measured 7,280,060 KiB free. The app authority then ran its bounded
Cargo test/build/Clippy slot and released it. The SDK completed the host tests
above and immediately released the slot for the next authority diagnostic
rebuild; free space at release was 7,002,984 KiB. The host verification
command was:

```sh
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/.t3/worktrees/roadmap-engine/feat-laptop-workspace-a-8bf94ffcab161fd7/crates/surrealdb-sync-surrealdb-authority/target \
  cargo +1.96.1 test --locked --offline -p surrealdb-rn-core \
  prepared_merge_preserves_large_numbers_across_replay_and_receipt -- --nocapture
```

The four focused migration/receipt commands used the same environment and
respective test-name filters. The full cached suite used `cargo +1.96.1 test
--locked --offline -p surrealdb-rn-core --lib --quiet` and exposed one stale
test assertion: `subscription_format_upgrade_preserves_pending_and_refuses_v2_downgrade`
was creating a new format 5 state with `save_atomic`, then asserting format 2.
It is now named `subscription_state_preserves_pending_and_refuses_v2_reader_or_state_downgrade`
and asserts format 5 before and after subscription operations; it still checks
that a format 2 reader and a subscription-dropping save fail closed. The
separate populated v2/v3 fixture still proves read-only legacy load followed
by rewrite on the first successful CAS save. The affected test and full suite
passed after correction. The older `sync-receipt-sdk` mobile cache remains
untouched. Native generation awaits explicit slot and headroom assessment.

## Generated lane ABI and partial native probes (2026-11-21)

The source is split into reviewable commits: `eba56329` is the Rust/FFI native
input pinned to engine `5f104c16`; `be42c754` is the handwritten runtime
wrapper with fail-closed optional lane capability checks; `77a9e7e` adds a
test-only SurrealKV rollback regression and does not change production native
input. The generated UniFFI TS/C++ files were produced by pinned
`uniffi-bindgen-react-native 0.31.0-3` from the `eba56329` input. Android arm64
and iOS arm64 simulator probes generated byte-identical TS/C++ bindings.
`Cargo.lock` SHA256 remained
`f54d6f6e372d9a4f825d9c6f76833af6a8d35c9d517760fb543e1883549931c4`.
The per-file hash and size inventory is
`/tmp/sdk-two-target-provenance-20261121.json`. Only Android arm64 and iOS
arm64 simulator native slices currently match the generated ABI, so this
source commit is not a full package. Preserve their raw and staged binaries.
The generated `surrealdb_rn_core.ts` is 323,750 bytes (up 38,064 bytes) with
SHA256 `b6f9879fa243af33f9f851bd655b46e26654fd1b009b5506c53958a732591393`;
the C++ source is 374,909 bytes (up 41,707 bytes) with SHA256
`77dab3f9322168e0c65875f344ba35ffbe98f9f8a11b92e1e1c6e4a7df2ed82d`.
These increases are the generated lane methods, source-profile transport,
resolution receipt, and intent read bindings. The current Android arm64 raw
and staged binary SHA256 is
`3d0bfa205ff099dd33e94c607fbf5a2a4ea019191eba5370dc512e23f06dc664`;
the current iOS arm64 simulator raw and staged archive SHA256 is
`d713b1760642122bc2eb05f96f81826198da4cfe214405b474defe55c5089900`.

The rollback test
`surrealkv_thread_replacement_failure_rolls_back_prior_native_transaction_writes`
passed with a locked/offline one-job host run. It uses the production native
begin/query/cancel path against SurrealKV: after UPSERT, DELETE, first CREATE,
and a failing second CREATE under a test-only UNIQUE index, cancellation keeps
the seeded thread and message intact immediately and after database reopen.
The SDK released the host Cargo slot afterward. Package TypeScript typecheck
passed; package Vitest passed 158/158 tests across 18 files after generation.
No package publish or device-compatibility claim follows from these checks.

### Full matching matrix when the native slot returns

The inactive `/Users/timhorgas/git/worktrees/sync-receipt-sdk/target` has
existing release caches for Android x86_64 (about 1.5 GiB) and iOS device
arm64 (about 1.4 GiB), as well as the two probed targets. The Android command
replaces staged `jniLibs`; the iOS command replaces the entire XCFramework.
Run both targets per platform in each command from
`packages/react-native-surrealdb`, sequentially:

```sh
env CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/git/worktrees/sync-receipt-sdk/target CARGO_NET_OFFLINE=true RUSTUP_TOOLCHAIN=1.96.1 ANDROID_NDK_HOME=/Users/timhorgas/Library/Android/sdk/ndk/27.1.12297006 ./node_modules/.bin/ubrn build android --and-generate --release --targets aarch64-linux-android,x86_64-linux-android
env CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/Users/timhorgas/git/worktrees/sync-receipt-sdk/target CARGO_NET_OFFLINE=true RUSTUP_TOOLCHAIN=1.96.1 IPHONEOS_DEPLOYMENT_TARGET=15.1 ./node_modules/.bin/ubrn build ios --and-generate --release --targets aarch64-apple-ios,aarch64-apple-ios-sim
./node_modules/.bin/prettier --write src/native.tsx src/NativeSurrealdb.ts
```

Require an explicit exclusive build slot, recheck cache ownership and no
active compiler, and start with roughly 9 GiB free to allow an estimated
2-4 GiB of new release output and transient linking while maintaining the
5 GiB stop floor. Measure `df -k .` before and during each platform build and
stop below the floor. After both commands, compare generated TS/C++ hashes
across Android and iOS generation from source `b0442a8`, then commit those
fresh generated files; record raw and staged binary hashes for all four
targets, check the XCFramework has device and simulator identifiers, and
confirm lock and manifest hashes are unchanged. Record raw hashes, then run
the repository's Android and iOS strip scripts and record staged hashes.
Run `release:check` against the stripped four-slice matrix before local
`release:pack`. Do not
publish, tag, or claim device acceptance from this matrix alone. Package
metadata now names the separate private candidate described below; keep that
version until its local archive is reviewed, and never overwrite the `haus`
alpha7 artifact.

The package verifier now requires nonempty iOS device and simulator archives,
as well as their XCFramework metadata. With the current partial matrix,
`node scripts/verify-package.mjs` correctly exits 1 and reports the missing
Android x86_64 and iOS device binaries (plus unbuilt JS declarations). This
is the expected state until both native platform commands and the package
build finish.

## Private TDT package candidate and vendored asset gate (2026-11-21)

The new local candidate name is `0.1.0-alpha.7.tdt.20261121.1`. A worktree
search and local/remote tag lookup found no collision; the npm version lookup
returned E404 (the registry also uses this response for inaccessible versions).
It is distinct from immutable `0.1.0-alpha.7.haus.20261023.1`. Only the package
wrapper version changed; the Rust crate version, pnpm lock graph, Cargo lock
graph, platform targets, and dependency versions did not. The package's
old `src/generated` and `cpp/generated` files came from pinned UniFFI generator
`0.31.0-3` and SDK native input `eba56329e744e65c1da674dcd1d316955d389aba`
at engine `5f104c16e0d367b686fba6905550b38ef57653f4`. They are superseded.
The validated replacement source commit `b0442a8c96b0f8d3133855afcd905d0723a14fb1`
is both `nativeInputSourceSha` and `runtimeSourceSha`; do not leave the latter
null in the final package proof. The test-only rollback commit `77a9e7e`
changed no production native input. The old generated source commit was
`39f38bd`; the package inventory fix is `3d257ed`.

The package allowlist retains `LICENSE` (MIT wrapper),
`LICENSE-APACHE-2.0` (engine client/protocol), exact locked SurrealDB 3.2.4
and protocol 0.10.2 license files, and `THIRD_PARTY_NOTICES.md` (including
MPL-2.0 UniFFI/generator attribution and SurrealDB Cargo license metadata).
The notice now identifies the canonical Git dependency SHA rather than the
historical copied source as the native core's actual input. At the local pack
gate, inspect the tarball's regular-file inventory and hashes, and confirm all
five license/notice files and `RELEASING.md` are present. Compare the final compressed and
unpacked bytes, file count, and per-slice sizes with immutable `haus` alpha7's
152,993,844-byte/155-file archive; explain size changes before accepting the
candidate. The generated TS increased 38,064 bytes and C++ increased 41,707
bytes over their prior checked-in versions because of lane/source-profile
bindings; the final binary and tarball deltas remain unmeasured until the
four-target build.

After both paired native commands above produce four matched slices, run from
`packages/react-native-surrealdb` using the already installed pnpm 11.5.0 and
Node 22.22.0:

```sh
pnpm run release:strip:android
pnpm run release:strip:ios
pnpm run release:check
pnpm run release:pack -- /absolute/owned/output/directory
```

`release:check` runs Bob to build JS declarations, package typecheck, Vitest,
and the four-slice package verifier. The pack helper uses
`npm pack --ignore-scripts`, records `npm-pack.json`, and enforces a
180,000,000-byte compressed ceiling; it does not rebuild or retest. Check the tarball name,
version, regular-file list and hashes, license/notice files, all four native
slice hashes/checksums and generated ABI against the source manifest. Keep raw
pre-strip and staged post-strip hashes separate. Consumer installation, RN compatibility
typecheck/native host tests, and iOS/Android device behavior remain separate
acceptance gates.

After the metadata and notice edits, package-local `tsc --noEmit` passed and
Vitest again passed 158/158 across 18 files. Prettier checked `package.json`,
and `git diff --check` passed. These are source checks; the full native matrix,
Bob declarations, tarball inventory, and consumer install are still pending.

`packages/react-native-surrealdb/RELEASING.md` requires a separately authorized
signed version tag, the Build release candidate workflow, and an approved
`npm-production` deployment through the Publish tested package workflow for
public npm publication. This task authorizes a local reviewable tarball only:
no tag, GitHub release, npm publish, or version reuse.

## Lane inventory source correction and superseded native probes

The app adapter found a recovery gap: `readSnapshot().snapshot.rows` omits a
new record whose unmaterialized create is followed by a local delete, although
its durable lane remains. The SDK is adding `readIntentLanes()` backed by one
native runtime lock over every engine-owned lane, plus the same read-journal
cursor used by `readChanges`. This is a new production Rust/FFI input, so the
earlier Android arm64 and iOS simulator probes, although hash-preserved above,
are superseded and must not be packaged with new bindings. The paired Android
command began before this gap was reported: arm64 completed with its previous
hash, then the command automatically entered x86_64. It was interrupted with
exit 130 during x86_64 compilation, before generation/staging; no compiler
remained and no iOS/full-package command ran. Free space after stop was
15,342,864 KiB.

The selected facade is `readIntentLanes(options?): Promise<{
revision: string; cursor: SyncReadCursor; lanes: readonly Lane[] }>` with each
lane carrying `recordId`, `pendingIntentIds`, `headIdentityJson`,
`blockedSnapshotJson`, `blockedReason`, and decimal `resolutionRevision`.
The existing SDK `createReadStore().watchCollection` emits even empty
row-change batches for lane-only revisions and emits a fresh snapshot after
`reset_required`; it owns polling and cancellation. Consumers re-enumerate on
snapshot/batch events, with no app durable ID index. Source tests cover the
absent projected row, persistence reopen, empty-batch invalidation, reset
cursor, blocked and awaiting-pull lanes, scope barriers, and large counters.
The source commit is `b0442a8c96b0f8d3133855afcd905d0723a14fb1`, both
`nativeInputSourceSha` and `runtimeSourceSha` for the new ABI. Its Rust and TS
inputs passed focused tests before push and remote readback. Fresh generated
ABI and native hashes are recorded below; the older probe hashes are historical only.
The package source `tsc --noEmit` passed and full Vitest passed 160/160 across
18 files after the inventory facade and empty-batch watcher tests. `cargo
+1.96.1 fmt --all` and `git diff --check` passed. The repository's existing
`sync.ts` and `sync-http.test.ts` do not pass whole-file Prettier at baseline;
formatting them wholesale would rewrite hundreds of unrelated lines, so the
source diff remains scoped.

The SDK then used a bounded host-only Rust slot with one Cargo job, the shared
authority target, `--locked --offline`, and the 5 GiB stop floor. The new
`intent_lane_inventory_retains_unprojected_delete_and_invalidates_empty_read_batch`
test passed. The affected blocked/reopen, accepted-awaiting-pull/reopen,
scope-barrier, and read-cursor tests passed individually. Full cached
`cargo +1.96.1 test --locked --offline -p surrealdb-rn-core --lib --quiet`
passed with 177 tests, 3 intentional ignored, and 0 failed. `cargo +1.96.1
fmt --all -- --check` passed. No compiler remained, and the host slot was
released at 13,369,016 KiB free.

The vendored-license audit inspected the exact locked registry contents:
`surrealdb`, `surrealdb-core`, and `surrealdb-types` 3.2.4 all declare
`license-file = "LICENSE"` with no `license` SPDX expression and share the
same 4,633-byte Business Source License 1.1 file (SHA256
`98a94ac615f88370865016487b436fa404560910bd329794ed7502277a94b805`).
Locked `surrealdb-protocol` 0.10.2 declares both `license = "BUSL-1.1"` and
`license-file = "LICENSE"`; its distinct 4,564-byte file has SHA256
`ace8dd034851d648d4104b7d20ab7bbf3d9666aa4e665e9fc5b865f4433aca13`.
Those exact files are now copied into the package allowlist and verified by
hash in `verify-package.mjs`, alongside wrapper MIT, engine Apache, and UniFFI
MPL notice. This closes a previously missing notice-text gap without making a
legal conclusion.

## Complete private four-target package candidate

With no SDK compiler or competing Cargo build active and 13,285,328 KiB free,
the SDK built sequential Android arm64/x86_64 and iOS device/simulator arm64
using the approved offline Rust 1.96.1, one Cargo job, and the existing inactive
`/Users/timhorgas/git/worktrees/sync-receipt-sdk/target` cache. The Android and
iOS UniFFI generations produced identical TS/C++ hashes. The XCFramework
contains exactly `ios-arm64` and `ios-arm64-simulator`. Cargo.lock, pnpm lock,
and Rust manifest SHA256 stayed `f54d6f6e372d9a4f825d9c6f76833af6a8d35c9d517760fb543e1883549931c4`,
`e142612f80f63e3aa7cb2f72a94cb6c16f8738d464d0643bf2490255b54322f0`,
and `faa7ef0c62ef211a6e8cbad229b0e2ff53fa63f31b0e8d07facfca5a77e2fc30`.
The raw four-target manifest is `/tmp/sdk-full-matrix-raw-20261121.json`.
The compiler slot was released after the iOS pair at 13,305,204 KiB free.

The repository strip scripts changed only staged libraries. The packed staged
SHA256 values are Android arm64 `8a1cac455933859bc3b734d7aa734bf8adc83532803905e573364b3e4534ebdb`,
Android x86_64 `0ea0c91f0a5d654f94fba9cbe729c3c918909bbf5fcbce74ef606842afdebb0f`,
iOS device arm64 `0c2317c79244c3365cfb4fb50608073356f1379284e0913d2ad2bb4bf2f2c38b`,
and iOS simulator arm64 `09a4e7f555590a568c19bc71641014ae52ba2f954288c10e8d2faf1aa2323bea`.
The generated TS and C++ hashes are `2f487856c935d04e0079388dadf5c707df27b877024096435c8ac6a311013b06`
and `231ac1455b7bf8add7fb5eaa76676dde974c1063273eece894da166cb649b625`.
The pre-strip raw hashes and all generated/staged paths remain in the raw
manifest; `/tmp/sdk-private-package-20261121/provenance.json` checks every
packed generated file and staged library byte-for-byte against the worktree.

Node 22.22.0 and installed pnpm 11.5.0 ran `release:check`: Bob module and
TypeScript declarations, `tsc --noEmit`, 160/160 Vitest tests in 18 files,
and the four-slice `verify-package.mjs` all passed. npm 12.2.1 returned its
current keyed JSON `npm pack` report, so `pack-release.mjs` now accepts either
the keyed object or historical single-element array. Three permanent Node
tests cover both shapes and zero, multiple, or malformed artifacts; all passed
within `release:check`. The first real pack exposed the format mismatch and
the final rerun passed the 180,000,000-byte ceiling.
The private candidate at
`/tmp/sdk-private-package-20261121/react-native-surrealdb-0.1.0-alpha.7.tdt.20261121.1.tgz`
is 58,027,318 compressed bytes, 179,995,135 unpacked bytes, 157 regular
files, SHA256 `c2ac15bcad246841b19a6ba1ce1104afff1ceeb862df0f7381cfcccb0b4d7d52`.
The npm report SHA1 matches the archive. Against immutable `haus` alpha7's
152,993,844-byte/155-file archive, this candidate is 94,966,526 compressed
bytes smaller and has two more files (the exact SurrealDB license texts).
The smaller archive is consistent with stripping the staged native symbols;
raw outputs are retained separately. The archive contains the wrapper MIT,
engine Apache, exact locked SurrealDB license texts, third-party notice,
module entry, and TypeScript declarations. No app consumer/device execution or
public publication is claimed by this package gate.

## 2026-11-21 RN86 source-proof intent run and private `.2` wrapper

The RN86 Android arm64 APK built offline with Gradle 9.3.1, one worker, and
`-PreactNativeArchitectures=arm64-v8a` after bounded retrieval of existing
declared Gradle artifacts. Its SHA256 is
`b7bf0b9d3af51455c30a878ac4077f10adccd49d6d27a63ab403d63e612fa0be`.
The synthetic two-client test used only `TDT_Intent_Sync_API36` on
`emulator-5580`, a task-owned SurrealKV server running SurrealDB 3.2.4 binary
SHA256 `bf14960ed517bd6d3eced7d37cfbe2d4ff795266f59421df56f6b1ee3bf0876b`,
and the exact `5f104c16` authority binary SHA256
`dd502b97122c35ff9af15835d5497799cdbc1989750b2ceec4be87e619cb5fb5`.
The first run exposed a missing source-v2 decoder in the canonical JavaScript
codec factory. Source commit `af57b6da08cc6153276a59952154e6f333e587c3`
wires the generated decoder and accepts the authority's expected pre-scan
`503` as startup readiness. The rerun passed 1/1 real native harness tests:
initial source pull, two offline stores, invisible create-delete lane, reopen,
stable retry/equivocation, materialize/push, other-client pull, and causal
completion. Redacted logs and hashes are in
`/tmp/sdk-intent-live-proof-20261121/`. This is local Android harness evidence,
not app activation or physical-device acceptance. The task-owned server and
AVD were stopped. AVD cleanup belongs to `verification_setup`; SDK must not
delete or reboot it. The private temporary DB directory and Gradle init script
remain because their deletion hit a runtime approval gate; do not retry it via
another route.

`0.1.0-alpha.7.tdt.20261121.2` is a JS-only private correction. Its
`nativeInputSourceSha` remains `b0442a8c96b0f8d3133855afcd905d0723a14fb1`;
its `runtimeSourceSha` is `af57b6da08cc6153276a59952154e6f333e587c3`.
No native library or generated ABI changed. `release:check` passed Bob output,
TypeScript, 161/161 Vitest tests, 3/3 pack parser tests, and four-slice package
verification. A timing-sensitive WebSocket test failed on the first full run,
then passed alone and in the complete rerun. Local `release:pack` produced
`/tmp/sdk-private-package-20261121-2-final/react-native-surrealdb-0.1.0-alpha.7.tdt.20261121.2.tgz`,
58,027,761 compressed bytes, 157 regular files, SHA256
`5431e24e84f41a850892743e8d225e109566314794bc7ddc58ff25a72641d511`.
`/tmp/sdk-private-package-20261121-2-final/provenance.json` verifies every packed
native slice, generated file, and license against the prior four-target matrix
and verifies that the module client contains the source-v2 decoder. The `.1`
archive remains immutable. No tag, npm publication, or app integration occurred.

## Query wire datetime source slice (host verified; native artifacts pending)

The next SDK source slice adds strict inverse decoding for the existing native
datetime tag. It accepts only a SurrealDB 3.2.4 RFC3339 datetime whose quoted
`d'...'` literal reserializes byte-for-byte, so malformed dates, offsets,
trailing SQL, noncanonical values, and extra fields fail before query execution.
The TypeScript bridge permits only `SurrealSqlValue("datetime", exactLiteral)`
among SQL-formatted values. Tagged record IDs now construct scalar key types and
require exact native serialization; quoted UUID string keys and numeric keys
remain distinct, while array/object/range keys are rejected. Source tests cover
nanoseconds, nested tagged values, NONE versus NULL, hostile literals, and a
SurrealKV read/copy/reopen. With one Cargo job, locked/offline Rust 1.96.1 and
the shared cached host target, `cargo test -p surrealdb-rn-core wire::tests::`
passed 7/7 and the named
`surrealkv_native_datetime_content_survives_wire_copy_and_reopen` passed 1/1.
The app-style `type::record('note', uuidString)` path returned the quoted UUID
string key, which decoded to its original typed ID. Focused Vitest passed 6/6,
package `tsc --noEmit` passed, and rustfmt/Prettier/diff checks passed. The
host slot was released with 7,233,696 KiB free. The existing `.2` archive and four native slices do not
contain this source and remain historical; do not package or vendor them as a
datetime-capable runtime.

`sync_codec.rs` still rejects native datetime (and other canonical types not in
the current engine protocol). `sync_state.rs` materializes full canonical
content with `.upsert(...).content(content)`. This query wire change does not
make full Note intent or pull lossless, and classifier activation remains gated
on a separate engine protocol/profile and app codec decision.

### Existing app UUID physical record identity (host verified)

The SurrealKV regression now additionally creates an app-style string-key Note
with `type::record('note', $uuidString)`, obtains `SELECT id` through the native
tagged wire, uses that exact tag as `$id` for lookup and update, and checks the
same physical row. It also creates a UUID-typed key with the same UUID text and
checks that the string-key update leaves that second row intact. The typed UUID
key remains unsupported by the narrow query decoder and must fail closed.
The TypeScript tagged `SurrealRecordId` re-encode test passes with the exact
quoted app UUID. The named jobs1/locked/offline SurrealKV test executed **1/1
and passed** under the shared host target (183 other unit tests filtered), with
6,561,360 KiB free after the slot was released. This proves the query-wire
physical UUID string-key path; it does not validate the app graph snapshot's
own RID acceptance rules, which currently reject hyphenated keys. The engine's structural
`ProtocolRecordId` remains a separate identifier contract and still uses
`parse_simple` in `sync_state.rs`; the query-wire result alone does not solve it.

## 2026-11-21 iOS archive link gate

The private `.2` iOS archives fail Xcode 27.2 linking because the release
artifact script's `strip -S -x` transformation removes required Mach-O unwind
relocations. The raw b0442 p521, p384, p256, and Edwards25519 members pass
focused `ld -r` probes for both device and simulator; the corresponding staged
members fail. Reapplying either strip flag independently to a raw p521 member
reproduces the failure.

The iOS release flow now leaves archives unstripped and runs
`release:verify:ios` on macOS after archive generation. The validator checks
archive architecture/index, regression member inventory, platform metadata,
unwind relocations, and links each member with the installed Xcode linker. The
same gate runs in native compatibility CI and release-candidate generation. The
pack ceiling checks npm's compressed `size`, not `unpackedSize`; the exact
packlist with raw b0442 device/simulator libraries measured 158,568,920 bytes
compressed and 523,633,111 bytes unpacked through `release:pack`.

Do not use the b0442 raw archives as the next candidate binaries. SDK source
HEAD `785bf9c` has later production datetime and typed record-key wire decoder
changes absent from b0442. Those archives were only diagnostic inputs; the
next candidate needs binaries built from its matching current Rust source.
No native rebuild, `.2` archive mutation, package publication, or consuming-app
build was performed. See
`docs/evidence/2026-11-21-ios-link-sdk2/README.md`.

## Native-values fingerprint and Set precision review (design only)

This SDK documentation review is based on engine draft
`df88ce632bf8784b8bed439a19da224ef9936583` and source-reads the locked
SurrealDB 3.2.4 dependencies. See
[`docs/NATIVE_VALUES_PROFILE_PRECISION_REVIEW.md`](docs/NATIVE_VALUES_PROFILE_PRECISION_REVIEW.md)
for a candidate domain-separated fingerprint envelope and hand-derived
RecordKey vectors, selected Set representation/wire-order rules, pinned source
references, and the concrete candidate value/message schema. The engine lock resolves
`rust_decimal` 1.42.1; the engine draft's 1.43.0 citation needs correction.

This is a design amendment plus an isolated value-codec spike: no integrated
native-values profile, profile negotiation, route, persistence binding, or
full Note sync is implemented. Focused tests of pinned SurrealDB 3.2.4 Set
construction show that the last equal-value representative is retained; native
equality is only the duplicate and decoder-cardinality relation. Thus equal
Sets can have different
fingerprints when their retained member type, Float bits, or Decimal scale
differs. Retries must retain exact canonical bytes and fingerprint. Draft
contract fixtures and narrow JS checks pin hand-derived fingerprint bytes and
Set vectors. The private Rust value codec passed its focused unit suite
(12/12); it does not integrate sync or prove fingerprint replay. The shared
literal decode vectors are in
`packages/react-native-surrealdb/test/fixtures/native-values-decode-vectors.json`.
An isolated SDK-side draft TypeScript CBOR value codec is also present at
`packages/react-native-surrealdb/src/native-values-draft-codec.ts`, with
literal-vector and malformed-input coverage in
`packages/react-native-surrealdb/test/native-values-draft-codec.test.ts`.
It is not exported or called from production, does not instantiate native
SurrealDB types or approximate native Set equality, and does not implement
profile conversion, persistence, transport, or activation. The 6 codec Vitest
cases (11 passed across both draft suites, 3 existing TODOs) and package
TypeScript check passed under Node 22. The Rust draft now applies its aggregate
1 MiB payload budget to strings/bytes/Object keys/RecordId table and string
keys; its 4 MiB writer guard remains separate. Empty string RID keys and
identity fields over 1,024 bytes are permitted within the aggregate budget;
empty tables remain rejected. Existing SDK invocation TODOs remain open; full
persisted Note sync remains blocked.
The `.3` SDK artifact does not
close the full persisted Note sync gate. See the “SDK draft contract fixtures”
section of the precision review for the exact validation boundary. The schema
proposal assigns kind IDs 0–13 under internal CBOR tag 60000 and explicit
push/pull envelopes; it preserves finite Float bits, rejects NaN/infinity per
the engine draft, pins the Decimal coefficient ceiling and Datetime
constructor validation, and defines local validation errors separately from
durable wire rejections. Tag
60000 is not claimed externally registered. It keeps v1
tags 6/8, pull-source-v2 family/media/route, and the acceptance-receipt-v2
family/media/path namespace distinct. Route selection and external tag-registry
check/registration remain pending. An isolated private value-codec spike and
native golden/rejection tests are now in `crates/surrealdb-rn-core/src/`, have
passed the focused Rust suite, and remain unwired. It covers value
nodes only, not messages, operations, fingerprints, persistence, or activation.
Because the candidate profile rejects non-finite floats accepted by the native
type, it is not proven lossless for every native Number and does not close the
full persisted Note sync gate.
