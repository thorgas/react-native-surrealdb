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

## 2026-10-23 bounded pending queue source slice (unpublished)

Additive `pendingWindow(maxCommits,maxBytes)` and `enqueueBounded` reuse the
existing runtime lock/shielded atomic persistence. `docs/BOUNDED_PENDING_QUEUE.md`
is the portable command/semantics contract. Per-call admission is not a durable
policy certificate; old enqueue/pending APIs retain their behavior. FIFO windows
remain independently bounded for reopened/legacy queues, with explicit oversized
first item and no snapshot/all-pending fallback. Exact durable retries in FIFO or
retained outcomes do not enqueue/charge again. No storage migration.

Evidence: `output/bounded-queue-20261023/`. Existing runtime `target` was reused
with Rust1.96.1/jobs1/incremental0/dev+test debug0, locked/offline; target/debug
allocated size grew from2,406,920KiB to about2,410,320KiB. No dependency rebuild,
mobile crosscompile, package version change, publication or device action.
Actual existing prebuilt generator used directly:
`/Users/timhorgas/git/react-native-surrealdb/node_modules/.pnpm/uniffi-bindgen-react-native@0.31.0-3/node_modules/uniffi-bindgen-react-native/target/debug/uniffi-bindgen-react-native`.
Do not use its npm wrapper for help: that wrapper runs Cargo and can bootstrap.
Node22.22.0 came from the existing fnm installation; SDK pnpm pin remains11.5.0.
Existing SDK dependency closure was reused; root SDK node_modules was not restored.

Full workspace:239 tests pass (175 core+64 other),3 inherited core ignores.
Both ignored persistent-reopen cases were separately run against fresh owned
`inspection-fixture`/`owner-fixture` under the evidence directory and passed2/2.
Authenticated websocket external-server test remains unexecuted in this slice.
Workspace/all-target warning-denied Clippy, rustfmt, typecheck and SDK TS150/150
pass. HTTP localhost test initially hit sandbox EPERM; approved ephemeral-localhost
rerun passed without timeout changes. Abrupt process exit23 after durable ACK
bypasses Drop/close; actual parent reopen and duplicate retry pass. Concurrent
bounded admission has one durable winner; cancellation and commit/rollback
uncertainty reject unsafe reuse. Output records retain exact logs.

Four generated files reproduce byte-identically after formatting. All74 actual
host checksum function values match TS; previous72 are unchanged. Two new native
methods/result record/appended error variant require matching new native binaries.
Android/iOS canonical4 libraries remain unchanged and do NOT implement this ABI.
The immutable published alpha7 tar SHA408016b9840e750aeea33a562430af5f328c7c5fa72af9b464eae4afb7ce5b5f
still contains the prior ABI. Old hardlinked pack stages are mutable source views,
not frozen publication evidence. No next wrapper/artifact has been packaged.


### Wrap-up blocker and publication boundary

The bounded queue source slice is committed separately from activation. Its known
CAS bridge gap is explicit in `docs/BOUNDED_PENDING_QUEUE.md`: actual SDK
`encodeSurrealValue` encodes bigint `base_version` as an int-tag object, whereas
RN `ClientCommit` inputs use plain serde u64 at old enqueue, bounded enqueue and
conflict merge. This blocks compiler-emitted bigint CAS, including5n; unsafe values
must not be converted to Number. No correction, new cross-build or package was
attempted after the user requested wrap-up. Next work is planning-only until
explicit implementation authorization.

The SDK origin is PUBLIC `thorgas/react-native-surrealdb`, while this local
`private/bootstrap-staging-20261019` branch is101 commits beyond its public main.
It includes temporary canonical sync crates, private bootstrap/roadmap work,
Hauswirtschaft-specific packaging/harness history and machine-local evidence.
A full fast-forward would publish all that history. Do not push or rebase this
branch onto public main without an exact reviewed publication decision. Prefer
an explicit audited export/cherry-pick set from a clean public-main checkout,
with licenses/source-origin/privacy review and its real dependencies included.
This commit is local durable handoff only; no public push is authorized by the
coordinator yet.

App facade freeze manifest:
`roadmap-next-app/output/household-command-session-20261023/source-freeze.json`.
It records6 owned app paths,122 passing host regressions, scope/network lifetime
behavior and the same deferred CAS bridge blocker. It is not native acceptance.

## 2026-10-23 exact bigint CAS boundary correction

Branch `fix/rn-bigint-cas-boundary`, base public main
`eec57effd9e08860b4c818975e728a93543ddf3d`. This section supersedes the deferred
CAS decoder blocker above. A typed RN-only commit decoder accepts lossless int
wire tags solely in upsert exact/delete version fields at enqueue, bounded enqueue
and conflict merge. Payload JSON, canonical protocol/client crates, identity
fingerprinting, storage format and UniFFI signatures remain unchanged. No binding
regeneration or native library build is required to review this input-only patch.
Matching mobile builds are still required to execute it.

Permanent shared fixtures are verified by the real JS encoder and consumed by
native operation tests for zero,5,9007199254740993 and u64MAX, update/delete,
payload tag/key preservation, malformed input without queue mutation, numeric/tag
identity equivalence, bounded retry, changed-version identity refusal, disk reopen
and conflict merge. The before-fix native operation returned InvalidInput on the
first bigint fixture while all eight real JS encoder assertions passed. After the
fix all three native regression tests pass. Legacy enqueue retains duplicate
Protocol refusal; bounded enqueue retains exact idempotent retries.

The delete tests seed an existing physical row. An exploratory delete of a
nonexistent row returned Persistence; that separate behavior remains unresolved
and is not covered up by a protocol change. This correction is not a claim that
all sync defects are resolved.

Host validation uses the existing compatible Cargo target and installed SDK
dependency closure, Rust1.96.1, Node22.22.0, jobs1/incremental0/dev+test debug0.
A 10GiB hard-free guard applies to these scoped host checks; no dependencies or
native cross-builds should be bootstrapped. From repository root:

```sh
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cargo test --locked --offline -p surrealdb-rn-core bigint_tests -- --nocapture
cargo test --locked --offline --workspace
cargo clippy --locked --offline --workspace --all-targets -- -D warnings
cargo fmt --all --check
packages/react-native-surrealdb/node_modules/.bin/tsc --noEmit \
  -p packages/react-native-surrealdb/tsconfig.json
cd packages/react-native-surrealdb
./node_modules/.bin/vitest run --maxWorkers=1
./node_modules/.bin/prettier --check test/bigint-cas.test.ts test/fixtures/bigint-cas.json
```

The full TS suite needs ephemeral localhost binding. A sandbox EPERM run is not
a passed gate; rerun with that permission. No package installation, generated
binding change, platform artifact replacement, app activation, publication, tag
or release is part of this fix. Existing four mobile libraries remain on the
older ABI. Next mobile work requires matching all four artifacts and real device
acceptance; host results do not satisfy that gate.

Final host gates:242 Rust workspace tests passed,3 inherited core ignores;
158 SDK TS tests passed with zero skips on the localhost-enabled rerun. Strict
workspace/all-target Clippy, rustfmt, TypeScript and focused Prettier passed.
Both inherited persistent reopen gates were run explicitly and passed (2/2).
The authenticated external-server websocket test remains unexecuted. Diagnostic
logs are local under `output/bigint-cas-20261023/` and excluded from publication.
