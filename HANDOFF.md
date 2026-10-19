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
