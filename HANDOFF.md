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
