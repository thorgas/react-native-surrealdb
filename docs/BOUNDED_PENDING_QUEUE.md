# Bounded pending queue bridge

`ExperimentalSyncClient.pendingWindow(maxCommits, maxBytes)` borrows a FIFO prefix
under the native lock. It requires the new matching native ABI, with no
`pendingProtocolJson()` fallback. Existing APIs, protocol and storage formats are
unchanged. There is no migration or default app activation.

Limits: 1–64 commits, 2–4,194,304 bytes. `bytes` counts the exact UTF-8 JSON encoding
of the returned **string array**, including brackets, commas, quotes and escaped
protocol JSON. Fixed result metadata and UniFFI length headers are additional
bounded framing: the generated UniFFI record adds 21 fixed bytes plus 4 bytes per
string (at most 277 bytes for 64 strings), excluding allocator/FFI buffer-object
overhead. Thus its serialized record payload is at most 4MiB + 277 bytes. This is
not peak RSS. Checked native writes refuse before
exceeding the remaining serialization budget. At most one bounded candidate and
its bounded quoted form coexist with the bounded result. Oversized first commit:
empty prefix, `firstCommitTooLarge: true`, all items remain. Later oversized
commit: return the fitting prefix. Nothing is dropped or skipped.

`revision` is the observed local revision. There is no offset cursor: after each
durable push outcome request the FIFO again. Conflict/rejected/accepted-not-pulled
history is not pending FIFO. A window is not an exclusive networking or
authorization lease. Serialize coordinator work, retain captured owner/scope
checks around awaits, stop scheduling and drain actual cycle work before close.

`enqueueBounded(commit, maxPendingCommits, maxPendingBytes)` atomically checks
existing FIFO plus candidate with identical accounting before preparing/cloning
and persisting. Capacity refusal leaves state unchanged. Existing shielded-worker,
atomic persistence and uncertainty-poison behavior apply. Exact retries in FIFO
or retained outcome history return current status without charging twice; reused
identity with different operations is refused. If acknowledgement is lost, retry
the same exact client commit identity. Existing history retention still applies.

Admission is **per call**, not a persisted policy certificate. Legacy `enqueue()`
and other trusted writers can exceed selected limits. Reopened queues are never
assumed to have followed them; bounded reads independently refuse oversize output.
Do not bypass capacity with the old API. Persistent policy storage, automatic
retention and app conflict/recovery UX are outside this change. Existing journal
limits remain 4MiB/component, 64MiB aggregate and 16,384 components.

## Reproduce source checks and binding generation

Prerequisites: repository checkout, Rust 1.96.1, Node 22.22.0 and pnpm 11.5.0 already
installed; the SDK's frozen existing dependencies; an existing compatible host
Cargo target; an already-built `uniffi-bindgen-react-native` 0.31.0-3 executable;
Prettier and clang-format already installed. Do not install/change toolchains or
rebuild dependencies implicitly. `ubrn`/the npm generator wrapper runs `cargo run`
even for help and may bootstrap a missing generator. Use its existing native
executable directly. If prerequisites are absent, stop and report exactly which.

Run from repository root. Set `SDK_QUEUE_TARGET` to the existing compatible Cargo
target directory and `SDK_GENERATOR_BINARY` to the existing 0.31.0-3 native
executable. These are explicit local prerequisites, not executable placeholders.

```sh
node --version                 # v22.22.0
pnpm --version                 # 11.5.0
rustc --version                # 1.96.1
: "${SDK_QUEUE_TARGET:?set existing compatible Cargo target}"
: "${SDK_GENERATOR_BINARY:?set existing prebuilt generator executable}"
test -d "$SDK_QUEUE_TARGET/debug/deps"
test -x "$SDK_GENERATOR_BINARY"
# Require at least 11 GiB free; stop if target growth exceeds 256 MiB or evidence 100 MiB.
df -k .
du -sk "$SDK_QUEUE_TARGET"
export CARGO_TARGET_DIR="$SDK_QUEUE_TARGET"
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cargo fmt --all --check
cargo test --locked --offline -p surrealdb-rn-core sync_queue -- --nocapture
cargo test --locked --offline -p surrealdb-rn-core bounded_queue -- --nocapture
# Includes child-process exit 23 after durable ACK, bypassing Drop, then reopen.
cargo test --locked --offline -p surrealdb-rn-core cancelled_bounded_enqueue -- --nocapture
cargo test --locked --offline -p surrealdb-rn-core returned_commit_or_rollback_uncertainty -- --nocapture
cargo test --locked --offline --workspace
cargo clippy --locked --offline --workspace --all-targets -- -D warnings
cargo build --locked --offline -p surrealdb-rn-core --lib
mkdir -p output/bounded-queue/generated-ts output/bounded-queue/generated-cpp
"$SDK_GENERATOR_BINARY" generate jsi bindings --library --no-format \
  --ts-dir output/bounded-queue/generated-ts \
  --cpp-dir output/bounded-queue/generated-cpp \
  "$SDK_QUEUE_TARGET/debug/libsurrealdb_rn_core.dylib"
```

The final library suffix above is macOS host-specific. This recipe intentionally
does not guess a Linux/Windows artifact path. Generated output remains isolated:
format its two TS files with SDK Prettier and its two C++ files with clang-format,
then review them before replacing canonical generated files. Break any hardlinks
before writing generated/source outputs; published archives remain immutable.
Generate twice and compare the formatted four files to prove reproducibility.

From `packages/react-native-surrealdb`, run:

```sh
pnpm exec prettier --check src/sync.ts test/sync.test.ts test/helpers/native-read-mocks.ts
pnpm exec tsc --noEmit
pnpm exec vitest run test/sync.test.ts --maxWorkers=1
pnpm exec vitest run --maxWorkers=1
```

Expected: focused queue, reopen, retry, cancellation and uncertainty tests pass;
full SDK TS suite has 150 tests/zero skips. The HTTP network test needs ephemeral
127.0.0.1 binding; sandbox EPERM is a blocked gate, not a passed/skipped test.
No device/database server is required. Tests own temporary databases and close
them; the localhost test owns and closes its server. Retain logs and generated
source proof; remove only obsolete task-owned output after review, never shared
Cargo/dependency caches without authorization.

This changes the UniFFI ABI: 72 previous checksum functions plus 2 new methods,
a result record and an appended error variant. Build and verify **all four**
shipped native libraries before packaging: Android arm64-v8a/x86_64, iOS arm64 and
arm64 simulator. The published alpha7 archive contains the preceding ABI and must
not be repacked. Host tests/codegen do not establish mobile execution or product
startup performance. Machine-specific commands/hashes belong in HANDOFF/evidence.

## Exact bigint CAS input bridge

RN commit inputs accept the SDK wire encoder's `{ "$surreal": "int", "value": "5" }`
in upsert `base_version.exact` and delete `base_version`, including conflict-merge
replacement commits. Decimal digits are parsed directly into `u64`; zero and
`18446744073709551615` are supported. Legacy JSON integer inputs remain accepted.
Negative, fractional, overflowing, missing, extra-field or mismatched integer tags
fail with `InvalidInput` before journal mutation. Never convert bigint through
JavaScript `Number`.

The decoder is specific to these typed protocol positions. Payload JSON retains
its scalar tags and identically named nested keys verbatim. The canonical native
fingerprint still derives from the decoded protocol commit; numeric and tagged
versions have the same identity. Bounded enqueue accepts exact retries without
charging twice; legacy enqueue retains its duplicate-identity refusal.

The shared regression fixture is checked against the real JS `encodeSurrealValue`
and consumed by Rust native-operation tests. Run the existing host prerequisites
above, then from repository root:

```sh
cargo test --locked --offline -p surrealdb-rn-core bigint_tests -- --nocapture
```

From `packages/react-native-surrealdb`:

```sh
./node_modules/.bin/vitest run test/bigint-cas.test.ts --maxWorkers=1
```

This input-only correction changes no UniFFI signatures, generated bindings,
canonical protocol crate or storage schema. It does not remove the outstanding
mobile gate: all four matching native libraries must be rebuilt and verified
before packaging or activation. Existing binaries still predate the bounded-queue
ABI. Host regressions do not establish mobile execution or release readiness.
