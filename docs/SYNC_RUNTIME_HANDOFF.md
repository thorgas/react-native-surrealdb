# Sync runtime adapter handoff

Updated: 2026-08-28

## Branch goal

Stage temporary copies of the client-side Rust protocol/runtime crates and prove their first
SurrealKV persistence boundary. This branch intentionally exposes no React Native sync API and is
not usable as a synchronization engine.

- Runtime-copy commit: `9667aec2adc1a1b739d3742df8fa4439237501ec`.
- Persistence-adapter commit: `0f06fd777e1dd4e4aae06f86a16ab293f54c7b44`.
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

## Implemented persistence boundary

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
  outcome instead of claiming the write failed.

The JSON value/payload choice is a persistence prototype, not the canonical sync wire codec.
Application records are not yet updated in the same transaction, and no API is exported over
UniFFI.

## Verification

From the repository root:

```bash
cargo fmt --all --check
cargo clippy -p surrealdb-rn-core --all-targets -- -D warnings
cargo test -p surrealdb-rn-core sync_state
./scripts/verify-core.sh
```

The focused adapter tests cover revision conflicts, idempotent retry, explicit rollback, committed
replacement, malformed and semantically corrupt payloads, size limits, structured identity keys,
and SurrealKV close/reopen with an uncommitted transaction.

All documented commands passed on 2026-08-28. `./scripts/verify-core.sh` ran formatting,
warning-denied workspace Clippy, all workspace tests, and Rust checks for the iOS arm64 simulator
and Android arm64 targets.

## Next implementation slice

1. Extend `crates/surrealdb-rn-core` so optimistic domain-record writes and the prepared sync-state
   replacement commit in one transaction.
2. Replace abstract string record IDs and generic JSON values with an explicit mapping to native
   SurrealDB record IDs, nested values, arrays, links, and graph edges.
3. Add a narrow UniFFI facade only after the combined transaction passes: `open`, `enqueue`,
   `pending`, `record_outcome`, `apply_pull`, `conflicts`, and `status`.
4. Add React Native harness tests for enqueue, app restart, optimistic reconstruction, conflict
   retention, and pull application.
5. Keep network transport and `DEFINE API` authority deployment in separate reviewed slices.

## Hard blockers

- The long-term single-source/export mechanism is unresolved.
- GitHub Actions may remain unavailable until account billing permits runner allocation.
- The canonical native-value storage/wire codec is not selected yet.
- This branch must remain a draft and must not be released or advertised as sync support.
