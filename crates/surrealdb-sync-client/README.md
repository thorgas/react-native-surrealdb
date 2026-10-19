# `surrealdb-sync-client`

This dependency-light crate is the production-shaped, storage-neutral client state machine for
`surrealdb-sync/1`. It uses the accepted wire types directly and does not perform database, network,
clock, or async-runtime work.

An adapter owns serialization and atomic persistence:

```rust,ignore
let expected_revision = runtime.state().revision;
let prepared = runtime.prepare_enqueue(commit)?;
storage
    .save_atomic(Some(expected_revision), prepared.state())
    .await?;
runtime.install(prepared)?;
```

The adapter must serialize transitions for one client. If persistence fails, discard the prepared
transition; the runtime remains unchanged. On process restart, deserialize the complete
`DurableClientState<V>` and call `ClientRuntime::open`, which validates it before rebuilding the
optimistic projection.

Accepted commits remain in the derived optimistic projection until a complete pulled checkpoint
reaches their authority sequence. Conflict and rejected outcomes retain the original local commit
for explicit resolution but are not silently presented as canonical state.

Within a known unchanged epoch, a delayed reset covers accepted receipts only at or below its
frontier. Newer accepted intent survives durable restart until a later pull/reset covers it.
Higher-epoch and initially unknown-epoch behavior is unchanged.

Run the focused suite from any directory:

```bash
cd /path/to/react-native-surrealdb
cargo test -p surrealdb-sync-client
cargo clippy -p surrealdb-sync-client --all-targets -- -D warnings
```

The copied public tests cover crash boundaries, duplicate delivery, incomplete pulls, conflicts,
resets, and ID mapping. The private authority-cycle and comprehensive conformance suites remain in
the canonical development repository. Separate `surrealdb-rn-core` tests exercise the initial
SurrealKV adapter; neither suite proves an actual HTTP transport, canonical wire codec, or mobile
application lifecycle.

## Complete bootstrap snapshot preparation

`prepare_bootstrap_snapshot(checkpoint, records)` prepares a complete snapshot from typed values,
without fabricating a legacy reset reason. The external adapter must first verify the complete
ordered root, completion receipt, trusted context, expiry and its own local storage budgets.
This method supplies neither authorization nor a verified-bootstrap/install capability. It does
not persist, install, publish rows, retain pages or perform I/O. The adapter must atomically persist
the entire prepared state before installing it under serialized journal ownership.

Bootstrap records must be Present or Tombstone with positive versions; explicit Absent is invalid.
Complete enumeration establishes absence. Existing scope, subscription authorization, cursor,
duplicate-record and revision guards apply. Pending work and conflicts remain durable. In a known
unchanged epoch, accepted receipts at or below the bootstrap frontier are covered and newer accepted
intent survives restart. Any retained Accepted receipt, including one already covered, prevents a
bootstrap across an unknown or different epoch (`NeedsNewScopeJournal`). Legacy reset behavior is
unchanged. This API is one storage-neutral preparation step, not the full resumable native bootstrap.
