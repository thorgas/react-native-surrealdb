# `surrealdb-sync-client`

This dependency-light crate is the production-shaped, storage-neutral client state machine for
`surrealdb-sync/1`. It uses the accepted wire types directly and does not perform database, network,
clock, or async-runtime work.

An adapter owns serialization and atomic persistence:

```rust,ignore
let prepared = runtime.prepare_enqueue(commit)?;
storage.save_atomic(prepared.state()).await?;
runtime.install(prepared)?;
```

The adapter must serialize transitions for one client. If persistence fails, discard the prepared
transition; the runtime remains unchanged. On process restart, deserialize the complete
`DurableClientState<V>` and call `ClientRuntime::open`, which validates it before rebuilding the
optimistic projection.

Accepted commits remain in the derived optimistic projection until a complete pulled checkpoint
reaches their authority sequence. Conflict and rejected outcomes retain the original local commit
for explicit resolution but are not silently presented as canonical state.

Run the focused suite from any directory:

```bash
cd /path/to/react-native-surrealdb
cargo test -p surrealdb-sync-client
cargo clippy -p surrealdb-sync-client --all-targets -- -D warnings
```

The copied public tests cover crash boundaries, duplicate delivery, incomplete pulls, conflicts,
resets, and ID mapping. The private authority-cycle and comprehensive conformance suites remain in
the canonical development repository. These tests do not prove a SurrealKV adapter, actual HTTP
transport, canonical wire codec, or mobile lifecycle.
