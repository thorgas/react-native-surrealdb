# React Native SurrealDB

An experimental React Native binding for SurrealDB, built from the SurrealDB Rust SDK through UniFFI/JSI.

## Current status

The first native-core milestone is implemented:

- SurrealDB 3.2.1 with `mem://`, `surrealkv://`, `ws://`, and `wss://` engine features;
- async UniFFI API for connect, query, namespace/database selection, token authentication, root/database sign-in, invalidation, and close;
- versioned JSON wire values that preserve 64-bit integers, decimals, bytes, UUIDs, record IDs, `NONE`, sets, and other non-JSON SurrealDB values;
- host tests for queries, Hermes-safe integer transport, idempotent close, and SurrealKV persistence/reopen;
- verified cross-compilation and optimized linking for iOS arm64 simulator and Android arm64 (NDK 27, API 24).

The React Native package, generated bindings, live-query subscription handles, and example/Harness app are the next milestone.

## Development

```sh
cargo test -p surrealdb-rn-core
cargo clippy -p surrealdb-rn-core --all-targets -- -D warnings
```

The Rust toolchain and dependency graph are pinned through `rust-toolchain.toml` and `Cargo.lock`.

## Design documents

- [Architecture research](./RESEARCH.md)
- [Performance and device-test strategy](./PERFORMANCE.md)
