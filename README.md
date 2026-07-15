# React Native SurrealDB

An experimental React Native binding for SurrealDB, built from the SurrealDB Rust SDK through UniFFI/JSI.

## Current status

The native core and first React Native integration milestone are implemented:

- SurrealDB 3.2.1 with `mem://`, `surrealkv://`, `ws://`, and `wss://` engine features;
- async UniFFI API for connect, query, namespace/database selection, token authentication, root/database sign-in, invalidation, and close;
- pull-based live-query handles with backpressure, async iteration, explicit cancellation, and automatic cancellation when the database closes;
- versioned JSON wire values that preserve 64-bit integers, decimals, bytes, UUIDs, record IDs, `NONE`, sets, and other non-JSON SurrealDB values;
- checked-in UniFFI/JSI generated bindings and a hand-written TypeScript facade for React Native's New Architecture;
- host tests for queries, live notifications, Hermes-safe integer transport, idempotent close, and SurrealKV persistence/reopen;
- a `react-native-test-app` host with React Native Harness integration tests passing on React Native 0.86/Hermes V1 with an iPhone 17 Pro simulator and an Android API 36 arm64 Pixel 9 emulator;
- authenticated remote WebSocket live-query integration tested against a real SurrealDB server;
- size-oriented Rust LTO plus a separately packaged Android Rust `cdylib`, reducing the measured arm64 RNTA app increment from 61.88 MiB to 24.31 MiB while preserving 16 KB page-size support (NDK 27, API 24 minimum);
- the complete default crud-bench workload matrix as 141 device-side metrics, with smoke/canonical/upstream profiles, an RNTA manual benchmark lab, raw samples, and compatible-baseline regression checks.

Automatic WebSocket re-subscription and event deduplication across reconnects remain future work; the current subscription terminates when its SDK stream terminates.

## Development

```sh
./scripts/verify-core.sh
pnpm --filter react-native-surrealdb test
pnpm --filter react-native-surrealdb typecheck
```

To exercise the opt-in authenticated WebSocket integration test, start a local server and run:

```sh
surreal start --no-banner --bind 127.0.0.1:18080 --user root --pass root memory
SURREAL_TEST_WS_ENDPOINT=ws://127.0.0.1:18080 \
  cargo test -p surrealdb-rn-core authenticated_websocket_live_query -- --ignored
```

The Rust toolchain and dependency graph are pinned through `rust-toolchain.toml` and `Cargo.lock`.

## Design documents

- [Architecture research](./RESEARCH.md)
- [Performance and device-test strategy](./PERFORMANCE.md)
- [Native size and Rust implementation decisions](./NATIVE_SIZE.md)
