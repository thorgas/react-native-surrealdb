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

The checked-in generated binding source changed, but release iOS/Android native artifacts are
ignored and have not yet been rebuilt. A device claiming the opt-in path needs fresh native
artifacts and a development build; the existing released alpha package remains manual-only.
