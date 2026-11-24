# SurrealDB 4.0.1 upgrade + RN 0.87/0.88 hosts — handoff

Status: IN PROGRESS. Nothing pushed (owner: no online actions; push allowed after 19:00 only when told).

- Worktree: `/Users/timhorgas/git/rnsdb-surreal4`
- Branch: `feat/surrealdb-4`, base `origin/main` @ `020269a`
- PRs #14 and #15 were closed as superseded (all commits already on `main`); keep closed.

## Commits (each cherry-pickable)

| SHA | Slice |
| --- | --- |
| `eba5c7a` | `surrealdb =4.0.1`; sized numbers widened onto `int`/`float`/`decimal` wire tags (`wire::widen_number`); new `date`/`time`/`timestamp`/`interval` tagged SurrealQL strings (Rust + `SurrealSqlKind` in `wire.ts`); sync codec rejects the new temporal kinds |
| `1ed6c26` | INFO allow-lists admit 4.0 keys (`buckets`, `compatibility`=4, `extensions`, `types`), still fail-closed; adversarial test `surrealdb4_buckets_types_and_compatibility_changes_fail_closed` |
| `221da98` | `apps/harness-rn87` (0.87.1), `apps/harness-rn88` (0.88.1), catalogs, CI matrix, `react-native-test-app` 5.4.17 |

## Open items

0. BLOCKER (owner decision): SurrealDB 4.0.1 refuses to open SurrealKV data written by 3.x
   ("Upgrading it in place to SurrealDB 4 is experimental and is refused by default").
   Opt-in is datastore config `experimental_upgrade_from_3x` (env SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X);
   the conversion is one-way. Probe source: scratchpad `compat-probe/` (3.2.4 writes, 4.0.1 opens).
   Latest 3.x is 3.3.5 (on-disk compatible alternative).
1. 2 lib tests still fail: `sync_client::tests::cancelled_*enqueue_settles_before_sibling*` assert that the
   first commit poll is Pending; 4.0.1 commits the mem engine directly (no channel) and wraps commit in
   `RollbackOnDrop`. Needs owner decision (see chat).
2. Default 2 MiB test-thread stack overflows on 4.0.1; tests currently need
   `RUST_MIN_STACK=33554432`. Check whether production threads need a larger stack.
3. Replace remaining `3.2.4` mentions (harness `App.tsx` / benchmark labels,
   `run-surrealkv-migration-e2e.sh`, docs) and prove 3.2.4 -> 4.0.1 on-disk reopen.
4. Device E2E on rn87 + rn88 (iOS + Android), then the full matrix.

## Commands

```sh
cd /Users/timhorgas/git/rnsdb-surreal4
pnpm install
pnpm --filter react-native-surrealdb run build      # required before any harness typecheck
RUST_MIN_STACK=33554432 cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
pnpm --filter react-native-surrealdb test
(cd apps/harness-rn88 && pnpm typecheck)
```

Disk: Rust `target/` for this worktree is ~7 GB; keep >= 20 GB free before building.
