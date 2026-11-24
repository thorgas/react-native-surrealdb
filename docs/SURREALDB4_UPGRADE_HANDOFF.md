# SurrealDB 4.0.1 upgrade + RN 0.87/0.88 hosts — handoff

Status: IN PROGRESS. Nothing pushed (owner: no online actions; push allowed after 19:00 only when told).

- Worktree: `/Users/timhorgas/git/rnsdb-surreal4`
- Branch: `feat/surrealdb-4`, base `origin/main` @ `020269a`
- PRs #14 and #15 were closed as superseded (all commits already on `main`); keep closed.

## Commits (each cherry-pickable, oldest first)

| SHA | Slice |
| --- | --- |
| `eba5c7a` | `surrealdb =4.0.1`; sized numbers widened onto `int`/`float`/`decimal` (`wire::widen_number`); `date`/`time`/`timestamp`/`interval` wire tags |
| `221da98` | `apps/harness-rn87` (0.87.1), `apps/harness-rn88` (0.88.1), catalogs, CI matrix, `react-native-test-app` 5.4.17 |
| `1ed6c26` | INFO allow-lists admit 4.0 keys, still fail-closed; adversarial test |
| `1cbd56d` | this handoff |
| `c0a4c0d` | `migrate_3x`: automatic 3.x -> 4 SurrealKV export/import on first `connect` (feature `migrate-from-3x`, default) |
| `17f7c39` | in-flight commit cancellation tests run on SurrealKV |
| `0813d54` | migration device E2E seeds 3.2.4 -> reopens 4.0.1; labels; changelog |
| `a2133d5` | `.cargo/config.toml` `RUST_MIN_STACK` for debug tests |

## Status

- `cargo test --workspace --all-features`: all pass (debug with `.cargo` stack, release with default stack).
- Clippy `-D warnings` clean; package vitest 162/162; rn85-rn88 typecheck clean (after `pnpm --filter react-native-surrealdb run build`).
- Release build: default 2 MiB stack is enough, so no production stack change.

## Open items

1. Device E2E (iOS sim UDID 8F752138-1669-4549-94F4-F403770FCB30, AVD Pixel_9_PlayStore on port 5560)
   for rn86/87/88 plus `e2e:surrealkv-migration:{ios,android}` — delegated, pending.
2. Native size grows (bundled 3.x reader); measure and record in NATIVE_SIZE.md.
3. Push after 19:00 when the owner allows online actions; open the PR.

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
