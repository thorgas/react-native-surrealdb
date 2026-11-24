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
| `80025db` | package README: 3.x migration + new wire kinds |
| `8ac9a13` | rn87/rn88 op-sqlite 19.0.5 (RN 0.87 removed `RCTCxxBridge`); simulator defaults; migration E2E `SURREALDB_IOS_UDID`; rn86 ignores two-phase `sync-binary-upgrade`; reopen-reset Rust test; README device prerequisites |

## Status

- `cargo test --workspace --all-features`: all pass (debug with `.cargo` stack, release with default stack).
- Clippy `-D warnings` clean; package vitest 162/162; rn85-rn88 typecheck clean (after `pnpm --filter react-native-surrealdb run build`).
- Release build: default 2 MiB stack is enough, so no production stack change.

## Device evidence (2026-11-24, iOS sim 8F752138, iOS 26.1)

- `e2e:surrealkv-migration:ios`: PASS. A 3.2.4 build (020269a) seeded app data, then the 4.0.1 runtime migrated it on reopen.
- rn86 iOS: 15/17. `sync-binary-upgrade-verify` needs a cold stop (now ignored in `e2e:*`);
  `sync-read` reopen assertion depends on Hermes GC freeing the closed handle (pre-existing, not 4.0;
  Rust test `reopened_handle_requires_reset_for_pre_close_cursor` pins the contract).
- rn87/rn88 iOS: failed on op-sqlite 17.1.1 before `8ac9a13`; not re-run yet.
- Android: not run (free disk fell below 8 GB because of other agents' builds).

## Open items

1. Re-run iOS rn87/rn88 and all of Android (rn86/87/88 + `e2e:surrealkv-migration:android`). Needs about 30 GB free.
2. Measure Release native size (NATIVE_SIZE.md baseline: 24.28 MiB arm64 `.so`); 3.x reader roughly doubles it.
3. Push `feat/surrealdb-4` and open the PR once the owner allows online actions.

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
