# SurrealDB 4.0.1 upgrade + RN 0.87/0.88 hosts — handoff

Status: IN REVIEW as PR #22 (pushed). Merge with rebase after the device items below.

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

## Later commits

| SHA | Slice |
| --- | --- |
| `5ab3236`, `b63b150` | CI: pin JDK 17 for the Android size jobs |
| `a9a2c30` | `migrate-from-3x` opt-in (`scripts/with-migrate-from-3x.sh`); saves ~8 MiB native |
| `81d0f59` | Android native size budget 40 MiB |
| `763e169` | NATIVE_SIZE.md: iOS 3.2.4 vs 4.0.1 sizes |
| `3b7ee91` | npm tarball limit 300 MB (superseded by `a3fb1c8`) |
| `a3fb1c8` | Native binaries downloaded at build time from the GitHub release, SHA-256 pinned (`scripts/native-artifacts.mjs`); tarball 0.29 MB |
| `92713f3` | Fix: fetch ran as a no-op when called through the node_modules symlink |

Size finding: SurrealDB 4.0.1 adds ~8.7 MiB per platform, spread over its own crates.
Trimming its hard-coded optional features (lindera, egg, observability) saved only
0.6 MiB, so that patch was dropped.

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

## Device evidence for build-time download (2026-11-25)

- harness-rn86 iOS (sim 8F752138, renamed "SurrealDB4 iPhone 17 Pro" so other agents
  do not grab it): `pod install` fetched and verified the XCFramework; E2E 14/15
  (`sync-read` is the known pre-existing GC failure).
- harness-rn86 Android: Gradle fetched and verified the `.so`; the APK contains it.
  E2E not run: system_server died on both `Pixel_9_PlayStore` (even after wipe) and
  a fresh `SurrealDB4_API36` AVD under host load.
- YouMotion (worktree `~/git/youmotion-surreal4`, local branch
  `chore/surrealdb4-native-download`, package.json points at the packed tgz, not
  committed): typecheck clean, 24 DB Jest tests pass, `pod install` fetched the
  framework, iOS harness DB suites pass (migration runner, check-in repository,
  belief flow, archive/restore, history filter, onboarding). UI `.tsx` suites need
  the `HarnessUI` native module, which a plain debug build does not link.

## Open items

1. Android device E2E (rn86/87/88, migration E2E) on a host with free CPU/RAM.
2. YouMotion Android build and its UI harness suites (needs its E2E build setup).
3. Merge PR #22 with rebase once 1 is green; then cut a release so the native
   archives exist on GitHub Releases before anyone installs the new tarball.

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
