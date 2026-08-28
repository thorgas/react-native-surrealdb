# Sync runtime copy handoff

Updated: 2026-08-28

## Branch goal

Stage temporary copies of the client-side Rust protocol/runtime crates so a later branch can build a
SurrealKV and UniFFI prototype. This branch intentionally exposes no React Native sync API and is
not usable as a synchronization engine.

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

## Verification

From the repository root:

```bash
cargo fmt --all --check
cargo clippy -p surrealdb-sync-protocol -p surrealdb-sync-client --all-targets -- -D warnings
cargo test -p surrealdb-sync-protocol -p surrealdb-sync-client
./scripts/verify-core.sh
```

## Next implementation slice

1. In `crates/surrealdb-rn-core`, persist one serialized `DurableClientState` replacement inside the
   existing async SurrealDB transaction and prove failure leaves prior state visible.
2. Add forced close/reopen tests on SurrealKV before exposing an API.
3. Add a narrow UniFFI facade only after persistence passes: `open`, `enqueue`, `pending`,
   `record_outcome`, `apply_pull`, `conflicts`, and `status`.
4. Keep network transport and `DEFINE API` authority deployment in separate reviewed slices.

## Hard blockers

- The long-term single-source/export mechanism is unresolved.
- GitHub Actions may remain unavailable until account billing permits runner allocation.
- This branch must remain a draft and must not be released or advertised as sync support.
