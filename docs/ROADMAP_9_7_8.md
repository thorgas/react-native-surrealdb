# Local runtime roadmap execution

Base: RN candidate `3a1105af2c047d5c3bef3dd6c2792561cef370dd`.
Task branch: `feat/sync-observability-dst-20261016`.

1. Audit and extend existing observation APIs with versioned bounded telemetry,
   redacted support snapshots, exporter lifecycle, transport/scheduler boundaries;
   verify adversarial behavior and commit before simulation.
2. Exercise real runtime transitions and scheduler under deterministic injected
   faults, separate safety from fair recovery/liveness, replay and minimize
   failures, verify negative controls, then commit.
3. Add candidate-bound local campaigns with deterministic sharding, budgets,
   durable resume receipts and strict aggregation; verify interrupted campaigns.

No production changes, public publication, new dependencies or paid campaigns.
Tasks 1–6 hosted CI, production soak and encrypted backup retention remain separate.
Native status reads are not atomic application row snapshots. Client-observed
native/HTTP durations cannot establish server processing or Rust lock timings.

Task 9 verification in progress: TypeScript noEmit passes; full RN suite 60/60
passed with ephemeral loopback permission (sandbox listen EPERM was rerun, not
counted as coverage). Added metadata exporter stress/adversarial coverage; native
and server measurement verification tracked separately before task 9 commit.

Task 9 delivered: versioned React-free telemetry/support snapshot, typed phase and
reason allowlists, bounded commit ordinals, per-exporter FIFO/drop/error lifecycle,
hung/reentrant/throwing sink and unsubscribe isolation; native Rust fixed-size
phase counters/latest operation span and regenerated host bindings; server spans
in private engine commit d5f914f830cdde69d0792130025f41b97d4b17ec.

Verified: RN TypeScript noEmit, metadata stress/adversarial tests and full RN suite;
Rust native core 49 passed/1 explicitly ignored, host build and warning-denied
clippy. Server authority20 tests, listener35 default tests/13 explicit live fixture
ignores, format/clippy. Full private checker runs separately with pinned tools;
no ignored test is claimed executed. Matching mobile binaries need a rebuild;
no device, hosted, deployment or production acceptance is inferred.

Next input for task 7: preserve real Rust ClientRuntime prepare/install plus the
existing TypeScript scheduler; drive storage and transport seams rather than
implementing a second client. Keep healthy-core liveness separate from fault
exploration and retain permanently disconnected peers. Record both repository
SHAs for any mixed runtime/scheduler acceptance.

Task7 found stale reset rollback through real generated transitions (seed2).
The existing Rust client copy now mirrors the canonical narrow rejection of older
epoch/sequence/authorization resets; its transition regression passes. This is a
local source fix: mobile binaries and production have not been updated.

Task7 canonical simulator/fix commit: b61abc48ccadd7828c6d90693f10500f3eec9251.
Full private checker passed: Python26, Quint57, TLC235190 states, Rust135/18
explicit live adapter ignores; separate JS runtime suite6, bridge6, native49/1
explicit authenticated-server ignore. Exact RN transition copy17 tests passed.
