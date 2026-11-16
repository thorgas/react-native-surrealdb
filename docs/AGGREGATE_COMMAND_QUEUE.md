# Atomic aggregate successors on embedded SurrealDB 3.2.4

The private alpha.11 wrapper keeps `surrealdb = "=3.2.4"` and the existing
UniFFI ABI and sync wire protocol. It adds the optional read capability
`commandQueueProfile: "atomic-aggregate-successors-v1"`. Older binaries omit
this property; callers must check it before admitting aggregates through
`enqueueLatestBounded`. An OTA cannot add this native behavior.

Previously the latest-value queue fingerprinted and deferred only a single
upsert. A multi-record meal command fell through to ordinary admission without
native fingerprinting, and later meal edits could transmit stale CAS versions.
The queue now fingerprints every commit and defers a whole overlapping aggregate.
It never coalesces aggregates or changes transmitted bytes. Only unsent single
upserts retain their existing coalescing behavior.

Promotion requires exact acceptance-receipt and verified-pull coverage for every
overlapping predecessor and preserves FIFO for overlapping commands. Each update
and delete is rebased to its actual confirmed version in one transition. New
aggregate records keep their explicit absence precondition. Missing/deleted
update targets, conflicts and rejections retain the whole command for recovery;
they never cause resurrection, partial admission or automatic retry loops.
The queue retains its 64 deferred-command limit and 16 successors per record;
native caller-selected count/byte limits bound outbox plus deferred commands.

The durable state schema is unchanged. New readers accept prior single-upsert
queues. Old readers reject an aggregate in the deferred queue rather than
silently discarding it. ID mappings rewrite every deferred target and reference;
promotion recomputes the canonical fingerprint after all versions are rebased.
Recovery validates the canonical payload profile of deferred intent, including
data that has not yet been sent.

Engine `command_queue.rs` is copied from the canonical engine aggregate patch.
Only the matching deferred-ID-remapping addition is applied to the SDK's
`lib.rs`: its prior automatic conflict-policy implementation is preserved.
The canonical engine's unchanged `acceptance_receipts.allSafety` refinement
provides the per-record receipt/FIFO gate; `sync_protocol.allSafety` retains
atomic visibility, immutable identities and recoverable intent. Rust tests cover
their multi-record composition. No formal property is weakened.

From the SDK root, use the pinned toolchains in `README.md`:

```sh
pnpm install --frozen-lockfile
./scripts/verify-core.sh
pnpm --filter react-native-surrealdb release:artifacts
pnpm --filter react-native-surrealdb release:check
pnpm --filter react-native-surrealdb release:pack /absolute/private/output
```

Native regression `aggregate_native_fingerprints_and_offline_successors_persist_across_physical_reopens`
performs add → move → delete with both ACK/pull orderings and real SurrealKV
closes/reopens after each admission and response. It checks no early promotion,
atomic parent/child version changes and canonical fingerprints. Capacity refusal
is checked against unchanged memory and reopened durable state. Package tests
distinguish old, supported and malformed capability descriptors. Device proof
and release receipts are separate gates in the consuming application's QA lane.
