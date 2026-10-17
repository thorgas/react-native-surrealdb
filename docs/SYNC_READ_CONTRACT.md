# Private sync read protocol v1

This candidate adds a local read contract to the experimental engine. It is not a
published release. The authority still speaks `surrealdb-sync/1`; predicate and
window replication are not supported by this read protocol.

A snapshot and its cursor are captured under the same native runtime mutex.
Changes enter the replay journal only after atomic persistence succeeds and the
prepared state installs. One durable transition yields one complete batch,
including metadata-only changes. Failed writes emit nothing. An unknown commit
invalidates the handle as `RecoveryRequired`; reopen loads durable state before
reading again.

```ts
const capabilities = sync.readCapabilities();
if (await sync.requiresAuthorizedReset()) {
  await transport.bootstrapReadCoverage();
}
const store = sync.createReadStore();
const stop = store.watchCollection((event) => {
  if (event.kind === "snapshot") replaceView(event.snapshot);
  if (event.kind === "batch") replaceView(event.snapshot);
  if (event.kind === "error" && event.invalidated) withdrawView();
});
// Later: stop(); store.close();
```

`bootstrapReadCoverage()` performs an authenticated pull without a checkpoint.
It does not clear, dispatch or replace pending intent before the authority
response. A scope-boundary response may require quarantine or a new journal.
Applications must finish authentication and select the account, environment and
household before opening or displaying a journal. A crash before a quarantine
barrier reaches durable storage cannot prove that revocation was recorded.
Reopening must therefore retain application authentication/bootstrap gating.

`readSnapshot()` returns `{snapshot,cursor}`. `readChanges(cursor,maxBatches)`
returns contiguous `batches` or an explicit `reset_required` with a fresh snapshot.
The stateless cursor contains a UUID per native handle and a local revision.
Reopen, another handle, an unavailable revision or replay overflow requires a fresh
snapshot. Malformed cursor input is rejected. The journal holds at most 128 batches
and 4 MiB of encoded data; native output is capped at 16 MiB. Capability metadata reports the exact limits. There is
no silent drop or unbounded subscriber queue.

The JavaScript store uses one cancellable native-read pump for all its observers.
It preflights an entire response before publishing any of its batches, then
publishes one complete revision at a time. Removing the last observer stops the
pump. It does not close the engine, own a durable queue, retry mutations, or run
network synchronization. Observer exceptions are isolated and counted through
`observerErrors`. Ordinary read errors retain the last explicitly dated view;
`refresh()` is an explicit retry of local reads. Scope errors invalidate that
view and cannot be refreshed through the blocked store. Closing the client closes
stores created by `client.createReadStore()`.

Read metadata distinguishes local data availability from authorized scope
coverage. Old blob and normalized journals default to unknown coverage. Only a
complete authorized reset establishes durable coverage, bound to its epoch,
scope and authorization/subscription revisions. `asOf` identifies the installed
frontier; it is not a claim of current backend freshness. No opaque checkpoint or
authentication token is exposed. Missing cached rows remain unknown until this
coverage exists; tombstones and local pending deletes remain explicit row states.

Rows carry confirmed state, optimistic state, pending commit identities,
conflict identities/resolutions, and accepted receipt metadata. An original v1
acceptance receipt has no acceptance epoch; its sequence never establishes
current authoritative visibility by itself. A complete reset durably marks older
accepted overlays as covered without rewriting the original receipt. Late
acknowledgements retain explicit unknown epoch provenance.

All u64 metadata, row versions, base versions and cursors cross the JSON boundary
as canonical decimal strings. Record payloads use the existing tagged value
bridge. Structural record keys are not interpreted as value tags. Applications
must validate their own domain values and must not round versions into unsafe
JavaScript numbers.

`scopeTransitionDecision(scope,subscriptionRevision)` is diagnostic: it preserves
the old journal and returns `unchanged`, `needs_new_scope_journal` or
`pending_quarantine_required`. An authorization-boundary reset cannot silently
retarget pending intent or expose retained outcome-only records in a new scope.
Native scope rejection persists a separate identity-keyed quarantine barrier;
ordinary journal saves cannot remove or weaken it. Reopen and another handle
check that barrier. There is no automatic purge or unblock operation. Pending
retention and a new authorized journal's lifecycle remain application decisions.

The new methods and generated checksums require matching rebuilt native binaries.
`readCapabilities()` provides an explicit protocol check after initialization; it
cannot make an older binary pass newer generated bindings' initialization checks.
Build iOS and Android with the same native candidate, use their matching runtime
fingerprints, and reject incompatible updates before enabling this facade.
Host Rust/TypeScript tests do not prove mobile ABI, device acceptance, hosted
synchronization or release readiness.

The private [community adapter experiment](../experiments/community-adapters/README.md)
feeds Legend State and an eager TanStack DB collection from this contract, with
the engine retaining sole ownership of durability and mutations. It has its own
optional peers and lockfile; neither library is loaded by this SDK's exports.
