# Durable rejected-command resolution (private alpha.14)

A rejected command used to block every later overlapping command forever.
Alpha.14 allows the application to explicitly close a rejection with the existing
`keep_server` marker. The original rejected identity, outcome, code and intent
remain immutable audit history. It never retries that rejected command.

The storage-neutral client prepares closure and the existing aggregate successor
transition. The native adapter composes them, recomputes canonical fingerprints,
and commits the complete journal plus materialized projection in **one** SurrealDB
transaction before installing memory or publishing v1/v2 read changes. There is
no durable intermediate "closed but successor lost" state. The runtime lock and
revision check serialize pulls and resolutions. Once promoted, the pending
identity is immutable; a later pull cannot rewrite it. Ordinary CAS conflict
handling remains necessary when the authority advances after promotion.

The first eligible overlapping successor is rebased to the latest **confirmed**
authority versions, preserving whole aggregates and FIFO. Other records can
advance independently. Receipt coverage remains mandatory for accepted
predecessors; unresolved conflicts and other rejections still block. No bound
changes: 64 deferred commands globally, 16 per record. Missing/deleted update
targets remain retained for recovery, never resurrected. Subsequent user intent
continues to overlay confirmed state; with no successors the projection shows
only the confirmed authority value. A rejection does not contain a server value:
the app must keep normal authorized pulls running to refresh that authority.

## Upgrade and application API

No journal migration or automatic acknowledgment is needed. Existing
`Rejected` entries with absent `resolution` are immediately listed after opening
an existing journal, and the same explicit API resolves them. Do not clear or
recreate a journal to recover a rejection. Old native builds cannot interpret a
resolved rejection; install a matching native build, and do not downgrade its
journal. There is no wire grammar change; both v1 and v2 responses/reads remain
supported.

```ts
import {
  ExperimentalRejectionCode,
  ExperimentalRejectedOperation,
  type ExperimentalRejectedEntry,
} from "react-native-surrealdb";

// sync is an open ExperimentalSyncClient for the authorized household.
let after: string | undefined;
for (;;) {
  const page: ExperimentalRejectedEntry[] = await sync.rejectedEntries(after, 64);
  if (page.length === 0) break;
  for (const entry of page) {
    // Resolve the operation + recordId to a German feature label in app code.
    // Example for a meal change: "Die Änderung am Essensplan wurde abgelehnt.
    // Der Serverstand wird verwendet. Spätere Änderungen bleiben erhalten."
    // Persist/schedule the notice before closing if it must survive app exit.
    await sync.resolveRejectedKeepServer(entry.commitId);
  }
  after = page[page.length - 1]!.commitId;
}
// Continue the ordinary scheduler so promoted successors are pushed and pulled.
```

`rejectedEntries(afterCommitId?: string, limit = 64, options?: CallOptions)`
returns `Promise<ExperimentalRejectedEntry[]>`. Each entry contains only:

- `commitId: string`
- `rejectionCode: ExperimentalRejectionCode` (`InvalidOperation`,
  `CommitIdentityMismatch`, `Unauthorized`, `UnsupportedSchemaVersion`)
- `records: { recordId: string; operation: ExperimentalRejectedOperation }[]`
  (`Upsert` or `Delete`)

No payload, fingerprint, reference, or authority value crosses this API. Pages
follow durable outcome order; use the last returned commit ID as cursor. Resolved
cursors remain valid; unknown cursors and limits outside integer 1–64 fail.
Restart scanning from the beginning on journal replacement. Do not infer a human
name from record ID alone; app domain context supplies German labels.

`resolveRejectedKeepServer(rejectedCommitId: string, options?: CallOptions)`
returns `Promise<ExperimentalSyncStatus>`. Duplicate resolution is a durable
no-op; retry it after an uncertain result/restart. Unknown IDs and IDs for
non-rejected commits fail. It never creates an HTTP request or bypasses queue
transmission marking. Use at startup for old refusals and after rejected push
responses for new ones. Native methods are `rejected_entries` and
`resolve_rejected_keep_server`. Older native binaries fail closed; an OTA alone
cannot add them. The consuming app owns the notice and scheduler integration.

## Verification

`crates/surrealdb-sync-client/tests/rejected_resolution.rs` covers old journal
shape, duplicate response/resolution, v1/v2 rejection responses, full 64/16 bounds,
FIFO aggregates, successor rejection, prepare failure, invalid markers and both
pull race orderings. Every applied transition compares recovery before persistence,
after persistence/before install, and after install.

Native `sync_client::aggregate_tests::rejection_tests` uses SurrealKV and physical
reopens, checks metadata pagination and v1/v2 reads, races a pull against a
cancelled resolution waiter, and exits a child process without destructors at
six boundaries: before preparation, after closure preparation, after successor
preparation, after commit dispatch, after durable commit/lost ACK, after install.
Recovery must contain either the complete old state or complete new state,
including the projection, with no dropped successors. Existing receipt and
protocol Quint traces plus `allSafety` are run unchanged. These models cover
receipt/FIFO and protocol safety; rejection resolution is covered by the added
implementation crash/race tests, not claimed as a new formal-model action.
