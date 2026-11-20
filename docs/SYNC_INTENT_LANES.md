# Experimental durable intent lanes

This source contract requires matching generated UniFFI bindings and rebuilt
native binaries. Existing packaged binaries do not advertise it. The default
HTTP pull remains protocol v1; lane use requires the explicit source-proof
transport profile and native capability schema 2 with storage format 5.

An app saves an enrolled record by capturing `observeRecord(recordId)` with its
editor draft, then calling `enqueueIntent(intent, observationJson)`. The intent
ID is caller generated and stable across retries. The engine persists the
intent before the call resolves. The app does not first write SQL or maintain a
second queue. `materializeNextIntentWithCommitId(recordId, stableCommitId)`
turns only the ready head into a CAS wire commit; native code computes its
canonical fingerprint. A later edit stays in the durable lane suffix. The
commit ID is caller generated and stable across retries.

`readIntentRecord(recordId)` returns one revision of the engine's optimistic
read projection and durable lane metadata under one native runtime lock:

```ts
{
  record: SyncReadRecord,
  lane: {
    pendingIntentIds: string[],
    blockedReason: string | null,
    blockedSnapshotJson: string | null,
    headIdentityJson: string | null,
    resolutionRevision: string,
  },
}
```

`record.metadata.revision` and `lane.resolutionRevision` are exact decimal
strings. The `record.row?.optimistic` value reflects unmaterialized intents and
suffix edits. An optimistic value of `null` with pending intent IDs can mean a
local delete; use `record.absence` and confirmed coverage separately for
server absence. `record.row?.pending` is the existing wire-outbox list and does
not include unmaterialized IDs; use `lane.pendingIntentIds` for the full local
lane. `blockedSnapshotJson` and `headIdentityJson` are opaque protocol JSON.
Preserve the conflict snapshot unchanged when presenting an editor.

For an explicit conflict choice, first call
`projectLaneConflictSnapshot(blockedSnapshotJson)` to obtain the local,
remote, and observed domain values for an editor. Keep the original snapshot
JSON opaque. Its protocol versions, identities, and covered prefix must never
be parsed as JavaScript `Number` values.

Call `prepareLaneResolution(blockedSnapshotJson, choice)`, where `choice` is
`{ strategy: "keep_server" }`, `{ strategy: "keep_local" }`, or
`{ strategy: "merge", value, reference? }`. Native code creates a stable
resolution ID and, when required, a replacement commit identity with a
canonical fingerprint. It returns `{ resolutionId, requestJson }`. Persist
both strings together with the chosen decision and the submitted domain draft
in the app's own draft transaction **before** calling
`submitPreparedLaneResolution(resolutionId, blockedSnapshotJson, requestJson)`.
On retry or process restart, submit the exact saved request JSON. The engine
rejects the same resolution ID with a changed snapshot, decision, replacement
identity, or replacement value. The app must not build a protocol request or
replacement identity from decoded JSON.

`inspectLaneResolution(blockedSnapshotJson, attempt)` accepts the saved
`{ resolutionId, requestJson }` or `null` when no attempt was persisted. The
native lookup validates the exact saved snapshot and request against a retained
receipt and returns `{ state, receiptJson }`. `state` is `absent`, `unknown`,
`keep_server_applied`, `awaiting_acceptance`, `awaiting_pull`, `reconciled`, or
`needs_review`. `absent` means a supported exact lookup found no receipt
while the original blocked snapshot is still current; after that snapshot is
gone, missing evidence is `unknown`. Unsupported native bindings or source
transport fail with an error, never `absent`. `receiptJson` is opaque protocol
JSON; the state is the validated phase projection. A present receipt retains
the original snapshot, decision, replacement identity, accepted and pulled
proofs, status, and acknowledgment flag. `acknowledgeLaneResolutionReceipt`
marks it read without deleting evidence. Accepted ACK alone is not pull
completion.

The transport opt-in sends a v1 CBOR request to `/v1/sync/pull/source-v2` with
`Content-Type: application/vnd.surrealdb-sync.v1+cbor` and exact
`Accept: application/vnd.surrealdb-sync.pull-source-v2+cbor`. A response with
another content type is rejected before applying a pull. Legacy v1 fallback
never enables lane source-proof capability or invents an authoritative base.

Normalized journal format 5 stores engine receipts with intents, lanes and
last reset cursor. Existing formats 2, 3 and 4 are read without mutation; the
next successful CAS save atomically rewrites every component and metadata row.
Missing format 5 receipts or mixed format 5/older component rows are treated as
corruption. Unknown future formats fail closed.
