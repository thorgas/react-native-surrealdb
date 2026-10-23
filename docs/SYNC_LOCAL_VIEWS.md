# Private bounded local views v 1

`ExperimentalSyncClient.readView(request)` is an additive native local read API.
It requires matching generated bindings and rebuilt Android/iOS binaries and a
new app runtime fingerprint. It does not change persisted storage, perform a
migration, fetch a server page, or enable a product data provider. The existing
snapshot, replay and ordered-subscription APIs keep their contracts.

```ts
const count = await sync.readView({
  protocolVersion: 1, operation: "count",
  selector: { kind: "eq", path: ["category"], value: "example" },
});
const page = await sync.readView({
  protocolVersion: 1, operation: "page",
  selector: { kind: "all" }, projection: [["name"], ["details", "label"]],
  limit: 32,
});
```

Counts are **observed local matching canonical records in the effective view**,
including accepted-but-unpulled and pending optimistic overlays. They do not
prove queue idleness, migration eligibility or a confirmed-only total. The response always
includes the same local metadata/coverage/readiness as snapshots. Unknown
coverage never establishes an authoritative total or absence; even authorized
coverage is an installed as-of frontier, not current backend freshness. Generic
structural equality does not validate domain records. An application must prove
its own domain contract before displaying this as a product count.

The selector is a closed version 1 AST: `all`, `eq`, or `and` of 1–8 `eq` terms.
Equality accepts only strings (at most 1024 UTF-8 bytes), booleans and null.
Numeric equality, SQL, functions, arbitrary code, recursive conjunctions and
locale comparisons are unsupported. Paths contain 1–8 object keys, each 1–128
UTF-8 bytes. Array indexing, control characters, `__proto__`, `prototype`,
`constructor` and reserved `$surreal` are refused. Missing differs from null.
Tagged lossless values are canonical atoms: selecting an entire integer/bytes/record
field is allowed, but descending into its wire-tag `value` or `base 64` fields
does not match. Ordinary domain object keys named `value` remain traversable.

Pages use canonical record-ID order only (Rust string/UTF-8 lexical ordering).
This does **not** replace an application's existing global title/date ordering.
No full collection is sent to JavaScript for sorting. A page has 1–64 rows and
1–16 unique field paths. Each row carries explicit path/value pairs with a
`present` flag, plus pending identities, conflict resolutions and acceptance
provenance. The effective view preserves confirmed records followed by existing
accepted-but-unpulled and outbox overlays; optimistic deletes do not match.
Payloads use the existing tagged lossless value bridge. Structural keys are not
interpreted as value tags.

A native runtime lock and the existing durable scope barrier precede evaluation.
Closed, revoked/quarantined and recovery-required handles refuse reads. The
facade also withdraws late completions after close, cancellation or observed
scope rejection. This is application-authenticated local cache access, not a
new authorization grant or proof of current online membership.

A continuation cursor is a native-issued opaque JSON capability bound to its
process/cell epoch, exact local revision, full scope, selector/projection/limit
and last record ID. At most 64 issued cursors and 256KiB aggregate are retained.
Rotation/recovery invalidates them. Eviction, changed revision/scope/process or
unissued/tampered cursors return `stale_cursor` with metadata and **no rows or
snapshot fallback**. A valid cursor supplied to a different query is refused.
Restart an explicit bounded page request without its cursor. Cursors do not
own durable state, survive restart, or authorize retention/migration.

Work is bounded before cloning full payloads: input 16KiB; output 256KiB; combined
record/history-operation visits 16,384; value nodes 65,536; visited value/key bytes
4MiB; nesting 64. Historical and pending operations are charged even when they
do not contribute to the effective view. Selector/path and projection visits
are charged as well. A request exceeding bounds fails with `ReadOutputTooLarge`
without a partial count or large reset snapshot. Large retained histories may
therefore require a later indexed/native-storage design. This API is a bounded
native scan, not an indexed database query or a background materialized index.

`work` reports record/operation visits, value-node visits and visited bytes.
These describe native evaluation, not wall-clock latency, server work or CPU
instructions. Actual UTF-8 response length can be measured separately. Tests
compare a 1000-row/1MiB source fixture with a two-field/two-row response below 4KiB.
Host equivalence, malformed/budget, cursor and facade tests do not prove Android
performance, mobile ABI compatibility, provider activation or release readiness.
