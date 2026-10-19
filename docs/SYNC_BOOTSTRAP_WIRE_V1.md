# Bootstrap/1 canonical wire grammar

Status: codec and streaming verification slice, without server/native activation.
The separately approved canonical staging model remains an abstraction; this document
specifies bytes and hash construction, not a refinement proof. The private model and
ADR are not included in this SDK source export.
Existing `surrealdb-sync/1` and subscription bytes/APIs are unchanged.

## Envelope and primitives

Media type: `application/vnd.surrealdb-sync-bootstrap.v1+cbor`.
Every success message is exactly:

```text
["surrealdb-sync-bootstrap/1", 0, kind, payload]
```

Kinds0..5 below are distinct; unknown kinds/schema versions, trailing fields,
indefinite containers, nonminimal encodings, invalid UTF-8, duplicate/unsorted
canonical value maps, trailing bytes and unsupported tags reject. HTTP
errors/authentication are outside these six success-body messages.

- `id`: nonempty UTF-8 text, at most1024bytes; no normalization is performed.
- `checkpointToken`: nonempty UTF-8 text, at most4096bytes.
- `u64`: exact unsigned integer through18446744073709551615, never a float.
- `digest`: CBOR byte string of exactly32bytes, not textual hexadecimal.
- `cursor = [epoch:u64, sequence:u64]`.
- `scope = [scopeIdentity:id, authorizationRevision:u64, subscriptionRevision:u64]`.
- `context = [authority:id, principalBinding:id, partition:id, client:id,
  journalGeneration:id, scope]`. These claims are data until matched against
  trusted authenticated authority/current journal context.
- `limits = [maxRecords:u64, maxPages:u64, maxContentBytes:u64]`, each positive
  and no greater than16384,16384,67108864 respectively. The exact declared
  values are committed by the manifest digest; smaller local budgets still apply.
- `checkpoint = [checkpointToken, cursor, scope]`, exactly the existing v1 tuple.
- `record = [recordId:id, state]`, using the existing canonical record/value codec:
  `state = [1, canonicalValue, version:u64, reference:id|null]` or
  `[2, version:u64]`. Version must be positive. Explicit Absent state is forbidden.
  Absence follows only from complete verified enumeration, never an individual page.

Canonical values retain the existing profile, including None versus Null, finite
normalized float values, bytes, ordered maps, arrays and supported record-ID tags.
References are syntactically bounded identities; this codec does not prove domain
reference closure, household ownership or authorization.

## Six payloads

```text
0 StartRequest = [partition:id, client:id, requestId:id, requestedScope:id,
                  subscriptionRevision:u64, journalGeneration:id]

1 Manifest = [session:id, startRequestId:id, startFingerprint:digest, context,
              cursor, limits, pageCount:u64, recordCount:u64,
              contentBytes:u64, orderedRoot:digest, expiresAt:u64]

2 PageRequest = [partition:id, client:id, session:id, pageIndex:u64]

3 Page = [session:id, manifestDigest:digest, pageIndex:u64, [record...]]

4 CompleteRequest = [partition:id, client:id, session:id, manifestDigest:digest]

5 Receipt = [session:id, manifestDigest:digest, checkpoint, expiresAt:u64]
```

Expiry uses fixed UTC Unix seconds, matching the existing authority checkpoint
clock convention. The codec preserves full-u64 values; it does not inspect clocks,
renew expiry, authorize a lease or prove checkpoint issuance.

Page indices are below16384. Every page has at least one record and at most1024
records, subject to stricter full-body budgets. Record IDs are strictly increasing
by UTF-8 byte order within and across ordered pages; duplicate or decreasing IDs
reject. Pages are indexed consecutively from0. Empty snapshots have zero pages,
zero records and zero content bytes; they use the fixed empty ordered root below.
Nonempty manifests require `recordCount >= pageCount > 0` and positive content bytes.
Counts and content bytes must not exceed their declared or hard aggregate limits.

## Complete body and aggregate limits

Each complete envelope retains the existing4MiB encoded-body,4096total-item,
1024container-member,32depth and1MiB individual string/byte-string limits.
Header overhead and nested record/state/value depth count. The encoder performs a
borrow-only record/value budget pass before copying canonical payloads; the bounded
writer and complete-body preflight enforce final encoded size/depth/item bounds.
Content-only size is insufficient: a content tuple can fit while its page response
fails. One record that cannot fit a complete page fails explicitly; no record
fragmentation is selected.

The aggregate is separately capped at64MiB of encoded page CONTENT,16384records
and16384pages. Aggregates can exceed the single-body item limit because each
page is independently bounded and verified. Native storage remains a separate
constraint: existing data, staged envelopes/metadata, final replacement state and
component-count/per-component limits must all fit the client's actual budget.
A64MiB content declaration is not permission to consume64MiB of native storage.

## Noncircular hashes

Let `F(x) = uint64_be(byte_length(x)) || x`; every length is exactly8bytes.
Let `W(kind,payload)` be the complete canonical envelope above. Domain labels
below are their exact UTF-8 bytes, without a trailing zero or newline.

```text
startFingerprint = SHA256(
  F("surrealdb-sync-bootstrap/1/start/sha256") || F(W(0,StartRequest)))

content_i = canonicalCBOR([pageIndex, [record...]])
pageHash_i = SHA256(
  F("surrealdb-sync-bootstrap/1/page-content/sha256") || F(content_i))

orderedRoot = SHA256(
  F("surrealdb-sync-bootstrap/1/ordered-root/sha256") ||
  F(uint64_be(0)) || F(pageHash_0) ||
  ... || F(uint64_be(pageCount-1)) || F(pageHash_last))

manifestDigest = SHA256(
  F("surrealdb-sync-bootstrap/1/manifest/sha256") || F(W(1,Manifest)))
```

`contentBytes` sums byte lengths of `content_i`, not page headers, and not the
whole assembled snapshot. Content excludes session/manifest headers. Consequently
page headers can safely carry the manifest digest after the manifest commits the
ordered root: no hash cycle or expected per-page-hash array is introduced.
The start fingerprint binds exact request bytes; server-derived context is bound
separately in the manifest. Hash equality is not authentication by itself.

The empty root is:
`6fe78b05363a986225f66df2bd9ff735711df25aec702aa78cc7bb3e39d24c8b`.

## Streaming API and caller obligations

`BootstrapContentHasher` retains only SHA-256 state, session, counters and the last
record ID. The server may use a placeholder32byte manifest header while building
content/root; the header has fixed size and is excluded from content hashing.
`BootstrapVerifier` checks actual manifest-header matching, sequential indices,
complete wire budgets, strict global ID order and declared aggregate limits.
A failed push changes no hash state, counters or last identity. `finish` checks exact
page/record/content-byte totals and the final ordered root. A forged, structurally
valid, self-consistent page can enter the stream and must fail final root validation;
its declared hash cannot authenticate it against the manifest.

This ordered verifier is not the durable page store. Staging may accept responses
out of order and identical duplicate responses idempotently; it must reject
conflicting same-index replacements, then replay durable pages in index order for
final verification, including after restart. The hasher is not a durable storage
format. No aggregate snapshot is encoded as a single canonical CBOR container.

`validate_bootstrap_manifest_context` matches the start fingerprint, request ID,
requested scope/subscription revision and complete caller-provided expected context.
`validate_bootstrap_receipt` matches session/manifest digest, exact cursor/scope and
fixed expiry. These helpers do not authenticate their arguments or confer authority.
The expected context must be trusted, and membership, retention proof/lease, expiry
clock and exact retry policy remain server/native integration obligations.

Successful codec/root verification grants no install capability, authorized absence,
confirmed records, cursor or read publication. Native must additionally validate the
completion receipt, current generation/authorization, aggregate storage and revision
under its mutation lock, then atomically persist/install using the independently
reviewed bootstrap design. Pending edits and later accepted outcomes are not modeled
or modified by this pure codec.

## Conformance evidence

`tests/bootstrap_codec.rs` fixes all six synthetic message byte vectors plus start,
page-content, ordered-root, empty-root and manifest SHA-256 goldens. They were
independently crosschecked with a small Python canonical-array encoder/SHA-256
fixture. Tests include full-u64 fields, existing canonical values, malformed and
noncanonical CBOR, hostile declared lengths/depth, identity/digest/counter limits,
forged/reordered/duplicate/cross-session pages, failed-push state integrity, and
600records across two bounded pages when a single v1 reset exceeds its item budget.
A focused unit checks the content-versus-full-page boundary at4096encoder items.
Existing v1/subscription vectors remain unchanged.

```sh
CARGO_TARGET_DIR=/path/to/isolated-protocol-target \
  cargo test --offline --locked -p surrealdb-sync-protocol
CARGO_TARGET_DIR=/path/to/isolated-protocol-target \
  cargo clippy --offline --locked -p surrealdb-sync-protocol --all-targets -- -D warnings
```

Wire tests do not establish server transaction atomicity, durable retry/lease
persistence, native crash recovery, ABI compatibility, device acceptance or actual
cold-start improvement. Those remain later implementation gates.
