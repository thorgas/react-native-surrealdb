# Persisted Note sync: finite integration map

Updated: 2026-11-21. Source-only map; it does not claim a native-values runtime,
server route, durable app store, or device acceptance.

## Current evidence and boundaries

- Engine native-values design is `df88ce632bf8784b8bed439a19da224ef9936583:
  spec/wire-native-values-profile-draft.md`. It is a proposal. Engine v1 is the
  existing `crates/surrealdb-sync-protocol/src/canonical.rs` and
  `http_codec.rs`; keep those bytes and semantics frozen.
- SDK source at `99eb4ccb540f4aeb7b87391eb7e0a9214145d9e9` has working v1
  sync, source-v2 pull proof, intent lanes, and resolution receipts. Its native
  value modules remain an internal draft; `sync_state.rs` stores
  `DurableClientState<JsonValue>` and persisted formats 1–5. `laneCapabilitiesJson`
  reports schema 2, storage format 5, and `pull-source-v2`; it does not negotiate
  or persist a native-values profile.
- The app `main` baseline read here is `979a1c20a1652d215bb00148cb4df01d4efedcbe`.
  Existing Note schema is `src/core/domain/note.ts`: `id`, `body`,
  `privacyPolicy`, `classificationEnabled`, `classificationCategoryId`. The
  personal-sync contract (`src/core/domain/personal-note-sync.ts` and
  `src/core/ports/personal-note-sync-store.ts`) defaults new Notes to local-only,
  keeps legacy Notes local until explicit adoption, and requires durable intent
  plus optimistic projection in one engine-authoritative operation.
- Dirty app WIP read-only in that checkout includes
  `src/adapters/native/surreal-note-sync.ts` and
  `src/adapters/native/surreal-note-sync-runtime.ts`. It currently selects v1,
  hardcodes `fingerprint: "computed-by-native"`, restricts IDs to simple strings,
  and uses `my-devices-only` AI privacy as sync eligibility. Those are not a
  full-profile implementation; AI privacy and personal-sync consent must remain
  separate. Do not edit or ship that WIP as the integration.
- The remaining SDK invocation TODO is the public/native Set-construction case:
  there is no SDK `SurrealValue` Set constructor/conversion path to invoke. Keep
  that one explicitly blocked; do not export or wire the test codec to
  manufacture a public call site. The opt-in TS↔Rust bridge already tests the
  internal draft boundary and is not activation evidence.
- Personal sync remains separate from household/shopping sync. Shopping's native
  writer and HTTP outbox stay untouched. Shared-chat payloads and model context
  must not receive personal Note bodies absent separately authorized AI access.

## Smallest dependency-ordered implementation slices

### 1. Engine owns an isolated native-values protocol slice

**Next executable engine work:** from the engine draft commit
`df88ce632bf8784b8bed439a19da224ef9936583` (parent
`5f104c16e0d367b686fba6905550b38ef57653f4`), implement only a parallel
`native_values` module in `crates/surrealdb-sync-protocol`, with native value
AST, structural record identity, deterministic CBOR codec, and the already
proposed explicit envelope/fingerprint. Use existing workspace dependencies
`ciborium`, `serde`, and `sha2`; no new crate. Add module-local literal golden
and adversarial tests plus a conformance fixture. Do not add cases to v1
`CanonicalValue`, alter v1 fingerprinting, wire authority routes, or activate an
SDK entry point in this slice.

Acceptance: all native literal bytes decode and re-encode identically; every
value and operation has exact checked-in goldens; unknown tags/family/schema,
trailing/noncanonical CBOR, size/depth/item overflow, malformed datetime,
decimal, UUID, and record keys reject; finite float signed zero and Decimal
scale survive; native Set construction preserves the actually retained member
representation and rejects wire cardinality shrink; integer/string/UUID keys
remain distinct; v1 golden fixtures are unchanged. Use pinned SurrealDB 3.2.4
types for Set semantics. This proves a Rust protocol codec only, not HTTP,
server authentication, SDK binding, or Note sync.

### 2. Server profile binding and causal pull proof

After the engine codec is reviewed, implement authenticated profile negotiation
and a profile-bound server entry point. A binding covers principal, partition,
stable client, exact profile, scope and authorization/subscription revisions,
and partition epoch. For an existing client, local read/enqueue uses only its
persisted binding while offline; a first-time client needs authenticated
enrollment before durable sync enqueue. Every remote push/pull/cache/outcome/
checkpoint/log/record access reauthenticates and checks the binding before state
access. Profile mismatch is a typed fail-closed result.

Native pull carries server-owned source identity in the same transaction as
native values. Reuse source-v2's proof meaning (immutable `sync_log` identity;
null when undisclosable/legacy) but bind it to the native profile. Preserve
existing lane heads, exact source/fingerprint proof, accepted outcomes, and
resolution receipts; never infer source from a request or fall back to v1 or
standalone source-v2 bytes. Keep acceptance-receipt-v2 and native-values
families/routes/media types isolated. Route assignment and router collision
tests are part of this slice, not a path-prefix heuristic.

### 3. SDK profile-aware persistence without domain conversion

Add the next SDK storage format (candidate 6) with an explicit
`canonical_profile_id`, validated together with `state_version` and state CAS.
Format 5→6 is format-only: keep v1 profile, IDs, values, fingerprints, outcomes,
intents, lanes, subscriptions, checkpoints and receipts intact. No pending v1
commit is converted or re-fingerprinted. A future cross-profile migration is a
separate protocol/operations design and remains unauthorized by this map.

Add explicit parallel native-values APIs/capability reporting. Enrolled offline
operations validate against the persisted profile; unbound first use must finish
authenticated enrollment. Reject unknown stored profiles and preflight every
value and full projected state before mutation. Profile-specific CAS and cache
keys include the profile. Tests cover atomic legacy format migration and
rollback, old clients rejected before remote state access, enrolled offline
read/enqueue, first-use enrollment, profile mismatch, and all existing v1
goldens.

### 4. App store and complete Note projection

Implement the app's `PersonalNoteSyncStoreShape` against the SDK's explicit
profile-aware API. One native transaction must revalidate current personal-sync
configuration generation and opaque Note edit observation, then persist intent
and its optimistic full-row projection together. Keep historical/local-only
Notes out until explicit adoption. A receipt means local durability only.
Read/list must include unmaterialized intents, deletes and blocked conflicts;
startup discovery must enumerate lanes, not just projected rows. Preserve the
stable session owner's endpoint, scope and credentials; do not infer a personal
authority endpoint from the Bun/shared-chat origin.

The value mapping must round-trip the complete Note schema, including classifier
fields when present, and any native values in persisted Note content. No
JSON/tagged-map fallback or stringified Datetime/Decimal/UUID is acceptable.
ID mapping must preserve typed table/key identity. This app layer is not
implementable on today's v1 `JsonValue` profile adapter.

### 5. Classifier writes join guarded intent commit

Current native classification commit in
`src/adapters/native/surreal-note-classification-store.ts` atomically checks job
attempt, exact Note body/privacy/classification settings, category snapshot and
catalog fence, then writes Note assignment plus graph provenance and completes
the job. Enrolled classifier completion must preserve those guards and commit
the resulting full Note intent plus optimistic projection in the same
engine-authoritative transaction. A SQL update followed by enqueue is a crash
gap; a callback or app-owned queue is not a substitute. Ineligible or stale
completion must leave both Note and intent unchanged. Keep the existing local
transaction for local-only Notes. Acceptance injects failure between candidate
writes, races body/privacy/AI preference/assignment/catalog/job generation,
reopens the database, and proves no half-committed Note/intent/provenance state.

## Resolved technical contracts vs real decisions

Resolved by existing requirements/design: separate `surrealdb-sync/native-values/1`
family and fingerprint domain; v1 remains byte-frozen; finite float bits,
negative zero, Decimal coefficient/scale, datetime seconds/nanos, UUID bytes,
typed record keys and retained Set representation are preserved; native Set
equality is used only to reject cardinality-changing wire decode; candidate tag
60000 is internal/profile-scoped, not claimed registered; JSON object/tag-like
maps remain collision-free; limits are explicit; unsupported types fail closed;
format 6 does not convert v1 pending data; personal sync is explicit and does
not imply shared-chat or classifier/model-context access. These do not require
another generic review gate.

Still needs an owner/operator decision or action: which authenticated personal
workspace is enrolled and what scope it grants; the production authority,
partition provisioning and credential lifecycle; the explicit UI/setting and
legacy Note adoption flow (no UI work is authorized here); and whether
classification results may replicate when the user has enabled Note sync (the
safe default is to keep classifier writes local until the same guarded native
intent capability is available). These are finite product/operations choices,
not reasons to defer the isolated engine codec.

No native values profile is currently deployed or activated. The complete
persisted Note sync gate remains open until the ordered slices above pass at
their stated boundaries.
