# Existing-only sync attachment

`database.openExistingExperimentalSync(options, callOptions)` attaches a validated
existing journal through native `open_existing_sync_client`. Unlike the explicit
creation API, this path does not define schema, initialize an absent journal,
migrate legacy storage, repair projections or clear a scope barrier while opening.
Missing, corrupt, mismatched or inconsistent cached state refuses. The existing
`openExperimentalSync` creation behavior is retained.

Native attachment holds the existing runtime and physical database lifecycle
locks. It inspects durable state in one cancelled read transaction before reuse,
even when a sibling wrapper still owns a cached runtime. Exact partition/client,
requested scope and subscription revision must agree. A cached runtime must also
agree with durable state and barrier; drift requires recovery. A newly attached
runtime retains the existing barrier. Cancellation uses the existing detached
owner worker and lifecycle drain; namespace/close intent must still match before
publication. No authorization, application owner marker or repair right follows
from obtaining a handle.

A ready application should first validate physical ownership, the complete
storage footprint, its initialized exact-owner marker and durable fence under
its existing lease. It can then attach without allowing a missing journal to be
recreated, recheck the current lease, and publish guarded read operations. This
API does not replace that application admission or make an incomplete local cache
an authorized empty result. Native counts and full snapshots retain their own
bounds and performance costs.

The TypeScript facade requires the new native factory. An older ABI rejects with
a fixed matching-binaries error; it never falls back to initialization. Generated
bindings must be regenerated with the repository tools and all affected iOS and
Android libraries rebuilt before app activation. Do not hand-edit generated code.

## Verification status

Host-verified source foundation, not a shipped mobile/device result. All nine new
native cases passed, including cached barrier drift, cancelled attachment and
superseded session intent. The full Rust workspace passed222 tests with3 ordinary
ignored gates; the two owned persistent ownership/inspection gates were then
executed successfully on fresh preserved fixtures (one unrelated WebSocket gate
remains unexecuted). Full TypeScript passed137 tests, type checking and all five
React Native host typechecks passed; Rust fmt and warning-denied workspace/all-
target Clippy passed. The three facade cases and existing inspection controls
also passed separately (14 tests).

Logs under `/private/tmp/haus-sdk-existing-`: `rust-focused-3.log`,
`rust-workspace.log`, `owned-persistent.log`, `ts-focused.log`, `ts-full.log`,
`ts-types.log`, `rn-matrix.log`, and `rust-clippy.log`. Initial compilation failed
only because new tests treated native close() as returning a Result; the next
run found one corruption fixture targeting a nonexistent schema field. Corrected
fixtures now pass; failed logs remain preserved. No runtime failure was hidden.

Generated bindings were regenerated through ubrn and matching iOS device/simulator-arm64 and Android arm64/x86_64 release libraries built successfully. Both platform/device acceptance and product activation remain pending. No generated code was hand-edited; journal formats and dependencies are unchanged. Private artifact version is 0.1.0-alpha.5.haus.20261020.1. Generated-package build, types, all137 TypeScript tests and package inventory verification passed. Build logs: /private/tmp/haus-sdk-existing-{ios,android}-build.log; package gate: /private/tmp/haus-sdk-existing-generated-release-check.log. No public package publication is implied.
