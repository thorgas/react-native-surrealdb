# Private engine-fed community adapters

This standalone experiment tests Legend State `3.0.0-beta.48` and TanStack DB
`0.15.0` against the installed read protocol v1. It is private, excluded from the
root pnpm workspace and SDK exports, and is not a published adapter. Core and
Hauswirtschaft manifests and native binaries do not depend on these libraries.
The exact peer versions describe the tested compatibility surface; broader
compatibility has not been established. Select only the adapter module you use.

From this directory, with the repository's Node 22 and pnpm 11.5.0:

```sh
pnpm install --ignore-workspace --ignore-scripts --config.auto-install-peers=false --frozen-lockfile
pnpm test
pnpm typecheck
pnpm format
```

The explicit install flags prevent modifying the surrounding workspace, running
dependency lifecycle scripts, or silently installing peer integrations. Legend's
unused React integration has a transitive React peer warning; these imperative
adapters do not import React, Legend sync/persistence, or Expo SQLite. Adding
those integrations requires separate validation. The test tools match existing
repository versions. This experiment owns its lockfile and dependency directory.

## Ownership and lifetime

Pass the **same** engine-owned `ExperimentalSyncReadStore` to both projections.
It owns one native-read pump. Each adapter owns only its subscription and reactive
view; closing it does not close the engine or shared read store. The application
must close that read store and engine when their scope ends. Neither adapter
owns a database, outbox, network transport, retry loop, checkpoint, or recovery.

```ts
const source = sync.createReadStore();
const commands = new ExperimentalSyncApplicationAdapter({
  sync,
  encodeCommand,
});
const legend = createLegendProjection({ source, commands });
const tanstack = createTanStackProjection({
  source,
  commands,
  id: "household-view",
});

const stop = observe(() => render(legend.view.get()));
const receipt = await legend.commit(command); // Actual engine command receipt.
// A successful command does not manufacture a library update. Engine events do.

stop();
legend.close();
await tanstack.close();
source.close();
await sync.close();
```

The Legend view exposes tracking `get`, untracked `peek`, and `onChange` reads.
Its private observable publishes an opaque frame in one batch, preserving native
payload values and preventing accidental observable writes to the view. Read
`frame.snapshot.rows` and metadata together; this coarse projection deliberately
does not claim row-level render optimization.

TanStack's eager collection contains two entry kinds:

| Entry      | Key                      | Meaning                                                      |
| ---------- | ------------------------ | ------------------------------------------------------------ |
| `metadata` | `metadata`               | Frame phase and installed snapshot metadata                  |
| `record`   | `record:` plus record ID | Full engine row, including pending/conflict/receipt metadata |

One sync transaction updates the metadata entry and every changed record. A fresh
snapshot atomically replaces membership. The metadata entry also represents an
empty collection, so it cannot be confused with authoritative emptiness before
coverage. Queries should select `kind === "record"` for rows and read metadata
from the **same collection revision**. The metadata key and prefixed record keys
cannot collide, even for a domain record named `metadata` or `__proto__`.

TanStack's `$synced` and `$hasPendingWrites` describe its local provider, **not**
the engine's durable intent or authority acknowledgement. Use `entry.row.pending`,
`conflicts`, `accepted`, and the snapshot metadata for those facts. Tombstones,
optimistic deletes, and outcome-only rows remain explicit; consumers must not
render them as ordinary present values. An accepted receipt with unknown epoch
does not establish current visibility. Native bigint payloads and decimal-string
counters remain lossless.

Use the explicit `commit` port for mutations. TanStack insert/update/delete
handlers and optimistic actions are not configured; ordinary collection mutation
methods reject rather than creating a competing queue. Do not add library
persistence or mutation handlers to this engine-owned view.

## Coverage, errors and boundaries

`loading` means no snapshot yet. `local` means cached data is available with
unknown authorized coverage. Only an authorized-reset snapshot makes the
projection `ready`, and this means **as of its frontier**, not current network
freshness. TanStack readiness is withheld for initial unknown coverage; a later
downgrade leaves an explicit error until authorized coverage returns.

An ordinary read error retains the last dated view. Scope rejection withdraws all
rows, latches the projection blocked, cancels its subscription and refuses new
commands. Closing also withdraws the view and ignores delayed callbacks. Neither
operation purges durable data. Recovery and authenticated scope activation belong
to the application and engine, not these adapters.

The shared fixtures use both real libraries and one real SDK read store. They
cover complete multirow publication, reset replacement, readiness, bigint and
reserved-key values, pending/conflict/tombstone/receipt metadata, failures,
withdrawal, command forwarding, cancellation and sharing one native-read pump.
They prove local reactive integration, not mobile execution of these community
libraries, remote predicate/window replication, app upgrades or production
acceptance. TanStack queries operate on already-replicated local rows; they do
not create server subscriptions.
