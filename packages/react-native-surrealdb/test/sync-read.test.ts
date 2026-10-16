import { encodeSurrealValue } from "../src/wire";
import {
  decodeSyncReadBootstrap,
  decodeSyncReadChanges,
} from "../src/sync-read-codec";
import { describe, expect, it, vi } from "vitest";
import {
  ExperimentalSyncReadStore,
  SyncReadScopeError,
  applySyncReadBatch,
  readSyncRecord,
  type SyncReadBatch,
  type SyncReadRow,
  type SyncReadSnapshot,
  type SyncReadEvent,
  type SyncReadBootstrap,
  type SyncReadChanges,
} from "../src/sync-read";
function snapshot(): SyncReadSnapshot {
  return {
    protocolVersion: 1,
    metadata: {
      revision: "9007199254740993",
      scope: {
        partitionId: "household",
        scopeIdentity: "member",
        subscriptionRevision: "1",
        authorizationRevision: null,
      },
      asOf: null,
      readiness: "local_available_awaiting_coverage",
      coverage: { kind: "unknown" },
    },
    rows: {},
  };
}
function row(recordId: string): SyncReadRow {
  return {
    recordId,
    confirmed: { state: "absent" },
    confirmedAbsence: "unknown",
    optimistic: {
      state: "present",
      value: { name: recordId },
      baseVersion: "absent",
      reference: null,
    },
    pending: [{ clientCommitId: "commit", fingerprint: "fingerprint" }],
    conflicts: [],
    accepted: [],
  };
}
function batch(initial: SyncReadSnapshot): SyncReadBatch {
  const revision = (BigInt(initial.metadata.revision) + 1n).toString();
  return {
    protocolVersion: 1,
    beforeRevision: initial.metadata.revision,
    revision,
    metadata: { ...initial.metadata, revision },
    changes: [
      { kind: "upsert", row: row("recipe:a") },
      { kind: "upsert", row: row("recipe:b") },
    ],
  };
}
describe("committed sync read projection", () => {
  it("applies a whole commit without rounding revisions or mutating the prior view", () => {
    const initial = snapshot();
    const next = applySyncReadBatch(initial, batch(initial));
    expect(next.metadata.revision).toBe("9007199254740994");
    expect(Object.keys(next.rows)).toEqual(["recipe:a", "recipe:b"]);
    expect(initial.rows).toEqual({});
    expect(initial.metadata.revision).toBe("9007199254740993");
  });
  it("rejects gaps, replay and scope mismatch before exposing any rows", () => {
    const initial = snapshot(),
      change = batch(initial);
    expect(() =>
      applySyncReadBatch(initial, { ...change, beforeRevision: "3" }),
    ).toThrow("fresh consistent snapshot");
    expect(() =>
      applySyncReadBatch(applySyncReadBatch(initial, change), change),
    ).toThrow("fresh consistent snapshot");
    expect(() =>
      applySyncReadBatch(initial, {
        ...change,
        metadata: {
          ...change.metadata,
          scope: { ...change.metadata.scope, scopeIdentity: "other-member" },
        },
      }),
    ).toThrow("fresh consistent snapshot");
    expect(initial.rows).toEqual({});
  });
  it("keeps absent cached rows unknown until authorized coverage exists", () => {
    const initial = snapshot();
    expect(readSyncRecord(initial, "missing").absence).toBe("unknown");
    const ready: SyncReadSnapshot = {
      ...initial,
      metadata: {
        ...initial.metadata,
        readiness: "authorized_scope_as_of",
        coverage: {
          kind: "authorized_reset",
          cursor: { epoch: "1", sequence: "0" },
        },
      },
    };
    expect(readSyncRecord(ready, "missing").absence).toBe(
      "absent_within_authorized_scope",
    );
    expect(readSyncRecord(initial, "toString").row).toBeUndefined();
  });
  it("handles hostile record keys without changing the projection prototype", () => {
    const initial = snapshot(),
      change = batch(initial);
    const next = applySyncReadBatch(initial, {
      ...change,
      changes: [{ kind: "upsert", row: row("__proto__") }],
    });
    expect(Object.getPrototypeOf(next.rows)).toBe(Object.prototype);
    expect(Object.hasOwn(next.rows, "__proto__")).toBe(true);
    expect(readSyncRecord(next, "__proto__").row?.recordId).toBe("__proto__");
  });
  it("rejects duplicate row changes without mutating the previous view", () => {
    const initial = snapshot(),
      change = batch(initial);
    expect(() =>
      applySyncReadBatch(initial, {
        ...change,
        changes: [
          { kind: "upsert", row: row("a") },
          { kind: "remove", recordId: "a" },
        ],
      }),
    ).toThrow("repeats a record");
    expect(initial.rows).toEqual({});
  });
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function bootstrap(initial = snapshot()): SyncReadBootstrap {
  return {
    snapshot: initial,
    cursor: { processEpoch: "handle-1", revision: initial.metadata.revision },
  };
}
describe("one engine read pump", () => {
  it("delivers a commit racing initial subscription as one complete batch", async () => {
    const initial = bootstrap(),
      change = batch(initial.snapshot);
    const pending = deferred<SyncReadChanges>();
    const readChanges = vi
      .fn()
      .mockResolvedValueOnce({
        kind: "batches",
        batches: [change],
        cursor: { ...initial.cursor, revision: change.revision },
      })
      .mockImplementation(() => pending.promise);
    const store = new ExperimentalSyncReadStore({
      readSnapshot: async () => initial,
      readChanges,
    });
    const events: SyncReadEvent[] = [];
    const stop = store.watchCollection((event) => events.push(event));
    await vi.waitFor(() =>
      expect(events.map((event) => event.kind)).toEqual(["snapshot", "batch"]),
    );
    expect(Object.keys(store.getSnapshot()?.rows ?? {})).toEqual([
      "recipe:a",
      "recipe:b",
    ]);
    expect(readChanges).toHaveBeenCalledTimes(2);
    stop();
    store.close();
    pending.resolve({
      kind: "batches",
      batches: [],
      cursor: { ...initial.cursor, revision: change.revision },
    });
  });
  it("ignores a late initial read after the last observer cancels", async () => {
    const initial = deferred<SyncReadBootstrap>(),
      readChanges = vi.fn();
    const store = new ExperimentalSyncReadStore({
      readSnapshot: () => initial.promise,
      readChanges,
    });
    const observer = vi.fn(),
      stop = store.watchCollection(observer);
    stop();
    initial.resolve(bootstrap());
    await Promise.resolve();
    await Promise.resolve();
    expect(observer).not.toHaveBeenCalled();
    expect(readChanges).not.toHaveBeenCalled();
    expect(store.getSnapshot()).toBeUndefined();
    store.close();
  });
  it("replaces an overflowed view with the explicit fresh snapshot", async () => {
    const initial = bootstrap(),
      change = batch(initial.snapshot);
    const fresh = applySyncReadBatch(initial.snapshot, change);
    const pending = deferred<SyncReadChanges>();
    const readChanges = vi
      .fn()
      .mockResolvedValueOnce({
        kind: "reset_required",
        snapshot: fresh,
        cursor: { processEpoch: "reopened", revision: change.revision },
      })
      .mockImplementation(() => pending.promise);
    const store = new ExperimentalSyncReadStore({
      readSnapshot: async () => initial,
      readChanges,
    });
    const events: SyncReadEvent[] = [];
    store.watchCollection((event) => events.push(event));
    await vi.waitFor(() =>
      expect(events.map((event) => event.kind)).toEqual([
        "snapshot",
        "snapshot",
      ]),
    );
    expect(store.getSnapshot()).toBe(fresh);
    store.close();
    pending.resolve({
      kind: "batches",
      batches: [],
      cursor: { processEpoch: "reopened", revision: change.revision },
    });
  });
  it("isolates observer failures and does not automatically retry a failed read", async () => {
    const initial = bootstrap(),
      readSnapshot = vi.fn(async () => initial);
    const readChanges = vi.fn(async () => {
      throw new Error("native recovery required");
    });
    const store = new ExperimentalSyncReadStore({ readSnapshot, readChanges });
    store.watchCollection(() => {
      throw new Error("observer failed");
    });
    const events: SyncReadEvent[] = [];
    store.watchCollection((event) => events.push(event));
    await vi.waitFor(() =>
      expect(events.map((event) => event.kind)).toEqual(["snapshot", "error"]),
    );
    expect(store.observerErrors).toBe(2);
    expect(readSnapshot).toHaveBeenCalledOnce();
    expect(readChanges).toHaveBeenCalledOnce();
    store.close();
    expect(store.getSnapshot()).toBeUndefined();
  });
});

describe("native read wire validation", () => {
  it("preserves tagged record payload integers while metadata stays decimal strings", () => {
    const initial = bootstrap();
    const entry: SyncReadRow = {
      ...row("recipe:a"),
      optimistic: {
        state: "present",
        value: { large: 9007199254740993n },
        baseVersion: { exact: "9007199254740995" },
        reference: null,
      },
    };
    const decoded = decodeSyncReadBootstrap(
      encodeSurrealValue({
        ...initial,
        snapshot: { ...initial.snapshot, rows: { "recipe:a": entry } },
      }),
    );
    expect(decoded.snapshot.rows["recipe:a"]?.optimistic).toEqual(
      entry.optimistic,
    );
    expect(decoded.snapshot.metadata.revision).toBe("9007199254740993");
  });

  it("keeps decimal counters lossless and rejects rounded numeric metadata", () => {
    const initial = bootstrap();
    expect(decodeSyncReadBootstrap(JSON.stringify(initial))).toEqual(initial);
    const rounded = {
      ...initial,
      snapshot: {
        ...initial.snapshot,
        metadata: {
          ...initial.snapshot.metadata,
          revision: Number(initial.cursor.revision),
        },
      },
    };
    expect(() => decodeSyncReadBootstrap(JSON.stringify(rounded))).toThrow(
      "Invalid sync read protocol",
    );
    expect(() =>
      decodeSyncReadBootstrap(
        JSON.stringify({
          ...initial,
          cursor: { ...initial.cursor, revision: "18446744073709551616" },
        }),
      ),
    ).toThrow("Invalid sync read protocol");
  });
  it("rejects cursor mismatches, conflicting record identities and unsupported versions", () => {
    const initial = bootstrap();
    expect(() =>
      decodeSyncReadChanges(
        JSON.stringify({
          kind: "reset_required",
          ...initial,
          cursor: { ...initial.cursor, revision: "1" },
        }),
      ),
    ).toThrow("Invalid sync read protocol");
    expect(() =>
      decodeSyncReadBootstrap(
        JSON.stringify({
          ...initial,
          cursor: { ...initial.cursor, revision: "1" },
        }),
      ),
    ).toThrow("Invalid sync read protocol");
    expect(() =>
      decodeSyncReadBootstrap(
        JSON.stringify({
          ...initial,
          snapshot: { ...initial.snapshot, rows: { a: row("b") } },
        }),
      ),
    ).toThrow("Invalid sync read protocol");
    expect(() =>
      decodeSyncReadBootstrap(
        JSON.stringify({
          ...initial,
          snapshot: { ...initial.snapshot, protocolVersion: 2 },
        }),
      ),
    ).toThrow("Invalid sync read protocol");
  });
  it("validates nested versions before exposing a committed row", () => {
    const initial = bootstrap(),
      change = batch(initial.snapshot);
    const message = {
      kind: "batches",
      batches: [change],
      cursor: { ...initial.cursor, revision: change.revision },
    };
    expect(decodeSyncReadChanges(JSON.stringify(message))).toEqual(message);
    const malformed = {
      ...change,
      changes: [
        {
          kind: "upsert",
          row: {
            ...row("a"),
            confirmed: {
              state: "present",
              value: "value",
              version: 9007199254740992,
              reference: null,
            },
            confirmedAbsence: "not_absent",
          },
        },
      ],
    };
    expect(() =>
      decodeSyncReadChanges(
        JSON.stringify({ ...message, batches: [malformed] }),
      ),
    ).toThrow("Invalid sync read protocol");
  });
});

describe("adversarial read envelopes", () => {
  it("withdraws a batch before later observers when a callback invalidates its scope", async () => {
    const initial = bootstrap(),
      change = batch(initial.snapshot);
    const store = new ExperimentalSyncReadStore({
      readSnapshot: async () => initial,
      readChanges: async () => ({
        kind: "batches",
        batches: [change],
        cursor: { ...initial.cursor, revision: change.revision },
      }),
    });
    store.watchCollection((event) => {
      if (event.kind === "batch")
        store.invalidateScope(
          new SyncReadScopeError("pending_quarantine_required"),
        );
    });
    const events: SyncReadEvent[] = [];
    store.watchCollection((event) => events.push(event));
    await vi.waitFor(() =>
      expect(events.map((event) => event.kind)).toEqual(["snapshot", "error"]),
    );
    expect(store.getSnapshot()).toBeUndefined();
    store.close();
  });
  it("does not publish any batch from an envelope with an invalid final cursor", async () => {
    const initial = bootstrap(),
      change = batch(initial.snapshot);
    const store = new ExperimentalSyncReadStore({
      readSnapshot: async () => initial,
      readChanges: async () => ({
        kind: "batches",
        batches: [change],
        cursor: {
          ...initial.cursor,
          revision: (BigInt(change.revision) + 1n).toString(),
        },
      }),
    });
    const events: SyncReadEvent[] = [];
    store.watchCollection((event) => events.push(event));
    await vi.waitFor(() =>
      expect(events.map((event) => event.kind)).toEqual(["snapshot", "error"]),
    );
    expect(store.getSnapshot()?.rows).toEqual({});
    expect(store.getSnapshot()?.metadata.revision).toBe(
      initial.cursor.revision,
    );
    store.close();
  });
  it("preflights later batches before exposing earlier rows", async () => {
    const initial = bootstrap(),
      change = batch(initial.snapshot);
    const store = new ExperimentalSyncReadStore({
      readSnapshot: async () => initial,
      readChanges: async () => ({
        kind: "batches",
        batches: [change, change],
        cursor: { ...initial.cursor, revision: change.revision },
      }),
    });
    const events: SyncReadEvent[] = [];
    store.watchCollection((event) => events.push(event));
    await vi.waitFor(() =>
      expect(events.map((event) => event.kind)).toEqual(["snapshot", "error"]),
    );
    expect(store.getSnapshot()?.rows).toEqual({});
    store.close();
  });
  it("does not decode structural row keys as payload tags", () => {
    const initial = bootstrap();
    const decoded = decodeSyncReadBootstrap(
      JSON.stringify({
        ...initial,
        snapshot: { ...initial.snapshot, rows: { $surreal: row("$surreal") } },
      }),
    );
    expect(decoded.snapshot.rows.$surreal?.recordId).toBe("$surreal");
  });
  it("rejects authoritative absence when coverage or confirmed state disagrees", () => {
    const initial = bootstrap();
    for (const entry of [
      { ...row("a"), confirmedAbsence: "absent_within_authorized_scope" },
      { ...row("a"), confirmedAbsence: "not_absent" },
      {
        ...row("a"),
        confirmed: {
          state: "present",
          value: "value",
          version: "1",
          reference: null,
        },
      },
    ]) {
      expect(() =>
        decodeSyncReadBootstrap(
          JSON.stringify({
            ...initial,
            snapshot: { ...initial.snapshot, rows: { a: entry } },
          }),
        ),
      ).toThrow("Invalid sync read protocol");
    }
  });
});
