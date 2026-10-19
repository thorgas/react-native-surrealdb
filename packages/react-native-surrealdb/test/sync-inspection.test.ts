import { describe, expect, it, vi } from "vitest";
import type { SurrealDatabaseLike } from "../src/generated/surrealdb_rn_core";

const bindings = vi.hoisted(() => ({
  inspectSyncState: vi.fn(),
  openSyncClient: vi.fn(),
}));
vi.mock("../src/native", () => bindings);
import { SurrealClient } from "../src/client";

const expected = {
  partitionId: "partition",
  clientId: "account.installation",
  requestedScope: "household-state-v2",
  subscriptionRevision: 1n,
};

describe("closed journal inspection facade", () => {
  it("preserves Missing without opening or initializing a journal", async () => {
    const native = {};
    const database = new SurrealClient(
      native as unknown as SurrealDatabaseLike,
    );
    bindings.inspectSyncState.mockResolvedValueOnce({ tag: "Missing" });
    const signal = new AbortController().signal;
    expect(
      await database.inspectExperimentalSync(expected, { signal }),
    ).toEqual({ kind: "missing" });
    expect(bindings.inspectSyncState).toHaveBeenLastCalledWith(
      native,
      expected,
      { signal },
    );
    expect(bindings.openSyncClient).not.toHaveBeenCalled();
  });

  it("preserves lossless revision, unresolved counts and blocked scope", async () => {
    const database = new SurrealClient({} as SurrealDatabaseLike);
    bindings.inspectSyncState.mockResolvedValueOnce({
      tag: "Present",
      inner: {
        revision: 9007199254740993n,
        pendingCount: 2,
        outcomeCount: 3,
        conflictCount: 1,
        barrier: 2,
      },
    });
    expect(await database.inspectExperimentalSync(expected)).toEqual({
      kind: "present",
      revision: 9007199254740993n,
      pendingCount: 2,
      outcomeCount: 3,
      conflictCount: 1,
      barrier: "pending_quarantine_required",
    });
    const failure = new Error("persistence failure");
    bindings.inspectSyncState.mockRejectedValueOnce(failure);
    await expect(database.inspectExperimentalSync(expected)).rejects.toBe(
      failure,
    );
  });

  it("fails closed when the inspection ABI is unavailable", async () => {
    const inspect = bindings.inspectSyncState;
    Object.assign(bindings, { inspectSyncState: undefined });
    try {
      await expect(
        new SurrealClient({} as SurrealDatabaseLike).inspectExperimentalSync(
          expected,
        ),
      ).rejects.toThrow("matching rebuilt native binaries");
    } finally {
      bindings.inspectSyncState = inspect;
    }
  });
});
