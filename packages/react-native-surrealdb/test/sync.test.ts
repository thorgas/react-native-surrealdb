import { describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import { ExperimentalSyncClient } from "../src/sync";

const initialStatus: NativeSyncStatus = {
  revision: 1n,
  pendingCount: 1,
  outcomeCount: 0,
  conflictCount: 0,
};

function createNative() {
  let closed = false;
  const native = {
    applyPullResponse: vi.fn(async () => initialStatus),
    close: vi.fn(async () => {
      closed = true;
    }),
    conflictsJson: vi.fn(async () => ['{"status":"conflict"}']),
    enqueue: vi.fn(async () => initialStatus),
    isClosed: vi.fn(() => closed),
    pendingJson: vi.fn(async () => ['{"identity":"commit-1"}']),
    recordPushResponse: vi.fn(async () => initialStatus),
    status: vi.fn(async () => initialStatus),
  } as NativeSyncClientLike;
  return native;
}

describe("ExperimentalSyncClient", () => {
  it("serializes protocol inputs and decodes durable queues", async () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    const signal = new AbortController().signal;
    const commit = {
      identity: { clientCommitId: "commit-1", fingerprint: "fingerprint-1" },
      operations: [],
    } as const;

    await expect(client.enqueue(commit, { signal })).resolves.toBe(
      initialStatus,
    );
    expect(native.enqueue).toHaveBeenCalledWith(JSON.stringify(commit), {
      signal,
    });
    await expect(client.pending()).resolves.toEqual([{ identity: "commit-1" }]);
    await expect(client.conflicts()).resolves.toEqual([{ status: "conflict" }]);
  });

  it("forwards cancellation and reflects native closure", async () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    const signal = new AbortController().signal;

    expect(client.isClosed).toBe(false);
    await client.close({ signal });
    await client.close({ signal });

    expect(native.close).toHaveBeenNthCalledWith(1, { signal });
    expect(native.close).toHaveBeenNthCalledWith(2, { signal });
    expect(client.isClosed).toBe(true);
  });
});
