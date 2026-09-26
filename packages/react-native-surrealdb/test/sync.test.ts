import { describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import { ExperimentalSyncClient } from "../src/sync";
import { NONE, SurrealRecordId } from "../src/wire";

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
    checkpointToken: vi.fn(async () => "checkpoint-1"),
    close: vi.fn(async () => {
      closed = true;
    }),
    conflictsJson: vi.fn(async () => ['{"status":"conflict"}']),
    enqueue: vi.fn(async () => initialStatus),
    isClosed: vi.fn(() => closed),
    pendingJson: vi.fn(async () => ['{"identity":"commit-1"}']),
    recordPushResponse: vi.fn(async () => initialStatus),
    resolveConflictKeepLocal: vi.fn(async () => initialStatus),
    resolveConflictKeepServer: vi.fn(async () => initialStatus),
    resolveConflictMerge: vi.fn(async () => initialStatus),
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
    await expect(client.checkpointToken({ signal })).resolves.toBe(
      "checkpoint-1",
    );
    expect(native.checkpointToken).toHaveBeenCalledWith({ signal });
  });

  it("forwards explicit conflict decisions through stable facade methods", async () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    const signal = new AbortController().signal;
    const replacement = {
      identity: { clientCommitId: "merge-1", fingerprint: "untrusted" },
      operations: [],
    } as const;

    await client.resolveConflictKeepServer("conflict-1", { signal });
    await client.resolveConflictKeepLocal("conflict-1", "retry-1", {
      signal,
    });
    await client.resolveConflictMerge("conflict-1", replacement, { signal });

    expect(native.resolveConflictKeepServer).toHaveBeenCalledWith(
      "conflict-1",
      { signal },
    );
    expect(native.resolveConflictKeepLocal).toHaveBeenCalledWith(
      "conflict-1",
      "retry-1",
      { signal },
    );
    expect(native.resolveConflictMerge).toHaveBeenCalledWith(
      "conflict-1",
      JSON.stringify(replacement),
      { signal },
    );
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

  it("preserves canonical bigint, bytes, fractions, NONE, and record values", async () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    const commit = {
      identity: { clientCommitId: "commit-1", fingerprint: "untrusted" },
      operations: [
        {
          kind: "upsert",
          recordId: "person:ada",
          baseVersion: "absent",
          value: {
            bytes: new Uint8Array([1, 2, 3]),
            friend: new SurrealRecordId("person:bob"),
            maximum: 9223372036854775807n,
            missing: NONE,
            servings: 2.5,
          },
        },
      ],
    } as const;

    await client.enqueue(commit);

    const encoded = vi.mocked(native.enqueue).mock.calls[0]?.[0];
    expect(JSON.parse(encoded ?? "null")).toMatchObject({
      operations: [
        {
          value: {
            bytes: { $surreal: "bytes", base64: "AQID" },
            friend: {
              $surreal: "record",
              value: "person:bob",
            },
            maximum: {
              $surreal: "int",
              value: "9223372036854775807",
            },
            missing: { $surreal: "none" },
            servings: 2.5,
          },
        },
      ],
    });
  });
});
