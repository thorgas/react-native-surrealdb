import { createNativeReadMocks } from "./helpers/native-read-mocks";
import { describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import { NativeConflictPolicy } from "../src/generated/surrealdb_rn_core";
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
    ...createNativeReadMocks(),
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
    recordPushResponseWithPolicy: vi.fn(async () => initialStatus),
    resolveConflictKeepLocal: vi.fn(async () => initialStatus),
    resolveConflictKeepServer: vi.fn(async () => initialStatus),
    resolveConflictMerge: vi.fn(async () => initialStatus),
    status: vi.fn(async () => initialStatus),
    diagnostics: vi.fn(() => ({
      schemaVersion: 1,
      spanPublishDrops: 0n,
      runtimeLockAcquisitions: 1n,
      runtimeLockWaitNs: 2n,
      runtimeLockHoldNs: 3n,
      storageAttempts: 1n,
      storageNs: 4n,
      storageErrors: 0n,
      stateEncodingAttempts: 1n,
      stateEncodingNs: 1n,
    })),
  } as NativeSyncClientLike;
  return native;
}

describe("ExperimentalSyncClient", () => {
  it("uses bounded native APIs and never the all-pending fallback", async () => {
    const native = createNative();
    const result = {
      revision: 2n,
      commits: ["{}"],
      bytes: 8,
      remainingCount: 1,
      firstCommitTooLarge: false,
    };
    const pendingWindow = vi.fn(
      async (_count: number, _bytes: number) => result,
    );
    const enqueueBounded = vi.fn(
      async (_commit: string, _count: number, _bytes: number) => initialStatus,
    );
    const client = new ExperimentalSyncClient({
      ...native,
      pendingWindow,
      enqueueBounded,
    });
    expect(await client.pendingWindow(2, 1024)).toBe(result);
    expect(pendingWindow).toHaveBeenCalledWith(2, 1024, undefined);
    expect(await client.enqueueBounded({ value: 42n }, 4, 2048)).toEqual(
      initialStatus,
    );
    expect(enqueueBounded.mock.calls[0]?.[0]).toContain("42");
    expect(native.pendingJson).not.toHaveBeenCalled();
    expect(native.enqueue).not.toHaveBeenCalled();
    await expect(client.pendingWindow(0, 1024)).rejects.toThrow(RangeError);
    await expect(client.enqueueBounded({}, 65, 1024)).rejects.toThrow(
      RangeError,
    );
  });
  it("refuses old queue ABI without fallback", async () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    await expect(client.pendingWindow(1, 1024)).rejects.toThrow(
      "ABI unavailable",
    );
    await expect(client.enqueueBounded({}, 1, 1024)).rejects.toThrow(
      "ABI unavailable",
    );
    expect(native.pendingJson).not.toHaveBeenCalled();
    expect(native.enqueue).not.toHaveBeenCalled();
  });

  it("exposes native measurements without React or async runtime work", () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    expect(client.nativeDiagnostics().storageNs).toBe(4n);
    expect(native.diagnostics).toHaveBeenCalledOnce();
    expect(native.status).not.toHaveBeenCalled();
  });
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

  it("requires an explicit policy call and keeps the ordinary response manual", async () => {
    const native = createNative();
    const client = new ExperimentalSyncClient(native);
    const response = { schemaVersion: "v1" } as const;
    const signal = new AbortController().signal;

    await client.recordPushResponse(response, { signal });
    expect(native.recordPushResponse).toHaveBeenCalledWith(
      JSON.stringify(response),
      { signal },
    );
    expect(native.recordPushResponseWithPolicy).not.toHaveBeenCalled();

    await client.recordPushResponseWithPolicy(response, "preferServerV1", {
      signal,
    });
    expect(native.recordPushResponseWithPolicy).toHaveBeenCalledWith(
      JSON.stringify(response),
      NativeConflictPolicy.PreferServerV1,
      { signal },
    );
    await client.recordPushResponseWithPolicyProtocolJson(
      JSON.stringify(response),
      "manual",
    );
    expect(native.recordPushResponseWithPolicy).toHaveBeenLastCalledWith(
      JSON.stringify(response),
      NativeConflictPolicy.Manual,
      undefined,
    );
    expect(() =>
      client.recordPushResponseWithPolicy(
        response,
        "lastWriteWins" as "manual",
      ),
    ).toThrow(/Unknown experimental sync conflict policy/);
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
