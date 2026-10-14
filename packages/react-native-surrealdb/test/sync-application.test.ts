import { describe, expect, it, vi } from "vitest";
import {
  ExperimentalSyncApplicationAdapter,
  type ExperimentalSyncCommit,
} from "../src/sync-application";
import { ExperimentalSyncObservedClient } from "../src/sync-observation";
import type { NativeSyncClientLike } from "../src/generated/surrealdb_rn_core";

const status = {
  revision: 2n,
  pendingCount: 1,
  outcomeCount: 0,
  conflictCount: 0,
};
const commit: ExperimentalSyncCommit = {
  identity: { clientCommitId: "private-commit", fingerprint: "native" },
  operations: [{ kind: "delete", record_id: "private-row", base_version: 1 }],
};
function fixture(
  logger?: ConstructorParameters<typeof ExperimentalSyncObservedClient>[1],
) {
  const native = {
    enqueue: vi.fn(async () => status),
    recordPushResponse: vi.fn(async () => status),
    recordPushResponseWithPolicy: vi.fn(async () => status),
    status: vi.fn(async () => status),
  } as unknown as NativeSyncClientLike;
  return {
    native,
    sync: new ExperimentalSyncObservedClient(
      {
        enqueue: native.enqueue,
        recordPushResponseProtocolJson: native.recordPushResponse,
        status: native.status,
      },
      logger,
    ),
  };
}
const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

describe("application sync receipts", () => {
  it("returns a local receipt only after persistence, then wakes the injected scheduler", async () => {
    let resolve!: (value: typeof status) => void;
    const enqueue = vi.fn(
      () =>
        new Promise<typeof status>((done) => {
          resolve = done;
        }),
    );
    const requestSync = vi.fn();
    const adapter = new ExperimentalSyncApplicationAdapter({
      sync: { enqueue },
      encodeCommand: (_command: { remove: string }) => commit,
      requestSync,
    });
    const receipt = adapter.commit({ remove: "private-row" });
    expect(requestSync).not.toHaveBeenCalled();
    resolve(status);
    await expect(receipt).resolves.toEqual({
      clientCommitId: "private-commit",
      milestone: "local_durable",
      status,
    });
    expect(requestSync).toHaveBeenCalledOnce();
  });
  it("does not schedule or report success after a failed durable enqueue", async () => {
    const requestSync = vi.fn();
    const adapter = new ExperimentalSyncApplicationAdapter({
      sync: {
        enqueue: async () => {
          throw new Error("storage unavailable");
        },
      },
      encodeCommand: () => commit,
      requestSync,
    });
    await expect(adapter.commit(undefined)).rejects.toThrow(
      "storage unavailable",
    );
    expect(requestSync).not.toHaveBeenCalled();
  });
  it("keeps durable success when the scheduler callback throws", async () => {
    const { sync } = fixture();
    const adapter = new ExperimentalSyncApplicationAdapter({
      sync,
      encodeCommand: () => commit,
      requestSync: () => {
        throw new Error("observer");
      },
    });
    await expect(adapter.commit(undefined)).resolves.toHaveProperty(
      "milestone",
      "local_durable",
    );
  });
  it("isolates a rejected asynchronous scheduler wakeup", async () => {
    const { sync } = fixture();
    const adapter = new ExperimentalSyncApplicationAdapter({
      sync,
      encodeCommand: () => commit,
      requestSync: async () => {
        throw new Error("scheduler unavailable");
      },
    });
    await expect(adapter.commit(undefined)).resolves.toHaveProperty(
      "milestone",
      "local_durable",
    );
    await settle();
  });
});

describe("persisted outcome diagnostics", () => {
  it.each(["accepted", "conflict", "rejected"] as const)(
    "reports %s only after persistence and strips private fields from logs",
    async (outcome) => {
      const logger = vi.fn();
      const { sync, native } = fixture({ logger });
      const listener = vi.fn();
      const unsubscribe = sync.subscribeEvents(listener);
      const response = {
        outcome: {
          identity: commit.identity,
          status: outcome,
          reason: "private-message",
          authoritative: { value: "private-record" },
        },
      };
      await sync.recordPushResponseProtocolJson(JSON.stringify(response));
      await settle();
      expect(native.recordPushResponse).toHaveBeenCalledOnce();
      expect(listener).toHaveBeenCalledWith({
        clientCommitId: "private-commit",
        milestone: `authority_${outcome}`,
        status,
      });
      expect(logger).toHaveBeenCalledWith({
        milestone: `authority_${outcome}`,
        ...status,
      });
      expect(
        JSON.stringify(logger.mock.calls, (_, value) =>
          typeof value === "bigint" ? String(value) : value,
        ),
      ).not.toContain("private");
      unsubscribe();
      await sync.enqueue(commit);
      expect(listener).toHaveBeenCalledOnce();
    },
  );
  it("never publishes an outcome when native persistence rejects", async () => {
    const { sync, native } = fixture();
    vi.mocked(native.recordPushResponse).mockRejectedValueOnce(
      new Error("disk"),
    );
    const listener = vi.fn();
    sync.subscribeEvents(listener);
    await expect(
      sync.recordPushResponseProtocolJson(
        JSON.stringify({
          outcome: { identity: commit.identity, status: "accepted" },
        }),
      ),
    ).rejects.toThrow("disk");
    expect(listener).not.toHaveBeenCalled();
  });
  it("bounds a hung logger to one invocation and still persists subsequent mutations", async () => {
    const logger = vi.fn(() => new Promise<void>(() => {}));
    const { sync, native } = fixture({ logger });
    await sync.enqueue(commit);
    await settle();
    await sync.enqueue(commit);
    await settle();
    expect(logger).toHaveBeenCalledOnce();
    expect(native.enqueue).toHaveBeenCalledTimes(2);
    const diagnostic = await sync.diagnosticSnapshot();
    expect(diagnostic.logging).toEqual({
      inFlight: true,
      droppedCount: 1,
      errorCount: 0,
    });
    expect(Object.isFrozen(diagnostic)).toBe(true);
    expect(Object.isFrozen(diagnostic.logging)).toBe(true);
    expect(diagnostic).not.toBe(status);
  });
  it("isolates synchronous and asynchronous logger failures and throwing subscribers", async () => {
    const logger = vi
      .fn()
      .mockImplementationOnce(() => {
        throw new Error("log");
      })
      .mockRejectedValueOnce(new Error("log"));
    const { sync } = fixture({ logger });
    sync.subscribeEvents(() => {
      throw new Error("consumer");
    });
    await expect(sync.enqueue(commit)).resolves.toEqual(status);
    await settle();
    await expect(sync.enqueue(commit)).resolves.toEqual(status);
    await settle();
    await expect(sync.enqueue(commit)).resolves.toEqual(status);
    expect(logger).toHaveBeenCalledTimes(3);
    await settle();
    await expect(sync.diagnosticSnapshot()).resolves.toEqual({
      ...status,
      logging: { inFlight: false, droppedCount: 0, errorCount: 2 },
    });
  });
  it("does not fabricate outcomes for opaque or unrecognized metadata", async () => {
    const { sync } = fixture();
    const listener = vi.fn();
    sync.subscribeEvents(listener);
    await sync.recordPushResponseProtocolJson(
      "invalid but accepted by test native",
    );
    await sync.recordPushResponseProtocolJson(
      JSON.stringify({
        outcome: { identity: commit.identity, status: "invented" },
      }),
    );
    expect(listener).not.toHaveBeenCalled();
  });
});
