import { describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import {
  ExperimentalSyncHttpAdapter,
  ExperimentalSyncHttpError,
  experimentalJsonSyncHttpCodec,
  type ExperimentalSyncHttpCodec,
} from "../src/sync-http";
import { ExperimentalSyncClient } from "../src/sync";

const status: NativeSyncStatus = {
  revision: 1n,
  pendingCount: 1,
  outcomeCount: 0,
  conflictCount: 0,
};

const commit = {
  identity: {
    clientCommitId: "commit-1",
    fingerprint: "fingerprint-1",
  },
  operations: [],
} as const;

function createSync() {
  const native = {
    applyPullResponse: vi.fn(async () => status),
    checkpointToken: vi.fn(async () => "checkpoint-0"),
    close: vi.fn(async () => undefined),
    conflictsJson: vi.fn(async () => []),
    enqueue: vi.fn(async () => status),
    isClosed: vi.fn(() => false),
    pendingJson: vi.fn(async () => [JSON.stringify(commit)]),
    recordPushResponse: vi.fn(async () => status),
    status: vi.fn(async () => status),
  };
  return {
    native,
    sync: new ExperimentalSyncClient(native as NativeSyncClientLike),
  };
}

function response(value: unknown, statusCode = 200): Response {
  return new Response(JSON.stringify(value), {
    headers: { "Content-Type": "application/json" },
    status: statusCode,
  });
}

function pullResponse(checkpoint = "checkpoint-1") {
  return {
    response: "reset",
    reason: "checkpoint_expired",
    checkpoint: {
      token: checkpoint,
      cursor: { epoch: 1, sequence: 1 },
      scope: {
        identity: "all",
        authorizationRevision: 1,
        subscriptionRevision: 7,
      },
    },
    records: [],
  };
}

function createAdapter(
  fetch: typeof globalThis.fetch,
  codec: ExperimentalSyncHttpCodec = experimentalJsonSyncHttpCodec,
) {
  const { native, sync } = createSync();
  const adapter = new ExperimentalSyncHttpAdapter({
    sync,
    baseUrl: "https://sync.example.test/",
    partitionId: "partition-1",
    clientId: "client-1",
    requestedScope: "all",
    subscriptionRevision: 7n,
    accessToken: async () => "redacted-test-token",
    codec,
    fetch,
  });
  return { adapter, native };
}

describe("ExperimentalSyncHttpAdapter", () => {
  it("pushes pending commits then pulls from the durable checkpoint", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response({
          schemaVersion: "v1",
          partitionId: "partition-1",
          clientId: "client-1",
          outcome: { status: "accepted" },
        }),
      )
      .mockResolvedValueOnce(response(pullResponse()));
    const { adapter, native } = createAdapter(fetch);
    const signal = new AbortController().signal;

    await expect(adapter.syncOnce({ signal })).resolves.toEqual({
      push: [status],
      pull: status,
    });

    expect(fetch).toHaveBeenCalledTimes(2);
    const [pushUrl, pushInit] = fetch.mock.calls[0]!;
    expect(pushUrl).toBe("https://sync.example.test/v1/sync/push");
    expect(pushInit).toMatchObject({
      method: "POST",
      signal,
      headers: {
        Accept: "application/json",
        Authorization: "Bearer redacted-test-token",
        "Content-Type": "application/json",
      },
    });
    expect(JSON.parse(pushInit?.body as string)).toEqual({
      schemaVersion: "v1",
      partitionId: "partition-1",
      clientId: "client-1",
      commit,
    });
    const [pullUrl, pullInit] = fetch.mock.calls[1]!;
    expect(pullUrl).toBe("https://sync.example.test/v1/sync/pull");
    expect(JSON.parse(pullInit?.body as string)).toEqual({
      schemaVersion: "v1",
      partitionId: "partition-1",
      clientId: "client-1",
      checkpoint: "checkpoint-0",
      requestedScope: "all",
      subscriptionRevision: 7,
    });
    expect(native.recordPushResponse).toHaveBeenCalledOnce();
    expect(native.applyPullResponse).toHaveBeenCalledOnce();
    expect(native.checkpointToken).toHaveBeenCalledOnce();
  });

  it.each([400, 401, 403])(
    "does not apply an HTTP %s response to native state",
    async (statusCode) => {
      const fetch = vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValue(response({ error: "redacted" }, statusCode));
      const { adapter, native } = createAdapter(fetch);

      await expect(adapter.push()).rejects.toEqual(
        expect.objectContaining<Partial<ExperimentalSyncHttpError>>({
          name: "ExperimentalSyncHttpError",
          status: statusCode,
        }),
      );
      expect(native.recordPushResponse).not.toHaveBeenCalled();
    },
  );

  it("leaves a lost push response pending and permits an explicit retry", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockRejectedValueOnce(new TypeError("network lost"))
      .mockResolvedValueOnce(
        response({
          schemaVersion: "v1",
          partitionId: "partition-1",
          clientId: "client-1",
          outcome: { status: "accepted" },
        }),
      );
    const { adapter, native } = createAdapter(fetch);

    await expect(adapter.push()).rejects.toThrow("network lost");
    expect(native.recordPushResponse).not.toHaveBeenCalled();
    await expect(adapter.push()).resolves.toEqual([status]);
    expect(native.recordPushResponse).toHaveBeenCalledOnce();
  });

  it("uses an injected codec and serializes concurrent operations", async () => {
    let releaseFirst: (() => void) | undefined;
    const firstResponse = new Promise<Response>((resolve) => {
      releaseFirst = () => resolve(response({ status: "accepted" }));
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockReturnValueOnce(firstResponse)
      .mockResolvedValueOnce(response(pullResponse()));
    const body = new ArrayBuffer(3);
    const codec: ExperimentalSyncHttpCodec = {
      mediaType: "application/vnd.surrealdb.sync.test",
      encode: vi.fn(() => body),
      decode: vi.fn(async (value) => value.json()),
    };
    const { adapter } = createAdapter(fetch, codec);

    const push = adapter.push();
    const pull = adapter.pull();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    releaseFirst?.();
    await expect(push).resolves.toEqual([status]);
    await expect(pull).resolves.toEqual(status);

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(codec.encode).toHaveBeenCalledTimes(2);
    expect(fetch.mock.calls[0]?.[1]?.body).toBe(body);
  });

  it("reuses the old native checkpoint when pull application fails", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementation(async () => response(pullResponse("checkpoint-2")));
    const { adapter, native } = createAdapter(fetch);
    native.checkpointToken.mockResolvedValue("checkpoint-1");
    native.applyPullResponse
      .mockRejectedValueOnce(new Error("native persistence unavailable"))
      .mockResolvedValueOnce(status);

    await expect(adapter.pull()).rejects.toThrow("persistence unavailable");
    expect(native.applyPullResponse).toHaveBeenCalledOnce();

    await expect(adapter.pull()).resolves.toEqual(status);
    expect(native.applyPullResponse).toHaveBeenCalledTimes(2);
    expect(native.checkpointToken).toHaveBeenCalledTimes(2);
    for (const [, init] of fetch.mock.calls) {
      expect(JSON.parse(init?.body as string).checkpoint).toBe("checkpoint-1");
    }
  });

  it("forwards cancellation and rejects unsafe JSON integers", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(pullResponse()));
    const { adapter } = createAdapter(fetch);
    const controller = new AbortController();

    await adapter.pull({ signal: controller.signal });
    expect(fetch.mock.calls[0]?.[1]?.signal).toBe(controller.signal);
    expect(() =>
      experimentalJsonSyncHttpCodec.encode({ value: 2n ** 63n }),
    ).toThrow("safe range");
  });
});
