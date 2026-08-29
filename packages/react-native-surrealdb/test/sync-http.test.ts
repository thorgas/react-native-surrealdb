import { describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import {
  ExperimentalSyncHttpAdapter,
  ExperimentalSyncHttpError,
  experimentalCanonicalCborSyncHttpCodec,
  experimentalJsonSyncHttpCodec,
  type ExperimentalSyncHttpCodec,
  type ExperimentalSyncHttpLimits,
  type ExperimentalSyncHttpNativeBridge,
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

function response(
  value: unknown,
  statusCode = 200,
  contentType = "application/json",
): Response {
  return new Response(JSON.stringify(value), {
    headers: { "Content-Type": contentType },
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
  limits?: Partial<ExperimentalSyncHttpLimits>,
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
    limits,
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

    await expect(adapter.push()).rejects.toMatchObject({ kind: "network" });
    expect(native.recordPushResponse).not.toHaveBeenCalled();
    await expect(adapter.push()).resolves.toEqual([status]);
    expect(native.recordPushResponse).toHaveBeenCalledOnce();
  });

  it("uses an injected codec and serializes concurrent operations", async () => {
    let releaseFirst: (() => void) | undefined;
    const firstResponse = new Promise<Response>((resolve) => {
      releaseFirst = () =>
        resolve(
          response(
            { status: "accepted" },
            200,
            "application/vnd.surrealdb.sync.test",
          ),
        );
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockReturnValueOnce(firstResponse)
      .mockResolvedValueOnce(
        response(pullResponse(), 200, "application/vnd.surrealdb.sync.test"),
      );
    const body = new ArrayBuffer(3);
    const encode = vi.fn(async () => body);
    const decode = vi.fn(async (value: ArrayBuffer) =>
      new TextDecoder().decode(value),
    );
    const codec: ExperimentalSyncHttpCodec = {
      mediaType: "application/vnd.surrealdb.sync.test",
      encodePushRequest: encode,
      decodePushResponse: decode,
      encodePullRequest: encode,
      decodePullResponse: decode,
    };
    const { adapter } = createAdapter(fetch, codec);

    const push = adapter.push();
    const pull = adapter.pull();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    releaseFirst?.();
    await expect(push).resolves.toEqual([status]);
    await expect(pull).resolves.toEqual(status);

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(encode).toHaveBeenCalledTimes(2);
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
    expect(fetch.mock.calls[0]?.[1]?.signal).not.toBe(controller.signal);
    await expect(
      experimentalJsonSyncHttpCodec.encodePullRequest({
        partitionId: "p",
        clientId: "c",
        requestedScope: "all",
        subscriptionRevision: 2n ** 63n,
      }),
    ).rejects.toThrow("safe range");
  });

  it("bounds request and response bodies before protocol decoding", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    const oversizedCodec: ExperimentalSyncHttpCodec = {
      ...experimentalJsonSyncHttpCodec,
      encodePushRequest: vi.fn(async () => new ArrayBuffer(5)),
    };
    const request = createAdapter(fetch, oversizedCodec, {
      maxRequestBytes: 4,
    }).adapter;
    await expect(request.push()).rejects.toMatchObject({ kind: "body_limit" });
    expect(fetch).not.toHaveBeenCalled();

    const declared = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response("12345", {
        headers: {
          "Content-Length": "5",
          "Content-Type": "application/json",
        },
      }),
    );
    const declaredAdapter = createAdapter(declared, undefined, {
      maxResponseBytes: 4,
    }).adapter;
    await expect(declaredAdapter.pull()).rejects.toMatchObject({
      kind: "body_limit",
    });

    const streamed = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response("12345"));
    const streamedAdapter = createAdapter(streamed, undefined, {
      maxResponseBytes: 4,
    }).adapter;
    await expect(streamedAdapter.pull()).rejects.toMatchObject({
      kind: "body_limit",
    });
  });

  it("rejects wrong content types without decoding the body", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(pullResponse(), 200, "text/plain"));
    const { adapter, native } = createAdapter(fetch);

    await expect(adapter.pull()).rejects.toMatchObject({
      kind: "content_type",
    });
    expect(native.applyPullResponse).not.toHaveBeenCalled();
  });

  it("distinguishes timeout from caller cancellation", async () => {
    const abortingFetch = vi.fn<typeof globalThis.fetch>(
      (_url, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }),
    );
    const timeoutAdapter = createAdapter(abortingFetch, undefined, {
      requestTimeoutMs: 1,
    }).adapter;
    await expect(timeoutAdapter.pull()).rejects.toMatchObject({
      kind: "timeout",
    });

    const controller = new AbortController();
    const cancelled = createAdapter(abortingFetch).adapter.pull({
      signal: controller.signal,
    });
    controller.abort();
    await expect(cancelled).rejects.toMatchObject({ kind: "aborted" });
  });

  it("wires canonical CBOR bytes and raw protocol JSON without coercion", async () => {
    const pushGolden = Uint8Array.from([0x84, 0x70, 0x73]).buffer;
    const pullGolden = Uint8Array.from([0x84, 0x70, 0x74]).buffer;
    const bridge: ExperimentalSyncHttpNativeBridge = {
      encodeSyncPushRequest: vi.fn(async () => pushGolden),
      decodeSyncPushResponse: vi.fn(async () => '{"status":"push"}'),
      encodeSyncPullRequest: vi.fn(async () => pullGolden),
      decodeSyncPullResponse: vi.fn(async () => '{"status":"pull"}'),
    };
    const codec = experimentalCanonicalCborSyncHttpCodec(bridge);

    const pushBody = await codec.encodePushRequest({
      partitionId: "p",
      clientId: "c",
      commitJson: '{"identity":{"clientCommitId":"i"}}',
    });
    expect(pushBody).toEqual(pushGolden);
    expect(await codec.decodePushResponse(pushGolden)).toBe(
      '{"status":"push"}',
    );

    const pullBody = await codec.encodePullRequest({
      partitionId: "p",
      clientId: "c",
      checkpoint: "opaque",
      requestedScope: "all",
      subscriptionRevision: 2n ** 63n,
    });
    expect(pullBody).toEqual(pullGolden);
    expect(await codec.decodePullResponse(pullGolden)).toBe(
      '{"status":"pull"}',
    );
    expect(bridge.encodeSyncPullRequest).toHaveBeenCalledWith(
      "p",
      "c",
      "opaque",
      "all",
      2n ** 63n,
      undefined,
    );
  });
});
